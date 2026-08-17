"""复用 ffprobe命名补充缓存的 Emby MediaInfo JSON 持久化插件。

本插件刻意不调用 ffprobe。它只消费 FFprobeNamingSupplement 在整理命名阶段
已经缓存的原始 ffprobe JSON，因此两者必须同时启用。
"""

from __future__ import annotations

import json
import re
import sys
from concurrent.futures import Future, ThreadPoolExecutor
from subprocess import TimeoutExpired, run
from threading import Lock, Thread, Timer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type

from app.core.cache import TTLCache
from app.core.config import settings
from app.core.event import Event, eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import FileItem, TransferRenameBuildEventData
from app.schemas.types import ChainEventType, EventType


_MEDIA_INFO_SUFFIX = "-mediainfo.json"
_DOVI_TAGS = frozenset({"dvh1", "dvhe", "dva1", "dvav"})
_DEFAULT_FALLBACK_FFPROBE_TIMEOUT_SEC = 10
_MAX_FALLBACK_FFPROBE_TIMEOUT_SEC = 300
_SOURCE_JSON_CLEANUP_DELAY_SEC = 10
_TRANSFER_METHOD_ALIASES = {
    "复制": "copy", "copy": "copy",
    "移动": "move", "move": "move",
    "硬链接": "hardlink", "硬链": "hardlink", "hardlink": "hardlink", "link": "hardlink",
    "软链接": "softlink", "软链": "softlink", "softlink": "softlink", "symlink": "softlink",
}


class FFprobeMediaInfoPersistence(_PluginBase):
    """将 ffprobe命名补充已缓存的探测结果写为 Emby 可读 JSON。"""

    plugin_name = "ffprobe媒体信息持久化"
    plugin_desc = "复用 ffprobe命名补充的媒体信息并持久化为 Emby MediaInfo JSON。"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/refs/heads/main/icons/ffmpeg.png"
    plugin_version = "1.0.0"
    plugin_author = "gitbose"
    author_url = "https://github.com/gitbose"
    plugin_config_prefix = "ffprobemediainfopersistence_"
    # 命名补充为 50；保持更后顺序，使它先写入自己的 ffprobe 缓存。
    plugin_order = 51
    auth_level = 1

    _pending_probe_cache = TTLCache(
        region="ffprobe_media_info_persistence_pending", maxsize=2048, ttl=3600
    )
    _FILTER_MATCH_ALL = "all"
    _FILTER_MATCH_ANY = "any"
    _FALLBACK_WORKERS = 3

    def __init__(self) -> None:
        super().__init__()
        self._enabled = False
        self._overwrite_json = False
        self._fallback_probe = True
        self._fallback_workers = type(self)._FALLBACK_WORKERS
        self._fallback_timeout = _DEFAULT_FALLBACK_FFPROBE_TIMEOUT_SEC
        self._cleanup_moved_source_json = True
        self._transfer_methods: List[str] = []
        self._destination_roots: List[str] = []
        self._filter_match_mode = type(self)._FILTER_MATCH_ALL
        self._fallback_executor: Optional[ThreadPoolExecutor] = None
        self._fallback_tasks: set[Future] = set()
        self._fallback_tasks_lock = Lock()
        self._cleanup_timers: set[Timer] = set()
        self._cleanup_timers_lock = Lock()

    def init_plugin(self, config: dict = None) -> None:
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._overwrite_json = bool(config.get("overwrite_json"))
        self._fallback_probe = bool(config.get("fallback_probe", True))
        try:
            fallback_workers = int(config.get("fallback_workers", type(self)._FALLBACK_WORKERS))
        except (TypeError, ValueError):
            fallback_workers = type(self)._FALLBACK_WORKERS
        requested_workers = max(1, min(fallback_workers, 10))
        workers_changed = requested_workers != self._fallback_workers
        self._fallback_workers = requested_workers
        try:
            fallback_timeout = int(
                config.get("fallback_timeout", _DEFAULT_FALLBACK_FFPROBE_TIMEOUT_SEC)
            )
        except (TypeError, ValueError):
            fallback_timeout = _DEFAULT_FALLBACK_FFPROBE_TIMEOUT_SEC
        self._fallback_timeout = max(
            1, min(fallback_timeout, _MAX_FALLBACK_FFPROBE_TIMEOUT_SEC)
        )
        self._cleanup_moved_source_json = bool(
            config.get("cleanup_moved_source_json", True)
        )
        self._transfer_methods = type(self)._lines(config.get("transfer_methods"))
        self._destination_roots = type(self)._lines(config.get("destination_roots"))
        mode = str(config.get("filter_match_mode") or type(self)._FILTER_MATCH_ALL)
        self._filter_match_mode = (
            mode
            if mode in (type(self)._FILTER_MATCH_ALL, type(self)._FILTER_MATCH_ANY)
            else type(self)._FILTER_MATCH_ALL
        )
        if not self._enabled:
            type(self)._clear_pending_cache()
            self._stop_background_tasks()
            logger.info("【ffprobe媒体信息持久化】插件未启用，不监听整理事件")
        elif workers_changed and self._fallback_executor is not None:
            self._stop_background_tasks()
            self._fallback_executor = ThreadPoolExecutor(
                max_workers=self._fallback_workers,
                thread_name_prefix="ffprobe-media-info",
            )
        elif self._fallback_executor is None:
            self._fallback_executor = ThreadPoolExecutor(
                max_workers=self._fallback_workers,
                thread_name_prefix="ffprobe-media-info",
            )
        if self._enabled:
            logger.info(
                "【ffprobe媒体信息持久化】插件已启用：主动提取=%s，并发=%s，超时=%s 秒，清理孤立 JSON=%s",
                self._fallback_probe,
                self._fallback_workers,
                self._fallback_timeout,
                self._cleanup_moved_source_json,
            )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """按整理流程排序的五行紧凑配置页。"""
        return [{"component": "VForm", "content": [
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                    {"component": "VSwitch", "props": {
                        "model": "enabled", "label": "启用插件",
                        "hint": "开启后监听媒体整理事件并写入 MediaInfo JSON。",
                        "persistent-hint": True,
                    }}]},
                {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                    {"component": "VSwitch", "props": {
                        "model": "cleanup_moved_source_json", "label": "清理已搬离源文件的同名 MediaInfo JSON",
                        "hint": "整理完成 10 秒后，媒体文件若被删除，则删除同目录下严格同名的 JSON 文件。",
                        "persistent-hint": True,
                    }}]},
            ]},
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                    {"component": "VSwitch", "props": {
                        "model": "fallback_probe", "label": "上游缓存缺失时主动提取",
                        "hint": "仅缓存未命中时，对整理后的目标文件执行 ffprobe；任务在后台运行。",
                        "persistent-hint": True,
                    }}]},
                {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                    {"component": "VSwitch", "props": {
                        "model": "overwrite_json", "label": "覆盖同名 JSON",
                        "hint": "关闭时保留目标目录中已有的同名 JSON。",
                        "persistent-hint": True,
                    }}]},
            ]},
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                    {"component": "VTextField", "props": {
                        "model": "fallback_workers", "label": "主动提取并发数",
                        "type": "number", "min": 1, "max": 10,
                        "hint": "范围 1–10；驱动为网盘时不建议设置过大。",
                        "persistent-hint": True,
                    }}]},
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                    {"component": "VTextField", "props": {
                        "model": "fallback_timeout", "label": "主动提取超时（秒）",
                        "type": "number", "min": 1, "max": 300,
                        "hint": "默认 10 秒，范围 1–300 秒。",
                        "persistent-hint": True,
                    }}]},
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                    {"component": "VSelect", "props": {
                        "model": "filter_match_mode", "label": "生成 JSON 匹配条件",
                        "items": [
                            {"title": "同时匹配", "value": "all"},
                            {"title": "任一匹配", "value": "any"},
                        ],
                        "hint": "同时匹配：所有已填写条件都命中；任一匹配：任意一项已填写条件命中。",
                        "persistent-hint": True,
                    }}]},
            ]},
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12}, "content": [
                    {"component": "VTextarea", "props": {
                        "model": "transfer_methods", "label": "限定整理方式（可选，一行一个）",
                        "placeholder": "复制\n移动\n硬链接\n软链接", "rows": 3,
                        "hint": "可填写：复制、移动、硬链接、软链接。留空表示不限制整理方式。",
                        "persistent-hint": True,
                    }}]},
            ]},
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12}, "content": [
                    {"component": "VTextarea", "props": {
                        "model": "destination_roots", "label": "限定整理目标路径（可选，一行一个）",
                        "placeholder": "/media/电影\n/media/剧集", "rows": 3,
                        "hint": "只有最终文件在任一填写目录下才生成 JSON；留空表示不限制。填写 MP 容器内路径。",
                        "persistent-hint": True,
                    }}]},
            ]},
            {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "density": "compact", "text": "使用说明：优先复用“ffprobe命名补充”已获取的缓存，缓存命中后立即后台写入 JSON，不限制写入线程。仅缓存缺失时才按“主动提取”配置对最终目标文件运行 ffprobe。上游 ffprobe 未请求章节，因此输出 JSON 的 Chapters 为空。"}},
            {"component": "VAlert", "props": {"type": "warning", "variant": "tonal", "density": "compact", "text": "JSON清理：文件整理完成后延迟 10 秒检查，若媒体文件已不存在，则清理媒体文件目录下严格同名的 -mediainfo.json 文件。"}},
        ]}], {
            "enabled": False,
            "overwrite_json": False,
            "fallback_probe": True,
            "fallback_workers": 3,
            "fallback_timeout": _DEFAULT_FALLBACK_FFPROBE_TIMEOUT_SEC,
            "cleanup_moved_source_json": True,
            "filter_match_mode": "all",
            "transfer_methods": "",
            "destination_roots": "",
        }

    def get_page(self) -> Optional[List[dict]]:
        # 与 ffprobe命名补充保持一致：不定义独立详情页。
        pass

    def stop_service(self) -> None:
        type(self)._clear_pending_cache()
        self._stop_background_tasks()

    def _stop_background_tasks(self) -> None:
        """停止尚未开始的兜底任务；已运行的 ffprobe 仍受 10 秒超时保护。"""
        executor = self._fallback_executor
        self._fallback_executor = None
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        with self._fallback_tasks_lock:
            self._fallback_tasks.clear()
        with self._cleanup_timers_lock:
            for timer in self._cleanup_timers:
                timer.cancel()
            self._cleanup_timers.clear()

    def _submit_cached_persist(self, destination: Path, probe: Dict[str, Any]) -> None:
        """每个缓存命中的文件直接启动写入线程，不参与 ffprobe 兜底限流。"""
        Thread(
            target=self._persist,
            args=(destination, probe),
            name="ffprobe-media-info-write",
            daemon=True,
        ).start()

    def _submit_fallback_task(self, destination: Path) -> None:
        """提交后台兜底探测，不阻塞 MP 当前整理事件。"""
        executor = self._fallback_executor
        if executor is None:
            logger.warning("【ffprobe媒体信息持久化】后台提取器未启动，跳过：%s", destination)
            return
        future = executor.submit(self._background_probe_and_persist, destination)
        with self._fallback_tasks_lock:
            self._fallback_tasks.add(future)
        future.add_done_callback(self._on_background_task_done)

    def _on_background_task_done(self, future: Future) -> None:
        with self._fallback_tasks_lock:
            self._fallback_tasks.discard(future)
        try:
            future.result()
        except Exception as error:
            logger.warning("【ffprobe媒体信息持久化】后台 MediaInfo 任务异常：%s", error)

    def _background_probe_and_persist(self, destination: Path) -> None:
        """对已整理到位的目标文件执行一次 10 秒兜底探测并写入 JSON。"""
        if not destination.is_file():
            logger.warning("【ffprobe媒体信息持久化】后台提取时目标不存在，跳过：%s", destination)
            return
        logger.info("【ffprobe媒体信息持久化】开始后台 ffprobe：%s", destination)
        probe = type(self)._run_fallback_ffprobe(
            str(destination), self._fallback_timeout
        )
        if not isinstance(probe, dict):
            logger.warning("【ffprobe媒体信息持久化】后台 ffprobe 未得到结果，跳过：%s", destination)
            return
        self._persist(destination, probe)

    @classmethod
    def _clear_pending_cache(cls) -> None:
        try:
            cls._pending_probe_cache.clear()
        except Exception as error:
            logger.debug("【ffprobe媒体信息持久化】清理缓存失败：%s", error)

    @staticmethod
    def _lines(value: Any) -> List[str]:
        """读取一行一个的配置，删除空行与两侧空白。"""
        return [line.strip() for line in str(value or "").splitlines() if line.strip()]

    @staticmethod
    def _normalize(value: Any) -> str:
        return str(value or "").strip().casefold()

    @classmethod
    def _normalize_transfer_method(cls, value: Any) -> str:
        """将中文配置和 MP 内部整理方式值映射到统一值。"""
        normalized = cls._normalize(value)
        return _TRANSFER_METHOD_ALIASES.get(normalized, normalized)

    @staticmethod
    def _value(value: Any, names: Tuple[str, ...]) -> Any:
        """兼容 MP 事件中 dict、Pydantic 模型和普通对象三种传参。"""
        if isinstance(value, dict):
            for name in names:
                if value.get(name) is not None:
                    return value[name]
            return None
        for name in names:
            candidate = getattr(value, name, None)
            if candidate is not None:
                return candidate
        return None

    @classmethod
    def _event_transfer_info(cls, data: Any) -> Any:
        return cls._value(data, ("transferinfo", "transfer_info")) or data

    @classmethod
    def _source_path(cls, data: Any) -> Optional[str]:
        transfer_info = cls._event_transfer_info(data)
        value = cls._value(
            transfer_info,
            ("source_path", "src_path", "src", "source", "origin_path", "path"),
        )
        if value is None:
            value = cls._value(data, ("source_path", "src_path", "src", "source"))
        return str(value).strip() if value else None

    @classmethod
    def _destination_path(cls, data: Any) -> Optional[Path]:
        transfer_info = cls._event_transfer_info(data)
        value = cls._value(
            transfer_info,
            ("target_path", "dest_path", "dest", "target", "destination"),
        )
        if value is None:
            value = cls._value(data, ("target_path", "dest_path", "dest", "target"))
        if not value:
            return None
        path = Path(str(value).strip())
        return path if path.suffix else None

    @classmethod
    def _transfer_method(cls, data: Any) -> str:
        """读取不同 MP 版本可能采用的整理方式字段。"""
        transfer_info = cls._event_transfer_info(data)
        value = cls._value(
            transfer_info,
            ("transfer_mode", "transfer_type", "transfer_method", "mode", "method", "type"),
        )
        if value is None:
            value = cls._value(
                data,
                ("transfer_mode", "transfer_type", "transfer_method", "mode", "method"),
            )
        # 枚举类字段的 value 通常才是配置中应填写的值。
        return str(getattr(value, "value", value) or "").strip()

    def _matches_filters(self, destination: Optional[Path], transfer_method: str) -> bool:
        """依据配置执行“同时匹配”或“任一匹配”。"""
        matches: List[bool] = []
        if self._transfer_methods:
            allowed_methods = {
                type(self)._normalize_transfer_method(item)
                for item in self._transfer_methods
            }
            matches.append(
                type(self)._normalize_transfer_method(transfer_method) in allowed_methods
            )
        if self._destination_roots:
            if destination is None:
                # 未得知最终路径时不能保证命中路径白名单，延后至完成事件处理。
                matches.append(False)
            else:
                destination_text = str(destination).replace("\\", "/").rstrip("/").casefold()
                matches.append(any(
                    root_text
                    and (destination_text == root_text or destination_text.startswith(root_text + "/"))
                    for root_text in (
                        root.replace("\\", "/").rstrip("/").casefold()
                        for root in self._destination_roots
                    )
                ))
        if not matches:
            return True
        if self._filter_match_mode == type(self)._FILTER_MATCH_ANY:
            return any(matches)
        return all(matches)

    @staticmethod
    def _is_media_path(path: str) -> bool:
        return Path(path).suffix.lower() in settings.RMT_MEDIAEXT

    def _schedule_source_json_cleanup(self, source_path: str) -> None:
        """给媒体文件刷新留出 10 秒窗口，再判断是否需要清理孤立 JSON。"""
        if not self._cleanup_moved_source_json:
            return
        timer: Timer

        def cleanup_task() -> None:
            try:
                self._cleanup_source_json(source_path)
            finally:
                with self._cleanup_timers_lock:
                    self._cleanup_timers.discard(timer)

        timer = Timer(_SOURCE_JSON_CLEANUP_DELAY_SEC, cleanup_task)
        timer.daemon = True
        with self._cleanup_timers_lock:
            self._cleanup_timers.add(timer)
        timer.start()

    def _cleanup_source_json(self, source_path: str) -> None:
        """源文件在延迟检查时不存在，就删除严格同名的持久化 JSON。"""
        try:
            if not self._cleanup_moved_source_json:
                return
            source = Path(source_path)
            if source.exists():
                return
            source_json = source.with_name(source.stem + _MEDIA_INFO_SUFFIX)
            if not source_json.is_file():
                return
            source_json.unlink()
            logger.info("【ffprobe媒体信息持久化】已清理已搬离源文件的 MediaInfo JSON：%s", source_json)
        except OSError as error:
            logger.warning("【ffprobe媒体信息持久化】清理源 JSON 失败 source=%s error=%s", source_path, error)

    @staticmethod
    def _cache_key(source_path: str) -> str:
        return str(Path(source_path)).casefold()

    @classmethod
    def _naming_plugin_class(cls) -> Optional[Type[Any]]:
        """查找运行时已加载的 ffprobe命名补充类，不导入或修改其源码。"""
        for module in tuple(sys.modules.values()):
            candidate = getattr(module, "FFprobeNamingSupplement", None)
            if (
                isinstance(candidate, type)
                and getattr(candidate, "plugin_config_prefix", None)
                == "ffprobenamingsupplement_"
                and hasattr(candidate, "_probe_cache")
            ):
                return candidate
        return None

    @classmethod
    def _get_cached_probe(cls, source_path: str) -> Optional[Dict[str, Any]]:
        """从上游插件的私有缓存取结果；兜底探测由调用方明确决定。"""
        plugin_class = cls._naming_plugin_class()
        if plugin_class is None:
            logger.debug("【ffprobe媒体信息持久化】未加载 ffprobe命名补充，跳过")
            return None
        try:
            probe_target = plugin_class._resolve_probe_target(source_path)
            if not probe_target:
                return None
            result = plugin_class._probe_cache.get(probe_target)
            return result if isinstance(result, dict) else None
        except Exception as error:
            logger.debug("【ffprobe媒体信息持久化】读取上游 ffprobe 缓存失败：%s", error)
            return None

    @classmethod
    def _run_fallback_ffprobe(
        cls, media_path: str, timeout: int
    ) -> Optional[Dict[str, Any]]:
        """缓存缺失时的唯一兜底：按配置超时执行一次 ffprobe。"""
        probe_target = media_path
        naming_plugin = cls._naming_plugin_class()
        if naming_plugin is not None:
            try:
                probe_target = naming_plugin._resolve_probe_target(media_path)
            except Exception:
                probe_target = media_path
        if not probe_target:
            return None
        try:
            process = run(
                [
                    "ffprobe", "-v", "error", "-print_format", "json", "-show_streams",
                    "-show_format", "-i", str(probe_target),
                ],
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except TimeoutExpired:
            logger.warning(
                "【ffprobe媒体信息持久化】兜底 ffprobe 超时（%s 秒），跳过：%s",
                timeout,
                probe_target,
            )
            return None
        except OSError as error:
            logger.warning("【ffprobe媒体信息持久化】无法执行兜底 ffprobe：%s", error)
            return None
        if process.returncode != 0:
            logger.debug("【ffprobe媒体信息持久化】兜底 ffprobe 失败 rc=%s target=%s err=%s", process.returncode, probe_target, (process.stderr or "")[:500])
            return None
        try:
            result = json.loads(process.stdout)
        except (TypeError, json.JSONDecodeError) as error:
            logger.warning("【ffprobe媒体信息持久化】兜底 ffprobe JSON 解析失败：%s", error)
            return None
        return result if isinstance(result, dict) else None

    @eventmanager.register(ChainEventType.TransferRenameBuild)
    def on_transfer_rename_build(self, event: Event) -> None:
        """在上游命名插件提取后暂存其 JSON，等待整理实际完成后写入目标目录。"""
        if not self._enabled:
            return
        data = event.event_data
        if not isinstance(data, TransferRenameBuildEventData):
            logger.warning("【ffprobe媒体信息持久化】收到非预期的重命名构建事件数据，跳过")
            return
        source_item: Optional[FileItem] = data.source_item
        source_path = str(data.source_path or "").strip()
        if not source_path or not source_item or source_item.storage != "local":
            logger.warning("【ffprobe媒体信息持久化】重命名构建事件缺少本地源文件，跳过")
            return
        if not type(self)._is_media_path(source_path):
            logger.info("【ffprobe媒体信息持久化】源文件不是受支持媒体类型，跳过：%s", source_path)
            return
        logger.info("【ffprobe媒体信息持久化】收到重命名构建事件：%s", source_path)
        destination = type(self)._destination_path(data)
        transfer_method = type(self)._transfer_method(data)
        # 如果此时已有目标路径，先筛选再读取缓存，避免无用处理。
        if destination is not None and not self._matches_filters(destination, transfer_method):
            logger.info("【ffprobe媒体信息持久化】不符合生成 JSON 筛选条件，跳过：%s", source_path)
            return
        probe = type(self)._get_cached_probe(source_path)
        if probe is None:
            logger.info(
                "【ffprobe媒体信息持久化】未命中上游 ffprobe 缓存，等待整理完成后按配置决定是否后台兜底 source=%s",
                source_path,
            )
            return
        type(self)._pending_probe_cache.set(
            type(self)._cache_key(source_path),
            {
                "probe": probe,
                "transfer_method": transfer_method,
            },
        )
        logger.info("【ffprobe媒体信息持久化】已复用上游 ffprobe 缓存：%s", source_path)

    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event: Event) -> None:
        """整理成功后，从暂存结果写入实际整理目录。"""
        if not self._enabled:
            return
        data = event.event_data or {}
        destination = type(self)._destination_path(data)
        source_path = type(self)._source_path(data)
        if destination is None or source_path is None:
            logger.warning("【ffprobe媒体信息持久化】整理完成事件缺少源或目标路径，跳过")
            return
        if not type(self)._is_media_path(source_path):
            logger.info("【ffprobe媒体信息持久化】整理完成事件的源文件不是受支持媒体类型，跳过：%s", source_path)
            return
        logger.info("【ffprobe媒体信息持久化】收到整理完成事件：%s -> %s", source_path, destination)
        transfer_method = type(self)._transfer_method(data)
        try:
            if not self._matches_filters(destination, transfer_method):
                logger.info("【ffprobe媒体信息持久化】目标不符合生成 JSON 筛选条件，跳过：%s", destination)
                return
            pending = type(self)._pending_probe_cache.get(type(self)._cache_key(source_path))
            probe = pending.get("probe") if isinstance(pending, dict) else None
            if not isinstance(probe, dict):
                # 若本插件在前一个链式事件中稍早执行，完成事件时再读一次上游缓存。
                probe = type(self)._get_cached_probe(source_path)
            if not isinstance(probe, dict):
                if self._fallback_probe:
                    self._submit_fallback_task(destination)
                    logger.info(
                        "【ffprobe媒体信息持久化】未命中上游缓存，已提交后台 ffprobe（%s 秒超时）：%s",
                        self._fallback_timeout,
                        destination,
                    )
                else:
                    logger.warning("【ffprobe媒体信息持久化】没有可复用的 ffprobe 结果，跳过：%s", destination)
                return
            # 命中缓存的 JSON 写入不受 ffprobe 兜底线程数限制；每条整理记录自行完成。
            self._submit_cached_persist(destination, probe)
            logger.info("【ffprobe媒体信息持久化】已提交缓存 JSON 写入：%s", destination)
        finally:
            # 清理放在该条整理记录的处理末尾；不受生成 JSON 筛选条件影响。
            self._schedule_source_json_cleanup(source_path)

    def _persist(self, destination: Path, probe: Dict[str, Any]) -> None:
        if not destination.is_file():
            logger.warning("【ffprobe媒体信息持久化】整理目标不存在，跳过：%s", destination)
            return
        json_path = destination.with_name(destination.stem + _MEDIA_INFO_SUFFIX)
        if json_path.exists() and not self._overwrite_json:
            logger.info("【ffprobe媒体信息持久化】JSON 已存在，按配置不覆盖：%s", json_path)
            return
        try:
            document = type(self)._to_emby_document(probe)
            temporary = json_path.with_suffix(json_path.suffix + ".tmp")
            temporary.write_text(
                json.dumps(document, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            temporary.replace(json_path)
            logger.info("【ffprobe媒体信息持久化】已保存 MediaInfo JSON：%s", json_path)
        except OSError as error:
            logger.warning("【ffprobe媒体信息持久化】写入 JSON 失败 path=%s error=%s", json_path, error)

    @staticmethod
    def _integer(value: Any, default: Optional[int] = None) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @classmethod
    def _number(cls, value: Any, default: float = 0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @classmethod
    def _frame_rate(cls, value: Any) -> Optional[float]:
        if not value or value == "0/0":
            return None
        try:
            numerator, denominator = str(value).split("/", 1)
            return float(numerator) / float(denominator)
        except (TypeError, ValueError, ZeroDivisionError):
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

    @classmethod
    def _bit_depth(cls, stream: Dict[str, Any]) -> Optional[int]:
        depth = cls._integer(stream.get("bits_per_raw_sample"))
        if depth:
            return depth
        match = re_search(r"(?:p|le|be)(10|12|14|16)(?:le|be)?$", str(stream.get("pix_fmt") or ""))
        return int(match.group(1)) if match else (8 if stream.get("pix_fmt") else None)

    @staticmethod
    def _container(format_name: Any) -> Optional[str]:
        names = str(format_name or "").split(",")
        for name in names:
            normalized = name.strip().lower()
            if normalized == "matroska":
                return "mkv"
            if normalized in {"mov", "mp4", "m4a", "3gp", "3g2", "mj2"}:
                return "mp4"
            if normalized:
                return normalized
        return None

    @classmethod
    def _video_range(cls, stream: Dict[str, Any]) -> str:
        tag = str(stream.get("codec_tag_string") or "").lower()
        sides = stream.get("side_data_list") or []
        if tag in _DOVI_TAGS or any("dovi" in str(item.get("side_data_type") or "").lower() for item in sides if isinstance(item, dict)):
            return "DolbyVision"
        transfer = str(stream.get("color_transfer") or "").lower()
        return "HDR" if transfer in {"smpte2084", "arib-std-b67"} else "SDR"

    @classmethod
    def _stream(cls, stream: Dict[str, Any], fallback_index: int) -> Optional[Dict[str, Any]]:
        stream_type = str(stream.get("codec_type") or "").lower()
        emby_type = {"video": "Video", "audio": "Audio", "subtitle": "Subtitle"}.get(stream_type)
        if emby_type is None:
            return None
        tags = stream.get("tags") or {}
        disposition = stream.get("disposition") or {}
        result: Dict[str, Any] = {
            "Codec": stream.get("codec_name"),
            "CodecTag": stream.get("codec_tag_string"),
            "Language": tags.get("language") or "und",
            "TimeBase": stream.get("time_base"),
            "Title": tags.get("title"),
            "IsInterlaced": str(stream.get("field_order") or "").lower() not in ("", "progressive", "unknown"),
            "ChannelLayout": stream.get("channel_layout"),
            "BitRate": cls._integer(stream.get("bit_rate")),
            "BitDepth": cls._bit_depth(stream),
            "Channels": cls._integer(stream.get("channels")),
            "SampleRate": cls._integer(stream.get("sample_rate")),
            "IsDefault": bool(disposition.get("default")),
            "IsForced": bool(disposition.get("forced")),
            "IsHearingImpaired": bool(disposition.get("hearing_impaired")),
            "Type": emby_type,
            "Index": cls._integer(stream.get("index"), fallback_index),
            "IsExternal": False,
            "IsTextSubtitleStream": False,
            "SupportsExternalStream": False,
            "Protocol": "File",
            "ExtendedVideoType": "None",
            "ExtendedVideoSubType": "None",
            "ExtendedVideoSubTypeDescription": "None",
            "AttachmentSize": 0,
        }
        if emby_type == "Video":
            result.update({
                "ColorTransfer": stream.get("color_transfer"),
                "ColorPrimaries": stream.get("color_primaries"),
                "ColorSpace": stream.get("color_space"),
                "VideoRange": cls._video_range(stream),
                "Height": cls._integer(stream.get("height")),
                "Width": cls._integer(stream.get("width")),
                "AverageFrameRate": cls._frame_rate(stream.get("avg_frame_rate")),
                "RealFrameRate": cls._frame_rate(stream.get("r_frame_rate")),
                "Profile": stream.get("profile"),
                "AspectRatio": stream.get("display_aspect_ratio"),
                "PixelFormat": stream.get("pix_fmt"),
                "Level": cls._integer(stream.get("level")),
                "IsAnamorphic": False,
            })
            if result["VideoRange"] == "DolbyVision":
                result["ExtendedVideoType"] = "DolbyVision"
        elif emby_type == "Subtitle":
            result["SubtitleLocationType"] = "InternalStream"
        return {key: value for key, value in result.items() if value is not None}

    @classmethod
    def _to_emby_document(cls, probe: Dict[str, Any]) -> List[Dict[str, Any]]:
        """生成与神医助手 ``-mediainfo.json`` 相同顶层合同的 JSON。"""
        format_info = probe.get("format") if isinstance(probe.get("format"), dict) else {}
        streams = probe.get("streams") if isinstance(probe.get("streams"), list) else []
        media_streams = [
            converted
            for index, item in enumerate(streams)
            if isinstance(item, dict)
            for converted in [cls._stream(item, index)]
            if converted is not None
        ]
        source = {
            "Chapters": [],
            "Protocol": "File",
            "Type": "Default",
            "Container": cls._container(format_info.get("format_name")),
            "Size": cls._integer(format_info.get("size"), 0),
            "IsRemote": True,
            "HasMixedProtocols": False,
            "RunTimeTicks": int(cls._number(format_info.get("duration")) * 10_000_000),
            "SupportsTranscoding": True,
            "SupportsDirectStream": True,
            "SupportsDirectPlay": True,
            "IsInfiniteStream": False,
            "RequiresOpening": False,
            "RequiresClosing": False,
            "RequiresLooping": False,
            "SupportsProbing": True,
            "MediaStreams": media_streams,
            "Formats": [],
            "Bitrate": cls._integer(format_info.get("bit_rate"), 0),
            "RequiredHttpHeaders": {},
            "AddApiKeyToDirectStreamUrl": False,
            "ReadAtNativeFramerate": False,
        }
        return [{"MediaSourceInfo": source, "Chapters": [], "ZeroFingerprintConfidence": False}]


# 原插件已使用 re.search；为保持依赖最小，这里保留模块级别别名。
re_search = re.search
