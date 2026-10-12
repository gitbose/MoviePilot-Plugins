"""复用 ffprobe命名补充缓存的 Emby MediaInfo JSON 持久化插件。

本插件优先消费 FFprobeNamingSupplement 在整理命名阶段已经缓存的原始
ffprobe JSON；仅缓存缺失且配置允许时，才对最终目标文件后台兜底提取。
"""

from __future__ import annotations

import json
import re
import sys
import time
from heapq import heappop, heappush
from hashlib import sha1, sha256
from concurrent.futures import Future, ThreadPoolExecutor
from subprocess import TimeoutExpired, run
from threading import Condition, Lock, Thread
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type
from urllib.parse import unquote

from app.core.config import settings
from app.core.event import Event, eventmanager
from app.chain.transfer import TransferChain
from app.db.transferhistory_oper import TransferHistoryOper
from app.log import logger
from app.plugins import _PluginBase
from app import schemas
from app.schemas import FileItem, TransferRenameBuildEventData
from app.schemas.types import ChainEventType, EventType, MediaType


_MEDIA_INFO_SUFFIX = "-mediainfo.json"
_MIN_RELIABLE_MEDIA_SIZE_BYTES = 1024 * 1024
_DOVI_TAGS = frozenset({"dvh1", "dvhe", "dva1", "dvav"})
_DEFAULT_FALLBACK_FFPROBE_TIMEOUT_SEC = 10
_MAX_FALLBACK_FFPROBE_TIMEOUT_SEC = 300
_SOURCE_JSON_CLEANUP_DELAY_SEC = 10
_PENDING_PROBE_RETENTION_SEC = 43200
# 正常记录会在完成事件中立即取走；仅对没有完成事件的异常记录定期回收即可。
_PENDING_PROBE_PRUNE_INTERVAL_SEC = 300
_FAILED_EXTRACTIONS_DATA_KEY = "failed_extractions"
_FAILED_EXTRACTIONS_VIEW_KEY = "failed_extractions_view"
_MAX_FAILED_EXTRACTION_RECORDS = 2000
_FAILED_EXTRACTION_PAGE_SIZES = (50, 100, 300, 500, 1000)
_DEFAULT_FAILED_EXTRACTION_PAGE_SIZE = 50
_CACHE_WRITE_WORKERS = 32
_HDR_FRAME_PACKET_LIMIT = 60
_HDR_FRAME_MAX_WORKERS = 3
_RECORD_CATEGORY_FAILURE = "failure"
_RECORD_CATEGORY_ABNORMAL_SIZE = "abnormal_size"
_RECORD_CATEGORY_ABNORMAL_SIZE_PENDING = "abnormal_size_pending"
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
    plugin_version = "1.1"
    
    plugin_author = "gitbose"
    author_url = "https://github.com/gitbose"
    plugin_config_prefix = "ffprobemediainfopersistence_"
    # 命名补充为 50；保持更后顺序，使它先写入自己的 ffprobe 缓存。
    plugin_order = 51
    auth_level = 1

    _FILTER_MATCH_ALL = "all"
    _FILTER_MATCH_ANY = "any"
    _FALLBACK_WORKERS = 3

    def __init__(self) -> None:
        super().__init__()
        self._enabled = False
        self._overwrite_json = False
        self._allow_abnormal_size_json = True
        self._fallback_probe = True
        self._fallback_workers = type(self)._FALLBACK_WORKERS
        self._fallback_timeout = _DEFAULT_FALLBACK_FFPROBE_TIMEOUT_SEC
        self._hdr_frame_probe = False
        self._hdr_executor: Optional[ThreadPoolExecutor] = None
        self._hdr_generation = 0
        self._hdr_paths: set[Tuple[int, str]] = set()
        self._hdr_paths_lock = Lock()
        self._cleanup_moved_source_json = True
        self._transfer_methods: List[str] = []
        self._destination_roots: List[str] = []
        self._filter_match_mode = type(self)._FILTER_MATCH_ALL
        self._fallback_executor: Optional[ThreadPoolExecutor] = None
        self._fallback_tasks: set[Future] = set()
        self._fallback_tasks_lock = Lock()
        # 上游缓存命中后的 JSON 写入与主动 ffprobe 分开限流，避免大批量整理创建海量线程。
        self._cached_write_executor: Optional[ThreadPoolExecutor] = None
        self._cached_write_tasks: set[Future] = set()
        self._cached_write_tasks_lock = Lock()
        # 单一延迟调度线程代替“每个文件一个 Timer”，大批量整理时不会积累大量休眠线程。
        self._cleanup_condition = Condition()
        self._cleanup_queue: List[Tuple[float, int, str]] = []
        self._cleanup_sequence = 0
        self._cleanup_generation = 0
        self._cleanup_worker: Optional[Thread] = None
        self._cleanup_worker_generation = -1
        self._writing_json_paths: set[str] = set()
        self._writing_json_paths_lock = Lock()
        # 跨 TransferRenameBuild / TransferComplete 的临时交接表。
        # 不设容量淘汰；成功事件一到即取出并删除，避免大批量任务误入主动提取。
        self._pending_probes: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._pending_probes_lock = Lock()
        self._pending_probe_last_prune = 0.0
        self._failed_extractions_lock = Lock()
        self._manual_retry_lock = Lock()
        self._manual_retry_running_ids: set[str] = set()
        # 由本插件发起的 MP 重新整理，命名阶段一旦拿到上游 ffprobe 结果即可确认成功。
        # 即使最终因同名文件不覆盖而整理失败，也应移除旧的“提取失败”记录。
        self._manual_reorganize_records: Dict[str, set[str]] = {}
        self._manual_reorganize_confirmed_ids: set[str] = set()
        self._manual_retry_progress: Dict[str, Any] = {
            "running": False,
            "total": 0,
            "started": 0,
            "completed": 0,
            "success": 0,
            "failed": 0,
        }

    def init_plugin(self, config: dict = None) -> None:
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._overwrite_json = bool(config.get("overwrite_json"))
        self._allow_abnormal_size_json = bool(config.get("allow_abnormal_size_json", True))
        self._fallback_probe = bool(config.get("fallback_probe", True))
        self._hdr_frame_probe = bool(config.get("hdr_frame_probe", False))
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
            self._clear_pending_probes()
            self._stop_background_tasks()
            logger.info("【ffprobe媒体信息持久化】插件未启用，不监听整理事件")
        elif workers_changed and self._fallback_executor is not None:
            self._stop_background_tasks()
        if self._enabled and self._fallback_executor is None:
            self._fallback_executor = ThreadPoolExecutor(
                max_workers=self._fallback_workers,
                thread_name_prefix="ffprobe-media-info",
            )
        if self._enabled and self._cached_write_executor is None:
            self._cached_write_executor = ThreadPoolExecutor(
                max_workers=_CACHE_WRITE_WORKERS,
                thread_name_prefix="ffprobe-media-info-write",
            )
        if self._enabled and self._hdr_frame_probe and self._hdr_executor is None:
            self._hdr_executor = ThreadPoolExecutor(
                max_workers=min(self._fallback_workers, _HDR_FRAME_MAX_WORKERS),
                thread_name_prefix="ffprobe-media-info-hdr",
            )
        elif not self._hdr_frame_probe:
            self._stop_hdr_tasks()
        if self._enabled:
            logger.info(
                "【ffprobe媒体信息持久化】插件已启用：主动提取=%s，并发=%s，超时=%s 秒，清理孤立 JSON=%s，HDR 增强=%s",
                self._fallback_probe,
                self._fallback_workers,
                self._fallback_timeout,
                self._cleanup_moved_source_json,
                self._hdr_frame_probe,
            )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/failed-extractions/list",
                "endpoint": self.get_failed_extractions_page,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "分页读取失败提取记录",
            },
            {
                "path": "/failed-extractions/ids",
                "endpoint": self.get_failed_extraction_ids,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "读取当前筛选记录 ID",
            },
            {
                "path": "/failed-extractions/retry",
                "endpoint": self.retry_failed_extractions,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "后台重新提取指定记录",
            },
            {
                "path": "/failed-extractions/delete",
                "endpoint": self.delete_failed_extractions,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "删除指定插件记录",
            },
            {
                "path": "/failed-extractions/select",
                "endpoint": self.set_failed_extraction_selected,
                "methods": ["GET"],
                "summary": "选择或取消选择失败提取记录",
            },
            {
                "path": "/failed-extractions/select-scope",
                "endpoint": self.select_failed_extraction_scope,
                "methods": ["GET"],
                "summary": "选择当前页或当前筛选条件下的失败提取记录",
            },
            {
                "path": "/failed-extractions/view",
                "endpoint": self.set_failed_extractions_view,
                "methods": ["GET"],
                "summary": "切换失败提取记录筛选",
            },
            {
                "path": "/failed-extractions/retry-selected",
                "endpoint": self.retry_selected_failed_extractions,
                "methods": ["GET"],
                "summary": "后台重新提取已选择记录",
            },
            {
                "path": "/failed-extractions/delete-selected",
                "endpoint": self.delete_selected_failed_extractions,
                "methods": ["GET"],
                "summary": "删除已选择失败提取记录",
            },
            {
                "path": "/failed-extractions/clear-selection",
                "endpoint": self.clear_failed_extractions_selection,
                "methods": ["GET"],
                "summary": "清除失败提取记录选择",
            },
            {
                "path": "/failed-extractions/refresh-progress",
                "endpoint": self.refresh_failed_extractions_progress,
                "methods": ["GET"],
                "summary": "刷新失败提取任务进度",
            },
        ]

    @staticmethod
    def get_render_mode() -> Tuple[str, str]:
        """记录页使用 Vue 本地状态，避免逐条勾选触发宿主页面刷新。"""
        return "vue", "dist/assets"

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """按整理流程排序的五行紧凑配置页。"""
        return [{"component": "VForm", "props": {"class": "ffprobe-media-info-config"}, "content": [
            {
                "component": "style",
                "text": ".ffprobe-media-info-config .v-messages__message { line-height: 1rem !important; }",
            },
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
            {
                "component": "div",
                "props": {"style": "margin-top: -10px;"},
                "content": [
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                    {"component": "VSwitch", "props": {
                        "model": "fallback_probe", "label": "上游缓存缺失时主动提取",
                        "hint": "仅缓存未命中时，对整理后的目标文件执行 ffprobe；任务在后台运行。",
                        "persistent-hint": True,
                    }}]},
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                    {"component": "VSwitch", "props": {
                        "model": "overwrite_json", "label": "覆盖同名 JSON",
                        "hint": "目标目录已有同名 JSON 时",
                        "persistent-hint": True,
                    }}]},
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                    {"component": "VSwitch", "props": {
                        "model": "allow_abnormal_size_json", "label": "生成异常大小 JSON",
                        "hint": "当ffprobe读取到的 Size < 1M 时，Size值写为0",
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
            {"component": "VSwitch", "props": {
                "model": "hdr_frame_probe", "label": "HDR 帧级增强识别",
                "hint": "额外限量读取视频帧，识别 HDR10+ 和色彩标记冲突；独立后台最多 3 并发，超时沿用主动提取设置，失败保留基础 JSON，默认关闭",
                "persistent-hint": True,
            }},
            {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "density": "compact", "text": "使用说明：优先复用“ffprobe命名补充”已获取的缓存，缓存命中后立即后台写入 JSON，最多 32 个并发。仅缓存缺失时才按“主动提取”配置对最终目标文件运行 ffprobe。上游 ffprobe 未请求章节，因此输出 JSON 的 Chapters 为空。HDR 帧级增强为可选的额外探测，关闭时不读取视频帧。"}},
            {"component": "VAlert", "props": {"type": "warning", "variant": "tonal", "density": "compact", "text": "JSON清理：文件整理完成后延迟 10 秒检查，若媒体文件已不存在，则清理媒体文件目录下严格同名的 -mediainfo.json 文件。"}},
                ],
            },
        ]}], {
            "enabled": False,
            "overwrite_json": False,
            "allow_abnormal_size_json": True,
            "fallback_probe": True,
            "hdr_frame_probe": False,
            "fallback_workers": 3,
            "fallback_timeout": _DEFAULT_FALLBACK_FFPROBE_TIMEOUT_SEC,
            "cleanup_moved_source_json": True,
            "filter_match_mode": "all",
            "transfer_methods": "",
            "destination_roots": "",
        }

    def get_page(self) -> Optional[List[dict]]:
        """失败提取记录页；所有点击操作经插件 API 完成后自动刷新本页。"""
        records = self._failed_extractions()
        view = self._failed_extractions_view()
        state = view["state"]
        reason = view["reason"]
        state_records = type(self)._records_for_state(records, state)
        filtered_records = type(self)._records_for_reason(state_records, reason)
        page_size = int(view["page_size"])
        total_records = len(filtered_records)
        page_count = max(1, (total_records + page_size - 1) // page_size)
        page = min(max(1, int(view["page"])), page_count)
        page_start = (page - 1) * page_size
        visible_records = filtered_records[page_start:page_start + page_size]
        selected_count = sum(1 for record in records if record.get("selected"))
        pending_count = sum(
            1 for record in records
            if record.get("category") == _RECORD_CATEGORY_FAILURE and not record.get("handled")
        )
        handled_count = sum(
            1 for record in records
            if record.get("category") == _RECORD_CATEGORY_FAILURE and record.get("handled")
        )
        abnormal_size_count = sum(
            1 for record in records
            if record.get("category") == _RECORD_CATEGORY_ABNORMAL_SIZE
        )
        abnormal_size_pending_count = sum(
            1 for record in records
            if record.get("category") == _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING
        )
        apikey = settings.API_TOKEN
        progress = self._manual_retry_progress_text()
        retry_running = bool(self._manual_retry_progress_snapshot().get("running"))

        def page_action(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            payload = {"apikey": apikey}
            if params:
                payload.update(params)
            return {"click": {"api": path, "method": "get", "params": payload}}

        status_buttons = []
        for key, label, count in (
            ("pending", "未处理", pending_count),
            ("handled", "已处理", handled_count),
            (_RECORD_CATEGORY_ABNORMAL_SIZE, "异常大小已生成", abnormal_size_count),
            (
                _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING,
                "异常大小未生成",
                abnormal_size_pending_count,
            ),
        ):
            status_buttons.append({
                "component": "VBtn",
                "props": {
                    "variant": "tonal" if key == state else "text",
                    "color": "primary" if key == state else "default",
                    "size": "small",
                },
                "text": f"{label}（{count}）",
                "events": page_action(
                    "plugin/FFprobeMediaInfoPersistence/failed-extractions/view",
                    {"state": key, "reason": "", "page": 1, "page_size": page_size},
                ),
            })

        reason_buttons = [{
            "component": "VBtn",
            "props": {
                "variant": "tonal" if not reason else "text",
                "color": "primary" if not reason else "default",
                "size": "x-small",
            },
            "text": "全部原因",
            "events": page_action(
                "plugin/FFprobeMediaInfoPersistence/failed-extractions/view",
                {"state": state, "reason": "", "page": 1, "page_size": page_size},
            ),
        }]
        for item_reason in sorted({type(self)._failure_reason_label(record.get("reason")) for record in state_records}):
            reason_buttons.append({
                "component": "VBtn",
                "props": {
                    "variant": "tonal" if item_reason == reason else "text",
                    "color": "primary" if item_reason == reason else "default",
                    "size": "x-small",
                },
                "text": item_reason,
                "events": page_action(
                    "plugin/FFprobeMediaInfoPersistence/failed-extractions/view",
                    {"state": state, "reason": item_reason, "page": 1, "page_size": page_size},
                ),
            })

        page_size_buttons: List[dict] = []
        for size in _FAILED_EXTRACTION_PAGE_SIZES:
            page_size_buttons.append({
                "component": "VBtn",
                "props": {
                    "variant": "tonal" if size == page_size else "text",
                    "color": "primary" if size == page_size else "default",
                    "size": "x-small",
                },
                "text": f"每页 {size} 条",
                "events": page_action(
                    "plugin/FFprobeMediaInfoPersistence/failed-extractions/view",
                    {"state": state, "reason": reason, "page": 1, "page_size": size},
                ),
            })

        page_numbers = {1, page_count}
        page_numbers.update(
            candidate for candidate in range(page - 2, page + 3)
            if 1 <= candidate <= page_count
        )
        pagination_buttons: List[dict] = [
            {
                "component": "VBtn",
                "props": {"size": "x-small", "variant": "text", "disabled": page <= 1},
                "text": "上一页",
                "events": page_action(
                    "plugin/FFprobeMediaInfoPersistence/failed-extractions/view",
                    {"state": state, "reason": reason, "page": page - 1, "page_size": page_size},
                ),
            }
        ]
        previous_page = 0
        for number in sorted(page_numbers):
            if previous_page and number - previous_page > 1:
                pagination_buttons.append({
                    "component": "span", "props": {"class": "text-caption"}, "text": "…"
                })
            pagination_buttons.append({
                "component": "VBtn",
                "props": {
                    "size": "x-small",
                    "variant": "tonal" if number == page else "text",
                    "color": "primary" if number == page else "default",
                },
                "text": str(number),
                "events": page_action(
                    "plugin/FFprobeMediaInfoPersistence/failed-extractions/view",
                    {"state": state, "reason": reason, "page": number, "page_size": page_size},
                ),
            })
            previous_page = number
        pagination_buttons.append({
            "component": "VBtn",
            "props": {"size": "x-small", "variant": "text", "disabled": page >= page_count},
            "text": "下一页",
            "events": page_action(
                "plugin/FFprobeMediaInfoPersistence/failed-extractions/view",
                {"state": state, "reason": reason, "page": page + 1, "page_size": page_size},
            ),
        })
        page_all_selected = bool(visible_records) and all(
            record.get("selected") for record in visible_records
        )

        table_rows: List[dict] = []
        for record in visible_records:
            record_id = str(record["id"])
            selected = bool(record.get("selected"))
            table_rows.append({
                "component": "tr",
                "content": [
                    {
                        "component": "td",
                        "content": [{
                            "component": "VCheckbox",
                            "props": {
                                "modelValue": selected,
                                "hideDetails": True,
                                "density": "compact",
                                "disabled": retry_running,
                            },
                            "events": page_action(
                                "plugin/FFprobeMediaInfoPersistence/failed-extractions/select",
                                {"record_id": record_id, "selected": "0" if selected else "1"},
                            ),
                        }],
                    },
                    {
                        "component": "td",
                        "text": str(record.get("first_failed_at") or "-"),
                    },
                    {
                        "component": "td",
                        "text": (
                            f"原始 Size: {record.get('raw_size')}（已按 0 写入 JSON）"
                            if record.get("category") == _RECORD_CATEGORY_ABNORMAL_SIZE
                            else (
                                f"原始 Size: {record.get('raw_size')}（未生成 JSON）"
                                if record.get("category") == _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING
                                else type(self)._failure_reason_label(record.get("reason"))
                            )
                        ),
                    },
                    {"component": "td", "text": str(record.get("attempt_count") or 0)},
                    {
                        "component": "td",
                        "props": {"class": "break-all text-caption"},
                        "text": str(record.get("destination") or "-"),
                    },
                ],
            })

        action_disabled = selected_count == 0 or retry_running
        content: List[dict] = [
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "density": "compact",
                    "text": progress,
                },
            },
            {
                "component": "div",
                "props": {"class": "d-flex flex-wrap ga-2"},
                "content": status_buttons,
            },
            {"component": "div", "props": {"class": "d-flex flex-wrap ga-2 mt-3"}, "content": reason_buttons},
            {
                "component": "div",
                "props": {"class": "d-flex flex-wrap align-center ga-2 mt-3"},
                "content": [
                    {
                        "component": "span",
                        "props": {"class": "text-body-2"},
                        "text": f"共 {total_records} 条，当前第 {page} / {page_count} 页",
                    },
                    *page_size_buttons,
                    *pagination_buttons,
                ],
            },
            {
                "component": "div",
                "props": {"class": "d-flex flex-wrap align-center ga-2 my-4"},
                "content": [
                    {"component": "span", "props": {"class": "text-body-2"}, "text": f"已选 {selected_count} 项"},
                    {
                        "component": "VBtn",
                        "props": {"color": "primary", "size": "small", "disabled": action_disabled},
                        "text": "重新提取",
                        "events": page_action("plugin/FFprobeMediaInfoPersistence/failed-extractions/retry-selected"),
                    },
                    {
                        "component": "VBtn",
                        "props": {"color": "error", "variant": "tonal", "size": "small", "disabled": action_disabled},
                        "text": "删除记录",
                        "events": page_action("plugin/FFprobeMediaInfoPersistence/failed-extractions/delete-selected"),
                    },
                    {
                        "component": "VBtn",
                        "props": {"variant": "text", "size": "small", "disabled": action_disabled},
                        "text": "取消选择",
                        "events": page_action("plugin/FFprobeMediaInfoPersistence/failed-extractions/clear-selection"),
                    },
                    {
                        "component": "VBtn",
                        "props": {"variant": "text", "size": "small", "disabled": retry_running or not visible_records},
                        "text": "全选本页" if not page_all_selected else "取消本页全选",
                        "events": page_action(
                            "plugin/FFprobeMediaInfoPersistence/failed-extractions/select-scope",
                            {"scope": "page", "selected": "0" if page_all_selected else "1"},
                        ),
                    },
                    {
                        "component": "VBtn",
                        "props": {"variant": "text", "size": "small", "disabled": retry_running or not filtered_records},
                        "text": "全选当前筛选",
                        "events": page_action(
                            "plugin/FFprobeMediaInfoPersistence/failed-extractions/select-scope",
                            {"scope": "filtered", "selected": "1"},
                        ),
                    },
                    {
                        "component": "VBtn",
                        "props": {"variant": "text", "size": "small"},
                        "text": "刷新进度",
                        "events": page_action("plugin/FFprobeMediaInfoPersistence/failed-extractions/refresh-progress"),
                    },
                ],
            },
        ]
        if not table_rows:
            content.append({
                "component": "VAlert",
                "props": {
                    "type": "success",
                    "variant": "tonal",
                    "density": "compact",
                    "text": "当前筛选条件下没有失败提取记录",
                },
            })
        else:
            content.append({
                "component": "VTable",
                "props": {"density": "compact"},
                "content": [
                    {"component": "thead", "content": [{"component": "tr", "content": [
                        {
                            "component": "th",
                            "content": [{
                                "component": "VCheckbox",
                                "props": {
                                    "modelValue": page_all_selected,
                                    "hideDetails": True,
                                    "density": "compact",
                                    "disabled": retry_running,
                                },
                                "events": page_action(
                                    "plugin/FFprobeMediaInfoPersistence/failed-extractions/select-scope",
                                    {"scope": "page", "selected": "0" if page_all_selected else "1"},
                                ),
                            }],
                        },
                        {"component": "th", "text": "首次记录时间"},
                        {"component": "th", "text": "失败原因 / 大小信息"},
                        {"component": "th", "text": "尝试次数"},
                        {"component": "th", "text": "目标文件"},
                    ]}]},
                    {"component": "tbody", "content": table_rows},
                ],
            })
        content.append({
            "component": "VAlert",
            "props": {
                "type": "warning",
                "variant": "tonal",
                "density": "compact",
            },
            "content": [
                {
                    "component": "span",
                    "text": "删除记录仅移除本插件的失败提取记录，不删除媒体文件、MoviePilot 整理历史；异常大小栏是 ",
                },
                {"component": "strong", "text": "ffprobe "},
                {
                    "component": "span",
                    "text": "读取后，json信息的 Size < 1MB 的文件记录",
                },
            ],
        })
        return [{"component": "div", "props": {"class": "d-flex flex-column ga-3"}, "content": content}]

    def get_failed_extractions_page(
        self,
        state: str = "pending",
        reason: str = "",
        page: int = 1,
        page_size: int = _DEFAULT_FAILED_EXTRACTION_PAGE_SIZE,
    ) -> Dict[str, Any]:
        """仅返回当前页，供交互记录页按需加载，不持久化页面勾选状态。"""
        normalized_state = type(self)._normalize_failed_extraction_state(state)
        normalized_page_size = type(self)._normalize_failed_extraction_page_size(page_size)
        normalized_page = max(1, type(self)._integer(page, 1) or 1)
        with self._failed_extractions_lock:
            records = self._failed_extractions()
        state_records = type(self)._records_for_state(records, normalized_state)
        filtered_records = type(self)._records_newest_first(
            type(self)._records_for_reason(state_records, str(reason or ""))
        )
        total = len(filtered_records)
        page_count = max(1, (total + normalized_page_size - 1) // normalized_page_size)
        normalized_page = min(normalized_page, page_count)
        start = (normalized_page - 1) * normalized_page_size
        visible_records = filtered_records[start:start + normalized_page_size]
        return {
            "state": normalized_state,
            "reason": str(reason or ""),
            "page": normalized_page,
            "page_size": normalized_page_size,
            "page_sizes": list(_FAILED_EXTRACTION_PAGE_SIZES),
            "total": total,
            "page_count": page_count,
            "records": [type(self)._failed_extraction_page_record(record) for record in visible_records],
            "reasons": sorted({type(self)._failure_reason_label(record.get("reason")) for record in state_records}),
            "counts": type(self)._failed_extraction_counts(records),
            "progress": self._manual_retry_progress_snapshot(),
            "progress_text": self._manual_retry_progress_text(),
        }

    def get_failed_extraction_ids(
        self, state: str = "pending", reason: str = ""
    ) -> Dict[str, Any]:
        """只在用户点击“全选当前筛选”时读取 ID，避免每页响应重复传输全部 ID。"""
        normalized_state = type(self)._normalize_failed_extraction_state(state)
        with self._failed_extractions_lock:
            records = self._failed_extractions()
        filtered_records = type(self)._records_for_reason(
            type(self)._records_for_state(records, normalized_state), str(reason or "")
        )
        return {
            "ids": [
                str(record["id"])
                for record in type(self)._records_newest_first(filtered_records)
            ]
        }

    @staticmethod
    def _normalize_failed_extraction_state(state: Any) -> str:
        value = str(state or "pending")
        return value if value in {
            "pending",
            "handled",
            _RECORD_CATEGORY_ABNORMAL_SIZE,
            _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING,
        } else "pending"

    @staticmethod
    def _normalize_failed_extraction_page_size(value: Any) -> int:
        parsed = FFprobeMediaInfoPersistence._integer(
            value, _DEFAULT_FAILED_EXTRACTION_PAGE_SIZE
        ) or _DEFAULT_FAILED_EXTRACTION_PAGE_SIZE
        return parsed if parsed in _FAILED_EXTRACTION_PAGE_SIZES else _DEFAULT_FAILED_EXTRACTION_PAGE_SIZE

    @staticmethod
    def _failed_extraction_page_record(record: Dict[str, Any]) -> Dict[str, Any]:
        """API 记录不携带旧静态页的 selected 字段，选择只保存于浏览器。"""
        result = {
            key: record.get(key)
            for key in (
                "id", "destination", "first_failed_at", "reason", "attempt_count",
                "handled", "category", "raw_size",
            )
        }
        result["reason"] = FFprobeMediaInfoPersistence._failure_reason_label(record.get("reason"))
        return result

    @staticmethod
    def _failed_extraction_counts(records: List[Dict[str, Any]]) -> Dict[str, int]:
        return {
            "pending": sum(
                1 for record in records
                if record.get("category") == _RECORD_CATEGORY_FAILURE and not record.get("handled")
            ),
            "handled": sum(
                1 for record in records
                if record.get("category") == _RECORD_CATEGORY_FAILURE and record.get("handled")
            ),
            _RECORD_CATEGORY_ABNORMAL_SIZE: sum(
                1 for record in records
                if record.get("category") == _RECORD_CATEGORY_ABNORMAL_SIZE
            ),
            _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING: sum(
                1 for record in records
                if record.get("category") == _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING
            ),
        }

    @staticmethod
    def _payload_record_ids(payload: Any) -> set[str]:
        """限制批量操作的输入为非空字符串 ID，忽略畸形请求。"""
        if not isinstance(payload, dict):
            return set()
        raw_ids = payload.get("ids")
        if not isinstance(raw_ids, list):
            return set()
        return {str(item) for item in raw_ids if str(item).strip()}

    def retry_failed_extractions(self, payload: Optional[Dict[str, Any]] = None) -> Any:
        """Vue 页面传入本地选中的 ID；不会把每次勾选写入插件数据。"""
        record_ids = type(self)._payload_record_ids(payload)
        if not record_ids:
            return schemas.Response(success=False, message="请先选择至少一条失败记录")
        with self._failed_extractions_lock:
            selected_records = [
                dict(record) for record in self._failed_extractions()
                if str(record["id"]) in record_ids
            ]
        return self._submit_manual_retry_records(selected_records)

    def delete_failed_extractions(self, payload: Optional[Dict[str, Any]] = None) -> Any:
        """仅删除指定插件记录；不操作媒体文件、JSON 或整理历史。"""
        record_ids = type(self)._payload_record_ids(payload)
        if not record_ids:
            return schemas.Response(success=False, message="请先选择至少一条记录")
        if self._manual_retry_progress_snapshot().get("running"):
            return schemas.Response(success=False, message="重新提取进行中，请完成后再删除记录")
        with self._failed_extractions_lock:
            records = self._failed_extractions()
            kept_records = [record for record in records if str(record["id"]) not in record_ids]
            deleted = len(records) - len(kept_records)
            if deleted:
                self._save_failed_extractions(kept_records)
        return schemas.Response(success=True, message=f"已删除 {deleted} 条插件失败记录")

    @staticmethod
    def _failure_record_id(destination: str) -> str:
        return sha1(str(Path(destination)).casefold().encode("utf-8")).hexdigest()

    @staticmethod
    def _abnormal_size_record_id(destination: str) -> str:
        return "abnormal_size:" + sha1(
            str(Path(destination)).casefold().encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _records_for_state(records: List[Dict[str, Any]], state: str) -> List[Dict[str, Any]]:
        if state in {
            _RECORD_CATEGORY_ABNORMAL_SIZE,
            _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING,
        }:
            return [record for record in records if record.get("category") == state]
        return [
            record for record in records
            if record.get("category") == _RECORD_CATEGORY_FAILURE
            and bool(record.get("handled")) == (state == "handled")
        ]

    @staticmethod
    def _failure_reason_label(reason: Any) -> str:
        """合并 MP 带文件名的同类错误；原始记录保持不变，旧数据无需迁移。"""
        text = str(reason or "未知错误").strip() or "未知错误"
        label = "没有找到可整理的媒体文件"
        if text == label or text.endswith(" " + label):
            return label
        return text

    @staticmethod
    def _records_for_reason(
        records: List[Dict[str, Any]], reason: str
    ) -> List[Dict[str, Any]]:
        label = FFprobeMediaInfoPersistence._failure_reason_label(reason) if reason else ""
        return [
            record for record in records
            if not label or FFprobeMediaInfoPersistence._failure_reason_label(record.get("reason")) == label
        ]

    @staticmethod
    def _records_newest_first(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """记录页按首次记录时间倒序展示；不改变磁盘中的原始记录顺序。"""
        return sorted(
            records,
            key=lambda record: (
                str(record.get("first_failed_at") or ""), str(record.get("id") or ""),
            ),
            reverse=True,
        )

    def _failed_extractions(self) -> List[Dict[str, Any]]:
        """读取并规范化失败提取记录，兼容旧数据或局部损坏数据。"""
        raw_records = self.get_data(_FAILED_EXTRACTIONS_DATA_KEY) or []
        if not isinstance(raw_records, list):
            return []
        records: List[Dict[str, Any]] = []
        for raw in raw_records:
            if not isinstance(raw, dict) or not raw.get("destination"):
                continue
            destination = str(raw["destination"])
            records.append({
                "id": str(raw.get("id") or type(self)._failure_record_id(destination)),
                "destination": destination,
                "source_path": str(raw.get("source_path") or ""),
                "first_failed_at": str(raw.get("first_failed_at") or "-"),
                "reason": str(raw.get("reason") or "未知错误"),
                "attempt_count": max(0, type(self)._integer(raw.get("attempt_count"), 0) or 0),
                "handled": bool(raw.get("handled")),
                "selected": bool(raw.get("selected")),
                "category": (
                    raw.get("category")
                    if raw.get("category") in {
                        _RECORD_CATEGORY_ABNORMAL_SIZE,
                        _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING,
                    }
                    else _RECORD_CATEGORY_FAILURE
                ),
                "raw_size": type(self)._integer(raw.get("raw_size"), 0) or 0,
            })
        return records

    def _save_failed_extractions(self, records: List[Dict[str, Any]]) -> None:
        self.save_data(_FAILED_EXTRACTIONS_DATA_KEY, records[-_MAX_FAILED_EXTRACTION_RECORDS:])

    def _failed_extractions_view(self) -> Dict[str, Any]:
        raw_view = self.get_data(_FAILED_EXTRACTIONS_VIEW_KEY) or {}
        state = str(raw_view.get("state") if isinstance(raw_view, dict) else "pending")
        raw_page = type(self)._integer(
            raw_view.get("page") if isinstance(raw_view, dict) else 1, 1
        ) or 1
        raw_page_size = type(self)._integer(
            raw_view.get("page_size") if isinstance(raw_view, dict) else _DEFAULT_FAILED_EXTRACTION_PAGE_SIZE,
            _DEFAULT_FAILED_EXTRACTION_PAGE_SIZE,
        ) or _DEFAULT_FAILED_EXTRACTION_PAGE_SIZE
        return {
            "state": (
                state
                if state in {
                    "pending",
                    "handled",
                    _RECORD_CATEGORY_ABNORMAL_SIZE,
                    _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING,
                }
                else "pending"
            ),
            "reason": str(raw_view.get("reason") or "") if isinstance(raw_view, dict) else "",
            "page": max(1, raw_page),
            "page_size": (
                raw_page_size
                if raw_page_size in _FAILED_EXTRACTION_PAGE_SIZES
                else _DEFAULT_FAILED_EXTRACTION_PAGE_SIZE
            ),
        }

    def _save_failed_extractions_view(
        self, state: str, reason: str, page: int, page_size: int
    ) -> None:
        self.save_data(_FAILED_EXTRACTIONS_VIEW_KEY, {
            "state": state,
            "reason": reason,
            "page": max(1, page),
            "page_size": (
                page_size
                if page_size in _FAILED_EXTRACTION_PAGE_SIZES
                else _DEFAULT_FAILED_EXTRACTION_PAGE_SIZE
            ),
        })

    @staticmethod
    def _api_authorized(apikey: Optional[str]) -> bool:
        return bool(apikey) and apikey == settings.API_TOKEN

    def _api_denied(self) -> Any:
        return schemas.Response(success=False, message="API 密钥错误")

    def set_failed_extraction_selected(
        self, record_id: str, selected: str, apikey: Optional[str] = None
    ) -> Any:
        if not self._api_authorized(apikey):
            return self._api_denied()
        selected_value = str(selected).lower() in {"1", "true", "yes", "on"}
        with self._failed_extractions_lock:
            records = self._failed_extractions()
            found = False
            for record in records:
                if record["id"] == record_id:
                    record["selected"] = selected_value
                    found = True
                    break
            if found:
                self._save_failed_extractions(records)
        return schemas.Response(success=found, message="已更新选择" if found else "未找到失败记录")

    def set_failed_extractions_view(
        self,
        state: str = "pending",
        reason: str = "",
        page: int = 1,
        page_size: int = _DEFAULT_FAILED_EXTRACTION_PAGE_SIZE,
        apikey: Optional[str] = None,
    ) -> Any:
        if not self._api_authorized(apikey):
            return self._api_denied()
        normalized_state = (
            state if state in {
                "pending",
                "handled",
                _RECORD_CATEGORY_ABNORMAL_SIZE,
                _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING,
            }
            else "pending"
        )
        normalized_page = max(1, type(self)._integer(page, 1) or 1)
        normalized_page_size = type(self)._integer(
            page_size, _DEFAULT_FAILED_EXTRACTION_PAGE_SIZE
        ) or _DEFAULT_FAILED_EXTRACTION_PAGE_SIZE
        with self._failed_extractions_lock:
            # 翻页、改每页数量或切换筛选条件都不清空勾选，便于跨页批量处理。
            self._save_failed_extractions_view(
                normalized_state, reason, normalized_page, normalized_page_size
            )
        return schemas.Response(success=True, message="已切换筛选条件")

    def select_failed_extraction_scope(
        self,
        scope: str = "page",
        selected: str = "1",
        apikey: Optional[str] = None,
    ) -> Any:
        """批量选择当前页或当前筛选结果，避免逐条勾选造成重复刷新。"""
        if not self._api_authorized(apikey):
            return self._api_denied()
        selected_value = str(selected).lower() in {"1", "true", "yes", "on"}
        with self._failed_extractions_lock:
            records = self._failed_extractions()
            view = self._failed_extractions_view()
            filtered = type(self)._records_for_reason(
                type(self)._records_for_state(records, str(view["state"])),
                str(view["reason"]),
            )
            if scope == "page":
                page_size = int(view["page_size"])
                page_count = max(1, (len(filtered) + page_size - 1) // page_size)
                page = min(max(1, int(view["page"])), page_count)
                start = (page - 1) * page_size
                targets = filtered[start:start + page_size]
            elif scope == "filtered":
                targets = filtered
            else:
                return schemas.Response(success=False, message="未知选择范围")
            target_ids = {str(record["id"]) for record in targets}
            changed = 0
            for record in records:
                if str(record["id"]) in target_ids and bool(record.get("selected")) != selected_value:
                    record["selected"] = selected_value
                    changed += 1
            if changed:
                self._save_failed_extractions(records)
        return schemas.Response(
            success=True,
            message=f"已{'选择' if selected_value else '取消选择'} {len(target_ids)} 条记录",
        )

    def clear_failed_extractions_selection(self, apikey: Optional[str] = None) -> Any:
        if not self._api_authorized(apikey):
            return self._api_denied()
        with self._failed_extractions_lock:
            records = self._failed_extractions()
            for record in records:
                record["selected"] = False
            self._save_failed_extractions(records)
        return schemas.Response(success=True, message="已取消选择")

    def refresh_failed_extractions_progress(self, apikey: Optional[str] = None) -> Any:
        """数据页点击后由 MP 重新渲染页面，从而显示后台任务的最新进度。"""
        if not self._api_authorized(apikey):
            return self._api_denied()
        return schemas.Response(success=True, message="已刷新重新提取进度")

    def delete_selected_failed_extractions(self, apikey: Optional[str] = None) -> Any:
        """只删除插件自身的失败记录；不触碰媒体、JSON 或 MP 整理历史。"""
        if not self._api_authorized(apikey):
            return self._api_denied()
        if self._manual_retry_progress_snapshot().get("running"):
            return schemas.Response(success=False, message="重新提取进行中，请完成后再删除记录")
        with self._failed_extractions_lock:
            records = self._failed_extractions()
            kept_records = [record for record in records if not record.get("selected")]
            deleted = len(records) - len(kept_records)
            self._save_failed_extractions(kept_records)
        return schemas.Response(success=True, message=f"已删除 {deleted} 条插件失败记录")

    def _manual_retry_progress_snapshot(self) -> Dict[str, Any]:
        with self._manual_retry_lock:
            return dict(self._manual_retry_progress)

    def _manual_retry_progress_text(self) -> str:
        progress = self._manual_retry_progress_snapshot()
        if progress.get("running"):
            return (
                f"重新整理进行中：共 {progress['total']} 个文件，已开始 {progress['started']} 个，"
                f"已完成 {progress['completed']} 个，成功 {progress['success']} 个，失败 {progress['failed']} 个"
            )
        if progress.get("total"):
            return (
                f"上次重新整理：共 {progress['total']} 个文件，成功 {progress['success']} 个，"
                f"失败 {progress['failed']} 个"
            )
        return "选择记录后点击“重新整理”，任务将在后台运行，关闭此页面不影响执行"

    def retry_selected_failed_extractions(self, apikey: Optional[str] = None) -> Any:
        """将已选记录交给 MP 原生重新整理链路，接口立即返回、不阻塞页面。"""
        if not self._api_authorized(apikey):
            return self._api_denied()
        with self._failed_extractions_lock:
            selected_records = [
                dict(record) for record in self._failed_extractions() if record.get("selected")
            ]
        return self._submit_manual_retry_records(selected_records)

    def _submit_manual_retry_records(self, selected_records: List[Dict[str, Any]]) -> Any:
        """将已确认的记录交给 MP 原生重新整理；页面选择状态不参与后续执行。"""
        if not self._enabled:
            return schemas.Response(success=False, message="插件未启用，无法重新整理")
        executor = self._fallback_executor
        if executor is None:
            return schemas.Response(success=False, message="后台整理器未启动")
        if not selected_records:
            return schemas.Response(success=False, message="请先选择至少一条失败记录")
        with self._manual_retry_lock:
            if self._manual_retry_progress.get("running"):
                return schemas.Response(success=False, message="已有重新整理任务正在后台运行")
            selected_records = [
                record for record in selected_records
                if record["id"] not in self._manual_retry_running_ids
            ]
            if not selected_records:
                return schemas.Response(success=False, message="所选记录正在重新整理")
            self._manual_retry_running_ids.update(record["id"] for record in selected_records)
            self._manual_retry_progress = {
                "running": True,
                "total": len(selected_records),
                "started": 0,
                "completed": 0,
                "success": 0,
                "failed": 0,
            }
        for record in selected_records:
            future = executor.submit(self._manual_retry_one, record)
            with self._fallback_tasks_lock:
                self._fallback_tasks.add(future)
            future.add_done_callback(self._on_background_task_done)
        logger.info("【ffprobe媒体信息持久化】已提交 %s 个 MP 原生重新整理任务", len(selected_records))
        return schemas.Response(success=True, message=f"已在后台提交 {len(selected_records)} 个重新整理任务")

    def _manual_retry_one(self, record: Dict[str, Any]) -> None:
        """调用 MP 原生手动整理；仅 ffprobe 命名阶段成功后才清理对应插件记录。"""
        record_id = str(record["id"])
        with self._manual_retry_lock:
            self._manual_retry_progress["started"] += 1
        success = False
        failure_reason = "未知错误"
        source_path = ""
        try:
            history = type(self)._manual_reorganize_history(record)
            if history is None:
                failure_reason = "未找到对应的 MP 整理历史"
            else:
                fileitem = type(self)._manual_reorganize_fileitem(history)
                if fileitem is None or not fileitem.path:
                    failure_reason = "整理历史缺少可重新整理的源文件"
                else:
                    source_path = str(fileitem.path)
                    self._register_manual_reorganize(record_id, source_path)
                    state, message = TransferChain().manual_transfer(
                        fileitem=fileitem,
                        target_storage=None,
                        target_path=None,
                        tmdbid=type(self)._integer(getattr(history, "tmdbid", None), None),
                        doubanid=str(getattr(history, "doubanid", "") or "") or None,
                        bangumiid=type(self)._integer(getattr(history, "bangumiid", None), None),
                        anilistid=type(self)._integer(getattr(history, "anilistid", None), None),
                        media_source=str(getattr(history, "media_source", "") or "") or None,
                        media_id=str(getattr(history, "media_id", "") or "") or None,
                        mtype=type(self)._history_media_type(history),
                        season=type(self)._history_season(history),
                        episode_group=str(getattr(history, "episode_group", "") or "") or None,
                        transfer_type=None,
                        epformat=type(self)._history_episode_format(history),
                        min_filesize=0,
                        scrape=None,
                        library_type_folder=None,
                        library_category_folder=None,
                        force=True,
                        background=False,
                        downloader=getattr(history, "downloader", None),
                        download_hash=getattr(history, "download_hash", None),
                        preview=False,
                        reorganize=True,
                        sync_extra_files=False,
                    )
                    # TransferRenameBuild 在 MP 整理过程中同步触发。此标记为真说明上游
                    # ffprobe 已实际返回，哪怕最终因同名文件未覆盖而整理失败也可移除记录。
                    success = self._consume_manual_reorganize_confirmation(record_id)
                    if not success:
                        failure_reason = str(message or "未获取到上游 ffprobe 结果")
                    elif not state:
                        logger.info(
                            "【ffprobe媒体信息持久化】MP 重新整理未落盘但 ffprobe 已成功，已移除记录：%s",
                            source_path,
                        )
        except Exception as error:
            failure_reason = "MP 重新整理执行错误"
            logger.warning("【ffprobe媒体信息持久化】手动重新整理异常 path=%s error=%s", source_path, error)
        finally:
            if source_path:
                self._unregister_manual_reorganize(record_id, source_path)
        if record.get("category") in {
            _RECORD_CATEGORY_ABNORMAL_SIZE,
            _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING,
        }:
            # 若没有命名阶段 ffprobe 结果，异常大小记录保留并增加尝试次数。
            if not success:
                self._mark_abnormal_size_retry_failed(record_id)
        elif not success:
            self._mark_manual_retry_failed(record_id, failure_reason)
        with self._manual_retry_lock:
            self._manual_retry_running_ids.discard(record_id)
            self._manual_retry_progress["completed"] += 1
            self._manual_retry_progress["success" if success else "failed"] += 1
            if self._manual_retry_progress["completed"] >= self._manual_retry_progress["total"]:
                self._manual_retry_progress["running"] = False

    def _register_manual_reorganize(self, record_id: str, source_path: str) -> None:
        key = type(self)._cache_key(source_path)
        with self._manual_retry_lock:
            self._manual_reorganize_records.setdefault(key, set()).add(record_id)

    def _manual_reorganize_record_ids(self, source_path: str) -> set[str]:
        key = type(self)._cache_key(source_path)
        with self._manual_retry_lock:
            return set(self._manual_reorganize_records.get(key) or set())

    def _confirm_manual_reorganize_probe(self, source_path: str) -> set[str]:
        """命名阶段拿到上游结果时确认本插件发起的重整记录。"""
        key = type(self)._cache_key(source_path)
        with self._manual_retry_lock:
            record_ids = self._manual_reorganize_records.pop(key, set())
            self._manual_reorganize_confirmed_ids.update(record_ids)
        return record_ids

    def _consume_manual_reorganize_confirmation(self, record_id: str) -> bool:
        with self._manual_retry_lock:
            if record_id not in self._manual_reorganize_confirmed_ids:
                return False
            self._manual_reorganize_confirmed_ids.discard(record_id)
            return True

    def _unregister_manual_reorganize(self, record_id: str, source_path: str) -> None:
        key = type(self)._cache_key(source_path)
        with self._manual_retry_lock:
            record_ids = self._manual_reorganize_records.get(key)
            if record_ids:
                record_ids.discard(record_id)
                if not record_ids:
                    self._manual_reorganize_records.pop(key, None)
            self._manual_reorganize_confirmed_ids.discard(record_id)

    @classmethod
    def _manual_reorganize_history(cls, record: Dict[str, Any]) -> Any:
        """从插件记录的源或目标路径恢复对应的 MP 整理历史。"""
        source_path = str(record.get("source_path") or "").strip()
        destination = str(record.get("destination") or "").strip()
        try:
            history_oper = TransferHistoryOper()
            if source_path:
                history = history_oper.get_by_src(source_path)
                if history:
                    return history
            if destination:
                history = history_oper.get_by_dest(destination)
                if history:
                    return history
        except Exception as error:
            logger.warning(
                "【ffprobe媒体信息持久化】读取 MP 整理历史失败 source=%s destination=%s error=%s",
                source_path,
                destination,
                error,
            )
        return None

    @classmethod
    def _manual_reorganize_fileitem(cls, history: Any) -> Optional[FileItem]:
        """与 MP 手动整理页面保持一致：成功移动记录从目标重整，其余从源文件重整。"""
        mode = str(getattr(history, "mode", "") or "").lower()
        use_destination = bool(getattr(history, "status", False) and "move" in mode)
        raw_fileitem = getattr(
            history, "dest_fileitem" if use_destination else "src_fileitem", None
        )
        if not isinstance(raw_fileitem, dict):
            return None
        try:
            return FileItem(**raw_fileitem)
        except Exception as error:
            logger.warning("【ffprobe媒体信息持久化】读取整理历史文件项失败：%s", error)
            return None

    @staticmethod
    def _history_media_type(history: Any) -> Optional[MediaType]:
        value = getattr(history, "type", None)
        if isinstance(value, MediaType):
            return value
        try:
            return MediaType(str(value)) if value else None
        except ValueError:
            return None

    @classmethod
    def _history_season(cls, history: Any) -> Optional[int]:
        value = str(getattr(history, "seasons", "") or "")
        digits = "".join(character for character in value if character.isdigit())
        return cls._integer(digits, None) if digits else None

    @classmethod
    def _history_episode_format(cls, history: Any) -> Optional[Any]:
        episodes = str(getattr(history, "episodes", "") or "")
        if not episodes:
            return None
        try:
            if "-" not in episodes:
                detail = episodes.replace("E", "")
            else:
                start, end = episodes.split("-", 1)
                start_number = cls._integer(start.replace("E", ""), None)
                end_number = cls._integer(end.replace("E", ""), None)
                if start_number is None or end_number is None:
                    return None
                detail = ",".join(str(number) for number in range(start_number, end_number + 1))
            return schemas.EpisodeFormat(detail=detail)
        except (TypeError, ValueError):
            return None

    def _remove_plugin_record(self, record_id: str) -> None:
        """移除插件运行记录；不影响媒体文件、JSON 或 MP 整理历史。"""
        with self._failed_extractions_lock:
            records = self._failed_extractions()
            remaining = [record for record in records if str(record["id"]) != record_id]
            if len(remaining) != len(records):
                self._save_failed_extractions(remaining)

    def _record_failed_extraction(
        self, destination: Path, reason: str, source_path: Optional[str] = None
    ) -> None:
        """记录“上游缓存缺失且主动提取失败”的首次失败；保留首次失败时间。"""
        destination_text = str(destination)
        source_text = str(source_path or "").strip()
        record_id = type(self)._failure_record_id(destination_text)
        with self._failed_extractions_lock:
            records = self._failed_extractions()
            for record in records:
                if (
                    record["id"] == record_id
                    and record.get("category") == _RECORD_CATEGORY_FAILURE
                ):
                    record["reason"] = reason
                    if source_text:
                        record["source_path"] = source_text
                    self._save_failed_extractions(records)
                    return
            records.append({
                "id": record_id,
                "destination": destination_text,
                "source_path": source_text,
                "first_failed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "reason": reason,
                "attempt_count": 0,
                "handled": False,
                "selected": False,
                "category": _RECORD_CATEGORY_FAILURE,
                "raw_size": 0,
            })
            self._save_failed_extractions(records)

    def _mark_manual_retry_failed(self, record_id: str, reason: str) -> None:
        with self._failed_extractions_lock:
            records = self._failed_extractions()
            for record in records:
                if (
                    record["id"] == record_id
                    and record.get("category") == _RECORD_CATEGORY_FAILURE
                ):
                    record["handled"] = True
                    record["selected"] = False
                    record["reason"] = reason
                    record["attempt_count"] = int(record.get("attempt_count") or 0) + 1
                    break
            self._save_failed_extractions(records)

    def _remove_failed_extraction(self, record_id: str) -> None:
        with self._failed_extractions_lock:
            records = self._failed_extractions()
            remaining = [
                record for record in records
                if not (
                    record["id"] == record_id
                    and record.get("category") == _RECORD_CATEGORY_FAILURE
                )
            ]
            if len(remaining) != len(records):
                self._save_failed_extractions(remaining)

    def _clear_succeeded_failure(
        self, destination: Path, source_path: Optional[str] = None
    ) -> None:
        """JSON 成功后清理同目标或同源文件的普通失败记录。"""
        destination_id = type(self)._failure_record_id(str(destination))
        source_key = type(self)._cache_key(source_path) if source_path else ""
        with self._failed_extractions_lock:
            records = self._failed_extractions()
            remaining = []
            for record in records:
                is_failure = record.get("category") == _RECORD_CATEGORY_FAILURE
                same_destination = str(record.get("id")) == destination_id
                saved_source = str(record.get("source_path") or "")
                same_source = bool(source_key and saved_source) and (
                    type(self)._cache_key(saved_source) == source_key
                )
                if is_failure and (same_destination or same_source):
                    continue
                remaining.append(record)
            if len(remaining) != len(records):
                self._save_failed_extractions(remaining)

    def _record_abnormal_size(
        self, destination: Path, raw_size: int, generated: bool
    ) -> None:
        """记录远程 Size 不可信的小值；生成与未生成分别显示，不影响失败提取分类。"""
        record_id = type(self)._abnormal_size_record_id(str(destination))
        category = (
            _RECORD_CATEGORY_ABNORMAL_SIZE
            if generated
            else _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING
        )
        with self._failed_extractions_lock:
            records = self._failed_extractions()
            for record in records:
                if record["id"] == record_id:
                    record["raw_size"] = raw_size
                    record["selected"] = False
                    record["category"] = category
                    self._save_failed_extractions(records)
                    return
            records.append({
                "id": record_id,
                "destination": str(destination),
                "first_failed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "reason": "异常大小",
                "attempt_count": 0,
                "handled": False,
                "selected": False,
                "category": category,
                "raw_size": raw_size,
            })
            self._save_failed_extractions(records)

    def _remove_abnormal_size(self, destination: Path) -> None:
        record_id = type(self)._abnormal_size_record_id(str(destination))
        with self._failed_extractions_lock:
            records = self._failed_extractions()
            remaining = [
                record for record in records
                if not (
                    record["id"] == record_id
                    and record.get("category") in {
                        _RECORD_CATEGORY_ABNORMAL_SIZE,
                        _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING,
                    }
                )
            ]
            # 正常媒体通常没有异常记录；未移除任何项时不做无意义的数据持久化。
            if len(remaining) != len(records):
                self._save_failed_extractions(remaining)

    def _mark_abnormal_size_retry_failed(self, record_id: str) -> None:
        """重试未完成时保留异常大小记录，但不将它混入“已处理”失败列表。"""
        with self._failed_extractions_lock:
            records = self._failed_extractions()
            for record in records:
                if (
                    record["id"] == record_id
                    and record.get("category") in {
                        _RECORD_CATEGORY_ABNORMAL_SIZE,
                        _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING,
                    }
                ):
                    record["selected"] = False
                    record["attempt_count"] = int(record.get("attempt_count") or 0) + 1
                    break
            self._save_failed_extractions(records)

    def stop_service(self) -> None:
        self._enabled = False
        self._clear_pending_probes()
        self._stop_background_tasks()

    def _stop_background_tasks(self) -> None:
        """停止尚未开始的兜底任务；已运行的 ffprobe 仍受配置超时保护。"""
        executor = self._fallback_executor
        self._fallback_executor = None
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        cached_write_executor = self._cached_write_executor
        self._cached_write_executor = None
        if cached_write_executor is not None:
            cached_write_executor.shutdown(wait=False, cancel_futures=True)
        self._stop_hdr_tasks()
        with self._fallback_tasks_lock:
            self._fallback_tasks.clear()
        with self._cached_write_tasks_lock:
            self._cached_write_tasks.clear()
        with self._cleanup_condition:
            self._cleanup_generation += 1
            self._cleanup_queue.clear()
            self._cleanup_condition.notify_all()

    def _stop_hdr_tasks(self) -> None:
        with self._hdr_paths_lock:
            self._hdr_generation += 1
            self._hdr_paths.clear()
            executor = self._hdr_executor
            self._hdr_executor = None
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)

    def _submit_hdr_enhancement(
        self, destination: Path, probe: Dict[str, Any], json_path: Path, base_digest: str
    ) -> None:
        """基础 JSON 已落盘后再探测，不占用 32 线程写入池或 MP 整理回调。"""
        if not self._enabled or not self._hdr_frame_probe or not type(self)._needs_hdr_frames(probe):
            return
        try:
            stat = destination.stat()
            source_identity = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
        except OSError:
            return
        with self._hdr_paths_lock:
            executor = self._hdr_executor
            key = (self._hdr_generation, str(json_path))
            if executor is None or key in self._hdr_paths:
                return
            self._hdr_paths.add(key)
        try:
            future = executor.submit(
                self._background_hdr_enhancement,
                destination, probe, json_path, base_digest, key, self._fallback_timeout, source_identity,
            )
        except RuntimeError:
            with self._hdr_paths_lock:
                self._hdr_paths.discard(key)
            return
        future.add_done_callback(lambda done: self._hdr_task_done(done, key))

    def _hdr_task_done(self, future: Future, key: Tuple[int, str]) -> None:
        with self._hdr_paths_lock:
            self._hdr_paths.discard(key)
        if future.cancelled():
            return
        try:
            future.result()
        except Exception as error:
            logger.warning("【ffprobe媒体信息持久化】HDR 增强任务异常，保留基础 JSON：%s", error)

    def _hdr_task_active(self, key: Tuple[int, str]) -> bool:
        return self._enabled and self._hdr_frame_probe and key[0] == self._hdr_generation

    def _background_hdr_enhancement(
        self, destination: Path, probe: Dict[str, Any], json_path: Path,
        base_digest: str, key: Tuple[int, str], timeout: int, source_identity: Tuple[int, int, int, int],
    ) -> None:
        if not self._hdr_task_active(key):
            return
        try:
            before_stat = destination.stat()
            if (before_stat.st_dev, before_stat.st_ino, before_stat.st_size, before_stat.st_mtime_ns) != source_identity:
                return
            if sha256(json_path.read_bytes()).hexdigest() != base_digest:
                return
        except OSError:
            return
        frames, reason = type(self)._run_hdr_frame_probe(str(destination), timeout)
        if not self._hdr_task_active(key):
            return
        if frames is None:
            logger.warning("【ffprobe媒体信息持久化】HDR 增强%s，保留基础 JSON：%s", reason, destination)
            return
        enhanced = type(self)._merge_hdr_frames(probe, frames)
        document = type(self)._to_emby_document(enhanced)
        if document == type(self)._to_emby_document(probe):
            return
        path_key = str(json_path).casefold()
        with self._writing_json_paths_lock:
            if path_key in self._writing_json_paths:
                return
            self._writing_json_paths.add(path_key)
        temporary = json_path.with_suffix(json_path.suffix + ".hdr.tmp")
        try:
            after_stat = destination.stat()
            stat_keys = ("st_dev", "st_ino", "st_size", "st_mtime_ns")
            if any(getattr(before_stat, name) != getattr(after_stat, name) for name in stat_keys):
                return
            # 排队/探测期间可能已重新整理或被其他插件改写，不覆盖变化后的 JSON。
            if sha256(json_path.read_bytes()).hexdigest() != base_digest:
                return
            temporary.write_text(json.dumps(document, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
            # 和停止/重载共享此锁，旧任务不能在新一轮启用后落盘。
            with self._hdr_paths_lock:
                if not self._hdr_task_active(key):
                    return
                temporary.replace(json_path)
            logger.info("【ffprobe媒体信息持久化】已更新 HDR 帧级信息：%s", json_path)
        except OSError as error:
            logger.warning("【ffprobe媒体信息持久化】HDR 增强写入失败，保留基础 JSON：%s", error)
        finally:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            with self._writing_json_paths_lock:
                self._writing_json_paths.discard(path_key)

    def _submit_cached_persist(
        self,
        destination: Path,
        probe: Dict[str, Any],
        source_path: Optional[str] = None,
        transfer_history_id: Optional[int] = None,
    ) -> None:
        """命中缓存后立即提交独立写入池，不占用主动 ffprobe 并发。"""
        executor = self._cached_write_executor
        if executor is None:
            logger.warning("【ffprobe媒体信息持久化】JSON 写入器未启动，跳过：%s", destination)
            return
        try:
            future = executor.submit(
                self._persist,
                destination,
                probe,
                "复用上游缓存",
                record_source_path=source_path,
                transfer_history_id=transfer_history_id,
            )
        except RuntimeError:
            # 插件停用或重载时执行器可能刚好关闭；不让事件回调因此报错。
            logger.debug("【ffprobe媒体信息持久化】JSON 写入器已停止，跳过：%s", destination)
            return
        with self._cached_write_tasks_lock:
            self._cached_write_tasks.add(future)
        future.add_done_callback(self._on_cached_write_done)

    def _on_cached_write_done(self, future: Future) -> None:
        with self._cached_write_tasks_lock:
            self._cached_write_tasks.discard(future)
        try:
            future.result()
        except Exception as error:
            logger.warning("【ffprobe媒体信息持久化】后台 JSON 写入任务异常：%s", error)

    def _submit_fallback_task(
        self,
        destination: Path,
        source_path: Optional[str] = None,
        transfer_history_id: Optional[int] = None,
    ) -> None:
        """提交后台兜底探测，不阻塞 MP 当前整理事件。"""
        executor = self._fallback_executor
        if executor is None:
            logger.warning("【ffprobe媒体信息持久化】后台提取器未启动，跳过：%s", destination)
            return
        try:
            future = executor.submit(
                self._background_probe_and_persist,
                destination,
                source_path,
                transfer_history_id,
            )
        except RuntimeError:
            logger.debug("【ffprobe媒体信息持久化】后台提取器已停止，跳过：%s", destination)
            return
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

    def _background_probe_and_persist(
        self,
        destination: Path,
        source_path: Optional[str] = None,
        transfer_history_id: Optional[int] = None,
    ) -> None:
        """对已整理到位的目标文件执行一次 10 秒兜底探测并写入 JSON。"""
        destination = type(self)._resolve_existing_destination(
            destination, transfer_history_id
        )
        if not destination.is_file():
            logger.warning("【ffprobe媒体信息持久化】后台提取时目标不存在，跳过：%s", destination)
            self._record_failed_extraction(destination, "整理目标不存在", source_path)
            return
        probe, failure_reason = type(self)._run_fallback_ffprobe_detail(
            str(destination), self._fallback_timeout
        )
        if not isinstance(probe, dict):
            logger.warning("【ffprobe媒体信息持久化】后台 ffprobe 未得到结果，跳过：%s", destination)
            self._record_failed_extraction(destination, failure_reason, source_path)
            return
        if not self._persist(
            destination,
            probe,
            "主动提取",
            record_source_path=source_path,
            transfer_history_id=transfer_history_id,
        ):
            self._record_failed_extraction(destination, "JSON 写入失败", source_path)

    def _clear_pending_probes(self) -> None:
        with self._pending_probes_lock:
            self._pending_probes.clear()
            self._pending_probe_last_prune = 0.0
        with self._manual_retry_lock:
            self._manual_reorganize_records.clear()
            self._manual_reorganize_confirmed_ids.clear()

    def _prune_pending_probes_locked(self, now: float) -> None:
        """清理未收到完成事件的异常旧记录，防止长期运行时无界积累。"""
        if now - self._pending_probe_last_prune < _PENDING_PROBE_PRUNE_INTERVAL_SEC:
            return
        expired_keys = [
            key
            for key, (created_at, _) in self._pending_probes.items()
            if now - created_at > _PENDING_PROBE_RETENTION_SEC
        ]
        for key in expired_keys:
            self._pending_probes.pop(key, None)
        self._pending_probe_last_prune = now

    def _stash_pending_probe(
        self, source_path: str, probe: Dict[str, Any], transfer_method: str
    ) -> None:
        now = time.monotonic()
        key = type(self)._cache_key(source_path)
        with self._pending_probes_lock:
            self._prune_pending_probes_locked(now)
            self._pending_probes[key] = (
                now,
                {"probe": probe, "transfer_method": transfer_method},
            )

    def _take_pending_probe(self, source_path: str) -> Optional[Dict[str, Any]]:
        key = type(self)._cache_key(source_path)
        now = time.monotonic()
        with self._pending_probes_lock:
            self._prune_pending_probes_locked(now)
            item = self._pending_probes.pop(key, None)
        if item is None:
            return None
        # 若完成事件异常延迟超过保留时间，不再使用过期的媒体信息。
        if now - item[0] > _PENDING_PROBE_RETENTION_SEC:
            return None
        return item[1]

    def _discard_pending_probe(self, source_path: str) -> None:
        """该条已成功整理但不符合筛选条件，不必保留其命名阶段暂存。"""
        key = type(self)._cache_key(source_path)
        with self._pending_probes_lock:
            self._pending_probes.pop(key, None)

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
        if value is None:
            return None
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
    def _file_path_value(cls, value: Any) -> Optional[str]:
        """从路径字符串、FileItem 或序列化后的 FileItem 读取 path。"""
        if isinstance(value, (str, Path)):
            text = str(value).strip()
            return text or None
        path = cls._value(value, ("path",))
        if path is None:
            return None
        text = str(path).strip()
        return text or None

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
        if value is None:
            file_item = cls._value(data, ("fileitem", "file_item", "source_item"))
            value = cls._value(file_item, ("path",))
        return str(value).strip() if value else None

    @classmethod
    def _destination_path(cls, data: Any) -> Optional[Path]:
        transfer_info = cls._event_transfer_info(data)
        value = cls._value(
            transfer_info,
            ("target_path", "dest_path", "dest", "target", "destination"),
        )
        if value is None:
            # MP V2 TransferComplete：最终单文件路径位于
            # transferinfo.target_item.path，而非 target_path。
            target_item = cls._value(
                transfer_info, ("target_item", "target_fileitem", "target_file_item")
            )
            value = cls._file_path_value(target_item)
        if value is None:
            value = cls._value(data, ("target_path", "dest_path", "dest", "target"))
        if value is None:
            target_item = cls._value(
                data, ("target_item", "target_fileitem", "target_file_item")
            )
            value = cls._file_path_value(target_item)
        if value is None:
            # 部分 MP 版本只保证 file_list_new；第一项为本次最终整理文件。
            file_list_new = cls._value(transfer_info, ("file_list_new",))
            if isinstance(file_list_new, (list, tuple)) and file_list_new:
                value = cls._file_path_value(file_list_new[0])
        if not value:
            return None
        path = Path(str(value).strip())
        return path if path.suffix else None

    @classmethod
    def _transfer_history_id(cls, data: Any) -> Optional[int]:
        value = cls._value(data, ("transfer_history_id", "history_id"))
        try:
            history_id = int(value)
        except (TypeError, ValueError):
            return None
        return history_id if history_id > 0 else None

    @classmethod
    def _history_destination(cls, history_id: Optional[int]) -> Optional[Path]:
        """按本次事件的整理历史读取最终目标，仅用于事件目标已经失效的情况。"""
        if history_id is None:
            return None
        try:
            from app.db.transferhistory_oper import TransferHistoryOper

            history = TransferHistoryOper().get(history_id)
            value = getattr(history, "dest", None) if history else None
            path = Path(str(value).strip()) if value else None
            return path if path and path.suffix else None
        except Exception as error:
            logger.debug(
                "【ffprobe媒体信息持久化】读取整理历史最终路径失败 id=%s error=%s",
                history_id,
                error,
            )
            return None

    @classmethod
    def _history_destination_by_source(cls, source_path: str) -> Optional[Path]:
        """手动重试旧记录时，从同一源文件的最新成功整理历史恢复最终目标路径。"""
        source_text = str(source_path or "").strip()
        if not source_text:
            return None
        try:
            from app.db.transferhistory_oper import TransferHistoryOper

            histories = TransferHistoryOper().list_success_by_src(source_text)
            for history in sorted(
                histories or [],
                key=lambda item: str(getattr(item, "date", "")),
                reverse=True,
            ):
                value = getattr(history, "dest", None)
                path = Path(str(value).strip()) if value else None
                if path and path.suffix and path.is_file():
                    logger.info(
                        "【ffprobe媒体信息持久化】手动重试改用整理历史最终路径：%s",
                        path,
                    )
                    return path
        except Exception as error:
            logger.debug(
                "【ffprobe媒体信息持久化】手动重试读取整理历史最终路径失败 source=%s error=%s",
                source_text,
                error,
            )
        return None

    @classmethod
    def _resolve_existing_destination(
        cls, destination: Path, history_id: Optional[int] = None
    ) -> Path:
        """优先事件路径；其已失效时才回查同一条整理历史，避免批量整理额外查库。"""
        if destination.is_file():
            return destination
        history_destination = cls._history_destination(history_id)
        if history_destination and history_destination.is_file():
            logger.info(
                "【ffprobe媒体信息持久化】事件目标已变更，改用整理历史最终路径：%s -> %s",
                destination,
                history_destination,
            )
            return history_destination
        return destination

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

    @classmethod
    def _transfer_succeeded(cls, data: Any) -> bool:
        """仅在 MP 明确标记整理成功后，才写 JSON 或启动兜底 ffprobe。"""
        transfer_info = cls._event_transfer_info(data)
        success = cls._value(transfer_info, ("success",))
        if success is None:
            success = cls._value(data, ("success",))
        return success is True

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
        """给媒体文件刷新留出 10 秒窗口，再由单一调度线程判断是否清理孤立 JSON。"""
        if not self._cleanup_moved_source_json:
            return
        with self._cleanup_condition:
            generation = self._cleanup_generation
            if (
                self._cleanup_worker is None
                or not self._cleanup_worker.is_alive()
                or self._cleanup_worker_generation != generation
            ):
                self._cleanup_worker_generation = generation
                self._cleanup_worker = Thread(
                    target=self._cleanup_worker_loop,
                    args=(generation,),
                    name="ffprobe-media-info-cleanup",
                    daemon=True,
                )
                self._cleanup_worker.start()
            self._cleanup_sequence += 1
            heappush(
                self._cleanup_queue,
                (
                    time.monotonic() + _SOURCE_JSON_CLEANUP_DELAY_SEC,
                    self._cleanup_sequence,
                    source_path,
                ),
            )
            self._cleanup_condition.notify()

    def _cleanup_worker_loop(self, generation: int) -> None:
        """按到期时间依次执行清理；停用/重载后立即退出旧调度线程。"""
        while True:
            with self._cleanup_condition:
                while generation == self._cleanup_generation:
                    if not self._cleanup_queue:
                        self._cleanup_condition.wait()
                        continue
                    due_at, _, source_path = self._cleanup_queue[0]
                    wait_seconds = due_at - time.monotonic()
                    if wait_seconds > 0:
                        self._cleanup_condition.wait(wait_seconds)
                        continue
                    heappop(self._cleanup_queue)
                    break
                else:
                    return
            # 不在条件锁中执行文件 I/O，避免阻塞后续延迟任务的入队。
            self._cleanup_source_json(source_path)

    def _cleanup_source_json(self, source_path: str) -> None:
        """源文件在延迟检查时不存在，就删除严格同名的持久化 JSON。"""
        try:
            if not self._enabled or not self._cleanup_moved_source_json:
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
    def _naming_plugin_classes(cls) -> List[Type[Any]]:
        """查找所有运行时已加载的 ffprobe命名补充类。"""
        candidates: List[Type[Any]] = []
        seen: set[int] = set()
        for module in tuple(sys.modules.values()):
            candidate = getattr(module, "FFprobeNamingSupplement", None)
            if (
                isinstance(candidate, type)
                and getattr(candidate, "plugin_config_prefix", None)
                == "ffprobenamingsupplement_"
                and hasattr(candidate, "_probe_cache")
                and id(candidate) not in seen
            ):
                candidates.append(candidate)
                seen.add(id(candidate))
        return candidates

    @classmethod
    def _get_cached_probe(cls, source_path: str) -> Optional[Dict[str, Any]]:
        """从上游插件的私有缓存取结果；兜底探测由调用方明确决定。"""
        plugin_classes = cls._naming_plugin_classes()
        if not plugin_classes:
            logger.debug("【ffprobe媒体信息持久化】未加载 ffprobe命名补充，跳过")
            return None
        for plugin_class in plugin_classes:
            try:
                probe_target = plugin_class._resolve_probe_target(source_path)
                if not probe_target:
                    continue
                result = plugin_class._probe_cache.get(probe_target)
                if isinstance(result, dict):
                    return result
            except Exception as error:
                logger.debug("【ffprobe媒体信息持久化】读取上游 ffprobe 缓存失败：%s", error)
        return None

    @classmethod
    def _run_fallback_ffprobe(
        cls, media_path: str, timeout: int
    ) -> Optional[Dict[str, Any]]:
        """缓存缺失时的唯一兜底：按配置超时执行一次 ffprobe。"""
        result, _ = cls._run_fallback_ffprobe_detail(media_path, timeout)
        return result

    @classmethod
    def _run_fallback_ffprobe_detail(
        cls, media_path: str, timeout: int
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        """执行一次主动 ffprobe，并返回可供失败记录筛选的简短失败原因。"""
        probe_target = cls._resolve_ffprobe_target(media_path)
        if not probe_target:
            return None, "无法读取 STRM 目标"
        try:
            process = run(
                [
                    "ffprobe", "-v", "error", "-print_format", "json", "-show_streams",
                    "-show_format", "-i", str(probe_target),
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout,
            )
        except TimeoutExpired:
            logger.warning(
                "【ffprobe媒体信息持久化】兜底 ffprobe 超时（%s 秒），跳过：%s",
                timeout,
                probe_target,
            )
            return None, "主动提取超时"
        except OSError as error:
            logger.warning("【ffprobe媒体信息持久化】无法执行兜底 ffprobe：%s", error)
            return None, "主动提取执行错误"
        if process.returncode != 0:
            logger.debug("【ffprobe媒体信息持久化】兜底 ffprobe 失败 rc=%s target=%s err=%s", process.returncode, probe_target, (process.stderr or "")[:500])
            return None, "主动提取失败"
        try:
            result = json.loads(process.stdout)
        except (TypeError, json.JSONDecodeError) as error:
            logger.warning("【ffprobe媒体信息持久化】兜底 ffprobe JSON 解析失败：%s", error)
            return None, "主动提取 JSON 解析失败"
        if not isinstance(result, dict):
            return None, "主动提取返回无效结果"
        return result, ""

    @staticmethod
    def _resolve_ffprobe_target(media_path: str) -> Optional[str]:
        """独立读取 STRM，与上游相同地取首个有效行；不改写文件内容。"""
        path = Path(media_path)
        if path.suffix.lower() != ".strm":
            return media_path.strip() or None
        try:
            for line in path.read_text(encoding="utf-8-sig", errors="replace").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if len(line) >= 2 and line[0] == line[-1] and line[0] in "\"'":
                    line = line[1:-1].strip()
                target = unquote(line).strip()
                if target:
                    return target
        except OSError:
            pass
        return None

    @classmethod
    def _run_hdr_frame_probe(
        cls, media_path: str, timeout: int
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        target = cls._resolve_ffprobe_target(media_path)
        if not target:
            return None, "无法读取目标"
        try:
            process = run(
                ["ffprobe", "-v", "error", "-rw_timeout", str(timeout * 1_000_000),
                 "-select_streams", "v", "-read_intervals", f"%+#{_HDR_FRAME_PACKET_LIMIT}",
                 "-show_frames", "-show_entries",
                 "frame=stream_index,color_transfer,color_primaries,color_space,side_data_list",
                 "-print_format", "json", "-i", target],
                capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout,
            )
        except TimeoutExpired:
            return None, "超时"
        except OSError:
            return None, "执行失败"
        if process.returncode != 0:
            return None, "探测失败"
        try:
            result = json.loads(process.stdout)
        except (TypeError, json.JSONDecodeError):
            return None, "返回无效数据"
        if not isinstance(result, dict) or not isinstance(result.get("frames"), list) or not result["frames"]:
            return None, "未返回视频帧"
        return result, ""

    @eventmanager.register(ChainEventType.TransferRenameBuild)
    def on_transfer_rename_build(self, event: Event) -> None:
        """在上游命名插件提取后暂存其 JSON，等待整理实际完成后写入目标目录。"""
        if not self._enabled:
            return
        data = event.event_data
        if not isinstance(data, TransferRenameBuildEventData):
            logger.debug("【ffprobe媒体信息持久化】收到非预期的重命名构建事件数据，跳过")
            return
        source_item: Optional[FileItem] = data.source_item
        source_path = str(data.source_path or "").strip()
        if not source_path or not source_item or source_item.storage != "local":
            logger.debug("【ffprobe媒体信息持久化】重命名构建事件缺少本地源文件，跳过")
            return
        if not type(self)._is_media_path(source_path):
            logger.debug("【ffprobe媒体信息持久化】源文件不是受支持媒体类型，跳过：%s", source_path)
            return
        destination = type(self)._destination_path(data)
        transfer_method = type(self)._transfer_method(data)
        matches_filters = (
            destination is None
            or self._matches_filters(destination, transfer_method)
        )
        manual_record_ids = self._manual_reorganize_record_ids(source_path)
        # 非筛选范围的普通整理无需读取缓存；但本插件发起的 MP 重新整理仍需确认
        # ffprobe 是否成功，以便即使最终重名失败也能收尾对应记录。
        if not matches_filters and not manual_record_ids:
            logger.debug("【ffprobe媒体信息持久化】不符合生成 JSON 筛选条件，跳过：%s", source_path)
            return
        probe = type(self)._get_cached_probe(source_path)
        if probe is None:
            logger.debug(
                "【ffprobe媒体信息持久化】未命中上游 ffprobe 缓存，等待整理完成后按配置决定是否后台兜底 source=%s",
                source_path,
            )
            return
        # 这说明 ffprobe命名补充已实际取得媒体信息。普通旧失败记录在用户从
        # MP 原生媒体整理再次处理后也应自动消失，不取决于后续是否因重名未落盘。
        self._clear_succeeded_failure(Path(source_path), source_path)
        confirmed_record_ids = self._confirm_manual_reorganize_probe(source_path)
        for record_id in confirmed_record_ids:
            self._remove_plugin_record(record_id)
        if not matches_filters:
            return
        self._stash_pending_probe(source_path, probe, transfer_method)

    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event: Event) -> None:
        """整理成功后，从暂存结果写入实际整理目录。"""
        if not self._enabled:
            return
        data = event.event_data or {}
        if not type(self)._transfer_succeeded(data):
            # 命名阶段可能已暂存结果；失败记录不会有 JSON，立即释放即可。
            failed_source_path = type(self)._source_path(data)
            if failed_source_path:
                self._discard_pending_probe(failed_source_path)
            logger.debug("【ffprobe媒体信息持久化】整理未成功，跳过 JSON 写入和主动提取")
            return
        destination = type(self)._destination_path(data)
        source_path = type(self)._source_path(data)
        if destination is None or source_path is None:
            logger.warning(
                "【ffprobe媒体信息持久化】整理完成事件缺少源或最终目标路径，跳过"
            )
            return
        if not type(self)._is_media_path(source_path):
            logger.debug("【ffprobe媒体信息持久化】整理完成事件的源文件不是受支持媒体类型，跳过：%s", source_path)
            return
        transfer_method = type(self)._transfer_method(data)
        transfer_history_id = type(self)._transfer_history_id(data)
        destination = type(self)._resolve_existing_destination(
            destination, transfer_history_id
        )
        try:
            if not self._matches_filters(destination, transfer_method):
                self._discard_pending_probe(source_path)
                logger.debug("【ffprobe媒体信息持久化】目标不符合生成 JSON 筛选条件，跳过：%s", destination)
                return
            pending = self._take_pending_probe(source_path)
            probe = pending.get("probe") if isinstance(pending, dict) else None
            if not isinstance(probe, dict):
                # 若本插件在前一个链式事件中稍早执行，完成事件时再读一次上游缓存。
                probe = type(self)._get_cached_probe(source_path)
            if not isinstance(probe, dict):
                if self._fallback_probe:
                    self._submit_fallback_task(
                        destination, source_path, transfer_history_id
                    )
                else:
                    logger.warning("【ffprobe媒体信息持久化】没有可复用的 ffprobe 结果，跳过：%s", destination)
                return
            # 命中缓存的 JSON 写入不受 ffprobe 兜底线程数限制；每条整理记录自行完成。
            self._submit_cached_persist(
                destination, probe, source_path, transfer_history_id
            )
        finally:
            # 清理放在该条整理记录的处理末尾；不受生成 JSON 筛选条件影响。
            self._schedule_source_json_cleanup(source_path)

    def _persist(
        self,
        destination: Path,
        probe: Dict[str, Any],
        source: str,
        force_overwrite: bool = False,
        record_source_path: Optional[str] = None,
        transfer_history_id: Optional[int] = None,
    ) -> bool:
        destination = type(self)._resolve_existing_destination(
            destination, transfer_history_id
        )
        if not destination.is_file():
            logger.warning("【ffprobe媒体信息持久化】整理目标不存在，跳过：%s", destination)
            return False
        raw_size = type(self)._raw_abnormal_size(probe)
        if raw_size is not None and not self._allow_abnormal_size_json:
            # 这是受配置控制的正常拦截，不应再记录为“JSON 写入失败”。
            self._record_abnormal_size(destination, raw_size, generated=False)
            self._clear_succeeded_failure(destination, record_source_path)
            logger.info(
                "【ffprobe媒体信息持久化】检测到异常 Size=%s，按配置不生成 JSON：%s",
                raw_size,
                destination,
            )
            return True
        json_path = destination.with_name(destination.stem + _MEDIA_INFO_SUFFIX)
        path_key = str(json_path).casefold()
        with self._writing_json_paths_lock:
            # 同一条整理记录被 MP 重试或重复投递时，仅允许一个线程写同一 JSON。
            if path_key in self._writing_json_paths:
                logger.debug("【ffprobe媒体信息持久化】同名 JSON 正在写入，跳过重复任务：%s", json_path)
                return True
            self._writing_json_paths.add(path_key)
        saved_text: Optional[str] = None
        try:
            if json_path.exists() and not (self._overwrite_json or force_overwrite):
                # 已有 JSON 也表明本次 ffprobe 成功；旧失败记录不应继续保留。
                self._clear_succeeded_failure(destination, record_source_path)
                logger.info("【ffprobe媒体信息持久化】JSON 已存在，按配置不覆盖：%s", json_path)
                return True
            document = type(self)._to_emby_document(probe)
            json_text = json.dumps(document, ensure_ascii=False, separators=(",", ":"))
            temporary = json_path.with_suffix(json_path.suffix + ".tmp")
            temporary.write_text(
                json_text,
                encoding="utf-8",
            )
            temporary.replace(json_path)
            if raw_size is None:
                self._remove_abnormal_size(destination)
            else:
                self._record_abnormal_size(destination, raw_size, generated=True)
            self._clear_succeeded_failure(destination, record_source_path)
            logger.info("【ffprobe媒体信息持久化】已保存 MediaInfo JSON（%s）：%s", source, json_path)
            saved_text = json_text
            return True
        except OSError as error:
            logger.warning("【ffprobe媒体信息持久化】写入 JSON 失败 path=%s error=%s", json_path, error)
            return False
        finally:
            with self._writing_json_paths_lock:
                self._writing_json_paths.discard(path_key)
            # 先释放基础写入锁，极快的本地探测也不会因本次写入仍在进行而被跳过。
            if saved_text is not None and self._hdr_frame_probe:
                self._submit_hdr_enhancement(
                    destination, probe, json_path, sha256(saved_text.encode("utf-8")).hexdigest()
                )

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
    def _media_size(cls, value: Any) -> int:
        """远程 STRM 的 format.size 常为接口响应长度，过小时按未知大小输出。"""
        size = cls._integer(value, 0) or 0
        return size if size >= _MIN_RELIABLE_MEDIA_SIZE_BYTES else 0

    @classmethod
    def _raw_abnormal_size(cls, probe: Dict[str, Any]) -> Optional[int]:
        """仅记录明确返回的非零小 Size；缺失或零表示未知，不作为异常记录。"""
        format_info = probe.get("format") if isinstance(probe, dict) else None
        if not isinstance(format_info, dict):
            return None
        size = cls._integer(format_info.get("size"), None)
        if size is not None and 0 < size < _MIN_RELIABLE_MEDIA_SIZE_BYTES:
            return size
        return None

    @classmethod
    def _video_range_fields(cls, stream: Dict[str, Any]) -> Dict[str, str]:
        """Emby 的 VideoRange 是派生字段，必须同时填写可写的扩展类型。"""
        result = {"VideoRange": "SDR", "ExtendedVideoType": "None",
                  "ExtendedVideoSubType": "None", "ExtendedVideoSubTypeDescription": "None"}
        tag = str(stream.get("codec_tag_string") or "").lower()
        sides = stream.get("side_data_list")
        sides = [side for side in sides if isinstance(side, dict)] if isinstance(sides, list) else []
        dovi = next((side for side in sides if "dovi" in str(side.get("side_data_type") or "").lower()
                     or "dolby vision" in str(side.get("side_data_type") or "").lower()), None)
        if tag in _DOVI_TAGS or dovi is not None:
            result.update(VideoRange="DolbyVision", ExtendedVideoType="DolbyVision")
            if dovi is not None:
                pair = (cls._integer(dovi.get("dv_profile")), cls._integer(dovi.get("dv_bl_signal_compatibility_id")))
                subtype, description = {
                    (0, 2): ("DoviProfile02", "Profile 0.2 (SDR)"),
                    (1, 0): ("DoviProfile10", "Profile 1.0"),
                    (2, 2): ("DoviProfile22", "Profile 2.2 (SDR)"),
                    (3, 0): ("DoviProfile30", "Profile 3.0"),
                    (4, 2): ("DoviProfile42", "Profile 4.2 (SDR)"),
                    (5, 0): ("DoviProfile50", "Profile 5.0"),
                    (6, 1): ("DoviProfile61", "Profile 6.1 (HDR10 compatible)"),
                    (7, 6): ("DoviProfile76", "Profile 7.6 (Bluray)"),
                    (8, 1): ("DoviProfile81", "Profile 8.1 (HDR10 compatible)"),
                    (8, 2): ("DoviProfile82", "Profile 8.2 (SDR)"),
                    (8, 3): ("DoviProfile83", "Profile 8.3"),
                    (8, 4): ("DoviProfile84", "Profile 8.4 (HLG compatible)"),
                    (8, 5): ("DoviProfile85", "Profile 8.5"),
                    (9, 2): ("DoviProfile92", "Profile 9.2 (SDR)"),
                }.get(pair, ("None", "None"))
                result.update(ExtendedVideoSubType=subtype, ExtendedVideoSubTypeDescription=description)
            return result
        if any(cls._is_hdr10plus_side(side) for side in sides):
            result.update(VideoRange="HDR 10+", ExtendedVideoType="Hdr10Plus",
                          ExtendedVideoSubType="Hdr10Plus0", ExtendedVideoSubTypeDescription="HDR 10+")
            return result
        transfer = str(stream.get("color_transfer") or "").lower()
        if transfer == "smpte2084":
            result.update(VideoRange="HDR 10", ExtendedVideoType="Hdr10",
                          ExtendedVideoSubType="Hdr10", ExtendedVideoSubTypeDescription="HDR 10")
        elif transfer == "arib-std-b67":
            result.update(VideoRange="HLG", ExtendedVideoType="HyperLogGamma",
                          ExtendedVideoSubType="HyperLogGamma", ExtendedVideoSubTypeDescription="HLG")
        return result

    @classmethod
    def _video_range(cls, stream: Dict[str, Any]) -> str:
        return cls._video_range_fields(stream)["VideoRange"]

    @staticmethod
    def _is_hdr10plus_side(side: Dict[str, Any]) -> bool:
        kind = str(side.get("side_data_type") or "").lower()
        return "smpte2094-40" in kind or "smpte 2094-40" in kind or "hdr10+" in kind

    @classmethod
    def _hdr_frame_indices(cls, probe: Dict[str, Any]) -> set[int]:
        indices = set()
        streams = probe.get("streams")
        if not isinstance(streams, list):
            return indices
        for index, stream in enumerate(streams):
            if not isinstance(stream, dict) or stream.get("codec_type") != "video":
                continue
            fields = cls._video_range_fields(stream)
            if fields["ExtendedVideoType"] in {"DolbyVision", "Hdr10Plus", "HyperLogGamma"}:
                continue
            # PQ 检查帧级 HDR10+；bt2020-10/12 是歧义标记，不直接认定为 HLG。
            transfer = str(stream.get("color_transfer") or "").lower()
            if transfer in {"smpte2084", "bt2020-10", "bt2020-12"}:
                indices.add(cls._integer(stream.get("index"), index))
        return indices

    @classmethod
    def _needs_hdr_frames(cls, probe: Dict[str, Any]) -> bool:
        return bool(cls._hdr_frame_indices(probe))

    @classmethod
    def _merge_hdr_frames(cls, probe: Dict[str, Any], frame_data: Dict[str, Any]) -> Dict[str, Any]:
        """仅合并有帧级证据的色彩/HDR字段，绝不修改共享的上游缓存。"""
        indices = cls._hdr_frame_indices(probe)
        if not indices:
            return probe
        frames = frame_data.get("frames")
        if not isinstance(frames, list):
            return probe
        streams = list(probe["streams"])
        for position, original in enumerate(streams):
            if not isinstance(original, dict):
                continue
            index = cls._integer(original.get("index"), position)
            if index not in indices:
                continue
            relevant = [frame for frame in frames if isinstance(frame, dict)
                        and cls._integer(frame.get("stream_index")) == index]
            if not relevant:
                continue
            updated = dict(original)
            for field in ("color_transfer", "color_primaries", "color_space"):
                values = {str(frame[field]).lower() for frame in relevant if frame.get(field)
                          and str(frame[field]).lower() not in {"unknown", "unspecified", "reserved"}}
                # 多帧数据互相矛盾时不推断，保留原探测结果。
                if len(values) == 1:
                    updated[field] = values.pop()
            plus_side = next((side for frame in relevant
                              if isinstance(frame.get("side_data_list"), list)
                              for side in frame["side_data_list"]
                              if isinstance(side, dict) and cls._is_hdr10plus_side(side)), None)
            if plus_side is not None:
                original_sides = original.get("side_data_list")
                updated["side_data_list"] = (list(original_sides) if isinstance(original_sides, list) else []) + [dict(plus_side)]
            streams[position] = updated
        return {**probe, "streams": streams}

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
            result.update(cls._video_range_fields(stream))
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
            "Size": cls._media_size(format_info.get("size")),
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
