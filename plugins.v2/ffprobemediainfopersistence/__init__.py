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
from hashlib import sha1
from concurrent.futures import Future, ThreadPoolExecutor
from subprocess import TimeoutExpired, run
from threading import Condition, Lock, Thread
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type

from app.core.config import settings
from app.core.event import Event, eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app import schemas
from app.schemas import FileItem, TransferRenameBuildEventData
from app.schemas.types import ChainEventType, EventType


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
_CACHE_WRITE_WORKERS = 32
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
    plugin_version = "1.0.0"
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
        return [
            {
                "path": "/failed-extractions/select",
                "endpoint": self.set_failed_extraction_selected,
                "methods": ["GET"],
                "summary": "选择或取消选择失败提取记录",
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
            {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "density": "compact", "text": "使用说明：优先复用“ffprobe命名补充”已获取的缓存，缓存命中后立即后台写入 JSON，最多 32 个并发。仅缓存缺失时才按“主动提取”配置对最终目标文件运行 ffprobe。上游 ffprobe 未请求章节，因此输出 JSON 的 Chapters 为空。"}},
            {"component": "VAlert", "props": {"type": "warning", "variant": "tonal", "density": "compact", "text": "JSON清理：文件整理完成后延迟 10 秒检查，若媒体文件已不存在，则清理媒体文件目录下严格同名的 -mediainfo.json 文件。"}},
                ],
            },
        ]}], {
            "enabled": False,
            "overwrite_json": False,
            "allow_abnormal_size_json": True,
            "fallback_probe": True,
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
        if state in {
            _RECORD_CATEGORY_ABNORMAL_SIZE,
            _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING,
        }:
            state_records = [
                record for record in records
                if record.get("category") == state
            ]
        else:
            state_records = [
                record for record in records
                if record.get("category") == _RECORD_CATEGORY_FAILURE
                and bool(record.get("handled")) == (state == "handled")
            ]
        visible_records = [
            record for record in state_records
            if not reason or record.get("reason") == reason
        ]
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
                    {"state": key, "reason": ""},
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
                {"state": state, "reason": ""},
            ),
        }]
        for item_reason in sorted({str(record.get("reason") or "未知错误") for record in state_records}):
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
                    {"state": state, "reason": item_reason},
                ),
            })

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
                                else str(record.get("reason") or "未知错误")
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
                        {"component": "th", "text": "选择"},
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

    @staticmethod
    def _failure_record_id(destination: str) -> str:
        return sha1(str(Path(destination)).casefold().encode("utf-8")).hexdigest()

    @staticmethod
    def _abnormal_size_record_id(destination: str) -> str:
        return "abnormal_size:" + sha1(
            str(Path(destination)).casefold().encode("utf-8")
        ).hexdigest()

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

    def _failed_extractions_view(self) -> Dict[str, str]:
        raw_view = self.get_data(_FAILED_EXTRACTIONS_VIEW_KEY) or {}
        state = str(raw_view.get("state") if isinstance(raw_view, dict) else "pending")
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
        }

    def _save_failed_extractions_view(self, state: str, reason: str) -> None:
        self.save_data(_FAILED_EXTRACTIONS_VIEW_KEY, {"state": state, "reason": reason})

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
        self, state: str = "pending", reason: str = "", apikey: Optional[str] = None
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
        with self._failed_extractions_lock:
            records = self._failed_extractions()
            for record in records:
                record["selected"] = False
            self._save_failed_extractions(records)
            self._save_failed_extractions_view(normalized_state, reason)
        return schemas.Response(success=True, message="已切换筛选条件")

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
                f"重新提取进行中：共 {progress['total']} 个文件，已开始 {progress['started']} 个，"
                f"已完成 {progress['completed']} 个，成功 {progress['success']} 个，失败 {progress['failed']} 个"
            )
        if progress.get("total"):
            return (
                f"上次重新提取：共 {progress['total']} 个文件，成功 {progress['success']} 个，"
                f"失败 {progress['failed']} 个"
            )
        return "选择失败记录后点击“重新提取”，任务将在后台运行，关闭此页面不影响执行"

    def retry_selected_failed_extractions(self, apikey: Optional[str] = None) -> Any:
        """将已选失败记录交给现有主动提取线程池，接口立即返回、不阻塞页面。"""
        if not self._api_authorized(apikey):
            return self._api_denied()
        if not self._enabled:
            return schemas.Response(success=False, message="插件未启用，无法重新提取")
        executor = self._fallback_executor
        if executor is None:
            return schemas.Response(success=False, message="主动提取器未启动")
        with self._failed_extractions_lock:
            selected_records = [
                dict(record) for record in self._failed_extractions() if record.get("selected")
            ]
        if not selected_records:
            return schemas.Response(success=False, message="请先选择至少一条失败记录")
        with self._manual_retry_lock:
            if self._manual_retry_progress.get("running"):
                return schemas.Response(success=False, message="已有重新提取任务正在后台运行")
            selected_records = [
                record for record in selected_records
                if record["id"] not in self._manual_retry_running_ids
            ]
            if not selected_records:
                return schemas.Response(success=False, message="所选记录正在重新提取")
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
        logger.info("【ffprobe媒体信息持久化】已提交 %s 个手动重新提取任务", len(selected_records))
        return schemas.Response(success=True, message=f"已在后台提交 {len(selected_records)} 个重新提取任务")

    def _manual_retry_one(self, record: Dict[str, Any]) -> None:
        """执行单条手动重试；成功即移除失败记录，失败转入已处理并增加尝试次数。"""
        record_id = str(record["id"])
        destination = Path(str(record["destination"]))
        with self._manual_retry_lock:
            self._manual_retry_progress["started"] += 1
        success = False
        failure_reason = "未知错误"
        try:
            if not destination.is_file():
                failure_reason = "整理目标不存在"
            else:
                probe, failure_reason = type(self)._run_fallback_ffprobe_detail(
                    str(destination), self._fallback_timeout
                )
                if isinstance(probe, dict):
                    # 手动重新提取的目的是修复已记录项目，即使关闭全局覆盖也要写入新结果。
                    success = self._persist(
                        destination, probe, "手动重新提取", force_overwrite=True
                    )
                    if not success:
                        failure_reason = "JSON 写入失败"
        except Exception as error:
            failure_reason = "主动提取执行错误"
            logger.warning("【ffprobe媒体信息持久化】手动重新提取异常 path=%s error=%s", destination, error)
        if record.get("category") in {
            _RECORD_CATEGORY_ABNORMAL_SIZE,
            _RECORD_CATEGORY_ABNORMAL_SIZE_PENDING,
        }:
            # _persist 会按此次 ffprobe 的原始 Size 自动保留或移除异常大小记录。
            if not success:
                self._mark_abnormal_size_retry_failed(record_id)
        elif success:
            self._remove_failed_extraction(record_id)
        else:
            self._mark_manual_retry_failed(record_id, failure_reason)
        with self._manual_retry_lock:
            self._manual_retry_running_ids.discard(record_id)
            self._manual_retry_progress["completed"] += 1
            self._manual_retry_progress["success" if success else "failed"] += 1
            if self._manual_retry_progress["completed"] >= self._manual_retry_progress["total"]:
                self._manual_retry_progress["running"] = False

    def _record_failed_extraction(self, destination: Path, reason: str) -> None:
        """记录“上游缓存缺失且主动提取失败”的首次失败；保留首次失败时间。"""
        destination_text = str(destination)
        record_id = type(self)._failure_record_id(destination_text)
        with self._failed_extractions_lock:
            records = self._failed_extractions()
            for record in records:
                if (
                    record["id"] == record_id
                    and record.get("category") == _RECORD_CATEGORY_FAILURE
                ):
                    record["reason"] = reason
                    self._save_failed_extractions(records)
                    return
            records.append({
                "id": record_id,
                "destination": destination_text,
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
            self._save_failed_extractions([
                record for record in records
                if not (
                    record["id"] == record_id
                    and record.get("category") == _RECORD_CATEGORY_FAILURE
                )
            ])

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
        """停止尚未开始的兜底任务；已运行的 ffprobe 仍受 10 秒超时保护。"""
        executor = self._fallback_executor
        self._fallback_executor = None
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        cached_write_executor = self._cached_write_executor
        self._cached_write_executor = None
        if cached_write_executor is not None:
            cached_write_executor.shutdown(wait=False, cancel_futures=True)
        with self._fallback_tasks_lock:
            self._fallback_tasks.clear()
        with self._cached_write_tasks_lock:
            self._cached_write_tasks.clear()
        with self._cleanup_condition:
            self._cleanup_generation += 1
            self._cleanup_queue.clear()
            self._cleanup_condition.notify_all()

    def _submit_cached_persist(self, destination: Path, probe: Dict[str, Any]) -> None:
        """命中缓存后立即提交独立写入池，不占用主动 ffprobe 并发。"""
        executor = self._cached_write_executor
        if executor is None:
            logger.warning("【ffprobe媒体信息持久化】JSON 写入器未启动，跳过：%s", destination)
            return
        try:
            future = executor.submit(self._persist, destination, probe, "复用上游缓存")
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

    def _submit_fallback_task(self, destination: Path) -> None:
        """提交后台兜底探测，不阻塞 MP 当前整理事件。"""
        executor = self._fallback_executor
        if executor is None:
            logger.warning("【ffprobe媒体信息持久化】后台提取器未启动，跳过：%s", destination)
            return
        try:
            future = executor.submit(self._background_probe_and_persist, destination)
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

    def _background_probe_and_persist(self, destination: Path) -> None:
        """对已整理到位的目标文件执行一次 10 秒兜底探测并写入 JSON。"""
        if not destination.is_file():
            logger.warning("【ffprobe媒体信息持久化】后台提取时目标不存在，跳过：%s", destination)
            self._record_failed_extraction(destination, "整理目标不存在")
            return
        probe, failure_reason = type(self)._run_fallback_ffprobe_detail(
            str(destination), self._fallback_timeout
        )
        if not isinstance(probe, dict):
            logger.warning("【ffprobe媒体信息持久化】后台 ffprobe 未得到结果，跳过：%s", destination)
            self._record_failed_extraction(destination, failure_reason)
            return
        if not self._persist(destination, probe, "主动提取"):
            self._record_failed_extraction(destination, "JSON 写入失败")

    def _clear_pending_probes(self) -> None:
        with self._pending_probes_lock:
            self._pending_probes.clear()
            self._pending_probe_last_prune = 0.0

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
        probe_target = media_path
        naming_plugins = cls._naming_plugin_classes()
        if naming_plugins:
            try:
                probe_target = naming_plugins[-1]._resolve_probe_target(media_path)
            except Exception:
                probe_target = media_path
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
        # 如果此时已有目标路径，先筛选再读取缓存，避免无用处理。
        if destination is not None and not self._matches_filters(destination, transfer_method):
            logger.debug("【ffprobe媒体信息持久化】不符合生成 JSON 筛选条件，跳过：%s", source_path)
            return
        probe = type(self)._get_cached_probe(source_path)
        if probe is None:
            logger.debug(
                "【ffprobe媒体信息持久化】未命中上游 ffprobe 缓存，等待整理完成后按配置决定是否后台兜底 source=%s",
                source_path,
            )
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
                    self._submit_fallback_task(destination)
                else:
                    logger.warning("【ffprobe媒体信息持久化】没有可复用的 ffprobe 结果，跳过：%s", destination)
                return
            # 命中缓存的 JSON 写入不受 ffprobe 兜底线程数限制；每条整理记录自行完成。
            self._submit_cached_persist(destination, probe)
        finally:
            # 清理放在该条整理记录的处理末尾；不受生成 JSON 筛选条件影响。
            self._schedule_source_json_cleanup(source_path)

    def _persist(
        self,
        destination: Path,
        probe: Dict[str, Any],
        source: str,
        force_overwrite: bool = False,
    ) -> bool:
        if not destination.is_file():
            logger.warning("【ffprobe媒体信息持久化】整理目标不存在，跳过：%s", destination)
            return False
        raw_size = type(self)._raw_abnormal_size(probe)
        if raw_size is not None and not self._allow_abnormal_size_json:
            # 这是受配置控制的正常拦截，不应再记录为“JSON 写入失败”。
            self._record_abnormal_size(destination, raw_size, generated=False)
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
        try:
            if json_path.exists() and not (self._overwrite_json or force_overwrite):
                logger.info("【ffprobe媒体信息持久化】JSON 已存在，按配置不覆盖：%s", json_path)
                return True
            document = type(self)._to_emby_document(probe)
            temporary = json_path.with_suffix(json_path.suffix + ".tmp")
            temporary.write_text(
                json.dumps(document, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            temporary.replace(json_path)
            if raw_size is None:
                self._remove_abnormal_size(destination)
            else:
                self._record_abnormal_size(destination, raw_size, generated=True)
            logger.info("【ffprobe媒体信息持久化】已保存 MediaInfo JSON（%s）：%s", source, json_path)
            return True
        except OSError as error:
            logger.warning("【ffprobe媒体信息持久化】写入 JSON 失败 path=%s error=%s", json_path, error)
            return False
        finally:
            with self._writing_json_paths_lock:
                self._writing_json_paths.discard(path_key)

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
