"""Fixed, throttled phase notices. No reasoning, arguments, or external descriptions."""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from dataclasses import dataclass

from shittim_core.delivery.service import Delivery
from shittim_core.domain.model import ToolCall
from shittim_core.domain.types import ContextView, RunHalted, RunRequest, StaleView, StateConflict
from shittim_core.state import StateStore

_TOOL_LABELS = {
    "terminal": "执行命令",
    "process_manage": "查看执行进展",
    "read_file": "读取文件",
    "write_file": "写入文件",
    "patch": "修改文件",
    "search_files": "查找文件",
    "read_material": "读取资料",
    "create_material": "整理成果文件",
    "send_material": "发送成果文件",
    "import_material": "导入资料",
    "export_file": "准备成果文件",
    "web_search": "检索网络资料",
    "web_extract": "读取网页",
    "vision_analyze": "分析图片",
    "skills_list": "查找可用技能",
    "skill_view": "读取技能",
    "skill_manage": "管理任务技能",
    "ask_person": "准备协作询问",
    "wait_for_input": "等待补充信息",
    "manage_inquiry": "处理协作答复",
    "read_forward": "读取合并转发",
    "send_forward": "准备转发消息",
    "history_search": "查找历史记录",
}


@dataclass(frozen=True)
class ProgressConfig:
    enabled: bool = False
    min_interval_seconds: float = 15.0

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool:
            raise ValueError("Progress enabled must be a boolean")
        if not math.isfinite(self.min_interval_seconds) or self.min_interval_seconds < 0:
            raise ValueError("Progress interval must be finite and nonnegative")


class ProgressReporter:
    def __init__(
        self,
        store: StateStore,
        delivery: Delivery,
        *,
        config: ProgressConfig | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.store, self.delivery = store, delivery
        self.config = config or ProgressConfig()
        self.clock = clock
        self.failures: list[str] = []

    async def _emit(self, request: RunRequest, view: ContextView, key: str, text: str) -> None:
        if not self.config.enabled:
            return
        try:
            delivery_id = self.store.prepare_progress(
                request.run_id,
                request.control_epoch,
                view.input_seq,
                request.run_id + ":" + key,
                text,
                self.clock(),
                self.config.min_interval_seconds,
            )
            if delivery_id is not None:
                await self.delivery.send(delivery_id)
        except (RunHalted, StaleView, StateConflict):
            # A changed/stopped request no longer owns permission to report a phase.
            return
        except Exception as exc:
            # Progress reporting is ancillary; preserve its outbox state but don't
            # turn a reporting transport failure into a fabricated tool failure.
            self.failures.append(type(exc).__name__)

    async def processing(self, request: RunRequest, view: ContextView) -> None:
        await self._emit(request, view, "processing", "正在处理这项请求。")

    async def tool(self, request: RunRequest, call: ToolCall, view: ContextView) -> None:
        if call.name not in request.access.allowed_tools:
            return
        label = _TOOL_LABELS.get(call.name)
        if label is None:
            label = "调用已配置的工具"
        # Parameters and provider-controlled names/descriptions never enter text.
        await self._emit(request, view, "tool:" + str(call.id), "正在处理：" + label + "。")
