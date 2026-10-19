"""The only capabilities made available to the execution loop."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Protocol

from shittim_core.domain.model import NormalizedResponse, ToolCall
from shittim_core.domain.types import ContextView, RunRequest, ToolResult


class AsyncModel(Protocol):
    async def complete(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> NormalizedResponse: ...


class RunPorts(Protocol):
    async def build_view(self, request: RunRequest) -> ContextView: ...

    def tool_schemas(self, request: RunRequest) -> list[dict[str, Any]]: ...

    async def check_current(self, request: RunRequest, view: ContextView) -> None: ...

    async def record_assistant(self, request: RunRequest, response: NormalizedResponse) -> None: ...

    async def execute_tool(
        self, request: RunRequest, call: ToolCall, view: ContextView
    ) -> ToolResult: ...

    async def record_tool_result(
        self, request: RunRequest, call: ToolCall, result: ToolResult
    ) -> None: ...


@dataclass
class RunControl:
    cancel: asyncio.Event = field(default_factory=asyncio.Event)
    reason: str = ""
    inflight: set[asyncio.Task[None]] = field(
        default_factory=lambda: set[asyncio.Task[None]](), repr=False
    )
    late_errors: list[str] = field(default_factory=lambda: list[str](), repr=False)

    def stop(self, reason: str = "stop_requested") -> None:
        self.reason = reason
        self.cancel.set()

    async def drain_inflight(self, timeout: float = 5.0) -> bool:
        """Wait without cancelling effects; False means work remains, not that it stopped."""
        tasks = tuple(self.inflight)
        if not tasks:
            return True
        _, pending = await asyncio.wait(tasks, timeout=timeout)
        return not pending
