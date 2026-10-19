from __future__ import annotations

from collections.abc import Callable
from typing import Any

from shittim_core.agent.model import response_to_message
from shittim_core.app.progress import ProgressReporter
from shittim_core.context.builder import ContextBuilder
from shittim_core.domain.model import NormalizedResponse, ToolCall
from shittim_core.domain.types import (
    ContextView,
    ExecutionContext,
    RunHalted,
    RunRequest,
    ToolResult,
)
from shittim_core.state import StateStore
from shittim_core.tools.executor import ToolExecutor


class RuntimePorts:
    def __init__(
        self,
        store: StateStore,
        context: ContextBuilder,
        tools: ToolExecutor,
        policy_check: Callable[[dict[str, Any]], bool] | None = None,
        *,
        progress: ProgressReporter | None = None,
    ) -> None:
        self.store, self.context, self.tools = store, context, tools
        self.progress = progress
        self.policy_check: Callable[[dict[str, Any]], bool] = policy_check or (
            lambda _context: True
        )

    async def build_view(self, request: RunRequest) -> ContextView:
        if not self.policy_check(self.store.get_context(request.context_id)):
            raise RunHalted("current_policy_rejects_saved_authority")
        self.store.assert_current(request.run_id, request.control_epoch)
        view = await self.context.build(request)
        self.store.consume_inputs(request.run_id, view.input_seq)
        if self.progress is not None:
            await self.progress.processing(request, view)
        return view

    def tool_schemas(self, request: RunRequest) -> list[dict[str, Any]]:
        return self.tools.schemas(request.access.allowed_tools)

    async def check_current(self, request: RunRequest, view: ContextView) -> None:
        if not self.policy_check(self.store.get_context(request.context_id)):
            raise RunHalted("current_policy_rejects_saved_authority")
        self.store.assert_current(request.run_id, request.control_epoch, view.input_seq)

    async def record_assistant(self, request: RunRequest, response: NormalizedResponse) -> None:
        self.store.append_history(request.run_id, response_to_message(response))

    async def execute_tool(
        self, request: RunRequest, call: ToolCall, view: ContextView
    ) -> ToolResult:
        if self.progress is not None:
            await self.check_current(request, view)
            await self.progress.tool(request, call, view)
        return await self.tools.execute(
            call, ExecutionContext(request, view.input_seq, view.history_version)
        )

    async def record_tool_result(
        self, request: RunRequest, call: ToolCall, result: ToolResult
    ) -> None:
        if not call.id:
            raise ValueError("Tool result must have its original call identity")
        self.store.upsert_tool_result(
            request.run_id,
            call.id,
            {"role": "tool", "tool_call_id": call.id, "content": result.to_json()},
        )
