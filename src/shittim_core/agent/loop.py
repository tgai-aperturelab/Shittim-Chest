"""Hermes-derived turn ordering with project-owned state and effects.

Sources: conversation_loop.py:1656-1709, turn_tool_round.py:76-177,
turn_iteration_prep.py:175-181, and message_sanitization.py:503-588.
MIT source attribution and deliberate changes: third_party/hermes-port-manifest.json.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

from shittim_core.agent.budget import IterationBudget
from shittim_core.agent.model import ModelTransientError, validate_response
from shittim_core.agent.ports import AsyncModel, RunControl, RunPorts
from shittim_core.domain.json_args import parse_tool_arguments
from shittim_core.domain.model import NormalizedResponse, ToolCall
from shittim_core.domain.types import (
    ContextView,
    RunHalted,
    RunOutcome,
    RunRequest,
    StaleView,
    ToolResult,
)
from shittim_core.engine.hermes.tool_call_ids import (
    coalesce_tool_call_id,
    deterministic_call_id,
    normalize_provider_tool_call_ids,
    uniquify_tool_call_ids,
)

T = TypeVar("T")


class _Stopped(Exception):
    def __init__(self, reason: str, task: asyncio.Task[Any]) -> None:
        self.reason = reason
        self.task = task


async def _controlled(
    operation: Coroutine[Any, Any, T],
    control: RunControl,
    deadline: float,
    *,
    retain: bool = False,
) -> T:
    """Only tool work may outlive the caller; its observer owns result collection."""
    task = asyncio.create_task(operation)
    stopped = asyncio.create_task(control.cancel.wait())
    try:
        await asyncio.wait(
            (task, stopped),
            timeout=max(0.0, deadline - asyncio.get_running_loop().time()),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task.done():
            return task.result()
        reason = control.reason or "stop_requested" if control.cancel.is_set() else "run_timeout"
        if not retain:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        raise _Stopped(reason, task)
    except asyncio.CancelledError:
        if not retain:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        # Translate caller cancellation so a running tool always gets a durable unknown result.
        control.stop("caller_cancelled")
        raise _Stopped("caller_cancelled", task) from None
    finally:
        stopped.cancel()
        await asyncio.gather(stopped, return_exceptions=True)


def _assign_call_ids(response: NormalizedResponse, messages: tuple[dict[str, Any], ...]) -> None:
    """Hermes deterministic fallback + uniqueness; IDs pair protocol rows, not effects."""
    taken = {
        str(call.get("id"))
        for message in messages
        for call in message.get("tool_calls", [])
        if call.get("id")
    }
    calls = response.tool_calls or []
    normalize_provider_tool_call_ids(calls)
    for index, call in enumerate(calls):
        call.id = coalesce_tool_call_id(call) or deterministic_call_id(
            call.name, call.arguments, index
        )
    uniquify_tool_call_ids(calls, taken)
    for call in calls:
        call.id = coalesce_tool_call_id(call)


class AgentEngine:
    def __init__(self, model: AsyncModel) -> None:
        self.model = model

    @staticmethod
    async def _check(
        request: RunRequest,
        ports: RunPorts,
        control: RunControl,
        view: ContextView,
        deadline: float,
    ) -> None:
        if control.cancel.is_set():
            raise RunHalted(control.reason or "stop_requested")
        if asyncio.get_running_loop().time() >= deadline:
            raise RunHalted("run_timeout")
        await ports.check_current(request, view)

    @staticmethod
    async def _close_calls(
        request: RunRequest,
        ports: RunPorts,
        calls: list[ToolCall],
        reason: str,
    ) -> None:
        for call in calls:
            await ports.record_tool_result(request, call, ToolResult("not_executed", reason))

    @staticmethod
    def _observe_late(
        task: asyncio.Task[ToolResult],
        request: RunRequest,
        ports: RunPorts,
        call: ToolCall,
        control: RunControl,
    ) -> None:
        async def observe() -> None:
            try:
                result = await task
                await ports.record_tool_result(request, call, result)
            except (RunHalted, StaleView):
                try:
                    await ports.record_tool_result(
                        request, call, ToolResult("not_executed", "dispatch_refused")
                    )
                except Exception as exc:
                    control.late_errors.append(type(exc).__name__)
            except (Exception, asyncio.CancelledError) as exc:
                # Retain unknown; an exception is not proof of effect failure.
                control.late_errors.append(type(exc).__name__)

        observer = asyncio.create_task(observe())
        control.inflight.add(observer)
        observer.add_done_callback(control.inflight.discard)

    async def run(self, request: RunRequest, ports: RunPorts, control: RunControl) -> RunOutcome:
        budget = IterationBudget(request.max_model_calls)
        deadline = asyncio.get_running_loop().time() + request.timeout_seconds
        tool_count = 0
        unresolved: list[str] = []
        try:
            schemas = ports.tool_schemas(request)
            while budget.remaining > 0:
                view = await ports.build_view(request)
                await self._check(request, ports, control, view, deadline)
                if not budget.consume():
                    break
                try:
                    response = await _controlled(
                        self.model.complete(list(view.messages), schemas), control, deadline
                    )
                except ModelTransientError:
                    continue
                validate_response(response)
                _assign_call_ids(response, view.messages)
                calls = response.tool_calls or []
                if not calls:
                    try:
                        await self._check(request, ports, control, view, deadline)
                    except StaleView:
                        continue
                    await ports.record_assistant(request, response)
                    # Final output is still checked by the delivery owner before actual send.
                    try:
                        await self._check(request, ports, control, view, deadline)
                    except StaleView:
                        continue
                    if response.finish_reason == "content_filter":
                        return RunOutcome("failed", response.content or "", reason="model_refusal")
                    return RunOutcome("completed", response.content or "")

                # Hermes invariant: full assistant call group durable before any side effect.
                await ports.record_assistant(request, response)
                replan = False
                for index, call in enumerate(calls):
                    try:
                        await self._check(request, ports, control, view, deadline)
                        if tool_count >= request.max_tool_calls:
                            await self._close_calls(request, ports, calls[index:], "tool_budget")
                            return RunOutcome("failed", reason="tool_budget")
                        # All call arguments were validated before recording the group.
                        parse_tool_arguments(call.arguments)
                        tool_count += 1
                        result = await _controlled(
                            ports.execute_tool(request, call, view), control, deadline, retain=True
                        )
                    except StaleView:
                        await self._close_calls(request, ports, calls[index:], "superseded_input")
                        replan = True
                        break
                    except RunHalted as exc:
                        await self._close_calls(request, ports, calls[index:], "dispatch_refused")
                        return RunOutcome("cancelled", reason=str(exc))
                    except _Stopped as exc:
                        unresolved.append(call.id or "")
                        # Install observer even if persistence fails: no uncollected task exception.
                        try:
                            await ports.record_tool_result(
                                request, call, ToolResult("unknown", "inflight_outcome_pending")
                            )
                        finally:
                            self._observe_late(exc.task, request, ports, call, control)
                        await self._close_calls(request, ports, calls[index + 1 :], "run_stopped")
                        return RunOutcome(
                            "cancelled" if control.cancel.is_set() else "failed",
                            reason=exc.reason,
                            unresolved_effects=tuple(unresolved),
                        )
                    except Exception:
                        # execute_tool may have crossed its external dispatch boundary.
                        result = ToolResult("unknown", "tool_outcome_unconfirmed")
                    await ports.record_tool_result(request, call, result)
                    if result.status in {"unknown", "needs_input"}:
                        await self._close_calls(request, ports, calls[index + 1 :], "run_yielded")
                        if result.status == "unknown":
                            unresolved.append(result.effect_ref or call.id or "")
                        return RunOutcome(
                            "yielded", reason=result.status, unresolved_effects=tuple(unresolved)
                        )
                if replan:
                    continue
            return RunOutcome("failed", reason="model_budget")
        except (RunHalted, _Stopped) as exc:
            reason = exc.reason if isinstance(exc, _Stopped) else str(exc)
            return RunOutcome(
                "failed" if reason == "run_timeout" else "cancelled",
                reason=reason,
                unresolved_effects=tuple(unresolved),
            )
        except asyncio.CancelledError:
            control.stop("caller_cancelled")
            return RunOutcome(
                "cancelled", reason="caller_cancelled", unresolved_effects=tuple(unresolved)
            )
        except Exception as exc:
            # No raw exception body: storage/model errors can contain secrets or request payloads.
            return RunOutcome(
                "failed", reason=type(exc).__name__, unresolved_effects=tuple(unresolved)
            )
