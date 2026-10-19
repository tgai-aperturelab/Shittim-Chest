from __future__ import annotations

import asyncio
import copy
from typing import Any

import pytest

from shittim_core.agent.loop import AgentEngine
from shittim_core.agent.model import ModelTransientError, response_to_message
from shittim_core.agent.ports import RunControl
from shittim_core.domain.model import NormalizedResponse, ToolCall
from shittim_core.domain.types import (
    AccessContext,
    ContextView,
    ConversationAddress,
    RunHalted,
    RunRequest,
    StaleView,
    ToolResult,
)


def request(**kwargs: Any) -> RunRequest:
    return RunRequest(
        "run1",
        "ctx1",
        ConversationAddress("bot", "private", "member"),
        AccessContext("member", "private", frozenset({"read"})),
        1,
        **kwargs,
    )


def reply(text: str = "完成") -> NormalizedResponse:
    return NormalizedResponse(text, None, "stop")


def calls(*ids: str, arguments: str = "{}") -> NormalizedResponse:
    return NormalizedResponse("", [ToolCall(i, "read", arguments) for i in ids], "tool_calls")


class Model:
    def __init__(self, *responses: NormalizedResponse | Exception) -> None:
        self.responses = list(responses)
        self.inputs: list[list[dict[str, Any]]] = []

    async def complete(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> NormalizedResponse:
        self.inputs.append(copy.deepcopy(messages))
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return copy.deepcopy(value)


class Ports:
    def __init__(self) -> None:
        self.history: list[dict[str, Any]] = [{"role": "user", "content": "处理材料"}]
        self.order: list[str] = []
        self.seq = 1
        self.executed: list[str] = []
        self.results: dict[str, ToolResult] = {}
        self.fail_assistant = self.fail_result = self.stale_on_result = False
        self.started, self.release = asyncio.Event(), asyncio.Event()
        self.block = False
        self.effect_count = 0
        self.rejected = False

    async def build_view(self, request: RunRequest) -> ContextView:
        return ContextView(tuple(copy.deepcopy(self.history)), self.seq, len(self.history), 1, 1)

    def tool_schemas(self, request: RunRequest) -> list[dict[str, Any]]:
        return [
            {"type": "function", "function": {"name": "read", "parameters": {"type": "object"}}}
        ]

    async def check_current(self, request: RunRequest, view: ContextView) -> None:
        if self.rejected:
            raise RunHalted("revoked")
        if view.input_seq != self.seq:
            raise StaleView("new_input")

    async def record_assistant(self, request: RunRequest, response: NormalizedResponse) -> None:
        if self.fail_assistant:
            raise OSError("storage fixture failure")
        self.order.append("assistant")
        self.history.append(response_to_message(response))

    async def execute_tool(
        self, request: RunRequest, call: ToolCall, view: ContextView
    ) -> ToolResult:
        self.order.append("execute:" + str(call.id))
        self.executed.append(str(call.id))
        self.started.set()
        if self.block:
            await self.release.wait()
        self.effect_count += 1
        return ToolResult("succeeded", "actual observation", effect_ref="effect1")

    async def record_tool_result(
        self, request: RunRequest, call: ToolCall, result: ToolResult
    ) -> None:
        if self.fail_result:
            raise OSError("result commit failed")
        self.order.append("result:" + str(call.id))
        self.results[str(call.id)] = result
        item = {"role": "tool", "tool_call_id": call.id, "content": result.to_json()}
        for index, row in enumerate(self.history):
            if row.get("role") == "tool" and row.get("tool_call_id") == call.id:
                self.history[index] = item
                break
        else:
            self.history.append(item)
        if self.stale_on_result and len(self.executed) == 1 and self.seq == 1:
            self.seq += 1


async def test_tool_effect_is_recorded_before_follow_up_model():
    model, ports = Model(calls("a"), reply()), Ports()
    outcome = await AgentEngine(model).run(request(), ports, RunControl())
    assert outcome.status == "completed"
    assert ports.order[:3] == ["assistant", "execute:a", "result:a"]
    assert model.inputs[1][-1]["role"] == "tool"
    assert model.inputs[1][-1]["tool_call_id"] == "a"


@pytest.mark.parametrize(
    "response",
    [
        calls("a", arguments='{"path":'),
        calls("a", arguments="[]"),
        NormalizedResponse("partial", None, None),
    ],
)
async def test_incomplete_response_never_dispatches(response):
    ports = Ports()
    outcome = await AgentEngine(Model(response)).run(request(), ports, RunControl())
    assert outcome.status == "failed"
    assert ports.executed == []
    assert ports.order == []


@pytest.mark.parametrize(
    "arguments",
    [
        '{"value":NaN}',
        '{"value":Infinity}',
        '{"value":-Infinity}',
        '{"value":1e999}',
        '{"value":1,"value":2}',
        '{"nested":{"target":"a","target":"b"}}',
        '{"items":[{"key":1,"key":2}]}',
        '{"a":1,"\\u0061":2}',
    ],
)
async def test_non_strict_tool_json_never_dispatches(arguments):
    ports = Ports()
    outcome = await AgentEngine(Model(calls("a", arguments=arguments), reply())).run(
        request(), ports, RunControl()
    )
    assert outcome.status == "failed"
    assert ports.executed == []
    assert ports.order == []


async def test_failed_assistant_persistence_prevents_side_effect():
    ports = Ports()
    ports.fail_assistant = True
    outcome = await AgentEngine(Model(calls("a"))).run(request(), ports, RunControl())
    assert outcome.status == "failed"
    assert not ports.executed


async def test_failed_result_persistence_prevents_second_tool():
    ports = Ports()
    ports.fail_result = True
    outcome = await AgentEngine(Model(calls("a", "b"))).run(request(), ports, RunControl())
    assert outcome.status == "failed"
    assert ports.executed == ["a"]


async def test_stale_input_closes_remaining_group_then_rebuilds():
    ports = Ports()
    ports.stale_on_result = True
    model = Model(calls("a", "b"), reply("已按补充处理"))
    outcome = await AgentEngine(model).run(request(), ports, RunControl())
    assert outcome.status == "completed"
    assert ports.executed == ["a"]
    assert ports.results["b"].status == "not_executed"
    assert model.inputs[1][-1]["tool_call_id"] == "b"


async def test_cancel_retains_inflight_real_result_without_redispatch():
    ports, control = Ports(), RunControl()
    ports.block = True
    run = asyncio.create_task(AgentEngine(Model(calls("a", "b"))).run(request(), ports, control))
    await ports.started.wait()
    control.stop()
    outcome = await run
    assert outcome.status == "cancelled"
    assert ports.results["a"].status == "unknown"
    assert ports.results["b"].status == "not_executed"
    assert not await control.drain_inflight(timeout=0)
    ports.release.set()
    assert await control.drain_inflight(timeout=2)
    assert ports.results["a"].status == "succeeded"
    assert ports.effect_count == 1
    assert ports.executed == ["a"]
    assert len([x for x in ports.history if x.get("tool_call_id") == "a"]) == 1


async def test_caller_cancellation_also_keeps_late_effect():
    ports, control = Ports(), RunControl()
    ports.block = True
    run = asyncio.create_task(AgentEngine(Model(calls("a"))).run(request(), ports, control))
    await ports.started.wait()
    run.cancel()
    assert (await run).status == "cancelled"
    ports.release.set()
    assert await control.drain_inflight(timeout=2)
    assert ports.results["a"].status == "succeeded"


async def test_bounded_model_retries_are_charged():
    model = Model(ModelTransientError("retry"), ModelTransientError("retry"), reply())
    outcome = await AgentEngine(model).run(request(max_model_calls=2), Ports(), RunControl())
    assert outcome.reason == "model_budget"
    assert len(model.inputs) == 2


async def test_tool_budget_closes_unexecuted_calls():
    ports = Ports()
    outcome = await AgentEngine(Model(calls("a", "b"))).run(
        request(max_tool_calls=1), ports, RunControl()
    )
    assert outcome.reason == "tool_budget"
    assert ports.executed == ["a"]
    assert ports.results["b"].status == "not_executed"


async def test_unknown_yields_without_running_dependent_calls():
    class UnknownPorts(Ports):
        async def execute_tool(self, request, call, view):
            return ToolResult("unknown", "request sent, receipt absent", effect_ref="ambiguous")

    ports = UnknownPorts()
    outcome = await AgentEngine(Model(calls("a", "b"))).run(request(), ports, RunControl())
    assert outcome.status == "yielded"
    assert outcome.unresolved_effects == ("ambiguous",)
    assert ports.results["b"].status == "not_executed"


async def test_refusal_is_not_success():
    outcome = await AgentEngine(Model(NormalizedResponse("不能处理", None, "content_filter"))).run(
        request(), Ports(), RunControl()
    )
    assert outcome.status == "failed"
    assert outcome.reason == "model_refusal"


async def test_new_input_between_final_record_and_delivery_replans():
    class CorrectingPorts(Ports):
        async def record_assistant(self, request, response):
            await super().record_assistant(request, response)
            if self.seq == 1:
                self.seq = 2
                self.history.append({"role": "user", "content": "修正要求"})

    ports = CorrectingPorts()
    outcome = await AgentEngine(Model(reply("old"), reply("new"))).run(
        request(), ports, RunControl()
    )
    assert outcome.text == "new"


async def test_stop_during_model_request_cancels_request_without_dispatch():
    entered, cleaned = asyncio.Event(), asyncio.Event()

    class WaitingModel:
        async def complete(self, messages, tools):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.set()
            return calls("never")

    ports, control = Ports(), RunControl()
    run = asyncio.create_task(AgentEngine(WaitingModel()).run(request(), ports, control))
    await entered.wait()
    control.stop()
    assert (await run).status == "cancelled"
    assert cleaned.is_set()
    assert ports.executed == []


async def test_late_tool_exception_is_observed_and_unknown_retained():
    class FailingPorts(Ports):
        async def execute_tool(self, request, call, view):
            self.started.set()
            await self.release.wait()
            raise OSError("ambiguous external transport")

    ports, control = FailingPorts(), RunControl()
    run = asyncio.create_task(AgentEngine(Model(calls("a"))).run(request(), ports, control))
    await ports.started.wait()
    control.stop()
    assert (await run).status == "cancelled"
    ports.release.set()
    assert await control.drain_inflight(2)
    assert ports.results["a"].status == "unknown"
    assert control.late_errors == ["OSError"]


async def test_duplicate_provider_call_id_gets_unique_protocol_pairing():
    ports = Ports()
    outcome = await AgentEngine(Model(calls("a", "a"), reply())).run(request(), ports, RunControl())
    assert outcome.status == "completed"
    assert ports.executed == ["a", "a_d2"]
