"""Offline behavior: real runtime/state owners, scripted model, fake QQ transport."""

from __future__ import annotations

import asyncio
import copy
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest

from shittim_core.agent.loop import AgentEngine
from shittim_core.app.run_ports import RuntimePorts
from shittim_core.collaboration.coordinator import Coordinator
from shittim_core.context.builder import ContextBuilder
from shittim_core.delivery.service import Delivery
from shittim_core.domain.model import NormalizedResponse, ToolCall
from shittim_core.domain.types import (
    AccessContext,
    ControlCommand,
    ConversationAddress,
    ExecutionContext,
    InputEvent,
    OutboundIntent,
    ProtocolReceipt,
    RunRequest,
    StateConflict,
    ToolResult,
)
from shittim_core.state import StateStore
from shittim_core.tools.collaboration import register_collaboration_tools
from shittim_core.tools.executor import ToolExecutor, ToolSpec

type Messages = list[dict[str, Any]]
type Step = NormalizedResponse | Callable[[Messages], Awaitable[NormalizedResponse]]

ADDRESS = ConversationAddress("999", "group", "10")


class ScriptModel:
    def __init__(self, steps: list[Step]) -> None:
        self.steps = list(steps)
        self.requests: list[Messages] = []

    async def complete(self, messages: Messages, tools: list[dict[str, Any]]) -> NormalizedResponse:
        self.requests.append(copy.deepcopy(messages))
        if not self.steps:
            raise AssertionError("Unexpected extra model request")
        step = self.steps.pop(0)
        return await step(messages) if callable(step) else step


class FakeQQ:
    def __init__(self, statuses: list[str] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.statuses = list(statuses or [])

    async def call(self, action: str, params: dict[str, Any], attempt_id: str) -> ProtocolReceipt:
        self.calls.append(
            {"action": action, "params": copy.deepcopy(params), "attempt_id": attempt_id}
        )
        status = self.statuses.pop(0) if self.statuses else "acknowledged"
        if status == "unknown":
            return ProtocolReceipt("unknown", error="simulated lost acknowledgement")
        return ProtocolReceipt("acknowledged", message_id=str(1000 + len(self.calls)))


@dataclass
class Runtime:
    store: StateStore
    model: ScriptModel
    qq: FakeQQ
    tools: ToolExecutor
    delivery: Delivery
    coordinator: Coordinator

    async def close(self) -> None:
        assert await self.coordinator.close()
        self.store.close()


def runtime(path: Path, steps: list[Step], statuses: list[str] | None = None) -> Runtime:
    store = StateStore(path)
    model = ScriptModel(steps)
    qq = FakeQQ(statuses)
    delivery = Delivery(store, qq.call)
    tools = ToolExecutor(store)

    async def known_fixture_recipient(address: ConversationAddress, recipient_id: str) -> bool:
        # Explicit synthetic membership for this offline harness, never platform evidence.
        return address == ADDRESS and recipient_id in {"101", "202"}

    register_collaboration_tools(tools, store, delivery, known_recipient=known_fixture_recipient)
    ports = RuntimePorts(store, ContextBuilder(store), tools)
    access = AccessContext(
        "101",
        ADDRESS.key,
        frozenset({"perform", "ask_person", "wait_for_input"}),
        frozenset({"resource:one", "qq:" + ADDRESS.key}),
    )
    coordinator = Coordinator(
        store,
        AgentEngine(model),
        ports,
        delivery,
        bot_id="999",
        allowed_users=frozenset({"101", "202"}),
        allowed_groups=frozenset({"10"}),
        access_factory=lambda event: replace(access, actor_id=event.author_id),
        timeout_seconds=4,
    )
    return Runtime(store, model, qq, tools, delivery, coordinator)


def event(
    text: str,
    *,
    author: str = "101",
    mid: str = "request",
    reply: str | None = None,
    mentioned: bool = True,
) -> InputEvent:
    return InputEvent(ADDRESS, author, mid, text, reply_to=reply, mentioned_bot=mentioned)


def tool_response(
    name: str = "perform", args: dict[str, Any] | None = None, call_id: str = "call1"
) -> NormalizedResponse:
    return NormalizedResponse(None, [ToolCall(call_id, name, json.dumps(args or {}))], "tool_calls")


def final(text: str) -> NormalizedResponse:
    return NormalizedResponse(text, None, "stop")


def register_effect(rt: Runtime, handler: Callable[..., Awaitable[ToolResult]]) -> None:
    rt.tools.register(
        ToolSpec(
            "perform",
            "Generic test effect",
            {"type": "object", "additionalProperties": False},
            handler,
            lambda _a, _c: ("resource:one",),
        )
    )


async def idle(rt: Runtime) -> None:
    await asyncio.wait_for(rt.coordinator.wait_idle(), 2)


@pytest.mark.asyncio
async def test_real_runtime_single_tool_round_and_one_final_delivery(tmp_path: Path):
    rt = runtime(tmp_path / "state.db", [tool_response(), final("已完成")])
    effects: list[str] = []

    async def perform(_args: dict[str, Any], ctx: ExecutionContext) -> ToolResult:
        history = rt.store.history(ctx.run.context_id)
        assert history[-1]["tool_calls"][0]["id"] == "call1"
        effects.append(ctx.run.run_id)
        return ToolResult("succeeded", "effect exists")

    register_effect(rt, perform)
    try:
        accepted = await rt.coordinator.accept(event("处理这个请求"))
        await idle(rt)
        assert accepted.status == "associated" and accepted.context_id
        assert len(effects) == 1 and len(rt.qq.calls) == 1
        assert len(rt.model.requests) == 2
        tool_messages = [m for m in rt.model.requests[1] if m["role"] == "tool"]
        assert len(tool_messages) == 1
        assert json.loads(tool_messages[0]["content"])["status"] == "succeeded"
        assert rt.store.list_deliveries()[0]["status"] == "acknowledged"
        assert not rt.coordinator.failures
    finally:
        await rt.close()


@pytest.mark.asyncio
async def test_b_quotes_a_while_model_busy_invalidates_old_action(tmp_path: Path):
    entered, release = asyncio.Event(), asyncio.Event()

    async def stale_decision(_messages: Messages) -> NormalizedResponse:
        entered.set()
        await release.wait()
        return tool_response()

    rt = runtime(tmp_path / "state.db", [stale_decision, final("采用补充信息")])
    effects: list[int] = []

    async def perform(_a: dict[str, Any], _ctx: ExecutionContext) -> ToolResult:
        effects.append(1)
        return ToolResult("succeeded", "unexpected old action")

    register_effect(rt, perform)
    try:
        first = await rt.coordinator.accept(event("处理请求A"))
        await asyncio.wait_for(entered.wait(), 1)
        second = await rt.coordinator.accept(
            event("补充条件B", author="202", mid="supplement", reply="request", mentioned=False)
        )
        assert second.context_id == first.context_id
        release.set()
        await idle(rt)
        assert effects == []
        assert len(rt.model.requests) == 2 and len(rt.qq.calls) == 1
        contents = "\n".join(str(m.get("content", "")) for m in rt.model.requests[1])
        assert "作者QQ=101" in contents and "作者QQ=202" in contents and "补充条件B" in contents
        closed = [m for m in rt.model.requests[1] if m["role"] == "tool"]
        assert json.loads(closed[0]["content"])["status"] == "not_executed"
        assert not rt.coordinator.failures
    finally:
        release.set()
        await rt.close()


@pytest.mark.asyncio
async def test_stop_preserves_late_actual_effect_without_late_final(tmp_path: Path):
    started, release = asyncio.Event(), asyncio.Event()
    rt = runtime(tmp_path / "state.db", [tool_response()])
    effects: list[int] = []

    async def perform(_a: dict[str, Any], _ctx: ExecutionContext) -> ToolResult:
        effects.append(1)
        started.set()
        await release.wait()
        return ToolResult("succeeded", "actual external result")

    register_effect(rt, perform)
    try:
        accepted = await rt.coordinator.accept(event("执行"))
        assert accepted.context_id
        await asyncio.wait_for(started.wait(), 1)
        receipt = await rt.coordinator.control(ControlCommand(accepted.context_id, "101", "stop"))
        assert receipt.accepted
        await idle(rt)
        history = rt.store.history(accepted.context_id)
        result = [m for m in history if m["role"] == "tool"]
        assert len(result) == 1 and json.loads(result[0]["content"])["status"] == "unknown"
        assert not rt.qq.calls and effects == [1]
        release.set()
        assert await rt.coordinator.close()
        result = [m for m in rt.store.history(accepted.context_id) if m["role"] == "tool"]
        assert len(result) == 1 and json.loads(result[0]["content"])["status"] == "succeeded"
        effect_id = json.loads(result[0]["content"])["effect_ref"]
        assert rt.store.get_effect(effect_id)["status"] == "succeeded"
        assert rt.store.get_context(accepted.context_id)["control_state"] == "paused"
        assert not rt.qq.calls and effects == [1]
    finally:
        release.set()
        await rt.close()


@pytest.mark.asyncio
async def test_final_outbox_survives_reopen_without_reexecuting_model_or_tool(tmp_path: Path):
    path = tmp_path / "state.db"
    before = runtime(path, [])
    initial = event("request")
    input_id, _ = before.store.ingest(initial)
    before.store.create_context("c", initial, AccessContext("101", ADDRESS.key))
    before.store.attach_input("c", input_id)
    before.store.claim_run("c", "r")
    effect = before.store.begin_effect("r", "call", "perform", {}, ["resource:one"], 1)
    before.store.finish_effect(effect, ToolResult("succeeded", "already exists"))
    delivery_id = before.store.finish_run("r", "completed", "结果", OutboundIntent(ADDRESS, "结果"))
    await before.close()
    after = runtime(path, [])
    try:
        assert after.coordinator.recover()["active_contexts"] == []
        await after.delivery.drain_prepared()
        await after.delivery.drain_prepared()
        assert not after.model.requests and len(after.qq.calls) == 1
        assert after.store.get_effect(effect)["status"] == "succeeded"
        assert after.store.get_delivery(delivery_id)["status"] == "acknowledged"
    finally:
        await after.close()


@pytest.mark.asyncio
async def test_crash_recovery_keeps_unknown_block_and_paused_context(tmp_path: Path):
    path = tmp_path / "state.db"
    before = runtime(path, [])
    initial = event("request")
    input_id, _ = before.store.ingest(initial)
    before.store.create_context("c", initial, AccessContext("101", ADDRESS.key))
    before.store.attach_input("c", input_id)
    before.store.claim_run("c", "r")
    effect = before.store.begin_effect("r", "call", "perform", {}, ["resource:one"], 1)
    before.store.control("c", "101", "stop")
    await before.close()
    after = runtime(path, [])
    try:
        report = after.coordinator.recover()
        assert report["interrupted_runs"] == ["r"] and not report["active_contexts"]
        assert after.store.get_effect(effect)["status"] == "unknown"
        after.store.control("c", "101", "resume")
        after.store.claim_run("c", "r2")
        with pytest.raises(StateConflict):
            after.store.begin_effect("r2", "new-id", "perform", {}, ["resource:one"], 1)
        assert not after.model.requests and not after.qq.calls
    finally:
        await after.close()


@pytest.mark.asyncio
async def test_inquiry_wait_and_real_answer_continue_same_context(tmp_path: Path):
    rt = runtime(tmp_path / "state.db", [])

    async def wait_for_question(_messages: Messages) -> NormalizedResponse:
        pending = rt.store.list_open_inquiries()
        assert len(pending) == 1
        return tool_response("wait_for_input", {"inquiry_id": pending[0]["inquiry_id"]}, "wait")

    rt.model.steps = [
        tool_response(
            "ask_person",
            {"recipient_id": "202", "kind": "group", "target_id": "10", "question": "请补充信息"},
            "ask",
        ),
        wait_for_question,
        final("已根据回复继续"),
    ]
    try:
        first = await rt.coordinator.accept(event("需要另一位成员提供信息"))
        await idle(rt)
        assert len(rt.qq.calls) == 1 and len(rt.model.requests) == 2
        pending = rt.store.list_open_inquiries(first.context_id)
        assert len(pending) == 1
        second = await rt.coordinator.accept(
            event("真实补充", author="202", mid="answer", reply="1001", mentioned=False)
        )
        await idle(rt)
        assert second.context_id == first.context_id
        assert rt.store.get_inquiry(pending[0]["inquiry_id"])["status"] == "answered"
        assert len(rt.qq.calls) == 2 and len(rt.model.requests) == 3
        assert any(
            "作者QQ=202" in str(m.get("content")) and "真实补充" in str(m.get("content"))
            for m in rt.model.requests[-1]
        )
        assert not rt.coordinator.failures
    finally:
        await rt.close()


@pytest.mark.asyncio
async def test_compacted_covered_inputs_are_not_appended_again(tmp_path: Path):
    rt = runtime(tmp_path / "state.db", [final("已记录")])
    try:
        accepted = await rt.coordinator.accept(event("原始请求应由摘要覆盖"))
        await idle(rt)
        assert accepted.context_id
        current = rt.store.get_context(accepted.context_id)
        epoch = rt.store.claim_run(accepted.context_id, "compaction-check")
        request = RunRequest(
            "compaction-check", accepted.context_id, ADDRESS, current["access"], epoch
        )
        builder = ContextBuilder(rt.store)
        basis = await builder.build(request)
        assert accepted.input_id in basis.source_ids
        candidate = [
            {
                "role": "user",
                "content": "保留来源的摘要",
                "_audience": ADDRESS.key,
                "_covered_input_ids": list(basis.source_ids),
            }
        ]
        assert builder.commit_compaction(request, basis, candidate)
        rebuilt = await builder.build(request)
        assert rt.store.history(accepted.context_id) == candidate
        assert accepted.input_id in rebuilt.source_ids
        assert len(rt.model.requests) == 1 and len(rt.qq.calls) == 1
        rt.store.finish_run("compaction-check", "yielded")
    finally:
        await rt.close()
