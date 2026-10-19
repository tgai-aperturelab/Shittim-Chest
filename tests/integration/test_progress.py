"""Phase notices are bounded, truthful outbox records rather than model narration."""

import json
from dataclasses import replace

import pytest

from shittim_core.app.progress import ProgressConfig, ProgressReporter
from shittim_core.app.run_ports import RuntimePorts
from shittim_core.context.builder import ContextBuilder
from shittim_core.delivery.service import Delivery
from shittim_core.domain.model import ToolCall
from shittim_core.domain.types import (
    AccessContext,
    ContextView,
    ConversationAddress,
    InputEvent,
    ProtocolReceipt,
    RunRequest,
    StateConflict,
    ToolResult,
)
from shittim_core.state import StateStore
from shittim_core.tools.executor import ToolExecutor, ToolSpec


def seed(tmp_path):
    store = StateStore(tmp_path / "state.sqlite")
    address = ConversationAddress("123", "private", "456")
    event = InputEvent(address, "456", "101", "process request")
    access = AccessContext(
        "456",
        address.key,
        frozenset({"read_material", "terminal", "mcp_SECRET_NAME"}),
        frozenset({"qq:" + address.key}),
    )
    input_id, _ = store.ingest(event)
    store.create_context("ctx", event, access)
    store.attach_input("ctx", input_id)
    epoch = store.claim_run("ctx", "run")
    request = RunRequest("run", "ctx", address, access, epoch)
    return store, request, ContextView((), 1, 0, 1, epoch)


async def test_disabled_progress_has_no_outbox_or_network(tmp_path):
    store, request, view = seed(tmp_path)

    async def forbidden(*args):
        pytest.fail("Disabled progress cannot send")

    try:
        reporter = ProgressReporter(store, Delivery(store, forbidden))
        await reporter.processing(request, view)
        await reporter.tool(request, ToolCall("c", "terminal", '{"command":"secret"}'), view)
        assert not store.list_deliveries()
    finally:
        store.close()


async def test_fixed_phase_names_rate_limit_and_no_secrets(tmp_path):
    store, request, view = seed(tmp_path)
    now, calls = [100.0], []

    async def sent(action, params, attempt):
        assert store.get_delivery(attempt)["intent"].purpose == "progress"
        assert store.get_delivery(attempt)["status"] == "attempting"
        calls.append(params["message"][0]["data"]["text"])
        return ProtocolReceipt("acknowledged", message_id=str(300 + len(calls)))

    try:
        reporter = ProgressReporter(
            store, Delivery(store, sent), config=ProgressConfig(True, 15), clock=lambda: now[0]
        )
        await reporter.processing(request, view)
        await reporter.tool(request, ToolCall("c1", "terminal", "SECRET_ARGUMENT"), view)
        assert calls == ["正在处理这项请求。"]
        now[0] += 16
        await reporter.tool(request, ToolCall("c1", "terminal", "SECRET_ARGUMENT"), view)
        now[0] += 16
        await reporter.tool(request, ToolCall("c2", "mcp_SECRET_NAME", "SECRET_REASONING"), view)
        assert calls == [
            "正在处理这项请求。",
            "正在处理：执行命令。",
            "正在处理：调用已配置的工具。",
        ]
        assert "SECRET" not in json.dumps(calls)
        assert len(store.list_deliveries(context_id="ctx")) == 3
        assert not store.list_deliveries(context_id="other")
    finally:
        store.close()


@pytest.mark.parametrize("change", ["stop", "epoch", "input"])
async def test_stopped_or_stale_progress_never_dispatches(tmp_path, change):
    store, request, view = seed(tmp_path)

    async def forbidden(*args):
        pytest.fail("Stale progress reached transport")

    try:
        reporter = ProgressReporter(
            store, Delivery(store, forbidden), config=ProgressConfig(True, 0)
        )
        if change == "stop":
            store.control("ctx", "456", "stop")
        elif change == "epoch":
            request = replace(request, control_epoch=request.control_epoch - 1)
        else:
            event = InputEvent(request.address, "456", "102", "new input")
            input_id, _ = store.ingest(event)
            store.attach_input("ctx", input_id)
        await reporter.processing(request, view)
        assert not store.list_deliveries()
    finally:
        store.close()


async def test_prepared_progress_for_finished_run_is_superseded(tmp_path):
    store, request, view = seed(tmp_path)

    async def forbidden(*args):
        pytest.fail("Finished-run phase reached transport")

    try:
        delivery_id = store.prepare_progress(
            "run", request.control_epoch, 1, "phase", "正在处理这项请求。", 100, 0
        )
        store.finish_run("run", "interrupted")
        await Delivery(store, forbidden).send(delivery_id)
        assert store.get_delivery(delivery_id)["status"] == "superseded"
    finally:
        store.close()


async def test_unknown_progress_never_retried_and_blocks_qq_bypass(tmp_path):
    store, request, view = seed(tmp_path)
    calls = []

    async def lost(action, params, attempt):
        calls.append(attempt)
        return ProtocolReceipt("unknown", error="lost ack")

    reporter = ProgressReporter(store, Delivery(store, lost), config=ProgressConfig(True, 0))
    await reporter.processing(request, view)
    await reporter.tool(request, ToolCall("c1", "terminal", ""), view)
    assert len(calls) == 1
    with pytest.raises(StateConflict, match="progress"):
        store.begin_effect("run", "send", "send_forward", {}, ("qq:" + request.address.key,), 1)
    # Unrelated work is not prevented by an unconfirmed progress message.
    effect = store.begin_effect("run", "file", "read_file", {}, ("files:ctx",), 1)
    store.finish_effect_with_history(effect, ToolResult("succeeded", "read"))
    store.close()
    reopened = StateStore(tmp_path / "state.sqlite")
    try:
        replacement = ProgressReporter(
            reopened, Delivery(reopened, lost), config=ProgressConfig(True, 0)
        )
        await replacement.tool(request, ToolCall("c2", "terminal", ""), view)
        await replacement.delivery.drain_prepared()
        assert len(calls) == 1 and reopened.list_deliveries()[0]["status"] == "unknown"
    finally:
        reopened.close()


async def test_existing_qq_unknown_effect_blocks_progress(tmp_path):
    store, request, view = seed(tmp_path)

    async def forbidden(*args):
        pytest.fail("Progress bypassed an unknown QQ operation")

    try:
        effect = store.begin_effect(
            "run", "old", "send_forward", {}, ("qq:" + request.address.key,), 1
        )
        store.finish_effect_with_history(effect, ToolResult("unknown", "lost ack"))
        reporter = ProgressReporter(
            store, Delivery(store, forbidden), config=ProgressConfig(True, 0)
        )
        await reporter.processing(request, view)
        assert not store.list_deliveries()
    finally:
        store.close()


async def test_new_context_does_not_bypass_unknown_progress_to_same_address(tmp_path):
    store, request, view = seed(tmp_path)
    calls = []

    async def lost(action, params, attempt):
        calls.append(attempt)
        return ProtocolReceipt("unknown", error="lost ack")

    try:
        reporter = ProgressReporter(store, Delivery(store, lost), config=ProgressConfig(True, 0))
        await reporter.processing(request, view)
        event = InputEvent(request.address, "456", "102", "new request")
        input_id, _ = store.ingest(event)
        store.create_context("other", event, request.access)
        store.attach_input("other", input_id)
        epoch = store.claim_run("other", "newrun")
        other = replace(request, run_id="newrun", context_id="other", control_epoch=epoch)
        await reporter.processing(other, replace(view, control_epoch=epoch))
        assert len(calls) == 1
        assert not store.list_deliveries(context_id="other")
    finally:
        store.close()


async def test_runtime_ports_emits_real_stage_without_model_content(tmp_path):
    store, request, _ = seed(tmp_path)
    events = []

    async def sent(action, params, attempt):
        events.append(params["message"][0]["data"]["text"])
        return ProtocolReceipt("acknowledged", message_id=str(300 + len(events)))

    async def handler(args, ctx):
        events.append("actual tool handler")
        return ToolResult("succeeded", "secret output")

    try:
        tools = ToolExecutor(store)
        tools.register(
            ToolSpec(
                "terminal",
                "secret external description",
                {"type": "object"},
                handler,
                lambda args, ctx: (),
            )
        )
        reporter = ProgressReporter(store, Delivery(store, sent), config=ProgressConfig(True, 0))
        ports = RuntimePorts(store, ContextBuilder(store), tools, progress=reporter)
        view = await ports.build_view(request)
        result = await ports.execute_tool(request, ToolCall("c1", "terminal", "{}"), view)
        assert result.status == "succeeded"
        assert events == ["正在处理这项请求。", "正在处理：执行命令。", "actual tool handler"]
    finally:
        store.close()
