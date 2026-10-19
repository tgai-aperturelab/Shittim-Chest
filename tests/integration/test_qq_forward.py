"""Merged forwards through the sole durable delivery path; no live QQ sends."""

import asyncio
import json
from dataclasses import replace

import pytest
from websockets.asyncio.client import connect

from shittim_core.delivery.service import Delivery
from shittim_core.domain.model import ToolCall
from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    ExecutionContext,
    InputEvent,
    OutboundIntent,
    ProtocolReceipt,
    RunRequest,
    Segment,
)
from shittim_core.qq.transport import QQTransport
from shittim_core.state import StateStore
from shittim_core.tools.collaboration import register_collaboration_tools
from shittim_core.tools.executor import ToolExecutor
from shittim_core.tools.qq_forward import (
    QQ_FORWARD_TOOLS,
    forward_resource,
    register_qq_forward_tools,
)


def setup_context(tmp_path, kind="private"):
    store = StateStore(tmp_path / "state.sqlite")
    address = ConversationAddress("123", kind, "456")
    access = AccessContext(
        "456",
        address.key,
        QQ_FORWARD_TOOLS,
        frozenset({forward_resource("ctx"), "qq:" + address.key}),
    )
    first = InputEvent(
        address, "456", "101", "first", (Segment("forward", {"id": "container-uuid"}),)
    )
    second = InputEvent(address, "789" if kind == "group" else "456", "102", "second")
    input_id, _ = store.ingest(first)
    store.create_context("ctx", first, access)
    store.attach_input("ctx", input_id)
    second_id, _ = store.ingest(second)
    store.attach_input("ctx", second_id)
    epoch = store.claim_run("ctx", "run")
    ctx = ExecutionContext(RunRequest("run", "ctx", address, access, epoch), 2, 0)
    return store, ctx


async def execute(tools, name, args, ctx, call_id="c1"):
    return await tools.execute(ToolCall(call_id, name, json.dumps(args)), ctx)


@pytest.mark.parametrize("kind", ["private", "group"])
async def test_existing_messages_forward_without_spoofed_authors(tmp_path, kind):
    store, ctx = setup_context(tmp_path, kind)
    calls = []

    async def call(action, params, attempt):
        # Intent and attempt are persisted before the actual OneBot call.
        assert store.get_delivery(attempt)["status"] == "attempting"
        calls.append((action, params))
        return ProtocolReceipt("acknowledged", message_id="301")

    try:
        tools = ToolExecutor(store)
        delivery = Delivery(store, call)
        register_qq_forward_tools(tools, store, call, delivery)
        result = await execute(tools, "send_forward", {"message_ids": ["101", "102"]}, ctx)
        assert result.status == "succeeded", result.content
        assert calls == [
            (
                f"send_{kind}_forward_msg",
                {
                    "self_id": "123",
                    "group_id" if kind == "group" else "user_id": 456,
                    "messages": [
                        {"type": "node", "data": {"id": "101"}},
                        {"type": "node", "data": {"id": "102"}},
                    ],
                },
            )
        ]
        assert store.list_deliveries()[0]["status"] == "acknowledged"
        assert store.list_effects()[0]["status"] == "succeeded"
        assert store.resolve_sent(ctx.run.address, "301")["context_id"] == "ctx"
    finally:
        store.close()


async def test_read_container_paging_authorship_and_nested_id_only(tmp_path):
    store, ctx = setup_context(tmp_path)
    calls = []

    async def call(action, params, attempt):
        calls.append((action, params))
        return ProtocolReceipt(
            "acknowledged",
            data={
                "messages": [
                    {
                        "message_id": "901",
                        "sender": {"user_id": 1, "nickname": "Claimed author"},
                        "time": 42,
                        "content": [
                            {"type": "text", "data": {"text": "source content"}},
                            {"type": "forward", "data": {"id": "nested-uuid"}},
                        ],
                    },
                    {
                        "message_id": "902",
                        "sender": {"user_id": 2},
                        "content": [{"type": "text", "data": {"text": "second node"}}],
                    },
                ]
            },
        )

    try:
        tools = ToolExecutor(store)
        register_qq_forward_tools(tools, store, call, Delivery(store, call))
        first = await execute(tools, "read_forward", {"message_id": "101", "limit": 1}, ctx)
        data = json.loads(first.content)
        assert first.status == "succeeded"
        assert data["source_author_id"] == "456" and data["total"] == 2 and data["next_offset"] == 1
        assert data["messages"][0]["author_attribution"] == "forward_payload_claim"
        assert data["messages"][0]["segments"] == [
            {"type": "forward", "id": "nested-uuid", "expanded": False}
        ]
        second = await execute(
            tools, "read_forward", {"message_id": "101", "offset": 1, "limit": 1}, ctx, "c2"
        )
        assert json.loads(second.content)["messages"][0]["text"] == "second node"
        assert json.loads(second.content)["response_sha256"] == data["response_sha256"]
        assert (
            calls == [("get_forward_msg", {"message_id": "container-uuid", "self_id": "123"})] * 2
        )
        # An inner message ID in a forward response is not a source capability.
        forbidden = await execute(tools, "send_forward", {"message_ids": ["901"]}, ctx, "c3")
        assert forbidden.status == "not_executed" and len(calls) == 2
    finally:
        store.close()


@pytest.mark.parametrize(
    "name,args",
    [
        ("read_forward", {"message_id": "999"}),
        ("read_forward", {"message_id": "101", "container_id": "unseen"}),
        ("send_forward", {"message_ids": ["101"], "author_id": "forged"}),
        ("send_forward", {"message_ids": ["999"]}),
    ],
)
async def test_unknown_source_and_extra_authority_rejected(tmp_path, name, args):
    store, ctx = setup_context(tmp_path)

    async def forbidden(*args):
        pytest.fail("Unauthorized action reached transport")

    try:
        tools = ToolExecutor(store)
        register_qq_forward_tools(tools, store, forbidden, Delivery(store, forbidden))
        result = await execute(tools, name, args, ctx)
        assert result.status == "not_executed" and not store.list_deliveries()
    finally:
        store.close()


async def test_same_group_other_context_source_is_not_visible(tmp_path):
    store, ctx = setup_context(tmp_path, "group")
    other = InputEvent(ctx.run.address, "789", "999", "another request")
    other_id, _ = store.ingest(other)
    store.create_context("other", other, ctx.run.access)
    store.attach_input("other", other_id)

    async def forbidden(*args):
        pytest.fail("Another context reached transport")

    try:
        tools = ToolExecutor(store)
        register_qq_forward_tools(tools, store, forbidden, Delivery(store, forbidden))
        result = await execute(tools, "send_forward", {"message_ids": ["999"]}, ctx)
        assert result.status == "not_executed"
    finally:
        store.close()


async def test_unknown_delivery_blocks_new_call_and_survives_restart(tmp_path):
    store, ctx = setup_context(tmp_path)
    calls = []

    async def lost(action, params, attempt):
        calls.append(attempt)
        return ProtocolReceipt("unknown", error="lost_response")

    tools = ToolExecutor(store)
    register_qq_forward_tools(tools, store, lost, Delivery(store, lost))
    result = await execute(tools, "send_forward", {"message_ids": ["101"]}, ctx)
    assert result.status == "unknown" and store.list_deliveries()[0]["status"] == "unknown"
    retry = await execute(tools, "send_forward", {"message_ids": ["102"]}, ctx, "different")
    assert retry.status == "not_executed" and len(calls) == 1
    store.close()
    reopened = StateStore(tmp_path / "state.sqlite")
    try:
        await Delivery(reopened, lost).drain_prepared()
        assert len(calls) == 1 and reopened.list_deliveries()[0]["intent"].forward_message_ids == (
            "101",
        )
    finally:
        reopened.close()


async def test_prepared_forward_recovery_and_legacy_intent(tmp_path):
    store, ctx = setup_context(tmp_path)
    intent = OutboundIntent(ctx.run.address, "", "message", forward_message_ids=("101", "102"))
    delivery_id = store.prepare_delivery(ctx.run.run_id, intent, ctx.input_seq)
    store.close()
    reopened = StateStore(tmp_path / "state.sqlite")
    calls = []

    async def send(action, params, attempt):
        calls.append(action)
        return ProtocolReceipt("acknowledged", message_id=str(555 + len(calls)))

    try:
        await Delivery(reopened, send).drain_prepared()
        assert calls == ["send_private_forward_msg"]
        assert reopened.get_delivery(delivery_id)["status"] == "acknowledged"
        legacy = reopened.prepare_delivery(
            ctx.run.run_id,
            OutboundIntent(ctx.run.address, "old plain text", "message"),
            ctx.input_seq,
        )
        raw = json.loads(
            reopened._db.execute(
                "SELECT intent FROM deliveries WHERE delivery_id=?", (legacy,)
            ).fetchone()[0]
        )
        raw.pop("forward_message_ids")
        reopened._db.execute(
            "UPDATE deliveries SET intent=? WHERE delivery_id=?", (json.dumps(raw), legacy)
        )
        assert reopened.get_delivery(legacy)["intent"].forward_message_ids == ()
        await Delivery(reopened, send).send(legacy)
        assert calls[-1] == "send_private_msg"
    finally:
        reopened.close()


async def test_acknowledged_bot_message_can_be_forwarded(tmp_path):
    store, ctx = setup_context(tmp_path)
    calls = []

    async def send(action, params, attempt):
        calls.append((action, params))
        return ProtocolReceipt("acknowledged", message_id=str(300 + len(calls)))

    try:
        delivery = Delivery(store, send)
        await delivery.submit(OutboundIntent(ctx.run.address, "bot message", "message"), ctx)
        tools = ToolExecutor(store)
        register_qq_forward_tools(tools, store, send, delivery)
        result = await execute(tools, "send_forward", {"message_ids": ["301"]}, ctx)
        assert result.status == "succeeded"
        assert calls[-1][1]["messages"] == [{"type": "node", "data": {"id": "301"}}]
    finally:
        store.close()


def test_store_rejects_cross_destination_even_if_direct_intent_constructed(tmp_path):
    store, ctx = setup_context(tmp_path)
    try:
        intent = OutboundIntent(
            ConversationAddress("123", "private", "789"),
            "",
            "message",
            forward_message_ids=("101",),
        )
        with pytest.raises(PermissionError):
            store.prepare_delivery("run", intent, 2)
        assert not store.list_deliveries()
    finally:
        store.close()


async def test_prepared_forward_is_revalidated_before_dispatch(tmp_path):
    store, ctx = setup_context(tmp_path)
    intent = OutboundIntent(ctx.run.address, "", "message", forward_message_ids=("101",))
    delivery_id = store.prepare_delivery("run", intent, 2)
    # Simulate a stale/corrupted persisted reference, not an authorized new input.
    raw = json.loads(
        store._db.execute(
            "SELECT intent FROM deliveries WHERE delivery_id=?", (delivery_id,)
        ).fetchone()[0]
    )
    raw["forward_message_ids"] = ["999"]
    store._db.execute(
        "UPDATE deliveries SET intent=? WHERE delivery_id=?", (json.dumps(raw), delivery_id)
    )

    async def forbidden(*args):
        pytest.fail("Revalidation failed to stop invalid reference")

    try:
        receipt = await Delivery(store, forbidden).send(delivery_id)
        assert receipt.status == "failed"
        assert store.get_delivery(delivery_id)["status"] == "failed"
    finally:
        store.close()


async def test_forward_fetch_failure_preserves_unavailable_status(tmp_path):
    store, ctx = setup_context(tmp_path)

    async def failed(*args):
        return ProtocolReceipt("failed", error="container expired")

    try:
        tools = ToolExecutor(store)
        register_qq_forward_tools(tools, store, failed, Delivery(store, failed))
        result = await execute(tools, "read_forward", {"message_id": "101"}, ctx)
        assert result.status == "failed"
        assert json.loads(result.content) == {"status": "unavailable", "receipt_status": "failed"}
        assert not store.list_deliveries()
    finally:
        store.close()


@pytest.mark.parametrize("first", ["send_forward", "ask_person"])
async def test_unknown_qq_send_cannot_be_bypassed_with_other_tool(tmp_path, first):
    store, ctx = setup_context(tmp_path)
    ctx = replace(
        ctx,
        run=replace(
            ctx.run,
            access=replace(
                ctx.run.access, allowed_tools=ctx.run.access.allowed_tools | {"ask_person"}
            ),
        ),
    )
    calls = []

    async def lost(action, params, attempt):
        calls.append(action)
        return ProtocolReceipt("unknown", error="missing_acknowledgement")

    try:
        tools = ToolExecutor(store)
        delivery = Delivery(store, lost)
        register_qq_forward_tools(tools, store, lost, delivery)
        register_collaboration_tools(tools, store, delivery)
        args = {
            "send_forward": {"message_ids": ["101"]},
            "ask_person": {
                "recipient_id": "456",
                "kind": "private",
                "target_id": "456",
                "question": "Please clarify",
            },
        }
        result = await execute(tools, first, args[first], ctx)
        assert result.status == "unknown"
        second = "ask_person" if first == "send_forward" else "send_forward"
        retry = await execute(tools, second, args[second], ctx, "different-call")
        assert retry.status == "not_executed"
        assert len(calls) == 1 and len(store.list_deliveries()) == 1
    finally:
        store.close()


async def test_forward_pages_use_distinct_real_websocket_attempts(tmp_path):
    store, ctx = setup_context(tmp_path)

    async def ignore_event(event):
        return None

    transport = QQTransport("123", "fixture-token", ignore_event, timeout_seconds=1)
    port = await transport.start()
    calls = []
    peer = None
    try:
        async with connect(
            f"ws://127.0.0.1:{port}/onebot",
            additional_headers={"Authorization": "Bearer fixture-token", "X-Self-ID": "123"},
        ) as socket:
            await transport.ready.wait()

            async def serve():
                async for raw in socket:
                    request = json.loads(raw)
                    calls.append(request)
                    await socket.send(
                        json.dumps(
                            {
                                "echo": request["echo"],
                                "status": "ok",
                                "retcode": 0,
                                "data": {
                                    "messages": [
                                        {
                                            "sender": {"user_id": 7},
                                            "content": [
                                                {"type": "text", "data": {"text": f"page-{index}"}}
                                            ],
                                        }
                                        for index in range(2)
                                    ]
                                },
                            }
                        )
                    )

            peer = asyncio.create_task(serve())
            tools = ToolExecutor(store)
            register_qq_forward_tools(tools, store, transport.call, Delivery(store, transport.call))
            first = await execute(
                tools, "read_forward", {"message_id": "101", "offset": 0, "limit": 1}, ctx, "page0"
            )
            second = await execute(
                tools, "read_forward", {"message_id": "101", "offset": 1, "limit": 1}, ctx, "page1"
            )
            assert first.status == "succeeded"
            assert second.status == "succeeded", second.content
            assert json.loads(second.content)["messages"][0]["text"] == "page-1"
            assert len(calls) == 2 and len(transport._attempts) == 2
            assert {effect["call_id"] for effect in store.list_effects()} == {"page0", "page1"}
            duplicate = await execute(
                tools, "read_forward", {"message_id": "101", "offset": 1, "limit": 1}, ctx, "page1"
            )
            assert duplicate.status == "not_executed"
            assert len(calls) == 2
    finally:
        if peer is not None:
            peer.cancel()
            await asyncio.gather(peer, return_exceptions=True)
        await transport.close()
        store.close()
