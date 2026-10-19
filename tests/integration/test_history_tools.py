"""Durable source retrieval, visibility filtering and real application grants."""

import asyncio
import json
from dataclasses import replace

import pytest
from websockets.asyncio.client import connect

from shittim_core.app.config import Settings
from shittim_core.app.service import Application
from shittim_core.context.builder import ContextBuilder
from shittim_core.domain.model import NormalizedResponse, ToolCall
from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    ExecutionContext,
    InputEvent,
    OutboundIntent,
    ProtocolReceipt,
    RunRequest,
)
from shittim_core.state import StateStore
from shittim_core.tools.executor import ToolExecutor
from shittim_core.tools.history import (
    HISTORY_TOOLS,
    HistoryView,
    history_resource,
    register_history_tools,
)


@pytest.fixture
def setup(tmp_path):
    store = StateStore(tmp_path / "history.db")
    address = ConversationAddress("123", "group", "456")
    first = InputEvent(address, "1001", "1", "原始关键词：甲乙丙" + "长内容" * 1000)
    second = InputEvent(address, "1002", "2", "另一个成员补充资料")
    private = InputEvent(
        ConversationAddress("123", "private", "1002"), "1002", "3", "PRIVATE_SECRET"
    )
    access = AccessContext("1001", address.key, HISTORY_TOOLS, frozenset({history_resource("ctx")}))
    store.create_context("ctx", first, access)
    ids = []
    for event in (first, second, private):
        identity, _ = store.ingest(event)
        ids.append(identity)
        store.attach_input("ctx", identity)
    other = InputEvent(address, "1001", "4", "OTHER_CONTEXT_SECRET")
    oid, _ = store.ingest(other)
    store.create_context("other", other, access)
    store.attach_input("other", oid)
    epoch = store.claim_run("ctx", "run")
    ctx = ExecutionContext(RunRequest("run", "ctx", address, access, epoch), 3, 0)
    policy = {"allowed": True}
    tools = ToolExecutor(store)
    register_history_tools(tools, store, policy_check=lambda context: policy["allowed"])
    count = 0

    async def invoke(name, args, context=None):
        nonlocal count
        count += 1
        return await tools.execute(
            ToolCall(f"call-{count}", name, json.dumps(args)), context or ctx
        )

    yield store, ctx, policy, invoke, first, ids
    store.close()


def data(result):
    assert result.status == "succeeded", result.content
    return json.loads(result.content)


async def test_original_input_full_pagination_after_compression(setup):
    store, ctx, _, invoke, first, ids = setup
    builder = ContextBuilder(store)
    basis = await builder.build(ctx.run)
    assert builder.commit_compaction(
        ctx.run,
        basis,
        [
            {
                "role": "user",
                "content": "精简摘要",
                "_covered_input_ids": ids[:2],
                "_audience": ctx.run.access.audience,
            }
        ],
    )
    assert "原始关键词" not in json.dumps(store.history("ctx"), ensure_ascii=False)
    search = data(await invoke("history_search", {"query": "原始关键词"}))
    assert search["total_matches"] == 1
    hit = search["matches"][0]
    assert hit["source_refs"] == [f"input:{ids[0]}"]
    assert hit["author_id"] == "1001" and hit["message_id"] == "1"
    pages, offset = [], 0
    while offset is not None:
        read = data(
            await invoke(
                "history_read",
                {
                    "source_ref": hit["source_ref"],
                    "offset": offset,
                    "limit": 256,
                    "snapshot_key": search["snapshot_key"],
                },
            )
        )
        pages.append(read["text"])
        offset = read["next_offset"]
    assert "".join(pages) == first.text


async def test_excludes_private_other_context_system_reasoning_and_tool_payload(setup):
    store, _, _, invoke, _, ids = setup
    store.append_history("run", {"role": "system", "content": "SYSTEM_SECRET"})
    store.append_history(
        "run",
        {
            "role": "assistant",
            "content": "UNPUBLISHED_SECRET",
            "reasoning_content": "REASONING_SECRET",
        },
    )
    store.append_history(
        "run", {"role": "tool", "tool_call_id": "hidden", "content": "TOOL_SECRET"}
    )
    result = data(await invoke("history_search", {"query": ""}))
    encoded = json.dumps(result)
    assert result["total_matches"] == 2
    for secret in (
        "PRIVATE_SECRET",
        "OTHER_CONTEXT_SECRET",
        "SYSTEM_SECRET",
        "REASONING_SECRET",
        "TOOL_SECRET",
        "UNPUBLISHED_SECRET",
    ):
        assert secret not in encoded
    for ref in (f"input:{ids[2]}", "input:arbitrary-private-id"):
        assert (await invoke("history_read", {"source_ref": ref})).status == "not_executed"


async def test_only_acknowledged_visible_messages_no_progress_or_internal_receipt(setup):
    store, ctx, _, invoke, _, ids = setup

    def prepare(text, purpose="final"):
        return store.prepare_delivery(
            "run", OutboundIntent(ctx.run.address, text, purpose=purpose, reply_to="1"), 3
        )

    sent = prepare("可见答复")
    store.claim_delivery(sent)
    store.finish_delivery(
        sent, ProtocolReceipt("acknowledged", data={"debug": "RECEIPT_SECRET"}, message_id="100")
    )
    prepare("尚未发送", "message")
    progress = prepare("处理中", "progress")
    store.claim_delivery(progress)
    store.finish_delivery(progress, ProtocolReceipt("acknowledged", message_id="101"))
    found = data(await invoke("history_search", {"query": "", "role": "assistant"}))
    assert found["total_matches"] == 1
    read = data(await invoke("history_read", {"source_ref": f"delivery:{sent}"}))
    assert read["text"] == "可见答复" and read["source_refs"] == [f"input:{ids[0]}"]
    assert "RECEIPT_SECRET" not in json.dumps(read)


async def test_history_preserves_long_reply_parts_in_confirmed_send_order(setup):
    store, ctx, _, invoke, _, _ = setup
    parts = [str(index) * 2 for index in range(1, 10)]
    deliveries = store.prepare_delivery_items(
        "run", [OutboundIntent(ctx.run.address, text, "message") for text in parts], ctx.input_seq
    )
    for index, identity in enumerate(deliveries):
        assert store.claim_delivery(identity) is not None
        store.finish_delivery(
            identity, ProtocolReceipt("acknowledged", message_id=str(8000 + index))
        )
    values, refs, offset, snapshot = [], [], 0, None
    while offset is not None:
        args = {"query": "", "role": "assistant", "limit": 2, "offset": offset}
        if snapshot:
            args["snapshot_key"] = snapshot
        page = data(await invoke("history_search", args))
        values.extend(match["snippet"] for match in page["matches"])
        refs.extend(match["source_ref"] for match in page["matches"])
        offset, snapshot = page["next_offset"], page["snapshot_key"]
    assert values == parts
    assert refs == ["delivery:" + identity for identity in deliveries]


async def test_history_uses_receipt_order_not_preparation_or_input_sequence(setup):
    store, ctx, _, invoke, _, _ = setup
    first = store.prepare_delivery(
        "run", OutboundIntent(ctx.run.address, "prepared-first-ack-last", "message"), 3
    )
    assert store.claim_delivery(first)
    # A second input arrives while the first actual send is still awaiting receipt.
    identity, _ = store.ingest(InputEvent(ctx.run.address, "1001", "new", "新的输入"))
    seq = store.attach_input("ctx", identity)
    current = replace(ctx, input_seq=seq)
    second = store.prepare_delivery(
        "run", OutboundIntent(ctx.run.address, "prepared-second-ack-first", "message"), seq
    )
    assert store.claim_delivery(second)
    store.finish_delivery(second, ProtocolReceipt("acknowledged", message_id="9001"))
    store.finish_delivery(first, ProtocolReceipt("acknowledged", message_id="9002"))
    # Same conversation address, different work context must not enter this view.
    store.claim_run("other", "other-run")
    hidden = store.prepare_delivery(
        "other-run", OutboundIntent(ctx.run.address, "OTHER_CONTEXT_REPLY", "message"), 1
    )
    assert store.claim_delivery(hidden)
    store.finish_delivery(hidden, ProtocolReceipt("acknowledged", message_id="9003"))
    page = data(await invoke("history_search", {"query": "", "role": "assistant"}, current))
    assert [hit["snippet"] for hit in page["matches"]] == [
        "prepared-second-ack-first",
        "prepared-first-ack-last",
    ]
    assert [hit["source_ref"] for hit in page["matches"]] == [
        "delivery:" + second,
        "delivery:" + first,
    ]
    assert all(hit["delivery_order_basis"] == "confirmed_receipt" for hit in page["matches"])
    assert "OTHER_CONTEXT_REPLY" not in json.dumps(page)
    # The ordering evidence survives opening the persisted SQLite again.
    reopened = StateStore(store.path)
    try:
        persisted = HistoryView(reopened).search({"query": "", "role": "assistant"}, current)
        assert [hit["source_ref"] for hit in persisted["matches"]] == [
            "delivery:" + second,
            "delivery:" + first,
        ]
        assert persisted["snapshot_key"] == page["snapshot_key"]
    finally:
        reopened.close()


async def test_ack_without_message_identity_does_not_claim_known_send_order(setup):
    store, ctx, _, invoke, _, _ = setup
    identity = store.prepare_delivery(
        "run", OutboundIntent(ctx.run.address, "receipt-without-id", "message"), 3
    )
    assert store.claim_delivery(identity)
    store.finish_delivery(identity, ProtocolReceipt("acknowledged"))
    page = data(await invoke("history_search", {"query": "", "role": "assistant"}))
    assert page["matches"][0]["source_ref"] == "delivery:" + identity
    assert page["matches"][0]["delivery_order_basis"] == "unavailable"


async def test_policy_revocation_and_policy_version_rechecked(setup):
    _, ctx, policy, invoke, _, ids = setup
    assert data(await invoke("history_search", {"query": "资料"}))["total_matches"] == 1
    policy["allowed"] = False
    assert (
        await invoke("history_read", {"source_ref": f"input:{ids[0]}"})
    ).status == "not_executed"
    policy["allowed"] = True
    changed = replace(ctx, run=replace(ctx.run, access=replace(ctx.run.access, policy_version=2)))
    assert (await invoke("history_search", {"query": ""}, changed)).status == "not_executed"


async def test_scope_tool_and_resource_grants_are_independent(setup):
    _, ctx, _, invoke, _, _ = setup
    for access in (
        replace(ctx.run.access, allowed_tools=frozenset()),
        replace(ctx.run.access, allowed_resources=frozenset()),
        replace(ctx.run.access, actor_id="1002"),
        replace(ctx.run.access, audience="different"),
    ):
        denied = replace(ctx, run=replace(ctx.run, access=access))
        assert (await invoke("history_search", {"query": ""}, denied)).status == "not_executed"


async def test_history_cannot_rebind_current_run_to_another_context(setup):
    store, ctx, _, invoke, _, _ = setup
    event = InputEvent(
        ctx.run.address, ctx.run.access.actor_id, "other-owned-message", "OTHER_OWNED_SECRET"
    )
    other_access = replace(
        ctx.run.access, allowed_resources=frozenset({history_resource("other-owned")})
    )
    store.create_context("other-owned", event, other_access)
    identity, _ = store.ingest(event)
    store.attach_input("other-owned", identity)
    forged = replace(ctx, run=replace(ctx.run, context_id="other-owned", access=other_access))
    denied = await invoke("history_search", {"query": ""}, forged)
    assert denied.status == "not_executed" and "OTHER_OWNED_SECRET" not in denied.content


async def test_search_pagination_snapshot_and_unknown_reference(setup):
    _, _, _, invoke, _, _ = setup
    page = data(await invoke("history_search", {"query": "", "limit": 1}))
    assert page["next_offset"] == 1
    next_page = data(
        await invoke(
            "history_search",
            {"query": "", "offset": 1, "limit": 1, "snapshot_key": page["snapshot_key"]},
        )
    )
    assert next_page["next_offset"] is None
    assert page["matches"][0]["source_ref"] != next_page["matches"][0]["source_ref"]
    assert (
        await invoke("history_search", {"query": "", "snapshot_key": "0" * 64})
    ).status == "not_executed"


async def test_application_history_grants_and_progress_wire_integration(tmp_path):
    class Model:
        count = 0

        async def complete(self, messages, tools):
            self.count += 1
            assert {t["function"]["name"] for t in tools} == HISTORY_TOOLS
            if self.count == 1:
                return NormalizedResponse(
                    None,
                    [ToolCall("search", "history_search", '{"query":"资料关键词"}')],
                    "tool_calls",
                )
            if self.count == 2:
                result = json.loads([m["content"] for m in messages if m["role"] == "tool"][-1])
                found = json.loads(result["content"])["matches"][0]["source_ref"]
                return NormalizedResponse(
                    None,
                    [ToolCall("read", "history_read", json.dumps({"source_ref": found}))],
                    "tool_calls",
                )
            return NormalizedResponse("历史读取完成", None, "stop")

    settings = Settings(
        "123",
        "TOKEN",
        "model",
        "https://unused.invalid/v1",
        "KEY",
        tmp_path / "data",
        port=0,
        allowed_users=frozenset({"456"}),
        allowed_tools=HISTORY_TOOLS,
        progress_enabled=True,
        progress_min_interval_seconds=0,
    )
    app = Application(settings, model=Model(), environ={"TOKEN": "synthetic-test-token"})
    port = await app.start()
    try:
        async with connect(
            f"ws://127.0.0.1:{port}/onebot",
            additional_headers={"Authorization": "Bearer synthetic-test-token", "X-Self-ID": "123"},
        ) as socket:
            await socket.send(
                json.dumps(
                    {
                        "post_type": "message",
                        "message_type": "private",
                        "self_id": 123,
                        "user_id": 456,
                        "message_id": 1,
                        "message": [{"type": "text", "data": {"text": "资料关键词：请查阅"}}],
                    }
                )
            )
            observed = []
            while True:
                packet = json.loads(await asyncio.wait_for(socket.recv(), 10))
                text = "".join(
                    item["data"].get("text", "")
                    for item in packet["params"]["message"]
                    if item["type"] == "text"
                )
                observed.append(text)
                await socket.send(
                    json.dumps(
                        {
                            "echo": packet["echo"],
                            "status": "ok",
                            "retcode": 0,
                            "data": {"message_id": 100 + len(observed)},
                        }
                    )
                )
                if text == "历史读取完成":
                    break
            await app.coordinator.wait_idle()
            assert any(text != "历史读取完成" for text in observed)
            context = app.store.list_contexts()[0]
            assert history_resource(context["context_id"]) in context["access"].allowed_resources
            assert app.context_allowed(context)
            assert all(row["status"] == "succeeded" for row in app.store.list_effects())
            assert not app.progress.failures
    finally:
        assert (await app.close()).released
