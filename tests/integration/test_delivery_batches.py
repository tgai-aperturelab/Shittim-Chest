"""Atomic final-outbox plans and per-item effect evidence, with no actual QQ connection."""

import asyncio

import pytest

from shittim_core.delivery.service import Delivery, plain_text
from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    ExecutionContext,
    InputEvent,
    OutboundIntent,
    ProtocolReceipt,
    RunRequest,
    StateConflict,
)
from shittim_core.state import StateStore


@pytest.mark.parametrize(
    "body,expected",
    [
        ("```print(1)```", "print(1)"),
        ("```python\nprint(1)\n```", "print(1)"),
        ("前文\n~~~text\n保留内容\n~~~\n后文", "前文\n\n保留内容\n\n后文"),
    ],
)
def test_fence_removal_preserves_plain_content(body, expected):
    assert plain_text(body) == expected


async def test_shutdown_keeps_unsent_batch_tail_for_a_fresh_delivery_owner(tmp_path):
    store, source, _ = seeded(tmp_path)
    attempts = []

    async def call(_action, _params, attempt):
        attempts.append(attempt)
        delivery.stop_dispatch()
        return ProtocolReceipt("acknowledged", message_id=str(100 + len(attempts)))

    delivery = Delivery(store, call, text_limit=2)
    head = delivery.complete_run(
        "run", "completed", "abcdef", OutboundIntent(source.address, "abcdef")
    )
    await delivery.send(head)
    assert [row["status"] for row in store.list_delivery_items(head)] == [
        "acknowledged",
        "prepared",
        "prepared",
    ]
    assert len(attempts) == 1

    async def new_call(_action, _params, attempt):
        attempts.append(attempt)
        return ProtocolReceipt("acknowledged", message_id=str(100 + len(attempts)))

    restarted = Delivery(store, new_call, text_limit=2)
    store.recover()
    await restarted.drain_prepared()
    assert len(attempts) == len(set(attempts)) == 3
    assert all(row["status"] == "acknowledged" for row in store.list_delivery_items(head))
    store.close()


def seeded(tmp_path, *, kind="private"):
    store = StateStore(tmp_path / "state.db")
    address = ConversationAddress("123", kind, "456" if kind == "private" else "789")
    source = InputEvent(address, "456", "1", "question")
    input_id, _ = store.ingest(source)
    access = AccessContext("456", address.key)
    store.create_context("ctx", source, access)
    store.attach_input("ctx", input_id)
    epoch = store.claim_run("ctx", "run")
    store.consume_inputs("run", 1)
    return store, source, RunRequest("run", "ctx", address, access, epoch)


async def test_long_unicode_plain_text_preserves_every_character_and_receipt(tmp_path):
    store, source, _ = seeded(tmp_path)
    calls = []

    async def send(action, params, attempt):
        calls.append((action, params, attempt))
        return ProtocolReceipt("acknowledged", message_id=str(100 + len(calls)))

    delivery = Delivery(store, send, text_limit=5)
    raw = "**开头😀** 中间     \n    末尾😺"
    head = delivery.complete_run("run", "completed", raw, OutboundIntent(source.address, raw))
    assert head is not None
    items = store.list_delivery_items(head)
    expected = plain_text(raw)
    assert "".join(row["intent"].text for row in items) == expected
    assert len(items) > 1 and all(0 < len(row["intent"].text) <= 5 for row in items)
    assert store.get_run("run")["status"] == "completed"
    await delivery.send(head)
    sent_text = "".join(call[1]["message"][0]["data"]["text"] for call in calls)
    assert sent_text == expected
    assert len({call[2] for call in calls}) == len(items)
    assert all(row["status"] == "acknowledged" for row in store.list_delivery_items(head))
    for index, row in enumerate(store.list_delivery_items(head)):
        assert (
            store.resolve_sent(source.address, str(101 + index))["delivery_id"]
            == row["delivery_id"]
        )
    assert (
        delivery.complete_run("run", "completed", raw, OutboundIntent(source.address, raw)) == head
    )
    await delivery.send(head)
    assert len(calls) == len(items)
    store.close()


def test_all_items_and_outcome_roll_back_when_later_item_is_invalid(tmp_path):
    store, source, _ = seeded(tmp_path)
    with pytest.raises(ValueError):
        store.finish_run_items(
            "run",
            "completed",
            "result",
            [
                OutboundIntent(source.address, "valid"),
                OutboundIntent(source.address, "invalid mixed", artifact_refs=("artifact",)),
            ],
        )
    assert store.get_run("run")["status"] == "running"
    assert store.list_deliveries() == []
    assert store._db.execute("SELECT COUNT(*) FROM delivery_batches").fetchone()[0] == 0
    store.close()


@pytest.mark.parametrize("second_status", ["failed", "unknown"])
async def test_partial_failure_or_unknown_blocks_the_tail_even_after_restart(
    tmp_path, second_status
):
    store, source, _ = seeded(tmp_path)
    calls = []

    async def send(action, params, attempt):
        calls.append(attempt)
        return (
            ProtocolReceipt("acknowledged", message_id="100")
            if len(calls) == 1
            else ProtocolReceipt(second_status)
        )

    delivery = Delivery(store, send, text_limit=3)
    head = delivery.complete_run(
        "run", "completed", "abcdefghi", OutboundIntent(source.address, "abcdefghi")
    )
    await delivery.send(head)
    rows = store.list_delivery_items(head)
    assert [row["status"] for row in rows] == ["acknowledged", second_status, "prepared"]
    assert store.claim_delivery(rows[2]["delivery_id"]) is None
    store.close()
    reopened = StateStore(tmp_path / "state.db")
    reopened.recover()
    resumed = Delivery(reopened, send, text_limit=3)
    await resumed.drain_prepared()
    await resumed.send(rows[2]["delivery_id"])
    assert len(calls) == 2
    assert [row["status"] for row in reopened.list_delivery_items(head)] == [
        "acknowledged",
        second_status,
        "prepared",
    ]
    reopened.close()


async def test_recovery_of_acknowledged_prefix_sends_only_remaining_prepared_items(tmp_path):
    store, source, _ = seeded(tmp_path)
    head = store.finish_run_items(
        "run",
        "completed",
        "abcdef",
        [OutboundIntent(source.address, "abc"), OutboundIntent(source.address, "def")],
    )[0]
    store.claim_delivery(head)
    store.finish_delivery(head, ProtocolReceipt("acknowledged", message_id="100"))
    store.close()
    restored = StateStore(tmp_path / "state.db")
    restored.recover()
    calls = []

    async def send(action, params, attempt):
        calls.append(params["message"][0]["data"]["text"])
        return ProtocolReceipt("acknowledged", message_id="101")

    await Delivery(restored, send).drain_prepared()
    assert calls == ["def"]
    restored.close()


async def test_stop_after_first_ack_prevents_later_items(tmp_path):
    store, source, _ = seeded(tmp_path)
    calls = []

    async def send(action, params, attempt):
        calls.append(attempt)
        store.control("ctx", "456", "stop")
        return ProtocolReceipt("acknowledged", message_id="100")

    delivery = Delivery(store, send, text_limit=3)
    head = delivery.complete_run(
        "run", "completed", "abcdefghi", OutboundIntent(source.address, "abcdefghi")
    )
    await delivery.send(head)
    await delivery.drain_prepared()
    assert len(calls) == 1
    rows = store.list_delivery_items(head)
    assert rows[0]["status"] == "acknowledged"
    assert rows[1]["status"] == "superseded"
    assert all(row["status"] != "attempting" for row in rows)
    store.close()


async def test_concurrent_send_cannot_claim_same_item_or_send_tail_early(tmp_path):
    store, source, _ = seeded(tmp_path)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def send(action, params, attempt):
        calls.append(attempt)
        if len(calls) == 1:
            entered.set()
            await release.wait()
        return ProtocolReceipt("acknowledged", message_id=str(100 + len(calls)))

    delivery = Delivery(store, send, text_limit=3)
    head = delivery.complete_run(
        "run", "completed", "abcdef", OutboundIntent(source.address, "abcdef")
    )
    first = asyncio.create_task(delivery.send(head))
    await entered.wait()
    tail = store.list_delivery_items(head)[1]["delivery_id"]
    assert store.claim_delivery(tail) is None
    await delivery.send(head)
    assert len(calls) == 1
    release.set()
    await first
    assert len(calls) == 2 and len(set(calls)) == 2
    store.close()


async def test_submit_reports_batch_unknown_not_the_acknowledged_head(tmp_path):
    store, source, run = seeded(tmp_path)
    count = 0

    async def send(action, params, attempt):
        nonlocal count
        count += 1
        return (
            ProtocolReceipt("acknowledged", message_id="100")
            if count == 1
            else ProtocolReceipt("unknown")
        )

    delivery = Delivery(store, send, text_limit=3)
    result = await delivery.submit(
        OutboundIntent(source.address, "abcdef", purpose="message"), ExecutionContext(run, 1, 0)
    )
    assert result.status == "unknown"
    assert store.get_delivery(result.delivery_id)["status"] == "acknowledged"
    store.close()


async def test_ack_listeners_are_after_commit_and_cannot_downgrade_receipts(tmp_path):
    store, source, _ = seeded(tmp_path)
    calls, observed = [], []

    async def send(action, params, attempt):
        calls.append(attempt)
        return ProtocolReceipt("acknowledged", message_id=str(100 + len(calls)))

    async def broken_listener(delivery_id, receipt):
        observed.append(store.get_delivery(delivery_id)["status"])
        assert store.resolve_sent(source.address, receipt.message_id) is not None
        raise RuntimeError("private diagnostics must not be recorded")

    delivery = Delivery(store, send, text_limit=3)
    delivery.add_receipt_listener(broken_listener)
    head = delivery.complete_run(
        "run", "completed", "abcdef", OutboundIntent(source.address, "abcdef")
    )
    await delivery.send(head)
    assert observed == ["acknowledged", "acknowledged"]
    assert delivery.listener_failures == ["RuntimeError", "RuntimeError"]
    assert all(row["status"] == "acknowledged" for row in store.list_delivery_items(head))
    await delivery.send(head)
    assert len(calls) == 2 and len(observed) == 2
    store.close()


async def test_group_inquiry_mentions_recipient_and_keeps_question_single(tmp_path):
    store, source, _ = seeded(tmp_path, kind="group")
    question = "问" * 2000
    inquiry, delivery_id = store.create_inquiry("run", "999", source.address, question, 1)
    sent = []

    async def send(action, params, attempt):
        sent.append(params["message"])
        return ProtocolReceipt("acknowledged", message_id="88")

    await Delivery(store, send).send(delivery_id)
    assert len(sent) == 1
    assert sent[0][0] == {"type": "at", "data": {"qq": "999"}}
    assert sent[0][-1] == {"type": "text", "data": {"text": question}}
    assert store.resolve_sent(source.address, "88")["inquiry_id"] == inquiry
    store.close()


def test_terminal_reason_and_unresolved_effects_are_durable_and_idempotent(tmp_path):
    store, source, _ = seeded(tmp_path)

    async def never_send(action, params, attempt):
        pytest.fail("Completing a run must not perform network I/O")

    delivery = Delivery(store, never_send)
    head = delivery.complete_run(
        "run",
        "failed",
        "",
        OutboundIntent(source.address, "无法确认结果", purpose="message"),
        reason="effect_unknown",
        unresolved_effects=("external-check",),
    )
    outcome = store.get_run("run")["outcome"]
    assert outcome["reason"] == "effect_unknown" and outcome["unresolved_effects"] == [
        "external-check"
    ]
    assert store.get_run("run")["status"] == "failed" and head is not None
    assert (
        delivery.complete_run(
            "run",
            "failed",
            "",
            OutboundIntent(source.address, "无法确认结果", purpose="message"),
            reason="effect_unknown",
            unresolved_effects=("external-check",),
        )
        == head
    )
    with pytest.raises(StateConflict):
        delivery.complete_run("run", "failed", "", reason="different")
    store.close()
