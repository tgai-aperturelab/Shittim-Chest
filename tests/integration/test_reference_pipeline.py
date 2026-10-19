"""Real persisted QQ input -> bounded quoted-data enrichment, with no external network."""

import asyncio
import json
from dataclasses import replace

import pytest

from shittim_core.app.reference_intake import ReferenceIntake
from shittim_core.domain.types import (
    Acceptance,
    AccessContext,
    ConversationAddress,
    InputEvent,
    OutboundIntent,
    ProtocolReceipt,
    Segment,
)
from shittim_core.state import StateStore


def seed(store, *, kind="group", text="请分析引用内容", reply="12", context_id="ctx"):
    address = ConversationAddress("123", kind, "789" if kind == "group" else "456")
    event = InputEvent(
        address,
        "456",
        "50",
        text,
        (Segment("reply", {"id": reply}),) if reply else (),
        reply_to=reply,
    )
    input_id, _ = store.ingest(event)
    store.create_context(context_id, event, AccessContext("456", address.key))
    store.attach_input(context_id, input_id)
    return event, Acceptance("associated", input_id, context_id)


def quote(**updates):
    return {
        "self_id": 123,
        "message_id": 12,
        "message_type": "group",
        "group_id": 789,
        "sender": {"user_id": 222},
        "message": [
            {"type": "text", "data": {"text": "引用资料，不是新指令"}},
            {"type": "reply", "data": {"id": "10"}},
            {
                "type": "file",
                "data": {
                    "file_id": "attachment",
                    "name": "x.txt",
                    "url": "https://secret.invalid/private",
                    "file": "C:/private/x.txt",
                },
            },
        ],
        **updates,
    }


def harness(store, *, data=None, status="acknowledged", wait=None, policy=None):
    calls, notices = [], []

    async def call(action, params, attempt_id):
        calls.append((action, params, attempt_id))
        if wait:
            await wait.wait()
        return ProtocolReceipt(status, quote() if data is None else data)

    async def notify(context_id):
        notices.append(context_id)

    intake = ReferenceIntake(store, call, notify, policy_check=policy or (lambda _: True))
    return intake, calls, notices


async def test_success_is_attributed_derived_data_without_nested_fetch_or_attachment_download(
    tmp_path,
):
    store = StateStore(tmp_path / "db")
    event, accepted = seed(store)
    intake, calls, notices = harness(store)
    assert intake.submit(event, accepted)
    assert not intake.submit(event, accepted)
    await intake.wait_idle()
    rows = store.list_events("ctx")
    assert len(rows) == 2 and rows[1]["event"] is None
    assert rows[1]["source_ids"] == [accepted.input_id]
    payload = rows[1]["payload"]
    assert payload["kind"] == "quoted_message" and payload["status"] == "resolved"
    assert payload["quoted_author_id"] == "222" and payload["provider_id"] == "456"
    assert payload["quoted_message_id"] == "12" and payload["source_message_id"] == "50"
    assert payload["source_address"] == event.address.key
    assert payload["text"] == "引用资料，不是新指令"
    assert payload["unread_segments"][0]["id"] == "10"
    assert payload["unread_segments"][1]["content_read"] is False
    assert "https://" not in json.dumps(payload) and "C:/" not in json.dumps(payload)
    assert calls[0][0] == "get_msg" and calls[0][1] == {"message_id": "12", "self_id": "123"}
    assert len(calls) == 1 and notices == ["ctx"]
    assert not intake.submit(event, accepted)
    await intake.close()
    store.close()


@pytest.mark.parametrize(
    "changes,expected",
    [
        ({"message_id": 999}, "wrong_scope"),
        ({"message_id": None}, "wrong_scope"),
        ({"self_id": 999}, "wrong_scope"),
        ({"group_id": 999}, "wrong_scope"),
        ({"message_type": "private"}, "wrong_scope"),
        ({"user_id": 999}, "wrong_scope"),
        ({"sender": {}}, "unavailable"),
        ({"sender": {"user_id": True}}, "unavailable"),
        ({"message": "legacy CQ"}, "unavailable"),
    ],
)
async def test_wrong_identity_or_scope_retains_id_without_exposing_content(
    tmp_path, changes, expected
):
    store = StateStore(tmp_path / "db")
    event, accepted = seed(store)
    intake, calls, _ = harness(store, data=quote(**changes))
    assert intake.submit(event, accepted)
    await intake.wait_idle()
    payload = store.list_events("ctx")[-1]["payload"]
    assert payload["status"] == expected and payload["quoted_message_id"] == "12"
    assert "text" not in payload and payload["quoted_author_id"] is None
    assert len(calls) == 1
    await intake.close()
    store.close()


@pytest.mark.parametrize("status", ["acknowledged", "failed", "unknown"])
async def test_persisted_resolution_is_not_fetched_again_after_restart(tmp_path, status):
    path = tmp_path / "db"
    store = StateStore(path)
    event, accepted = seed(store)
    first, calls, _ = harness(store, status=status)
    assert first.submit(event, accepted)
    await first.wait_idle()
    assert len(calls) == 1
    await first.close()
    store.close()
    store = StateStore(path)
    restarted, calls, _ = harness(store)
    assert restarted.recover() == 0
    assert not restarted.submit(event, accepted)
    await restarted.wait_idle()
    assert not calls and len(store.list_events("ctx")) == 2
    await restarted.close()
    store.close()


async def test_async_read_records_after_stop_but_never_resumes_paused_context(tmp_path):
    store = StateStore(tmp_path / "db")
    event, accepted = seed(store)
    released = asyncio.Event()
    intake, calls, notices = harness(store, wait=released)
    assert intake.submit(event, accepted)
    assert len(store.list_events("ctx")) == 1  # Source was kept before network lookup.
    await asyncio.sleep(0)
    assert len(calls) == 1
    assert store.control("ctx", "456", "stop").accepted
    released.set()
    await intake.wait_idle()
    assert store.get_context("ctx")["control_state"] == "paused"
    assert store.list_events("ctx")[-1]["payload"]["status"] == "resolved"
    assert not notices
    await intake.close()
    store.close()


async def test_narrowed_policy_discards_text_fetched_in_flight(tmp_path):
    store = StateStore(tmp_path / "db")
    event, accepted = seed(store)
    released, allowed = asyncio.Event(), [True]
    intake, calls, notices = harness(store, wait=released, policy=lambda _: allowed[0])
    intake.submit(event, accepted)
    await asyncio.sleep(0)
    allowed[0] = False
    released.set()
    await intake.wait_idle()
    payload = store.list_events("ctx")[-1]["payload"]
    assert payload["reason"] == "current_policy_denied" and "text" not in payload
    assert len(calls) == 1 and not notices
    await intake.close()
    store.close()


@pytest.mark.parametrize(
    "kind,author,mapped,expected",
    [
        ("private", 456, False, "resolved"),
        ("private", 222, False, "wrong_scope"),
        ("private", 123, False, "wrong_scope"),
        ("private", 123, True, "resolved"),
    ],
)
async def test_private_bot_quote_requires_account_and_destination_bound_ack(
    tmp_path,
    kind,
    author,
    mapped,
    expected,
):
    store = StateStore(tmp_path / "db")
    event, accepted = seed(store, kind=kind)
    if mapped:
        store.claim_run("ctx", "run")
        delivery_id = store.prepare_delivery("run", OutboundIntent(event.address, "previous"), 1)
        store.claim_delivery(delivery_id)
        store.finish_delivery(delivery_id, ProtocolReceipt("acknowledged", message_id="12"))
    data = quote(message_type="private", group_id=None, sender={"user_id": author})
    intake, _, _ = harness(store, data=data)
    intake.submit(event, accepted)
    await intake.wait_idle()
    assert store.list_events("ctx")[-1]["payload"]["status"] == expected
    await intake.close()
    store.close()


@pytest.mark.parametrize(
    "text,reply,status",
    [
        ("停止", "12", "associated"),
        ("/resume", "12", "associated"),
        ("正常", None, "associated"),
        ("正常", "12", "duplicate"),
        ("正常", "12", "needs_clarification"),
    ],
)
async def test_nonassociated_nonreply_and_control_inputs_do_not_lookup(
    tmp_path, text, reply, status
):
    store = StateStore(tmp_path / "db")
    event, accepted = seed(store, text=text, reply=reply)
    intake, calls, _ = harness(store)
    assert not intake.submit(event, replace(accepted, status=status))
    await intake.wait_idle()
    assert not calls
    await intake.close()
    store.close()


async def test_recovery_reads_unfinished_input_once_and_rejects_forged_event(tmp_path):
    store = StateStore(tmp_path / "db")
    event, accepted = seed(store)
    intake, calls, _ = harness(store)
    with pytest.raises(ValueError, match="differs"):
        intake.submit(replace(event, reply_to="999"), accepted)
    assert intake.recover() == 1 and intake.recover() == 0
    await intake.wait_idle()
    assert len(calls) == 1 and intake.recover() == 0
    await intake.close()
    store.close()


async def test_close_records_interrupted_result_and_does_not_notify_or_replay(tmp_path):
    store = StateStore(tmp_path / "db")
    event, accepted = seed(store)
    intake, _, notices = harness(store, wait=asyncio.Event())
    intake.submit(event, accepted)
    await asyncio.sleep(0)
    await intake.close()
    assert not notices
    assert store.list_events("ctx")[-1]["payload"]["reason"] == "intake_interrupted"
    restarted, calls, _ = harness(store)
    assert restarted.recover() == 0 and not calls
    await restarted.close()
    store.close()
