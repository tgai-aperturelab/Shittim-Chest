from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    InputEvent,
    ProtocolReceipt,
    RunHalted,
    StateConflict,
)
from shittim_core.state import StateStore

ADDRESS = ConversationAddress("999", "group", "10")


@pytest.fixture
def store(tmp_path: Path):
    result = StateStore(tmp_path / "state.db")
    yield result
    result.close()


def input_record(
    store: StateStore, message_id: str, author: str = "101", address: ConversationAddress = ADDRESS
):
    event = InputEvent(address, author, message_id, "synthetic request")
    input_id, _ = store.ingest(event)
    return input_id, event, AccessContext(author, address.key)


def work_context(store: StateStore, *, consume: bool = True):
    input_id, event, access = input_record(store, "original")
    store.create_context("work", event, access, controllers=["202"])
    store.attach_input("work", input_id)
    epoch = store.claim_run("work", "running")
    if consume:
        store.consume_inputs("running", 1)
    return input_id, epoch


def test_standalone_notice_is_terminal_and_referencable_without_model_authority(store: StateStore):
    input_id, event, access = input_record(store, "unclear")
    delivery_id = store.prepare_notice(input_id, "请引用需要继续的消息", access)
    delivery = store.get_delivery(delivery_id)
    context = store.get_context(delivery["context_id"])
    run = store.get_run(delivery["run_id"])
    assert context["kind"] == "router_notice"
    assert context["address"] == event.address and context["owner_id"] == event.author_id
    assert run["status"] == "yielded" and run["outcome"]["kind"] == "router_notice"
    assert run["outcome"]["consumed_input_ids"] == [input_id]
    assert delivery["intent"].purpose == "control" and delivery["intent"].reply_to == "unclear"
    assert store.history(context["context_id"]) == []
    assert store.list_events(context["context_id"])[0]["handled_by_notice"] is True
    with pytest.raises(RunHalted):
        store.begin_effect(run["run_id"], "call", "tool", {}, [], 1)
    with pytest.raises(StateConflict):
        store.finish_run(run["run_id"], "yielded", "do not overwrite kind")
    assert not store.recover()["active_contexts"]
    assert store.claim_delivery(delivery_id) is not None
    store.finish_delivery(delivery_id, ProtocolReceipt("acknowledged", message_id="receipt"))
    sent = store.resolve_sent(ADDRESS, "receipt")
    assert sent["context_id"] == context["context_id"] and sent["purpose"] == "control"
    assert not store.list_effects()


def test_linked_notice_does_not_finish_live_work_or_split_tool_history(store: StateStore):
    _, epoch = work_context(store)
    assistant = {"role": "assistant", "tool_calls": [{"id": "call1"}]}
    store.append_history("running", assistant)
    notice_input, _, access = input_record(store, "control-input", "202")
    delivery_id = store.prepare_notice(notice_input, "控制请求已收到", access, "work")
    assert store.get_run("running")["status"] == "running"
    store.assert_current("running", epoch)
    assert store.get_context("work")["control_state"] == "active"
    assert store.history("work") == [assistant]
    assert store.get_delivery(delivery_id)["context_id"] == "work"
    assert store.list_events("work")[-1]["source_ids"] == [notice_input]


def test_notice_consumes_only_its_input_not_earlier_unconsumed_input(store: StateStore):
    original, _ = work_context(store, consume=False)
    notice_input, _, access = input_record(store, "unclear-control")
    delivery_id = store.prepare_notice(notice_input, "请说明控制目标", access, "work")
    notice_run = store.get_run(store.get_delivery(delivery_id)["run_id"])
    assert notice_run["outcome"]["consumed_input_ids"] == [notice_input]
    assert original not in notice_run["outcome"]["consumed_input_ids"]
    assert store.recover()["active_contexts"] == ["work"]


def test_handled_notice_does_not_schedule_work_on_recovery(store: StateStore):
    work_context(store)
    notice_input, _, access = input_record(store, "notice-only")
    store.prepare_notice(notice_input, "已记录该控制请求", access, "work")
    store.finish_run("running", "yielded")
    assert not store.recover()["active_contexts"]
    # Notice run's larger seq cannot become another run's consumed watermark.
    store.claim_run("work", "new-run")
    assert store.get_run("new-run")["input_seq"] == 1


def test_notice_identity_unknown_and_acknowledged_are_never_recreated(store: StateStore):
    input_id, _, access = input_record(store, "notice")
    delivery_id = store.prepare_notice(input_id, "请补充", access)
    assert store.prepare_notice(input_id, "请补充", access) == delivery_id
    store.claim_delivery(delivery_id)
    store.finish_delivery(delivery_id, ProtocolReceipt("unknown"))
    assert store.prepare_notice(input_id, "请补充", access) == delivery_id
    assert store.claim_delivery(delivery_id) is None
    assert len(store.list_deliveries()) == 1
    store.finish_delivery(delivery_id, ProtocolReceipt("acknowledged", message_id="later"))
    assert store.prepare_notice(input_id, "请补充", access) == delivery_id
    assert store.claim_delivery(delivery_id) is None


@pytest.mark.parametrize(
    "author,address",
    [
        ("303", ADDRESS),
        ("101", ConversationAddress("999", "private", "101")),
        ("101", ConversationAddress("other-bot", "group", "10")),
    ],
)
def test_linked_notice_rejects_wrong_controller_or_address(
    store: StateStore,
    author: str,
    address: ConversationAddress,
):
    work_context(store)
    input_id, _, access = input_record(store, "not-authorized", author, address)
    with pytest.raises(StateConflict):
        store.prepare_notice(input_id, "should not route", access, "work")
    assert store.get_context("work")["input_seq"] == 1
    assert input_id in [item[0] for item in store.list_unbound_inputs()]
    assert not store.list_deliveries()


def test_notice_access_must_match_verified_author_and_address(store: StateStore):
    input_id, _, _ = input_record(store, "notice")
    for access in [AccessContext("impostor", ADDRESS.key), AccessContext("101", "other scope")]:
        with pytest.raises(StateConflict):
            store.prepare_notice(input_id, "must not create", access)
    assert store.list_contexts() == []


def test_notice_atomic_failure_does_not_leave_context_or_terminal_run(store: StateStore):
    input_id, _, access = input_record(store, "notice")
    injector = sqlite3.connect(store.path)
    injector.execute(
        "CREATE TRIGGER reject_notice BEFORE INSERT ON deliveries "
        "BEGIN SELECT RAISE(ABORT, 'injected failure'); END"
    )
    injector.commit()
    injector.close()
    with pytest.raises(StateConflict):
        store.prepare_notice(input_id, "rollback all notice metadata", access)
    assert store.list_contexts() == [] and not store.list_deliveries()
    assert len(store.list_unbound_inputs()) == 1


def test_notice_can_follow_new_input_but_not_a_revoked_epoch(store: StateStore):
    work_context(store)
    notice_input, _, access = input_record(store, "notice")
    delivery_id = store.prepare_notice(notice_input, "请求已记录", access, "work")
    new_input, _, _ = input_record(store, "later")
    store.attach_input("work", new_input)
    assert store.claim_delivery(delivery_id) is not None
    another = store.prepare_notice(notice_input, "更新说明", access, "work")
    store.control("work", "101", "stop")
    assert store.claim_delivery(another) is None
    assert store.get_delivery(another)["status"] == "superseded"
