from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    InputEvent,
    OutboundIntent,
    ProtocolReceipt,
    RunHalted,
    StaleView,
    StateConflict,
    ToolResult,
)
from shittim_core.state import StateStore


@pytest.fixture
def store(tmp_path: Path):
    result = StateStore(tmp_path / "state.db")
    yield result
    result.close()


def context(store: StateStore, context_id: str = "c", target: str = "10"):
    address = ConversationAddress("bot", "group", target)
    event = InputEvent(address, "owner", "m1", "please help")
    access = AccessContext("owner", address.key, frozenset({"write"}))
    input_id, duplicate = store.ingest(event)
    store.create_context(context_id, event, access)
    seq = store.attach_input(context_id, input_id)
    return address, input_id, seq, duplicate


def tool_message(call_id: str, status: str):
    return {
        "role": "tool",
        "tool_call_id": call_id,
        "content": '{"status":"' + status + '","content":"recorded"}',
    }


def test_input_dedup_scope_and_persistent_source(store: StateStore):
    address, input_id, seq, duplicate = context(store)
    assert not duplicate and seq == 1
    assert store.ingest(InputEvent(address, "impostor", "m1", "changed")) == (input_id, True)
    assert store.get_input(input_id).author_id == "owner"
    assert store.attach_input("c", input_id) == 1
    other = ConversationAddress("bot", "group", "20")
    assert store.ingest(InputEvent(other, "owner", "m1", "same platform id"))[0] != input_id
    assert store.resolve_input(address, "m1") == {"input_id": input_id, "context_id": "c"}
    assert store.resolve_input(other, "m1") is None
    assert store.list_contexts(other) == []
    events = store.list_events("c")
    assert events[0]["audience"] == address.key
    assert events[0]["source_ids"] == [input_id]


def test_file_reopen_retains_inputs(tmp_path: Path):
    path = tmp_path / "real.db"
    first = StateStore(path)
    context(first)
    first.close()
    first.close()
    second = StateStore(path)
    assert second.get_context("c")["input_seq"] == 1
    assert second.list_events("c")[0]["event"].text == "please help"
    second.close()


def test_one_active_run_and_unauthorized_stop(store: StateStore):
    context(store)
    epoch = store.claim_run("c", "r")
    with pytest.raises(StateConflict):
        store.claim_run("c", "other")
    denied = store.control("c", "outsider", "stop")
    assert not denied.accepted and denied.control_epoch == epoch
    store.assert_current("r", epoch, 1)
    accepted = store.control("c", "owner", "stop")
    assert accepted.accepted and accepted.control_epoch > epoch
    with pytest.raises(RunHalted):
        store.assert_current("r", epoch)
    with pytest.raises(RunHalted):
        store.claim_run("c", "new")


def test_explicit_controller_and_new_input_invalidate_decision(store: StateStore):
    address, _, _, _ = context(store)
    store.set_controllers("c", "owner", ["member"])
    epoch = store.claim_run("c", "r")
    second, _ = store.ingest(InputEvent(address, "member", "m2", "correction"))
    assert store.attach_input("c", second) == 2
    with pytest.raises(StaleView):
        store.begin_effect("r", "call", "write", {}, ["shared"], 1)
    store.consume_inputs("r", 2)
    store.assert_current("r", epoch, 2)
    assert store.control("c", "member", "stop").accepted


def test_unknown_blocks_resource_across_runs_and_late_actual_releases(store: StateStore):
    context(store)
    context(store, "c2", "20")
    store.claim_run("c", "r")
    store.claim_run("c2", "r2")
    effect = store.begin_effect("r", "call", "write", {"value": 1}, ["shared"], 1)
    store.finish_effect(effect, ToolResult("unknown", "lost result"))
    store.finish_run("r", "cancelled")
    with pytest.raises(StateConflict):
        store.begin_effect("r2", "new-call", "write", {"value": 2}, ["shared"], 1)
    actual = ToolResult("succeeded", "written")
    store.finish_effect(effect, actual)
    store.finish_effect(effect, ToolResult("unknown", "late cancellation"))
    assert store.get_effect(effect)["result"] == actual
    next_effect = store.begin_effect("r2", "new-call", "write", {}, ["shared"], 1)
    assert next_effect != effect


def test_duplicate_call_never_authorizes_reexecution(store: StateStore):
    context(store)
    store.claim_run("c", "r")
    effect = store.begin_effect("r", "call", "write", {}, [], 1)
    store.finish_effect(effect, ToolResult("succeeded", "done"))
    with pytest.raises(StateConflict):
        store.begin_effect("r", "call", "write", {}, [], 1)


def test_tool_result_upsert_and_effect_authority_are_separate(store: StateStore):
    context(store)
    store.claim_run("c", "r")
    store.append_history("r", {"role": "assistant", "tool_calls": [{"id": "call"}]})
    effect = store.begin_effect("r", "call", "write", {}, ["resource"], 1)
    store.upsert_tool_result("r", "call", tool_message("call", "unknown"))
    assert store.get_effect(effect)["status"] == "attempting"
    store.finish_run("r", "cancelled")
    store.finish_effect(effect, ToolResult("succeeded", "actual task completed"))
    store.upsert_tool_result("r", "call", tool_message("call", "succeeded"))
    store.upsert_tool_result("r", "call", tool_message("call", "unknown"))
    rows = store.history("c")
    assert len(rows) == 2 and '"succeeded"' in rows[1]["content"]
    assert store.get_effect(effect)["status"] == "succeeded"


def test_final_outcome_delivery_atomic_idempotent_and_receipt_map(store: StateStore):
    address, _, _, _ = context(store)
    store.claim_run("c", "r")
    store.consume_inputs("r", 1)
    intent = OutboundIntent(address, "result")
    delivery = store.finish_run("r", "completed", "result", intent)
    assert delivery is not None
    assert store.finish_run("r", "completed", "result", intent) == delivery
    assert len(store.list_deliveries()) == 1
    assert store.claim_delivery(delivery) is not None
    assert store.claim_delivery(delivery) is None
    receipt = ProtocolReceipt("acknowledged", message_id="sent1")
    store.finish_delivery(delivery, receipt)
    store.finish_delivery(delivery, ProtocolReceipt("unknown"))
    assert store.get_delivery(delivery)["status"] == "acknowledged"
    assert store.resolve_sent(address, "sent1")["context_id"] == "c"
    assert store.resolve_sent(ConversationAddress("bot2", "group", "10"), "sent1") is None


def test_invalid_multi_item_delivery_rolls_back_outcome(store: StateStore):
    address, _, _, _ = context(store)
    store.claim_run("c", "r")
    with pytest.raises(ValueError):
        store.finish_run(
            "r", "completed", "result", OutboundIntent(address, "text", artifact_refs=("file",))
        )
    assert store.get_run("r")["status"] == "running"
    assert not store.list_deliveries()


def test_delivery_scope_and_new_input_revalidated(store: StateStore):
    address, _, _, _ = context(store)
    store.claim_run("c", "r")
    wrong = ConversationAddress("bot", "private", "stranger")
    forbidden = store.prepare_delivery("r", OutboundIntent(wrong, "private content", "message"), 1)
    assert store.claim_delivery(forbidden) is None
    assert store.get_delivery(forbidden)["status"] == "superseded"
    delivery = store.prepare_delivery("r", OutboundIntent(address, "old result"), 1)
    second, _ = store.ingest(InputEvent(address, "owner", "m2", "change"))
    store.attach_input("c", second)
    assert store.claim_delivery(delivery) is None
    assert store.get_delivery(delivery)["status"] == "superseded"


def test_inquiry_atomic_reply_identity_and_paused_answer(store: StateStore):
    address, _, _, _ = context(store)
    store.claim_run("c", "r")
    inquiry, delivery = store.create_inquiry("r", "member", address, "Which one?", 1, "q")
    assert store.create_inquiry("r", "member", address, "Which one?", 1, "q") == (inquiry, delivery)
    real_id, _ = store.ingest(InputEvent(address, "member", "answer", "this", reply_to="sent"))
    with pytest.raises(StateConflict):
        store.answer_inquiry(inquiry, real_id)
    store.claim_delivery(delivery)
    store.finish_delivery(delivery, ProtocolReceipt("acknowledged", message_id="sent"))
    fake_id, _ = store.ingest(InputEvent(address, "impostor", "fake", "member says this"))
    with pytest.raises(StateConflict):
        store.answer_inquiry(inquiry, fake_id)
    store.control("c", "owner", "stop")
    assert store.answer_inquiry(inquiry, real_id) == ("c", 2)
    assert store.answer_inquiry(inquiry, real_id) == ("c", 2)
    assert store.get_context("c")["control_state"] == "paused"
    assert store.get_inquiry(inquiry)["status"] == "answered"
    assert store.resolve_sent(address, "sent")["inquiry_id"] == inquiry


def test_recovery_unknown_and_paused_not_restarted(tmp_path: Path):
    path = tmp_path / "crash.db"
    before = StateStore(path)
    address, _, _, _ = context(before)
    context(before, "paused", "20")
    before.claim_run("c", "r")
    before.claim_run("paused", "rp")
    effect = before.begin_effect("r", "call", "write", {}, ["resource"], 1)
    delivery = before.prepare_delivery("r", OutboundIntent(address, "progress", "message"), 1)
    before.claim_delivery(delivery)
    before.control("paused", "owner", "stop")
    before.close()
    after = StateStore(path)
    report = after.recover()
    assert set(report["interrupted_runs"]) == {"r", "rp"}
    assert report["active_contexts"] == []  # no new, unconsumed input; do not replay old work
    assert after.get_effect(effect)["status"] == "unknown"
    assert after.get_delivery(delivery)["status"] == "unknown"
    after.claim_run("c", "r2")
    with pytest.raises(StateConflict):
        after.begin_effect("r2", "changed-call", "write", {}, ["resource"], 1)
    assert after.recover()["unknown_effects"] == []
    after.close()


def test_history_cas_preserves_raw_and_rejects_stale_versions(store: StateStore):
    context(store)
    epoch = store.claim_run("c", "r")
    store.append_history("r", {"role": "user", "content": "source"})
    version = store.get_context("c")["history_version"]
    assert not store.compare_and_swap_history("c", version - 1, [])
    assert store.compare_and_swap_history(
        "c",
        version,
        [{"role": "user", "content": "summary"}],
        expected_epoch=epoch,
        expected_input_seq=1,
        expected_policy_version=1,
    )
    assert store.history("c") == [{"role": "user", "content": "summary"}]
    assert len(store.history_records("c")) == 2
    effect = store.begin_effect("r", "call", "write", {}, ["resource"], 1)
    store.finish_effect(effect, ToolResult("unknown", "uncertain"))
    assert not store.compare_and_swap_history("c", version + 1, [])


def test_artifact_versions_keep_scope_and_do_not_read_files(store: StateStore):
    address, _, _, _ = context(store)
    store.register_artifact("a", "c", "owner", address.key, "/does/not/exist", "0" * 64)
    store.register_artifact("a", "c", "owner", address.key, "/new", "1" * 64, 2)
    assert store.get_artifact("a")["version"] == 2
    assert store.get_artifact("a", 1)["path"] == "/does/not/exist"
    with pytest.raises(StateConflict):
        store.register_artifact("a", "c", "owner", "wider", "/new", "2" * 64, 3)


def test_busy_write_fails_without_partial_input(store: StateStore):
    blocker = sqlite3.connect(store.path, timeout=0, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    event = InputEvent(ConversationAddress("bot", "private", "owner"), "owner", "m", "secret")
    with pytest.raises(StateConflict):
        store.ingest(event)
    blocker.rollback()
    blocker.close()
    _, duplicate = store.ingest(event)
    assert not duplicate


def test_ambiguous_input_mapping_is_not_guessed(store: StateStore):
    address, input_id, _, _ = context(store)
    event = store.get_input(input_id)
    store.create_context("c2", event, AccessContext("owner", address.key))
    store.attach_input("c2", input_id)
    assert store.resolve_input(address, "m1") is None


@pytest.mark.parametrize("cut", ["before_dispatch", "attempting", "result_persisted"])
def test_recover_closes_tool_tail_from_durable_effect_truth(store: StateStore, cut: str):
    context(store)
    store.claim_run("c", "r")
    store.append_history(
        "r",
        {
            "role": "assistant",
            "tool_calls": [
                {"id": "first", "function": {"name": "write", "arguments": "{}"}},
                {"id": "second", "function": {"name": "write", "arguments": "{}"}},
            ],
        },
    )
    if cut != "before_dispatch":
        effect = store.begin_effect("r", "first", "write", {}, ["resource"], 1)
        if cut == "result_persisted":
            store.finish_effect(effect, ToolResult("succeeded", "the actual result"))
    report = store.recover()
    assert set(report["closed_tool_calls"]) == {"r:first", "r:second"}
    history = store.history("c")
    assert [m.get("tool_call_id") for m in history[1:]] == ["first", "second"]
    import json

    first, second = [json.loads(m["content"]) for m in history[1:]]
    assert (
        first["status"]
        == {
            "before_dispatch": "not_executed",
            "attempting": "unknown",
            "result_persisted": "succeeded",
        }[cut]
    )
    assert second["status"] == "not_executed"
    if cut == "result_persisted":
        assert first["content"] == "the actual result"
    assert store.recover()["closed_tool_calls"] == []


def test_recovery_returns_only_actual_unconsumed_active_input(store: StateStore):
    address, _, _, _ = context(store)
    store.claim_run("c", "r")
    second, _ = store.ingest(InputEvent(address, "owner", "next", "new input"))
    store.attach_input("c", second)
    assert store.recover()["active_contexts"] == ["c"]


def test_claiming_run_does_not_consume_input_before_first_view(store: StateStore):
    context(store)
    store.claim_run("c", "r")
    assert store.get_run("r")["input_seq"] == 0
    assert store.recover()["active_contexts"] == ["c"]
