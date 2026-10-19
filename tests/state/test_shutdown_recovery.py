"""SQLite interruption candidates and synchronous shutdown authority fencing."""

import sqlite3

import pytest

from shittim_core.domain.types import AccessContext, InputEvent, RunHalted, StateConflict
from shittim_core.state import StateStore

from .test_store import context


def test_consumed_input_and_saved_history_still_yield_interruption_candidate(tmp_path):
    path = tmp_path / "state.db"
    before = StateStore(path)
    context(before)
    before.claim_run("c", "r")
    before.consume_inputs("r", 1)
    message = {"role": "assistant", "content": "Saved reasoning progress"}
    before.append_history("r", message)
    before.close()

    recovered = StateStore(path)
    report = recovered.recover()
    assert report["active_contexts"] == []
    assert report["interrupted_contexts"] == ["c"]
    assert report["interrupted_runs"] == ["r"]
    assert recovered.history("c") == [message]
    # A second recovery keeps the candidate; it neither fabricates a run nor consumes history.
    assert recovered.recover()["interrupted_contexts"] == ["c"]
    assert recovered.get_run("r")["status"] == "interrupted"
    recovered.close()


@pytest.mark.parametrize("terminal", ["completed", "yielded", "failed", "cancelled"])
def test_newer_terminal_run_prevents_old_interruption_from_reactivating(tmp_path, terminal):
    store = StateStore(tmp_path / "db")
    context(store)
    store.claim_run("c", "first")
    store.consume_inputs("first", 1)
    assert store.recover()["interrupted_contexts"] == ["c"]
    store.claim_run("c", "second")
    store.finish_run("second", terminal)
    assert store.recover()["interrupted_contexts"] == []
    assert store.recover()["active_contexts"] == []
    store.close()


@pytest.mark.parametrize("action", ["stop", "close"])
def test_paused_or_closed_context_is_not_an_interruption_candidate(tmp_path, action):
    store = StateStore(tmp_path / "db")
    context(store)
    store.claim_run("c", "r")
    store.consume_inputs("r", 1)
    store.control("c", "owner", action)
    report = store.recover()
    assert report["interrupted_runs"] == ["r"]
    assert report["active_contexts"] == report["interrupted_contexts"] == []
    store.close()


def test_later_router_notice_does_not_hide_latest_interrupted_work_run(tmp_path):
    store = StateStore(tmp_path / "db")
    address, _, _, _ = context(store)
    store.claim_run("c", "r")
    store.consume_inputs("r", 1)
    store.finish_run("r", "interrupted")
    event = InputEvent(address, "owner", "second", "routing feedback source")
    input_id, _ = store.ingest(event)
    store.prepare_notice(input_id, "Notice", AccessContext("owner", address.key), "c")
    report = store.recover()
    assert report["interrupted_contexts"] == ["c"]
    assert report["active_contexts"] == []
    store.close()


def test_shutdown_revokes_only_running_contexts_without_changing_state_or_outcome(tmp_path):
    store = StateStore(tmp_path / "db")
    context(store, "running", "10")
    context(store, "paused", "20")
    context(store, "idle", "30")
    first_epoch = store.claim_run("running", "r1")
    second_epoch = store.claim_run("paused", "r2")
    store.control("paused", "owner", "stop")
    original = {row["context_id"]: row for row in store.list_contexts()}
    assert store.revoke_running_authority() == ["paused", "running"]
    for context_id in ("running", "paused", "idle"):
        latest = store.get_context(context_id)
        assert latest["control_state"] == original[context_id]["control_state"]
        assert latest["control_epoch"] == original[context_id]["control_epoch"] + (
            context_id != "idle"
        )
    for run_id, epoch in (("r1", first_epoch), ("r2", second_epoch)):
        with pytest.raises(RunHalted):
            store.assert_current(run_id, epoch)
        assert store.get_run(run_id)["status"] == "running"
        assert store.get_run(run_id)["outcome"] is None
    store.finish_run("r1", "interrupted")
    assert store.get_run("r1")["status"] == "interrupted"
    store.close()


def test_failed_epoch_transaction_keeps_process_local_fence_and_does_not_leak_to_new_store(
    tmp_path,
):
    path = tmp_path / "db"
    store = StateStore(path)
    context(store)
    epoch = store.claim_run("c", "r")
    blocker = sqlite3.connect(path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(StateConflict):
            store.revoke_running_authority()
        assert store.get_context("c")["control_epoch"] == epoch
        with pytest.raises(RunHalted, match="revoked"):
            store.assert_current("r", epoch)
    finally:
        blocker.rollback()
        blocker.close()
    store.close()
    reopened = StateStore(path)
    # Demonstrate the fence is intentionally process-local, not a false durable success.
    reopened.assert_current("r", epoch)
    assert reopened.recover()["interrupted_contexts"] == ["c"]
    with pytest.raises(RunHalted):
        reopened.assert_current("r", epoch)
    reopened.close()
