from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from shittim_core.domain.types import AccessContext, ConversationAddress, InputEvent, StateConflict
from shittim_core.state import StateStore


def setup_context(store: StateStore, name: str = "c", target: str = "10"):
    address = ConversationAddress("999", "group", target)
    event = InputEvent(address, "101", "original", "attachment")
    input_id, _ = store.ingest(event)
    store.create_context(name, event, AccessContext("101", address.key))
    assert store.attach_input(name, input_id) == 1
    return address, input_id


def test_results_share_input_sequence_without_forging_human_events(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        address, original = setup_context(store)
        payload = {"artifact_id": "a", "status": "available", "version": 1}
        assert store.append_result_event("c", original, "download-1", payload, address.key) == 2
        other, _ = store.ingest(InputEvent(address, "202", "next", "supplement"))
        assert store.attach_input("c", other) == 3
        assert store.append_result_event("c", original, "download-1", payload, address.key) == 2
        assert (
            store.append_result_event(
                "c", other, "read-2", {"status": "failed", "reason": "not_found"}, address.key
            )
            == 4
        )
        rows = store.list_events("c")
        assert [row["seq"] for row in rows] == [1, 2, 3, 4]
        assert rows[0]["event_id"] == original and rows[0]["kind"] == "input"
        assert rows[0]["event"].author_id == "101"
        assert rows[1] == {
            "seq": 2,
            "event_id": "result:download-1",
            "kind": "material_result",
            "input_id": original,
            "event": None,
            "payload": payload,
            "source_ids": [original],
            "audience": address.key,
        }
        assert [row["seq"] for row in store.list_events("c", 2)] == [3, 4]
        assert store.get_input(original).text == "attachment"
    finally:
        store.close()


def test_result_requires_bound_source_and_rejects_identity_scope_reuse(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        address, source = setup_context(store)
        other_address, other_source = setup_context(store, "other", "20")
        payload = {"status": "available"}
        with pytest.raises(StateConflict):
            store.append_result_event("c", other_source, "wrong-source", payload, address.key)
        assert store.get_context("c")["input_seq"] == 1
        store.append_result_event("c", source, "stable-id", payload, address.key)
        for altered_payload, audience in [({"status": "failed"}, address.key), (payload, "wider")]:
            with pytest.raises(StateConflict):
                store.append_result_event("c", source, "stable-id", altered_payload, audience)
        with pytest.raises(StateConflict):
            store.append_result_event(
                "other", other_source, "stable-id", payload, other_address.key
            )
        assert store.get_context("c")["input_seq"] == 2
        assert store.get_context("other")["input_seq"] == 1
    finally:
        store.close()


@pytest.mark.parametrize("control", [None, "stop", "close"])
def test_reopen_recovers_unconsumed_result_only_for_active_context(
    tmp_path: Path,
    control: str | None,
):
    path = tmp_path / "state.db"
    store = StateStore(path)
    address, source = setup_context(store)
    store.claim_run("c", "r")
    store.consume_inputs("r", 1)
    store.finish_run("r", "yielded")
    if control:
        store.control("c", "101", control)
    store.append_result_event("c", source, "late-result", {"status": "available"}, address.key)
    before_state = store.get_context("c")["control_state"]
    store.close()
    reopened = StateStore(path)
    try:
        report = reopened.recover()
        assert report["active_contexts"] == (["c"] if control is None else [])
        assert reopened.get_context("c")["control_state"] == before_state
        assert reopened.list_events("c", 1)[0]["event_id"] == "result:late-result"
        assert not reopened.list_deliveries()
    finally:
        reopened.close()


def test_unbound_inputs_survive_reopen_without_automatic_association(tmp_path: Path):
    path = tmp_path / "state.db"
    store = StateStore(path)
    address, bound = setup_context(store)
    unclear = InputEvent(address, "202", "unclear", "那个")
    unbound, _ = store.ingest(unclear)
    assert store.ingest(unclear) == (unbound, True)
    assert store.list_unbound_inputs() == [(unbound, unclear)]
    store.close()
    reopened = StateStore(path)
    try:
        assert reopened.list_unbound_inputs() == [(unbound, unclear)]
        assert reopened.resolve_input(address, "unclear") is None
        assert bound not in [item[0] for item in reopened.list_unbound_inputs()]
        reopened.attach_input("c", unbound)
        assert reopened.list_unbound_inputs() == []
    finally:
        reopened.close()


def test_failed_result_write_does_not_advance_sequence(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    address, source = setup_context(store)
    blocker = sqlite3.connect(store.path, timeout=0, isolation_level=None)
    try:
        blocker.execute("BEGIN IMMEDIATE")
        with pytest.raises(StateConflict):
            store.append_result_event("c", source, "retry", {"status": "available"}, address.key)
        blocker.rollback()
        assert store.get_context("c")["input_seq"] == 1
        assert len(store.list_events("c")) == 1
        assert (
            store.append_result_event("c", source, "retry", {"status": "available"}, address.key)
            == 2
        )
    finally:
        blocker.close()
        store.close()
