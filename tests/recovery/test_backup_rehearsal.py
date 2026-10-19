"""Offline candidate-fixture backup/restore rehearsal, not a production rollback test."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest

from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    InputEvent,
    OutboundIntent,
    ProtocolReceipt,
    StateConflict,
    ToolResult,
)
from shittim_core.materials import MaterialStore
from shittim_core.state import StateStore


def _hash_files(directory: Path) -> dict[str, str]:
    return {
        path.relative_to(directory).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


def _inside_fixture(base: Path, path: Path) -> None:
    resolved = path.resolve()
    assert resolved != base and resolved.is_relative_to(base)
    assert not path.is_symlink() and not path.is_junction()


async def test_closed_fixture_backup_restore_keeps_versions_unknown_and_partial_outbox(tmp_path):
    base = tmp_path.resolve()
    live, backup, preserved = (
        base / name for name in ("candidate", "backup", "preserved-original")
    )
    for directory in (live, backup, preserved):
        _inside_fixture(base, directory)
    live.mkdir()
    database = live / "state.sqlite3"
    config = {
        "bot_id": "9001",
        "data_dir": str(live),
        "token_env": "FIXTURE_QQ_TOKEN",
        "api_key_env": "FIXTURE_MODEL_API_KEY",
    }
    (live / "config.local.json").write_text(json.dumps(config), encoding="utf-8")
    address = ConversationAddress("9001", "private", "9002")
    access = AccessContext("9002", address.key, allowed_resources=frozenset({"materials:c"}))
    event = InputEvent(address, "9002", "fixture-input", "synthetic source")
    store = StateStore(database)
    try:
        input_id, duplicate = store.ingest(event)
        assert duplicate is False
        store.create_context("c", event, access)
        seq = store.attach_input("c", input_id)
        store.claim_run("c", "r")
        materials = MaterialStore(live / "materials", store, "c")
        first = await materials.add_bytes(
            b"original fixture", "source.txt", access, source_ref=input_id
        )
        second = await materials.add_bytes(
            b"revised fixture",
            "source.txt",
            access,
            artifact_id=first.artifact_id,
            expected_version=first.version,
            source_ref=input_id,
        )
        material_path = Path(store.get_artifact(first.artifact_id)["path"])
        assert material_path.is_absolute() and material_path.is_relative_to(live)
        store.append_history(
            "r",
            {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": "uncertain",
                        "type": "function",
                        "function": {"name": "fixture_write", "arguments": "{}"},
                    }
                ],
            },
        )
        effect_id = store.begin_effect(
            "r", "uncertain", "fixture_write", {}, ["fixture:target"], seq
        )
        unknown = ToolResult(
            "unknown", "Synthetic request may have been applied", effect_ref=effect_id
        )
        store.finish_effect(effect_id, unknown)
        store.upsert_tool_result(
            "r",
            "uncertain",
            {
                "role": "tool",
                "tool_call_id": "uncertain",
                "content": unknown.to_json(),
            },
        )
        intents = tuple(
            OutboundIntent(address, text, purpose="message")
            for text in ("fixture prefix", "fixture uncertain", "fixture tail")
        )
        prefix, uncertain, tail = store.prepare_delivery_items("r", intents, seq)
        assert store.claim_delivery(prefix) is not None
        store.finish_delivery(prefix, ProtocolReceipt("acknowledged", message_id="fixture-ack"))
        assert store.claim_delivery(uncertain) is not None
        store.finish_delivery(
            uncertain, ProtocolReceipt("unknown", error="synthetic missing receipt")
        )
        assert store.claim_delivery(tail) is None
        pending_intent = OutboundIntent(address, "independent prepared fixture", purpose="message")
        pending = store.prepare_delivery("r", pending_intent, seq)
        store.finish_run("r", "yielded", reason="fixture_unknown", unresolved_effects=(effect_id,))
        original_delivery_ids = {row["delivery_id"] for row in store.list_deliveries()}
    finally:
        # Copy only after the fixture connection is closed; never claim a live DB-only copy is safe.
        store.close()

    expected_hashes = _hash_files(live)
    assert "state.sqlite3" in expected_hashes
    assert len([path for path in expected_hashes if path.startswith("materials/")]) == 2
    for directory in (live, backup, preserved):
        _inside_fixture(base, directory)
    shutil.copytree(live, backup)
    assert _hash_files(backup) == expected_hashes
    # Preserve the original elsewhere in this pytest fixture. Restoring to a different
    # path would falsely pass while SQLite's absolute material paths still read originals.
    _inside_fixture(base, live)
    _inside_fixture(base, preserved)
    live.rename(preserved)
    assert not live.exists()
    assert not material_path.exists()
    assert _hash_files(preserved) == expected_hashes
    _inside_fixture(base, backup)
    _inside_fixture(base, live)
    shutil.copytree(backup, live)
    assert _hash_files(live) == expected_hashes
    assert json.loads((live / "config.local.json").read_text()) == config
    assert set(config) == {"bot_id", "data_dir", "token_env", "api_key_env"}

    restored = StateStore(database)
    try:
        restored.recover()  # This classifies persisted state; it does not send or execute.
        assert restored.get_input(input_id) == event
        assert restored.get_context("c")["input_seq"] == seq
        materials = MaterialStore(live / "materials", restored, "c")
        old = await materials.read(first.artifact_id, access, version=1)
        new = await materials.read(first.artifact_id, access, version=2)
        assert old.data == b"original fixture" and old.ref.sha256 == first.sha256
        assert new.data == b"revised fixture" and new.ref.sha256 == second.sha256
        assert restored.get_artifact(first.artifact_id, version=1)["source_ref"] == input_id
        assert Path(restored.get_artifact(first.artifact_id)["path"]) == material_path
        assert restored.get_effect(effect_id)["status"] == "unknown"
        restored.claim_run("c", "retry-run")
        with pytest.raises(StateConflict, match="in-flight or unknown effect"):
            restored.begin_effect(
                "retry-run", "new-call", "fixture_write", {}, ["fixture:target"], seq
            )
        assert len(restored.list_effects("c")) == 1

        assert restored.claim_delivery(prefix) is None
        assert restored.claim_delivery(uncertain) is None
        assert restored.claim_delivery(tail) is None
        assert restored.get_delivery(tail)["status"] == "prepared"
        assert restored.get_delivery(uncertain)["status"] == "unknown"
        assert restored.resolve_sent(address, "fixture-ack")["delivery_id"] == prefix
        assert restored.prepare_delivery_items("r", intents, seq) == (prefix, uncertain, tail)
        assert restored.prepare_delivery("r", pending_intent, seq) == pending
        assert {row["delivery_id"] for row in restored.list_deliveries()} == original_delivery_ids
        # A definitely unattempted independent item may be claimed once. No real sender exists.
        simulated_sends = 0
        if restored.claim_delivery(pending) is not None:
            simulated_sends += 1
            restored.finish_delivery(
                pending, ProtocolReceipt("acknowledged", message_id="fixture-ack-2")
            )
        assert restored.claim_delivery(pending) is None
        assert simulated_sends == 1
        assert _hash_files(backup) == expected_hashes
        assert _hash_files(preserved) == expected_hashes
    finally:
        restored.close()
