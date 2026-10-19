"""Abrupt process-exit cuts with independent external-effect evidence files.

These tests exercise process restart and committed SQLite WAL state. They do not
simulate power loss, real QQ, provider requests, or distributed exactly-once.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    InputEvent,
    OutboundIntent,
    RunHalted,
    StateConflict,
)
from shittim_core.state import StateStore

CHILD = r"""
import json, os, pathlib, sys
from shittim_core.domain.types import (
    AccessContext, ConversationAddress, InputEvent, OutboundIntent, ToolResult,
)
from shittim_core.state import StateStore

base = pathlib.Path(sys.argv[1])
cut = sys.argv[2]
store = StateStore(base / 'state.db')
address = ConversationAddress('999', 'group', '10')
event = InputEvent(address, '101', 'original', 'synthetic crash request')

def exit_at(name):
    if cut == name:
        print(json.dumps({'cut_reached': name}), flush=True)
        os._exit(73)  # Deliberately skip close(), context managers and Python finalizers.

def independently_committed_effect(name, text):
    with (base / name).open('a', encoding='utf-8') as stream:
        stream.write(text + '\n')
        stream.flush()
        os.fsync(stream.fileno())

input_id, _ = store.ingest(event)
exit_at('ingest_unbound')
store.create_context('c', event, AccessContext('101', address.key))
store.attach_input('c', input_id)
store.claim_run('c', 'r')
exit_at('claimed_before_consume')
store.consume_inputs('r', 1)
store.append_history('r', {
    'role': 'assistant', 'content': '',
    'tool_calls': [{'id': 'call1', 'type': 'function',
                    'function': {'name': 'append_fixture', 'arguments': '{}'}}],
})
exit_at('assistant_before_dispatch')
effect_id = store.begin_effect('r', 'call1', 'append_fixture', {}, ['fixture:one'], 1)
independently_committed_effect('tool-effect.txt', 'effect-applied')
exit_at('external_effect_before_result')
result = ToolResult('succeeded', 'independent fixture contains one effect',
                    ('fixture:one',), effect_ref=effect_id)
store.finish_effect(effect_id, result)
exit_at('effect_result_before_tool_message')
store.upsert_tool_result('r', 'call1', {
    'role': 'tool', 'tool_call_id': 'call1', 'content': result.to_json(),
})
delivery_id = store.finish_run('r', 'completed', 'done', OutboundIntent(address, 'done'))
exit_at('outcome_and_delivery_committed')
assert store.claim_delivery(delivery_id) is not None
independently_committed_effect('delivery-effect.txt', 'send-attempted')
exit_at('delivery_attempted_without_receipt')
raise AssertionError('Unknown crash cut')
"""


CUTS = [
    "ingest_unbound",
    "claimed_before_consume",
    "assistant_before_dispatch",
    "external_effect_before_result",
    "effect_result_before_tool_message",
    "outcome_and_delivery_committed",
    "delivery_attempted_without_receipt",
]


def _assert_paired_history(messages: list[dict[str, Any]]) -> None:
    pending: set[str] = set()
    for message in messages:
        if message["role"] == "assistant":
            assert not pending, "Another assistant turn appeared before all tool results"
            pending = {call["id"] for call in message.get("tool_calls", [])}
        elif message["role"] == "tool":
            assert message["tool_call_id"] in pending, "Orphan or duplicate tool result"
            pending.remove(message["tool_call_id"])
        else:
            assert not pending, "A different role split the tool-call group"
    assert not pending, "Tool-call tail is incomplete after recovery"


def _fixture_bytes(base: Path) -> dict[str, bytes]:
    return {
        name: (base / name).read_bytes()
        for name in ("tool-effect.txt", "delivery-effect.txt")
        if (base / name).exists()
    }


@pytest.mark.parametrize("cut", CUTS)
def test_process_exit_recovery_preserves_truth_without_replaying_effects(tmp_path: Path, cut: str):
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    child = subprocess.run(
        [sys.executable, "-I", "-B", "-c", CHILD, str(tmp_path), cut],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    assert child.returncode == 73, child.stderr
    assert json.loads(child.stdout)["cut_reached"] == cut
    before_effects = _fixture_bytes(tmp_path)
    store = StateStore(tmp_path / "state.db")
    address = ConversationAddress("999", "group", "10")
    try:
        report = store.recover()
        assert _fixture_bytes(tmp_path) == before_effects
        if cut == "ingest_unbound":
            unbound = store.list_unbound_inputs()
            assert len(unbound) == 1 and unbound[0][1].text == "synthetic crash request"
            assert store.resolve_input(address, "original") is None
            assert not store.list_contexts() and not store.list_effects()
            assert report["active_contexts"] == []
            assert not before_effects
            return

        assert not store.list_unbound_inputs()
        assert store.list_events("c")[0]["event"].author_id == "101"
        if cut == "claimed_before_consume":
            assert report["active_contexts"] == ["c"]
            with pytest.raises(RunHalted):
                store.assert_current("r", 1, 1)
            assert not store.history("c") and not store.list_effects()
            assert not before_effects
            return

        assert report["active_contexts"] == []  # consumed source is not replayed as new work
        history = store.history("c")
        _assert_paired_history(history)
        results = [
            json.loads(message["content"]) for message in history if message["role"] == "tool"
        ]
        assert len(results) == 1

        if cut == "assistant_before_dispatch":
            assert results[0]["status"] == "not_executed"
            assert not store.list_effects() and not before_effects
        elif cut == "external_effect_before_result":
            assert before_effects["tool-effect.txt"].splitlines() == [b"effect-applied"]
            assert results[0]["status"] == "unknown"
            effect = store.list_effects("c")[0]
            assert effect["status"] == "unknown"
            assert results[0]["effect_ref"] == effect["effect_id"]
            store.claim_run("c", "new-run")
            with pytest.raises(StateConflict):
                store.begin_effect(
                    "new-run", "different-call", "append_fixture", {}, ["fixture:one"], 1
                )
            store.finish_run("new-run", "yielded")
        else:
            assert before_effects["tool-effect.txt"].splitlines() == [b"effect-applied"]
            assert results[0]["status"] == "succeeded"
            assert results[0]["content"] == "independent fixture contains one effect"
            assert results[0]["evidence_refs"] == ["fixture:one"]
            assert store.list_effects("c")[0]["result"].content == results[0]["content"]

        if cut == "outcome_and_delivery_committed":
            deliveries = store.list_deliveries()
            assert len(deliveries) == 1 and deliveries[0]["status"] == "prepared"
            assert store.get_run("r")["outcome"]["text"] == "done"
            same = store.finish_run("r", "completed", "done", OutboundIntent(address, "done"))
            assert same == deliveries[0]["delivery_id"] and len(store.list_deliveries()) == 1
            assert "delivery-effect.txt" not in before_effects
        elif cut == "delivery_attempted_without_receipt":
            delivery = store.list_deliveries()[0]
            assert before_effects["delivery-effect.txt"].splitlines() == [b"send-attempted"]
            assert delivery["status"] == "unknown"
            assert store.claim_delivery(delivery["delivery_id"]) is None
            same = store.prepare_delivery("r", OutboundIntent(address, "done"), 1)
            assert same == delivery["delivery_id"]
            assert store.claim_delivery(same) is None
        else:
            assert not store.list_deliveries()

        history_before = store.history("c")
        assert store.recover()["closed_tool_calls"] == []
        assert store.history("c") == history_before
        assert _fixture_bytes(tmp_path) == before_effects
    finally:
        store.close()


def test_outcome_delivery_insert_failure_rolls_back_as_a_unit(tmp_path: Path):
    path = tmp_path / "state.db"
    store = StateStore(path)
    address = ConversationAddress("999", "group", "10")
    event = InputEvent(address, "101", "original", "synthetic transaction failure")
    input_id, _ = store.ingest(event)
    store.create_context("c", event, AccessContext("101", address.key))
    store.attach_input("c", input_id)
    store.claim_run("c", "r")
    store.consume_inputs("r", 1)
    injector = sqlite3.connect(path)
    injector.execute(
        "CREATE TRIGGER reject_delivery BEFORE INSERT ON deliveries "
        "BEGIN SELECT RAISE(ABORT, 'injected delivery insert failure'); END"
    )
    injector.commit()
    injector.close()
    try:
        with pytest.raises(StateConflict):
            store.finish_run("r", "completed", "done", OutboundIntent(address, "done"))
        assert store.get_run("r")["status"] == "running"
        assert store.get_run("r")["outcome"] is None
        assert store.list_deliveries() == []
        assert store.get_input(input_id).text == event.text
    finally:
        store.close()
    reopened = StateStore(path)
    try:
        assert reopened.get_run("r")["status"] == "running"
        assert reopened.list_deliveries() == []
        assert reopened.recover()["interrupted_runs"] == ["r"]
        assert reopened.list_deliveries() == []
    finally:
        reopened.close()
