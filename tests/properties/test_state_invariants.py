from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    InputEvent,
    StateConflict,
    ToolResult,
)
from shittim_core.state import StateStore


@settings(max_examples=25, deadline=None)
@given(st.lists(st.integers(min_value=1, max_value=20), min_size=1, max_size=40))
def test_duplicate_platform_messages_have_one_input_and_one_sequence(ids: list[int]) -> None:
    with tempfile.TemporaryDirectory() as directory:
        store = StateStore(Path(directory) / "state.db")
        address = ConversationAddress("999", "group", "500")
        first = InputEvent(address, "100", str(ids[0]), "request", mentioned_bot=True)
        store.create_context("ctx", first, AccessContext("100", address.key))
        mapped: dict[int, tuple[str, int]] = {}
        for value in ids:
            event = InputEvent(address, "100", str(value), f"text-{value}", mentioned_bot=True)
            input_id, duplicate = store.ingest(event)
            sequence = store.attach_input("ctx", input_id)
            if value in mapped:
                assert duplicate and mapped[value] == (input_id, sequence)
            else:
                assert not duplicate
                mapped[value] = input_id, sequence
        assert len(store.list_events("ctx")) == len(set(ids))
        assert store.get_context("ctx")["input_seq"] == len(set(ids))
        store.close()


@settings(max_examples=20, deadline=None)
@given(st.lists(st.sampled_from(["stop", "resume"]), min_size=1, max_size=25))
def test_control_epochs_never_decrease_and_unauthorized_controls_do_not_mutate(
    actions: list[str],
) -> None:
    with tempfile.TemporaryDirectory() as directory:
        store = StateStore(Path(directory) / "state.db")
        address = ConversationAddress("999", "private", "100")
        store.create_context(
            "ctx", InputEvent(address, "100", "1", "request"), AccessContext("100", address.key)
        )
        previous = store.get_context("ctx")["control_epoch"]
        for action in actions:
            denied = store.control("ctx", "200", action)
            assert not denied.accepted
            assert store.get_context("ctx")["control_epoch"] == previous
            accepted = store.control("ctx", "100", action)
            assert accepted.accepted and accepted.control_epoch >= previous
            previous = accepted.control_epoch
        store.close()


def test_unknown_effect_blocks_same_resource_from_another_context_after_reopen() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "state.db"
        store = StateStore(path)
        address = ConversationAddress("999", "group", "500")
        for context_id, actor in [("first", "100"), ("second", "200")]:
            event = InputEvent(address, actor, context_id, "request", mentioned_bot=True)
            store.create_context(context_id, event, AccessContext(actor, address.key))
            input_id, _ = store.ingest(event)
            store.attach_input(context_id, input_id)
            store.claim_run(context_id, context_id + "-run")
            store.append_history(
                context_id + "-run",
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": context_id,
                            "type": "function",
                            "function": {"name": "write", "arguments": "{}"},
                        }
                    ],
                },
            )
        effect_id = store.begin_effect("first-run", "first", "write", {}, ["shared"], 1)
        store.finish_effect(effect_id, ToolResult("unknown", "transport lost"))
        store.control("first", "100", "close")
        with pytest.raises(StateConflict):
            store.begin_effect("second-run", "second", "write", {}, ["shared"], 1)
        store.close()
        store = StateStore(path)
        store.recover()
        assert store.get_effect(effect_id)["status"] == "unknown"
        store.claim_run("second", "third-run")
        store.append_history(
            "third-run",
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "changed-id",
                        "type": "function",
                        "function": {"name": "write", "arguments": "{}"},
                    }
                ],
            },
        )
        with pytest.raises(StateConflict):
            store.begin_effect("third-run", "changed-id", "write", {}, ["shared"], 1)
        store.close()
