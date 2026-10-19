"""Question authority and lifecycle with real persistence and a bounded fake QQ peer."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from shittim_core.app.config import Settings
from shittim_core.app.service import Application
from shittim_core.delivery.service import Delivery
from shittim_core.domain.model import ToolCall
from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    ExecutionContext,
    InputEvent,
    ProtocolReceipt,
    RunRequest,
    StateConflict,
    ToolResult,
)
from shittim_core.state import StateStore
from shittim_core.tools.collaboration import register_collaboration_tools
from shittim_core.tools.executor import ToolExecutor

ADDRESS = ConversationAddress("999", "group", "10")


class QQPeer:
    def __init__(self):
        self.members: dict[str, dict[str, Any]] = {"202": {"user_id": 202, "group_id": 10}}
        self.lookups: list[tuple[str, dict[str, Any]]] = []
        self.sends: list[dict[str, Any]] = []
        self.send_status = "acknowledged"

    async def call(self, action: str, params: dict[str, Any], attempt_id: str) -> ProtocolReceipt:
        if action in {"get_group_member_info", "get_stranger_info"}:
            self.lookups.append((action, params))
            member = self.members.get(str(params["user_id"]))
            return (
                ProtocolReceipt("acknowledged", member)
                if member is not None
                else ProtocolReceipt("unknown", error="synthetic lookup unavailable")
            )
        self.sends.append({"action": action, "params": params, "attempt_id": attempt_id})
        if self.send_status == "unknown":
            return ProtocolReceipt("unknown")
        return ProtocolReceipt("acknowledged", message_id=str(1000 + len(self.sends)))


class Fixture:
    def __init__(self, path: Path, address: ConversationAddress = ADDRESS):
        self.store = StateStore(path / "state.db")
        self.peer = QQPeer()
        self.delivery = Delivery(self.store, self.peer.call)
        self.tools = ToolExecutor(self.store)
        settings = Settings(
            "999",
            "TOKEN",
            "script",
            "https://example.invalid/v1",
            "KEY",
            path,
            allowed_users=frozenset({"101", "202"}),
            allowed_groups=frozenset({"10", "20"}),
            allowed_tools=frozenset({"ask_person", "wait_for_input", "manage_inquiry"}),
        )
        self.app = Application(settings)
        self.app.store = self.store
        self.app.transport = self.peer
        register_collaboration_tools(
            self.tools, self.store, self.delivery, known_recipient=self.app._known_recipient
        )
        event = InputEvent(address, "101", "original", "work")
        self.access = AccessContext(
            "101", address.key, settings.allowed_tools, frozenset({"qq:" + address.key})
        )
        input_id, _ = self.store.ingest(event)
        self.store.create_context("c", event, self.access)
        self.store.attach_input("c", input_id)
        epoch = self.store.claim_run("c", "r")
        self.store.consume_inputs("r", 1)
        self.request = RunRequest("r", "c", address, self.access, epoch)
        self.index = 0

    async def invoke(self, name: str, args: dict[str, Any]) -> ToolResult:
        self.index += 1
        call_id = f"call{self.index}"
        call = ToolCall(call_id, name, json.dumps(args))
        self.store.append_history(
            "r",
            {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": call_id,
                        "type": "function",
                        "function": {"name": name, "arguments": call.arguments},
                    }
                ],
            },
        )
        current = self.store.get_context("c")
        result = await self.tools.execute(
            call, ExecutionContext(self.request, current["input_seq"], current["history_version"])
        )
        self.store.upsert_tool_result(
            "r",
            call_id,
            {
                "role": "tool",
                "tool_call_id": call_id,
                "content": result.to_json(),
            },
        )
        return result


def ask_args(recipient: str = "202", address: ConversationAddress = ADDRESS) -> dict[str, Any]:
    return {
        "recipient_id": recipient,
        "kind": address.kind,
        "target_id": address.target_id,
        "question": "请提供缺少的信息",
    }


@pytest.mark.asyncio
async def test_configured_unseen_person_needs_matching_qq_identity_and_group_at(tmp_path: Path):
    fixture = Fixture(tmp_path)
    try:
        result = await fixture.invoke("ask_person", ask_args())
        assert result.status == "succeeded" and "answer status=open" in result.content
        assert len(fixture.peer.lookups) == 1 and len(fixture.peer.sends) == 1
        query = fixture.peer.lookups[0]
        assert query[0] == "get_group_member_info"
        assert query[1]["user_id"] == 202 and query[1]["group_id"] == 10
        first = fixture.peer.sends[0]["params"]["message"][0]
        assert first["type"] == "at" and str(first["data"]["qq"]) == "202"
        assert fixture.store.get_inquiry(result.evidence_refs[0])["recipient_id"] == "202"
    finally:
        fixture.store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "member", [None, {"user_id": 303, "group_id": 10}, {"user_id": 202, "group_id": 20}]
)
async def test_uncertain_or_wrong_lookup_never_sends_or_leaves_unknown(tmp_path: Path, member):
    fixture = Fixture(tmp_path)
    if member is None:
        fixture.peer.members.clear()
    else:
        fixture.peer.members["202"] = member
    try:
        result = await fixture.invoke("ask_person", ask_args())
        assert result.status == "not_executed"
        assert fixture.peer.sends == [] and fixture.store.list_open_inquiries() == []
        assert all(item["status"] != "unknown" for item in fixture.store.list_effects())
        fixture.peer.members["202"] = {"user_id": 202, "group_id": 10}
        assert (await fixture.invoke("ask_person", ask_args())).status == "succeeded"
    finally:
        fixture.store.close()


@pytest.mark.asyncio
async def test_configuration_and_observed_author_are_both_required(tmp_path: Path):
    fixture = Fixture(tmp_path)
    try:
        stranger = InputEvent(ADDRESS, "303", "observed-other", "someone else")
        fixture.store.ingest(stranger)
        assert (await fixture.invoke("ask_person", ask_args("303"))).status == "not_executed"
        assert not fixture.peer.lookups and not fixture.peer.sends
        # A quoted claim about 202 is not an actual observed author.
        fixture.store.ingest(InputEvent(ADDRESS, "101", "quote", "202 said hello"))
        fixture.peer.members.clear()
        assert (await fixture.invoke("ask_person", ask_args())).status == "not_executed"
        fixture.store.ingest(InputEvent(ADDRESS, "202", "real-author", "hello"))
        lookups = len(fixture.peer.lookups)
        assert (await fixture.invoke("ask_person", ask_args())).status == "succeeded"
        assert len(fixture.peer.lookups) == lookups
    finally:
        fixture.store.close()


@pytest.mark.asyncio
async def test_private_recipient_mismatch_is_rejected_before_effect_claim(tmp_path: Path):
    address = ConversationAddress("999", "private", "101")
    fixture = Fixture(tmp_path, address)
    try:
        assert (
            await fixture.invoke("ask_person", ask_args("202", address))
        ).status == "not_executed"
        assert fixture.store.list_effects() == []
        assert not fixture.peer.lookups and not fixture.peer.sends
    finally:
        fixture.store.close()


@pytest.mark.asyncio
async def test_cancel_prepared_question_stops_send_without_erasing_inflight_truth(tmp_path: Path):
    fixture = Fixture(tmp_path)
    try:
        inquiry, delivery = fixture.store.create_inquiry("r", "202", ADDRESS, "old", 1)
        result = await fixture.invoke("manage_inquiry", {"action": "cancel", "inquiry_id": inquiry})
        assert result.status == "succeeded"
        assert fixture.store.get_inquiry(inquiry)["status"] == "cancelled"
        assert fixture.store.get_delivery(delivery)["status"] == "superseded"
        assert await fixture.delivery.send(delivery) is None and not fixture.peer.sends
        # Another, already-attempted question remains uncertain after cancellation.
        fixture.peer.send_status = "unknown"
        asked = await fixture.invoke("ask_person", ask_args())
        assert asked.status == "unknown"
        pending = fixture.store.list_open_inquiries("c")[0]
        cancelled = await fixture.invoke(
            "manage_inquiry", {"action": "cancel", "inquiry_id": pending["inquiry_id"]}
        )
        assert cancelled.status == "succeeded"
        assert fixture.store.get_delivery(pending["delivery_id"])["status"] == "unknown"
        count = len(fixture.peer.sends)
        assert (await fixture.invoke("ask_person", ask_args())).status == "not_executed"
        assert len(fixture.peer.sends) == count
    finally:
        fixture.store.close()


@pytest.mark.asyncio
async def test_replace_question_atomically_supersedes_unsent_original(tmp_path: Path):
    fixture = Fixture(tmp_path)
    try:
        old, old_delivery = fixture.store.create_inquiry("r", "202", ADDRESS, "old", 1)
        result = await fixture.invoke(
            "manage_inquiry",
            {"action": "replace", "inquiry_id": old, "question": "updated question"},
        )
        assert result.status == "succeeded"
        new, new_delivery = result.evidence_refs
        assert new != old and new_delivery != old_delivery
        assert fixture.store.get_inquiry(old)["status"] == "superseded"
        assert fixture.store.get_delivery(old_delivery)["status"] == "superseded"
        assert fixture.store.get_inquiry(new)["question"] == "updated question"
        assert len(fixture.peer.sends) == 1
        again = await fixture.invoke(
            "manage_inquiry",
            {"action": "replace", "inquiry_id": old, "question": "updated question"},
        )
        assert again.status == "succeeded" and again.evidence_refs == result.evidence_refs
        assert len(fixture.peer.sends) == 1
    finally:
        fixture.store.close()


@pytest.mark.asyncio
async def test_inquiry_change_cannot_cross_context_boundary(tmp_path: Path):
    fixture = Fixture(tmp_path)
    try:
        source = InputEvent(ADDRESS, "101", "other-source", "separate work")
        input_id, _ = fixture.store.ingest(source)
        fixture.store.create_context("other", source, fixture.access)
        fixture.store.attach_input("other", input_id)
        fixture.store.claim_run("other", "other-run")
        old, _ = fixture.store.create_inquiry("other-run", "202", ADDRESS, "private question", 1)
        result = await fixture.invoke("manage_inquiry", {"action": "cancel", "inquiry_id": old})
        assert result.status == "not_executed" and "private question" not in result.content
        assert fixture.store.get_inquiry(old)["status"] == "open" and not fixture.peer.sends
    finally:
        fixture.store.close()


def test_replacement_failure_rolls_back_old_question_and_prepared_delivery(tmp_path: Path):
    fixture = Fixture(tmp_path)
    try:
        old, delivery = fixture.store.create_inquiry("r", "202", ADDRESS, "old", 1)
        injector = sqlite3.connect(fixture.store.path)
        injector.execute(
            "CREATE TRIGGER reject_replacement BEFORE INSERT ON inquiries "
            "WHEN NEW.inquiry_id='replacement' BEGIN SELECT RAISE(ABORT,'injected'); END"
        )
        injector.commit()
        injector.close()
        with pytest.raises(StateConflict):
            fixture.store.replace_inquiry("r", old, "202", ADDRESS, "new", 1, "replacement")
        assert fixture.store.get_inquiry(old)["status"] == "open"
        assert fixture.store.get_delivery(delivery)["status"] == "prepared"
        assert len(fixture.store.list_deliveries()) == 1
    finally:
        fixture.store.close()


def test_late_answer_is_audit_only_and_identity_checked(tmp_path: Path):
    fixture = Fixture(tmp_path)
    try:
        old, delivery = fixture.store.create_inquiry("r", "202", ADDRESS, "old", 1)
        fixture.store.claim_delivery(delivery)
        fixture.store.finish_delivery(delivery, ProtocolReceipt("acknowledged", message_id="sent"))
        fixture.store.cancel_inquiry("r", old, 1)
        answer = InputEvent(ADDRESS, "202", "late", "old answer", reply_to="sent")
        answer_id, _ = fixture.store.ingest(answer)
        before = fixture.store.get_context("c")["input_seq"]
        fixture.store.record_late_answer(old, answer_id)
        fixture.store.record_late_answer(old, answer_id)
        assert fixture.store.get_context("c")["input_seq"] == before
        records = fixture.store.list_late_answers(context_id="c")
        assert len(records) == 1 and records[0]["event"] == answer
        assert records[0]["source_ids"] == [answer_id]
        fake, _ = fixture.store.ingest(InputEvent(ADDRESS, "101", "fake", "202 said old answer"))
        with pytest.raises(StateConflict):
            fixture.store.record_late_answer(old, fake)
        assert fixture.store.get_inquiry(old)["answer_input_id"] is None
    finally:
        fixture.store.close()


def test_effect_result_history_and_lock_release_roll_back_together(tmp_path: Path):
    fixture = Fixture(tmp_path)
    store = fixture.store
    try:
        store.append_history("r", {"role": "assistant", "tool_calls": [{"id": "atomic"}]})
        effect = store.begin_effect("r", "atomic", "effect", {}, ["shared-resource"], 1)
        external = tmp_path / "external-effect.txt"
        external.write_text("one actual effect", encoding="utf-8")
        injector = sqlite3.connect(store.path)
        injector.execute(
            "CREATE TRIGGER reject_tool_history BEFORE INSERT ON history "
            "WHEN NEW.call_id='atomic' BEGIN SELECT RAISE(ABORT,'injected'); END"
        )
        injector.commit()
        actual = ToolResult("succeeded", "actual result", effect_ref=effect)
        with pytest.raises(StateConflict):
            store.finish_effect_with_history(effect, actual)
        assert store.get_effect(effect)["status"] == "attempting"
        assert not [m for m in store.history("c") if m.get("role") == "tool"]
        with pytest.raises(StateConflict):
            store.begin_effect("r", "second", "effect", {}, ["shared-resource"], 1)
        injector.execute("DROP TRIGGER reject_tool_history")
        injector.commit()
        injector.close()
        store.finish_effect_with_history(effect, actual)
        store.finish_effect_with_history(effect, ToolResult("unknown", "late cancellation"))
        assert store.get_effect(effect)["result"] == actual
        result_rows = [m for m in store.history("c") if m.get("role") == "tool"]
        assert (
            len(result_rows) == 1 and json.loads(result_rows[0]["content"])["status"] == "succeeded"
        )
        assert store.begin_effect("r", "second", "effect", {}, ["shared-resource"], 1) != effect
        assert external.read_text(encoding="utf-8") == "one actual effect"
    finally:
        store.close()
