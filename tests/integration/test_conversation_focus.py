"""Conversation continuity is persistent routing state, not a model guess."""

import asyncio
import json
from dataclasses import replace

import pytest
from websockets.asyncio.client import connect

from shittim_core.app.service import Application
from shittim_core.domain.model import NormalizedResponse
from shittim_core.domain.types import (
    ConversationAddress,
    Segment,
    StateConflict,
    ToolResult,
)
from tests.integration.test_app import headers, settings
from tests.integration.test_runtime import (
    event,
    final,
    idle,
    register_effect,
    runtime,
    tool_response,
)


def private_runtime(path, steps):
    rt = runtime(path, steps)
    factory = rt.coordinator.access_factory
    rt.coordinator.access_factory = lambda source: replace(
        factory(source), audience=source.address.key
    )
    return rt


def private(text, mid, author="101"):
    return replace(
        event(text, author=author, mid=mid, mentioned=False),
        address=ConversationAddress("999", "private", author),
    )


async def test_five_plain_private_turns_and_numeric_choice_share_visible_history(tmp_path):
    turns = ["你好", "你的工具列表", "原始工具名", "1", "向我输出你的上下文"]
    rt = private_runtime(tmp_path / "state.db", [final("已回复") for _ in turns])
    try:
        accepted = []
        for index, text in enumerate(turns):
            accepted.append(await rt.coordinator.accept(private(text, str(index))))
            await idle(rt)
        assert len({a.context_id for a in accepted}) == 1
        assert len(rt.store.list_contexts()) == 1
        view = json.dumps(rt.model.requests[-1], ensure_ascii=False)
        assert all(text in view for text in turns)
        assert rt.store.get_context(accepted[0].context_id)["input_seq"] == 5
    finally:
        await rt.close()


async def test_busy_plain_followup_invalidates_old_view_without_new_context(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()

    async def old_view(messages):
        entered.set()
        await release.wait()
        return final("过时结果")

    rt = private_runtime(tmp_path / "state.db", [old_view, final("根据补充处理")])
    try:
        first = await rt.coordinator.accept(private("处理资料", "1"))
        await asyncio.wait_for(entered.wait(), 1)
        second = await rt.coordinator.accept(private("补充条件：保留原件", "2"))
        assert second.context_id == first.context_id
        release.set()
        await idle(rt)
        assert len(rt.qq.calls) == 1 and "过时结果" not in json.dumps(
            rt.qq.calls, ensure_ascii=False
        )
        assert "保留原件" in json.dumps(rt.model.requests[-1], ensure_ascii=False)
    finally:
        release.set()
        await rt.close()


async def test_new_is_deterministic_and_quotes_switch_focus_back(tmp_path):
    rt = private_runtime(
        tmp_path / "state.db", [final("A"), final("B"), final("A补充"), final("选项一")]
    )
    try:
        first = await rt.coordinator.accept(private("第一段讨论", "1"))
        await idle(rt)
        opened = await rt.coordinator.accept(private("/new", "2"))
        await idle(rt)
        assert opened.reason == "new_conversation" and len(rt.model.requests) == 1
        assert rt.store.get_context(opened.context_id)["pending"] == 0
        second = await rt.coordinator.accept(private("另一段讨论", "3"))
        await idle(rt)
        assert second.context_id == opened.context_id != first.context_id
        chosen = await rt.coordinator.accept(replace(private("回到第一段", "4"), reply_to="1"))
        await idle(rt)
        followup = await rt.coordinator.accept(private("1", "5"))
        await idle(rt)
        assert chosen.context_id == followup.context_id == first.context_id
        assert "另一段讨论" not in json.dumps(rt.model.requests[-1], ensure_ascii=False)
        assert "回到第一段" in json.dumps(rt.model.requests[-1], ensure_ascii=False)
        assert len(rt.store.list_contexts()) == 2
    finally:
        await rt.close()


async def test_focus_and_empty_new_conversation_survive_restart(tmp_path):
    path = tmp_path / "state.db"
    before = private_runtime(path, [final("旧会话回复")])
    await before.coordinator.accept(private("旧话题", "1"))
    await idle(before)
    opened = await before.coordinator.accept(private("新会话", "2"))
    await idle(before)
    await before.close()
    after = private_runtime(path, [final("新话题回复")])
    try:
        assert after.coordinator.recover()["active_contexts"] == []
        await after.coordinator.recover_unbound_inputs()
        await idle(after)
        assert not after.model.requests
        accepted = await after.coordinator.accept(private("新问题", "3"))
        await idle(after)
        assert accepted.context_id == opened.context_id
        assert "旧话题" not in json.dumps(after.model.requests[-1], ensure_ascii=False)
    finally:
        await after.close()


async def test_legacy_database_bootstraps_latest_scoped_input_without_merging(tmp_path):
    rt = private_runtime(tmp_path / "state.db", [final("延续最近会话")])
    try:
        for index in range(2):
            source = private(f"旧库任务{index}", str(index))
            identity, _ = rt.store.ingest(source)
            rt.store.create_context(str(index), source, rt.coordinator.access_factory(source))
            rt.store.attach_input(str(index), identity)
        accepted = await rt.coordinator.accept(private("继续讨论", "2"))
        await idle(rt)
        assert accepted.context_id == "1"
        assert len(rt.store.list_contexts()) == 2
        assert "旧库任务0" not in json.dumps(rt.model.requests[-1], ensure_ascii=False)
    finally:
        await rt.close()


async def test_group_focus_keeps_authors_separate_and_does_not_consume_unaddressed_chat(tmp_path):
    rt = runtime(tmp_path / "state.db", [final("A"), final("B"), final("A补充")])
    try:
        first = await rt.coordinator.accept(event("A的问题", author="101", mid="1"))
        await idle(rt)
        second = await rt.coordinator.accept(event("B的问题", author="202", mid="2"))
        await idle(rt)
        ignored = await rt.coordinator.accept(
            event("普通群聊", author="101", mid="3", mentioned=False)
        )
        followup = await rt.coordinator.accept(event("1", author="101", mid="4"))
        await idle(rt)
        assert ignored.status == "ignored"
        assert first.context_id == followup.context_id != second.context_id
        view = json.dumps(rt.model.requests[-1], ensure_ascii=False)
        assert "B的问题" not in view and "普通群聊" not in view
    finally:
        await rt.close()


async def test_private_and_group_addresses_never_share_focus(tmp_path):
    rt = private_runtime(tmp_path / "state.db", [final("私聊"), final("群聊"), final("私聊继续")])
    try:
        first = await rt.coordinator.accept(private("PRIVATE_MATERIAL", "1"))
        await idle(rt)
        grouped = await rt.coordinator.accept(event("GROUP_MATERIAL", mid="2"))
        await idle(rt)
        assert "PRIVATE_MATERIAL" not in json.dumps(rt.model.requests[-1])
        second = await rt.coordinator.accept(private("1", "3"))
        await idle(rt)
        assert second.context_id == first.context_id != grouped.context_id
        assert "GROUP_MATERIAL" not in json.dumps(rt.model.requests[-1])
    finally:
        await rt.close()


async def test_late_causal_attachment_does_not_steal_newer_focus(tmp_path):
    rt = private_runtime(
        tmp_path / "state.db", [final("A"), final("B"), final("A附件"), final("B选项")]
    )
    try:
        first = await rt.coordinator.accept(replace(private("处理附件", "1"), reply_to="file"))
        await idle(rt)
        opened = await rt.coordinator.accept(private("/new", "2"))
        await idle(rt)
        await rt.coordinator.accept(private("讨论另一个问题", "3"))
        await idle(rt)
        source = replace(private("", "file"), segments=(Segment("file", {"file_id": "file"}),))
        attached = await rt.coordinator.accept(source)
        await idle(rt)
        next_turn = await rt.coordinator.accept(private("1", "4"))
        await idle(rt)
        assert attached.context_id == first.context_id
        assert next_turn.context_id == opened.context_id
    finally:
        await rt.close()


async def test_pause_accepts_followups_but_only_current_focus_resumes(tmp_path):
    rt = private_runtime(tmp_path / "state.db", [final("A"), final("B"), final("B继续")])
    try:
        first = await rt.coordinator.accept(private("话题A", "1"))
        await idle(rt)
        opened = await rt.coordinator.accept(private("/new", "2"))
        await idle(rt)
        await rt.coordinator.accept(private("话题B", "3"))
        await idle(rt)
        stopped = await rt.coordinator.accept(private("停止", "4"))
        added = await rt.coordinator.accept(private("附加条件", "5"))
        await idle(rt)
        assert stopped.context_id == added.context_id == opened.context_id
        assert len(rt.model.requests) == 2
        assert rt.store.get_context(first.context_id)["control_state"] == "active"
        resumed = await rt.coordinator.accept(private("继续", "6"))
        await idle(rt)
        assert resumed.context_id == opened.context_id and len(rt.model.requests) == 3
        assert "附加条件" in json.dumps(rt.model.requests[-1], ensure_ascii=False)
    finally:
        await rt.close()


async def test_revoked_focus_rejects_input_instead_of_creating_fresh_grants(tmp_path):
    rt = private_runtime(tmp_path / "state.db", [final("A")])
    try:
        first = await rt.coordinator.accept(private("话题", "1"))
        await idle(rt)
        rt.coordinator.policy_predicate = lambda context: False
        for index, text in enumerate(("1", "/new"), 2):
            denied = await rt.coordinator.accept(private(text, str(index)))
            assert denied.status == "rejected" and denied.reason == "context_policy_changed"
        assert len(rt.store.list_contexts()) == 1
        stopped = await rt.coordinator.accept(private("停止", "4"))
        assert stopped.context_id == first.context_id
        assert rt.store.get_context(first.context_id)["control_state"] == "paused"
    finally:
        await rt.close()


async def test_unknown_effect_survives_continuation_and_explicit_new(tmp_path):
    rt = private_runtime(
        tmp_path / "state.db",
        [
            tool_response(call_id="one"),
            tool_response(call_id="two"),
            final("未重做"),
            tool_response(call_id="three"),
            final("仍未重做"),
        ],
    )
    calls = []

    async def uncertain(args, ctx):
        calls.append(ctx.run.context_id)
        return ToolResult("unknown", "No receipt")

    register_effect(rt, uncertain)
    try:
        first = await rt.coordinator.accept(private("执行", "1"))
        await idle(rt)
        second = await rt.coordinator.accept(private("重试", "2"))
        await idle(rt)
        opened = await rt.coordinator.accept(private("/new", "3"))
        await idle(rt)
        await rt.coordinator.accept(private("相同资源操作", "4"))
        await idle(rt)
        assert first.context_id == second.context_id != opened.context_id
        assert calls == [first.context_id]
        assert rt.store.list_effects()[0]["status"] == "unknown"
    finally:
        await rt.close()


async def test_focus_update_rolls_back_with_failed_attachment(tmp_path, monkeypatch):
    rt = private_runtime(tmp_path / "state.db", [final("A")])
    try:
        first = await rt.coordinator.accept(private("话题", "1"))
        await idle(rt)
        pending = private("追加", "2")
        identity, _ = rt.store.ingest(pending)

        def failure(context_id, input_id):
            raise StateConflict("Injected focus write failure")

        with monkeypatch.context() as scoped:
            scoped.setattr(rt.store, "_set_conversation_focus", failure)
            with pytest.raises(StateConflict):
                rt.store.attach_input(first.context_id, identity, update_focus=True)
        assert rt.store.get_context(first.context_id)["input_seq"] == 1
        assert rt.store.resolve_input(pending.address, pending.message_id) is None
        assert (
            rt.store.conversation_focus(pending.address, pending.author_id)["context_id"]
            == first.context_id
        )
    finally:
        await rt.close()


async def test_closed_focus_starts_new_conversation_without_merging_old_history(tmp_path):
    rt = private_runtime(tmp_path / "state.db", [final("旧答复"), final("新答复")])
    try:
        first = await rt.coordinator.accept(private("旧会话", "1"))
        await idle(rt)
        assert rt.store.control(first.context_id, "101", "close").accepted
        second = await rt.coordinator.accept(private("新问题", "2"))
        await idle(rt)
        assert second.context_id != first.context_id
        assert "旧会话" not in json.dumps(rt.model.requests[-1], ensure_ascii=False)
        assert rt.store.get_context(first.context_id)["control_state"] == "closed"
    finally:
        await rt.close()


async def test_inquiry_answer_sets_responder_focus_only_after_verified_association(tmp_path):
    rt = runtime(tmp_path / "state.db", [])

    async def waiting(messages):
        inquiry = rt.store.list_open_inquiries()[0]
        return tool_response("wait_for_input", {"inquiry_id": inquiry["inquiry_id"]}, "wait")

    rt.model.steps = [
        tool_response(
            "ask_person",
            {"recipient_id": "202", "kind": "group", "target_id": "10", "question": "选一或二"},
            "ask",
        ),
        waiting,
        final("答案已收到"),
        final("选项已延续"),
    ]
    try:
        first = await rt.coordinator.accept(event("需要协作", mid="1"))
        await idle(rt)
        answered = await rt.coordinator.accept(
            event("补充", author="202", mid="2", reply="1001", mentioned=False)
        )
        await idle(rt)
        continuation = await rt.coordinator.accept(event("1", author="202", mid="3"))
        await idle(rt)
        assert first.context_id == answered.context_id == continuation.context_id
        assert "补充" in json.dumps(rt.model.requests[-1], ensure_ascii=False)
    finally:
        await rt.close()


async def test_older_input_retry_cannot_overwrite_new_focus(tmp_path):
    rt = private_runtime(tmp_path / "state.db", [final("A")])
    try:
        first = await rt.coordinator.accept(private("A", "1"))
        await idle(rt)
        new = await rt.coordinator.accept(private("/new", "2"))
        await idle(rt)
        rt.store.attach_input(first.context_id, first.input_id, update_focus=True)
        assert (
            rt.store.conversation_focus(private("", "3").address, "101")["context_id"]
            == new.context_id
        )
    finally:
        await rt.close()


async def test_application_reverse_websocket_keeps_five_turns_and_option_context(tmp_path):
    turns = ["你好", "你的工具列表", "原始工具名", "1", "向我输出你的上下文"]

    class Model:
        count = 0

        async def complete(self, messages, tools):
            current = self.count
            self.count += 1
            text = json.dumps(messages, ensure_ascii=False)
            assert all(turn in text for turn in turns[: current + 1])
            if current == 2:
                return NormalizedResponse("1. read_file\n2. terminal", None, "stop")
            if current == 3:
                assert "read_file" in text and "terminal" in text
                return NormalizedResponse("你选择了 read_file。", None, "stop")
            return NormalizedResponse("已保留本次对话。", None, "stop")

    model = Model()
    app = Application(
        settings(tmp_path), model=model, environ={"CANDIDATE_QQ_TOKEN": "local-secret"}
    )
    port = await app.start()
    try:
        async with connect(f"ws://127.0.0.1:{port}/onebot", additional_headers=headers()) as socket:
            for index, turn in enumerate(turns, 1):
                await socket.send(
                    json.dumps(
                        {
                            "post_type": "message",
                            "message_type": "private",
                            "self_id": 123,
                            "user_id": 456,
                            "message_id": index,
                            "message": [{"type": "text", "data": {"text": turn}}],
                        }
                    )
                )
                packet = json.loads(await asyncio.wait_for(socket.recv(), 2))
                if index == 4:
                    assert "你选择了 read_file" in json.dumps(packet, ensure_ascii=False)
                await socket.send(
                    json.dumps(
                        {
                            "echo": packet["echo"],
                            "status": "ok",
                            "retcode": 0,
                            "data": {"message_id": 100 + index},
                        }
                    )
                )
                await asyncio.wait_for(app.coordinator.wait_idle(), 2)
            contexts = app.store.list_contexts()
            assert len(contexts) == 1 and contexts[0]["input_seq"] == 5 and model.count == 5
            assert not app.coordinator.failures
    finally:
        assert (await app.close()).released
