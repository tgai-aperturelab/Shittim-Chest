"""Deterministic routing, early receipts and run lifetime regressions; no live QQ."""

import asyncio
import json

import pytest

from shittim_core.domain.model import NormalizedResponse, ToolCall
from shittim_core.domain.types import ControlCommand, ProtocolReceipt, ToolResult
from tests.integration.test_runtime import (
    event,
    final,
    idle,
    register_effect,
    runtime,
    tool_response,
)


@pytest.mark.asyncio
async def test_clarification_is_durable_once_and_does_not_create_model_work(tmp_path):
    rt = runtime(tmp_path / "state.db", [])
    try:
        accepted = await rt.coordinator.accept(event("/stop", mid="1"))
        await idle(rt)
        repeated = await rt.coordinator.accept(event("/stop", mid="1"))
        await idle(rt)
        assert accepted.status == "needs_clarification"
        assert repeated.status == "duplicate"
        assert len(rt.qq.calls) == 1 and not rt.model.requests
        notices = rt.store.list_contexts()
        assert len(notices) == 1 and notices[0]["kind"] == "router_notice"
        assert rt.coordinator.recover()["active_contexts"] == []
        rt.model.steps.append(final("新请求已收到"))
        followup = await rt.coordinator.accept(event("请解释这个概念", mid="follow", reply="1001"))
        await idle(rt)
        assert followup.context_id != notices[0]["context_id"]
        assert rt.store.get_context(followup.context_id)["kind"] == "work"
        assert len(rt.model.requests) == 1
    finally:
        await rt.close()


@pytest.mark.asyncio
async def test_unknown_notice_is_not_resent_after_reopen(tmp_path):
    path = tmp_path / "state.db"
    rt = runtime(path, [], ["unknown"])
    await rt.coordinator.accept(event("/stop", mid="1"))
    await idle(rt)
    assert len(rt.qq.calls) == 1
    await rt.close()
    reopened = runtime(path, [])
    try:
        reopened.coordinator.recover()
        await reopened.coordinator.recover_unbound_inputs()
        await reopened.delivery.drain_prepared()
        await idle(reopened)
        assert not reopened.qq.calls and not reopened.model.requests
        assert reopened.store.list_deliveries()[0]["status"] == "unknown"
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_explicit_new_while_busy_creates_separate_conversation(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()

    async def first_model(_messages):
        entered.set()
        await release.wait()
        return final("第一条结果")

    rt = runtime(tmp_path / "state.db", [first_model, final("第二条结果")])
    try:
        first = await rt.coordinator.accept(event("处理第一条独立请求"))
        await asyncio.wait_for(entered.wait(), 1)
        opened = await rt.coordinator.accept(event("/new", mid="20"))
        assert opened.reason == "new_conversation"
        second = await rt.coordinator.accept(event("处理第二条独立请求", mid="second"))
        release.set()
        await idle(rt)
        assert first.context_id != second.context_id
        assert len(rt.qq.calls) == 3 and len(rt.model.requests) == 2
        for request in rt.model.requests:
            text = json.dumps(request, ensure_ascii=False)
            assert not ("处理第一条独立请求" in text and "处理第二条独立请求" in text)
    finally:
        release.set()
        await rt.close()


@pytest.mark.asyncio
async def test_qq_stop_feedback_keeps_control_out_of_model_history(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()

    async def waiting(_messages):
        entered.set()
        await release.wait()
        return final("过期结果")

    rt = runtime(tmp_path / "state.db", [waiting, final("继续后的结果")])
    try:
        first = await rt.coordinator.accept(event("原始请求", mid="1"))
        await asyncio.wait_for(entered.wait(), 1)
        stop = await rt.coordinator.accept(event("/stop", mid="2", reply="1"))
        await idle(rt)
        assert stop.status == "associated"
        assert rt.store.get_context(first.context_id)["control_state"] == "paused"
        assert len(rt.qq.calls) == 1
        assert rt.store.list_deliveries()[0]["intent"].purpose == "control"
        await rt.coordinator.accept(event("/resume", mid="3", reply="1"))
        await idle(rt)
        assert len(rt.model.requests) == 2
        text = json.dumps(rt.model.requests[-1], ensure_ascii=False)
        assert "/stop" not in text and "/resume" not in text
        assert "原始请求" in text and not rt.coordinator.failures
    finally:
        release.set()
        await rt.close()


@pytest.mark.asyncio
async def test_direct_resume_before_stopped_task_settles_is_not_lost(tmp_path):
    entered = asyncio.Event()

    async def waiting(_messages):
        entered.set()
        await asyncio.Event().wait()

    rt = runtime(tmp_path / "state.db", [waiting, final("已继续")])
    try:
        first = await rt.coordinator.accept(event("请求"))
        await asyncio.wait_for(entered.wait(), 1)
        await rt.coordinator.control(ControlCommand(first.context_id, "101", "stop"))
        await rt.coordinator.control(ControlCommand(first.context_id, "101", "resume"))
        await idle(rt)
        assert len(rt.model.requests) == 2 and len(rt.qq.calls) == 1
        assert not rt.coordinator._resume_requested
        assert not rt.coordinator.failures
    finally:
        await rt.close()


@pytest.mark.asyncio
async def test_old_tool_observer_survives_a_new_run_in_same_context(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()
    rt = runtime(tmp_path / "state.db", [tool_response(), final("尚待旧操作结果")])

    async def perform(_args, _ctx):
        entered.set()
        await release.wait()
        return ToolResult("succeeded", "真实迟到结果")

    register_effect(rt, perform)
    try:
        first = await rt.coordinator.accept(event("执行"))
        await asyncio.wait_for(entered.wait(), 1)
        await rt.coordinator.control(ControlCommand(first.context_id, "101", "stop"))
        await idle(rt)
        assert len(rt.coordinator._run_controls) == 1
        await rt.coordinator.control(ControlCommand(first.context_id, "101", "resume"))
        await idle(rt)
        assert len(rt.coordinator._run_controls) == 1
        release.set()
        assert await rt.coordinator.close()
        assert rt.store.list_effects()[0]["status"] == "succeeded"
        assert not rt.coordinator._run_controls
    finally:
        release.set()
        await rt.close()


@pytest.mark.asyncio
async def test_inquiry_answer_before_ack_is_stored_then_correlated_once(tmp_path):
    rt = runtime(tmp_path / "state.db", [])
    rt.model.steps = [
        tool_response(
            "ask_person",
            {"recipient_id": "202", "kind": "group", "target_id": "10", "question": "请补充"},
            "ask",
        ),
        final("收到真实答复"),
    ]
    sends = []

    async def early_reply(action, params, attempt_id):
        sends.append(attempt_id)
        if len(sends) == 1:
            accepted = await rt.coordinator.accept(
                event("真实早到答复", author="202", mid="early", reply="5001", mentioned=False)
            )
            assert accepted.status == "stored" and accepted.context_id is None
            assert len(rt.store.list_unbound_inputs()) == 1
            return ProtocolReceipt("acknowledged", message_id="5001")
        return ProtocolReceipt("acknowledged", message_id="5002")

    rt.delivery.call = early_reply
    try:
        first = await rt.coordinator.accept(event("询问另一成员"))
        await idle(rt)
        assert len(sends) == 2 and len(rt.model.requests) == 2
        assert rt.store.list_open_inquiries() == []
        assert rt.store.list_unbound_inputs() == []
        assert rt.store.resolve_input(event("").address, "early")["context_id"] == first.context_id
        assert "真实早到答复" in json.dumps(rt.model.requests[-1], ensure_ascii=False)
        repeated = await rt.coordinator.accept(
            event("真实早到答复", author="202", mid="early", reply="5001", mentioned=False)
        )
        assert repeated.status == "duplicate"
        await idle(rt)
        assert len(rt.model.requests) == 2 and not rt.coordinator.failures
    finally:
        await rt.close()


@pytest.mark.asyncio
async def test_incomplete_model_response_records_failure_and_sends_plain_status(tmp_path):
    rt = runtime(
        tmp_path / "state.db",
        [NormalizedResponse(None, [ToolCall("broken", "perform", '{"secret":')], "tool_calls")],
    )
    try:
        first = await rt.coordinator.accept(event("执行请求"))
        await idle(rt)
        assert not rt.store.list_effects()
        delivery = rt.store.list_deliveries()[0]
        run = rt.store.get_run(delivery["run_id"])
        assert run["context_id"] == first.context_id and run["status"] == "failed"
        assert run["outcome"]["reason"] == "ModelProtocolError"
        assert "未能完成" in delivery["intent"].text
        assert "secret" not in json.dumps(rt.qq.calls)
        assert len(rt.qq.calls) == 1
    finally:
        await rt.close()


@pytest.mark.asyncio
async def test_unknown_effect_gets_uncertain_feedback_not_success_or_retry(tmp_path):
    rt = runtime(tmp_path / "state.db", [tool_response()])

    async def perform(_args, _ctx):
        return ToolResult("unknown", "pending external evidence")

    register_effect(rt, perform)
    try:
        await rt.coordinator.accept(event("执行请求"))
        await idle(rt)
        effect = rt.store.list_effects()[0]
        assert effect["status"] == "unknown"
        run = rt.store.get_run(effect["run_id"])
        assert run["status"] == "yielded" and run["outcome"]["reason"] == "unknown"
        assert effect["effect_id"] in run["outcome"]["unresolved_effects"]
        assert len(rt.qq.calls) == 1 and len(rt.model.requests) == 1
        assert "尚未确认" in rt.store.list_deliveries()[0]["intent"].text
    finally:
        await rt.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["cancel", "replace"])
async def test_inactive_inquiry_answer_is_audit_only_and_does_not_wake_work(tmp_path, action):
    rt = runtime(tmp_path / "state.db", [])

    async def yield_after_question(_messages):
        question = rt.store.list_open_inquiries()[0]
        return tool_response("wait_for_input", {"inquiry_id": question["inquiry_id"]}, "wait")

    rt.model.steps = [
        tool_response(
            "ask_person",
            {"recipient_id": "202", "kind": "group", "target_id": "10", "question": "旧问题"},
            "ask",
        ),
        yield_after_question,
    ]
    try:
        first = await rt.coordinator.accept(event("需要补充", mid="1"))
        await idle(rt)
        question = rt.store.list_open_inquiries()[0]
        rt.store.claim_run(first.context_id, "manage")
        rt.store.consume_inputs("manage", 1)
        if action == "cancel":
            rt.store.cancel_inquiry("manage", question["inquiry_id"], 1)
        else:
            rt.store.replace_inquiry(
                "manage", question["inquiry_id"], "202", event("").address, "新问题", 1
            )
        rt.store.finish_run("manage", "yielded")
        accepted = await rt.coordinator.accept(
            event("旧问题的迟到答复", author="202", mid="2", reply="1001", mentioned=False)
        )
        await idle(rt)
        assert accepted.status == "stored" and accepted.reason == "inactive_inquiry_answer"
        assert len(rt.store.list_late_answers(context_id=first.context_id)) == 1
        assert rt.store.get_context(first.context_id)["input_seq"] == 1
        assert len(rt.model.requests) == 2
        assert rt.coordinator.recover()["active_contexts"] == []
        assert "旧问题的迟到答复" not in json.dumps(
            rt.store.history(first.context_id), ensure_ascii=False
        )
        assert len(rt.qq.calls) == 2  # Original inquiry and its factual late-answer notice.
    finally:
        await rt.close()


@pytest.mark.asyncio
async def test_multiple_questions_same_person_project_the_verified_answer_target(tmp_path):
    rt = runtime(tmp_path / "state.db", [])

    async def wait_second(_messages):
        second = next(q for q in rt.store.list_open_inquiries() if q["question"] == "问题二")
        return tool_response("wait_for_input", {"inquiry_id": second["inquiry_id"]}, "wait")

    rt.model.steps = [
        tool_response(
            "ask_person",
            {"recipient_id": "202", "kind": "group", "target_id": "10", "question": "问题一"},
            "ask1",
        ),
        tool_response(
            "ask_person",
            {"recipient_id": "202", "kind": "group", "target_id": "10", "question": "问题二"},
            "ask2",
        ),
        wait_second,
        final("按明确答复继续"),
    ]
    try:
        await rt.coordinator.accept(event("需要两项信息", mid="1"))
        await idle(rt)
        question = next(q for q in rt.store.list_open_inquiries() if q["question"] == "问题二")
        await rt.coordinator.accept(
            event("第二项答复", author="202", mid="2", reply="1002", mentioned=False)
        )
        await idle(rt)
        answer = next(
            m
            for m in rt.model.requests[-1]
            if m.get("_input_id") == rt.store.get_inquiry(question["inquiry_id"])["answer_input_id"]
        )
        assert question["inquiry_id"] in answer["content"] and "问题二" in answer["content"]
        assert "已核验答复关联" in answer["content"] and "问题一" not in answer["content"]
        assert len(rt.store.list_open_inquiries()) == 1 and len(rt.model.requests) == 4
    finally:
        await rt.close()


@pytest.mark.asyncio
async def test_answer_to_paused_question_is_recorded_until_owner_resumes(tmp_path):
    rt = runtime(tmp_path / "state.db", [])

    async def wait_question(_messages):
        return tool_response(
            "wait_for_input",
            {"inquiry_id": rt.store.list_open_inquiries()[0]["inquiry_id"]},
            "wait",
        )

    rt.model.steps = [
        tool_response(
            "ask_person",
            {"recipient_id": "202", "kind": "group", "target_id": "10", "question": "请回答"},
            "ask",
        ),
        wait_question,
        final("明确继续后的结果"),
    ]
    try:
        first = await rt.coordinator.accept(event("原始请求", mid="1"))
        await idle(rt)
        await rt.coordinator.accept(event("/stop", mid="2", reply="1"))
        await idle(rt)
        question = rt.store.list_open_inquiries()[0]
        await rt.coordinator.accept(
            event("已暂停时的答复", author="202", mid="3", reply="1001", mentioned=False)
        )
        await idle(rt)
        assert rt.store.get_inquiry(question["inquiry_id"])["status"] == "answered"
        assert rt.store.get_context(first.context_id)["control_state"] == "paused"
        assert len(rt.model.requests) == 2
        await rt.coordinator.accept(event("/resume", mid="4", reply="1"))
        await idle(rt)
        assert len(rt.model.requests) == 3
        assert "已暂停时的答复" in json.dumps(rt.model.requests[-1], ensure_ascii=False)
    finally:
        await rt.close()


@pytest.mark.asyncio
async def test_other_participant_cannot_stop_owner_work_by_quoting_it(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()

    async def waiting(_messages):
        entered.set()
        await release.wait()
        return final("发起者的正常结果")

    rt = runtime(tmp_path / "state.db", [waiting])
    try:
        first = await rt.coordinator.accept(event("发起者请求", mid="1"))
        await asyncio.wait_for(entered.wait(), 1)
        denial = await rt.coordinator.accept(event("/stop", author="202", mid="2", reply="1"))
        assert denial.status == "rejected"
        assert rt.store.get_context(first.context_id)["control_state"] == "active"
        assert rt.store.get_context(first.context_id)["input_seq"] == 1
        release.set()
        await idle(rt)
        assert len(rt.model.requests) == 1 and len(rt.qq.calls) == 2
    finally:
        release.set()
        await rt.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_before_reply", [False, True])
async def test_inflight_quote_reclassified_after_receipt_even_if_cancelled_or_unmatched(
    tmp_path, cancel_before_reply
):
    rt = runtime(tmp_path / "state.db", [] if cancel_before_reply else [final("独立请求的结果")])
    original = event("原始询问来源", mid="1")
    source, _ = rt.store.ingest(original)
    rt.store.create_context("question-context", original, rt.coordinator.access_factory(original))
    rt.store.attach_input("question-context", source)
    rt.store.claim_run("question-context", "question-run")
    rt.store.consume_inputs("question-run", 1)
    inquiry_id, delivery_id = rt.store.create_inquiry(
        "question-run", "202", original.address, "旧问题", 1
    )
    entered, release = asyncio.Event(), asyncio.Event()
    attempts = []

    async def peer(_action, _params, attempt):
        attempts.append(attempt)
        if attempt == delivery_id:
            entered.set()
            await release.wait()
            return ProtocolReceipt("acknowledged", message_id="6001")
        return ProtocolReceipt("acknowledged", message_id=str(6000 + len(attempts)))

    rt.delivery.call = peer
    send = asyncio.create_task(rt.delivery.send(delivery_id))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        if cancel_before_reply:
            rt.store.cancel_inquiry("question-run", inquiry_id, 1)
        early = await rt.coordinator.accept(
            event(
                "真实迟到答复" if cancel_before_reply else "另一条独立请求",
                author="202",
                mid="2",
                reply="6001" if cancel_before_reply else "9999",
                mentioned=not cancel_before_reply,
            )
        )
        assert early.status == "stored" and len(rt.store.list_unbound_inputs()) == 1
        release.set()
        await send
        await idle(rt)
        assert rt.store.list_unbound_inputs() == []
        assert early.input_id not in rt.coordinator.deferred_inputs
        if cancel_before_reply:
            assert len(rt.store.list_late_answers(inquiry_id)) == 1
            assert not rt.model.requests
        else:
            routed = rt.store.resolve_input(original.address, "2")
            assert routed["context_id"] != "question-context"
            assert len(rt.model.requests) == 1
        assert len(attempts) == 2 and not rt.delivery.listener_failures
        rt.store.finish_run("question-run", "yielded")
    finally:
        release.set()
        await asyncio.gather(send, return_exceptions=True)
        await rt.close()


async def test_unmatched_unmentioned_early_quote_never_becomes_an_agent_trigger(tmp_path):
    rt = runtime(tmp_path / "state.db", [final("不应被群闲聊触发")])
    original = event("请询问另一位成员", mid="1")
    identity, _ = rt.store.ingest(original)
    rt.store.create_context("q", original, rt.coordinator.access_factory(original))
    rt.store.attach_input("q", identity)
    rt.store.claim_run("q", "r")
    rt.store.consume_inputs("r", 1)
    inquiry, delivery = rt.store.create_inquiry("r", "202", original.address, "问题", 1)
    started, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def peer(action, params, attempt):
        calls.append(attempt)
        if attempt == delivery:
            started.set()
            await release.wait()
        return ProtocolReceipt("acknowledged", message_id=str(6000 + len(calls)))

    rt.delivery.call = peer
    sending = asyncio.create_task(rt.delivery.send(delivery))
    try:
        await asyncio.wait_for(started.wait(), 1)
        unrelated = event(
            "回复其他成员的普通群聊", author="202", mid="2", reply="9999", mentioned=False
        )
        early = await rt.coordinator.accept(unrelated)
        assert early.status == "stored"
        release.set()
        await sending
        await idle(rt)
        assert rt.store.resolve_input(original.address, "2") is None
        assert not rt.model.requests and calls == [delivery]
        assert rt.store.get_inquiry(inquiry)["status"] == "open"
        await rt.coordinator.recover_unbound_inputs()
        await idle(rt)
        assert not rt.model.requests and calls == [delivery]
        rt.store.finish_run("r", "yielded")
    finally:
        release.set()
        await asyncio.gather(sending, return_exceptions=True)
        await rt.close()


async def test_recovery_keeps_an_explicitly_resolved_group_reply_without_new_mention(tmp_path):
    rt = runtime(tmp_path / "state.db", [final("已恢复明确关联的回复")])
    try:
        original = event("已有请求", mid="1")
        identity, _ = rt.store.ingest(original)
        rt.store.create_context("known", original, rt.coordinator.access_factory(original))
        rt.store.attach_input("known", identity)
        source = event("崩溃前已收到的明确补充", author="202", mid="2", reply="1", mentioned=False)
        input_id, _ = rt.store.ingest(source)
        recovered = await rt.coordinator.recover_unbound_inputs()
        await idle(rt)
        assert len(recovered) == 1 and recovered[0].status == "associated"
        assert recovered[0].input_id == input_id and recovered[0].context_id == "known"
        assert len(rt.model.requests) == 1
        assert "明确补充" in json.dumps(rt.model.requests[0], ensure_ascii=False)
        assert not rt.store.list_unbound_inputs()
    finally:
        await rt.close()
