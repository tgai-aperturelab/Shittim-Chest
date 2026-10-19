"""Display names are not identity, task-association evidence or recipient authorization."""

import json

from shittim_core.qq.codec import decode_event
from tests.integration.test_runtime import event, final, idle, runtime, tool_response


async def test_identical_display_names_keep_distinct_people_and_requests(tmp_path):
    rt = runtime(tmp_path / "state.db", [final("第一条回复"), final("第二条回复")])
    try:
        accepted = []
        for person in (101, 202):
            source = decode_event(
                {
                    "post_type": "message",
                    "message_type": "group",
                    "self_id": 999,
                    "group_id": 10,
                    "user_id": person,
                    "message_id": person,
                    "sender": {"user_id": person, "nickname": "同名成员"},
                    "message": [
                        {"type": "at", "data": {"qq": "999"}},
                        {"type": "text", "data": {"text": "请处理我的独立请求"}},
                    ],
                },
                "999",
            )
            assert source is not None
            accepted.append(await rt.coordinator.accept(source))
            await idle(rt)
        assert accepted[0].context_id != accepted[1].context_id
        assert {rt.store.get_context(item.context_id)["owner_id"] for item in accepted} == {
            "101",
            "202",
        }
        for request, expected in zip(rt.model.requests, ("101", "202"), strict=True):
            assert f"作者QQ={expected}" in json.dumps(request, ensure_ascii=False)
    finally:
        await rt.close()


async def test_ambiguous_display_name_cannot_be_used_as_question_recipient(tmp_path):
    rt = runtime(
        tmp_path / "state.db",
        [
            tool_response(
                "ask_person",
                {
                    "recipient_id": "同名成员",
                    "kind": "group",
                    "target_id": "10",
                    "question": "请补充",
                },
            ),
            final("请提供准确的QQ号，以便确认询问对象。"),
        ],
    )
    try:
        await rt.coordinator.accept(event("问一下同名成员", mid="1"))
        await idle(rt)
        assert not rt.store.list_effects() and not rt.store.list_open_inquiries()
        assert len(rt.qq.calls) == 1
        result = next(m for m in rt.model.requests[-1] if m["role"] == "tool")
        assert json.loads(result["content"])["status"] == "not_executed"
    finally:
        await rt.close()
