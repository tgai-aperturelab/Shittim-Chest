# SPDX-License-Identifier: AGPL-3.0-or-later
# Behavior fixtures adapted from AstrBot's OneBot files/reply tests; see provenance manifest.
from typing import Any

import pytest

from shittim_core.domain.types import ConversationAddress, ProtocolReceipt, Segment
from shittim_core.qq.codec import (
    decode_event,
    plan_send,
    resolve_files,
    resolve_reply,
    text_segments,
)


def payload(kind: str = "group", segments: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "post_type": "message",
        "message_type": kind,
        "self_id": 123,
        "user_id": 456,
        "group_id": 789 if kind == "group" else None,
        "message_id": -42,
        "time": 100,
        "sender": {"user_id": 456, "nickname": "Test"},
        "message": segments if segments is not None else [{"type": "text", "data": {"text": "Hi"}}],
    }


def test_decode_preserves_identity_order_mention_and_unknown_attachment():
    raw = payload(
        segments=[
            {"type": "text", "data": {"text": "Hi "}},
            {"type": "text", "data": {"text": "there"}},
            {"type": "at", "data": {"qq": "123"}},
            {"type": "reply", "data": {"id": "11"}},
            {"type": "file", "data": {"file_id": "opaque", "name": "a.zip"}},
            {"type": "mface", "data": {"id": "unhandled"}},
        ]
    )
    event = decode_event(raw, "123")
    assert event is not None
    assert event.address == ConversationAddress("123", "group", "789")
    assert event.author_id == "456" and event.message_id == "-42"
    assert event.text == "Hi there" and event.mentioned_bot and event.reply_to == "11"
    assert [s.type for s in event.segments] == ["text", "text", "at", "reply", "file", "mface"]
    assert event.source_time == 100 and event.raw["sender"]["nickname"] == "Test"


@pytest.mark.parametrize(
    "changes",
    [
        {"self_id": 999},
        {"user_id": 123},
        {"post_type": "notice"},
        {"message": "CQ string"},
        {"message": [{"type": "text", "data": None}]},
        {"message_id": None},
        {"user_id": True},
        {"group_id": None},
    ],
)
def test_invalid_or_self_input_is_not_accepted(changes):
    assert decode_event({**payload(), **changes}, "123") is None


@pytest.mark.parametrize("kind", ["group", "private"])
@pytest.mark.parametrize("leading_text", [False, True])
async def test_each_file_uses_its_own_id_and_account(kind, leading_text):
    segments = [
        {"type": "file", "data": {"file_id": "first", "file": "first.pdf"}},
        {"type": "file", "data": {"file_id": "second", "file": "second.xlsx"}},
    ]
    if leading_text:
        segments.insert(0, {"type": "text", "data": {"text": "Read these"}})
    event = decode_event(payload(kind, segments), "123")
    assert event is not None
    calls = []

    async def call(action, params, attempt_id):
        calls.append((action, params, attempt_id))
        return ProtocolReceipt(
            "acknowledged",
            {
                "url": f"https://example.invalid/{params['file_id']}",
                "file_name": f"{params['file_id']}.txt",
            },
        )

    files = await resolve_files(event, call)
    assert [c[1]["file_id"] for c in calls] == ["first", "second"]
    assert all(c[0] == f"get_{kind}_file_url" and c[1]["self_id"] == "123" for c in calls)
    assert ("group_id" in calls[0][1]) == (kind == "group")
    assert [(f.name, f.status) for f in files] == [
        ("first.txt", "located"),
        ("second.txt", "located"),
    ]
    assert len({c[2] for c in calls}) == 2


async def test_file_failure_does_not_remove_source_or_other_files():
    event = decode_event(
        payload(
            segments=[
                {"type": "file", "data": {"file_id": "bad", "name": "bad.pdf"}},
                {
                    "type": "file",
                    "data": {"file_id": "good", "url": "https://example.invalid/good"},
                },
            ]
        ),
        "123",
    )
    assert event is not None

    async def fail(action, params, attempt_id):
        raise OSError("offline")

    result = await resolve_files(event, fail)
    assert result[0].file_id == "bad" and result[0].status == "unavailable"
    assert result[1].status == "located" and len(event.segments) == 2


async def test_reply_is_one_level_and_only_same_group():
    event = decode_event(payload(segments=[{"type": "reply", "data": {"id": "12"}}]), "123")
    assert event is not None
    calls = []

    async def lookup(action, params, attempt_id):
        calls.append(params)
        return ProtocolReceipt(
            "acknowledged",
            {
                "message_id": 12,
                "group_id": 789,
                "sender": {"user_id": 123},
                "message": [
                    {"type": "reply", "data": {"id": "13"}},
                    {"type": "text", "data": {"text": "Earlier"}},
                ],
            },
        )

    result = await resolve_reply(event, lookup)
    assert result is not None and result.status == "resolved" and result.author_id == "123"
    assert result.segments[0].data["id"] == "13" and len(calls) == 1

    async def wrong_group(action, params, attempt_id):
        return ProtocolReceipt(
            "acknowledged",
            {"message_id": 12, "group_id": 999, "sender": {"user_id": 123}, "message": []},
        )

    assert (await resolve_reply(event, wrong_group)).status == "wrong_scope"


async def test_bot_private_quote_requires_trusted_destination_mapping():
    event = decode_event(payload("private", [{"type": "reply", "data": {"id": "12"}}]), "123")
    assert event is not None

    async def lookup(action, params, attempt_id):
        return ProtocolReceipt(
            "acknowledged", {"message_id": 12, "sender": {"user_id": 123}, "message": []}
        )

    assert (await resolve_reply(event, lookup)).status == "wrong_scope"
    assert (await resolve_reply(event, lookup, event.address)).status == "resolved"

    async def unavailable(action, params, attempt_id):
        return ProtocolReceipt("failed")

    failed = await resolve_reply(event, unavailable)
    assert failed.reply_id == "12" and failed.status == "unavailable"


@pytest.mark.parametrize("kind", ["private", "group"])
def test_outgoing_reply_contains_only_id_and_mention_has_space(kind):
    address = ConversationAddress("123", kind, "456")
    result = plan_send(
        address,
        [
            Segment("reply", {"id": "12", "sender_id": "other", "chain": []}),
            Segment("at", {"qq": "789"}),
            Segment("text", {"text": "Hi"}),
        ],
    )
    assert len(result) == 1
    assert result[0].action == f"send_{kind}_msg"
    assert result[0].params["self_id"] == "123"
    assert result[0].params["group_id" if kind == "group" else "user_id"] == 456
    assert result[0].params["message"] == [
        {"type": "reply", "data": {"id": "12"}},
        {"type": "at", "data": {"qq": "789"}},
        {"type": "text", "data": {"text": " "}},
        {"type": "text", "data": {"text": "Hi"}},
    ]


def test_files_are_separate_ordered_items_and_paths_are_uri_encoded():
    result = plan_send(
        ConversationAddress("123", "group", "789"),
        [
            *text_segments("Before"),
            Segment("file", {"file": r"C:\a b\out.txt", "name": "out.txt"}),
            *text_segments("After"),
        ],
    )
    assert len(result) == 3
    assert result[1].params["message"][0]["data"]["file"] == "file:///C:/a%20b/out.txt"
    assert result[2].params["message"][0]["data"]["text"] == "After"
    with pytest.raises(ValueError, match="File source missing"):
        plan_send(ConversationAddress("123", "private", "456"), [Segment("video", {})])


def test_quote_stays_with_text_when_a_file_is_also_sent():
    result = plan_send(
        ConversationAddress("123", "group", "789"),
        [
            *text_segments("Here", reply_to="12"),
            Segment("file", {"file": "https://example.invalid/report.txt", "name": "report.txt"}),
        ],
    )
    assert len(result) == 2
    assert [p["type"] for p in result[0].params["message"]] == ["reply", "text"]
    assert [p["type"] for p in result[1].params["message"]] == ["file"]
