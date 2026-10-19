"""Reversed OneBot arrival order must preserve explicit same-author causality."""

import asyncio
import hashlib
import json
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import url2pathname

import httpx
import pytest
from websockets.asyncio.client import connect

from shittim_core.app.service import Application
from shittim_core.domain.model import NormalizedResponse, ToolCall
from shittim_core.domain.types import ConversationAddress, InputEvent, Segment
from tests.integration.test_app import headers, settings
from tests.integration.test_runtime import ADDRESS, event, final, idle, runtime


async def test_file_arriving_after_request_joins_explicit_referencing_context(tmp_path):
    rt = runtime(tmp_path / "state.db", [final("等待附件"), final("已有附件")])
    try:
        request = await rt.coordinator.accept(
            event("处理这个附件", mid="2083064483", reply="212925218")
        )
        await idle(rt)
        attachment = InputEvent(
            ADDRESS,
            "101",
            "212925218",
            "",
            (Segment("file", {"file_id": "f", "name": "input.txt"}),),
        )
        received = await rt.coordinator.accept(attachment)
        assert received.status == "associated"
        assert received.context_id == request.context_id
        await idle(rt)
        assert len(rt.store.list_contexts()) == 1
    finally:
        await rt.close()


def seed_request(rt, name="one", author="101", address=ADDRESS, text="处理所引用附件"):
    source = InputEvent(
        address, author, "request-" + name, text, reply_to="212925218", mentioned_bot=True
    )
    identity, _ = rt.store.ingest(source)
    rt.store.create_context(name, source, rt.coordinator.access_factory(source))
    rt.store.attach_input(name, identity)
    return name


def pure_attachment(author="101", address=ADDRESS):
    return InputEvent(
        address,
        author,
        "212925218",
        "",
        (Segment("file", {"file_id": "fixture-file", "name": "notes.txt"}),),
    )


@pytest.mark.parametrize(
    "case", ["other_author", "other_address", "closed", "policy_denied", "ambiguous"]
)
async def test_reverse_reference_never_guesses_or_bypasses_scope(tmp_path, case):
    rt = runtime(tmp_path / "state.db", [])
    try:
        seed_request(rt)
        source = pure_attachment()
        if case == "other_author":
            source = pure_attachment(author="202")
        elif case == "other_address":
            source = pure_attachment(address=ConversationAddress("999", "group", "11"))
            rt.coordinator.allowed_groups |= {"11"}
        elif case == "closed":
            assert rt.store.control("one", "101", "close").accepted
        elif case == "policy_denied":
            rt.coordinator.policy_predicate = lambda context: False
        else:
            seed_request(rt, "two")
        accepted = await rt.coordinator.accept(source)
        assert accepted.status in {"ignored", "stored"}
        assert accepted.context_id is None
        assert all(
            row["event"].message_id != "212925218"
            for row in rt.store.list_events("one")
            if row["kind"] == "input"
        )
        assert not rt.model.requests
        assert len(rt.store.list_contexts()) == (2 if case == "ambiguous" else 1)
    finally:
        await rt.close()


async def test_paused_request_accepts_late_material_without_resuming_execution(tmp_path):
    rt = runtime(tmp_path / "state.db", [])
    try:
        seed_request(rt)
        assert rt.store.control("one", "101", "stop").accepted
        accepted = await rt.coordinator.accept(pure_attachment())
        assert accepted.status == "associated" and accepted.context_id == "one"
        assert rt.store.get_context("one")["control_state"] == "paused"
        assert len(rt.store.list_events("one")) == 2
        assert not rt.model.requests
    finally:
        await rt.close()


@pytest.mark.parametrize("text", ["新的独立请求", "/stop", "继续"])
async def test_text_and_control_never_take_attachment_shortcut(tmp_path, text):
    rt = runtime(tmp_path / "state.db", [])
    try:
        seed_request(rt)
        source = replace(
            pure_attachment(),
            text=text,
            segments=(*pure_attachment().segments, Segment("text", {"text": text})),
        )
        assert rt.coordinator._late_attachment_reference(source) == (None, "")
    finally:
        await rt.close()


async def test_old_control_reference_does_not_authorize_late_file(tmp_path):
    rt = runtime(tmp_path / "state.db", [])
    try:
        seed_request(rt, text="/resume")
        assert (await rt.coordinator.accept(pure_attachment())).status == "ignored"
        assert not rt.model.requests
    finally:
        await rt.close()


async def test_reverse_reference_survives_restart_from_persisted_inputs(tmp_path):
    path = tmp_path / "state.db"
    old = runtime(path, [])
    seed_request(old)
    await old.close()
    rt = runtime(path, [final("读取已关联附件")])
    try:
        received = await rt.coordinator.accept(pure_attachment())
        assert received.status == "associated" and received.context_id == "one"
        await idle(rt)
        assert len(rt.store.list_contexts()) == 1
    finally:
        await rt.close()


async def test_application_reverse_ws_request_before_file_reads_and_delivers_same_bytes(tmp_path):
    entered, release, quote_seen, location_seen, final_seen = [asyncio.Event() for _ in range(5)]
    contents = "后到的真实附件：金额 0.10 + 0.20 = 0.30\n".encode()
    expected_hash = hashlib.sha256(contents).hexdigest()
    uploaded_hashes, sent_texts = [], []

    class Model:
        def __init__(self):
            self.calls = []
            self.artifact_id = None
            self.read_hash = None

        async def complete(self, messages, tools):
            self.calls.append(messages)
            if len(self.calls) == 1:
                entered.set()
                await release.wait()
                return NormalizedResponse("OLD_RESULT_MUST_NOT_DELIVER", None, "stop")
            last = messages[-1]
            if last["role"] == "tool":
                result = json.loads(last["content"])
                assert result["status"] == "succeeded", result
                if last["tool_call_id"] == "read-file-content":
                    material = json.loads(result["content"])
                    assert material["text"] == contents.decode()
                    self.read_hash = material["sha256"]
                    return NormalizedResponse(
                        None,
                        [
                            ToolCall(
                                "return-file",
                                "send_material",
                                json.dumps({"artifact_id": self.artifact_id}),
                            )
                        ],
                        "tool_calls",
                    )
                return NormalizedResponse("已读取并发回原附件", None, "stop")
            results = [
                message
                for message in messages
                if message.get("_event_id", "").startswith("result:material:")
            ]
            assert results, "The original request must see the late file's material result"
            metadata = json.loads(results[-1]["content"].split("\n", 1)[1])
            assert metadata["status"] == "stored" and metadata["sha256"] == expected_hash
            self.artifact_id = metadata["artifact_id"]
            return NormalizedResponse(
                None,
                [
                    ToolCall(
                        "read-file-content",
                        "read_material",
                        json.dumps({"artifact_id": self.artifact_id}),
                    )
                ],
                "tool_calls",
            )

    model = Model()
    config = settings(
        tmp_path,
        allowed_tools=frozenset({"read_material", "send_material"}),
        allowed_download_hosts=frozenset({"files.example.invalid"}),
        napcat_shared_local_files=True,
        protocol_timeout_seconds=2,
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=contents))
    ) as client:
        app = Application(
            config,
            model=model,
            environ={"CANDIDATE_QQ_TOKEN": "local-secret"},
            download_client=client,
        )
        port = await app.start()
        peer_task = None
        try:
            async with connect(
                f"ws://127.0.0.1:{port}/onebot", additional_headers=headers()
            ) as socket:

                async def peer():
                    async for raw in socket:
                        request = json.loads(raw)
                        action = request["action"]
                        if action == "get_msg":
                            assert str(request["params"]["message_id"]) == "212925218"
                            response = {
                                "self_id": 123,
                                "message_id": 212925218,
                                "message_type": "private",
                                "sender": {"user_id": 456},
                                "message": [
                                    {
                                        "type": "file",
                                        "data": {"file_id": "fixture-file", "name": "notes.txt"},
                                    }
                                ],
                            }
                        elif action == "get_private_file_url":
                            response = {"url": "https://files.example.invalid/notes.txt"}
                        else:
                            assert action == "send_private_msg"
                            for segment in request["params"]["message"]:
                                if segment["type"] == "file":
                                    uri = segment["data"]["file"]
                                    local = Path(url2pathname(urlsplit(uri).path))
                                    uploaded_hashes.append(
                                        hashlib.sha256(local.read_bytes()).hexdigest()
                                    )
                                elif segment["type"] == "text":
                                    sent_texts.append(segment["data"]["text"])
                            response = {"message_id": 1000 + len(sent_texts) + len(uploaded_hashes)}
                        await socket.send(
                            json.dumps(
                                {
                                    "echo": request["echo"],
                                    "status": "ok",
                                    "retcode": 0,
                                    "data": response,
                                }
                            )
                        )
                        if action == "get_msg":
                            quote_seen.set()
                        if action == "get_private_file_url":
                            location_seen.set()
                        if sent_texts and sent_texts[-1] == "已读取并发回原附件":
                            final_seen.set()

                peer_task = asyncio.create_task(peer())
                common = {
                    "post_type": "message",
                    "message_type": "private",
                    "self_id": 123,
                    "user_id": 456,
                }
                await socket.send(
                    json.dumps(
                        {
                            **common,
                            "message_id": 2083064483,
                            "message": [
                                {"type": "reply", "data": {"id": "212925218"}},
                                {"type": "text", "data": {"text": "读完这份资料并把原附件发回"}},
                            ],
                        }
                    )
                )
                await asyncio.wait_for(entered.wait(), 2)
                await asyncio.wait_for(quote_seen.wait(), 2)
                await app.reference_intake.wait_idle()
                original = app.store.list_contexts()[0]["context_id"]
                await socket.send(
                    json.dumps(
                        {
                            **common,
                            "message_id": 212925218,
                            "message": [
                                {
                                    "type": "file",
                                    "data": {"file_id": "fixture-file", "name": "notes.txt"},
                                }
                            ],
                        }
                    )
                )
                await asyncio.wait_for(location_seen.wait(), 2)
                await app.material_intake.wait_idle()
                assert len(app.store.list_contexts()) == 1
                assert (
                    app.store.resolve_input(
                        ConversationAddress("123", "private", "456"), "212925218"
                    )["context_id"]
                    == original
                )
                release.set()
                await asyncio.wait_for(final_seen.wait(), 3)
                await asyncio.wait_for(app.coordinator.wait_idle(), 3)
                assert model.read_hash == expected_hash and uploaded_hashes == [expected_hash]
                assert "OLD_RESULT_MUST_NOT_DELIVER" not in sent_texts
                assert app.store.get_artifact(model.artifact_id)["context_id"] == original
                assert [
                    row["event"].message_id
                    for row in app.store.list_events(original)
                    if row["kind"] == "input"
                ] == ["2083064483", "212925218"]
                assert all(effect["status"] == "succeeded" for effect in app.store.list_effects())
                assert not app.coordinator.failures and not app.material_intake.failures
            await peer_task
        finally:
            release.set()
            if peer_task is not None and not peer_task.done():
                peer_task.cancel()
                await asyncio.gather(peer_task, return_exceptions=True)
            assert (await app.close()).released
