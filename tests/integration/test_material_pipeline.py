"""File metadata -> durable result event -> scoped read; all HTTP/QQ endpoints are substitutes."""

import asyncio
import json
from dataclasses import replace
from pathlib import Path

import httpx
import pytest
from websockets.asyncio.client import connect

from shittim_core.app.config import Settings
from shittim_core.app.material_intake import MaterialIntake
from shittim_core.app.service import Application
from shittim_core.context.builder import ContextBuilder
from shittim_core.domain.model import NormalizedResponse, ToolCall
from shittim_core.domain.types import (
    Acceptance,
    AccessContext,
    ConversationAddress,
    ExecutionContext,
    InputEvent,
    ProtocolReceipt,
    RunRequest,
    Segment,
)
from shittim_core.materials.store import MaterialStore
from shittim_core.state import StateStore
from shittim_core.tools.executor import ToolExecutor
from shittim_core.tools.material_tools import register_material_tools


def seed(store, context_id="ctx", text="读取附件", author="456", name="notes.txt"):
    source = InputEvent(
        ConversationAddress("123", "private", author),
        author,
        "1",
        text,
        (Segment("file", {"file_id": "real-file-id", "name": name}),),
    )
    input_id, _ = store.ingest(source)
    access = AccessContext(
        author,
        source.address.key,
        frozenset({"read_material"}),
        frozenset({"materials:" + context_id}),
    )
    store.create_context(context_id, source, access)
    store.attach_input(context_id, input_id)
    return source, Acceptance("associated", input_id, context_id), access


async def locate(action, params, attempt_id):
    assert action == "get_private_file_url" and params["file_id"] == "real-file-id"
    assert params["self_id"] == "123"
    return ProtocolReceipt("acknowledged", {"url": "https://files.example.invalid/content"})


@pytest.mark.parametrize(
    "content,name,parsed",
    [
        ("材料正文：alpha".encode(), "notes.txt", True),
        (b"%PDF-1.7 Fake PDF", "notes.pdf", False),
        (b"\x00\x01binary", "notes.txt", False),
    ],
)
async def test_download_result_is_metadata_and_tool_explicitly_reads_scoped_material(
    tmp_path,
    content,
    name,
    parsed,
):
    store = StateStore(tmp_path / "state.db")
    event, accepted, access = seed(store, name=name)
    notifications, requests = [], []

    async def notify(context_id):
        notifications.append(context_id)

    def http_handler(request):
        requests.append(str(request.url))
        return httpx.Response(200, content=content)

    async with httpx.AsyncClient(transport=httpx.MockTransport(http_handler)) as client:
        intake = MaterialIntake(
            store,
            tmp_path / "files",
            locate,
            notify,
            client=client,
            allowed_hosts=frozenset({"files.example.invalid"}),
            max_bytes=1000,
            policy_check=lambda _: True,
        )
        assert intake.submit(event, accepted)
        await intake.wait_idle()
        rows = store.list_events("ctx")
        assert [r["kind"] for r in rows] == ["input", "material_result"]
        assert rows[1]["event"] is None and rows[1]["source_ids"] == [accepted.input_id]
        metadata = rows[1]["payload"]
        assert metadata["status"] == "stored" and metadata["content_read"] is False
        assert metadata["provider_id"] == "456"
        assert "text" not in metadata and "url" not in metadata
        assert notifications == ["ctx"] and len(requests) == 1
        assert not intake.submit(event, Acceptance("duplicate", accepted.input_id, "ctx"))
        assert not intake.submit(event, accepted)
        assert intake.recover() == 0
        epoch = store.claim_run("ctx", "reader")
        run = RunRequest("reader", "ctx", event.address, access, epoch)
        view = await ContextBuilder(store).build(run)
        assert metadata["artifact_id"] in json.dumps(view.messages)
        assert "材料正文：alpha" not in json.dumps(view.messages, ensure_ascii=False)
        tools = ToolExecutor(store)
        register_material_tools(tools, store, tmp_path / "files", max_bytes=1000)
        call = ToolCall(
            "read1", "read_material", json.dumps({"artifact_id": metadata["artifact_id"]})
        )
        store.append_history(
            "reader",
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "read1",
                        "type": "function",
                        "function": {"name": call.name, "arguments": call.arguments},
                    }
                ],
            },
        )
        result = await tools.execute(
            call, ExecutionContext(run, view.input_seq, view.history_version)
        )
        assert result.status == "succeeded"
        decoded = json.loads(result.content)
        assert decoded["parsed_as_text"] is parsed
        assert decoded["source_kind"] == "untrusted_material"
        if parsed:
            assert decoded["text"] == content.decode()
        else:
            assert "text" not in decoded
        await intake.close()
    store.close()


async def test_blocked_download_records_failure_without_http_and_control_input_is_skipped(tmp_path):
    store = StateStore(tmp_path / "state.db")
    event, accepted, _ = seed(store)
    requests, notifications = [], []

    async def notify(context_id):
        notifications.append(context_id)

    def forbidden(request):
        requests.append(request)
        raise AssertionError("Host rejection must precede HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
        intake = MaterialIntake(
            store,
            tmp_path / "files",
            locate,
            notify,
            client=client,
            allowed_hosts=frozenset({"other.example.invalid"}),
            max_bytes=1000,
            policy_check=lambda _: True,
        )
        intake.submit(event, accepted)
        await intake.wait_idle()
        result = store.list_events("ctx")[-1]
        assert result["kind"] == "material_result" and result["payload"]["status"] == "unavailable"
        assert result["payload"]["reason"] == "url_not_allowed" and requests == []
        stopped, stopped_acceptance, _ = seed(store, "control", text="/stop", author="789")
        assert not intake.submit(stopped, stopped_acceptance)
        assert len(store.list_events("control")) == 1
        await intake.close()
    store.close()


async def test_paused_context_keeps_completed_material_but_is_not_resumed(tmp_path):
    store = StateStore(tmp_path / "state.db")
    event, accepted, _ = seed(store)
    entered, release = asyncio.Event(), asyncio.Event()
    observed_states = []

    async def handler(request):
        entered.set()
        await release.wait()
        return httpx.Response(200, content=b"later material")

    async def notify(context_id):
        observed_states.append(store.get_context(context_id)["control_state"])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        intake = MaterialIntake(
            store,
            tmp_path / "files",
            locate,
            notify,
            client=client,
            allowed_hosts=frozenset({"files.example.invalid"}),
            max_bytes=1000,
            policy_check=lambda _: True,
        )
        intake.submit(event, accepted)
        await asyncio.wait_for(entered.wait(), 1)
        store.control("ctx", "456", "stop")
        release.set()
        await intake.wait_idle()
        assert store.list_events("ctx")[-1]["payload"]["status"] == "stored"
        assert observed_states == ["paused"]
        assert store.get_context("ctx")["control_state"] == "paused"
        await intake.close()
    store.close()


async def test_read_material_cannot_cross_context_or_accept_model_path(tmp_path):
    store = StateStore(tmp_path / "state.db")
    first, accepted, _ = seed(store)

    async def notify(_):
        pass

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"private content"))
    ) as client:
        intake = MaterialIntake(
            store,
            tmp_path / "files",
            locate,
            notify,
            client=client,
            allowed_hosts=frozenset({"files.example.invalid"}),
            max_bytes=1000,
            policy_check=lambda _: True,
        )
        intake.submit(first, accepted)
        await intake.wait_idle()
        artifact_id = store.list_events("ctx")[-1]["payload"]["artifact_id"]
        second, _, access = seed(store, "another", author="789")
        epoch = store.claim_run("another", "wrong-reader")
        run = RunRequest("wrong-reader", "another", second.address, access, epoch)
        tools = ToolExecutor(store)
        register_material_tools(tools, store, tmp_path / "files")
        for number, args in enumerate(
            ({"artifact_id": artifact_id}, {"artifact_id": artifact_id, "path": str(tmp_path)})
        ):
            result = await tools.execute(
                ToolCall(str(number), "read_material", json.dumps(args)),
                ExecutionContext(run, 1, 0),
            )
            assert result.status == "not_executed"
            assert "private content" not in result.content
        await intake.close()
    store.close()


class ReadingModel:
    def __init__(self):
        self.calls = 0

    async def complete(self, messages, tools):
        self.calls += 1
        if messages[-1]["role"] == "tool":
            tool_result = json.loads(messages[-1]["content"])
            material = json.loads(tool_result["content"])
            return NormalizedResponse("读取结果：" + material["text"], None, "stop")
        results = [m for m in messages if m.get("_event_id", "").startswith("result:material:")]
        if not results:
            return NormalizedResponse("材料尚未取得", None, "stop")
        metadata = json.loads(results[-1]["content"].split("\n", 1)[1])
        return NormalizedResponse(
            "",
            [
                ToolCall(
                    "read-content",
                    "read_material",
                    json.dumps({"artifact_id": metadata["artifact_id"]}),
                )
            ],
            "tool_calls",
        )


async def test_application_download_completion_wakes_new_run_and_reads_actual_bytes(tmp_path):
    release = asyncio.Event()

    async def handler(request):
        await release.wait()
        return httpx.Response(200, content="真正的文件内容".encode())

    config = Settings(
        "123",
        "QQ_TOKEN",
        "scripted",
        "https://example.invalid/v1",
        "MODEL_KEY",
        tmp_path / "app",
        port=0,
        allowed_users=frozenset({"456"}),
        allowed_tools=frozenset({"read_material"}),
        allowed_download_hosts=frozenset({"files.example.invalid"}),
    )
    model = ReadingModel()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        app = Application(
            config, model=model, environ={"QQ_TOKEN": "local-secret"}, download_client=client
        )
        port = await app.start()
        try:
            async with connect(
                f"ws://127.0.0.1:{port}/onebot",
                additional_headers={"Authorization": "Bearer local-secret", "X-Self-ID": "123"},
            ) as socket:
                await app.recovery_complete.wait()
                await socket.send(
                    json.dumps(
                        {
                            "post_type": "message",
                            "message_type": "private",
                            "self_id": 123,
                            "user_id": 456,
                            "message_id": 1,
                            "message": [
                                {"type": "text", "data": {"text": "读取附件"}},
                                {
                                    "type": "file",
                                    "data": {"file_id": "real-file-id", "name": "notes.txt"},
                                },
                            ],
                        }
                    )
                )
                first_reply = None
                location_seen = False
                while first_reply is None or not location_seen:
                    request = json.loads(await asyncio.wait_for(socket.recv(), 2))
                    if request["action"] == "get_private_file_url":
                        assert request["params"]["file_id"] == "real-file-id"
                        location_seen = True
                        data = {"url": "https://files.example.invalid/content"}
                    else:
                        first_reply = request["params"]["message"][0]["data"]["text"]
                        data = {"message_id": 70}
                    await socket.send(
                        json.dumps(
                            {"echo": request["echo"], "status": "ok", "retcode": 0, "data": data}
                        )
                    )
                assert first_reply == "材料尚未取得"
                await app.coordinator.wait_idle()
                release.set()
                final = json.loads(await asyncio.wait_for(socket.recv(), 3))
                assert final["action"] == "send_private_msg"
                assert final["params"]["message"][0]["data"]["text"] == "读取结果：真正的文件内容"
                await socket.send(
                    json.dumps(
                        {
                            "echo": final["echo"],
                            "status": "ok",
                            "retcode": 0,
                            "data": {"message_id": 71},
                        }
                    )
                )
                await app.material_intake.wait_idle()
                await app.coordinator.wait_idle()
                context = app.store.list_contexts()[0]
                assert [r["kind"] for r in app.store.list_events(context["context_id"])] == [
                    "input",
                    "material_result",
                ]
                assert model.calls == 3 and not app.material_intake.failures
        finally:
            release.set()
            await app.close()


async def test_download_remains_disabled_without_both_explicit_grants(tmp_path):
    for index, (tools, hosts) in enumerate(
        (
            (frozenset(), frozenset({"files.example.invalid"})),
            (frozenset({"read_material"}), frozenset()),
        )
    ):
        config = Settings(
            "123",
            "QQ_TOKEN",
            "scripted",
            "https://example.invalid/v1",
            "MODEL_KEY",
            tmp_path / str(index),
            port=0,
            allowed_tools=tools,
            allowed_download_hosts=hosts,
        )
        app = Application(config, model=ReadingModel(), environ={"QQ_TOKEN": "local-secret"})
        await app.start()
        assert app.material_intake is None
        await app.close()


async def test_shutdown_records_interruption_without_waking_new_execution(tmp_path):
    store = StateStore(tmp_path / "state.db")
    event, accepted, _ = seed(store)
    entered = asyncio.Event()
    notifications = []

    async def blocked_http(_request):
        entered.set()
        await asyncio.Event().wait()
        return httpx.Response(200, content=b"not reached")

    async def notify(context_id):
        notifications.append(context_id)

    async with httpx.AsyncClient(transport=httpx.MockTransport(blocked_http)) as client:
        intake = MaterialIntake(
            store,
            tmp_path / "files",
            locate,
            notify,
            client=client,
            allowed_hosts=frozenset({"files.example.invalid"}),
            max_bytes=1000,
            policy_check=lambda _: True,
        )
        intake.submit(event, accepted)
        await asyncio.wait_for(entered.wait(), 1)
        await intake.close()
        result = store.list_events("ctx")[-1]["payload"]
        assert result["status"] == "unavailable" and result["reason"] == "intake_interrupted"
        assert notifications == [] and list((tmp_path / "files").iterdir()) == []
    store.close()


async def test_size_cap_preserves_source_and_produces_failed_result(tmp_path):
    store = StateStore(tmp_path / "state.db")
    event, accepted, _ = seed(store)

    async def notify(_context_id):
        pass

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"oversized"))
    ) as client:
        intake = MaterialIntake(
            store,
            tmp_path / "files",
            locate,
            notify,
            client=client,
            allowed_hosts=frozenset({"files.example.invalid"}),
            max_bytes=3,
            policy_check=lambda _: True,
        )
        intake.submit(event, accepted)
        await intake.wait_idle()
        rows = store.list_events("ctx")
        assert rows[0]["event"].segments[0].data["file_id"] == "real-file-id"
        assert rows[1]["payload"]["status"] == "unavailable"
        assert "artifact_id" not in rows[1]["payload"]
        assert list((tmp_path / "files").iterdir()) == []
        await intake.close()
    store.close()


def test_file_send_requires_explicit_shared_local_declaration(tmp_path):
    with pytest.raises(ValueError, match="shared-local"):
        Settings(
            "123",
            "QQ_TOKEN",
            "scripted",
            "https://example.invalid/v1",
            "MODEL_KEY",
            tmp_path,
            allowed_tools=frozenset({"send_material"}),
        )


@pytest.mark.parametrize("receipt_status", ["acknowledged", "unknown"])
async def test_authorized_file_send_pins_version_and_unknown_is_not_replayed(
    tmp_path, receipt_status
):
    config = Settings(
        "123",
        "QQ_TOKEN",
        "scripted",
        "https://example.invalid/v1",
        "MODEL_KEY",
        tmp_path / "app",
        port=0,
        allowed_users=frozenset({"456"}),
        allowed_tools=frozenset({"send_material"}),
        napcat_shared_local_files=True,
    )
    app = Application(config, model=ReadingModel(), environ={"QQ_TOKEN": "local-secret"})
    await app.start()
    calls = []

    async def fake_qq(action, params, attempt):
        calls.append((action, params, attempt))
        return ProtocolReceipt(
            receipt_status,
            {"message_id": 88} if receipt_status == "acknowledged" else {},
            "88" if receipt_status == "acknowledged" else None,
        )

    try:
        app.delivery.call = fake_qq
        event = InputEvent(
            ConversationAddress("123", "private", "456"), "456", "send-source", "发文件"
        )
        input_id, _ = app.store.ingest(event)
        access = replace(
            app.access_for(event),
            allowed_resources=app.access_for(event).allowed_resources | {"materials:send-context"},
        )
        app.store.create_context("send-context", event, access)
        app.store.attach_input("send-context", input_id)
        materials = MaterialStore(config.data_dir / "materials", app.store, "send-context")
        first = await materials.add_bytes(
            b"original file", "report.txt", access, source_ref=input_id
        )
        await materials.add_bytes(
            b"new version",
            "report.txt",
            access,
            artifact_id=first.artifact_id,
            expected_version=1,
            source_ref=input_id,
        )
        epoch = app.store.claim_run("send-context", "sender")
        run = RunRequest("sender", "send-context", event.address, access, epoch)
        call = ToolCall(
            "send-file",
            "send_material",
            json.dumps({"artifact_id": first.artifact_id, "version": 1}),
        )
        app.store.append_history(
            "sender",
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": call.id,
                        "type": "function",
                        "function": {"name": call.name, "arguments": call.arguments},
                    }
                ],
            },
        )
        ctx = ExecutionContext(run, 1, 0)
        result = await app.tools.execute(call, ctx)
        assert result.status == ("succeeded" if receipt_status == "acknowledged" else "unknown")
        assert result.artifact_refs == (first.artifact_id + "@1",)
        assert len(calls) == 1 and calls[0][0] == "send_private_msg"
        assert calls[0][1]["user_id"] == 456
        local_path = app.store.get_artifact(first.artifact_id, version=1)["path"]
        assert calls[0][1]["message"] == [
            {"type": "file", "data": {"file": Path(local_path).as_uri(), "name": "report.txt"}}
        ]
        if receipt_status == "unknown":
            second = await app.tools.execute(
                ToolCall("new-model-id", "send_material", call.arguments), ctx
            )
            assert second.status == "not_executed" and len(calls) == 1
            assert len(app.store.list_deliveries("unknown")) == 1
    finally:
        await app.close()


async def test_file_send_denies_other_context_and_model_supplied_path(tmp_path):
    config = Settings(
        "123",
        "QQ_TOKEN",
        "scripted",
        "https://example.invalid/v1",
        "MODEL_KEY",
        tmp_path / "app",
        port=0,
        allowed_users=frozenset({"456", "789"}),
        allowed_tools=frozenset({"send_material"}),
        napcat_shared_local_files=True,
    )
    app = Application(config, model=ReadingModel(), environ={"QQ_TOKEN": "local-secret"})
    await app.start()
    calls = []

    async def fake_qq(action, params, attempt):
        calls.append(action)
        return ProtocolReceipt("acknowledged", message_id="1")

    try:
        app.delivery.call = fake_qq
        source, source_id, access = seed(app.store, "private-other", author="789")
        other_materials = MaterialStore(config.data_dir / "materials", app.store, "private-other")
        private = await other_materials.add_bytes(
            b"private", "private.txt", access, source_ref=source_id.input_id
        )
        own, accepted, _ = seed(app.store, "sender", author="456")
        own_access = replace(
            app.access_for(own),
            allowed_resources=app.access_for(own).allowed_resources | {"materials:sender"},
        )
        epoch = app.store.claim_run("sender", "sender-run")
        request = RunRequest("sender-run", "sender", own.address, own_access, epoch)
        for index, args in enumerate(
            (
                {"artifact_id": private.artifact_id},
                {"artifact_id": private.artifact_id, "path": str(tmp_path)},
                {"artifact_id": private.artifact_id, "target_id": "999"},
            )
        ):
            result = await app.tools.execute(
                ToolCall(str(index), "send_material", json.dumps(args)),
                ExecutionContext(request, 1, 0),
            )
            assert result.status == "not_executed"
        assert calls == [] and app.store.list_deliveries() == []
    finally:
        await app.close()
