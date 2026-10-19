"""Candidate composition and OS ownership tests; network is loopback only."""

import asyncio
import json
import subprocess
import sys
from dataclasses import replace

import pytest
from websockets.asyncio.client import connect

from shittim_core.app.config import Settings
from shittim_core.app.lease import AlreadyRunning, DataDirectoryLease
from shittim_core.app.service import Application
from shittim_core.domain.model import NormalizedResponse
from shittim_core.domain.types import AccessContext, ConversationAddress, InputEvent, OutboundIntent
from shittim_core.state import StateStore


class ScriptModel:
    def __init__(self):
        self.calls = []

    async def complete(self, messages, tools):
        self.calls.append((messages, tools))
        return NormalizedResponse("**测试完成**", None, "stop")


def settings(tmp_path, **changes):
    base = Settings(
        "123",
        "CANDIDATE_QQ_TOKEN",
        "scripted",
        "https://example.invalid/v1",
        "CANDIDATE_MODEL_KEY",
        tmp_path / "candidate-data",
        port=0,
        allowed_users=frozenset({"456"}),
        protocol_timeout_seconds=0.2,
    )
    return replace(base, **changes)


def event(message_id="1", author="456"):
    return InputEvent(ConversationAddress("123", "private", author), author, message_id, "测试")


def wire_message(author=456, message_id=1, text="你好"):
    return {
        "post_type": "message",
        "message_type": "private",
        "self_id": 123,
        "user_id": author,
        "message_id": message_id,
        "message": [{"type": "text", "data": {"text": text}}],
    }


def headers():
    return {"Authorization": "Bearer local-secret", "X-Self-ID": "123"}


def test_settings_are_explicit_and_relative_to_config_directory(tmp_path):
    file = tmp_path / "candidate.json"
    file.write_text(
        json.dumps(
            {
                "bot_id": "123",
                "token_env": "CANDIDATE_QQ_TOKEN",
                "model": "declared",
                "base_url": "https://example.invalid/v1",
                "api_key_env": "CANDIDATE_MODEL_KEY",
                "data_dir": "data",
                "allowed_users": ["456"],
            }
        ),
        encoding="utf-8",
    )
    config = Settings.load(file)
    assert config.data_dir == (tmp_path / "data").resolve()
    assert config.host == "127.0.0.1" and config.port == 3002
    assert config.allowed_tools == frozenset()
    assert not config.data_dir.exists()


@pytest.mark.parametrize(
    "changes",
    [
        {"allowed_tools": frozenset({"terminal"})},
        {"max_concurrent": 0},
        {"max_model_calls": -1},
        {"max_tool_calls": True},
        {"timeout_seconds": float("inf")},
        {"token_env": "actual-token-with-hyphens"},
        {"base_url": "https://user:secret@example.invalid"},
        {"allowed_users": frozenset({"123"})},
        {"timeout_seconds": True},
        {"reasoning_echo": "false"},
    ],
)
def test_settings_reject_unregistered_tools_and_unbounded_or_secret_values(tmp_path, changes):
    with pytest.raises(ValueError):
        settings(tmp_path, **changes)


def test_import_and_cli_help_create_no_runtime_files(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import shittim_core.app.config, shittim_core.app.service, shittim_core.app.lease",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert list(tmp_path.iterdir()) == []
    help_result = subprocess.run(
        [sys.executable, "-m", "shittim_core"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert help_result.returncode == 0 and "--serve" in help_result.stdout
    assert list(tmp_path.iterdir()) == []


def test_os_lease_refuses_second_process_and_releases_without_deleting_lock(tmp_path):
    lease = DataDirectoryLease(tmp_path)
    lease.acquire()
    try:
        with pytest.raises(AlreadyRunning):
            DataDirectoryLease(tmp_path).acquire()
        command = (
            "from pathlib import Path; import sys; "
            "from shittim_core.app.lease import DataDirectoryLease, AlreadyRunning\n"
            "try: DataDirectoryLease(Path(sys.argv[1])).acquire()\n"
            "except AlreadyRunning: sys.exit(23)\n"
        )
        child = subprocess.run(
            [sys.executable, "-c", command, str(tmp_path)],
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert child.returncode == 23, child.stderr
    finally:
        lease.release()
    assert (tmp_path / ".instance.lock").exists()
    contender = DataDirectoryLease(tmp_path)
    contender.acquire()
    contender.release()


async def test_missing_credentials_do_not_create_data_or_start_model(tmp_path):
    app = Application(settings(tmp_path), model=ScriptModel(), environ={})
    with pytest.raises(ValueError, match="Missing credential reference"):
        await app.start()
    assert not app.settings.data_dir.exists()
    assert not app.lease.acquired


async def test_application_end_to_end_with_script_model_and_local_onebot(tmp_path):
    model = ScriptModel()
    config = settings(tmp_path)
    app = Application(config, model=model, environ={"CANDIDATE_QQ_TOKEN": "local-secret"})
    port = await app.start()
    assert app.store is not None and app.coordinator is not None and app.tools is not None
    assert app.tools.schemas(config.allowed_tools) == []
    access = app.access_for(event())
    assert access.allowed_resources == {"qq:" + event().address.key}
    assert access.allowed_recipients == frozenset()
    competitor = Application(
        config, model=ScriptModel(), environ={"CANDIDATE_QQ_TOKEN": "local-secret"}
    )
    with pytest.raises(AlreadyRunning):
        await competitor.start()
    try:
        async with connect(f"ws://127.0.0.1:{port}/onebot", additional_headers=headers()) as socket:
            await app.recovery_complete.wait()
            await socket.send(json.dumps(wire_message(author=999)))
            await socket.send(json.dumps(wire_message()))
            send = json.loads(await asyncio.wait_for(socket.recv(), 2))
            assert send["action"] == "send_private_msg" and send["params"]["user_id"] == 456
            assert send["params"]["message"] == [{"type": "text", "data": {"text": "测试完成"}}]
            await socket.send(
                json.dumps(
                    {"status": "ok", "retcode": 0, "echo": send["echo"], "data": {"message_id": 55}}
                )
            )
            await app.coordinator.wait_idle()
            assert len(model.calls) == 1
            assert len(app.store.list_deliveries("acknowledged")) == 1
    finally:
        closed = await app.close()
    assert closed.drained and closed.released
    assert not app.lease.acquired
    reopened = StateStore(config.data_dir / "state.sqlite3")
    assert len(reopened.list_deliveries("acknowledged")) == 1
    reopened.close()


def seed_context(
    config, context_id, message_id, *, completed=False, tools=frozenset(), prepared=False
):
    store = StateStore(config.data_dir / "state.sqlite3")
    source = event(message_id)
    input_id, _ = store.ingest(source)
    access = AccessContext(
        "456",
        source.address.key,
        allowed_tools=tools,
        allowed_resources=frozenset({"qq:" + source.address.key}),
    )
    store.create_context(context_id, source, access)
    store.attach_input(context_id, input_id)
    if completed or prepared:
        store.claim_run(context_id, "run-" + context_id)
        store.consume_inputs("run-" + context_id, 1)
        store.finish_run(
            "run-" + context_id,
            "completed",
            "old result",
            OutboundIntent(source.address, "old result") if prepared else None,
        )
    store.close()


async def test_recovery_waits_for_transport_and_only_runs_unconsumed_inputs(tmp_path):
    config = settings(tmp_path)
    seed_context(config, "already-done", "1", completed=True)
    seed_context(config, "pending", "2")
    model = ScriptModel()
    app = Application(config, model=model, environ={"CANDIDATE_QQ_TOKEN": "local-secret"})
    port = await app.start()
    assert model.calls == []
    assert app.recovery["active_contexts"] == ["pending"]
    try:
        async with connect(f"ws://127.0.0.1:{port}/onebot", additional_headers=headers()) as socket:
            await app.recovery_complete.wait()
            request = json.loads(await asyncio.wait_for(socket.recv(), 2))
            await socket.send(
                json.dumps(
                    {
                        "echo": request["echo"],
                        "status": "ok",
                        "retcode": 0,
                        "data": {"message_id": 56},
                    }
                )
            )
            await app.coordinator.wait_idle()
            assert len(model.calls) == 1
            assert app.store.get_run("run-already-done")["status"] == "completed"
    finally:
        await app.close()


async def test_recovery_narrowed_policy_pauses_old_grants_and_supersedes_outbox(tmp_path):
    config = settings(tmp_path)
    seed_context(config, "old-grant", "1", tools=frozenset({"ask_person"}), prepared=True)
    model = ScriptModel()
    app = Application(config, model=model, environ={"CANDIDATE_QQ_TOKEN": "local-secret"})
    port = await app.start()
    try:
        async with connect(f"ws://127.0.0.1:{port}/onebot", additional_headers=headers()):
            await app.recovery_complete.wait()
            assert app.deferred_policy["old-grant"] == "persisted_grant_outside_current_settings"
            assert app.store.get_context("old-grant")["control_state"] == "paused"
            assert len(app.store.list_deliveries("superseded")) == 1
            assert model.calls == []
            # New input cannot use resume to revive the persisted broader tool grant.
            reply = InputEvent(event().address, "456", "2", "/resume", reply_to="1")
            accepted = await app.coordinator.accept(reply)
            assert accepted.status == "rejected"
            assert app.store.get_context("old-grant")["control_state"] == "paused"
    finally:
        await app.close()


async def test_shutdown_retains_lease_and_database_until_inflight_drains(tmp_path, monkeypatch):
    config = settings(tmp_path)
    app = Application(config, model=ScriptModel(), environ={"CANDIDATE_QQ_TOKEN": "local-secret"})
    await app.start()
    assert app.coordinator is not None
    original_close = app.coordinator.close

    async def not_drained():
        return False

    monkeypatch.setattr(app.coordinator, "close", not_drained)
    result = await app.close()
    assert not result.drained and not result.released and app.lease.acquired
    assert app.store.list_contexts() == []
    with pytest.raises(AlreadyRunning):
        DataDirectoryLease(config.data_dir).acquire()
    monkeypatch.setattr(app.coordinator, "close", original_close)
    assert (await app.close()).released


async def test_failed_bind_releases_local_lease_without_touching_running_instance(tmp_path):
    first = Application(
        settings(tmp_path), model=ScriptModel(), environ={"CANDIDATE_QQ_TOKEN": "local-secret"}
    )
    port = await first.start()
    second_settings = settings(tmp_path / "other", port=port)
    second = Application(
        second_settings, model=ScriptModel(), environ={"CANDIDATE_QQ_TOKEN": "local-secret"}
    )
    try:
        with pytest.raises(OSError):
            await second.start()
        assert not second.lease.acquired and first.lease.acquired
    finally:
        await first.close()


async def test_ingested_but_unbound_input_recovers_once_after_connection(tmp_path):
    config = settings(tmp_path)
    source = event("9998")
    original = StateStore(config.data_dir / "state.sqlite3")
    original.ingest(source)
    original.close()
    model = ScriptModel()
    app = Application(config, model=model, environ={"CANDIDATE_QQ_TOKEN": "local-secret"})
    port = await app.start()
    assert model.calls == []
    try:
        async with connect(f"ws://127.0.0.1:{port}/onebot", additional_headers=headers()) as socket:
            await app.recovery_complete.wait()
            request = json.loads(await asyncio.wait_for(socket.recv(), 2))
            await socket.send(
                json.dumps(
                    {
                        "echo": request["echo"],
                        "status": "ok",
                        "retcode": 0,
                        "data": {"message_id": 100},
                    }
                )
            )
            await app.coordinator.wait_idle()
            assert len(model.calls) == 1
            assert len(app.store.list_contexts()) == 1
            assert (await app.coordinator.accept(source)).status == "duplicate"
            await app.coordinator.recover_unbound_inputs()
            await app.coordinator.wait_idle()
            assert len(model.calls) == 1
    finally:
        await app.close()
