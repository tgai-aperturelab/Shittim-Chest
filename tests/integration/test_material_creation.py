"""Creation is local UTF-8 material registration, not code execution or arbitrary path access."""

import asyncio
import json
import threading
from pathlib import Path

import pytest

from shittim_core.domain.model import ToolCall
from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    ExecutionContext,
    InputEvent,
    RunRequest,
)
from shittim_core.materials.store import MaterialStore
from shittim_core.state import StateStore
from shittim_core.tools.executor import ToolExecutor
from shittim_core.tools.material_tools import register_material_tools


def runtime(tmp_path, *, max_bytes=4096, allowed=True, extra_private=False):
    store = StateStore(tmp_path / "state.db")
    address = ConversationAddress("123", "group", "999")
    event = InputEvent(address, "456", "1", "create document")
    source_id, _ = store.ingest(event)
    access = AccessContext(
        "456",
        address.key,
        frozenset({"create_material"}) if allowed else frozenset(),
        frozenset({"materials:ctx"}),
    )
    store.create_context("ctx", event, access)
    store.attach_input("ctx", source_id)
    if extra_private:
        private = InputEvent(
            ConversationAddress("123", "private", "789"), "789", "2", "private unrelated"
        )
        private_id, _ = store.ingest(private)
        store.attach_input("ctx", private_id)
    epoch = store.claim_run("ctx", "run")
    seq = store.get_context("ctx")["input_seq"]
    run = RunRequest("run", "ctx", address, access, epoch)
    tools = ToolExecutor(store)
    register_material_tools(tools, store, tmp_path / "files", max_bytes=max_bytes)
    return store, tools, ExecutionContext(run, seq, 0), source_id


async def invoke(store, tools, ctx, args, call_id="create"):
    call = ToolCall(call_id, "create_material", json.dumps(args, ensure_ascii=False))
    store.append_history(
        ctx.run.run_id,
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
    return await tools.execute(call, ctx)


@pytest.mark.parametrize(
    "name,content",
    [
        ("report.md", "# 标题\n\n**内容保持格式**\n"),
        ("example.py", "print('This file must never be executed')\n"),
        ("empty.txt", ""),
    ],
)
async def test_create_preserves_utf8_file_format_and_returns_only_reference(
    tmp_path, name, content
):
    store, tools, ctx, source_id = runtime(tmp_path, extra_private=True)
    result = await invoke(store, tools, ctx, {"name": name, "content": content})
    assert result.status == "succeeded"
    info = json.loads(result.content)
    assert "content" not in info and "text" not in info and "path" not in info
    assert result.artifact_refs == (info["artifact_id"] + "@1",)
    materials = MaterialStore(tmp_path / "files", store, "ctx")
    read = await materials.read(info["artifact_id"], ctx.run.access)
    assert read.data == content.encode("utf-8")
    meta = store.get_artifact(info["artifact_id"])
    lineage = json.loads(meta["source_ref"])
    assert lineage["input_ids"] == [source_id]
    assert len(lineage["events"]) == 1 and lineage["audience"] == ctx.run.access.audience
    assert meta["audience"] == ctx.run.access.audience and meta["owner_id"] == "456"
    store.close()


@pytest.mark.parametrize(
    "args",
    [
        {"name": "../outside.txt", "content": "x"},
        {"name": r"C:\outside.txt", "content": "x"},
        {"name": "trailing. ", "content": "x"},
        {"name": "safe.txt", "content": "x", "directory": "/tmp"},
        {"name": "safe.txt", "content": "x", "artifact_id": "no-version"},
        {"name": "safe.txt", "content": "x", "expected_version": 1},
        {"name": "safe.txt", "content": "汉字"},
    ],
)
async def test_creation_rejects_paths_incomplete_versions_and_utf8_byte_overflow(tmp_path, args):
    store, tools, ctx, _ = runtime(tmp_path, max_bytes=4)
    result = await invoke(store, tools, ctx, args)
    assert result.status == "not_executed"
    assert not (tmp_path / "outside.txt").exists()
    assert not (tmp_path / "files").exists() or list((tmp_path / "files").iterdir()) == []
    store.close()


async def test_ungranted_creation_is_not_exposed_and_writes_no_file(tmp_path):
    store, tools, ctx, _ = runtime(tmp_path, allowed=False)
    assert tools.schemas(ctx.run.access.allowed_tools) == []
    result = await invoke(store, tools, ctx, {"name": "safe.txt", "content": "x"})
    assert result.status == "not_executed" and not (tmp_path / "files").exists()
    store.close()


async def test_version_update_preserves_original_and_records_exact_artifact_sources(tmp_path):
    store, tools, ctx, _ = runtime(tmp_path)
    created = await invoke(
        store, tools, ctx, {"name": "original.md", "content": "# Original\n"}, "first"
    )
    original_id = json.loads(created.content)["artifact_id"]
    source = {"artifact_id": original_id, "version": 1}
    updated = await invoke(
        store,
        tools,
        ctx,
        {
            "name": "original.md",
            "content": "# Updated\n",
            "artifact_id": original_id,
            "expected_version": 1,
            "artifact_sources": [source, source],
        },
        "second",
    )
    assert updated.status == "succeeded" and updated.artifact_refs == (original_id + "@2",)
    materials = MaterialStore(tmp_path / "files", store, "ctx")
    assert (await materials.read(original_id, ctx.run.access, version=1)).data == b"# Original\n"
    assert (await materials.read(original_id, ctx.run.access, version=2)).data == b"# Updated\n"
    meta = store.get_artifact(original_id, version=2)
    lineage = json.loads(meta["source_ref"])
    assert lineage["previous_version"]["version"] == 1
    assert len(lineage["artifact_sources"]) == 1
    assert (
        lineage["artifact_sources"][0]["sha256"]
        == store.get_artifact(original_id, version=1)["sha256"]
    )
    stale = await invoke(
        store,
        tools,
        ctx,
        {
            "name": "original.md",
            "content": "stale",
            "artifact_id": original_id,
            "expected_version": 1,
        },
        "third",
    )
    assert stale.status == "not_executed"
    assert store.get_artifact(original_id)["version"] == 2
    assert len(list((tmp_path / "files").iterdir())) == 2
    store.close()


async def test_private_or_corrupted_artifact_source_cannot_be_used_to_derive_public_material(
    tmp_path,
):
    store, tools, ctx, _ = runtime(tmp_path)
    other = InputEvent(ConversationAddress("123", "private", "789"), "789", "private-1", "private")
    other_id, _ = store.ingest(other)
    private_access = AccessContext(
        "789", other.address.key, allowed_resources=frozenset({"materials:private"})
    )
    store.create_context("private", other, private_access)
    store.attach_input("private", other_id)
    private = await MaterialStore(tmp_path / "files", store, "private").add_bytes(
        b"private bytes", "private.txt", private_access, source_ref=other_id
    )
    result = await invoke(
        store,
        tools,
        ctx,
        {
            "name": "public.txt",
            "content": "should not register",
            "artifact_sources": [{"artifact_id": private.artifact_id, "version": 1}],
        },
    )
    assert result.status == "not_executed" and "private bytes" not in result.content
    assert len(list((tmp_path / "files").iterdir())) == 1
    own = await MaterialStore(tmp_path / "files", store, "ctx").add_bytes(
        b"original", "source.txt", ctx.run.access
    )
    Path(store.get_artifact(own.artifact_id)["path"]).write_bytes(b"tampered")
    corrupt = await invoke(
        store,
        tools,
        ctx,
        {
            "name": "derived.txt",
            "content": "not registered",
            "artifact_sources": [{"artifact_id": own.artifact_id, "version": 1}],
        },
        "corrupt-source",
    )
    assert corrupt.status == "not_executed"
    assert len(list((tmp_path / "files").iterdir())) == 2
    store.close()


async def test_new_input_during_started_write_is_not_misreported_as_source(tmp_path, monkeypatch):
    store, tools, ctx, original_source = runtime(tmp_path)
    entered, release = threading.Event(), threading.Event()
    original_write = MaterialStore._write_bytes

    def blocked_write(self, path, data):
        entered.set()
        assert release.wait(2)
        original_write(self, path, data)

    monkeypatch.setattr(MaterialStore, "_write_bytes", blocked_write)
    task = asyncio.create_task(
        invoke(store, tools, ctx, {"name": "file.txt", "content": "old-view content"})
    )
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        newer = InputEvent(ctx.run.address, "456", "2", "new input after dispatch")
        new_id, _ = store.ingest(newer)
        store.attach_input("ctx", new_id)
    finally:
        release.set()
    result = await task
    assert result.status == "succeeded"
    lineage = json.loads(
        store.get_artifact(json.loads(result.content)["artifact_id"])["source_ref"]
    )
    assert lineage["input_ids"] == [original_source]
    assert lineage["input_seq"] == ctx.input_seq == 1
    store.close()


async def test_stop_while_reading_sources_prevents_a_new_file_write(tmp_path, monkeypatch):
    store, tools, ctx, _ = runtime(tmp_path)
    source = await MaterialStore(tmp_path / "files", store, "ctx").add_bytes(
        b"source", "source.txt", ctx.run.access
    )
    original_read = MaterialStore.read

    async def read_then_stop(self, *args, **kwargs):
        result = await original_read(self, *args, **kwargs)
        store.control("ctx", "456", "stop")
        return result

    monkeypatch.setattr(MaterialStore, "read", read_then_stop)
    result = await invoke(
        store,
        tools,
        ctx,
        {
            "name": "must-not-exist.txt",
            "content": "new output",
            "artifact_sources": [{"artifact_id": source.artifact_id, "version": source.version}],
        },
    )
    assert result.status == "not_executed"
    assert [p.name for p in (tmp_path / "files").iterdir()] == [
        Path(store.get_artifact(source.artifact_id)["path"]).name
    ]
    assert store.list_effects()[0]["status"] == "not_executed"
    store.close()
