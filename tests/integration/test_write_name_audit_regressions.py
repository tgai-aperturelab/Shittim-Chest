"""Audit regressions: pre-write failure classification and actual long-name DOCX intake."""

from __future__ import annotations

import json
from io import BytesIO
from pathlib import PurePosixPath

import pytest
from docx import Document

from shittim_core.domain.model import ToolCall
from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    ExecutionContext,
    InputEvent,
    RunRequest,
)
from shittim_core.engine.files import WorkspaceFiles
from shittim_core.engine.processes import ExecutionRuntime
from shittim_core.materials.names import sanitize_name
from shittim_core.materials.store import MaterialStore
from shittim_core.state import StateStore
from shittim_core.tools.execution import HOST_RESOURCE, register_execution_tools
from shittim_core.tools.executor import ToolExecutor
from shittim_core.tools.material_tools import register_material_tools


def context(store: StateStore, name: str) -> ExecutionContext:
    address = ConversationAddress("bot", "private", name)
    access = AccessContext(
        name,
        address.key,
        frozenset({"write_file", "read_material"}),
        frozenset({HOST_RESOURCE, "materials:" + name}),
    )
    event = InputEvent(address, name, "1", "process file")
    input_id, _ = store.ingest(event)
    store.create_context(name, event, access)
    store.attach_input(name, input_id)
    epoch = store.claim_run(name, "run-" + name)
    return ExecutionContext(RunRequest("run-" + name, name, address, access, epoch), 1, 0)


async def invoke(executor, store, ctx, name, args, call_id):
    call = ToolCall(call_id, name, json.dumps(args))
    store.append_history(
        ctx.run.run_id,
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "id": call_id,
                    "type": "function",
                    "function": {"name": name, "arguments": call.arguments},
                },
            ],
        },
    )
    return await executor.execute(call, ctx)


@pytest.fixture
async def setup(tmp_path, tmp_path_factory):
    store = StateStore(tmp_path / "state.sqlite")
    first, second = context(store, "first"), context(store, "second")
    runtime = ExecutionRuntime(tmp_path / "workspaces", store)
    tools = ToolExecutor(store)
    # Keep the actual material path below Windows MAX_PATH so this regression
    # isolates extension dispatch rather than the test runner's long directory.
    material_root = tmp_path_factory.mktemp("m")
    register_execution_tools(tools, runtime, material_root, max_material_bytes=1024 * 1024)
    register_material_tools(tools, store, material_root)
    yield store, runtime, tools, first, second, material_root
    await runtime.close()
    store.close()


async def test_missing_expected_hash_source_does_not_lock_other_context(setup):
    store, runtime, tools, first, second, _ = setup
    missing = await invoke(
        tools,
        store,
        first,
        "write_file",
        {"path": "missing.txt", "content": "must not appear", "expected_sha256": "0" * 64},
        "missing",
    )
    subsequent = await invoke(
        tools, store, second, "write_file", {"path": "valid.txt", "content": "written"}, "valid"
    )
    assert (missing.status, subsequent.status) == ("not_executed", "succeeded")
    assert not runtime.workspace("first").path("missing.txt").exists()
    assert runtime.workspace("second").bytes("valid.txt") == b"written"
    assert not any(effect["status"] == "unknown" for effect in store.list_effects())


async def test_file_not_found_after_write_still_records_unknown(setup, monkeypatch):
    store, runtime, tools, first, second, _ = setup
    real_write = WorkspaceFiles.write_file

    def write_then_lose_result(files, name, content, **kwargs):
        real_write(files, name, content, **kwargs)
        raise FileNotFoundError("fixture result-observation failure after mutation")

    monkeypatch.setattr(WorkspaceFiles, "write_file", write_then_lose_result)
    result = await invoke(
        tools,
        store,
        first,
        "write_file",
        {"path": "changed.txt", "content": "actual side effect"},
        "changed",
    )
    assert runtime.workspace("first").bytes("changed.txt") == b"actual side effect"
    assert result.status == "unknown"
    assert store.list_effects()[0]["status"] == "unknown"
    blocked = await invoke(
        tools, store, second, "write_file", {"path": "later.txt", "content": "no"}, "later"
    )
    assert blocked.status == "not_executed"


@pytest.mark.parametrize("name", ["meeting-notes-" * 12 + ".docx", "团队会议纪要" * 30 + ".docx"])
async def test_real_long_named_docx_is_registered_and_read_by_document_parser(setup, name):
    store, _, tools, first, _, root = setup
    document = Document()
    document.add_paragraph("Long filename document evidence 42")
    stream = BytesIO()
    document.save(stream)
    materials = MaterialStore(root, store, "first")
    artifact = await materials.add_bytes(stream.getvalue(), name, first.run.access)
    result = await invoke(
        tools, store, first, "read_material", {"artifact_id": artifact.artifact_id}, "read-docx"
    )
    data = json.loads(result.content)
    assert data.get("format") == "docx", data
    assert data["parsed_document"]["parsed"]
    assert data["parsed_document"]["units"][0]["text"] == "Long filename document evidence 42"
    assert artifact.name.endswith(".docx") and len(artifact.name) <= 120
    assert len(("0" * 32 + "-" + artifact.name).encode("utf-8")) <= 255


@pytest.mark.parametrize(
    "name,suffix",
    [
        ("a" * 150 + ".docx", ".docx"),
        ("a" * 150 + ".draft.DOCX", ".DOCX"),
        (".hidden" + "x" * 150 + ".xlsx", ".xlsx"),
        ("a" * 150 + ".tar.gz", ".gz"),
    ],
)
def test_bounded_labels_keep_final_format_suffix(name, suffix):
    result = sanitize_name(name)
    assert len(result) <= 120
    assert result.endswith(suffix)
    assert PurePosixPath(result).suffix == suffix


@pytest.mark.parametrize(
    "name",
    [
        ".gitignore",
        "." + "x" * 150,
        "report." + "x" * 200,
        "a" * 119 + "." + "x" * 200,
        "..",
        "\x00\x01. ",
        "../../" + "a" * 140 + ".docx",
        "C:\\private\\" + "a" * 140 + ".docx",
        "会议" * 100 + ".docx",
    ],
)
def test_filename_boundary_cases_remain_safe_and_bounded(name):
    result = sanitize_name(name)
    assert 1 <= len(result) <= 120
    assert len(result.encode("utf-8")) <= 220
    assert result not in {".", ".."} and not result.endswith((".", " "))
    assert not any(char in result for char in '\\/:*?"<>|\x00')
    assert all(ord(char) >= 32 for char in result)
    if name == ".gitignore":
        assert result == ".gitignore"
