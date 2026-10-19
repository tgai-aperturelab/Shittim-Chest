"""Real process-tree and Unicode pagination regressions; no network or service access."""

import asyncio
import base64
import os
import shlex
import sys

import pytest

from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    ExecutionContext,
    InputEvent,
    RunHalted,
    RunRequest,
)
from shittim_core.engine.process_tree import ProcessTree
from shittim_core.engine.processes import ExecutionRuntime
from shittim_core.state import StateStore


@pytest.fixture
async def runtime_context(tmp_path):
    store = StateStore(tmp_path / "state.sqlite")
    address = ConversationAddress("bot", "private", "owner")
    event = InputEvent(address, "owner", "1", "test")
    access = AccessContext("owner", address.key)
    input_id, _ = store.ingest(event)
    store.create_context("context", event, access)
    store.attach_input("context", input_id)
    epoch = store.claim_run("context", "run")
    context = ExecutionContext(RunRequest("run", "context", address, access, epoch), 1, 0)
    runtime = ExecutionRuntime(tmp_path / "workspaces", store, timeout=20)
    yield runtime, context
    await runtime.close()
    store.close()


def child_command(runtime, path, *, delay=1.5):
    if os.name == "nt":
        child = (
            f"Start-Sleep -Milliseconds {int(delay * 1000)}; [System.IO.File]::WriteAllText('"
            + str(path).replace("'", "''")
            + "', 'late effect')"
        )
        encoded = base64.b64encode(child.encode("utf-16le")).decode()
        return f"Start-Process -FilePath '{runtime.shell}' -ArgumentList '-NoProfile','-NonInteractive','-EncodedCommand','{encoded}' -WindowStyle Hidden"
    code = f"import time; from pathlib import Path; time.sleep({delay}); Path({str(path)!r}).write_text('late effect')"
    return f"{shlex.quote(sys.executable)} -c {shlex.quote(code)} >/dev/null 2>&1 &"


async def root_exits(runtime, process_id):
    async def wait():
        while runtime.live[process_id].process.returncode is None:
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait(), 5)


@pytest.mark.parametrize("action", ["stop", "close"])
async def test_exited_shell_does_not_hide_live_child_or_allow_post_stop_effect(
    runtime_context, action
):
    runtime, ctx = runtime_context
    output = runtime.workspace(ctx.run.context_id).root / "must-not-exist.txt"
    pid = await runtime.start(child_command(runtime, output), ctx)
    await root_exits(runtime, pid)
    before = runtime.record(pid, ctx.run.context_id)
    assert before["status"] == "running", before
    assert runtime.live[pid].tree.alive()
    if action == "stop":
        runtime.store.control(ctx.run.context_id, "owner", "stop")
        await asyncio.wait_for(runtime.live[pid].task, 5)
    else:
        await asyncio.wait_for(runtime.close(), 5)
    await asyncio.sleep(1.8)
    assert not output.exists()
    assert runtime.record(pid, ctx.run.context_id)["status"] == "terminated"
    assert not runtime.live[pid].tree.alive()


async def test_tree_can_finish_naturally_after_root_exit(runtime_context):
    runtime, ctx = runtime_context
    output = runtime.workspace(ctx.run.context_id).root / "completed.txt"
    pid = await runtime.start(child_command(runtime, output, delay=0.5), ctx)
    await asyncio.wait_for(runtime.live[pid].task, 5)
    result = runtime.record(pid, ctx.run.context_id)
    assert result["status"] == "exited" and output.read_text() == "late effect"
    assert result["exit_code_scope"] == "root_process"


async def test_failed_assignment_never_releases_user_command(runtime_context, monkeypatch):
    runtime, ctx = runtime_context
    output = runtime.workspace(ctx.run.context_id).root / "not-authorized.txt"

    def refuse(_self, _pid):
        raise OSError("fixture assignment refusal")

    monkeypatch.setattr(ProcessTree, "assign", refuse)
    command = (
        "[System.IO.File]::WriteAllText('" + str(output).replace("'", "''") + "', 'bad')"
        if os.name == "nt"
        else f"touch {shlex.quote(str(output))}"
    )
    with pytest.raises(OSError, match="assignment refusal"):
        await runtime.start(command, ctx)
    assert not output.exists()
    assert runtime.store.list_processes()[0]["status"] == "failed"


async def test_stop_during_spawn_prevents_releasing_command(runtime_context, monkeypatch):
    runtime, ctx = runtime_context
    output = runtime.workspace(ctx.run.context_id).root / "stale-command.txt"
    assign = ProcessTree.assign

    def assign_and_stop(tree, pid):
        assign(tree, pid)
        runtime.store.control(ctx.run.context_id, "owner", "stop")

    monkeypatch.setattr(ProcessTree, "assign", assign_and_stop)
    command = (
        "[System.IO.File]::WriteAllText('" + str(output).replace("'", "''") + "', 'bad')"
        if os.name == "nt"
        else f"touch {shlex.quote(str(output))}"
    )
    with pytest.raises(RunHalted):
        await runtime.start(command, ctx)
    assert not output.exists()
    assert runtime.store.list_processes()[0]["status"] == "failed"


async def test_process_output_pagination_retains_utf8_characters(runtime_context, tmp_path):
    runtime, ctx = runtime_context
    text = "中" * 11000 + "🙂" * 3
    log = tmp_path / "unicode.log"
    log.write_bytes(text.encode())
    details = {"command": "fixture", "cwd": str(tmp_path), "output": str(log), "exit_code": 0}
    runtime.store.create_process("unicode", ctx.run.run_id, details)
    runtime.store.update_process("unicode", "exited", details)
    offset, parts = 0, []
    while True:
        page = await runtime.manage("unicode", ctx.run.context_id, "output", offset=offset)
        parts.append(page["output"])
        if page["next_offset"] == offset:
            break
        offset = page["next_offset"]
    assert "".join(parts) == text
    assert offset == len(text.encode())


@pytest.mark.skipif(os.name != "nt", reason="Windows kernel handle ownership")
def test_job_handles_are_closed():
    import ctypes
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetCurrentProcess.argtypes = []
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    kernel.GetProcessHandleCount.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel.GetProcessHandleCount.restype = wintypes.BOOL

    def count():
        value = wintypes.DWORD()
        assert kernel.GetProcessHandleCount(kernel.GetCurrentProcess(), ctypes.byref(value))
        return value.value

    before = count()
    for _ in range(50):
        job = ProcessTree()
        job.close()
        job.close()
    assert count() <= before + 1
