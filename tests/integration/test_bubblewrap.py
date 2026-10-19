"""Linux namespace tests use an explicitly supplied binary and synthetic task files only.

SHITTIM_BWRAP=/path/to/bwrap SHITTIM_PYTHON_RUNTIME=/path/to/python/prefix pytest ...
Other hosts still verify that requesting this backend never silently uses local_host.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shlex
import socket
import sys
from pathlib import Path

import pytest

from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    ExecutionContext,
    InputEvent,
    RunRequest,
    ToolResult,
)
from shittim_core.engine.bubblewrap import BubblewrapBackend
from shittim_core.engine.files import WorkspaceFiles
from shittim_core.engine.processes import ExecutionRuntime
from shittim_core.state import StateStore


def make_context(store, name):
    address = ConversationAddress("bot", "private", name)
    event = InputEvent(address, name, "1", "task")
    access = AccessContext(name, address.key)
    input_id, _ = store.ingest(event)
    store.create_context(name, event, access)
    store.attach_input(name, input_id)
    epoch = store.claim_run(name, "run-" + name)
    return ExecutionContext(RunRequest("run-" + name, name, address, access, epoch), 1, 0)


@pytest.fixture
async def sandbox(tmp_path):
    if sys.platform != "linux" or not os.environ.get("SHITTIM_BWRAP"):
        pytest.skip("Explicit Linux bubblewrap validation environment is required")
    store = StateStore(tmp_path / "state.sqlite")
    context = make_context(store, "first")
    runtime = ExecutionRuntime(
        tmp_path / "workspaces",
        store,
        backend="bubblewrap",
        bwrap_path=os.environ["SHITTIM_BWRAP"],
        python_runtime=os.environ.get("SHITTIM_PYTHON_RUNTIME", sys.prefix),
    )
    yield runtime, context
    await runtime.close()
    store.close()


def python_command(runtime, source):
    return f"{shlex.quote(runtime.python_executable)} -c {shlex.quote(source)}"


async def finish(runtime, process_id):
    await asyncio.wait_for(runtime.live[process_id].task, 15)
    return runtime.record(process_id, "first")


@pytest.mark.skipif(sys.platform == "linux", reason="Non-Linux rejection")
def test_no_silent_windows_or_macos_fallback(tmp_path):
    store = StateStore(tmp_path / "s.sqlite")
    try:
        with pytest.raises(ValueError, match="only on Linux"):
            ExecutionRuntime(tmp_path / "workspaces", store, backend="bubblewrap")
    finally:
        store.close()


def test_virtual_workspace_paths_share_file_tool_mapping(tmp_path):
    files = WorkspaceFiles(tmp_path / "task", virtual_root="/workspace")
    assert files.write_file("/workspace/a.txt", "mapped").verified
    assert files.bytes("a.txt") == b"mapped"
    with pytest.raises(PermissionError):
        files.path("/workspace/../../private")


async def test_local_host_python_command_uses_runtime_interpreter(tmp_path):
    store = StateStore(tmp_path / "python.sqlite")
    ctx = make_context(store, "first")
    runtime = ExecutionRuntime(tmp_path / "workspaces", store)
    try:
        process_id = await runtime.start('python -c "import sys; print(sys.executable)"', ctx)
        result = await finish(runtime, process_id)
        assert result["exit_code"] == 0, result
        assert Path(result["output"].strip()).resolve() == Path(sys.executable).resolve()
    finally:
        await runtime.close()
        store.close()


async def test_python_bash_mounts_network_and_credentials_isolated(sandbox, tmp_path, monkeypatch):
    runtime, ctx = sandbox
    secret = tmp_path / "synthetic-private.txt"
    secret.write_text("another task secret")
    other = runtime.workspace("second").root / "other.txt"
    other.write_text("not this task")
    monkeypatch.setenv("SHITTIM_TEST_PRIVATE_TOKEN", "must-not-enter-sandbox")
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    port = listener.getsockname()[1]
    source = f"""import os,json,socket,sqlite3,ssl,sys
from pathlib import Path
s=socket.socket();s.settimeout(.5)
try: s.connect(('127.0.0.1',{port})); network=True
except OSError: network=False
try: Path('/runtime/not-writable').write_text('bad'); readonly=False
except OSError: readonly=True
result={{'cwd':os.getcwd(),'python':sys.executable,'host_private':Path({str(secret)!r}).exists(),'other_task':Path({str(other)!r}).exists(),'home_exists':Path('/home').exists(),'root_exists':Path('/root').exists(),'secret_env':os.environ.get('SHITTIM_TEST_PRIVATE_TOKEN'),'network':network,'runtime_readonly':readonly,'net_namespace':os.readlink('/proc/self/ns/net')}}
Path('/workspace/result.json').write_text(json.dumps(result));print(json.dumps(result))"""
    try:
        process_id = await runtime.start(python_command(runtime, source), ctx)
        result = await finish(runtime, process_id)
    finally:
        listener.close()
    assert result["exit_code"] == 0, result
    actual = json.loads(runtime.workspace("first").bytes("/workspace/result.json"))
    assert actual["cwd"] == "/workspace"
    assert actual["python"].startswith("/runtime/")
    assert actual["runtime_readonly"]
    assert not any(
        actual[key]
        for key in (
            "host_private",
            "other_task",
            "home_exists",
            "root_exists",
            "secret_env",
            "network",
        )
    )
    assert actual["net_namespace"] != os.readlink("/proc/self/ns/net")
    assert runtime.resource_key("first") == "execution:workspace:first"


@pytest.mark.parametrize("action", ["stop", "close"])
async def test_setsid_child_cannot_outlive_namespace_stop(sandbox, action):
    runtime, ctx = sandbox
    child = "from pathlib import Path; import time; Path('/workspace/child-started').write_text('yes'); time.sleep(2); Path('/workspace/late-effect').write_text('bad')"
    source = f"import subprocess,time; subprocess.Popen([{runtime.python_executable!r},'-c',{child!r}],start_new_session=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); time.sleep(30)"
    process_id = await runtime.start(python_command(runtime, source), ctx)
    started = runtime.workspace("first").root / "child-started"

    async def wait_started():
        while not started.exists():
            if runtime.live[process_id].task.done():
                raise AssertionError(runtime.record(process_id, "first"))
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait_started(), 10)
    if action == "stop":
        runtime.store.control("first", "first", "stop")
        await finish(runtime, process_id)
    else:
        await runtime.close()
    await asyncio.sleep(2.2)
    assert not (runtime.workspace("first").root / "late-effect").exists()
    assert runtime.record(process_id, "first")["status"] == "terminated"


async def test_unknown_scope_only_blocks_affected_sandbox_and_host_unknown_blocks_all(sandbox):
    runtime, ctx = sandbox
    second = make_context(runtime.store, "second")
    details = {"backend": "bubblewrap", "cwd": "/workspace", "output": "/unused"}
    runtime.store.create_process("uncertain", ctx.run.run_id, details)
    runtime.store.update_process("uncertain", "unknown", details)
    with pytest.raises(ValueError, match="unknown"):
        runtime.assert_mutation_ready("first")
    runtime.assert_mutation_ready("second")
    with pytest.raises(ValueError, match="unknown"):
        runtime.assert_mutation_ready()
    host = {**details, "backend": "local_host"}
    runtime.store.create_process("host-uncertain", second.run.run_id, host)
    runtime.store.update_process("host-uncertain", "unknown", host)
    with pytest.raises(ValueError, match="unknown"):
        runtime.assert_mutation_ready("second")


@pytest.mark.parametrize("tool", ["write_file", "mcp_stdio_tool"])
async def test_unknown_host_file_effect_blocks_sandbox_after_backend_switch(sandbox, tool):
    runtime, ctx = sandbox
    effect = runtime.store.begin_effect(
        ctx.run.run_id, "old-write", tool, {}, ("execution:local_host",), 1
    )
    runtime.store.finish_effect(effect, ToolResult("unknown", "old host write outcome uncertain"))
    with pytest.raises(ValueError, match="unknown"):
        runtime.assert_mutation_ready("first")
    with pytest.raises(ValueError, match="unknown"):
        runtime.assert_mutation_ready("unrelated-sandbox")


async def test_runtime_prefix_must_not_overlap_task_store(sandbox):
    runtime, _ = sandbox
    prefix = runtime.sandbox.python_runtime
    for tasks in (prefix.parent, prefix / "tasks"):
        with pytest.raises(ValueError, match="overlap"):
            BubblewrapBackend(tasks, bwrap_path=runtime.sandbox.executable, python_runtime=prefix)


async def test_explicit_host_network_connects_http_without_exposing_other_files(sandbox, tmp_path):
    isolated, _ = sandbox
    hits = []

    async def serve(reader, writer):
        request = await reader.read(4096)
        hits.append(request)
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: close\r\n\r\nlocal-fixture"
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    private = tmp_path / "synthetic-network-private.txt"
    private.write_text("not a network mount")
    try:
        for mode in ("isolated", "host"):
            store = StateStore(tmp_path / f"{mode}-network.sqlite")
            ctx = make_context(store, "first")
            runtime = ExecutionRuntime(
                tmp_path / (mode + "-workspaces"),
                store,
                backend="bubblewrap",
                bwrap_path=isolated.sandbox.executable,
                python_runtime=isolated.sandbox.python_runtime,
                network=mode,
            )
            other = runtime.workspace("other").root / "private.txt"
            other.write_text("other task")
            source = f"""import json,ssl,urllib.request
from pathlib import Path
try: response=urllib.request.urlopen('http://127.0.0.1:{port}/fixture',timeout=1).read().decode()
except OSError: response=None
result={{'response':response,'private':Path({str(private)!r}).exists(),'other_task':Path({str(other)!r}).exists(),'passwd':Path('/etc/passwd').exists(),'private_keys':Path('/etc/ssl/private').exists(),'resolver':Path('/etc/resolv.conf').exists(),'ca_count':len(ssl.create_default_context().get_ca_certs())}}
print(json.dumps(result))"""
            try:
                pid = await runtime.start(python_command(runtime, source), ctx)
                result = await finish(runtime, pid)
                assert result["exit_code"] == 0, result
                payload = json.loads(result["output"])
                assert not any(
                    payload[name] for name in ("private", "other_task", "passwd", "private_keys")
                )
                assert payload["response"] == ("local-fixture" if mode == "host" else None)
                assert payload["resolver"] == (mode == "host")
                assert result["network"] == mode
                if mode == "host":
                    assert payload["ca_count"] > 0
                    assert (
                        runtime.resource_key("first")
                        == runtime.resource_key("other")
                        == "execution:local_host"
                    )
                else:
                    assert runtime.resource_key("first") == "execution:workspace:first"
            finally:
                await runtime.close()
                store.close()
        assert len(hits) == 1
    finally:
        server.close()
        await server.wait_closed()


async def test_unknown_shared_network_process_blocks_other_task(sandbox):
    runtime, ctx = sandbox
    details = {"backend": "bubblewrap", "network": "host", "cwd": "/workspace", "output": "/unused"}
    runtime.store.create_process("remote-effect", ctx.run.run_id, details)
    runtime.store.update_process("remote-effect", "unknown", details)
    with pytest.raises(ValueError, match="unknown"):
        runtime.assert_mutation_ready("another-context")


async def test_owner_death_kills_namespace_even_after_child_setsid(sandbox, tmp_path):
    runtime, _ = sandbox
    root = tmp_path / "crashed-owner"
    root.mkdir()
    workspace = root / "workspaces" / hashlib.sha256(b"first").hexdigest()
    child = "from pathlib import Path; import time; Path('/workspace/child-started').write_text('yes'); time.sleep(2); Path('/workspace/late-effect').write_text('bad')"
    source = f"import subprocess,time; subprocess.Popen([{runtime.python_executable!r},'-c',{child!r}],start_new_session=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); time.sleep(30)"
    command = python_command(runtime, source)
    script = root / "owner.py"
    script.write_text(
        f"""import asyncio
from pathlib import Path
from shittim_core.state import StateStore
from shittim_core.domain.types import AccessContext,ConversationAddress,InputEvent,ExecutionContext,RunRequest
from shittim_core.engine.processes import ExecutionRuntime
async def main():
 root=Path({str(root)!r}); store=StateStore(root/'state.sqlite')
 address=ConversationAddress('bot','private','first'); event=InputEvent(address,'first','1','task'); access=AccessContext('first',address.key)
 iid,_=store.ingest(event); store.create_context('first',event,access); store.attach_input('first',iid); epoch=store.claim_run('first','run')
 ctx=ExecutionContext(RunRequest('run','first',address,access,epoch),1,0)
 runtime=ExecutionRuntime(root/'workspaces',store,backend='bubblewrap',bwrap_path={str(runtime.sandbox.executable)!r},python_runtime={str(runtime.sandbox.python_runtime)!r})
 pid=await runtime.start({command!r},ctx)
 (root/'bwrap-pid').write_text(str(runtime.live[pid].process.pid))
 await asyncio.Event().wait()
asyncio.run(main())
""",
        encoding="utf-8",
    )
    owner = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:

        async def wait_started():
            while not (workspace / "child-started").exists():
                if owner.returncode is not None:
                    _, error = await owner.communicate()
                    raise AssertionError(error.decode())
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_started(), 10)
        owner.kill()
        await asyncio.wait_for(owner.wait(), 5)
        await asyncio.sleep(2.2)
        assert not (workspace / "late-effect").exists()
        pid = int((root / "bwrap-pid").read_text())
        status = Path(f"/proc/{pid}/stat")
        assert not status.exists() or status.read_text().rsplit(")", 1)[1].split()[0] == "Z"
        reopened = StateStore(root / "state.sqlite")
        try:
            assert reopened.recover_processes() == 1
            assert reopened.list_processes()[0]["status"] == "unknown"
        finally:
            reopened.close()
    finally:
        if owner.returncode is None:
            owner.kill()
        await owner.communicate()
