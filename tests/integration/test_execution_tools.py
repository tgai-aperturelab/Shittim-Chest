"""Real local processes/files through the same durable tool dispatcher used by QQ."""

import asyncio
import hashlib
import json
import os
import shlex
import sys
from pathlib import Path

import pytest

from shittim_core.agent.loop import AgentEngine
from shittim_core.agent.ports import RunControl
from shittim_core.app.run_ports import RuntimePorts
from shittim_core.context.builder import ContextBuilder
from shittim_core.domain.model import NormalizedResponse, ToolCall
from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    ExecutionContext,
    InputEvent,
    RunRequest,
)
from shittim_core.engine.files import WorkspaceFiles
from shittim_core.engine.processes import ExecutionRuntime
from shittim_core.materials.store import MaterialStore
from shittim_core.state import StateStore
from shittim_core.tools.execution import EXECUTION_TOOLS, HOST_RESOURCE, register_execution_tools
from shittim_core.tools.executor import ToolExecutor


def python_command(code):
    if os.name == "nt":

        def quote(s):
            return "'" + s.replace("'", "''") + "'"

        return f"& {quote(sys.executable)} -X utf8 -c {quote(code)}"
    return f"{shlex.quote(sys.executable)} -X utf8 -c {shlex.quote(code)}"


@pytest.fixture
async def setup(tmp_path):
    store = StateStore(tmp_path / "state.db")
    address = ConversationAddress("123", "private", "456")
    event = InputEvent(address, "456", "1", "处理本任务的资料并提供成果")
    input_id, _ = store.ingest(event)
    access = AccessContext(
        "456", address.key, EXECUTION_TOOLS, frozenset({HOST_RESOURCE, "materials:ctx"})
    )
    store.create_context("ctx", event, access)
    store.attach_input("ctx", input_id)
    epoch = store.claim_run("ctx", "run")
    request = RunRequest("run", "ctx", address, access, epoch, max_tool_calls=30)
    ctx = ExecutionContext(request, 1, 0)
    tools = ToolExecutor(store)
    runtime = ExecutionRuntime(tmp_path / "workspaces", store, timeout=10)
    register_execution_tools(tools, runtime, tmp_path / "materials", max_material_bytes=1024 * 1024)
    count = 0

    async def invoke(name, args):
        nonlocal count
        count += 1
        call = ToolCall(f"call{count}", name, json.dumps(args, ensure_ascii=False))
        store.append_history(
            "run",
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": call.id,
                        "type": "function",
                        "function": {"name": name, "arguments": call.arguments},
                    }
                ],
            },
        )
        return await tools.execute(call, ctx)

    yield store, runtime, tools, ctx, invoke
    await runtime.close()
    store.close()


async def test_real_terminal_file_patch_export_roundtrip(setup):
    store, runtime, _, ctx, invoke = setup
    result = await invoke(
        "terminal",
        {
            "command": python_command(
                "from pathlib import Path; Path('answer.txt').write_text('hello old', encoding='utf-8'); print('created')"
            ),
            "yield_seconds": 5,
        },
    )
    info = json.loads(result.content)
    assert result.status == "succeeded" and info["exit_code"] == 0 and "created" in info["output"]
    read = await invoke("read_file", {"path": "answer.txt"})
    assert "hello old" in json.loads(read.content)["content"]
    patched = await invoke(
        "patch",
        {
            "patch": "*** Begin Patch\n*** Update File: answer.txt\n@@\n-hello old\n+hello 团队\n*** End Patch"
        },
    )
    assert patched.status == "succeeded", patched.content
    exported = await invoke("export_file", {"path": "answer.txt"})
    ref = json.loads(exported.content)
    materials = MaterialStore(runtime.root.parent / "materials", store, "ctx")
    data = await materials.read(ref["artifact_id"], ctx.run.access)
    assert data.data.decode() == "hello 团队"
    assert data.ref.sha256 == hashlib.sha256(data.data).hexdigest()
    assert all(e["status"] == "succeeded" for e in store.list_effects())


async def test_background_progress_stop_and_other_context_denied(setup):
    store, runtime, _, _, invoke = setup
    result = await invoke(
        "terminal",
        {
            "command": python_command("import time; print('ready', flush=True); time.sleep(30)"),
            "yield_seconds": 0.1,
        },
    )
    process = json.loads(result.content)
    assert process["status"] == "running"
    with pytest.raises(PermissionError):
        runtime.record(process["process_id"], "somebody-else")
    store.control("ctx", "456", "stop")
    await asyncio.wait_for(runtime.live[process["process_id"]].task, 10)
    final = runtime.record(process["process_id"], "ctx")
    assert final["status"] == "terminated"
    assert final["exit_code"] is not None


async def test_process_output_pagination_and_stdin(setup):
    _, runtime, _, _, invoke = setup
    result = await invoke(
        "terminal",
        {
            "command": python_command(
                "import sys; print('waiting',flush=True); print(sys.stdin.readline().strip(),flush=True)"
            ),
            "yield_seconds": 0.05,
        },
    )
    pid = json.loads(result.content)["process_id"]
    await invoke("process_manage", {"action": "write", "process_id": pid, "data": "中文输入\n"})
    waited = await invoke(
        "process_manage", {"action": "wait", "process_id": pid, "wait_seconds": 5}
    )
    assert "中文输入" in json.loads(waited.content)["output"]
    output = await invoke("process_manage", {"action": "output", "process_id": pid, "offset": 0})
    assert json.loads(output.content)["next_offset"] > 0


async def test_restart_never_replays_uncertain_process(setup):
    store, runtime, _, _, invoke = setup
    store.create_process(
        "interrupted-spawn",
        "run",
        {"command": "do not run", "cwd": str(runtime.root), "output": str(runtime.root / "absent")},
    )
    assert store.recover_processes() == 1
    rejected = await invoke("terminal", {"command": python_command("print('must not run')")})
    assert rejected.status == "not_executed"
    assert "unknown" in rejected.content
    assert not runtime.live
    assert store.list_processes()[0]["status"] == "unknown"


async def test_unknown_host_effect_blocks_stdin_but_allows_stop(setup):
    store, runtime, _, _, invoke = setup
    result = await invoke(
        "terminal",
        {
            "command": python_command(
                "import sys; from pathlib import Path; value=sys.stdin.readline(); Path('written.txt').write_text(value)"
            ),
            "yield_seconds": 0.05,
        },
    )
    pid = json.loads(result.content)["process_id"]
    details = {"cwd": str(runtime.root), "output": str(runtime.root / "absent")}
    store.create_process("orphan", "run", details)
    store.update_process("orphan", "unknown", details)
    refused = await invoke(
        "process_manage", {"action": "write", "process_id": pid, "data": "bad\n"}
    )
    assert refused.status == "not_executed"
    assert not runtime.workspace("ctx").path("written.txt").exists()
    stopped = await invoke("process_manage", {"action": "stop", "process_id": pid})
    assert json.loads(stopped.content)["exit_code"] is not None


async def test_expected_file_errors_do_not_poison_effect_resources(setup):
    store, _, _, _, invoke = setup
    missing = await invoke("export_file", {"path": "missing.txt"})
    assert missing.status == "not_executed"
    written = await invoke("write_file", {"path": "ok.txt", "content": "works"})
    assert written.status == "succeeded"
    assert not any(e["status"] == "unknown" for e in store.list_effects())


async def test_read_pagination_and_stale_write(setup):
    _, _, _, _, invoke = setup
    await invoke("write_file", {"path": "large.txt", "content": "x" * 40000})
    first = json.loads((await invoke("read_file", {"path": "large.txt"})).content)
    assert first["truncated"] and first["next_character_offset"] == 32000
    second = json.loads(
        (
            await invoke(
                "read_file",
                {"path": "large.txt", "character_offset": first["next_character_offset"]},
            )
        ).content
    )
    assert len(second["content"]) > 0 and not second["truncated"]
    rejected = await invoke(
        "write_file", {"path": "large.txt", "content": "oops", "expected_sha256": "stale"}
    )
    assert rejected.status == "not_executed"


def test_patch_prevalidates_all_paths_and_preserves_newlines(tmp_path):
    files = WorkspaceFiles(tmp_path / "workspace")
    path = files.root / "a.txt"
    path.write_bytes(b"\xef\xbb\xbfhello\r\nworld\r\n")
    result = files.patch(
        "*** Begin Patch\n*** Update File: a.txt\n@@\n-world\n+team\n*** End Patch"
    )
    assert result["success"], result
    assert path.read_bytes() == b"\xef\xbb\xbfhello\r\nteam\r\n"
    old = path.read_bytes()
    with pytest.raises(PermissionError):
        files.patch(
            "*** Begin Patch\n*** Update File: a.txt\n@@\n-team\n+oops\n*** Add File: ../escaped.txt\n+bad\n*** End Patch"
        )
    assert path.read_bytes() == old and not (tmp_path / "escaped.txt").exists()


async def test_actual_engine_uses_executable_tools_and_persists_result(setup):
    store, runtime, tools, ctx, _ = setup

    class Model:
        calls = 0

        async def complete(self, messages, schemas):
            self.calls += 1
            names = {s["function"]["name"] for s in schemas}
            assert {"terminal", "read_file", "export_file"} <= names
            if self.calls == 1:
                return NormalizedResponse(
                    None,
                    [
                        ToolCall(
                            "t",
                            "terminal",
                            json.dumps(
                                {
                                    "command": python_command(
                                        "from pathlib import Path; Path('result.csv').write_text('value\\n42\\n'); print(42)"
                                    ),
                                    "yield_seconds": 5,
                                }
                            ),
                        )
                    ],
                    "tool_calls",
                )
            content = json.loads(next(m["content"] for m in messages if m.get("role") == "tool"))
            assert content["status"] == "succeeded"
            assert json.loads(content["content"])["exit_code"] == 0
            return NormalizedResponse("已执行并生成结果。", [], "stop")

    ports = RuntimePorts(store, ContextBuilder(store), tools)
    outcome = await AgentEngine(Model()).run(ctx.run, ports, RunControl())
    assert outcome.status == "completed"
    assert runtime.workspace("ctx").bytes("result.csv").decode().splitlines() == ["value", "42"]
    assert store.list_processes()[0]["status"] == "exited"


@pytest.mark.parametrize("kind", ["private", "group"])
@pytest.mark.parametrize("presentation", ["file", "image"])
@pytest.mark.parametrize("backend", ["local_host", "bubblewrap"])
async def test_application_executes_then_delivers_verified_artifact(
    tmp_path, kind, presentation, backend
):
    import base64
    from urllib.parse import unquote, urlsplit

    from websockets.asyncio.client import connect

    from shittim_core.app.config import Settings
    from shittim_core.app.service import Application

    if backend == "bubblewrap" and (os.name == "nt" or not os.getenv("SHITTIM_BWRAP")):
        pytest.skip("Linux Bubblewrap executable is required for this backend")

    data = (
        b"actual generated file\n"
        if presentation == "file"
        else base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a1XkAAAAASUVORK5CYII="
        )
    )

    class Model:
        count = 0

        async def complete(self, messages, schemas):
            self.count += 1
            if self.count == 1:
                name, args = (
                    "terminal",
                    {
                        "command": (
                            "/runtime/bin/python -c "
                            + shlex.quote(
                                f"from pathlib import Path; Path('result.bin').write_bytes({data!r})"
                            )
                        )
                        if backend == "bubblewrap"
                        else python_command(
                            f"from pathlib import Path; Path('result.bin').write_bytes({data!r})"
                        ),
                        "yield_seconds": 5,
                    },
                )
            elif self.count == 2:
                name, args = "export_file", {"path": "result.bin"}
            elif self.count == 3:
                result = json.loads([m["content"] for m in messages if m.get("role") == "tool"][-1])
                artifact = json.loads(result["content"])
                name, args = (
                    "send_material",
                    {"artifact_id": artifact["artifact_id"], "presentation": presentation},
                )
            else:
                return NormalizedResponse("已生成并发送。", [], "stop")
            return NormalizedResponse(
                None, [ToolCall(f"step{self.count}", name, json.dumps(args))], "tool_calls"
            )

    settings = Settings(
        "123",
        "TEST_TOKEN",
        "fixture",
        "https://unused.invalid/v1",
        "TEST_KEY",
        tmp_path / "app",
        port=0,
        allowed_users=frozenset({"456"}),
        allowed_groups=frozenset({"789"}),
        allowed_tools=EXECUTION_TOOLS | {"send_material"},
        execution_backend=backend,
        execution_bwrap_path=Path(os.environ["SHITTIM_BWRAP"]) if backend == "bubblewrap" else None,
        execution_python_runtime=Path(os.environ["SHITTIM_PYTHON_RUNTIME"])
        if backend == "bubblewrap"
        else None,
        napcat_shared_local_files=True,
        protocol_timeout_seconds=5,
    )
    app = Application(settings, model=Model(), environ={"TEST_TOKEN": "local-token"})
    port = await app.start()
    try:
        async with connect(
            f"ws://127.0.0.1:{port}/onebot",
            additional_headers={"Authorization": "Bearer local-token", "X-Self-ID": "123"},
        ) as socket:
            await socket.send(
                json.dumps(
                    {
                        "post_type": "message",
                        "message_type": kind,
                        "self_id": 123,
                        "user_id": 456,
                        "group_id": 789,
                        "message_id": 1,
                        "message": [
                            {"type": "at", "data": {"qq": "123"}},
                            {"type": "text", "data": {"text": "生成文件并发给我"}},
                        ],
                    }
                )
            )
            sent = []
            for number in range(2):
                packet = json.loads(await asyncio.wait_for(socket.recv(), 15))
                assert packet["action"] == f"send_{kind}_msg"
                sent.append(packet["params"]["message"])
                await socket.send(
                    json.dumps(
                        {
                            "echo": packet["echo"],
                            "status": "ok",
                            "retcode": 0,
                            "data": {"message_id": number + 10},
                        }
                    )
                )
            await app.coordinator.wait_idle()
            segment = sent[0][0]
            assert segment["type"] == presentation
            if presentation == "image":
                actual = base64.b64decode(segment["data"]["file"].removeprefix("base64://"))
            else:
                path = unquote(urlsplit(segment["data"]["file"]).path)
                if os.name == "nt":
                    path = path.lstrip("/")
                actual = Path(path).read_bytes()
            assert actual == data
            assert len(app.store.list_deliveries("acknowledged")) == 2
            assert app.coordinator.failures == []
    finally:
        assert (await app.close()).released
