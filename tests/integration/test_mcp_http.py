"""Actual loopback HTTP transport against the pinned official MCP SDK server."""

import asyncio
import json
import sys
from dataclasses import replace

import pytest

from shittim_core.domain.model import ToolCall
from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    ExecutionContext,
    InputEvent,
    RunRequest,
)
from shittim_core.engine.mcp_client import MCPRuntime, MCPServerConfig
from shittim_core.state import StateStore
from shittim_core.tools.executor import ToolExecutor
from shittim_core.tools.mcp_tools import register_mcp_tools

SERVER = r"""
import asyncio,json,os,socket
from pathlib import Path
import uvicorn
from mcp.server import Server
from mcp.types import Tool,ListToolsResult,CallToolResult,TextContent

root=Path.cwd()
def note(record):
    with (root/'events.jsonl').open('a',encoding='utf-8') as f:
        f.write(json.dumps(record)+'\n')

async def listing(ctx,params):
    return ListToolsResult(tools=[Tool(name='effect',description='A deterministic test effect',
        inputSchema={'type':'object','properties':{'mode':{'type':'string'}},'required':['mode'],'additionalProperties':False})])

async def call(ctx,params):
    mode=(params.arguments or {})['mode']
    note({'effect':mode})
    (root/'effect.txt').write_text(mode,encoding='utf-8')
    if mode=='disconnect':
        os._exit(17)
    if mode=='slow':
        await asyncio.sleep(5)
    return CallToolResult(content=[TextContent(type='text',text='HTTP effect complete')],structuredContent={'mode':mode,'done':True})

server=Server('local-http-fixture',version='1',on_list_tools=listing,on_call_tool=call)
app=server.streamable_http_app()

async def recording(scope,receive,send):
    if scope['type']!='http':
        return await app(scope,receive,send)
    note({'http_method':scope['method'],'path':scope['path']})
    body=bytearray()
    async def observed_receive():
        message=await receive()
        if message['type']=='http.request':
            body.extend(message.get('body',b''))
            if not message.get('more_body',False) and body:
                parsed=json.loads(body)
                note({'rpc':parsed.get('method')})
        return message
    await app(scope,observed_receive,send)

async def main():
    sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.bind(('127.0.0.1',0))
    sock.listen(128)
    sock.setblocking(False)
    port=sock.getsockname()[1]
    host=uvicorn.Server(uvicorn.Config(recording,host='127.0.0.1',log_level='error',access_log=False,timeout_graceful_shutdown=1))
    task=asyncio.create_task(host.serve(sockets=[sock]))
    while not host.started:
        if task.done():
            await task
            raise RuntimeError('Server exited before startup')
        await asyncio.sleep(.01)
    (root/'ready.tmp').write_text(json.dumps({'port':port}),encoding='utf-8')
    (root/'ready.tmp').replace(root/'ready.json')
    await task
asyncio.run(main())
"""


@pytest.fixture
async def http_server(tmp_path):
    script = tmp_path / "server.py"
    script.write_text(SERVER, encoding="utf-8")
    stderr = (tmp_path / "server.stderr.log").open("wb")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        cwd=tmp_path,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=stderr,
    )
    try:
        async with asyncio.timeout(10):
            while not (tmp_path / "ready.json").exists():
                if process.returncode is not None:
                    pytest.fail((tmp_path / "server.stderr.log").read_text(errors="replace"))
                await asyncio.sleep(0.02)
        port = json.loads((tmp_path / "ready.json").read_text())["port"]
        yield (
            MCPServerConfig(
                "http-fixture",
                "streamable_http",
                url=f"http://127.0.0.1:{port}/mcp",
                timeout_seconds=1,
            ),
            process,
        )
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 3)
            except TimeoutError:
                process.kill()
                await process.wait()
        stderr.close()


def context(store, definitions):
    address = ConversationAddress("bot", "private", "owner")
    event = InputEvent(address, "owner", "1", "test local HTTP tool")
    access = AccessContext(
        "owner",
        address.key,
        frozenset(t.name for t in definitions),
        frozenset(t.resource_key for t in definitions),
    )
    input_id, _ = store.ingest(event)
    store.create_context("ctx", event, access)
    store.attach_input("ctx", input_id)
    epoch = store.claim_run("ctx", "run")
    return ExecutionContext(RunRequest("run", "ctx", address, access, epoch), 1, 0)


async def invoke(executor, store, ctx, tool_name, mode, call_id="first"):
    args = json.dumps({"mode": mode})
    store.append_history(
        "run",
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "id": call_id,
                    "type": "function",
                    "function": {"name": tool_name, "arguments": args},
                }
            ],
        },
    )
    return await executor.execute(ToolCall(call_id, tool_name, args), ctx)


async def test_real_http_initialize_list_call_and_close(http_server, tmp_path):
    config, process = http_server
    runtime = MCPRuntime((config,))
    store = StateStore(tmp_path / "state.sqlite")
    try:
        await runtime.start()
        assert runtime.status()[0]["state"] == "ready", runtime.status()
        executor = ToolExecutor(store)
        definitions = register_mcp_tools(executor, runtime)
        assert len(definitions) == 1 and definitions[0].transport == "streamable_http"
        ctx = context(store, definitions)
        result = await invoke(executor, store, ctx, definitions[0].name, "normal")
        assert result.status == "succeeded", result.content
        assert json.loads(result.content)["structuredContent"] == {"mode": "normal", "done": True}
        assert store.list_effects()[0]["status"] == "succeeded"
    finally:
        await runtime.close()
        store.close()
    assert runtime.status()[0]["state"] == "closed"
    assert process.returncode is None  # closing the client does not own the HTTP server
    records = [json.loads(line) for line in (tmp_path / "events.jsonl").read_text().splitlines()]
    rpc = [row["rpc"] for row in records if "rpc" in row]
    assert (
        rpc.count("initialize") == 1
        and rpc.count("tools/list") == 1
        and rpc.count("tools/call") == 1
    )
    assert (tmp_path / "effect.txt").read_text() == "normal"
    assert sum(row.get("http_method") == "DELETE" for row in records) == 1
    closed_call = await runtime.call(definitions[0], {"mode": "normal"})
    assert closed_call.status == "not_executed"


@pytest.mark.parametrize("mode", ["slow", "disconnect"])
async def test_http_lost_result_stays_unknown_without_retry(http_server, tmp_path, mode):
    config, process = http_server
    runtime = MCPRuntime((replace(config, timeout_seconds=0.5),))
    store = StateStore(tmp_path / "state.sqlite")
    try:
        await runtime.start()
        assert runtime.status()[0]["state"] == "ready", runtime.status()
        executor = ToolExecutor(store)
        definitions = register_mcp_tools(executor, runtime)
        ctx = context(store, definitions)
        result = await invoke(executor, store, ctx, definitions[0].name, mode)
        assert result.status == "unknown", result.content
        assert (tmp_path / "effect.txt").read_text() == mode
        assert store.list_effects()[0]["status"] == "unknown"
        again = await invoke(executor, store, ctx, definitions[0].name, "normal", "second")
        assert again.status == "not_executed"
        records = [
            json.loads(line) for line in (tmp_path / "events.jsonl").read_text().splitlines()
        ]
        assert sum(row.get("rpc") == "tools/call" for row in records) == 1
        if mode == "disconnect":
            assert await asyncio.wait_for(process.wait(), 2) == 17
    finally:
        await runtime.close()
        store.close()
