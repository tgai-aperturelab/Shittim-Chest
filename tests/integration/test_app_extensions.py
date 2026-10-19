import asyncio
import json
import os
import sys

from websockets.asyncio.client import connect

from shittim_core.app.config import Settings
from shittim_core.app.diagnostics import inspect_configuration
from shittim_core.app.service import Application
from shittim_core.domain.model import NormalizedResponse, ToolCall
from shittim_core.engine.mcp_client import MCPServerConfig

SERVER = """
import anyio, os
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, ListToolsResult, CallToolResult, TextContent
async def listing(ctx, params):
    return ListToolsResult(tools=[Tool(name="echo", description="Fixture", inputSchema={"type":"object","properties":{"text":{"type":"string"}},"required":["text"]})])
async def call(ctx, params):
    return CallToolResult(content=[TextContent(type="text",text=params.arguments["text"]+os.environ.get("MODE",""))])
server=Server("fixture",version="1",on_list_tools=listing,on_call_tool=call)
async def main():
    async with stdio_server() as (r,w):
        await server.run(r,w,server.create_initialization_options())
anyio.run(main)
"""


async def test_application_mcp_discovery_grant_environment_and_call(tmp_path):
    script = tmp_path / "server.py"
    script.write_text(SERVER, encoding="utf-8")

    class Model:
        count = 0

        async def complete(self, messages, tools):
            self.count += 1
            assert len(tools) == 1
            if self.count == 1:
                return NormalizedResponse(
                    None,
                    [ToolCall("external", tools[0]["function"]["name"], '{"text":"hello "}')],
                    "tool_calls",
                )
            returned = json.loads(next(m["content"] for m in messages if m["role"] == "tool"))
            assert returned["status"] == "succeeded", returned
            assert "hello configured" in returned["content"]
            return NormalizedResponse("已取得实际结果。", None, "stop")

    config = Settings(
        "123",
        "TOKEN",
        "fixture",
        "https://unused.invalid/v1",
        "KEY",
        tmp_path / "data",
        port=0,
        allowed_users=frozenset({"456"}),
        mcp_servers=(
            MCPServerConfig(
                "fixture",
                "stdio",
                command=sys.executable,
                args=(str(script),),
                cwd=tmp_path,
                env={"MODE": "MODE_REF"},
            ),
        ),
        mcp_tool_grants=(("fixture", ("echo",)),),
    )
    app = Application(config, model=Model(), environ={"TOKEN": "token", "MODE_REF": "configured"})
    port = await app.start()
    try:
        assert app.mcp_failures == []
        assert len(app.mcp_granted_tools) == 1, [runtime.status() for runtime in app.mcp]
        async with connect(
            f"ws://127.0.0.1:{port}/onebot",
            additional_headers={"Authorization": "Bearer token", "X-Self-ID": "123"},
        ) as socket:
            await socket.send(
                json.dumps(
                    {
                        "post_type": "message",
                        "message_type": "private",
                        "self_id": 123,
                        "user_id": 456,
                        "message_id": 1,
                        "message": [{"type": "text", "data": {"text": "调用已有工具"}}],
                    }
                )
            )
            packet = json.loads(await asyncio.wait_for(socket.recv(), 10))
            await socket.send(
                json.dumps(
                    {
                        "echo": packet["echo"],
                        "status": "ok",
                        "retcode": 0,
                        "data": {"message_id": 7},
                    }
                )
            )
            await app.coordinator.wait_idle()
            assert len(app.store.list_deliveries("acknowledged")) == 1
            assert app.store.list_effects()[0]["status"] == "succeeded"
    finally:
        assert (await app.close()).released


def test_extended_config_resolves_paths_and_never_creates_runtime_data(tmp_path):
    config = {
        "bot_id": "123",
        "token_env": "TOKEN",
        "model": "model",
        "base_url": "https://unused.invalid/v1",
        "api_key_env": "KEY",
        "data_dir": "data",
        "allowed_users": ["456"],
        "allowed_tools": ["terminal", "web_search", "skills_list"],
        "execution_backend": "local_host",
        "runtime_skill_roots": [{"id": "team", "path": "skills"}],
        "mcp_servers": [
            {
                "name": "fixture",
                "transport": "stdio",
                "command": sys.executable,
                "cwd": ".",
                "allowed_tools": ["echo"],
                "env": {"MODE": "MODE_REF"},
            }
        ],
    }
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    settings = Settings.load(path)
    assert settings.runtime_skill_roots[0].path == tmp_path / "skills"
    assert settings.mcp_tool_grants == (("fixture", ("echo",)),)
    report = inspect_configuration(settings, os.environ)
    assert report["execution_backend"] == "local_host"
    assert not settings.data_dir.exists()
