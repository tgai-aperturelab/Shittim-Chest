import asyncio
import json
import os
import shlex
import sys

from websockets.asyncio.client import connect

from shittim_core.app.config import Settings
from shittim_core.app.service import Application
from shittim_core.domain.model import NormalizedResponse, ToolCall
from shittim_core.tools.execution import EXECUTION_TOOLS


async def test_background_completion_resumes_without_another_user_message(tmp_path):
    code = "import time; from pathlib import Path; time.sleep(0.6); Path('done.txt').write_text('finished')"
    command = (
        ("& '" + sys.executable.replace("'", "''") + "' -c '" + code.replace("'", "''") + "'")
        if os.name == "nt"
        else f"{shlex.quote(sys.executable)} -c {shlex.quote(code)}"
    )

    class Model:
        count = 0

        async def complete(self, messages, tools):
            self.count += 1
            if self.count == 1:
                name, args = "terminal", {"command": command, "yield_seconds": 0}
            elif self.count == 2:
                result = json.loads(next(m["content"] for m in messages if m.get("role") == "tool"))
                process = json.loads(result["content"])
                name, args = (
                    "process_manage",
                    {"action": "await_completion", "process_id": process["process_id"]},
                )
            else:
                assert any("process_completion" in m.get("content", "") for m in messages)
                return NormalizedResponse("后台工作已完成。", None, "stop")
            return NormalizedResponse(
                None, [ToolCall(f"step{self.count}", name, json.dumps(args))], "tool_calls"
            )

    model = Model()
    settings = Settings(
        "123",
        "TOKEN",
        "fixture",
        "https://unused.invalid/v1",
        "KEY",
        tmp_path / "data",
        port=0,
        allowed_users=frozenset({"456"}),
        allowed_tools=EXECUTION_TOOLS,
        execution_backend="local_host",
    )
    app = Application(settings, model=model, environ={"TOKEN": "local"})
    port = await app.start()
    try:
        async with connect(
            f"ws://127.0.0.1:{port}/onebot",
            additional_headers={"Authorization": "Bearer local", "X-Self-ID": "123"},
        ) as socket:
            await socket.send(
                json.dumps(
                    {
                        "post_type": "message",
                        "message_type": "private",
                        "self_id": 123,
                        "user_id": 456,
                        "message_id": 1,
                        "message": [{"type": "text", "data": {"text": "运行任务"}}],
                    }
                )
            )
            response = json.loads(await asyncio.wait_for(socket.recv(), 10))
            assert "后台工作已完成" in response["params"]["message"][0]["data"]["text"]
            await socket.send(
                json.dumps(
                    {
                        "echo": response["echo"],
                        "status": "ok",
                        "retcode": 0,
                        "data": {"message_id": 9},
                    }
                )
            )
            await app.coordinator.wait_idle()
            assert model.count == 3
            context = app.store.list_contexts()[0]
            assert len(app.store.list_events(context["context_id"])) == 2
            assert app.execution.workspace(context["context_id"]).bytes("done.txt") == b"finished"
            # A recovery pass cannot manufacture another completion input.
            app.execution.notify = None
            await app.execution.recover_completions()
            assert len(app.store.list_events(context["context_id"])) == 2
    finally:
        assert (await app.close()).released
