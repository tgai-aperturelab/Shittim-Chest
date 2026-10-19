"""Full Application wiring through a real loopback OneBot socket, with a scripted model."""

import asyncio
import json

from websockets.asyncio.client import connect

from shittim_core.app.service import Application
from shittim_core.domain.model import NormalizedResponse
from tests.integration.test_app import headers, settings


async def test_unknown_local_quote_becomes_attributed_data_in_candidate_model_view(tmp_path):
    entered, release, quote_received = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class ScriptModel:
        def __init__(self):
            self.calls = []

        async def complete(self, messages, tools):
            self.calls.append(messages)
            if len(self.calls) == 1:
                entered.set()
                await release.wait()
                return NormalizedResponse("旧视图的结果不能发送", None, "stop")
            return NormalizedResponse("已根据实际引用资料回答", None, "stop")

    model = ScriptModel()
    app = Application(
        settings(tmp_path, allowed_groups=frozenset({"10"})),
        model=model,
        environ={"CANDIDATE_QQ_TOKEN": "local-secret"},
    )
    port = await app.start()
    sent = []
    peer_task = None
    try:
        async with connect(f"ws://127.0.0.1:{port}/onebot", additional_headers=headers()) as socket:

            async def peer():
                async for raw in socket:
                    request = json.loads(raw)
                    if request["action"] == "get_msg":
                        await asyncio.wait_for(entered.wait(), 1)
                        assert request["params"]["message_id"] == "900"
                        data = {
                            "self_id": 123,
                            "message_id": 900,
                            "message_type": "group",
                            "group_id": 10,
                            "sender": {"user_id": 789},
                            "message": [{"type": "text", "data": {"text": "被引用的真实内容"}}],
                        }
                    else:
                        assert request["action"] == "send_group_msg"
                        sent.append(request)
                        data = {"message_id": 1000 + len(sent)}
                    await socket.send(
                        json.dumps(
                            {"echo": request["echo"], "status": "ok", "retcode": 0, "data": data}
                        )
                    )
                    if request["action"] == "get_msg":
                        quote_received.set()

            peer_task = asyncio.create_task(peer())
            await socket.send(
                json.dumps(
                    {
                        "post_type": "message",
                        "message_type": "group",
                        "self_id": 123,
                        "group_id": 10,
                        "user_id": 456,
                        "message_id": 1,
                        "message": [
                            {"type": "at", "data": {"qq": "123"}},
                            {"type": "reply", "data": {"id": "900"}},
                            {"type": "text", "data": {"text": "总结这条引用内容"}},
                        ],
                    }
                )
            )
            await asyncio.wait_for(entered.wait(), 1)
            await asyncio.wait_for(quote_received.wait(), 1)
            await app.reference_intake.wait_idle()
            release.set()
            await asyncio.wait_for(app.coordinator.wait_idle(), 2)
            assert len(model.calls) == 2 and len(sent) == 1
            text = json.dumps(model.calls[-1], ensure_ascii=False)
            assert "被引用的真实内容" in text and '"quoted_author_id": "789"' in text.replace(
                '\\"', '"'
            )
            assert "untrusted_quotation" in text
            assert "旧视图" not in json.dumps(sent, ensure_ascii=False)
            assert not app.reference_intake.failures and not app.coordinator.failures
        await peer_task
    finally:
        release.set()
        if peer_task is not None and not peer_task.done():
            peer_task.cancel()
            await asyncio.gather(peer_task, return_exceptions=True)
        assert (await app.close()).released
