import asyncio
import base64
import hashlib
import json
from io import BytesIO

import httpx
import pytest
from openai import AsyncOpenAI
from PIL import Image

from shittim_core.agent.model import OpenAIModel
from shittim_core.domain.model import NormalizedResponse, ToolCall
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
from shittim_core.tools.vision import VISION_TOOLS, VisionConfig, register_vision_tool


def image_bytes(format="PNG", *, size=(20, 10)):
    output = BytesIO()
    Image.new("RGB", size, "red").save(output, format=format)
    return output.getvalue()


@pytest.fixture
def setup(tmp_path):
    store = StateStore(tmp_path / "state.db")
    address = ConversationAddress("123", "private", "456")
    event = InputEvent(address, "456", "1", "识别这张图片")
    input_id, _ = store.ingest(event)
    access = AccessContext("456", address.key, VISION_TOOLS, frozenset({"materials:ctx"}))
    store.create_context("ctx", event, access)
    store.attach_input("ctx", input_id)
    epoch = store.claim_run("ctx", "run")
    ctx = ExecutionContext(RunRequest("run", "ctx", address, access, epoch), 1, 0)
    yield store, tmp_path / "materials", ctx
    store.close()


class FakeModel:
    def __init__(self, response=None):
        self.calls = []
        self.response = response or NormalizedResponse("红色图片", None, "stop")

    async def complete(self, messages, tools):
        self.calls.append((messages, tools))
        return self.response


async def invoke(setup, data, *, model=None, config=None, filename="input.png", args=None):
    store, root, ctx = setup
    materials = MaterialStore(root, store, "ctx")
    ref = await materials.add_bytes(data, filename, ctx.run.access, source_ref="qq:image:input-1")
    tools = ToolExecutor(store)
    register_vision_tool(
        tools,
        store,
        root,
        model,
        config=config or VisionConfig(state="ready", model_label="fixture-vision"),
    )
    result = await tools.execute(
        ToolCall(
            "c1",
            "vision_analyze",
            json.dumps(args or {"artifact_id": ref.artifact_id, "question": "图片是什么颜色？"}),
        ),
        ctx,
    )
    return result, ref


@pytest.mark.parametrize(
    "format,mime",
    [("PNG", "image/png"), ("JPEG", "image/jpeg"), ("GIF", "image/gif"), ("WEBP", "image/webp")],
)
async def test_valid_rasters_preserve_bytes_hash_and_source(setup, format, mime):
    model = FakeModel()
    data = image_bytes(format)
    result, ref = await invoke(setup, data, model=model, filename="misleading.txt")
    assert result.status == "succeeded", result.content
    output = json.loads(result.content)
    assert output["source"]["sha256"] == hashlib.sha256(data).hexdigest()
    assert output["source"]["source_ref"] == "qq:image:input-1"
    assert output["source"]["mime_type"] == mime
    assert result.artifact_refs == (f"{ref.artifact_id}@1",)
    messages, tools = model.calls[0]
    assert tools == []
    wire_url = messages[1]["content"][2]["image_url"]["url"]
    assert wire_url.startswith(f"data:{mime};base64,")
    assert base64.b64decode(wire_url.partition(",")[2]) == data


async def test_real_openai_sdk_multimodal_request(setup):
    captured = []

    def handler(request):
        captured.append(json.loads(request.content))
        chunk = {
            "id": "test",
            "object": "chat.completion.chunk",
            "created": 0,
            "model": "fixture-vision",
            "choices": [{"index": 0, "delta": {"content": "红色"}, "finish_reason": "stop"}],
        }
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text="data: " + json.dumps(chunk) + "\n\ndata: [DONE]\n\n",
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport:
        async with AsyncOpenAI(
            api_key="test",
            base_url="https://vision.invalid/v1",
            max_retries=0,
            http_client=transport,
        ) as client:
            model = OpenAIModel(
                model="fixture-vision",
                base_url="https://vision.invalid/v1",
                api_key="test",
                client=client,
            )
            result, _ = await invoke(setup, image_bytes(), model=model)
    assert result.status == "succeeded", result.content
    assert len(captured) == 1 and captured[0]["model"] == "fixture-vision"
    assert "tools" not in captured[0]
    assert captured[0]["messages"][1]["content"][2]["type"] == "image_url"


@pytest.mark.parametrize("state", ["unconfigured", "unsupported"])
async def test_readiness_blocks_model_calls(setup, state):
    model = FakeModel()
    result, _ = await invoke(setup, image_bytes(), model=model, config=VisionConfig(state=state))
    assert result.status == "not_executed" and json.loads(result.content)["status"] == state
    assert not model.calls


@pytest.mark.parametrize(
    "data",
    [
        b"\x89PNG\r\n\x1a\ncorrupt",
        b"<svg>image</svg>",
        b"\xff\xd8\xffcorrupt",
        b"GIF89acorrupt",
        b"RIFF0000WEBPcorrupt",
    ],
)
async def test_corrupt_or_unsupported_images_do_not_reach_model(setup, data):
    model = FakeModel()
    result, _ = await invoke(setup, data, model=model)
    assert (
        result.status == "not_executed" and json.loads(result.content)["status"] == "invalid_image"
    )
    assert not model.calls


async def test_pixel_limit_is_checked_before_model(setup):
    model = FakeModel()
    result, _ = await invoke(
        setup,
        image_bytes(size=(30, 30)),
        model=model,
        config=VisionConfig(state="ready", model_label="fixture", max_pixels=100),
    )
    assert result.status == "not_executed" and not model.calls


async def test_external_path_or_url_arguments_rejected(setup):
    model = FakeModel()
    result, _ = await invoke(
        setup,
        image_bytes(),
        model=model,
        args={"artifact_id": "unused", "question": "read", "url": "https://example.org/a.png"},
    )
    assert result.status == "not_executed" and not model.calls


async def test_image_cannot_produce_tool_execution(setup):
    model = FakeModel(
        NormalizedResponse(None, [ToolCall("evil", "terminal", '{"command":"bad"}')], "tool_calls")
    )
    result, _ = await invoke(setup, image_bytes(), model=model)
    assert (
        result.status == "failed"
        and json.loads(result.content)["status"] == "invalid_analysis_response"
    )
    assert model.calls[0][1] == []


async def test_provider_rejection_is_not_universal_unsupported(setup):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                400,
                json={"error": {"message": "Image unsupported", "type": "invalid_request_error"}},
            )
        )
    ) as transport:
        async with AsyncOpenAI(
            api_key="test",
            base_url="https://vision.invalid/v1",
            max_retries=0,
            http_client=transport,
        ) as client:
            model = OpenAIModel(
                model="fixture", base_url="https://vision.invalid/v1", api_key="test", client=client
            )
            result, _ = await invoke(setup, image_bytes(), model=model)
    assert result.status == "failed"
    assert json.loads(result.content)["status"] == "provider_rejected_request"


async def test_context_and_hash_checks_before_model(setup):
    store, root, ctx = setup
    model = FakeModel()
    materials = MaterialStore(root, store, "ctx")
    ref = await materials.add_bytes(image_bytes(), "private.png", ctx.run.access)
    metadata = store.get_artifact(ref.artifact_id)
    from pathlib import Path

    Path(metadata["path"]).write_bytes(image_bytes("JPEG"))
    tools = ToolExecutor(store)
    register_vision_tool(
        tools, store, root, model, config=VisionConfig(state="ready", model_label="fixture")
    )
    result = await tools.execute(
        ToolCall(
            "c1", "vision_analyze", json.dumps({"artifact_id": ref.artifact_id, "question": "read"})
        ),
        ctx,
    )
    assert (
        result.status == "not_executed"
        and json.loads(result.content)["status"] == "integrity_error"
    )
    assert not model.calls


async def test_other_context_material_denied(setup):
    store, root, ctx = setup
    event = InputEvent(ctx.run.address, "456", "other", "another request")
    access = AccessContext("456", ctx.run.address.key, VISION_TOOLS, frozenset({"materials:other"}))
    store.create_context("other", event, access)
    ref = await MaterialStore(root, store, "other").add_bytes(image_bytes(), "other.png", access)
    model = FakeModel()
    tools = ToolExecutor(store)
    register_vision_tool(
        tools, store, root, model, config=VisionConfig(state="ready", model_label="fixture")
    )
    result = await tools.execute(
        ToolCall(
            "c1", "vision_analyze", json.dumps({"artifact_id": ref.artifact_id, "question": "read"})
        ),
        ctx,
    )
    assert result.status == "not_executed" and json.loads(result.content)["status"] == "denied"
    assert not model.calls


async def test_model_timeout_is_explicit_failure(setup):
    class SlowModel(FakeModel):
        async def complete(self, messages, tools):
            await asyncio.sleep(2)
            return self.response

    result, _ = await invoke(
        setup,
        image_bytes(),
        model=SlowModel(),
        config=VisionConfig(state="ready", model_label="fixture", timeout_seconds=0.02),
    )
    assert (
        result.status == "failed" and json.loads(result.content)["status"] == "provider_unavailable"
    )


async def test_animated_first_frame_is_explicit_and_lineage_preserved(setup):
    output = BytesIO()
    Image.new("RGB", (10, 10), "red").save(
        output,
        format="GIF",
        save_all=True,
        append_images=[Image.new("RGB", (10, 10), "blue")],
        duration=100,
        loop=0,
    )
    model = FakeModel()
    result, _ = await invoke(setup, output.getvalue(), model=model)
    assert result.status == "succeeded"
    source = json.loads(result.content)["source"]
    assert source["frame_count"] == 2 and source["analyzed_frame"] == 0
    assert source["mime_type"] == "image/png"
    assert source["sha256"] != source["transmitted_sha256"]


@pytest.mark.parametrize("change", ["stop", "input"])
async def test_authority_change_during_local_image_check_is_not_unknown(setup, monkeypatch, change):
    import shittim_core.tools.vision as vision

    store, _, ctx = setup
    original = asyncio.to_thread

    async def changed(function, *args, **kwargs):
        result = await original(function, *args, **kwargs)
        if function is vision._inspect_image:
            if change == "stop":
                store.control("ctx", "456", "stop")
            else:
                event = InputEvent(ctx.run.address, "456", "new", "updated request")
                input_id, _ = store.ingest(event)
                store.attach_input("ctx", input_id)
        return result

    monkeypatch.setattr(asyncio, "to_thread", changed)
    model = FakeModel()
    result, _ = await invoke(setup, image_bytes(), model=model)
    assert not model.calls
    assert result.status == "not_executed"
    assert store.list_effects()[0]["status"] == "not_executed"
