import json
from types import SimpleNamespace

import httpx
import pytest
from openai import AsyncOpenAI

from shittim_core.agent.model import (
    ModelProtocolError,
    ModelTransientError,
    OpenAIModel,
    response_to_message,
)
from shittim_core.agent.tool_call_stream import ToolCallAccumulator


def chunk(delta, finish=None):
    return {
        "id": "test",
        "object": "chat.completion.chunk",
        "created": 0,
        "model": "fixture",
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }


def sse(*chunks):
    return "".join("data: " + json.dumps(c) + "\n\n" for c in chunks) + "data: [DONE]\n\n"


async def test_real_sdk_mocktransport_stream_tools_reasoning_and_request():
    requests = []

    def handle(req):
        requests.append(json.loads(req.content))
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            text=sse(
                chunk({"role": "assistant", "reasoning_content": "internal reasoning"}),
                chunk(
                    {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "c1",
                                "type": "function",
                                "function": {"name": "read", "arguments": '{"path":'},
                            }
                        ]
                    }
                ),
                chunk({"tool_calls": [{"index": 0, "function": {"arguments": '"a.txt"}'}}]}),
                chunk({}, "tool_calls"),
            ),
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as transport:
        async with AsyncOpenAI(
            api_key="fixture-not-secret",
            base_url="https://fixture.invalid/v1",
            max_retries=0,
            http_client=transport,
        ) as sdk:
            model = OpenAIModel(
                model="fixture",
                base_url="https://fixture.invalid/v1",
                api_key="fixture-not-secret",
                client=sdk,
                reasoning_echo=True,
            )
            answer = await model.complete(
                [
                    {"role": "assistant", "content": "", "tool_calls": [], "internal_id": "hidden"},
                    {"role": "user", "content": "test"},
                ],
                [
                    {
                        "type": "function",
                        "function": {"name": "read", "parameters": {"type": "object"}},
                    }
                ],
            )
    assert answer.content == ""
    assert answer.tool_calls[0].arguments == '{"path":"a.txt"}'
    assert response_to_message(answer)["reasoning_content"] == "internal reasoning"
    assert "internal_id" not in requests[0]["messages"][0]
    assert len(requests) == 1


@pytest.mark.parametrize(
    "payload",
    [
        sse(chunk({"content": "incomplete"})),
        sse(
            chunk(
                {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "a",
                            "function": {"name": "read", "arguments": '{"path":'},
                        }
                    ]
                }
            ),
            chunk({}, "tool_calls"),
        ),
    ],
)
async def test_incomplete_sdk_stream_rejected(payload):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200, headers={"Content-Type": "text/event-stream"}, text=payload
            )
        )
    ) as transport:
        async with AsyncOpenAI(
            api_key="fixture",
            base_url="https://fixture.invalid/v1",
            max_retries=0,
            http_client=transport,
        ) as sdk:
            model = OpenAIModel(
                model="fixture",
                base_url="https://fixture.invalid/v1",
                api_key="fixture",
                client=sdk,
            )
            with pytest.raises(ModelProtocolError):
                await model.complete([{"role": "user", "content": "test"}], [])


# Ported from Hermes tests/agent/test_chat_completion_helpers_tool_calls.py.
def accumulate(*deltas):
    acc = ToolCallAccumulator()
    for call_id, name, args in deltas:
        acc.feed(
            SimpleNamespace(
                index=0, id=call_id, function=SimpleNamespace(name=name, arguments=args)
            )
        )
    return [
        (e["function"]["name"], e["function"]["arguments"])
        for _, e in sorted(acc.materialize().items())
    ]


def test_hermes_nameless_fragments_with_fresh_ids_stay_one_call():
    assert accumulate(("1", "read", ""), ("2", None, '{"pa'), ("3", None, 'th":"a"}')) == [
        ("read", '{"path":"a"}')
    ]


def test_hermes_same_name_splits_only_when_arguments_close():
    assert accumulate(("1", "read", '{"path":'), ("2", "read", '"a"}'), ("3", "read", "{}")) == [
        ("read", '{"path":"a"}'),
        ("read", "{}"),
    ]


def test_extra_content_uses_local_helper_without_upstream_import():
    acc = ToolCallAccumulator()
    acc.feed(
        SimpleNamespace(
            index=0,
            id="a",
            extra_content={"fixture": True},
            function=SimpleNamespace(name="read", arguments="{}"),
        )
    )
    assert acc.materialize()[0]["extra_content"] == {"fixture": True}


@pytest.mark.parametrize("status,error", [(401, ModelProtocolError), (503, ModelTransientError)])
async def test_sdk_does_not_hide_network_retry(status, error):
    attempts = []

    def handle(req):
        attempts.append(req)
        return httpx.Response(status, json={"error": {"message": "fixture"}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as transport:
        async with AsyncOpenAI(
            api_key="fixture",
            base_url="https://fixture.invalid/v1",
            max_retries=0,
            http_client=transport,
        ) as sdk:
            model = OpenAIModel(
                model="fixture",
                base_url="https://fixture.invalid/v1",
                api_key="fixture",
                client=sdk,
            )
            with pytest.raises(error):
                await model.complete([{"role": "user", "content": "test"}], [])
    assert len(attempts) == 1
