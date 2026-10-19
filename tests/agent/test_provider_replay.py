"""Hermes replay behavior through SDK streaming, real SQLite reopen, and the next request.

These tests use a deterministic HTTP transport; they do not claim live provider validation.
"""

import copy
import json

import httpx
import pytest
from openai import AsyncOpenAI

from shittim_core.agent.model import OpenAIModel, response_to_message, wire_messages
from shittim_core.domain.model import NormalizedResponse, ToolCall
from shittim_core.domain.types import AccessContext, ConversationAddress, InputEvent, ToolResult
from shittim_core.engine.hermes.reasoning_carriers import CARRIER_TYPE
from shittim_core.engine.hermes.tool_call_ids import (
    coalesce_tool_call_id,
    normalize_provider_tool_call_ids,
    uniquify_tool_call_ids,
)
from shittim_core.state import StateStore


def _chunk(delta, finish=None):
    return {
        "id": "fixture",
        "object": "chat.completion.chunk",
        "created": 0,
        "model": "gemini-fixture",
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }


def _sse(*chunks):
    return "".join("data: " + json.dumps(item) + "\n\n" for item in chunks) + "data: [DONE]\n\n"


async def test_stream_reopen_and_follow_up_replays_signature_with_matching_tool_result(tmp_path):
    requests = []

    def handle(request):
        requests.append(json.loads(request.content))
        payload = (
            _sse(
                _chunk({"reasoning_content": "private reasoning"}),
                _chunk({"extra_content": {"google": {"thought_signature": "TEXT_SIGNATURE"}}}),
                _chunk(
                    {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call_1",
                                "type": "function",
                                "function": {"name": "read_file", "arguments": '{"path":'},
                                "extra_content": {
                                    "google": {"thought_signature": "TOOL_SIGNATURE"}
                                },
                            }
                        ]
                    }
                ),
                _chunk({"tool_calls": [{"index": 0, "function": {"arguments": '"input.txt"}'}}]}),
                _chunk({}, "tool_calls"),
            )
            if len(requests) == 1
            else _sse(_chunk({"content": "已检查结果"}), _chunk({}, "stop"))
        )
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, text=payload)

    db_path = tmp_path / "journal.sqlite"
    store = StateStore(db_path)
    address = ConversationAddress("bot", "private", "owner")
    event = InputEvent(address, "owner", "message1", "读取文件")
    input_id, _ = store.ingest(event)
    store.create_context(
        "context", event, AccessContext("owner", address.key, frozenset({"read_file"}))
    )
    seq = store.attach_input("context", input_id)
    store.claim_run("context", "run")
    store.append_history("run", {"role": "user", "content": event.text})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http_client:
        async with AsyncOpenAI(
            api_key="fixture", base_url="https://fixture.invalid/v1", http_client=http_client
        ) as sdk:
            model = OpenAIModel(
                model="gemini-fixture",
                base_url="https://fixture.invalid/v1",
                api_key="fixture",
                client=sdk,
            )
            answer = await model.complete(store.history("context"), [])
            store.append_history("run", response_to_message(answer))
            effect_id = store.begin_effect(
                "run", "call_1", "read_file", {"path": "input.txt"}, (), seq
            )
            store.finish_effect_with_history(
                effect_id, ToolResult("succeeded", "observed file content")
            )
            store.close()
            store = StateStore(db_path)
            canonical = store.history("context")
            snapshot = copy.deepcopy(canonical)
            final = await model.complete(canonical, [])
            assert canonical == snapshot
            assert final.content == "已检查结果"
    store.close()
    assistant, tool = requests[1]["messages"][-2:]
    assert assistant["tool_calls"][0]["id"] == tool["tool_call_id"] == "call_1"
    assert (
        assistant["tool_calls"][0]["extra_content"]["google"]["thought_signature"]
        == "TOOL_SIGNATURE"
    )
    assert assistant["extra_content"]["google"]["thought_signature"] == "TEXT_SIGNATURE"
    assert "_provider_data" not in assistant
    assert "_provider_data" not in assistant["tool_calls"][0]
    assert CARRIER_TYPE not in json.dumps(requests[1])


async def test_streamed_reasoning_details_merge_fragments_and_preserve_opaque_blocks():
    payload = _sse(
        _chunk({"reasoning_details": [{"type": "reasoning.text", "text": "part "}]}),
        _chunk(
            {"reasoning_details": [{"type": "reasoning.text", "text": "two", "signature": "SIG"}]}
        ),
        _chunk({"reasoning_details": [{"type": "reasoning.encrypted", "data": "OPAQUE"}]}),
        _chunk({"content": "answer"}),
        _chunk({}, "stop"),
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200, headers={"Content-Type": "text/event-stream"}, text=payload
            )
        )
    ) as client:
        async with AsyncOpenAI(
            api_key="fixture", base_url="https://openrouter.ai/api/v1", http_client=client
        ) as sdk:
            model = OpenAIModel(
                model="fixture",
                base_url="https://openrouter.ai/api/v1",
                api_key="fixture",
                client=sdk,
            )
            response = await model.complete([], [])
    row = response_to_message(response)
    assert row["reasoning_details"] == [
        {"type": "reasoning.text", "text": "part two", "signature": "SIG"},
        {"type": "reasoning.encrypted", "data": "OPAQUE"},
    ]
    assert (
        wire_messages([row], reasoning_echo=False, base_url="https://openrouter.ai/api/v1")[0][
            "reasoning_details"
        ]
        == row["reasoning_details"]
    )
    assert (
        "reasoning_details"
        not in wire_messages(
            [row], reasoning_echo=False, base_url="https://api.groq.com/openai/v1"
        )[0]
    )


def test_all_native_data_survives_storage_but_other_api_modes_are_not_sent_as_chat_fields():
    data = {
        "reasoning_content": "private",
        "anthropic_content_blocks": [{"type": "thinking", "signature": "a"}],
        "codex_reasoning_items": [{"encrypted_content": "b"}],
        "custom_provider_state": {"x": 1},
    }
    response = NormalizedResponse(
        "public",
        [
            ToolCall(
                "call",
                "read_file",
                "{}",
                {"call_id": "call", "response_item_id": "fc_1", "custom_call_state": "c"},
            )
        ],
        "tool_calls",
        provider_data=data,
    )
    row = response_to_message(response)
    assert row["_provider_data"] == data
    assert row["tool_calls"][0]["_provider_data"]["custom_call_state"] == "c"
    wire = wire_messages([row], reasoning_echo=False, base_url="https://api.groq.com/openai/v1")[0]
    assert set(wire) == {"role", "content", "tool_calls"}
    assert set(wire["tool_calls"][0]) == {"id", "type", "function"}
    data["custom_provider_state"]["x"] = 2
    assert row["_provider_data"]["custom_provider_state"]["x"] == 1


def test_copilot_native_carriers_only_reach_copilot_and_canonical_history_is_unchanged():
    response = NormalizedResponse(
        "",
        [ToolCall("call", "read_file", "{}", {"thought_signature": "FSIG"})],
        "tool_calls",
        provider_data={"reasoning_opaque": "OPAQUE", "reasoning_text": "private"},
    )
    canonical = response_to_message(response)
    snapshot = copy.deepcopy(canonical)
    wire = wire_messages(
        [canonical],
        reasoning_echo=False,
        model="gemini-fixture",
        base_url="https://api.githubcopilot.com",
    )[0]
    assert wire["reasoning_opaque"] == "OPAQUE"
    assert wire["tool_calls"][0]["function"]["thought_signature"] == "FSIG"
    other = wire_messages(
        [canonical], reasoning_echo=False, model="other", base_url="https://api.groq.com"
    )[0]
    assert "reasoning_opaque" not in other
    assert "thought_signature" not in other["tool_calls"][0]["function"]
    assert canonical == snapshot


@pytest.mark.parametrize(
    "host",
    [
        "https://api.githubcopilot.com.attacker.invalid",
        "https://attacker.invalid/githubcopilot.com",
    ],
)
def test_provider_hostname_is_not_matched_as_substring(host):
    row = response_to_message(
        NormalizedResponse(
            "answer",
            None,
            "stop",
            provider_data={"reasoning_opaque": "OPAQUE", "reasoning_text": "private"},
        )
    )
    assert "reasoning_opaque" not in wire_messages([row], reasoning_echo=False, base_url=host)[0]


def test_alias_model_on_native_gemini_compat_host_keeps_signature():
    row = response_to_message(
        NormalizedResponse(
            "answer",
            None,
            "stop",
            provider_data={"extra_content": {"google": {"thought_signature": "SIG"}}},
        )
    )
    assert (
        wire_messages(
            [row],
            reasoning_echo=False,
            model="alias",
            base_url="https://generativelanguage.googleapis.com/v1beta/openai",
        )[0]["extra_content"]["google"]["thought_signature"]
        == "SIG"
    )


def test_duplicate_ids_update_provider_backing_field_and_preserve_response_item():
    calls = [
        ToolCall("same|fc_1", "a", "{}", {"call_id": "same", "response_item_id": "fc_1"}),
        ToolCall("same|fc_2", "b", "{}", {"call_id": "same", "response_item_id": "fc_2"}),
    ]
    uniquify_tool_call_ids(calls)
    assert [coalesce_tool_call_id(call) for call in calls] == ["same", "same_d2"]
    assert calls[1].response_item_id == "fc_2"
    assert calls[1].id == "same_d2|fc_2"
    assert calls[1].call_id == "same_d2"


def test_provider_minted_parallel_ids_normalize_deterministically():
    calls = [
        ToolCall("chatcmpl-tool-a", "read_file", "{}"),
        ToolCall("chatcmpl-tool-b", "read_file", "{}"),
    ]
    again = copy.deepcopy(calls)
    normalize_provider_tool_call_ids(calls)
    normalize_provider_tool_call_ids(again)
    assert [call.id for call in calls] == [call.id for call in again]
    assert all(call.id.startswith("call_") for call in calls)
    assert len({call.id for call in calls}) == 2
