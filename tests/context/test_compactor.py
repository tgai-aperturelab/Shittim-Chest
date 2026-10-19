import copy
import json
from dataclasses import replace

import pytest

from shittim_core.agent.model import OpenAIModel, response_to_message, wire_messages
from shittim_core.context.builder import ContextBuilder
from shittim_core.context.compactor import (
    Compactor,
    align_boundary_backward,
    align_boundary_forward,
)
from shittim_core.domain.model import NormalizedResponse, ToolCall
from shittim_core.domain.types import (
    AccessContext,
    ConversationAddress,
    InputEvent,
    RunRequest,
    ToolResult,
)
from shittim_core.state import StateStore


class SummaryModel:
    def __init__(self, action=None, error=False):
        self.calls = []
        self.action, self.error = action, error

    async def complete(self, messages, tools):
        self.calls.append(copy.deepcopy(messages))
        if self.action:
            self.action()
        if self.error:
            raise RuntimeError("summary fixture failure")
        return NormalizedResponse("之前已讨论资料，仍需按最新请求处理。", None, "stop")


@pytest.fixture
def env(tmp_path):
    store = StateStore(tmp_path / "state.db")
    address = ConversationAddress("bot", "group", "g")
    access = AccessContext("owner", address.key, allowed_resources=frozenset({"fixture"}))
    first = InputEvent(address, "owner", "0", "old facts " * 1000)
    input_id, _ = store.ingest(first)
    store.create_context("c", first, access)
    store.attach_input("c", input_id)
    request = RunRequest("r", "c", address, access, store.claim_run("c", "r"))
    yield store, ContextBuilder(store), request
    store.close()


async def old_history(store, builder, request):
    await builder.build(request)
    store.append_history(
        "r", {"role": "assistant", "content": "done", "reasoning_content": "HIDDEN_THOUGHT"}
    )
    event = InputEvent(request.address, "owner", "1", "older discussion " * 1000)
    identity, _ = store.ingest(event)
    store.attach_input("c", identity)
    await builder.build(request)
    store.append_history("r", {"role": "assistant", "content": "done"})
    latest = InputEvent(request.address, "owner", "2", "current request")
    identity, _ = store.ingest(latest)
    store.attach_input("c", identity)
    return await builder.build(request)


async def test_compaction_retains_sources_latest_request_and_does_not_reinflate(env):
    store, builder, request = env
    basis = await old_history(store, builder, request)
    model = SummaryModel()
    result = await Compactor(builder, model).maybe_compact(
        request, basis, max_chars=4000, keep_tail=2
    )
    assert result.status == "compacted"
    assert len(model.calls) == 1
    assert "HIDDEN_THOUGHT" not in model.calls[0][1]["content"]
    candidate = store.history("c")
    assert candidate[0]["_covered_input_ids"] == sorted(basis.source_ids)
    assert "current request" in candidate[-1]["content"]
    count = len(candidate)
    rebuilt = await builder.build(request)
    assert len(store.history("c")) == count
    assert set(rebuilt.source_ids) == set(basis.source_ids)


async def test_below_budget_uses_no_summary_call(env):
    _, builder, request = env
    basis = await builder.build(request)
    model = SummaryModel()
    outcome = await Compactor(builder, model).maybe_compact(request, basis, max_chars=100000)
    assert outcome.reason == "within_budget"
    assert not model.calls


async def test_model_failure_keeps_exact_history(env):
    store, builder, request = env
    basis = await old_history(store, builder, request)
    before = store.history("c")
    result = await Compactor(builder, SummaryModel(error=True)).maybe_compact(
        request, basis, max_chars=4000, keep_tail=2
    )
    assert result.status == "failed"
    assert store.history("c") == before


async def test_new_input_during_summary_makes_candidate_stale(env):
    store, builder, request = env
    basis = await old_history(store, builder, request)
    before = store.history("c")

    def arrival():
        identity, _ = store.ingest(InputEvent(request.address, "owner", "3", "new correction"))
        store.attach_input("c", identity)

    result = await Compactor(builder, SummaryModel(action=arrival)).maybe_compact(
        request, basis, max_chars=4000, keep_tail=2
    )
    assert result.status == "stale"
    assert store.history("c") == before
    assert store.list_events("c")[-1]["event"].text == "new correction"


async def test_protected_facts_are_canonical_not_model_invention(env):
    store, builder, request = env
    basis = await old_history(store, builder, request)
    inquiry, _ = store.create_inquiry(
        "r", "owner", request.address, "Unanswered question", basis.input_seq
    )
    store.append_history(
        "r",
        {
            "role": "assistant",
            "tool_calls": [
                {"id": "a", "type": "function", "function": {"name": "write", "arguments": "{}"}}
            ],
        },
    )
    effect = store.begin_effect(
        "r", "a", "write", {"secret": "DO_NOT_SUMMARIZE"}, ["fixture"], basis.input_seq
    )
    actual = ToolResult("succeeded", "PRIVATE_OUTPUT", evidence_refs=("evidence:a",))
    store.finish_effect(effect, actual)
    store.upsert_tool_result(
        "r", "a", {"role": "tool", "tool_call_id": "a", "content": actual.to_json()}
    )
    basis = await builder.build(request)
    outcome = await Compactor(builder, SummaryModel()).maybe_compact(
        request, basis, max_chars=5000, keep_tail=2
    )
    assert outcome.status == "compacted"
    facts = next(m["_protected_facts"] for m in store.history("c") if "_protected_facts" in m)
    assert facts["inquiries"][0]["inquiry_id"] == inquiry
    assert facts["effects"][0]["effect_id"] == effect
    assert facts["effects"][0]["status"] == "succeeded"
    assert "DO_NOT_SUMMARIZE" not in json.dumps(facts)
    assert "PRIVATE_OUTPUT" not in json.dumps(facts)


async def test_unknown_effect_skips_summary_and_preserves_blocker(env):
    store, builder, request = env
    basis = await old_history(store, builder, request)
    store.append_history(
        "r",
        {
            "role": "assistant",
            "tool_calls": [{"id": "a", "function": {"name": "write", "arguments": "{}"}}],
        },
    )
    effect = store.begin_effect("r", "a", "write", {}, ["fixture"], basis.input_seq)
    result = ToolResult("unknown", "pending")
    store.finish_effect(effect, result)
    store.upsert_tool_result(
        "r", "a", {"role": "tool", "tool_call_id": "a", "content": result.to_json()}
    )
    basis = await builder.build(request)
    model = SummaryModel()
    outcome = await Compactor(builder, model).maybe_compact(
        request, basis, max_chars=5000, keep_tail=2
    )
    assert outcome.reason == "unresolved_effect"
    assert not model.calls
    assert store.get_effect(effect)["status"] == "unknown"


def test_hermes_boundaries_keep_entire_parallel_tool_group():
    history = [
        {"role": "user"},
        {"role": "assistant", "tool_calls": [{"id": "a"}, {"id": "b"}]},
        {"role": "tool", "tool_call_id": "a"},
        {"role": "tool", "tool_call_id": "b"},
        {"role": "user"},
    ]
    assert align_boundary_backward(history, 2) == 1
    assert align_boundary_backward(history, 3) == 1
    assert align_boundary_forward(history, 2) == 4


async def test_wider_source_view_is_not_sent_to_summary_model(env):
    store, builder, request = env
    basis = await old_history(store, builder, request)
    contaminated = [*basis.messages, {"role": "user", "content": "PRIVATE", "_audience": "other"}]
    basis = replace(basis, messages=tuple(contaminated))
    model = SummaryModel()
    before = store.history("c")
    result = await Compactor(builder, model).maybe_compact(
        request, basis, max_chars=4000, keep_tail=2
    )
    assert result.reason == "view_scope_mismatch"
    assert not model.calls
    assert store.history("c") == before


async def test_new_inquiry_during_summary_invalidates_protected_facts(env):
    store, builder, request = env
    basis = await old_history(store, builder, request)
    before = store.history("c")

    def ask():
        store.create_inquiry("r", "owner", request.address, "new question", basis.input_seq)

    result = await Compactor(builder, SummaryModel(action=ask)).maybe_compact(
        request, basis, max_chars=4000, keep_tail=2
    )
    assert result.reason == "protected_facts_changed"
    assert store.history("c") == before


async def test_401_sources_compact_under_default_wire_budget_without_audit_duplication(env):
    store, builder, request = env
    for index in range(1, 401):
        event = InputEvent(
            request.address,
            "owner",
            str(index),
            "current request" if index == 400 else "source facts " * 45,
        )
        input_id, _ = store.ingest(event)
        store.attach_input("c", input_id)
    basis = await builder.build(request)
    assert len(basis.source_ids) == 401
    model = SummaryModel()
    result = await Compactor(builder, model).maybe_compact(request, basis, max_chars=32000)
    assert result.status == "compacted", result
    assert result.candidate_chars < 32000
    summary = next(row for row in store.history("c") if row.get("_covered_input_ids"))
    assert summary["_covered_input_ids"] == sorted(basis.source_ids)
    assert "_source_ids" not in summary
    rebuilt = await builder.build(request)
    again = await Compactor(builder, model).maybe_compact(request, rebuilt, max_chars=32000)
    assert again.reason == "within_budget" and len(model.calls) == 1
    assert set(rebuilt.source_ids) == set(basis.source_ids)
    # A later compaction merges coverage as a set; it does not clone the previous
    # coverage list into both metadata fields or into the next summary prompt.
    for index in range(401, 405):
        identity, _ = store.ingest(
            InputEvent(
                request.address,
                "owner",
                str(index),
                "latest" if index == 404 else "new long facts " * 3000,
            )
        )
        store.attach_input("c", identity)
    newer = await builder.build(request)
    result = await Compactor(builder, model).maybe_compact(
        request, newer, max_chars=32000, keep_tail=1
    )
    assert result.status == "compacted"
    row = next(row for row in store.history("c") if row.get("_covered_input_ids"))
    assert len(row["_covered_input_ids"]) == 405 and "_source_ids" not in row
    assert "_covered_input_ids" not in model.calls[-1][1]["content"]
    assert set((await builder.build(request)).source_ids) == set(newer.source_ids)


@pytest.mark.parametrize(
    "base_url,model_name",
    [
        ("https://openrouter.ai/api/v1", "fixture"),
        ("https://api.githubcopilot.com", "gemini-fixture"),
        ("https://api.deepseek.com/v1", "deepseek-reasoner"),
    ],
)
def test_budget_uses_actual_wire_route_and_preserves_provider_carriers(env, base_url, model_name):
    _, builder, _ = env
    model = OpenAIModel(
        model=model_name, base_url=base_url, api_key="fixture", client=object(), reasoning_echo=True
    )
    response = NormalizedResponse(
        "answer",
        [ToolCall("call", "read_file", "{}", {"thought_signature": "signature" * 600})],
        "tool_calls",
        provider_data={
            "reasoning_opaque": "opaque" * 600,
            "reasoning_content": "reasoning" * 600,
            "reasoning_details": [{"type": "reasoning.encrypted", "data": "encrypted" * 600}],
        },
    )
    row = response_to_message(response)
    row["_covered_input_ids"] = ["internal-audit" * 1000]
    rows = [
        {"role": "system", "content": "system budget is real"},
        row,
        {"role": "tool", "tool_call_id": "call", "content": "result"},
    ]
    before = copy.deepcopy(rows)
    projected = wire_messages(rows, reasoning_echo=True, model=model_name, base_url=base_url)
    measured = Compactor(builder, model).message_chars(rows)
    assert measured == len(json.dumps(projected, ensure_ascii=False, sort_keys=True))
    assert measured > 4000  # required route-native carriers are not discarded
    assert "_covered_input_ids" not in json.dumps(projected)
    assert rows == before
