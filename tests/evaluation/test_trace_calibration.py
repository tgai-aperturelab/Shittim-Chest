"""A real offline run plus deliberate bad traces calibrate the contract oracle."""

import copy

import pytest

from shittim_core.domain.types import ToolResult
from tests.integration.test_runtime import (
    ADDRESS,
    event,
    final,
    idle,
    register_effect,
    runtime,
    tool_response,
)
from tools.trace_oracle import evaluate_trace


@pytest.mark.asyncio
async def test_actual_run_and_duplicate_scope_pairing_negative_controls(tmp_path):
    rt = runtime(tmp_path / "state.db", [tool_response(), final("已得到工具结果")])

    async def perform(_args, _ctx):
        return ToolResult("succeeded", "independent fixture operation")

    register_effect(rt, perform)
    try:
        accepted = await rt.coordinator.accept(event("执行合成测试"))
        await idle(rt)
        trace = {
            "schema_version": 1,
            "messages": rt.store.history(accepted.context_id),
            "send_attempts": [
                {"delivery_id": row["attempt_id"], "address": ADDRESS.key} for row in rt.qq.calls
            ],
            "views": [
                {
                    "audience": ADDRESS.key,
                    "source_audiences": [
                        message["_audience"] for message in request if "_audience" in message
                    ],
                }
                for request in rt.model.requests
            ],
        }
        expected = frozenset({ADDRESS.key})
        assert evaluate_trace(trace, allowed_addresses=expected)["status"] == "pass"
        duplicate = copy.deepcopy(trace)
        duplicate["send_attempts"].append(copy.deepcopy(duplicate["send_attempts"][0]))
        assert (
            "duplicate_send_attempt"
            in evaluate_trace(duplicate, allowed_addresses=expected)["findings"]
        )
        unauthorized = copy.deepcopy(trace)
        unauthorized["send_attempts"][0]["address"] = "unauthorized-private-address"
        assert (
            "unauthorized_delivery_address"
            in evaluate_trace(unauthorized, allowed_addresses=expected)["findings"]
        )
        privacy = copy.deepcopy(trace)
        privacy["views"][0]["source_audiences"].append("unrelated-private-address")
        assert (
            "out_of_scope_view_source"
            in evaluate_trace(privacy, allowed_addresses=expected)["findings"]
        )
        unpaired = copy.deepcopy(trace)
        unpaired["messages"] = [m for m in unpaired["messages"] if not m.get("tool_calls")]
        assert (
            "orphan_tool_result" in evaluate_trace(unpaired, allowed_addresses=expected)["findings"]
        )
        broken_harness = copy.deepcopy(trace)
        del broken_harness["send_attempts"]
        assert (
            evaluate_trace(broken_harness, allowed_addresses=expected)["status"] == "inconclusive"
        )
    finally:
        await rt.close()
