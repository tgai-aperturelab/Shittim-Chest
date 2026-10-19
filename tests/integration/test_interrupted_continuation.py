"""Consumed input is not a completed request; resume saved history without replaying effects."""

import asyncio

from shittim_core.domain.types import RunHalted, RunRequest, ToolResult
from tests.integration.test_runtime import (
    event,
    final,
    idle,
    register_effect,
    runtime,
    tool_response,
)


async def test_shutdown_revokes_new_dispatch_and_interrupted_run_resumes_once(tmp_path):
    path = tmp_path / "state.db"
    entered, release = asyncio.Event(), asyncio.Event()
    rt = runtime(path, [tool_response()])
    actual_effects = []

    async def preflight_then_effect(_args, ctx):
        entered.set()
        await release.wait()
        try:
            rt.store.assert_current(ctx.run.run_id, ctx.run.control_epoch, ctx.input_seq)
        except RunHalted:
            return ToolResult("not_executed", "authority revoked before mutation")
        actual_effects.append("must not happen")
        return ToolResult("succeeded", "unexpected mutation")

    register_effect(rt, preflight_then_effect)
    accepted = await rt.coordinator.accept(event("完成这条请求", mid="1"))
    await asyncio.wait_for(entered.wait(), 1)
    rt.coordinator.begin_shutdown()
    release.set()
    await rt.close()
    assert actual_effects == [] and rt.qq.calls == []
    reopened = runtime(path, [final("基于保存的历史继续")])
    try:
        report = reopened.coordinator.recover()
        assert report["interrupted_contexts"] == [accepted.context_id]
        assert reopened.store.list_effects()[0]["status"] == "not_executed"
        assert reopened.coordinator.resume_pending() == [accepted.context_id]
        await idle(reopened)
        assert len(reopened.model.requests) == 1 and len(reopened.qq.calls) == 1
        assert len(reopened.store.list_events(accepted.context_id)) == 1
        assert reopened.coordinator.recover()["interrupted_contexts"] == []
        assert reopened.coordinator.resume_pending() == []
    finally:
        await reopened.close()


async def test_completed_effect_before_crash_resumes_model_from_saved_result(tmp_path):
    path = tmp_path / "state.db"
    before = runtime(path, [])
    source = event("执行一次并告诉我结果", mid="1")
    input_id, _ = before.store.ingest(source)
    access = before.coordinator.access_factory(source)
    before.store.create_context("ctx", source, access)
    before.store.attach_input("ctx", input_id)
    epoch = before.store.claim_run("ctx", "old-run")
    request = RunRequest("old-run", "ctx", source.address, access, epoch)
    await before.coordinator.ports.build_view(request)
    response = tool_response()
    await before.coordinator.ports.record_assistant(request, response)
    effect_id = before.store.begin_effect("old-run", "call1", "perform", {}, ["resource:one"], 1)
    before.store.finish_effect_with_history(
        effect_id,
        ToolResult("succeeded", "independent operation already occurred", effect_ref=effect_id),
    )
    before.store.close()  # Simulate lost process ownership, not a graceful Coordinator close.
    after = runtime(path, [final("已确认先前操作的结果")])
    try:
        assert after.coordinator.recover()["interrupted_contexts"] == ["ctx"]
        assert after.coordinator.resume_pending() == ["ctx"]
        await idle(after)
        assert len(after.store.list_effects()) == 1
        assert len(after.model.requests) == 1 and len(after.qq.calls) == 1
        assert any(
            "independent operation already occurred" in str(m.get("content"))
            for m in after.model.requests[0]
        )
        assert after.store.get_effect(effect_id)["status"] == "succeeded"
    finally:
        await after.close()


async def test_unknown_effect_blocks_automatic_interrupted_continuation(tmp_path):
    path = tmp_path / "state.db"
    before = runtime(path, [])
    source = event("请求", mid="1")
    input_id, _ = before.store.ingest(source)
    before.store.create_context("ctx", source, before.coordinator.access_factory(source))
    before.store.attach_input("ctx", input_id)
    before.store.claim_run("ctx", "old-run")
    before.store.consume_inputs("old-run", 1)
    before.store.begin_effect("old-run", "call1", "perform", {}, ["resource:one"], 1)
    before.store.close()
    after = runtime(path, [])
    try:
        assert after.coordinator.recover()["interrupted_contexts"] == ["ctx"]
        assert after.coordinator.resume_pending() == []
        assert after.coordinator.deferred_contexts["ctx"] == "unresolved_external_effect"
        await idle(after)
        assert not after.model.requests and not after.qq.calls
    finally:
        await after.close()
