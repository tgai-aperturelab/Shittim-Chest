import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from shittim_core.agent.ports import RunControl
from shittim_core.context.builder import ContextBuilder
from shittim_core.context.compactor import Compactor
from shittim_core.context.runtime_compaction import prepare_run
from shittim_core.domain.model import NormalizedResponse
from shittim_core.domain.types import AccessContext, ConversationAddress, InputEvent, RunRequest
from shittim_core.state import StateStore


class Model:
    def __init__(self, *, fail=False, wait=False):
        self.calls = 0
        self.fail, self.wait = fail, wait
        self.entered, self.cleaned = asyncio.Event(), asyncio.Event()

    async def complete(self, messages, tools):
        self.calls += 1
        self.entered.set()
        try:
            if self.wait:
                await asyncio.Event().wait()
            if self.fail:
                raise ValueError("fixture summary failure")
            return NormalizedResponse("先前资料的简短概括。", None, "stop")
        finally:
            self.cleaned.set()


async def test_preparation_preserves_route_projection_when_wrapping_metered_model(env):
    import json

    from shittim_core.agent.model import wire_messages

    store, builder, request, ports = env
    await long_history(store, builder, request)
    model = Model()
    projected_lengths = []

    def project(messages):
        # Stand in for a route-native carrier; this large required wire payload
        # must remain counted even when the model is wrapped for call metering.
        result = wire_messages(messages, reasoning_echo=False)
        result.append({"role": "system", "content": "required-route-state" * 500})
        projected_lengths.append(len(json.dumps(result, ensure_ascii=False, sort_keys=True)))
        return result

    before = store.history("c")
    await prepare_run(
        request, ports, RunControl(), Compactor(builder, model, message_projection=project), 4000
    )
    assert model.calls == 1
    assert len(projected_lengths) >= 3  # preflight, original, candidate share route projection
    assert store.history("c") == before  # required carrier still exceeds budget


class Ports:
    def __init__(self, builder):
        self.builder = builder

    async def build_view(self, request):
        return await self.builder.build(request)

    async def check_current(self, request, view):
        self.builder.store.assert_current(request.run_id, request.control_epoch, view.input_seq)


@pytest.fixture
def env(tmp_path):
    store = StateStore(tmp_path / "state.db")
    address = ConversationAddress("bot", "group", "g")
    access = AccessContext("owner", address.key)
    event = InputEvent(address, "owner", "0", "historical text " * 1000)
    identity, _ = store.ingest(event)
    store.create_context("c", event, access)
    store.attach_input("c", identity)
    epoch = store.claim_run("c", "r")
    request = RunRequest("r", "c", address, access, epoch, max_model_calls=3)
    builder = ContextBuilder(store)
    yield store, builder, request, Ports(builder)
    store.close()


async def long_history(store, builder, request):
    await builder.build(request)
    store.append_history("r", {"role": "assistant", "content": "ack"})
    for index in range(1, 6):
        identity, _ = store.ingest(
            InputEvent(request.address, "owner", str(index), "short followup")
        )
        store.attach_input("c", identity)
        await builder.build(request)
        store.append_history("r", {"role": "assistant", "content": "ack"})


@pytest.mark.parametrize("max_calls,max_chars", [(1, 1000), (3, 100000)])
async def test_no_summary_call_keeps_call_budget_but_charges_preparation_time(
    env, max_calls, max_chars
):
    store, builder, request, ports = env
    request = replace(request, max_model_calls=max_calls)
    await long_history(store, builder, request)
    model = Model()
    prepared = await prepare_run(request, ports, RunControl(), Compactor(builder, model), max_chars)
    assert prepared.max_model_calls == request.max_model_calls
    assert 0 < prepared.timeout_seconds <= request.timeout_seconds
    assert model.calls == 0


async def test_single_summary_is_charged_to_same_run_and_remaining_time(env):
    store, builder, request, ports = env
    await long_history(store, builder, request)
    model = Model()
    prepared = await prepare_run(request, ports, RunControl(), Compactor(builder, model), 4000)
    assert prepared.run_id == request.run_id
    assert model.calls == 1
    assert prepared.max_model_calls == request.max_model_calls - 1
    assert 0 < prepared.timeout_seconds < request.timeout_seconds
    assert any(row.get("_summary") for row in store.history("c"))


async def test_failed_summary_still_consumes_one_call_and_keeps_history(env):
    store, builder, request, ports = env
    await long_history(store, builder, request)
    before = store.history("c")
    model = Model(fail=True)
    prepared = await prepare_run(request, ports, RunControl(), Compactor(builder, model), 4000)
    assert model.calls == 1
    assert prepared.max_model_calls == request.max_model_calls - 1
    assert store.history("c") == before


async def test_unsafe_window_keeps_model_budget_and_charges_elapsed_time(env):
    _, builder, request, ports = env
    model = Model()
    prepared = await prepare_run(request, ports, RunControl(), Compactor(builder, model), 1000)
    assert prepared.max_model_calls == request.max_model_calls
    assert 0 < prepared.timeout_seconds <= request.timeout_seconds
    assert model.calls == 0


async def test_stop_during_summary_keeps_history_and_charges_started_call(env):
    store, builder, request, ports = env
    await long_history(store, builder, request)
    before = store.history("c")
    model, control = Model(wait=True), RunControl()
    work = asyncio.create_task(
        prepare_run(request, ports, control, Compactor(builder, model), 4000)
    )
    await model.entered.wait()
    control.stop("stop_requested")
    prepared = await work
    assert model.cleaned.is_set()
    assert control.cancel.is_set()
    assert prepared.max_model_calls == request.max_model_calls - 1
    assert store.history("c") == before


@pytest.mark.parametrize("elapsed,exhausted", [(7.0, False), (121.0, True)])
async def test_slow_view_without_summary_spends_existing_deadline(
    env, monkeypatch, elapsed, exhausted
):
    from shittim_core.context import runtime_compaction

    _, builder, request, _ = env
    ticks = [1000.0]
    monkeypatch.setattr(runtime_compaction, "time", SimpleNamespace(perf_counter=lambda: ticks[0]))

    class SlowViewPorts(Ports):
        async def build_view(self, request):
            view = await super().build_view(request)
            ticks[0] += elapsed
            return view

    model, control = Model(), RunControl()
    prepared = await prepare_run(
        request, SlowViewPorts(builder), control, Compactor(builder, model), 100000
    )
    assert model.calls == 0
    assert prepared.max_model_calls == request.max_model_calls
    assert prepared.timeout_seconds == max(request.timeout_seconds - elapsed, 0.000001)
    assert control.cancel.is_set() is exhausted
    assert control.reason == ("run_timeout" if exhausted else "")


async def test_total_timeout_cannot_restart_full_run_deadline(env):
    store, builder, request, ports = env
    await long_history(store, builder, request)
    request = replace(request, timeout_seconds=0.1)
    model, control = Model(wait=True), RunControl()
    prepared = await prepare_run(request, ports, control, Compactor(builder, model), 4000)
    assert model.calls == 1
    assert model.cleaned.is_set()
    assert control.reason == "run_timeout"
    assert control.cancel.is_set()
    assert prepared.timeout_seconds <= 0.000001
    assert prepared.max_model_calls == request.max_model_calls - 1


async def test_compaction_result_counts_failed_attempt(env):
    store, builder, request, _ = env
    await long_history(store, builder, request)
    basis = await builder.build(request)
    result = await Compactor(builder, Model(fail=True)).maybe_compact(
        request, basis, max_chars=4000
    )
    assert result.status == "failed"
    assert result.model_calls == 1


async def test_short_summary_timeout_preserves_remaining_run_budget(env):
    store, builder, request, ports = env
    await long_history(store, builder, request)
    before = store.history("c")
    model, control = Model(wait=True), RunControl()
    prepared = await prepare_run(
        request, ports, control, Compactor(builder, model, timeout_seconds=0.01), 4000
    )
    assert model.calls == 1
    assert not control.cancel.is_set()
    assert prepared.max_model_calls == request.max_model_calls - 1
    assert 0 < prepared.timeout_seconds < request.timeout_seconds
    assert store.history("c") == before


async def test_caller_cancel_is_latched_and_started_summary_charged(env):
    store, builder, request, ports = env
    await long_history(store, builder, request)
    model, control = Model(wait=True), RunControl()
    work = asyncio.create_task(
        prepare_run(request, ports, control, Compactor(builder, model), 4000)
    )
    await model.entered.wait()
    work.cancel()
    prepared = await work
    assert model.cleaned.is_set()
    assert control.reason == "caller_cancelled"
    assert control.cancel.is_set()
    assert prepared.max_model_calls == request.max_model_calls - 1
