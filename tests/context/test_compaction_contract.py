from dataclasses import replace

import pytest

from shittim_core.context.builder import ContextBuilder
from shittim_core.domain.types import AccessContext, ConversationAddress, InputEvent, RunRequest
from shittim_core.state import StateStore


@pytest.fixture
def setup(tmp_path):
    store = StateStore(tmp_path / "state.db")
    address = ConversationAddress("bot", "group", "g")
    access = AccessContext("owner", address.key)
    event = InputEvent(address, "owner", "1", "first")
    input_id, _ = store.ingest(event)
    store.create_context("c", event, access)
    store.attach_input("c", input_id)
    epoch = store.claim_run("c", "r")
    yield store, ContextBuilder(store), RunRequest("r", "c", address, access, epoch)
    store.close()


async def test_compaction_cas_rejects_new_input_without_losing_it(setup):
    store, builder, request = setup
    basis = await builder.build(request)
    event = InputEvent(request.address, "member_b", "2", "new correction")
    input_id, _ = store.ingest(event)
    store.attach_input("c", input_id)
    before = store.history("c")
    assert not builder.commit_compaction(
        request, basis, [{"role": "user", "content": "old summary"}]
    )
    assert store.history("c") == before
    assert any(x["event"].text == "new correction" for x in store.list_events("c"))


async def test_compaction_cas_rejects_control_epoch_change(setup):
    store, builder, request = setup
    basis = await builder.build(request)
    store.control("c", "owner", "stop")
    assert not builder.commit_compaction(request, basis, [{"role": "user", "content": "summary"}])


@pytest.mark.parametrize(
    "candidate",
    [
        [{"role": "tool", "tool_call_id": "orphan", "content": "x"}],
        [{"role": "assistant", "tool_calls": [{"id": "a"}]}],
        [
            {"role": "assistant", "tool_calls": [{"id": "a"}]},
            {"role": "user", "content": "interrupt"},
        ],
    ],
)
async def test_incomplete_tool_groups_do_not_replace_history(setup, candidate):
    store, builder, request = setup
    basis = await builder.build(request)
    before = store.history("c")
    with pytest.raises(ValueError):
        builder.commit_compaction(request, basis, candidate)
    assert store.history("c") == before


async def test_complete_tool_group_can_commit_and_raw_inputs_remain(setup):
    store, builder, request = setup
    basis = await builder.build(request)
    candidate = [
        {"role": "assistant", "tool_calls": [{"id": "a"}, {"id": "b"}]},
        {"role": "tool", "tool_call_id": "a", "content": "one"},
        {"role": "tool", "tool_call_id": "b", "content": "two"},
    ]
    assert builder.commit_compaction(request, basis, candidate)
    assert len(store.list_events("c")) == 1
    assert store.history("c") == candidate


async def test_compaction_cannot_widen_audience(setup):
    _, builder, request = setup
    basis = await builder.build(request)
    with pytest.raises(ValueError):
        builder.commit_compaction(
            request, basis, [{"role": "user", "content": "private", "_audience": "elsewhere"}]
        )


async def test_cutoff_does_not_mark_unseen_input_as_consumed(setup):
    store, builder, request = setup
    event = InputEvent(request.address, "owner", "2", "unseen")
    input_id, _ = store.ingest(event)
    store.attach_input("c", input_id)
    view = await builder.build(request, cutoff_seq=1)
    assert view.input_seq == 1
    assert not any("unseen" in m.get("content", "") for m in view.messages)


async def test_duplicate_call_ids_do_not_pass_complete_group_check(setup):
    _, builder, request = setup
    basis = await builder.build(request)
    candidate = [
        {"role": "assistant", "tool_calls": [{"id": "a"}, {"id": "a"}]},
        {"role": "tool", "tool_call_id": "a", "content": "one"},
    ]
    with pytest.raises(ValueError):
        builder.commit_compaction(request, basis, candidate)


async def test_different_audience_cannot_reuse_context(setup):
    _, builder, request = setup
    from shittim_core.domain.types import StateConflict

    denied = replace(request, access=replace(request.access, audience="different"))
    with pytest.raises(StateConflict):
        await builder.build(denied)
