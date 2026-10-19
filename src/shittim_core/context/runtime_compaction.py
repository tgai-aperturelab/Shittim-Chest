"""One optional summary attempt charged against the existing run's limits."""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from typing import Any

from shittim_core.agent.ports import AsyncModel, RunControl, RunPorts
from shittim_core.context.compactor import Compactor
from shittim_core.domain.model import NormalizedResponse
from shittim_core.domain.types import RunHalted, RunRequest, StaleView


class _MeteredModel:
    """Per-preparation counter also survives a cancelled summary coroutine."""

    def __init__(self, model: AsyncModel) -> None:
        self.model, self.calls = model, 0

    async def complete(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> NormalizedResponse:
        self.calls += 1
        return await self.model.complete(messages, tools)


async def prepare_run(
    request: RunRequest,
    ports: RunPorts,
    control: RunControl,
    compactor: Compactor,
    max_chars: int,
) -> RunRequest:
    """At most one summary; never refill the run's model-call or elapsed-time budget.

    The wrapper owns no Agent loop. It uses a local model counter rather than mutating
    a shared Compactor, so concurrent preparations cannot steal each other's budgets.
    """
    if max_chars < 1:
        raise ValueError("Compaction threshold must be positive")
    if control.cancel.is_set():
        return request
    started = time.perf_counter()
    meter = _MeteredModel(compactor.model)

    async def prepare() -> None:
        basis = await ports.build_view(request)
        if request.max_model_calls < 2:
            return
        size = compactor.message_chars(list(basis.messages))
        if size <= max_chars:
            return
        await ports.check_current(request, basis)
        if control.cancel.is_set():
            return
        remaining = request.timeout_seconds - (time.perf_counter() - started)
        if remaining <= 0:
            control.stop("run_timeout")
            return
        bounded = Compactor(
            compactor.builder,
            meter,
            timeout_seconds=min(compactor.timeout_seconds, remaining),
            message_projection=compactor.message_projection,
        )
        result = await bounded.maybe_compact(request, basis, max_chars=max_chars)
        if result.reason == "summary_timeout" and remaining <= compactor.timeout_seconds:
            # The run's deadline, rather than the shorter summary cap, fired. Timer
            # resolution must not reopen a tiny remainder for another model request.
            control.stop("run_timeout")

    worker = asyncio.create_task(prepare())
    stopped = asyncio.create_task(control.cancel.wait())
    try:
        done, _ = await asyncio.wait(
            (worker, stopped), timeout=request.timeout_seconds, return_when=asyncio.FIRST_COMPLETED
        )
        if worker not in done:
            if not control.cancel.is_set():
                control.stop("run_timeout")
            worker.cancel()
        try:
            await worker
        except RunHalted as exc:
            control.stop(str(exc))
        except (StaleView, asyncio.CancelledError):
            pass  # The main loop rebuilds a stale view; explicit stop remains latched.
    except asyncio.CancelledError:
        control.stop("caller_cancelled")
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
    finally:
        stopped.cancel()
        await asyncio.gather(stopped, return_exceptions=True)
    # View building and authority checks also spend the existing run's deadline,
    # even when the model-call budget or safe window suppresses summarization.
    remaining = request.timeout_seconds - (time.perf_counter() - started)
    if control.reason == "run_timeout":
        remaining = 0.0
    if remaining <= 0:
        control.stop("run_timeout")
    return replace(
        request,
        max_model_calls=request.max_model_calls - meter.calls,
        timeout_seconds=max(remaining, 0.000001),
    )
