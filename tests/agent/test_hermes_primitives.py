from concurrent.futures import ThreadPoolExecutor

from shittim_core.agent.budget import IterationBudget
from shittim_core.agent.repetition import is_repetition_dominated, is_runaway_repetition


def test_budget_consume_does_not_overspend_under_competing_threads():
    budget = IterationBudget(13)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: budget.consume(), range(80)))
    assert sum(results) == 13
    assert budget.used == 13
    assert budget.remaining == 0


def test_hermes_repetition_distinguishes_long_loop_from_small_requested_repetition():
    assert is_repetition_dominated(
        "a long repeated segment with enough length to indicate a loop. " * 100
    )
    assert is_runaway_repetition(
        "a long repeated segment with enough length to indicate a loop. " * 100
    )
    assert not is_repetition_dominated("hello " * 5)
