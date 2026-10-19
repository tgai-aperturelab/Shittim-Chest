"""One strict JSON-object decoder for model validation and tool dispatch."""

from __future__ import annotations

import json
import math
from typing import Any, NoReturn, cast


def _reject_constant(value: str) -> NoReturn:
    raise ValueError("Non-finite JSON constants are not tool arguments")


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("JSON number exceeds finite representation")
    return parsed


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON object key")
        result[key] = value
    return result


def parse_tool_arguments(raw: str) -> dict[str, Any]:
    """Reject duplicate decoded keys at every depth, non-finite numbers and non-objects.

    No syntax repair or implicit conversion is performed. Strings containing words such
    as 'NaN' remain ordinary strings; only JSON numeric values are restricted.
    """
    if type(raw) is not str:
        raise TypeError("Tool arguments must be a JSON string")
    parsed: Any = json.loads(
        raw,
        parse_constant=_reject_constant,
        parse_float=_finite_float,
        object_pairs_hook=_unique_object,
    )
    if not isinstance(parsed, dict):
        raise ValueError("Tool arguments must be a JSON object")
    return cast(dict[str, Any], parsed)
