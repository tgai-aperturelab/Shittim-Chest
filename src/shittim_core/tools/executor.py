from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol, cast

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from shittim_core.domain.json_args import parse_tool_arguments
from shittim_core.domain.model import ToolCall
from shittim_core.domain.types import ExecutionContext, StateConflict, ToolResult
from shittim_core.state import StateStore

type Handler = Callable[[dict[str, Any], ExecutionContext], Awaitable[ToolResult]]
type ResourceResolver = Callable[[dict[str, Any], ExecutionContext], tuple[str, ...]]


class _Validator(Protocol):
    def validate(self, instance: object) -> None: ...


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    parameters: dict[str, Any]
    handler: Handler
    resources: ResourceResolver


class ToolExecutor:
    def __init__(self, store: StateStore) -> None:
        self.store = store
        self._specs: dict[str, ToolSpec] = {}

    def register(self, spec: ToolSpec) -> None:
        if not spec.name or spec.name in self._specs:
            raise ValueError("Duplicate or empty tool name")
        Draft202012Validator.check_schema(spec.parameters)
        self._specs[spec.name] = spec

    def schemas(self, allowed: frozenset[str]) -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": s.name,
                    "description": s.description,
                    "parameters": s.parameters,
                },
            }
            for s in self._specs.values()
            if s.name in allowed
        ]

    async def execute(self, call: ToolCall, ctx: ExecutionContext) -> ToolResult:
        if call.name not in ctx.run.access.allowed_tools or call.name not in self._specs:
            return ToolResult("not_executed", "Tool is not authorized for this run")
        if not call.id:
            return ToolResult("not_executed", "Tool call has no identity")
        spec = self._specs[call.name]
        try:
            args = parse_tool_arguments(call.arguments)
            validator = cast(_Validator, Draft202012Validator(spec.parameters))
            validator.validate(args)
            resources = spec.resources(args, ctx)
            if any(r not in ctx.run.access.allowed_resources for r in resources):
                return ToolResult("not_executed", "Resource is not authorized")
        except (ValueError, ValidationError, TypeError, KeyError):
            return ToolResult("not_executed", "Invalid tool arguments or resource")
        self.store.assert_current(ctx.run.run_id, ctx.run.control_epoch, ctx.input_seq)
        try:
            effect_id = self.store.begin_effect(
                ctx.run.run_id, call.id, call.name, args, resources, ctx.input_seq
            )
        except StateConflict:
            return ToolResult("not_executed", "Duplicate or unresolved conflicting effect")
        try:
            result = await spec.handler(args, ctx)
        except asyncio.CancelledError:
            self.store.finish_effect_with_history(
                effect_id, ToolResult("unknown", "Interrupted effect", effect_ref=effect_id)
            )
            raise
        except Exception:
            result = ToolResult(
                "unknown", "Tool effect requires reconciliation", effect_ref=effect_id
            )
        result = ToolResult(
            result.status, result.content, result.evidence_refs, result.artifact_refs, effect_id
        )
        self.store.finish_effect_with_history(effect_id, result)
        return result
