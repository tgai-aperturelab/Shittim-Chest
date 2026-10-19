"""OpenAI-compatible transport with Hermes-derived normalization and stream assembly.

No provider login, global profile, tool discovery or hidden SDK retries.
"""

from __future__ import annotations

import copy
import math
from types import SimpleNamespace
from typing import Any, cast

from shittim_core.agent.repetition import RunawayStreamWatch
from shittim_core.agent.sdk_fields import field
from shittim_core.agent.tool_call_stream import ToolCallAccumulator
from shittim_core.domain.json_args import parse_tool_arguments
from shittim_core.domain.model import NormalizedResponse, ToolCall, Usage
from shittim_core.engine.hermes.reasoning_carriers import (
    StreamCarriers,
    carrier_record,
    shape_wire_carriers,
)
from shittim_core.engine.hermes.reasoning_summaries import append_streamed_reasoning_detail
from shittim_core.engine.hermes.replay_routes import (
    base_url_host_matches,
    model_consumes_thought_signature,
    must_echo_reasoning,
    reasoning_carriers,
)


class ModelProtocolError(Exception):
    """A response cannot safely enter the model/tool transcript."""


class ModelTransientError(Exception):
    """Retry is limited by the loop's model-call budget, never SDK automatic retries."""


def validate_response(response: NormalizedResponse) -> None:
    if response.finish_reason not in {"stop", "tool_calls", "content_filter"}:
        raise ModelProtocolError("incomplete_or_unsupported_finish_reason")
    for call in response.tool_calls or []:
        if not call.name:
            raise ModelProtocolError("invalid_tool_call")
        try:
            parse_tool_arguments(call.arguments)
        except (TypeError, ValueError) as exc:
            raise ModelProtocolError("invalid_tool_arguments") from exc
    if response.finish_reason == "content_filter" and response.tool_calls:
        raise ModelProtocolError("refused_tool_call")
    if response.finish_reason == "tool_calls" and not response.tool_calls:
        raise ModelProtocolError("missing_tool_calls")
    if not response.tool_calls and not (response.content or "").strip():
        raise ModelProtocolError("empty_response")


def response_to_message(response: NormalizedResponse) -> dict[str, Any]:
    """Canonical replay row; native carriers are retained even on a different wire route.

    Adapted from Hermes build_assistant_message / _assistant_tool_call_dict.
    Application state owns timestamps, IDs and persistence; hidden reasoning never
    becomes the public content. Opaque native data is not interpreted as an API mode.
    """
    message: dict[str, Any] = {"role": "assistant", "content": response.content or ""}
    if response.tool_calls:
        calls: list[dict[str, Any]] = []
        for call in response.tool_calls:
            item: dict[str, Any] = {
                "id": call.id,
                "type": "function",
                "function": {"name": call.name, "arguments": call.arguments},
            }
            data = copy.deepcopy(call.provider_data or {})
            for key in ("call_id", "response_item_id", "extra_content"):
                if key in data:
                    item[key] = data[key]
            if data.get("thought_signature"):
                item["function"]["thought_signature"] = data["thought_signature"]
            if data:
                item["_provider_data"] = data
            calls.append(item)
        message["tool_calls"] = calls
    data = copy.deepcopy(response.provider_data or {})
    if data:
        message["_provider_data"] = data
    for key in (
        "reasoning_content",
        "reasoning_details",
        "anthropic_content_blocks",
        "bedrock_content_blocks",
        "codex_reasoning_items",
        "codex_message_items",
    ):
        if key in data:
            message[key] = data[key]
    if response.reasoning is not None:
        message["reasoning"] = response.reasoning
    if record := carrier_record(response):
        message.update({key: value for key, value in record.items() if key != "type"})
        message["reasoning_details"] = [*message.get("reasoning_details", ()), record]
    return message


def wire_messages(
    messages: list[dict[str, Any]],
    *,
    reasoning_echo: bool,
    model: str = "",
    base_url: str = "",
) -> list[dict[str, Any]]:
    """Route-specific wire copy; canonical history and native signatures stay intact."""
    native = {"extra_content", "reasoning_opaque", "reasoning_text"}
    carriers: frozenset[str] = (
        reasoning_carriers(base_url)
        if base_url
        else (frozenset({"reasoning_content"}) if reasoning_echo else frozenset())
    )
    shaped = shape_wire_carriers(copy.deepcopy(messages), model=model, base_url=base_url)
    result: list[dict[str, Any]] = []
    for source in shaped:
        allowed = {"role", "content", "tool_calls", "tool_call_id", "name"}
        if source.get("role") == "assistant":
            allowed |= native | set(carriers)
        item: dict[str, Any] = {key: value for key, value in source.items() if key in allowed}
        if item.get("tool_calls"):
            calls: list[dict[str, Any]] = []
            for call in item["tool_calls"]:
                function: dict[str, Any] = call.get("function") or {}
                fn: dict[str, Any] = {
                    key: function[key] for key in ("name", "arguments") if key in function
                }
                if base_url_host_matches(base_url, "githubcopilot.com") and function.get(
                    "thought_signature"
                ):
                    fn["thought_signature"] = function["thought_signature"]
                wire_call: dict[str, Any] = {
                    "id": call.get("id"),
                    "type": "function",
                    "function": fn,
                }
                if model_consumes_thought_signature(model, base_url) and call.get("extra_content"):
                    wire_call["extra_content"] = call["extra_content"]
                calls.append(wire_call)
            item["tool_calls"] = calls
            if "reasoning_content" in carriers and (
                reasoning_echo or must_echo_reasoning(model, base_url)
            ):
                item["reasoning_content"] = item.get("reasoning_content") or " "
        result.append(item)
    return result


class OpenAIModel:
    def __init__(
        self,
        *,
        model: str,
        base_url: str,
        api_key: str,
        max_tokens: int = 4096,
        timeout: float = 30.0,
        reasoning_echo: bool = False,
        client: Any = None,
    ) -> None:
        if (
            not model
            or not base_url
            or max_tokens < 1
            or timeout <= 0
            or not math.isfinite(timeout)
        ):
            raise ValueError("Explicit model route and finite limits are required")
        self.model = model
        self.base_url = base_url
        self.max_tokens = max_tokens
        self.reasoning_echo = reasoning_echo
        self._owns_client = client is None
        if client is None:
            from openai import AsyncOpenAI

            client = AsyncOpenAI(api_key=api_key, base_url=base_url, timeout=timeout, max_retries=0)
        self.client: Any = client

    async def close(self) -> None:
        if self._owns_client:
            await self.client.close()

    async def complete(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> NormalizedResponse:
        from openai import APIConnectionError, APIStatusError, RateLimitError

        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": wire_messages(
                messages,
                reasoning_echo=self.reasoning_echo,
                model=self.model,
                base_url=self.base_url,
            ),
            "max_tokens": self.max_tokens,
            "stream": True,
        }
        if tools:
            kwargs["tools"] = copy.deepcopy(tools)
        stream: Any = None
        try:
            stream = await self.client.chat.completions.create(**kwargs)
            accumulator = ToolCallAccumulator()
            text: list[str] = []
            reasoning: list[str] = []
            details: list[dict[str, Any]] = []
            carriers = StreamCarriers()
            refusals: list[str] = []
            finish_reason: str | None = None
            usage: Usage | None = None
            text_watch, reasoning_watch = RunawayStreamWatch(), RunawayStreamWatch()
            async for chunk in stream:
                raw_usage = getattr(chunk, "usage", None)
                if raw_usage:
                    usage = Usage.from_openai(raw_usage)
                for choice in getattr(chunk, "choices", []) or []:
                    if getattr(choice, "index", 0) != 0:
                        continue
                    if choice.finish_reason is not None:
                        raw = str(choice.finish_reason).lower()
                        finish_reason = {"max_tokens": "length"}.get(raw, raw)
                    delta = choice.delta
                    content = field(delta, "content")
                    if isinstance(content, str):
                        text.append(content)
                        if text_watch.feed(content):
                            raise ModelProtocolError("repetition_limit")
                    thought = field(delta, "reasoning_content") or field(delta, "reasoning")
                    thought = carriers.feed(delta, thought if isinstance(thought, str) else None)
                    if isinstance(thought, str):
                        reasoning.append(thought)
                        if reasoning_watch.feed(thought):
                            raise ModelProtocolError("reasoning_repetition_limit")
                    detail_delta = field(delta, "reasoning_details")
                    if isinstance(detail_delta, list):
                        for detail in cast(list[Any], detail_delta):
                            append_streamed_reasoning_detail(details, detail)
                    refusal = field(delta, "refusal")
                    if isinstance(refusal, str):
                        refusals.append(refusal)
                    deltas: list[Any] = getattr(delta, "tool_calls", None) or []
                    for call_delta in deltas:
                        accumulator.feed(call_delta)
            # Hermes' completion gate: a dropped stream is never repaired into an action.
            if finish_reason is None:
                raise ModelProtocolError("stream_ended_without_finish")
            calls = [
                ToolCall(
                    id=str(item["id"]) or None,
                    name=item["function"]["name"],
                    arguments=item["function"]["arguments"],
                    provider_data={
                        key: item[key]
                        for key in ("extra_content", "thought_signature")
                        if item.get(key) is not None
                    }
                    or None,
                )
                for _, item in sorted(accumulator.materialize().items())
            ]
            content = "".join(text)
            if not content.strip() and refusals and not calls:
                content, finish_reason = "".join(refusals), "content_filter"
            carrier_message = SimpleNamespace()
            carriers.apply(SimpleNamespace(choices=[SimpleNamespace(message=carrier_message)]))
            provider_data: dict[str, Any] = dict(vars(carrier_message))
            if reasoning:
                provider_data["reasoning_content"] = "".join(reasoning)
            if details:
                provider_data["reasoning_details"] = details
            response = NormalizedResponse(
                content=content,
                tool_calls=calls or None,
                finish_reason=finish_reason,
                usage=usage,
                provider_data=provider_data or None,
            )
            validate_response(response)
            return response
        except (APIConnectionError, RateLimitError) as exc:
            raise ModelTransientError(type(exc).__name__) from exc
        except APIStatusError as exc:
            if exc.status_code >= 500:
                raise ModelTransientError(f"provider_status_{exc.status_code}") from exc
            raise ModelProtocolError(f"provider_status_{exc.status_code}") from exc
        finally:
            if stream is not None:
                await stream.close()
