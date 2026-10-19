"""Explicit MCP 2.3.0 sessions owned by one asyncio task per configured server.

The SDK's AnyIO scopes enter and exit in that same task. No global server discovery,
automatic reconnect, request retry, sampling, or permission inference from server text.
Hermes migration rationale and fixed source hashes: third_party/hermes-mcp-manifest.json.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import math
import os
import re
from collections.abc import Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Mapping, cast
from urllib.parse import urlsplit

from jsonschema import Draft202012Validator

from shittim_core.domain.types import ToolResult
from shittim_core.engine.hermes.mcp_result import strip_reserved_meta_keys


def _empty_mapping() -> dict[str, str]:
    return {}


@dataclass(frozen=True)
class MCPServerConfig:
    name: str
    transport: Literal["stdio", "streamable_http"]
    command: str | None = None
    args: tuple[str, ...] = ()
    cwd: Path | None = None
    env: Mapping[str, str] = field(default_factory=_empty_mapping, repr=False)
    url: str | None = None
    headers: Mapping[str, str] = field(default_factory=_empty_mapping, repr=False)
    timeout_seconds: float = 30.0
    max_pages: int = 100
    max_tools: int = 10000
    effect_scope: str | None = None

    def __post_init__(self) -> None:
        if not self.name or "\x00" in self.name:
            raise ValueError("MCP server requires an explicit nonempty name")
        if not math.isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("MCP timeout must be finite and positive")
        if self.max_pages < 1 or self.max_tools < 1:
            raise ValueError("MCP discovery limits must be positive")
        if self.transport == "stdio":
            if not self.command or self.cwd is None or self.url is not None:
                raise ValueError("stdio requires command and cwd, and cannot specify url")
            cwd = Path(self.cwd).resolve(strict=True)
            if not cwd.is_dir():
                raise ValueError("MCP cwd must be a directory")
            object.__setattr__(self, "cwd", cwd)
        elif self.transport == "streamable_http":
            url = urlsplit(self.url or "")
            if (
                url.scheme not in {"http", "https"}
                or not url.hostname
                or url.username
                or url.password
                or url.fragment
            ):
                raise ValueError(
                    "MCP HTTP requires an explicit HTTP(S) URL without embedded credentials or fragment"
                )
            if self.command is not None or self.args or self.cwd is not None or self.env:
                raise ValueError("HTTP configuration cannot contain stdio launch parameters")
        else:
            raise ValueError("Unsupported MCP transport")
        if self.effect_scope is not None and not self.effect_scope:
            raise ValueError("An explicit effect scope cannot be empty")
        object.__setattr__(self, "args", tuple(self.args))
        object.__setattr__(self, "env", MappingProxyType(dict(self.env)))
        object.__setattr__(self, "headers", MappingProxyType(dict(self.headers)))

    @property
    def resource_key(self) -> str:
        # Logical server aliases of the same endpoint must not bypass an unknown
        # effect. Configuration may explicitly join several endpoints' effect scope.
        if self.effect_scope is not None:
            identity: Any = ["scope", self.effect_scope]
        elif self.transport == "stdio":
            identity = ["stdio", self.command, self.args, str(self.cwd)]
        else:
            parsed = urlsplit(self.url or "")
            default_port = 443 if parsed.scheme == "https" else 80
            identity = [
                "http",
                parsed.scheme,
                parsed.hostname,
                parsed.port or default_port,
                parsed.path.rstrip("/"),
                parsed.query,
            ]
        return (
            "mcp:" + hashlib.sha256(json.dumps(identity, ensure_ascii=False).encode()).hexdigest()
        )


def namespaced_tool_name(server: str, tool: str) -> str:
    """Readable, deterministic wire name; collisions are checked before registration."""

    def slug(value: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_-]", "_", value)

    digest = hashlib.sha256(json.dumps([server, tool], ensure_ascii=False).encode()).hexdigest()[
        :20
    ]
    return f"mcp_{slug(server)[:12]}_{slug(tool)[:20]}_{digest}"


def _check_local_schema(schema: dict[str, Any]) -> None:
    # Schema documents are server data, not authorization for network fetches by
    # jsonschema's reference resolver. Local $defs references remain supported.
    pending: list[Any] = [schema]
    while pending:
        item = pending.pop()
        if isinstance(item, dict):
            node = cast(dict[str, Any], item)
            for key in ("$ref", "$dynamicRef"):
                if key in node and (
                    not isinstance(node[key], str) or not node[key].startswith("#")
                ):
                    raise ValueError("External MCP schema references are not supported")
            pending.extend(node.values())
        elif isinstance(item, list):
            pending.extend(cast(list[Any], item))
    Draft202012Validator.check_schema(schema)


def _input_schema(value: dict[str, Any]) -> dict[str, Any]:
    schema = copy.deepcopy(value)
    if schema.get("type", "object") != "object":
        raise ValueError("MCP tool arguments must have an object JSON schema")
    schema.setdefault("type", "object")
    _check_local_schema(schema)
    return schema


@dataclass(frozen=True)
class MCPToolDefinition:
    name: str
    server_name: str
    remote_name: str
    description: str
    parameters: dict[str, Any]
    resource_key: str
    transport: Literal["stdio", "streamable_http"]


@dataclass
class _Call:
    name: str
    arguments: dict[str, Any]
    future: asyncio.Future[ToolResult]
    started: bool = False
    cancelled: bool = False


class _Server:
    def __init__(
        self, config: MCPServerConfig, start_guard: Callable[[], None] | None = None
    ) -> None:
        self.config = config
        self._start_guard = start_guard
        self.state = "not_started"
        self.error: str | None = None
        self.definitions: list[MCPToolDefinition] = []
        self._queue: asyncio.Queue[_Call | None] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._ready: asyncio.Future[None] | None = None
        self._closing = False

    async def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("MCP server sessions cannot be implicitly restarted")
        self._ready = asyncio.get_running_loop().create_future()
        self._task = asyncio.create_task(self._serve())
        await asyncio.shield(self._ready)

    async def _serve(self) -> None:
        active: _Call | None = None
        self.state = "starting"
        try:
            if self.config.transport == "stdio" and self._start_guard is not None:
                self._start_guard()
            from mcp import ClientSession, StdioServerParameters
            from mcp.client.stdio import stdio_client
            from mcp.types import PaginatedRequestParams

            async with AsyncExitStack() as stack:
                if self.config.transport == "stdio":
                    # SDK inherits only its documented OS bootstrap allowlist,
                    # plus these explicitly configured entries; never os.environ.
                    params = StdioServerParameters(
                        command=self.config.command or "",
                        args=list(self.config.args),
                        cwd=self.config.cwd,
                        env=dict(self.config.env),
                        encoding="utf-8",
                    )
                    # Server stderr is not user-facing tool output; do not forward
                    # arbitrary server logs or credentials into QQ/application logs.
                    errlog = stack.enter_context(open(os.devnull, "w", encoding="utf-8"))
                    streams = await stack.enter_async_context(stdio_client(params, errlog=errlog))
                else:
                    import httpx2
                    from mcp.client.streamable_http import streamable_http_client

                    client = await stack.enter_async_context(
                        httpx2.AsyncClient(
                            headers=dict(self.config.headers),
                            timeout=self.config.timeout_seconds,
                            trust_env=False,
                            follow_redirects=False,
                        )
                    )
                    streams = await stack.enter_async_context(
                        streamable_http_client(
                            self.config.url or "",
                            http_client=client,
                            terminate_on_close=True,
                        )
                    )
                session = await stack.enter_async_context(
                    ClientSession(
                        streams[0],
                        streams[1],
                        read_timeout_seconds=self.config.timeout_seconds,
                    )
                )
                initialized = await session.initialize()
                if initialized.capabilities.tools is not None:
                    cursor: str | None = None
                    seen_cursors: set[str] = set()
                    seen_names: set[str] = set()
                    for _ in range(self.config.max_pages):
                        page = await session.list_tools(
                            params=PaginatedRequestParams(cursor=cursor)
                        )
                        for tool in page.tools:
                            if not tool.name or tool.name in seen_names:
                                raise ValueError(
                                    "MCP tool listing contains duplicate or empty tool names"
                                )
                            seen_names.add(tool.name)
                            if len(seen_names) > self.config.max_tools:
                                raise ValueError("MCP tool listing exceeded configured maximum")
                            if tool.output_schema is not None:
                                # The SDK validates tool results with this schema;
                                # disallow implicit remote fetching there as well.
                                _check_local_schema(tool.output_schema)
                            self.definitions.append(
                                MCPToolDefinition(
                                    namespaced_tool_name(self.config.name, tool.name),
                                    self.config.name,
                                    tool.name,
                                    tool.description or "",
                                    _input_schema(tool.input_schema),
                                    self.config.resource_key,
                                    self.config.transport,
                                )
                            )
                        cursor = page.next_cursor
                        if cursor is None:
                            break
                        if cursor in seen_cursors:
                            raise ValueError("MCP tool listing repeated a pagination cursor")
                        seen_cursors.add(cursor)
                    else:
                        raise ValueError("MCP tool listing exceeded configured page maximum")
                self.state = "ready"
                assert self._ready is not None
                self._ready.set_result(None)
                while (active := await self._queue.get()) is not None:
                    if self._closing or active.cancelled:
                        if not active.future.done():
                            active.future.set_result(
                                ToolResult(
                                    "not_executed",
                                    "MCP session is closing or request was withdrawn before dispatch",
                                )
                            )
                        active = None
                        continue
                    active.started = True
                    try:
                        result = await session.call_tool(
                            active.name,
                            active.arguments,
                            read_timeout_seconds=self.config.timeout_seconds,
                        )
                        # Preserve text, structured output and non-text content in
                        # their protocol format; never replace binary blocks with text.
                        payload = result.model_dump(mode="json", by_alias=True, exclude_none=True)
                        for envelope in [payload, *payload.get("content", [])]:
                            if not isinstance(envelope, dict) or "_meta" not in envelope:
                                continue
                            item = cast(dict[str, Any], envelope)
                            metadata = strip_reserved_meta_keys(item["_meta"])
                            if metadata:
                                item["_meta"] = metadata
                            else:
                                item.pop("_meta", None)
                        outcome = ToolResult(
                            "failed" if result.is_error else "succeeded",
                            json.dumps(payload, ensure_ascii=False),
                        )
                        if not active.future.done():
                            active.future.set_result(outcome)
                        active = None
                    except Exception as exc:
                        # The request crossed the dispatch boundary. A transport or
                        # decode error cannot prove that the remote effect did not occur.
                        self.error = type(exc).__name__
                        if not active.future.done():
                            active.future.set_result(
                                ToolResult(
                                    "unknown",
                                    "MCP result is unconfirmed; automatic retry is disabled",
                                )
                            )
                        active = None
                        self.state = "faulted"
                        break
        except BaseException as exc:
            self.error = type(exc).__name__
            self.state = "faulted"
            if self._ready is not None and not self._ready.done():
                self._ready.set_result(None)
            if active is not None and not active.future.done():
                active.future.set_result(
                    ToolResult(
                        "unknown" if active.started else "not_executed",
                        "MCP session ended before a confirmed result",
                    )
                )
        finally:
            if self.state != "faulted":
                self.state = "closed"
            if self._ready is not None and not self._ready.done():
                self._ready.set_result(None)
            while not self._queue.empty():
                pending = self._queue.get_nowait()
                if pending is not None and not pending.future.done():
                    pending.future.set_result(
                        ToolResult("not_executed", "MCP session is unavailable")
                    )

    async def call(self, remote_name: str, arguments: dict[str, Any]) -> ToolResult:
        if self.state != "ready" or self._closing:
            return ToolResult(
                "not_executed", "MCP server is not ready; check its configured connection"
            )
        future: asyncio.Future[ToolResult] = asyncio.get_running_loop().create_future()
        request = _Call(remote_name, copy.deepcopy(arguments), future)
        self._queue.put_nowait(request)
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            # Do not turn caller cancellation into a second remote request. The
            # session owner still observes any result from an already dispatched call.
            request.cancelled = True
            raise

    async def close(self) -> None:
        self._closing = True
        if self._task is None:
            self.state = "closed"
            return
        self._queue.put_nowait(None)
        try:
            await asyncio.wait_for(asyncio.shield(self._task), self.config.timeout_seconds + 10)
        except TimeoutError:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)


class MCPRuntime:
    """Explicit configured sessions. Empty configuration opens no connection."""

    def __init__(
        self,
        servers: tuple[MCPServerConfig, ...] = (),
        *,
        stdio_start_guard: Callable[[], None] | None = None,
    ) -> None:
        if len({server.name for server in servers}) != len(servers):
            raise ValueError("MCP server names must be unique")
        self._servers = {config.name: _Server(config, stdio_start_guard) for config in servers}
        self._started = False

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("MCPRuntime has already started")
        self._started = True
        try:
            await asyncio.gather(*(server.start() for server in self._servers.values()))
        except BaseException:
            await self.close()
            raise

    def status(self) -> list[dict[str, Any]]:
        return [
            {
                "server": name,
                "state": server.state,
                "error": server.error,
                "tool_count": len(server.definitions) if server.state == "ready" else 0,
            }
            for name, server in self._servers.items()
        ]

    def definitions(self) -> tuple[MCPToolDefinition, ...]:
        tools = tuple(
            tool
            for server in self._servers.values()
            if server.state == "ready"
            for tool in server.definitions
        )
        if len({tool.name for tool in tools}) != len(tools):
            raise ValueError("MCP tool namespace collision; no tools were registered")
        return tools

    async def call(self, tool: MCPToolDefinition, arguments: dict[str, Any]) -> ToolResult:
        server = self._servers.get(tool.server_name)
        if server is None or tool not in server.definitions:
            return ToolResult("not_executed", "MCP tool was not discovered on this runtime")
        return await server.call(tool.remote_name, arguments)

    async def close(self) -> None:
        await asyncio.gather(*(server.close() for server in self._servers.values()))
