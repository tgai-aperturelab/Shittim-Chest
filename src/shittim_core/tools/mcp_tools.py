"""MCP tools use the application's single durable authorization/effect dispatcher."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from shittim_core.domain.types import ExecutionContext, ToolResult
from shittim_core.engine.mcp_client import MCPRuntime, MCPServerConfig, MCPToolDefinition
from shittim_core.tools.executor import Handler, ToolExecutor, ToolSpec

__all__ = ["MCPRuntime", "MCPServerConfig", "register_mcp_tools"]


def register_mcp_tools(
    executor: ToolExecutor,
    runtime: MCPRuntime,
    *,
    host_resource: str | None = None,
    host_mutation_guard: Callable[[], None] | None = None,
) -> tuple[MCPToolDefinition, ...]:
    """Call after runtime.start; register discovery without granting any caller access.

    Every call claims its endpoint's effect resource regardless of remote readOnlyHint.
    AccessContext.allowed_tools AND allowed_resources must explicitly authorize it.
    """
    definitions = runtime.definitions()
    if executor.schemas(frozenset(tool.name for tool in definitions)):
        raise ValueError("MCP tool names are already registered")

    def handler(tool: MCPToolDefinition) -> Handler:
        async def invoke(arguments: dict[str, Any], _ctx: ExecutionContext) -> ToolResult:
            if tool.transport == "stdio" and host_mutation_guard is not None:
                try:
                    host_mutation_guard()
                except Exception:
                    return ToolResult(
                        "not_executed",
                        "Local execution authority or unknown process outcome must be resolved before this MCP operation",
                    )
            return await runtime.call(tool, arguments)

        return invoke

    for tool in definitions:
        resources = (tool.resource_key,)
        if tool.transport == "stdio" and host_resource is not None:
            resources = (*resources, host_resource)
        executor.register(
            ToolSpec(
                tool.name,
                f"MCP server {tool.server_name}, tool {tool.remote_name}. Server-provided description: {tool.description}",
                tool.parameters,
                handler(tool),
                lambda _args, _ctx, claimed=resources: claimed,
            )
        )
    return definitions
