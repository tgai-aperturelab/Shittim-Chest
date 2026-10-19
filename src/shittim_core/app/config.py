"""Explicit candidate configuration; importing this module reads no files or environment."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlsplit

from shittim_core.engine.mcp_client import MCPServerConfig
from shittim_core.tools.execution import EXECUTION_TOOLS
from shittim_core.tools.history import HISTORY_TOOLS
from shittim_core.tools.qq_forward import QQ_FORWARD_TOOLS
from shittim_core.tools.runtime_skills import RUNTIME_SKILL_TOOLS, SkillRoot
from shittim_core.tools.vision import VISION_TOOLS
from shittim_core.tools.web import WEB_TOOLS, WebConfig


def _identity(value: object) -> bool:
    return isinstance(value, str) and bool(re.fullmatch(r"[0-9]+", value))


def _reference(value: object) -> bool:
    return isinstance(value, str) and bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value))


def _bounded_int(value: object, lower: int, upper: int) -> bool:
    return type(value) is int and lower <= value <= upper


def _nonempty_text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _timeout(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and 0 < value <= 3600
    )


def _progress_interval(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and 0 <= value <= 3600
    )


@dataclass(frozen=True)
class Settings:
    bot_id: str
    token_env: str
    model: str
    base_url: str
    api_key_env: str
    data_dir: Path
    host: str = "127.0.0.1"
    port: int = 3002
    allowed_users: frozenset[str] = frozenset()
    allowed_groups: frozenset[str] = frozenset()
    allowed_tools: frozenset[str] = frozenset()
    allowed_resources: frozenset[str] = frozenset()
    allowed_download_hosts: frozenset[str] = frozenset()
    platform_public_files: bool = False
    max_attachment_bytes: int = 4 * 1024 * 1024
    max_context_chars: int = 32000
    napcat_shared_local_files: bool = False
    max_concurrent: int = 2
    max_model_calls: int = 8
    max_tool_calls: int = 12
    timeout_seconds: float = 120.0
    model_timeout_seconds: float = 30.0
    protocol_timeout_seconds: float = 15.0
    max_output_tokens: int = 4096
    reasoning_echo: bool = False
    execution_backend: Literal["disabled", "local_host", "bubblewrap"] = "disabled"
    execution_timeout_seconds: float = 300.0
    execution_bwrap_path: Path | None = None
    execution_python_runtime: Path | None = None
    execution_network: Literal["isolated", "host"] = "isolated"
    runtime_skill_roots: tuple[SkillRoot, ...] = ()
    web_config: WebConfig = WebConfig()
    mcp_servers: tuple[MCPServerConfig, ...] = ()
    mcp_tool_grants: tuple[tuple[str, tuple[str, ...]], ...] = ()
    vision_model: str | None = None
    progress_enabled: bool = False
    progress_min_interval_seconds: float = 15.0

    def __post_init__(self) -> None:
        if not _identity(self.bot_id):
            raise ValueError("bot_id must be an explicit QQ identity")
        if not _reference(self.token_env) or not _reference(self.api_key_env):
            raise ValueError("Credentials must be environment variable names")
        if not _nonempty_text(self.model) or not _nonempty_text(self.host):
            raise ValueError("Explicit model and bind host required")
        if not _nonempty_text(self.base_url):
            raise ValueError("Explicit model endpoint required")
        url = urlsplit(self.base_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
        ):
            raise ValueError("Model endpoint must not contain credentials or query parameters")
        for ids in (self.allowed_users, self.allowed_groups):
            if any(not _identity(identity) for identity in ids):
                raise ValueError("Allow lists require exact numeric identities")
        if self.bot_id in self.allowed_users:
            raise ValueError("The robot cannot be its own allowed human sender")
        if not self.allowed_tools.issubset(
            {
                "ask_person",
                "wait_for_input",
                "manage_inquiry",
                "read_material",
                "send_material",
                "create_material",
            }
            | EXECUTION_TOOLS
            | RUNTIME_SKILL_TOOLS
            | WEB_TOOLS
            | VISION_TOOLS
            | QQ_FORWARD_TOOLS
            | HISTORY_TOOLS
        ):
            raise ValueError("Unregistered tools are not permitted by this application")
        if self.execution_backend not in {"disabled", "local_host", "bubblewrap"}:
            raise ValueError("Execution backend must be disabled, local_host or bubblewrap")
        if self.execution_network not in {"isolated", "host"}:
            raise ValueError("Execution network must be isolated or host")
        if self.vision_model is not None and not _nonempty_text(self.vision_model):
            raise ValueError("vision_model must be a nonempty model name")
        if (
            self.allowed_tools.intersection(EXECUTION_TOOLS)
            and self.execution_backend == "disabled"
        ):
            raise ValueError("Execution tools require an explicitly configured execution backend")
        if "skill_manage" in self.allowed_tools and self.execution_backend == "disabled":
            raise ValueError("Task skill management requires an execution workspace")
        if len({root.id for root in self.runtime_skill_roots}) != len(self.runtime_skill_roots):
            raise ValueError("Duplicate runtime skill root ID")
        server_names = {s.name for s in self.mcp_servers}
        if len(server_names) != len(self.mcp_servers) or any(
            name not in server_names for name, _ in self.mcp_tool_grants
        ):
            raise ValueError("MCP grants must identify unique configured servers")
        for server in self.mcp_servers:
            if any(
                not _reference(value) for value in (*server.env.values(), *server.headers.values())
            ):
                raise ValueError(
                    "MCP env/header values in Settings must reference environment variable names"
                )
        if type(self.napcat_shared_local_files) is not bool:
            raise ValueError("napcat_shared_local_files must be a JSON boolean")
        if type(self.platform_public_files) is not bool:
            raise ValueError("platform_public_files must be a JSON boolean")
        if "send_material" in self.allowed_tools and not self.napcat_shared_local_files:
            raise ValueError(
                "send_material requires an explicit shared-local-files deployment declaration"
            )
        for host in self.allowed_download_hosts:
            if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", host) or ".." in host:
                raise ValueError(
                    "Download hosts must be exact lowercase hostnames without wildcards"
                )
        if any(not resource for resource in self.allowed_resources):
            raise ValueError("Resources require exact nonempty identifiers")
        for value, low, high in (
            (self.port, 0, 65535),
            (self.max_concurrent, 1, 16),
            (self.max_model_calls, 1, 1000),
            (self.max_tool_calls, 1, 1000),
            (self.max_output_tokens, 1, 65536),
            (self.max_attachment_bytes, 1, 64 * 1024 * 1024),
            (self.max_context_chars, 1, 2_000_000),
        ):
            if not _bounded_int(value, low, high):
                raise ValueError(
                    "Integer configuration value exceeds the finite application limits"
                )
        for value in (
            self.timeout_seconds,
            self.model_timeout_seconds,
            self.protocol_timeout_seconds,
            self.execution_timeout_seconds,
        ):
            if not _timeout(value):
                raise ValueError("Timeouts must be finite and positive, at most one hour")
        if type(self.reasoning_echo) is not bool:
            raise ValueError("reasoning_echo must be a JSON boolean")
        if type(self.progress_enabled) is not bool:
            raise ValueError("progress_enabled must be a JSON boolean")
        if not _progress_interval(self.progress_min_interval_seconds):
            raise ValueError("Progress interval must be finite and between zero and one hour")

    @classmethod
    def load(cls, path: str | Path) -> Settings:
        """Load one JSON file, resolving relative storage paths against its directory."""
        config_path = Path(path).resolve()
        decoded: object = json.loads(config_path.read_text(encoding="utf-8"))
        if not isinstance(decoded, dict):
            raise ValueError("Configuration must be an object")
        data = cast(dict[str, Any], decoded)
        for field in (
            "allowed_users",
            "allowed_groups",
            "allowed_tools",
            "allowed_resources",
            "allowed_download_hosts",
        ):
            values = data.get(field, [])
            if not isinstance(values, list):
                raise ValueError(f"{field} must be a list of strings")
            if any(not isinstance(v, str) for v in cast(list[object], values)):
                raise ValueError(f"{field} must be a list of strings")
            data[field] = frozenset(cast(list[str], values))
        directory = data.get("data_dir")
        if not isinstance(directory, str) or not directory.strip():
            raise ValueError("Explicit data_dir required")
        location = Path(directory)
        data["data_dir"] = (
            (config_path.parent / location).resolve()
            if not location.is_absolute()
            else location.resolve()
        )
        roots = data.get("runtime_skill_roots", [])
        if not isinstance(roots, list):
            raise ValueError("runtime_skill_roots must be a list")
        resolved_roots: list[SkillRoot] = []
        for raw in cast(list[object], roots):
            if not isinstance(raw, dict):
                raise ValueError("Runtime skill root must be an object")
            entry = cast(dict[str, Any], raw)
            if set(entry) - {"id", "path", "source"} or not all(
                isinstance(entry.get(k), str) and entry[k].strip() for k in ("id", "path")
            ):
                raise ValueError("Runtime skill root requires id and path")
            root_path = Path(entry["path"])
            root_path = (
                (config_path.parent / root_path).resolve()
                if not root_path.is_absolute()
                else root_path.resolve()
            )
            resolved_roots.append(
                SkillRoot(entry["id"], root_path, entry.get("source", "operator_configured"))
            )
        data["runtime_skill_roots"] = tuple(resolved_roots)
        for option in ("execution_bwrap_path", "execution_python_runtime"):
            if data.get(option):
                execution_path = Path(data[option])
                data[option] = (
                    (config_path.parent / execution_path).resolve()
                    if not execution_path.is_absolute()
                    else execution_path.resolve()
                )
        if "web_config" in data:
            if not isinstance(data["web_config"], dict):
                raise ValueError("web_config must be an object")
            web_options = cast(dict[str, Any], data["web_config"])
            web_options["allowed_private_networks"] = tuple(
                web_options.get("allowed_private_networks", ())
            )
            data["web_config"] = WebConfig(**web_options)
        servers = data.get("mcp_servers", [])
        if not isinstance(servers, list):
            raise ValueError("mcp_servers must be a list")
        configured: list[MCPServerConfig] = []
        grants: list[tuple[str, tuple[str, ...]]] = []
        for raw in cast(list[object], servers):
            if not isinstance(raw, dict):
                raise ValueError("MCP server must be an object")
            options = dict(cast(dict[str, Any], raw))
            allowed = options.pop("allowed_tools", [])
            if not isinstance(allowed, list) or any(
                not isinstance(n, str) for n in cast(list[object], allowed)
            ):
                raise ValueError("MCP allowed_tools must be a list of remote tool names or '*'")
            if options.get("cwd"):
                location = Path(options["cwd"])
                options["cwd"] = (
                    (config_path.parent / location).resolve()
                    if not location.is_absolute()
                    else location.resolve()
                )
            server = MCPServerConfig(**options)
            configured.append(server)
            grants.append((server.name, tuple(cast(list[str], allowed))))
        data["mcp_servers"], data["mcp_tool_grants"] = tuple(configured), tuple(grants)
        try:
            return cls(**data)
        except TypeError as error:
            raise ValueError("Missing or unknown configuration fields") from error
