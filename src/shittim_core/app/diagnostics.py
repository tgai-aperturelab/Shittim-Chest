"""Inspect configured capabilities without opening sockets, databases or processes."""

from __future__ import annotations

import os
import shutil
from collections.abc import Mapping
from typing import Any

from shittim_core.app.config import Settings
from shittim_core.tools.execution import EXECUTION_TOOLS
from shittim_core.tools.history import HISTORY_TOOLS
from shittim_core.tools.qq_forward import QQ_FORWARD_TOOLS
from shittim_core.tools.runtime_skills import RUNTIME_SKILL_TOOLS
from shittim_core.tools.vision import VISION_TOOLS
from shittim_core.tools.web import WEB_TOOLS


def inspect_configuration(settings: Settings, environ: Mapping[str, str]) -> dict[str, Any]:
    builtins = {
        "ask_person",
        "wait_for_input",
        "manage_inquiry",
        "read_material",
        "create_material",
        "send_material",
    }
    registered = (
        (builtins - {"send_material"}) | RUNTIME_SKILL_TOOLS | QQ_FORWARD_TOOLS | HISTORY_TOOLS
    )
    if settings.napcat_shared_local_files:
        registered |= {"send_material"}
    if settings.allowed_tools.intersection(WEB_TOOLS):
        registered |= WEB_TOOLS
    if settings.allowed_tools.intersection(VISION_TOOLS):
        registered |= VISION_TOOLS
    if settings.execution_backend != "disabled":
        registered |= EXECUTION_TOOLS
    rows: list[dict[str, Any]] = []
    for name in sorted(
        builtins
        | RUNTIME_SKILL_TOOLS
        | EXECUTION_TOOLS
        | WEB_TOOLS
        | VISION_TOOLS
        | QQ_FORWARD_TOOLS
        | HISTORY_TOOLS
    ):
        reason = ""
        if name not in registered:
            reason = "capability_not_enabled_in_configuration"
        elif name == "send_material" and not settings.napcat_shared_local_files:
            reason = "shared_file_delivery_not_configured"
        elif name == "search_files" and not shutil.which("rg"):
            reason = "ripgrep_not_installed"
        elif (
            name in EXECUTION_TOOLS
            and settings.execution_backend == "bubblewrap"
            and (
                os.name == "nt"
                or not (
                    settings.execution_bwrap_path
                    and settings.execution_bwrap_path.is_file()
                    or shutil.which("bwrap")
                )
            )
        ):
            reason = "linux_bubblewrap_backend_unavailable"
        elif name == "terminal" and not (
            shutil.which("pwsh") or shutil.which("powershell")
            if os.name == "nt"
            else shutil.which("bash")
        ):
            reason = "execution_shell_not_installed"
        rows.append(
            {
                "name": name,
                "implemented": True,
                "registered": name in registered,
                "configured_grant": name in settings.allowed_tools,
                "ready": not reason,
                "live_readiness": "not_checked",
                "reason": reason,
            }
        )
    return {
        "kind": "configuration_only_not_live_verification",
        "model": settings.model,
        "model_key_present": bool(environ.get(settings.api_key_env)),
        "qq_token_present": bool(environ.get(settings.token_env)),
        "allowed_user_count": len(settings.allowed_users),
        "tools": rows,
        "runtime_skill_roots": [
            {"id": root.id, "exists": root.path.is_dir()} for root in settings.runtime_skill_roots
        ],
        "execution_backend": settings.execution_backend,
        "progress": {
            "enabled": settings.progress_enabled,
            "min_interval_seconds": settings.progress_min_interval_seconds,
            "live_readiness": "not_checked",
        },
        "effective_execution_network": "host"
        if settings.execution_backend == "local_host"
        else settings.execution_network,
        "execution_boundary": "service_account_permissions_not_a_sandbox"
        if settings.execution_backend == "local_host"
        else "task_filesystem_pid_namespaces_with_configured_network"
        if settings.execution_backend == "bubblewrap"
        else "disabled",
        "mcp_servers": [
            {
                "name": s.name,
                "transport": s.transport,
                "discovery": "not_run",
                "allowed_remote_tools": list(dict(settings.mcp_tool_grants).get(s.name, ())),
                "missing_environment_references": [
                    ref for ref in (*s.env.values(), *s.headers.values()) if not environ.get(ref)
                ],
            }
            for s in settings.mcp_servers
        ],
    }
