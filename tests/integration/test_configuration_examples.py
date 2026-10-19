"""Configuration examples describe real tool groups without starting a service."""

import json
from pathlib import Path

import pytest

from shittim_core.app.config import Settings
from shittim_core.app.diagnostics import inspect_configuration
from shittim_core.tools.execution import EXECUTION_TOOLS
from shittim_core.tools.history import HISTORY_TOOLS
from shittim_core.tools.qq_forward import QQ_FORWARD_TOOLS
from shittim_core.tools.runtime_skills import RUNTIME_SKILL_TOOLS
from shittim_core.tools.vision import VISION_TOOLS
from shittim_core.tools.web import WEB_TOOLS

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    "filename", ["config.execution.example.json", "config.sandbox.example.json"]
)
def test_complete_examples_load_and_report_all_tool_groups(tmp_path, filename):
    data = json.loads((ROOT / filename).read_text(encoding="utf-8"))
    path = tmp_path / filename
    path.write_text(json.dumps(data), encoding="utf-8")
    settings = Settings.load(path)
    groups = (
        EXECUTION_TOOLS
        | RUNTIME_SKILL_TOOLS
        | WEB_TOOLS
        | VISION_TOOLS
        | QQ_FORWARD_TOOLS
        | HISTORY_TOOLS
        | {
            "ask_person",
            "wait_for_input",
            "manage_inquiry",
            "read_material",
            "create_material",
            "send_material",
        }
    )
    assert settings.allowed_tools == groups
    assert settings.platform_public_files is True
    assert settings.napcat_shared_local_files is True
    assert settings.allowed_users == settings.allowed_groups == frozenset()
    report = inspect_configuration(settings, {settings.api_key_env: "not-an-actual-secret"})
    rows = {row["name"]: row for row in report["tools"]}
    assert set(rows) == groups
    assert all(rows[name]["registered"] and rows[name]["configured_grant"] for name in groups)
    assert all(row["live_readiness"] == "not_checked" for row in rows.values())
    assert "not-an-actual-secret" not in json.dumps(report)
    assert not settings.data_dir.exists()
    if filename == "config.sandbox.example.json":
        assert settings.execution_backend == "bubblewrap"
        assert settings.execution_network == "isolated"
    else:
        assert report["effective_execution_network"] == "host"
        assert report["execution_boundary"] == "service_account_permissions_not_a_sandbox"


def test_forward_diagnostics_distinguish_implemented_and_granted(tmp_path):
    settings = Settings("123", "TOKEN", "model", "https://unused.invalid/v1", "KEY", tmp_path)
    report = inspect_configuration(settings, {})
    rows = {row["name"]: row for row in report["tools"]}
    for name in QQ_FORWARD_TOOLS:
        assert rows[name]["implemented"] and rows[name]["registered"]
        assert not rows[name]["configured_grant"]


def test_mcp_header_environment_reference_report_never_contains_secret(tmp_path):
    data = json.loads((ROOT / "config.execution.example.json").read_text(encoding="utf-8"))
    data["mcp_servers"] = [
        {
            "name": "example",
            "transport": "streamable_http",
            "url": "https://mcp.example.invalid/mcp",
            "headers": {"Authorization": "FUSION_MCP_AUTHORIZATION"},
            "allowed_tools": ["lookup_document"],
        }
    ]
    path = tmp_path / "config.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    settings = Settings.load(path)
    missing = inspect_configuration(settings, {})["mcp_servers"][0]
    assert missing["missing_environment_references"] == ["FUSION_MCP_AUTHORIZATION"]
    report = inspect_configuration(
        settings, {"FUSION_MCP_AUTHORIZATION": "Bearer synthetic-secret"}
    )
    assert report["mcp_servers"][0]["missing_environment_references"] == []
    assert report["mcp_servers"][0]["discovery"] == "not_run"
    assert "synthetic-secret" not in json.dumps(report)
