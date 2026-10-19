"""Pure skill metadata extraction adapted from Hermes agent/skill_utils.py (MIT).

Upstream 9e01b44dd23978f9ff40b7837467f3a9f1582130. Host/profile discovery,
plugin imports, installation and shell preprocessing deliberately live nowhere here.
"""

from __future__ import annotations

import re
import sys
from typing import Any

import yaml

EXCLUDED_SKILL_DIRS = frozenset({
    ".git", ".github", ".hub", ".archive", ".curator_backups", ".locks",
    ".venv", "venv", "node_modules", "site-packages", "__pycache__",
    ".tox", ".nox", ".pytest_cache", ".mypy_cache", ".ruff_cache", ".history",
})
SKILL_SUPPORT_DIRS = frozenset({"references", "templates", "assets", "scripts"})
PLATFORM_MAP = {"macos": "darwin", "linux": "linux", "windows": "win32"}


def parse_frontmatter(content: str) -> tuple[dict[str, Any], str]:
    """Hermes fence/BOM algorithm, strict safe YAML instead of lossy fallback.

Malformed metadata is reported to callers rather than silently reinterpreted.
PyYAML is the application dependency; no Hermes global config is imported.
"""
    content = content.removeprefix("\ufeff")
    end_match = re.search(r"\n---\s*\n", content[3:]) if content.startswith("---") else None
    if not end_match:
        if content.startswith("---"):
            raise ValueError("Unterminated skill frontmatter")
        return {}, content
    # Include the newline before the closing fence: YAML block scalars depend on it.
    parsed = yaml.safe_load(content[3:end_match.start() + 4])
    if parsed is not None and not isinstance(parsed, dict):
        raise ValueError("Skill frontmatter must be a mapping")
    return parsed or {}, content[end_match.end() + 3:]


def skill_matches_platform(frontmatter: dict[str, Any]) -> bool:
    """Hermes platform list matching, with absent platform meaning portable."""
    platforms = frontmatter.get("platforms")
    if not platforms:
        return True
    if not isinstance(platforms, list):
        platforms = [platforms]
    return any(sys.platform.startswith(PLATFORM_MAP.get(str(p).lower().strip(),
                         str(p).lower().strip())) for p in platforms)


# Readiness declaration normalization from tools/skills_tool_setup.py.
_ENV_VAR_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _as_dict_list(raw: Any) -> list:
    """Accept a single mapping or a list; anything else is treated as empty."""
    return [raw] if isinstance(raw, dict) else raw if isinstance(raw, list) else []

def _clean_str(value: Any) -> str | None:
    """Stripped string when *value* is a non-blank str, else None."""
    return value.strip() if isinstance(value, str) and value.strip() else None

def required_environment_variables(frontmatter: dict[str, Any]) -> list[dict[str, Any]]:
    """Merge required_environment_variables, setup.collect_secrets and legacy
    prerequisites.env_vars into one deduped, validated list (first entry wins)."""
    setup = frontmatter.get("setup")
    setup = setup if isinstance(setup, dict) else {}
    setup_help = _clean_str(setup.get("help"))
    prereqs = frontmatter.get("prerequisites")
    legacy = (prereqs.get("env_vars") if isinstance(prereqs, dict) else None) or []
    legacy = [legacy] if isinstance(legacy, str) else legacy
    required: dict[str, dict[str, Any]] = {}  # env name -> entry, insertion-ordered, first wins
    declared = _as_dict_list(frontmatter.get("required_environment_variables"))
    entries = [{"name": i} if isinstance(i, str) else i for i in declared
               if isinstance(i, (str, dict))]
    # collect_secrets entries: env_var is the name; provider_url (or url) doubles as help.
    entries += [
        {"name": i.get("env_var"), "prompt": i.get("prompt"),
         "url": str(i.get("provider_url") or i.get("url") or "").strip() or None}
        for i in _as_dict_list(setup.get("collect_secrets")) if isinstance(i, dict)]
    entries += [{"name": str(v)} for v in legacy if str(v).strip()]
    for entry in entries:
        env_name = str(entry.get("name") or entry.get("env_var") or "").strip()
        if not env_name or env_name in required or not _ENV_VAR_NAME_RE.match(env_name):
            continue
        normalized: dict[str, Any] = {
            "name": env_name,
            "prompt": str(entry.get("prompt") or f"Enter value for {env_name}").strip()}
        if help_text := _clean_str(
                entry.get("help") or entry.get("provider_url") or entry.get("url") or setup_help):
            normalized["help"] = help_text
        if required_for := _clean_str(entry.get("required_for")):
            normalized["required_for"] = required_for
        if entry.get("optional"):
            normalized["optional"] = True
        required[env_name] = normalized
    return list(required.values())
