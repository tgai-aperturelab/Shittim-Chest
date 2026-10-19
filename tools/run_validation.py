"""Explicit local validation; immutable attempts, no QQ/model live opt-ins or service changes."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import subprocess
import sys
import uuid
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def source_snapshot(root: Path) -> dict[str, str]:
    paths = [root / "pyproject.toml", *root.glob("requirements-*.lock")]
    for directory in ("src", "tests", "tools"):
        paths.extend((root / directory).rglob("*.py"))
    return {
        str(path.relative_to(root)).replace("\\", "/"): hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
        for path in sorted(paths)
        if path.is_file()
    }


def classify_pytest(exit_code: int | None, report: Path) -> dict[str, Any]:
    """Fixture failure, unavailable harness and successful selected tests are distinct."""
    try:
        root = ET.parse(report).getroot()
        suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
        if not suites:
            raise ValueError("No test suites")
        counts = {
            key: sum(int(suite.get(key, "0")) for suite in suites)
            for key in ("tests", "failures", "errors", "skipped")
        }
        if any(number < 0 for number in counts.values()):
            raise ValueError("Invalid count")
    except (OSError, ET.ParseError, ValueError):
        return {"status": "harness_error", "reason": "missing_or_invalid_test_report"}
    passed = counts["tests"] - counts["failures"] - counts["errors"] - counts["skipped"]
    if exit_code not in (0, 1) or counts["errors"] or passed < 0:
        status, reason = "harness_error", "test_setup_collection_or_execution_error"
    elif counts["failures"]:
        status, reason = "failed", "behavior_assertions_failed"
    elif exit_code != 0 or passed == 0:
        status, reason = "harness_error", "no_verified_behavior"
    else:
        status, reason = "passed", "selected_offline_behavior_verified"
    return {"status": status, "reason": reason, "counts": {**counts, "passed": max(0, passed)}}


def run_validation(root: Path, evidence_root: Path) -> Path:
    root = root.resolve(strict=True)
    attempt = evidence_root.resolve() / (
        datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    )
    attempt.mkdir(parents=True, exist_ok=False)
    before = source_snapshot(root)
    report = attempt / "pytest.xml"
    checks = [
        (
            "pytest",
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "-m",
                "not qq_live and not model_live",
                "--junitxml",
                str(report),
            ],
        ),
        ("ruff-check", [sys.executable, "-m", "ruff", "check", "src", "tests", "tools"]),
        (
            "ruff-format",
            [sys.executable, "-m", "ruff", "format", "--check", "src", "tests", "tools"],
        ),
        ("pyright", [sys.executable, "-m", "pyright", "--pythonpath", sys.executable]),
        ("pip-check", [sys.executable, "-m", "pip", "check"]),
    ]
    results = []
    for name, command in checks:
        started = datetime.now(UTC).isoformat()
        code, failure = None, ""
        with (attempt / (name + ".log")).open("wb") as output:
            try:
                completed = subprocess.run(
                    command,
                    cwd=root,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    timeout=180,
                    check=False,
                )
                code = completed.returncode
            except (OSError, subprocess.TimeoutExpired) as error:
                failure = type(error).__name__
        result = (
            classify_pytest(code, report)
            if name == "pytest"
            else {
                "status": "passed" if code == 0 else "harness_error" if code is None else "failed"
            }
        )
        results.append(
            {
                "name": name,
                "command": command,
                "started_utc": started,
                "exit_code": code,
                "exception_type": failure,
                **result,
            }
        )
        print(json.dumps({"check": name, "status": result["status"]}), flush=True)
    after = source_snapshot(root)
    payload = {
        "schema_version": 1,
        "evidence_kind": "offline_behavior",
        "python": platform.python_version(),
        "platform": platform.platform(),
        "source_stable": before == after,
        "checks": results,
        "result": "passed"
        if before == after and all(r["status"] == "passed" for r in results)
        else "not_passed",
        "scope": {
            "live_qq": False,
            "live_model": False,
            "production_changed": False,
            "whole_project_complete": False,
        },
        "live_tests": "explicitly_deselected_not_verified",
        "source_before": before,
        "source_after": after,
    }
    (attempt / "report.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return attempt / "report.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("evidence/validation"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    output = args.output if args.output.is_absolute() else root / args.output
    path = run_validation(root, output)
    result = json.loads(path.read_text(encoding="utf-8"))
    print(json.dumps({"report": str(path), "result": result["result"]}))
    return 0 if result["result"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
