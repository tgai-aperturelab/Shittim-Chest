"""Subprocess entrypoints and immutable evidence files; no live QQ access."""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tools import qq_candidate_probe, qq_live_probe

ROOT = Path(__file__).resolve().parents[2]
DRIVERS = ("qq_live_probe", "qq_candidate_probe")


def run_cli(driver: str, mode: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    command = [sys.executable]
    command += ["-m", "tools." + driver] if mode == "module" else ["tools/" + driver + ".py"]
    return subprocess.run(
        [*command, *arguments],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=15,
        check=False,
    )


@pytest.mark.parametrize("driver", DRIVERS)
@pytest.mark.parametrize("mode", ("module", "file"))
def test_entrypoint_without_pythonpath_returns_not_run(driver, mode):
    result = run_cli(driver, mode)
    assert not result.stderr
    report = json.loads(result.stdout)
    assert report["status"] == "not_run" and report["full_chain_verified"] is False
    assert report["reason"] == (
        "explicit_profile_and_opt_in_required"
        if driver == "qq_candidate_probe"
        else "explicit_allow_live_and_profile_required"
    )
    assert result.returncode == (2 if driver == "qq_candidate_probe" else 0)


@pytest.mark.parametrize("driver", DRIVERS)
def test_existing_report_is_not_overwritten_before_profile_or_connection(tmp_path, driver):
    output = tmp_path / "failure.json"
    previous = b'{"status":"fail","case":"prior evidence"}\n'
    output.write_bytes(previous)
    result = run_cli(
        driver,
        "file",
        "--allow-live",
        "--profile",
        str(tmp_path / "missing.json"),
        "--output",
        str(output),
    )
    assert result.returncode == 2 and not result.stderr
    report = json.loads(result.stdout)
    assert report["status"] == "not_run" and report["reason"] == "output_already_exists"
    assert report["cases"] == []
    assert output.read_bytes() == previous


@pytest.mark.parametrize("driver", DRIVERS)
def test_new_report_saves_not_run_and_matches_stdout(tmp_path, driver):
    output = tmp_path / "new-directory" / "report.json"
    result = run_cli(driver, "file", "--output", str(output))
    assert not result.stderr
    report = json.loads(result.stdout)
    assert report["status"] == "not_run" and not report["full_chain_verified"]
    assert json.loads(output.read_text(encoding="utf-8")) == report


@pytest.mark.parametrize("module", (qq_live_probe, qq_candidate_probe))
def test_output_creation_race_refuses_before_runner(tmp_path, monkeypatch, capsys, module):
    output = tmp_path / "raced-report.json"
    original = b"concurrent writer evidence\n"
    real_open = Path.open
    attempts = []

    def competing_open(path, mode="r", *args, **kwargs):
        if path == output and mode == "x":
            attempts.append(mode)
            with real_open(path, "wb") as competing:
                competing.write(original)
        return real_open(path, mode, *args, **kwargs)

    def forbidden_runner(*args, **kwargs):
        pytest.fail("Runner must not be invoked before exclusive output reservation succeeds")

    monkeypatch.setattr(Path, "open", competing_open)
    monkeypatch.setattr(module.asyncio, "run", forbidden_runner)
    monkeypatch.setattr(sys, "argv", [module.__name__, "--allow-live", "--output", str(output)])
    assert module.main() == 2
    report = json.loads(capsys.readouterr().out)
    assert report["reason"] == "output_already_exists" and attempts == ["x"]
    assert output.read_bytes() == original


def test_report_write_failure_keeps_complete_stdout(capsys):
    class BrokenFile(io.StringIO):
        def write(self, value):
            raise OSError("fixture disk full")

    report = {"status": "inconclusive", "cases": [{"case_id": "Q04", "reason": "fixture"}]}
    assert not qq_live_probe.emit_report(report, BrokenFile())
    retained = json.loads(capsys.readouterr().out)
    assert retained["status"] == report["status"] and retained["cases"] == report["cases"]
    assert retained["report_output"] == {"status": "failed", "failure_class": "OSError"}
    assert "fixture disk full" not in json.dumps(retained)
