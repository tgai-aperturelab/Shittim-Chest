"""C19: an oracle must reject bad behavior and distinguish a broken test harness."""

import pytest

from tools.run_validation import classify_pytest, source_snapshot


@pytest.mark.parametrize(
    ("code", "xml", "status"),
    [
        (
            0,
            '<testsuites><testsuite tests="2" failures="0" errors="0" skipped="0"/></testsuites>',
            "passed",
        ),
        (
            1,
            '<testsuites><testsuite tests="2" failures="1" errors="0" skipped="0"/></testsuites>',
            "failed",
        ),
        (
            2,
            '<testsuites><testsuite tests="1" failures="0" errors="1" skipped="0"/></testsuites>',
            "harness_error",
        ),
        (
            0,
            '<testsuites><testsuite tests="2" failures="0" errors="0" skipped="2"/></testsuites>',
            "harness_error",
        ),
        (0, '<testsuites><testsuite tests="0"/></testsuites>', "harness_error"),
        (None, '<testsuites><testsuite tests="1"/></testsuites>', "harness_error"),
        (0, "<broken", "harness_error"),
    ],
)
def test_calibrated_status(code, xml, status, tmp_path):
    path = tmp_path / "report.xml"
    path.write_text(xml, encoding="utf-8")
    assert classify_pytest(code, path)["status"] == status


def test_missing_report_is_not_a_behavior_pass(tmp_path):
    assert classify_pytest(0, tmp_path / "missing.xml")["status"] == "harness_error"


def test_snapshot_detects_changed_source_and_ignores_private_env(tmp_path):
    (tmp_path / "src").mkdir()
    file = tmp_path / "src" / "worker.py"
    file.write_text("x = 1\n", encoding="utf-8")
    (tmp_path / ".env").write_text("FAKE_KEY=not-a-real-secret", encoding="utf-8")
    before = source_snapshot(tmp_path)
    file.write_text("x = 2\n", encoding="utf-8")
    assert before != source_snapshot(tmp_path)
    assert set(before) == {"src/worker.py"}
