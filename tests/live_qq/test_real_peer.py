"""Explicit opt-in only. Missing infrastructure is skipped, never reported as a pass."""

import json
import os
from pathlib import Path

import pytest

from tools.qq_live_probe import ProbeProfile, run_probe


@pytest.mark.qq_live
async def test_real_peer_roundtrip(record_property):
    path = os.environ.get("QQ_LIVE_PROFILE")
    if os.environ.get("QQ_ALLOW_LIVE") != "1" or not path:
        pytest.skip("not_run: QQ_ALLOW_LIVE=1 and QQ_LIVE_PROFILE are both required")
    profile = ProbeProfile.load(Path(path))
    if not profile.forward_ws_url:
        pytest.skip("not_run: second-account OneBot endpoint is not configured")
    result = await run_probe(profile, allow_live=True)
    record_property("peer_probe_report", json.dumps(result, ensure_ascii=False))
    if result["status"] in ("not_run", "inconclusive"):
        pytest.skip(f"{result['status']}: {result.get('reason', 'missing evidence')}")
    assert result["status"] == "pass", result
    assert result["scope"] == "peer_observed_platform_roundtrip"
    assert result["full_chain_verified"] is False
