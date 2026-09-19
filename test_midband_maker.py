"""Tests for H-006 mid-band maker gates."""
from __future__ import annotations

import os
import sys

# Default strategy must be midband before config import
os.environ.pop("STRATEGY", None)

from twap_engine import TWAPEngine
from twap_midband_patch import patch_twap_engine

patch_twap_engine(TWAPEngine)
import config


def test_default_strategy_is_midband():
    assert config.STRATEGY in (
        "MIDBAND_MAKER",
        "MIDBAND",
        "H-006_MIDBAND_MAKER",
        "H-006",
    ), config.STRATEGY


def test_pass_near_mid_048_052():
    eng = TWAPEngine()
    sig = eng.evaluate_midband(
        side="UP",
        remaining_s=150.0,
        best_bid=0.48,
        best_ask=0.52,
        midband_lo=0.40,
        midband_hi=0.60,
        half_spread=0.02,
        min_seconds_left=75.0,
        max_seconds_left=270.0,
    )
    assert sig.passed_gates, sig.skip_reason
    assert sig.side == "UP"
    assert 0.40 <= sig.target_bid <= 0.60
    assert sig.target_bid < 0.52 - 0.009
    assert sig.edge_net > 0


def test_reject_late_favorite_090_092():
    eng = TWAPEngine()
    sig = eng.evaluate_midband(
        side="UP",
        remaining_s=150.0,
        best_bid=0.90,
        best_ask=0.92,
        midband_lo=0.40,
        midband_hi=0.60,
        half_spread=0.02,
    )
    assert not sig.passed_gates
    assert "band" in sig.skip_reason or "mid" in sig.skip_reason


def test_reject_remaining_too_small():
    eng = TWAPEngine()
    sig = eng.evaluate_midband(
        side="DOWN",
        remaining_s=30.0,
        best_bid=0.47,
        best_ask=0.51,
        midband_lo=0.40,
        midband_hi=0.60,
        half_spread=0.02,
        min_seconds_left=75.0,
        max_seconds_left=270.0,
    )
    assert not sig.passed_gates
    assert "remaining" in sig.skip_reason


def test_no_z_score_gate():
    """Midband must not require Z-score (flat spot still OK if mid in band)."""
    eng = TWAPEngine(min_z=9.0)  # would reject H-004
    sig = eng.evaluate_midband(
        side="UP",
        remaining_s=120.0,
        best_bid=0.49,
        best_ask=0.51,
    )
    assert sig.passed_gates, sig.skip_reason
    assert sig.z_score == 0.0


if __name__ == "__main__":
    test_default_strategy_is_midband()
    test_pass_near_mid_048_052()
    test_reject_late_favorite_090_092()
    test_reject_remaining_too_small()
    test_no_z_score_gate()
    print("midband maker tests OK")
    sys.exit(0)
