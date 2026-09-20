"""Tests for mid-band maker gates (H-001 retired; H-006 directionally-informed maker)."""
from __future__ import annotations

import config
from twap_engine import TWAPEngine


def _engine() -> TWAPEngine:
    try:
        return TWAPEngine(
            min_z=1.65,
            alpha=1.75,
            min_sigma_usd=2.0,
            max_chop_crosses=2,
            min_edge=0.035,
            min_buy_price=0.2,
            max_buy_price=0.92,
            min_maker_edge=0.05,
            min_maker_bid=0.55,
            max_maker_bid=0.90,
        )
    except TypeError:
        return TWAPEngine()


def test_default_strategy_is_midband():
    assert config.STRATEGY in {"MIDBAND_MAKER", "MIDBAND", "H-006_MIDBAND_MAKER", "H-006"}


def test_midband_passes_with_directional_z():
    """When spot is trending above strike (UP) with sufficient Z, UP quotes pass."""
    eng = _engine()
    # spot=101.5, strike=100.0, realized_twap=100.2, elapsed=120s, remaining=120s
    sig = eng.evaluate_midband(
        remaining_s=120.0,
        elapsed_s=120.0,
        spot=101.50,
        strike=100.00,
        realized_twap=100.20,
        best_bid=0.48,
        best_ask=0.52,
        midband_lo=0.40,
        midband_hi=0.60,
        half_spread=0.04,
        min_seconds_left=75.0,
        max_seconds_left=270.0,
        side="UP",
        min_z=0.50,
    )
    assert sig.passed_gates, sig.skip_reason
    assert 0.40 <= sig.target_bid <= 0.60
    assert sig.z_score >= 0.50
    assert sig.side == "UP"


def test_midband_rejects_counter_trend_side():
    """When spot favors UP, requesting DOWN should be rejected by directional gate."""
    eng = _engine()
    sig = eng.evaluate_midband(
        remaining_s=120.0,
        elapsed_s=120.0,
        spot=100.50,
        strike=100.00,
        realized_twap=100.20,
        best_bid=0.48,
        best_ask=0.52,
        midband_lo=0.40,
        midband_hi=0.60,
        half_spread=0.04,
        min_seconds_left=75.0,
        max_seconds_left=270.0,
        side="DOWN",
        min_z=0.50,
    )
    assert not sig.passed_gates
    assert "favored" in sig.skip_reason


def test_midband_rejects_flat_z():
    """When market is dead flat (Z < min_z), coin-flip should be rejected."""
    eng = _engine()
    sig = eng.evaluate_midband(
        remaining_s=120.0,
        elapsed_s=120.0,
        spot=100.00,
        strike=100.00,
        realized_twap=100.00,
        best_bid=0.48,
        best_ask=0.52,
        side="UP",
        min_z=0.50,
    )
    assert not sig.passed_gates
    assert "Z-score" in sig.skip_reason


def test_midband_rejects_late_favorite_book():
    eng = _engine()
    sig = eng.evaluate_midband(
        remaining_s=120.0,
        elapsed_s=120.0,
        spot=100.50,
        strike=100.00,
        realized_twap=100.20,
        best_bid=0.90,
        best_ask=0.92,
        midband_lo=0.40,
        midband_hi=0.60,
        half_spread=0.04,
        side="UP",
        min_z=0.50,
    )
    assert not sig.passed_gates
    assert "outside" in (sig.skip_reason or "").lower() or "mid" in (sig.skip_reason or "").lower()


def test_midband_rejects_too_late():
    eng = _engine()
    sig = eng.evaluate_midband(
        remaining_s=30.0,
        elapsed_s=270.0,
        spot=100.50,
        strike=100.00,
        realized_twap=100.20,
        best_bid=0.48,
        best_ask=0.52,
        min_seconds_left=75.0,
        side="UP",
        min_z=0.50,
    )
    assert not sig.passed_gates
    assert "too late" in sig.skip_reason
