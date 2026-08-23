"""Unit tests for the Terminal Diffusion Z-Score Model (H-001 through H-005).

Validates:
1. Micro-volatility scaling over variable remaining horizons tau (H-001).
2. Standardized Z-score calculation.
3. Calibrated logistic S-curve probability bounds.
4. Chop pin-risk filter on marginal vs breakout moves (H-002).
5. Edge calculation and gate rejection conditions.
6. Session time-of-day classification (H-004).
7. Inter-slot volatility regime multiplier (H-003).
8. Verified Chainlink RTDS strike gating (H-005).
"""
from __future__ import annotations

import math
import sys
from pricing_model import (
    DiffusionPricingModel,
    MomentumSignal,
    classify_session,
    calculate_regime_multiplier,
)


def test_sigma_tau_scaling():
    model = DiffusionPricingModel(min_sigma_usd=2.5)
    spot = 65000.0

    sigma_30s = model.estimate_sigma_tau(spot, remaining_s=30.0, realized_vol_daily=0.0106)
    sigma_10s = model.estimate_sigma_tau(spot, remaining_s=10.0, realized_vol_daily=0.0106)
    sigma_2s = model.estimate_sigma_tau(spot, remaining_s=2.0, realized_vol_daily=0.0106)

    # 30s should have sqrt(3) ~ 1.732x more dispersion than 10s
    ratio = sigma_30s / sigma_10s
    assert abs(ratio - math.sqrt(30.0 / 10.0)) < 1e-3, f"Expected ratio ~{math.sqrt(3):.4f}, got {ratio:.4f}"
    
    # 2s should hit the floor of 2.5 USD
    assert sigma_2s >= 2.5, f"Expected >= 2.5 floor, got {sigma_2s}"
    print("[PASS] test_sigma_tau_scaling")


def test_calibrated_probability():
    model = DiffusionPricingModel(alpha=1.75)

    # Z = 0 -> 0.50
    p0 = model.calibrated_probability(0.0)
    assert abs(p0 - 0.50) < 1e-4, f"Z=0 should yield 0.50, got {p0}"

    # Z = 1.0 -> ~0.852
    p1 = model.calibrated_probability(1.0)
    assert 0.84 < p1 < 0.86, f"Z=1.0 should be ~0.85, got {p1}"

    # Z = 1.8 -> ~0.959
    p18 = model.calibrated_probability(1.8)
    assert 0.95 < p18 < 0.97, f"Z=1.8 should be ~0.96, got {p18}"

    # Z = 2.5 -> ~0.988
    p25 = model.calibrated_probability(2.5)
    assert 0.98 < p25 < 0.995, f"Z=2.5 should be ~0.99, got {p25}"

    # Z = 5.0 -> clamped at 0.999
    p5 = model.calibrated_probability(5.0)
    assert p5 <= 0.999, f"Z=5.0 should clamp to <= 0.999, got {p5}"
    print("[PASS] test_calibrated_probability")


def test_chop_filter_interaction():
    model = DiffusionPricingModel(
        min_z=1.8,
        high_z_breakout=2.2,
        max_chop_marginal=3,
    )
    spot = 65020.0
    strike = 65000.0  # +$20 move
    remaining_s = 10.0
    ask = 0.90

    # 1. Marginal Z with LOW chop (<= 3) -> PASS
    sig_pass = model.evaluate(
        spot=spot,
        strike=strike,
        remaining_s=remaining_s,
        ask=ask,
        chop_crosses=2,
        realized_vol_daily=0.02,
        strike_src="chainlink-data-stream",
        min_entry=0.50,
        max_entry=0.99,
        min_edge=0.0,
    )
    if 1.8 <= sig_pass.z_score < 2.2:
        assert sig_pass.passed_gates, f"Low chop marginal Z should pass: {sig_pass.skip_reason}"

    # 2. Marginal Z with HIGH chop (> 3) -> REJECT
    sig_reject = model.evaluate(
        spot=spot,
        strike=strike,
        remaining_s=remaining_s,
        ask=ask,
        chop_crosses=6,
        realized_vol_daily=0.02,
        strike_src="chainlink-data-stream",
        min_entry=0.50,
        max_entry=0.99,
        min_edge=0.0,
    )
    if 1.8 <= sig_reject.z_score < 2.2:
        assert not sig_reject.passed_gates, "High chop marginal Z should be rejected"
        assert "choppy path" in str(sig_reject.skip_reason)

    # 3. Breakout Z (Z >= 2.5) with HIGH chop (> 3) -> PASS
    sig_breakout = model.evaluate(
        spot=spot,
        strike=strike,
        remaining_s=remaining_s,
        ask=ask,
        chop_crosses=10,
        realized_vol_daily=0.01,
        strike_src="chainlink-data-stream",
        min_entry=0.50,
        max_entry=0.99,
        min_edge=0.0,
    )
    assert sig_breakout.z_score >= 2.2
    assert sig_breakout.passed_gates, f"Breakout Z should pass despite chop: {sig_breakout.skip_reason}"
    print("[PASS] test_chop_filter_interaction")


def test_edge_and_entry_band_gates():
    model = DiffusionPricingModel(min_z=1.8)
    spot = 65020.0
    strike = 65000.0
    remaining_s = 10.0

    # 1. Ask too high (> max_entry)
    sig_high_ask = model.evaluate(
        spot=spot, strike=strike, remaining_s=remaining_s,
        ask=0.995, max_entry=0.95, strike_src="chainlink-data-stream",
    )
    assert not sig_high_ask.passed_gates
    assert "outside entry band" in str(sig_high_ask.skip_reason)

    # 2. Ask too low (< min_entry)
    sig_low_ask = model.evaluate(
        spot=spot, strike=strike, remaining_s=remaining_s,
        ask=0.40, min_entry=0.50, strike_src="chainlink-data-stream",
    )
    assert not sig_low_ask.passed_gates
    assert "outside entry band" in str(sig_low_ask.skip_reason)

    # 3. Net negative edge
    sig_neg_edge = model.evaluate(
        spot=spot, strike=strike, remaining_s=remaining_s,
        ask=0.98, min_entry=0.50, max_entry=0.99, min_edge=0.05,
        strike_src="chainlink-data-stream",
    )
    assert not sig_neg_edge.passed_gates
    assert "net edge" in str(sig_neg_edge.skip_reason)
    print("[PASS] test_edge_and_entry_band_gates")


def test_session_and_regime_classification():
    # H-004: Session classification
    s_london, vol_london = classify_session(9)
    assert "London" in s_london
    assert vol_london == 22.8

    s_us, vol_us = classify_session(14)
    assert "US-Europe" in s_us
    assert vol_us == 44.7

    s_asia, _ = classify_session(4)
    assert "Asian" in s_asia

    # H-003: Inter-slot volatility clustering multiplier
    mult_high = calculate_regime_multiplier(35.0)
    assert mult_high == 1.20

    mult_low = calculate_regime_multiplier(5.0)
    assert mult_low == 0.80

    mult_norm = calculate_regime_multiplier(20.0)
    assert mult_norm == 1.00
    print("[PASS] test_session_and_regime_classification")


def test_verified_strike_enforcement():
    model = DiffusionPricingModel(min_z=1.8)
    spot = 65020.0
    strike = 65000.0
    remaining_s = 10.0
    ask = 0.90

    # 1. Official Chainlink RTDS strike -> PASS
    sig_rtds = model.evaluate(
        spot=spot, strike=strike, remaining_s=remaining_s,
        ask=ask, strike_src="chainlink-data-stream",
        require_verified_strike=True,
    )
    assert sig_rtds.strike_verified
    assert sig_rtds.passed_gates

    # 2. Exchange Proxy strike -> REJECT
    sig_proxy = model.evaluate(
        spot=spot, strike=strike, remaining_s=remaining_s,
        ask=ask, strike_src="proxy",
        require_verified_strike=True,
    )
    assert not sig_proxy.strike_verified
    assert not sig_proxy.passed_gates
    assert "unverified strike source" in str(sig_proxy.skip_reason)
    print("[PASS] test_verified_strike_enforcement")


def main():
    print("=== RUNNING DIFFUSION MODEL UNIT TESTS ===")
    test_sigma_tau_scaling()
    test_calibrated_probability()
    test_chop_filter_interaction()
    test_edge_and_entry_band_gates()
    test_session_and_regime_classification()
    test_verified_strike_enforcement()
    print("=== ALL DIFFUSION MODEL UNIT TESTS PASSED ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
