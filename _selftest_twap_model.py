"""Unit tests for the TWAP Path-Accumulation Model (H-001).

Validates:
1. Closed-form cubic variance decay of the remaining TWAP integral (tau^3 / 3).
2. Required remaining average spot price S_req calculation under various realized paths.
3. TWAP Z-score acceleration as elapsed time t -> 300s.
4. Trapezoidal numerical integration of discrete tick series.
5. Gatekeeping on net edge, order book asks, and chop filters.
"""
from __future__ import annotations

import math
import sys
from pricing_model import TWAPPricingModel, MomentumSignal
from price_feed import ChainlinkDataStream


def test_cubic_variance_decay():
    model = TWAPPricingModel(min_sigma_usd=1.0)
    spot = 65000.0
    daily_vol = 0.0106
    var_sec = (daily_vol ** 2) / 86400.0

    # Test remaining standard deviation of TWAP at tau = 150s, 60s, 10s
    sigma_150 = model.estimate_sigma_twap_usd(spot, remaining_s=150.0, var_per_sec=var_sec)
    sigma_60 = model.estimate_sigma_twap_usd(spot, remaining_s=60.0, var_per_sec=var_sec)
    sigma_10 = model.estimate_sigma_twap_usd(spot, remaining_s=10.0, var_per_sec=var_sec)

    # Theoretical ratio: sqrt(60 / 150)
    expected_ratio = math.sqrt(60.0 / 150.0)
    actual_ratio = sigma_60 / sigma_150
    assert abs(actual_ratio - expected_ratio) < 1e-3, f"Expected ratio {expected_ratio:.4f}, got {actual_ratio:.4f}"

    # Theoretical ratio between 10s and 60s
    expected_ratio_2 = math.sqrt(10.0 / 60.0)
    actual_ratio_2 = sigma_10 / sigma_60
    assert abs(actual_ratio_2 - expected_ratio_2) < 1e-3, f"Expected ratio {expected_ratio_2:.4f}, got {actual_ratio_2:.4f}"

    print("[PASS] test_cubic_variance_decay")


def test_s_req_and_z_score_acceleration():
    model = TWAPPricingModel(alpha=1.75, min_sigma_usd=1.0)
    strike = 65000.0

    # Scenario: BTC drifts up to 65030.0 and holds there
    # At t = 60s (tau = 240s): realized TWAP = 65015.0, spot = 65030.0
    abs_z_60, signed_z_60, s_req_60, _ = model.calculate_twap_z_score(
        spot=65030.0, strike=strike, realized_twap=65015.0,
        elapsed_s=60.0, remaining_s=240.0, sigma_twap_usd=15.0
    )

    # At t = 180s (tau = 120s): realized TWAP = 65025.0, spot = 65030.0
    # Realized area: 180 * 65025 = 11,704,500. Target: 300 * 65000 = 19,500,000.
    # Remaining needed: (19,500,000 - 11,704,500) / 120 = 7,795,500 / 120 = 64962.5
    abs_z_180, signed_z_180, s_req_180, _ = model.calculate_twap_z_score(
        spot=65030.0, strike=strike, realized_twap=65025.0,
        elapsed_s=180.0, remaining_s=120.0, sigma_twap_usd=10.0
    )

    # At t = 240s (tau = 60s): realized TWAP = 65028.0, spot = 65030.0
    # Remaining needed: (19,500,000 - 240 * 65028) / 60 = (19,500,000 - 15,606,720) / 60 = 64888.0
    abs_z_240, signed_z_240, s_req_240, _ = model.calculate_twap_z_score(
        spot=65030.0, strike=strike, realized_twap=65028.0,
        elapsed_s=240.0, remaining_s=60.0, sigma_twap_usd=7.0
    )

    # S_req should decrease further below strike as time passes with spot > strike
    assert s_req_60 > s_req_180 > s_req_240, f"S_req not decreasing properly: {s_req_60}, {s_req_180}, {s_req_240}"
    # Z-score must accelerate upward as outcome locks in
    assert abs_z_240 > abs_z_180 > abs_z_60, f"Z-score not accelerating: {abs_z_60:.2f}, {abs_z_180:.2f}, {abs_z_240:.2f}"

    print("[PASS] test_s_req_and_z_score_acceleration")


def test_trapezoidal_twap_integration():
    stream = ChainlinkDataStream(asset="btc")
    
    # Simulate a 100-second tick sequence
    start_ts = 1000.0
    stream._record(1000.0, 65000.0)
    stream._record(1020.0, 65020.0)
    stream._record(1050.0, 65040.0)
    stream._record(1100.0, 65060.0)

    # Analytical trapezoidal areas:
    # 1000->1020: 20 * (65000 + 65020) / 2 = 20 * 65010 = 1,300,200
    # 1020->1050: 30 * (65020 + 65040) / 2 = 30 * 65030 = 1,950,900
    # 1050->1100: 50 * (65040 + 65060) / 2 = 50 * 65050 = 3,252,500
    # Total area: 6,503,600 / 100 = 65036.0
    twap, elapsed = stream.compute_realized_twap(start_ts=1000.0, end_ts=1100.0)
    assert abs(twap - 65036.0) < 1e-4, f"Expected TWAP 65036.0, got {twap}"
    assert abs(elapsed - 100.0) < 1e-4, f"Expected elapsed 100.0, got {elapsed}"

    print("[PASS] test_trapezoidal_twap_integration")


def test_twap_evaluation_and_gating():
    model = TWAPPricingModel(min_z=1.8)
    
    # High-conviction UP signal
    sig_up = model.evaluate(
        spot=65040.0,
        strike=65000.0,
        remaining_s=90.0,
        elapsed_s=210.0,
        realized_twap=65030.0,
        ask=0.75,
        chop_crosses=0,
        strike_src="chainlink-data-stream",
        require_verified_strike=True,
    )
    assert sig_up.side == "UP"
    assert sig_up.z_score >= 1.8
    assert sig_up.model_prob > 0.90
    assert sig_up.edge > 0.10
    assert sig_up.passed_gates is True

    # Flat / low-conviction signal rejected by Z-score gate
    sig_flat = model.evaluate(
        spot=65001.0,
        strike=65000.0,
        remaining_s=200.0,
        elapsed_s=100.0,
        realized_twap=65000.5,
        ask=0.52,
        chop_crosses=1,
        strike_src="chainlink-data-stream",
        require_verified_strike=True,
    )
    assert sig_flat.passed_gates is False
    assert "twap z-score" in sig_flat.skip_reason

    # Unverified strike source rejected by H-005 gate
    sig_unverified = model.evaluate(
        spot=65040.0,
        strike=65000.0,
        remaining_s=90.0,
        elapsed_s=210.0,
        realized_twap=65030.0,
        ask=0.75,
        chop_crosses=0,
        strike_src="proxy_coinbase",
        require_verified_strike=True,
    )
    assert sig_unverified.passed_gates is False
    assert "unverified strike" in sig_unverified.skip_reason

    print("[PASS] test_twap_evaluation_and_gating")


def main() -> int:
    print("=== RUNNING TWAP PATH-ACCUMULATION UNIT TESTS (H-001) ===")
    test_cubic_variance_decay()
    test_s_req_and_z_score_acceleration()
    test_trapezoidal_twap_integration()
    test_twap_evaluation_and_gating()
    print("=== ALL TWAP PATH-ACCUMULATION UNIT TESTS PASSED ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
