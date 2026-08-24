"""Unit and Integration Tests for High-Conviction TWAP Bot (H-001)."""
from __future__ import annotations

import math
import sys
import time

from twap_engine import TWAPEngine, TWAPSignal
from feeds import ChainlinkRTDSStream
from risk_manager import RiskManager


def test_twap_cubic_variance():
    engine = TWAPEngine(min_sigma_usd=1.0)
    spot = 65000.0
    daily_vol = 0.0106
    var_sec = (daily_vol ** 2) / 86400.0

    sigma_150 = engine.estimate_sigma_twap_usd(spot, remaining_s=150.0, var_per_sec=var_sec)
    sigma_60 = engine.estimate_sigma_twap_usd(spot, remaining_s=60.0, var_per_sec=var_sec)
    sigma_10 = engine.estimate_sigma_twap_usd(spot, remaining_s=10.0, var_per_sec=var_sec)

    # Theoretical ratio: sqrt(60 / 150)
    assert abs((sigma_60 / sigma_150) - math.sqrt(60.0 / 150.0)) < 1e-3
    assert abs((sigma_10 / sigma_60) - math.sqrt(10.0 / 60.0)) < 1e-3
    print("[PASS] test_twap_cubic_variance")


def test_s_req_and_z_acceleration():
    engine = TWAPEngine(alpha=1.75, min_sigma_usd=1.0)
    strike = 65000.0

    # BTC drifts up to 65030.0 and stays there
    # At t=60s (tau=240s): realized TWAP=65015.0
    z_60, _, s_req_60 = engine.compute_s_req_and_z(
        spot=65030.0, strike=strike, realized_twap=65015.0,
        elapsed_s=60.0, remaining_s=240.0, sigma_twap_usd=15.0
    )

    # At t=180s (tau=120s): realized TWAP=65025.0
    z_180, _, s_req_180 = engine.compute_s_req_and_z(
        spot=65030.0, strike=strike, realized_twap=65025.0,
        elapsed_s=180.0, remaining_s=120.0, sigma_twap_usd=10.0
    )

    # At t=240s (tau=60s): realized TWAP=65028.0
    z_240, _, s_req_240 = engine.compute_s_req_and_z(
        spot=65030.0, strike=strike, realized_twap=65028.0,
        elapsed_s=240.0, remaining_s=60.0, sigma_twap_usd=7.0
    )

    # Required price must drop further below strike as time passes with spot > strike
    assert s_req_60 > s_req_180 > s_req_240
    # Z-score must accelerate non-linearly
    assert z_240 > z_180 > z_60
    print("[PASS] test_s_req_and_z_acceleration")


def test_gatekeeper_conditions():
    engine = TWAPEngine(min_z=1.8, min_edge=0.03, min_entry=0.50, max_entry=0.95)

    # Valid high-conviction UP signal
    sig_valid = engine.evaluate(
        spot=65040.0, strike=65000.0, remaining_s=90.0, elapsed_s=210.0,
        realized_twap=65030.0, ask=0.75, chop_crosses=0
    )
    assert sig_valid.passed_gates is True
    assert sig_valid.side == "UP"
    assert sig_valid.z_score >= 1.8
    assert sig_valid.edge_net > 0.03

    # Flat market (rejected by Z-score)
    sig_flat = engine.evaluate(
        spot=65001.0, strike=65000.0, remaining_s=180.0, elapsed_s=120.0,
        realized_twap=65000.5, ask=0.52, chop_crosses=1
    )
    assert sig_flat.passed_gates is False
    assert "Z-score" in sig_flat.skip_reason

    # High chop market (rejected by chop filter)
    sig_chop = engine.evaluate(
        spot=65040.0, strike=65000.0, remaining_s=90.0, elapsed_s=210.0,
        realized_twap=65030.0, ask=0.75, chop_crosses=5
    )
    assert sig_chop.passed_gates is False
    assert "chop" in sig_chop.skip_reason

    print("[PASS] test_gatekeeper_conditions")


def test_trapezoidal_integration():
    stream = ChainlinkRTDSStream(asset="btc")
    stream._record(1000.0, 65000.0)
    stream._record(1020.0, 65020.0)
    stream._record(1050.0, 65040.0)
    stream._record(1100.0, 65060.0)

    twap, elapsed = stream.compute_realized_twap(start_ts=1000.0, end_ts=1100.0)
    assert abs(twap - 65036.0) < 1e-4
    assert abs(elapsed - 100.0) < 1e-4
    print("[PASS] test_trapezoidal_integration")


def test_risk_manager():
    risk = RiskManager()
    risk.bankroll = 50.0
    risk.daily_pnl = 0.0
    risk.consecutive_losses = 0

    can_trade, _ = risk.can_trade()
    assert can_trade is True

    # Record 3 losses -> cooldown triggers
    risk.record_trade_result(-5.0)
    risk.record_trade_result(-5.0)
    assert risk.can_trade()[0] is False  # Hit daily loss limit of -$10
    print("[PASS] test_risk_manager")


def main():
    print("=== RUNNING TWAP BOT TEST SUITE ===")
    test_twap_cubic_variance()
    test_s_req_and_z_acceleration()
    test_gatekeeper_conditions()
    test_trapezoidal_integration()
    test_risk_manager()
    print("=== ALL TESTS PASSED SUCCESSFULLY ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
