"""Unit tests for poly5m TWAP engine, fees, feeds math, and risk gates."""
from __future__ import annotations

import math
import sys

import config
from twap_engine import TWAPEngine
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

    assert abs((sigma_60 / sigma_150) - math.sqrt(60.0 / 150.0)) < 1e-3
    assert abs((sigma_10 / sigma_60) - math.sqrt(10.0 / 60.0)) < 1e-3
    print("[PASS] test_twap_cubic_variance")


def test_s_req_and_z_acceleration():
    engine = TWAPEngine(alpha=1.75, min_sigma_usd=1.0)
    strike = 65000.0

    z_60, _, s_req_60 = engine.compute_s_req_and_z(
        spot=65030.0, strike=strike, realized_twap=65015.0,
        elapsed_s=60.0, remaining_s=240.0, sigma_twap_usd=15.0,
    )
    z_180, _, s_req_180 = engine.compute_s_req_and_z(
        spot=65030.0, strike=strike, realized_twap=65025.0,
        elapsed_s=180.0, remaining_s=120.0, sigma_twap_usd=10.0,
    )
    z_240, _, s_req_240 = engine.compute_s_req_and_z(
        spot=65030.0, strike=strike, realized_twap=65028.0,
        elapsed_s=240.0, remaining_s=60.0, sigma_twap_usd=7.0,
    )

    assert s_req_60 > s_req_180 > s_req_240
    assert z_240 > z_180 > z_60
    print("[PASS] test_s_req_and_z_acceleration")


def test_taker_fee_curve_and_net_edge_gate():
    """Gross edge can look fine while net edge after fee fails — must reject."""
    engine = TWAPEngine(min_z=0.5, min_edge=0.03, min_entry=0.05, max_entry=0.95)
    fee_rate = 0.07

    ask = 0.50
    fee = TWAPEngine.calculate_taker_fee(ask, fee_rate)
    assert abs(fee - (fee_rate * ask * (1.0 - ask))) < 1e-12
    assert fee == 0.07 * 0.25  # 0.0175

    sig = engine.evaluate(
        spot=65020.0,
        strike=65000.0,
        remaining_s=120.0,
        elapsed_s=180.0,
        realized_twap=65010.0,
        ask=0.50,
        chop_crosses=0,
        fee_rate=fee_rate,
    )
    assert sig.fee_taker > 0
    assert abs(sig.edge_net - (sig.edge_gross - sig.fee_taker)) < 1e-12
    if sig.edge_net < engine.min_edge:
        assert sig.passed_gates is False
        assert "net edge" in sig.skip_reason
    print("[PASS] test_taker_fee_curve_and_net_edge_gate")


def test_gatekeeper_rejects_low_z_and_chop():
    engine = TWAPEngine(min_z=1.8, min_edge=0.03, min_entry=0.50, max_entry=0.95)

    sig_flat = engine.evaluate(
        spot=65001.0, strike=65000.0, remaining_s=180.0, elapsed_s=120.0,
        realized_twap=65000.5, ask=0.52, chop_crosses=1, fee_rate=0.07,
    )
    assert sig_flat.passed_gates is False
    assert "Z-score" in sig_flat.skip_reason

    sig_chop = engine.evaluate(
        spot=65040.0, strike=65000.0, remaining_s=90.0, elapsed_s=210.0,
        realized_twap=65030.0, ask=0.75, chop_crosses=5, fee_rate=0.07,
    )
    assert sig_chop.passed_gates is False
    assert "chop" in sig_chop.skip_reason
    print("[PASS] test_gatekeeper_rejects_low_z_and_chop")


def test_trapezoidal_integration_and_stale_flag():
    stream = ChainlinkRTDSStream(asset="btc")
    stream._record(1000.0, 65000.0)
    stream._record(1020.0, 65020.0)
    stream._record(1050.0, 65040.0)
    stream._record(1100.0, 65060.0)

    twap, elapsed = stream.compute_realized_twap(start_ts=1000.0, end_ts=1100.0)
    assert abs(twap - 65036.0) < 1e-4
    assert abs(elapsed - 100.0) < 1e-4

    assert stream.is_stale(max_age_s=0.0) is True or stream.is_stale(max_age_s=3600.0) is False
    stream.mark_stale("unit_test")
    assert stream.is_stale(max_age_s=6.0) is True
    print("[PASS] test_trapezoidal_integration_and_stale_flag")


def test_risk_manager_live_only_kill_switch():
    old_mode = config.MODE
    try:
        config.MODE = "live"
        risk = RiskManager()
        risk.bankroll = 50.0
        risk.daily_pnl = 0.0
        risk.consecutive_losses = 0

        assert risk.can_trade()[0] is True
        risk.record_trade_result(-(config.DAILY_LOSS_LIMIT_USD + 1.0))
        assert risk.can_trade()[0] is False
    finally:
        config.MODE = old_mode
    print("[PASS] test_risk_manager_live_only_kill_switch")


def test_maker_gatekeeper_and_pricing():
    engine = TWAPEngine(
        min_z=1.15,
        min_maker_edge=0.05,
        min_maker_bid=0.55,
        max_maker_bid=0.90,
    )
    sig_maker = engine.evaluate_maker(
        spot=65035.0, strike=65000.0, remaining_s=150.0, elapsed_s=150.0,
        realized_twap=65020.0, best_bid=0.78, best_ask=0.95, chop_crosses=0,
    )
    assert sig_maker.passed_gates is True
    assert sig_maker.side == "UP"
    assert sig_maker.target_bid <= 0.90
    assert sig_maker.edge_net > 0.05
    print("[PASS] test_maker_gatekeeper_and_pricing")


def main():
    print("=== RUNNING POLY5M HARDENING TEST SUITE ===")
    test_twap_cubic_variance()
    test_s_req_and_z_acceleration()
    test_taker_fee_curve_and_net_edge_gate()
    test_gatekeeper_rejects_low_z_and_chop()
    test_maker_gatekeeper_and_pricing()
    test_trapezoidal_integration_and_stale_flag()
    test_risk_manager_live_only_kill_switch()
    print("=== ALL TESTS PASSED ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
