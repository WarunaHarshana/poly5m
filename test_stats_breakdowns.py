"""Tests for /stats reporting breakdowns (by_exit_reason, by_entry_elapsed, by_strategy_leg)."""
from __future__ import annotations

from http_api import (
    build_stats,
    entry_elapsed_bucket,
    exit_reason_bucket,
    strategy_leg_bucket,
)


def test_exit_reason_bucket():
    assert exit_reason_bucket("settlement_won") == "settled_win"
    assert exit_reason_bucket("settlement_lost") == "settled_loss"
    assert exit_reason_bucket("take_profit") == "take_profit"
    assert exit_reason_bucket("trailing_profit (peak: 0.80, exit: 0.75, entry: 0.70)") == "trailing_profit"
    assert exit_reason_bucket("stop_loss") == "adverse_stop"
    assert exit_reason_bucket("urgent_stop_loss") == "adverse_stop"
    assert exit_reason_bucket("hard_flatten_expiry") == "hard_flatten_expiry"
    assert exit_reason_bucket("HARD_FLATTEN_EXPIRY") == "hard_flatten_expiry"
    assert exit_reason_bucket("failsafe_orderbook_flatten") == "failsafe"
    assert exit_reason_bucket("exit_before") == "exit_before"
    assert exit_reason_bucket(None) == "other"
    assert exit_reason_bucket("") == "other"
    assert exit_reason_bucket("something new") == "other"


def test_entry_elapsed_bucket():
    assert entry_elapsed_bucket(0) == "<45"
    assert entry_elapsed_bucket(44.9) == "<45"
    assert entry_elapsed_bucket(45) == "45-90"
    assert entry_elapsed_bucket(89.9) == "45-90"
    assert entry_elapsed_bucket(90) == "90-120"
    assert entry_elapsed_bucket(120) == "120-180"
    assert entry_elapsed_bucket(179.99) == "120-180"
    assert entry_elapsed_bucket(180) == "180+"
    assert entry_elapsed_bucket(299) == "180+"
    assert entry_elapsed_bucket("150") == "120-180"
    assert entry_elapsed_bucket(None) == "unknown"
    assert entry_elapsed_bucket("abc") == "unknown"
    assert entry_elapsed_bucket(float("nan")) == "unknown"
    assert entry_elapsed_bucket(-1) == "unknown"


def test_strategy_leg_bucket():
    assert strategy_leg_bucket("JEV_DYNAMIC_MOMENTUM") == "momentum"
    assert strategy_leg_bucket("JEV_DYNAMIC_MAKER") == "maker"
    assert strategy_leg_bucket("H-001_SNIPER") == "other"
    assert strategy_leg_bucket(None) == "other"


def _row(**kw):
    base = {"asset": "btc", "slot": "btc-updown-5m-1790180700", "cost_usd": 4.0, "fee": 0.1, "official": True}
    base.update(kw)
    return base


def test_build_stats_breakdowns():
    trades = [
        _row(won=True, pnl=0.8, exit_reason="take_profit", early_exit=True, entry_elapsed_s=100.0,
             strategy="JEV_DYNAMIC_MOMENTUM"),
        _row(won=True, pnl=0.5, exit_reason="trailing_profit (peak: 0.9, exit: 0.8, entry: 0.7)", early_exit=True,
             entry_elapsed_s=150.0, strategy="JEV_DYNAMIC_MOMENTUM"),
        _row(won=False, pnl=-3.82, exit_reason="hard_flatten_expiry", early_exit=True, entry_elapsed_s=195.0,
             strategy="JEV_DYNAMIC_MOMENTUM"),
        _row(won=False, pnl=-1.0, exit_reason="stop_loss", early_exit=True, entry_elapsed_s=30.0,
             strategy="JEV_DYNAMIC_MAKER"),
        # old record: no exit_reason, no entry_elapsed_s, no parsable entry time -> settled_* / unknown
        {"asset": "eth", "slot": "", "cost_usd": 3.0, "fee": 0.05, "won": True, "pnl": 1.0, "official": True},
        # unresolved rows are excluded from all breakdowns
        _row(won=None, pnl=None, exit_reason=None),
    ]
    stats = build_stats(trades, {"mode": "paper_live"})
    assert stats["trades"] == 5
    # existing keys untouched
    assert set(stats["by_asset"]) == {"btc", "eth"}

    ber = stats["by_exit_reason"]
    assert ber["take_profit"]["trades"] == 1 and ber["take_profit"]["wins"] == 1
    assert ber["trailing_profit"]["trades"] == 1
    assert ber["hard_flatten_expiry"]["pnl"] == -3.82
    assert ber["hard_flatten_expiry"]["win_rate"] == 0.0
    assert ber["adverse_stop"]["trades"] == 1
    assert ber["settled_win"]["trades"] == 1
    assert sum(b["trades"] for b in ber.values()) == 5
    for b in ber.values():
        assert set(b) == {"trades", "wins", "win_rate", "pnl", "cost", "fee", "total_cost"}

    bee = stats["by_entry_elapsed"]
    assert bee["90-120"]["trades"] == 1
    assert bee["120-180"]["trades"] == 1
    assert bee["180+"]["trades"] == 1
    assert bee["<45"]["trades"] == 1
    assert bee["unknown"]["trades"] == 1
    assert list(bee) == ["<45", "90-120", "120-180", "180+", "unknown"]
    assert abs(sum(b["pnl"] for b in bee.values()) - stats["pnl"]) < 0.02

    bsl = stats["by_strategy_leg"]
    assert bsl["momentum"]["trades"] == 3
    assert bsl["maker"]["trades"] == 1
    assert bsl["other"]["trades"] == 1
