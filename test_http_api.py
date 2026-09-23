"""Tests for the public JSON status API."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from http_api import build_stats, load_trades, public_trade


def test_public_trade_and_stats(tmp_path: Path):
    trades = [
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "asset": "btc",
            "slot": "btc-updown-5m-1790180700",
            "side": "DOWN",
            "price": 0.83,
            "shares": 9.0,
            "cost_usd": 7.5,
            "fee": 0.12,
            "total_cost": 7.62,
            "won": True,
            "pnl": 1.45,
            "outcome": "DOWN",
            "strategy": "H-001_SNIPER",
            "entry_elapsed_s": 45.0,
            "entry_window_time": "t=45s (00:45)",
            "exit_elapsed_s": 300.0,
            "exit_window_time": "t=300s (05:00 - expiry)",
            "exit_price": 1.0,
            "exit_reason": "settlement_won",
        },
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "asset": "eth",
            "slot": "eth-updown-5m-1790180700",
            "side": "UP",
            "price": 0.70,
            "shares": 5.0,
            "cost_usd": 3.5,
            "fee": 0.08,
            "exit_fee": 0.02,
            "won": False,
            "pnl": -1.25,
            "outcome": "EARLY_EXIT_stop_loss",
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "early_exit": True,
            "exit_price": 0.30,
            "exit_reason": "stop_loss",
            "entry_elapsed_s": 65.0,
            "entry_window_time": "t=65s (01:05)",
            "exit_elapsed_s": 240.0,
            "exit_window_time": "t=240s (04:00)",
        },
    ]
    p = tmp_path / "trades.json"
    p.write_text(json.dumps(trades), encoding="utf-8")
    loaded = load_trades(p)
    assert len(loaded) == 2
    row0 = public_trade(loaded[0])
    assert row0["slug"] == "btc-updown-5m-1790180700"
    assert row0["polymarket_url"].endswith("btc-updown-5m-1790180700")
    assert row0["cost"] == 7.5
    assert row0["fee"] == 0.12
    assert row0["total_cost"] == 7.62
    assert row0["exit_price"] == 1.0
    assert row0["exit_reason"] == "settlement_won"
    assert row0["entry_window_time"] == "t=45s (00:45)"
    assert row0["exit_window_time"] == "t=300s (05:00 - expiry)"

    row1 = public_trade(loaded[1])
    assert row1["slug"] == "eth-updown-5m-1790180700"
    assert row1["cost"] == 3.5
    assert row1["fee"] == 0.10
    assert row1["total_cost"] == 3.60
    assert row1["exit_price"] == 0.30
    assert row1["exit_reason"] == "stop_loss"
    assert row1["entry_window_time"] == "t=65s (01:05)"
    assert row1["exit_window_time"] == "t=240s (04:00)"

    from http_api import is_official_trade
    loaded[0]["settlement_source"] = "polymarket_outcome_prices"
    loaded[1]["settlement_source"] = "early_exit_stop_loss"
    assert is_official_trade(loaded[0]) is True
    stats = build_stats(loaded, {"mode": "paper_live", "bankroll": 48.0})
    assert stats["trades"] == 2
    assert stats["wins"] == 1
    assert stats["fee"] == 0.22
    assert stats["total_cost"] == 11.22
    assert abs(stats["pnl"] - (1.45 - 1.25)) < 1e-9
    assert stats["by_asset"]["btc"]["wins"] == 1
    assert stats["by_asset"]["eth"]["wins"] == 0
    assert stats["bankroll"] == 48.0
