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
            "slot": "btc-updown-5m-1",
            "side": "DOWN",
            "price": 0.83,
            "shares": 9.0,
            "cost_usd": 7.5,
            "won": True,
            "pnl": 1.45,
            "outcome": "DOWN",
            "strategy": "H-001_SNIPER",
        },
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "asset": "eth",
            "slot": "eth-updown-5m-1",
            "side": "UP",
            "price": 0.70,
            "shares": 5.0,
            "cost_usd": 3.5,
            "won": False,
            "pnl": -3.5,
            "outcome": "DOWN",
            "strategy": "H-001_SNIPER",
        },
    ]
    p = tmp_path / "trades.json"
    p.write_text(json.dumps(trades), encoding="utf-8")
    loaded = load_trades(p)
    assert len(loaded) == 2
    row = public_trade(loaded[0])
    assert row["slug"] == "btc-updown-5m-1"
    assert row["polymarket_url"].endswith("btc-updown-5m-1")
    stats = build_stats(loaded, {"mode": "paper_live", "bankroll": 48.0})
    assert stats["trades"] == 2
    assert stats["wins"] == 1
    assert abs(stats["pnl"] - (1.45 - 3.5)) < 1e-9
    assert stats["by_asset"]["btc"]["wins"] == 1
    assert stats["by_asset"]["eth"]["wins"] == 0
    assert stats["bankroll"] == 48.0
