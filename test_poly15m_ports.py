"""Tests for poly15m-ported battle patterns: journal, paper risk, fill parse."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import config
import journal
from polymarket_client import (
    extract_filled_shares,
    extract_order_status,
    fillable_ask_shares,
    is_fill_confirmed,
    parse_event_resolution,
)
from risk_manager import RiskManager


def test_journal_appends_jsonl(tmp_path: Path | None = None):
    d = Path(tempfile.mkdtemp())
    path = d / "journal.jsonl"
    journal.configure(str(path))
    journal.write("paper_fill", side="UP", price=0.8)
    journal.write("settlement", pnl=-1.2, won=False)
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    r0 = json.loads(lines[0])
    assert r0["event"] == "paper_fill"
    assert r0["side"] == "UP"
    assert journal.last()["event"] == "settlement"


def test_fillable_ask_shares_walks_book():
    book = {
        "asks": [
            {"price": "0.80", "size": "3"},
            {"price": "0.81", "size": "4"},
            {"price": "0.90", "size": "100"},
        ]
    }
    sh, vwap = fillable_ask_shares(book, 0.81)
    assert abs(sh - 7.0) < 1e-9
    assert abs(vwap - (3 * 0.80 + 4 * 0.81) / 7.0) < 1e-9
    sh2, _ = fillable_ask_shares(book, 0.79)
    assert sh2 == 0.0


def test_parse_event_resolution_waits_until_close():
    ev = {
        "slug": "btc-updown-5m-1000",
        "closed": False,
        "markets": [{
            "outcomes": '["Up", "Down"]',
            "outcomePrices": '["0.20", "0.80"]',
            "closed": False,
        }],
        "eventMetadata": {},
    }
    # Window still open (now << 1000+300)
    assert parse_event_resolution(ev, now_ts=1100.0) is None


def test_parse_event_resolution_uses_outcome_prices():
    ev = {
        "slug": "eth-updown-5m-1000",
        "closed": True,
        "markets": [{
            "outcomes": '["Up", "Down"]',
            "outcomePrices": '["0", "1"]',
            "closed": True,
            "automaticallyResolved": True,
            "umaResolutionStatus": "resolved",
        }],
        "eventMetadata": {"finalPrice": 2500.0, "priceToBeat": 2510.0},
    }
    res = parse_event_resolution(ev, now_ts=2000.0)
    assert res is not None
    assert res["side"] == "DOWN"
    assert res["source"] == "polymarket_outcome_prices"
    assert res["official"] is True


def test_fill_helpers_size_matched():
    assert is_fill_confirmed({"status": "live", "size_matched": "0"}) is False
    assert is_fill_confirmed({"status": "matched", "size_matched": "5"}) is True
    assert is_fill_confirmed({"status": "LIVE", "sizeMatched": 3.0}) is True
    assert extract_order_status({"status": "Matched"}) == "matched"
    assert extract_filled_shares({"size_matched": "2.5"}) == 2.5


def test_paper_mode_ignores_daily_loss(tmp_path=None):
    d = Path(tempfile.mkdtemp())
    old_mode = config.MODE
    try:
        config.MODE = "paper_live"
        risk = RiskManager(state_dir=d)
        risk.daily_pnl = -999.0
        risk.consecutive_losses = 99
        risk.bankroll = config.STARTING_BANKROLL
        ok, reason = risk.can_trade()
        assert ok, reason
    finally:
        config.MODE = old_mode


def test_live_mode_enforces_daily_loss(tmp_path=None):
    d = Path(tempfile.mkdtemp())
    old_mode = config.MODE
    try:
        config.MODE = "live"
        risk = RiskManager(state_dir=d)
        risk.daily_pnl = -config.DAILY_LOSS_LIMIT_USD - 1.0
        risk.bankroll = config.STARTING_BANKROLL
        ok, reason = risk.can_trade()
        assert not ok
        assert "daily loss" in reason
    finally:
        config.MODE = old_mode
