"""Tests for poly15m-ported battle patterns: journal, paper risk, fill parse."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import config
import journal
from polymarket_client import extract_filled_shares, extract_order_status, is_fill_confirmed
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
