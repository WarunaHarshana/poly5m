from __future__ import annotations
import json
import tempfile
from pathlib import Path

import config
from bot import TWAPBot


def test_slot_state_roundtrip(monkeypatch=None):
    d = Path(tempfile.mkdtemp())
    # Point STATE_DIR via monkeypatch of instance methods after construct is heavy;
    # call helpers on a lightweight stand-in.
    class _T:
        _slot_done = {"btc-updown-5m-1"}
        def _slot_state_path(self):
            return d / "slot_state.json"
        _load_slot_state = TWAPBot._load_slot_state
        _save_slot_state = TWAPBot._save_slot_state

    t = _T()
    TWAPBot._save_slot_state(t, slot_ts=123, slug="btc-updown-5m-1", strike=79551.8)
    data = TWAPBot._load_slot_state(t)
    assert data["slot_ts"] == 123
    assert abs(float(data["strike"]) - 79551.8) < 1e-6
    assert "btc-updown-5m-1" in data["slot_done"]
    # None strike keeps previous
    TWAPBot._save_slot_state(t, slot_ts=123, slug="btc-updown-5m-1", strike=None)
    data2 = TWAPBot._load_slot_state(t)
    assert abs(float(data2["strike"]) - 79551.8) < 1e-6


def test_reset_trade_data(monkeypatch):
    d = Path(tempfile.mkdtemp())
    monkeypatch.setattr(config, "STATE_DIR", d)
    monkeypatch.setattr(config, "JOURNAL_PATH", str(d / "journal.jsonl"))
    monkeypatch.setattr(config, "STARTING_BANKROLL", 50.0)
    monkeypatch.setenv("RESET_TRADE_DATA", "true")

    # Populate fake trades and journal
    trades_path = d / "trades.json"
    with open(trades_path, "w") as f:
        json.dump([{"slug": "test-slot", "won": True, "pnl": 5.0}], f)
    journal_path = d / "journal.jsonl"
    with open(journal_path, "w") as f:
        f.write('{"event": "test"}\n')
    slot_path = d / "slot_state.json"
    with open(slot_path, "w") as f:
        f.write('{"slot_ts": 123}\n')

    class _MockRisk:
        bankroll = 10.0
        daily_pnl = -40.0
        consecutive_losses = 3
        trades_today = 5
        def save_state(self):
            pass

    class _BotStandin:
        _trades_log_path = trades_path
        _slot_done = {"test-slot"}
        risk = _MockRisk()
        def _slot_state_path(self):
            return slot_path
        _check_reset_trade_data = TWAPBot._check_reset_trade_data

    b = _BotStandin()
    b._check_reset_trade_data()

    # Verify archives created
    archives = list(d.glob("trades_archive_*.json"))
    assert len(archives) == 1
    with open(archives[0]) as f:
        archived_data = json.load(f)
    assert len(archived_data) == 1
    assert archived_data[0]["slug"] == "test-slot"

    # Verify trades.json reset to []
    with open(trades_path) as f:
        fresh_trades = json.load(f)
    assert fresh_trades == []

    # Verify slot state cleared
    assert not slot_path.exists()
    assert len(b._slot_done) == 0

    # Verify risk state reset
    assert b.risk.bankroll == 50.0
    assert b.risk.daily_pnl == 0.0
    assert b.risk.consecutive_losses == 0
    assert b.risk.trades_today == 0


def test_restored_strike_slug_validation():
    bot = TWAPBot()
    saved = {
        "slot_ts": 1790184000,
        "assets": {
            "eth": {
                "slug": "eth-updown-5m-1790184000",
                "strike": 2661.1,
                "slot_ts": 1790184000,
            }
        }
    }

    # Stale slug should be rejected
    res_stale = bot._restored_strike(saved, "eth", "eth-updown-5m-1790184300", 1790184300)
    assert res_stale is None, "Should reject strike when current slug does not match saved slug"

    # Matching slug and slot_ts should be restored
    res_valid = bot._restored_strike(saved, "eth", "eth-updown-5m-1790184000", 1790184000)
    assert res_valid == 2661.1, "Should restore strike when current slug matches saved slug"


