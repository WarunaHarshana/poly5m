"""Tests for MAKER_HOLD_TO_EXPIRY: maker positions hold to settlement (TP still fires); momentum unchanged."""
from __future__ import annotations

import json
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import config
from bot import TWAPBot

STRIKE = 90000.0
ADVERSE_SPOT = STRIKE - 500.0  # far below strike: adverse for an UP position


class TestMakerHoldToExpiry(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        keys = ("MODE", "STRATEGY", "MAKER_HOLD_TO_EXPIRY", "PRICE_STOP_ENABLED", "PRICE_STOP_FRACTION",
                "PRICE_STOP_PERSISTENCE_S", "TRAILING_PROFIT_ENABLED", "TRAILING_BREAKEVEN_ENABLED")
        self.saved = {k: getattr(config, k) for k in keys}
        config.MODE = "paper_live"
        config.STRATEGY = "JEV_DYNAMIC"
        config.MAKER_HOLD_TO_EXPIRY = True
        # Turn every early-exit mechanism on so the test proves they are all skipped for maker.
        config.PRICE_STOP_ENABLED = True
        config.PRICE_STOP_FRACTION = 0.50
        config.PRICE_STOP_PERSISTENCE_S = 0.0
        config.TRAILING_PROFIT_ENABLED = True
        config.TRAILING_BREAKEVEN_ENABLED = True
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.temp_dir
        self.bot.risk.bankroll = 50.0
        self.bot._trades_log_path = self.temp_dir / "trades.json"
        self.bot._save_slot_state = lambda **kw: None
        self.book = {"bids": [{"price": "0.10", "size": "500"}], "asks": [{"price": "0.14", "size": "500"}]}
        self.bot.pub_client.get_order_book = MagicMock(side_effect=lambda tok: self.book)
        self.jp = patch("bot.journal_write")
        self.journal = self.jp.start()

    async def asyncTearDown(self):
        self.jp.stop()
        for k, v in self.saved.items():
            setattr(config, k, v)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _setup(self, strategy="JEV_DYNAMIC_MAKER", entry=0.50, remaining=15.0, spot=ADVERSE_SPOT):
        now = time.time()
        slot_start = now - (300.0 - remaining)
        market = {
            "slug": f"btc-updown-5m-test-hold-{strategy}-{entry}-{remaining}", "slot_start_ts": slot_start,
            "end_ts": slot_start + 300.0, "tokens": {"UP": "tok_up", "DOWN": "tok_down"}, "fee_rate": 0.0,
        }
        slot = self.bot.slots["btc"]
        slot.market = market
        slot.strike = STRIKE
        shares = 6.0
        slot.position = {
            "asset": "btc", "slot": market["slug"], "strategy": strategy, "side": "UP", "token_id": "tok_up",
            "opp_token_id": "tok_down", "price": entry, "shares": shares, "cost_usd": round(shares * entry, 2),
            "fee": 0.0, "strike": STRIKE, "entry_time": now - 60, "slot_start_ts": slot_start,
            "entry_time_utc": "2026-10-03T10:00:00+00:00",
        }
        for i in range(30):
            self.bot.cl_stream._record(now - 30 + i, spot, asset="btc")
        return slot, market, now, remaining

    async def _manage(self, slot, market, remaining, now, spot=ADVERSE_SPOT):
        await self.bot._manage_open_momentum_position(
            slot, market, remaining, market["tokens"], spot=spot, strike=STRIKE,
            realized_twap=spot, elapsed_s=300.0 - remaining, now=now,
        )

    def _last_trade(self):
        return json.loads(self.bot._trades_log_path.read_text())[-1]

    async def _crash_ticks(self, slot, market, rem, now):
        self.bot.book_stream._apply_update("tok_up", bid=0.10, ask=0.14, bid_sz=500, ask_sz=500)
        for k in range(6):
            if slot.position is None:
                break
            await self._manage(slot, market, rem - k, now + k)

    # (1) on: maker at bid 0.10, spot adverse, 15s left -> no exit
    async def test_on_maker_crash_does_not_exit(self):
        slot, market, now, rem = self._setup()
        with self.assertLogs("twap-bot", level="INFO") as cm:
            await self._crash_ticks(slot, market, rem, now)
        self.assertIsNotNone(slot.position)
        self.assertFalse(self.bot._trades_log_path.exists())
        holds = [m for m in cm.output if "MAKER_HOLD_TO_EXPIRY holding" in m]
        self.assertEqual(len(holds), 1)  # logged once per position

    async def test_on_maker_trailing_does_not_exit(self):
        slot, market, now, rem = self._setup(remaining=150.0, spot=STRIKE + 50.0)
        self.bot.book_stream._apply_update("tok_up", bid=0.90, ask=0.92, bid_sz=500, ask_sz=500)
        await self._manage(slot, market, rem, now, spot=STRIKE + 50.0)
        self.bot.book_stream._apply_update("tok_up", bid=0.70, ask=0.72, bid_sz=500, ask_sz=500)
        await self._manage(slot, market, rem - 1, now + 1, spot=STRIKE + 50.0)
        self.assertIsNotNone(slot.position)

    # (2) off: same scenario exits as before (legacy behaviour)
    async def test_off_maker_crash_exits_as_before(self):
        config.MAKER_HOLD_TO_EXPIRY = False
        config.PRICE_STOP_ENABLED = False
        slot, market, now, rem = self._setup()
        await self._crash_ticks(slot, market, rem, now)
        self.assertIsNone(slot.position)
        self.assertEqual(self._last_trade()["exit_reason"], "hard_flatten_expiry")

    # (3) momentum unaffected with the flag on
    async def test_on_momentum_still_exits(self):
        config.PRICE_STOP_ENABLED = False
        slot, market, now, rem = self._setup(strategy="JEV_DYNAMIC_MOMENTUM", entry=0.70)
        await self._crash_ticks(slot, market, rem, now)
        self.assertIsNone(slot.position)
        self.assertEqual(self._last_trade()["exit_reason"], "hard_flatten_expiry")

    # (4) maker TP still fires with the flag on
    async def test_on_maker_take_profit_fires(self):
        slot, market, now, rem = self._setup(remaining=100.0, spot=STRIKE + 80.0)
        self.book = {"bids": [{"price": "0.96", "size": "500"}], "asks": [{"price": "0.97", "size": "500"}]}
        self.bot.book_stream._apply_update("tok_up", bid=0.96, ask=0.97, bid_sz=500, ask_sz=500)
        await self._manage(slot, market, rem, now, spot=STRIKE + 80.0)
        self.assertIsNone(slot.position)
        t = self._last_trade()
        self.assertEqual(t["exit_reason"], "take_profit")
        self.assertGreater(t["pnl"], 0)

    # (5) the held position settles correctly at expiry (win and loss)
    async def test_on_maker_settles_at_expiry(self):
        for resolved, exp_pnl, exp_won in (("DOWN", -3.0, False), ("UP", 3.0, True)):
            slot, market, now, rem = self._setup()
            await self._crash_ticks(slot, market, rem, now)
            pos = slot.position
            self.assertIsNotNone(pos)
            slot.position = None
            self.bot._settle_filled({"position": pos, "market": market, "asset": "btc"},
                                    {"side": resolved, "source": "polymarket_test", "official": True})
            t = self._last_trade()
            self.assertEqual(t["strategy"], "JEV_DYNAMIC_MAKER")
            self.assertEqual(bool(t["won"]), exp_won)
            self.assertAlmostEqual(t["pnl"], exp_pnl, places=2)
            self.assertFalse(t.get("early_exit"))

    def test_helper_flag_and_scope(self):
        f = TWAPBot._maker_hold_to_expiry
        self.assertTrue(f({"strategy": "JEV_DYNAMIC_MAKER"}))
        self.assertTrue(f({"strategy": "MIDBAND_MAKER"}))
        self.assertFalse(f({"strategy": "JEV_DYNAMIC_MOMENTUM"}))
        self.assertFalse(f(None))
        config.MAKER_HOLD_TO_EXPIRY = False
        self.assertFalse(f({"strategy": "JEV_DYNAMIC_MAKER"}))
