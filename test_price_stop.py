"""Tests for the bid-based max-loss stop (PRICE_STOP_*)."""
from __future__ import annotations

import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import config
from bot import TWAPBot
from http_api import exit_reason_bucket
from polymarket_client import fillable_bid_shares

STRIKE = 90000.0


def test_fillable_bid_shares_walks_real_book():
    book = {"bids": [{"price": "0.10", "size": "100"}, {"price": "0.30", "size": "4"}, {"price": "0.25", "size": "3"}]}
    filled, vwap = fillable_bid_shares(book, 10.0)
    assert filled == 10.0
    assert abs(vwap - (4 * 0.30 + 3 * 0.25 + 3 * 0.10) / 10.0) < 1e-9
    assert fillable_bid_shares(None, 5.0) == (0.0, 0.0)
    assert fillable_bid_shares({"bids": []}, 5.0) == (0.0, 0.0)
    assert fillable_bid_shares({"bids": [{"price": "0.3", "size": "2"}]}, 5.0) == (2.0, 0.3)


def test_exit_reason_bucket_price_stop():
    assert exit_reason_bucket("price_stop") == "price_stop"


class TestPriceStop(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        keys = ("MODE", "STRATEGY", "PRICE_STOP_ENABLED", "PRICE_STOP_FRACTION", "PRICE_STOP_PERSISTENCE_S")
        self.saved = {k: getattr(config, k) for k in keys}
        config.MODE = "paper_live"
        config.STRATEGY = "JEV_DYNAMIC"
        config.PRICE_STOP_ENABLED = True
        config.PRICE_STOP_FRACTION = 0.50
        config.PRICE_STOP_PERSISTENCE_S = 3.0
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.temp_dir
        self.bot.risk.bankroll = 50.0
        self.bot._trades_log_path = self.temp_dir / "trades.json"
        self.bot._save_slot_state = lambda **kw: None
        self.book = {"bids": [{"price": "0.30", "size": "500"}], "asks": [{"price": "0.34", "size": "500"}]}
        self.bot.pub_client.get_order_book = MagicMock(side_effect=lambda tok: self.book)
        self.jp = patch("bot.journal_write")
        self.journal = self.jp.start()

    async def asyncTearDown(self):
        self.jp.stop()
        for k, v in self.saved.items():
            setattr(config, k, v)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _setup(self, strategy="JEV_DYNAMIC_MOMENTUM", entry=0.70, remaining=150.0):
        now = time.time()
        slot_start = now - (300.0 - remaining)
        market = {
            "slug": f"btc-updown-5m-test-pstop-{strategy}-{entry}", "slot_start_ts": slot_start,
            "end_ts": slot_start + 300.0, "tokens": {"UP": "tok_up", "DOWN": "tok_down"}, "fee_rate": 0.0,
        }
        slot = self.bot.slots["btc"]
        slot.market = market
        slot.strike = STRIKE
        slot.position = {
            "asset": "btc", "slot": market["slug"], "strategy": strategy, "side": "UP", "token_id": "tok_up",
            "opp_token_id": "tok_down", "price": entry, "shares": 10.0, "cost_usd": round(10 * entry, 2),
            "fee": 0.0, "strike": STRIKE, "entry_time": now - 30, "slot_start_ts": slot_start,
        }
        # Spot still favourable (no adverse cross) -> legacy adverse stop would NOT fire.
        self.bot.cl_stream._record(now, STRIKE + 50.0, asset="btc")
        return slot, market, now, remaining

    async def _manage(self, slot, market, remaining, now):
        await self.bot._manage_open_momentum_position(
            slot, market, remaining, market["tokens"], spot=STRIKE + 50.0, strike=STRIKE,
            realized_twap=STRIKE + 40.0, elapsed_s=300.0 - remaining, now=now,
        )

    def _last_trade(self):
        import json
        return json.loads(self.bot._trades_log_path.read_text())[-1]

    async def test_momentum_bid_below_threshold_persisted_exits_at_real_book(self):
        slot, market, now, rem = self._setup()
        self.bot.book_stream._apply_update("tok_up", bid=0.30, ask=0.34, bid_sz=500, ask_sz=500)
        with self.assertLogs("twap-bot", level="INFO") as cm:
            await self._manage(slot, market, rem, now)
            self.assertIsNotNone(slot.position)  # armed, not yet persisted
            await self._manage(slot, market, rem - 3.5, now + 3.5)
        self.assertIsNone(slot.position)
        self.assertTrue(any("PRICE_STOP armed" in m for m in cm.output))
        self.assertTrue(any("PRICE_STOP exit" in m for m in cm.output))
        t = self._last_trade()
        self.assertEqual(t["exit_reason"], "price_stop")
        self.assertAlmostEqual(t["exit_price"], 0.30)
        self.assertAlmostEqual(t["pnl"], round(10 * 0.30 - 7.0, 2))

    async def test_maker_leg_also_stopped(self):
        slot, market, now, rem = self._setup(strategy="JEV_DYNAMIC_MAKER", entry=0.50)
        self.book = {"bids": [{"price": "0.22", "size": "500"}]}
        self.bot.book_stream._apply_update("tok_up", bid=0.22, ask=0.26, bid_sz=500, ask_sz=500)
        await self._manage(slot, market, rem, now)
        await self._manage(slot, market, rem - 3.0, now + 3.0)
        self.assertIsNone(slot.position)
        self.assertEqual(self._last_trade()["exit_reason"], "price_stop")

    async def test_recovery_resets_persistence(self):
        slot, market, now, rem = self._setup()
        self.bot.book_stream._apply_update("tok_up", bid=0.30, ask=0.34, bid_sz=500, ask_sz=500)
        await self._manage(slot, market, rem, now)
        self.bot.book_stream._apply_update("tok_up", bid=0.60, ask=0.62, bid_sz=500, ask_sz=500)
        await self._manage(slot, market, rem - 2.0, now + 2.0)
        self.assertIsNone(slot.position.get("price_stop_start_ts"))
        self.bot.book_stream._apply_update("tok_up", bid=0.30, ask=0.34, bid_sz=500, ask_sz=500)
        await self._manage(slot, market, rem - 4.0, now + 4.0)
        self.assertIsNotNone(slot.position)  # timer restarted, not persisted yet

    async def test_bid_above_threshold_no_exit(self):
        slot, market, now, rem = self._setup()
        self.bot.book_stream._apply_update("tok_up", bid=0.36, ask=0.40, bid_sz=500, ask_sz=500)
        await self._manage(slot, market, rem, now)
        await self._manage(slot, market, rem - 10.0, now + 10.0)
        self.assertIsNotNone(slot.position)
        self.assertIsNone(slot.position.get("price_stop_start_ts"))

    async def test_disabled_is_legacy(self):
        config.PRICE_STOP_ENABLED = False
        slot, market, now, rem = self._setup()
        self.bot.book_stream._apply_update("tok_up", bid=0.30, ask=0.34, bid_sz=500, ask_sz=500)
        await self._manage(slot, market, rem, now)
        await self._manage(slot, market, rem - 10.0, now + 10.0)
        self.assertIsNotNone(slot.position)

    async def test_no_phantom_complement_bid(self):
        """Own book has no bid; a 1-opp_ask implied bid must NOT arm/fill the price stop."""
        slot, market, now, rem = self._setup()
        self.book = {"bids": [], "asks": []}
        self.bot.book_stream._apply_update("tok_down", bid=0.60, ask=0.75, bid_sz=500, ask_sz=500)  # implied 0.25
        self.assertIsNone(self.bot._real_best_bid("tok_up"))
        await self._manage(slot, market, rem, now)
        await self._manage(slot, market, rem - 5.0, now + 5.0)
        self.assertIsNotNone(slot.position)

    async def test_thin_book_paper_exit_walks_levels(self):
        slot, market, now, rem = self._setup()
        self.bot.book_stream._apply_update("tok_up", bid=0.30, ask=0.34, bid_sz=4, ask_sz=500)
        self.book = {"bids": [{"price": "0.30", "size": "4"}, {"price": "0.20", "size": "4"}]}
        await self._manage(slot, market, rem, now)
        await self._manage(slot, market, rem - 3.0, now + 3.0)
        self.assertIsNone(slot.position)
        exp = round((4 * 0.30 + 4 * 0.20 + 2 * 0.01) / 10.0, 4)
        self.assertAlmostEqual(self._last_trade()["exit_price"], exp)
