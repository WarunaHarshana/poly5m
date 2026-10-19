"""Tests for DIP_BOUNCE_EXIT: maker position sells at >= DIP_BOUNCE_SELL after its bid dipped <= DIP_BOUNCE_DIP."""
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


class TestDipBounceExit(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        keys = ("MODE", "STRATEGY", "MAKER_HOLD_TO_EXPIRY", "PRICE_STOP_ENABLED", "TRAILING_PROFIT_ENABLED",
                "TRAILING_BREAKEVEN_ENABLED", "DIP_BOUNCE_EXIT", "DIP_BOUNCE_DIP", "DIP_BOUNCE_SELL",
                "DIP_BOUNCE_MAX_ATTEMPTS")
        self.saved = {k: getattr(config, k) for k in keys}
        config.MODE = "paper_live"
        config.STRATEGY = "JEV_DYNAMIC"
        config.MAKER_HOLD_TO_EXPIRY = True
        config.PRICE_STOP_ENABLED = False
        config.TRAILING_PROFIT_ENABLED = False
        config.TRAILING_BREAKEVEN_ENABLED = False
        config.DIP_BOUNCE_EXIT = True
        config.DIP_BOUNCE_DIP = 0.35
        config.DIP_BOUNCE_SELL = 0.80
        config.DIP_BOUNCE_MAX_ATTEMPTS = 3
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.temp_dir
        self.bot.risk.bankroll = 50.0
        self.bot._trades_log_path = self.temp_dir / "trades.json"
        self.bot._save_slot_state = lambda **kw: None
        self.bot.pub_client.get_order_book = MagicMock(return_value={"bids": [], "asks": []})
        self.jp = patch("bot.journal_write")
        self.journal = self.jp.start()

    async def asyncTearDown(self):
        self.jp.stop()
        for k, v in self.saved.items():
            setattr(config, k, v)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _setup(self, strategy="JEV_DYNAMIC_MAKER", remaining=200.0):
        now = time.time()
        slot_start = now - (300.0 - remaining)
        market = {"slug": f"btc-updown-5m-dip-{strategy}-{remaining}", "slot_start_ts": slot_start,
                  "end_ts": slot_start + 300.0, "tokens": {"UP": "tok_up", "DOWN": "tok_down"}, "fee_rate": 0.0}
        slot = self.bot.slots["btc"]
        slot.market = market
        slot.strike = STRIKE
        slot.position = {
            "asset": "btc", "slot": market["slug"], "strategy": strategy, "side": "UP", "token_id": "tok_up",
            "opp_token_id": "tok_down", "price": 0.50, "shares": 6.0, "cost_usd": 3.0, "fee": 0.0,
            "strike": STRIKE, "entry_time": now - 30, "slot_start_ts": slot_start,
            "entry_time_utc": "2026-10-19T10:00:00+00:00",
        }
        return slot, market, remaining

    async def _ticks(self, slot, market, rem, bids):
        for k, b in enumerate(bids):
            if slot.position is None:
                break
            self.bot.book_stream._apply_update("tok_up", bid=b, ask=round(b + 0.02, 2), bid_sz=500, ask_sz=500)
            await self.bot._manage_open_momentum_position(
                slot, market, rem - k, market["tokens"], spot=STRIKE, strike=STRIKE,
                realized_twap=STRIKE, elapsed_s=300.0 - rem + k, now=time.time())

    def _last_trade(self):
        return json.loads(self.bot._trades_log_path.read_text())[-1]

    def _dip_events(self):
        return [c for c in self.journal.call_args_list if c.args and c.args[0] == "dip_bounce_exit"]

    async def test_no_trigger_without_dip(self):
        slot, market, rem = self._setup()
        await self._ticks(slot, market, rem, [0.50, 0.40, 0.36, 0.85, 0.90, 0.82])
        self.assertIsNotNone(slot.position)
        self.assertNotIn("dip_low", slot.position)
        self.assertEqual(self._dip_events(), [])

    async def test_trigger_after_dip_then_bounce(self):
        slot, market, rem = self._setup()
        await self._ticks(slot, market, rem, [0.45, 0.33, 0.30, 0.60, 0.79])
        self.assertIsNotNone(slot.position)
        self.assertEqual(slot.position["dip_low"], 0.30)
        await self._ticks(slot, market, rem - 5, [0.81])
        self.assertIsNone(slot.position)
        t = self._last_trade()
        self.assertEqual(t["exit_reason"], "dip_bounce")
        self.assertAlmostEqual(t["exit_price"], 0.81)
        self.assertAlmostEqual(t["pnl"], round(6 * 0.81 - 3.0, 2))
        ev = self._dip_events()
        self.assertEqual(len(ev), 1)
        kw = ev[0].kwargs
        self.assertEqual((kw["asset"], kw["side"], kw["dip_low"], kw["sell_bid"], kw["shares"]),
                         ("btc", "UP", 0.30, 0.81, 6.0))
        self.assertAlmostEqual(kw["pnl"], round(6 * 0.81 - 3.0, 2))
        self.assertEqual(kw["elapsed_s"], 105.0)

    async def test_take_profit_wins_when_bid_jumps_past(self):
        slot, market, rem = self._setup()
        await self._ticks(slot, market, rem, [0.30, 0.96])
        self.assertIsNone(slot.position)
        self.assertEqual(self._last_trade()["exit_reason"], "take_profit")
        self.assertEqual(self._dip_events(), [])

    async def test_flag_off_and_momentum_unaffected(self):
        config.DIP_BOUNCE_EXIT = False
        slot, market, rem = self._setup()
        await self._ticks(slot, market, rem, [0.30, 0.85])
        self.assertIsNotNone(slot.position)
        config.DIP_BOUNCE_EXIT = True
        self.assertFalse(await self.bot._maybe_dip_bounce_exit(
            slot, market, {"strategy": "JEV_DYNAMIC_MOMENTUM", "side": "UP", "dip_low": 0.3, "shares": 6.0},
            0.85, 0.86, 100.0, 200.0))

    async def test_wide_book_does_not_arm(self):
        slot, market, rem = self._setup()
        self.bot.book_stream._apply_update("tok_up", bid=0.20, ask=0.70, bid_sz=5, ask_sz=5)
        await self.bot._manage_open_momentum_position(slot, market, rem, market["tokens"], spot=STRIKE,
                                                      strike=STRIKE, realized_twap=STRIKE, elapsed_s=100.0,
                                                      now=time.time())
        self.assertNotIn("dip_low", slot.position)

    async def test_live_failed_sell_keeps_holding_capped_attempts(self):
        config.MODE = "live"
        self.bot.live_client = MagicMock()
        self.bot.live_client.place_order = MagicMock(return_value={"success": False})
        slot, market, rem = self._setup()
        await self._ticks(slot, market, rem, [0.30] + [0.85] * 6)
        self.assertIsNotNone(slot.position)
        self.assertEqual(self.bot.live_client.place_order.call_count, 3)
        args = self.bot.live_client.place_order.call_args.args
        self.assertEqual((args[0], args[1], args[2], args[3]), ("tok_up", "SELL", 0.83, 6.0))
        self.assertEqual(self._dip_events(), [])

    async def test_live_partial_then_complete(self):
        config.MODE = "live"
        self.bot.live_client = MagicMock()
        self.bot.live_client.place_order = MagicMock(side_effect=[
            {"success": True, "filled": True, "shares": 4.0, "avg_price": 0.85},
            {"success": True, "filled": True, "shares": 2.0, "avg_price": 0.84},
        ])
        slot, market, rem = self._setup()
        await self._ticks(slot, market, rem, [0.30, 0.85])
        self.assertAlmostEqual(slot.position["shares"], 2.0)
        await self._ticks(slot, market, rem - 2, [0.84])
        self.assertIsNone(slot.position)
        self.assertEqual(self._last_trade()["exit_reason"], "dip_bounce")
        ev = self._dip_events()
        self.assertEqual(len(ev), 1)
        self.assertEqual(ev[0].kwargs["shares"], 6.0)


if __name__ == "__main__":
    unittest.main()
