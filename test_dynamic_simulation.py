"""End-to-end paper simulation test for JEV_DYNAMIC mode in TWAPBot."""
from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

import config
from bot import TWAPBot
from jev_advisor import JevDecision


class TestDynamicSimulation(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.old_mode = config.MODE
        self.old_strat = config.STRATEGY
        self.old_mom_min_ask = config.DYNAMIC_MOMENTUM_MIN_ASK
        self.old_mom_max_ask = config.DYNAMIC_MOMENTUM_MAX_ASK

        config.MODE = "paper_live"
        config.STRATEGY = "JEV_DYNAMIC"

        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.temp_dir
        self.bot.risk.bankroll = 50.0
        self.bot._slot_done = set()
        self.bot._trades_log_path = self.temp_dir / "trades.json"

    async def asyncTearDown(self):
        config.MODE = self.old_mode
        config.STRATEGY = self.old_strat
        config.DYNAMIC_MOMENTUM_MIN_ASK = self.old_mom_min_ask
        config.DYNAMIC_MOMENTUM_MAX_ASK = self.old_mom_max_ask
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_dynamic_momentum_taker_flow(self):
        """Simulate dynamic momentum taker entry with Jev approval."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0  # 180s elapsed, 120s remaining

        market = {
            "slug": "btc-usd-5m-test-dynamic-mom",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 91000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.0,
        }

        # Populate feed stream with strong upward trend
        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 91000.0

        self.bot.cl_stream._record(slot_start, 91000.0, asset=asset)
        self.bot.cl_stream._record(slot_start + 60.0, 91040.0, asset=asset)
        self.bot.cl_stream._record(slot_start + 120.0, 91080.0, asset=asset)
        self.bot.cl_stream._record(now, 91300.0, asset=asset)  # spot=91300, +$300 move (~33 bps)

        # Setup order book with liquid ask (within positive payout range 0.38 - 0.50)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.45, ask=0.49, bid_sz=500, ask_sz=500)

        # Mock Jev decision: Approved clean trend
        mock_jev_decision = JevDecision(
            action="proceed",
            passed=True,
            size_multiplier=1.0,
            conviction=2.8,
            reversal_risk=0.12,
            market_regime="clean_trend",
            confidence=0.91,
            reason="Clean momentum breakout above strike with low reversal probability.",
            latency_ms=120.0,
            fallback_used=False,
        )
        self.bot.jev.evaluate_dynamic = AsyncMock(return_value=mock_jev_decision)

        # Run tick
        await self.bot._tick_asset(slot, now)

        # Assertions
        self.assertIsNotNone(slot.position)
        pos = slot.position
        self.assertEqual(pos["strategy"], "JEV_DYNAMIC_MOMENTUM")
        self.assertEqual(pos["side"], "UP")
        self.assertEqual(pos["price"], 0.49)
        self.assertIn("jev", pos)
        self.assertEqual(pos["jev"]["action"], "proceed")
        self.assertEqual(pos["jev"]["market_regime"], "clean_trend")
        self.assertIn(market["slug"], self.bot._slot_done)

    async def test_dynamic_maker_bid_flow(self):
        """Simulate dynamic midband maker bid placement and requoting with Jev."""
        asset = "sol"
        now = time.time()
        slot_start = now - 120.0  # 120s elapsed, 180s remaining

        market = {
            "slug": "sol-usd-5m-test-dynamic-maker",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 180.00,
            "tokens": {"UP": "tok_up_sol", "DOWN": "tok_down_sol"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 180.00

        self.bot.cl_stream._record(slot_start, 180.00, asset=asset)
        self.bot.cl_stream._record(slot_start + 60.0, 180.10, asset=asset)
        self.bot.cl_stream._record(now, 180.15, asset=asset)

        # Order book centered around 0.50
        self.bot.book_stream._apply_update("tok_up_sol", bid=0.48, ask=0.52, bid_sz=500, ask_sz=500)

        # Mock Jev decision: Approved with 50% downsize
        mock_jev_decision = JevDecision(
            action="downsize",
            passed=True,
            size_multiplier=0.5,
            conviction=1.8,
            reversal_risk=0.28,
            market_regime="clean_trend",
            confidence=0.75,
            reason="Clean trend with mild pullback; downsize maker inventory to reduce risk.",
            latency_ms=110.0,
            fallback_used=False,
        )
        self.bot.jev.evaluate_dynamic = AsyncMock(return_value=mock_jev_decision)

        # Run tick
        await self.bot._tick_asset(slot, now)

        # Assertions: resting order placed
        self.assertIsNotNone(slot.resting_order)
        ro = slot.resting_order
        self.assertEqual(ro["side"], "UP")
        self.assertEqual(ro["price"], 0.47)  # 0.50 mid - 0.03 half spread
        self.assertIn("jev", ro)
        self.assertEqual(ro["jev"]["action"], "downsize")
        self.assertEqual(ro["jev"]["size_multiplier"], 0.5)

    async def test_dynamic_veto_flow(self):
        """Simulate dynamic candidate vetoed by Jev guardian."""
        asset = "doge"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "doge-usd-5m-test-dynamic-veto",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 0.250,
            "tokens": {"UP": "tok_up_doge", "DOWN": "tok_down_doge"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 0.250

        self.bot.cl_stream._record(slot_start, 0.250, asset=asset)
        self.bot.cl_stream._record(slot_start + 60.0, 0.255, asset=asset)
        self.bot.cl_stream._record(slot_start + 120.0, 0.248, asset=asset)
        self.bot.cl_stream._record(now, 0.256, asset=asset)

        self.bot.book_stream._apply_update("tok_up_doge", bid=0.68, ask=0.72, bid_sz=500, ask_sz=500)

        # Mock Jev decision: Abstain / Veto
        mock_jev_decision = JevDecision(
            action="abstain",
            passed=False,
            size_multiplier=0.0,
            conviction=0.6,
            reversal_risk=0.75,
            market_regime="mean_reverting_chop",
            confidence=0.82,
            reason="High reversal probability (0.75 > 0.60) in choppy strike-straddling regime.",
            latency_ms=95.0,
            fallback_used=False,
        )
        self.bot.jev.evaluate_dynamic = AsyncMock(return_value=mock_jev_decision)

        # Run tick
        await self.bot._tick_asset(slot, now)

        # Assertions: No trade taken and no resting order
        self.assertIsNone(slot.position)
        self.assertIsNone(slot.resting_order)
        self.assertNotIn(market["slug"], self.bot._slot_done)

    async def test_dynamic_maker_adverse_move_cancellation(self):
        """Simulate dynamic maker order cancellation when spot price reverses adversely."""
        asset = "sol"
        now = time.time()
        slot_start = now - 120.0

        market = {
            "slug": "sol-usd-5m-test-adverse-cancel",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 180.00,
            "tokens": {"UP": "tok_up_sol", "DOWN": "tok_down_sol"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 180.00

        self.bot.cl_stream._record(slot_start, 180.00, asset=asset)
        self.bot.cl_stream._record(slot_start + 60.0, 180.10, asset=asset)
        self.bot.cl_stream._record(now, 180.15, asset=asset)

        self.bot.book_stream._apply_update("tok_up_sol", bid=0.48, ask=0.52, bid_sz=500, ask_sz=500)

        # Place initial maker bid
        mock_jev_decision = JevDecision(
            action="proceed", passed=True, size_multiplier=1.0, conviction=2.0,
            reversal_risk=0.15, market_regime="clean_trend", confidence=0.85,
            reason="Clean trend above strike.", latency_ms=100.0, fallback_used=False,
        )
        self.bot.jev.evaluate_dynamic = AsyncMock(return_value=mock_jev_decision)

        await self.bot._tick_asset(slot, now)
        self.assertIsNotNone(slot.resting_order)
        self.assertEqual(slot.resting_order["side"], "UP")

        # Now simulate market crash: spot drops to 179.80 (< strike 180.00)
        crash_time = now + 5.0
        self.bot.cl_stream._record(crash_time, 179.80, asset=asset)
        # Orderbook ask dumps down to 0.45 (which would have triggered an adverse paper fill under old code!)
        self.bot.book_stream._apply_update("tok_up_sol", bid=0.40, ask=0.45, bid_sz=500, ask_sz=500)

        await self.bot._tick_asset(slot, crash_time)

        # Assert: Resting order cancelled, position NEVER opened!
        self.assertIsNone(slot.resting_order)
        self.assertIsNone(slot.position)


if __name__ == "__main__":
    unittest.main()
