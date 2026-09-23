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

    def test_dynamic_price_time_feasibility_matrix(self):
        """Verify AVN-MAS Dynamic Price-Time Feasibility Matrix scaling & boundaries."""
        spot = 90000.0
        daily_vol = 0.0106  # BTC ~106 bps

        # Boundary: t < 30s prohibited
        allowed, min_move, max_ask, _ = config.compute_dynamic_feasibility(29.9, spot, daily_vol)
        self.assertFalse(allowed)

        # Tier 1: 30s <= t < 90s: mult=2.0, max_ask=0.72
        allowed, min_move, max_ask, sigma_t = config.compute_dynamic_feasibility(30.0, spot, daily_vol)
        self.assertTrue(allowed)
        self.assertEqual(max_ask, 0.72)
        expected_sigma_30 = daily_vol * ((30.0 / 86400.0) ** 0.5) * spot
        self.assertAlmostEqual(sigma_t, expected_sigma_30, places=4)
        self.assertAlmostEqual(min_move, 2.0 * expected_sigma_30, places=4)

        allowed, min_move, max_ask, _ = config.compute_dynamic_feasibility(89.9, spot, daily_vol)
        self.assertTrue(allowed)
        self.assertEqual(max_ask, 0.72)

        # Tier 2: 90s <= t < 150s: mult=1.5, max_ask=0.82
        allowed, min_move, max_ask, sigma_t = config.compute_dynamic_feasibility(90.0, spot, daily_vol)
        self.assertTrue(allowed)
        self.assertEqual(max_ask, 0.82)
        expected_sigma_90 = daily_vol * ((90.0 / 86400.0) ** 0.5) * spot
        self.assertAlmostEqual(min_move, 1.5 * expected_sigma_90, places=4)

        # Tier 3: 150s <= t < 210s: mult=1.2, max_ask=0.88
        allowed, min_move, max_ask, sigma_t = config.compute_dynamic_feasibility(150.0, spot, daily_vol)
        self.assertTrue(allowed)
        self.assertEqual(max_ask, 0.88)
        expected_sigma_150 = daily_vol * ((150.0 / 86400.0) ** 0.5) * spot
        self.assertAlmostEqual(min_move, 1.2 * expected_sigma_150, places=4)

        # Tier 4: 210s <= t < 240s: mult=1.0, max_ask=0.90
        allowed, min_move, max_ask, sigma_t = config.compute_dynamic_feasibility(210.0, spot, daily_vol)
        self.assertTrue(allowed)
        self.assertEqual(max_ask, 0.90)
        expected_sigma_210 = daily_vol * ((210.0 / 86400.0) ** 0.5) * spot
        self.assertAlmostEqual(min_move, 1.0 * expected_sigma_210, places=4)

        # Boundary: t >= 240s prohibited
        allowed, min_move, max_ask, _ = config.compute_dynamic_feasibility(240.0, spot, daily_vol)
        self.assertFalse(allowed)

        allowed, min_move, max_ask, _ = config.compute_dynamic_feasibility(275.0, spot, daily_vol)
        self.assertFalse(allowed)

    async def test_dynamic_tier1_momentum_flow(self):
        """Simulate momentum entry during Tier 1 (30s <= t < 90s) with P_max = 0.72."""
        asset = "btc"
        now = time.time()
        slot_start = now - 60.0  # 60s elapsed, 240s remaining (Tier 1)

        market = {
            "slug": "btc-usd-5m-test-tier1",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 90000.0

        self.bot.cl_stream._record(slot_start, 90000.0, asset=asset)
        self.bot.cl_stream._record(slot_start + 30.0, 90080.0, asset=asset)
        self.bot.cl_stream._record(now, 90150.0, asset=asset)  # spot=90150, +$150 move

        # Order book has ask=0.68, within Tier 1 P_max=0.72, spread=0.04 <= 0.05
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.64, ask=0.68, bid_sz=500, ask_sz=500)

        mock_jev = JevDecision(
            action="proceed", passed=True, size_multiplier=1.0, conviction=2.5,
            reversal_risk=0.15, market_regime="clean_trend", confidence=0.88,
            reason="Clean breakout in early window.", latency_ms=100.0, fallback_used=False,
        )
        self.bot.jev.evaluate_dynamic = AsyncMock(return_value=mock_jev)

        await self.bot._tick_asset(slot, now)

        self.assertIsNotNone(slot.position)
        self.assertEqual(slot.position["strategy"], "JEV_DYNAMIC_MOMENTUM")
        self.assertEqual(slot.position["price"], 0.68)
        self.assertEqual(slot.position["side"], "UP")

    async def test_directional_coherence_rejection(self):
        """Reject entry when spot and realized TWAP disagree on direction."""
        asset = "btc"
        now = time.time()
        slot_start = now - 120.0  # 120s elapsed

        market = {
            "slug": "btc-usd-5m-test-incoherent",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 90000.0

        # Spot spent most of the slot deep down (89800), then spiked to 90050 at the last second
        # Spot (90050) > Strike (90000), BUT TWAP (~89850) < Strike (90000)!
        self.bot.cl_stream._record(slot_start, 89800.0, asset=asset)
        self.bot.cl_stream._record(slot_start + 60.0, 89800.0, asset=asset)
        self.bot.cl_stream._record(slot_start + 110.0, 89820.0, asset=asset)
        self.bot.cl_stream._record(now, 90050.0, asset=asset)

        self.bot.book_stream._apply_update("tok_up_btc", bid=0.60, ask=0.64, bid_sz=500, ask_sz=500)
        mock_jev = JevDecision(
            action="abstain", passed=False, size_multiplier=0.0, conviction=0.0,
            reversal_risk=1.0, market_regime="mean_reverting_chop", confidence=0.0,
            reason="veto", latency_ms=10.0, fallback_used=False,
        )
        self.bot.jev.evaluate_dynamic = AsyncMock(return_value=mock_jev)

        await self.bot._tick_asset(slot, now)

        # Directional coherence violated -> no momentum trade, Jev never called for MOMENTUM_TAKER
        self.assertIsNone(slot.position)
        for call in self.bot.jev.evaluate_dynamic.call_args_list:
            self.assertNotEqual(call.kwargs.get("opportunity_type"), "MOMENTUM_TAKER")

    async def test_spread_gate_rejection(self):
        """Reject dynamic entry when orderbook spread exceeds DYNAMIC_MAX_BOOK_SPREAD (0.05)."""
        asset = "btc"
        now = time.time()
        slot_start = now - 120.0

        market = {
            "slug": "btc-usd-5m-test-wide-spread",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 90000.0

        self.bot.cl_stream._record(slot_start, 90000.0, asset=asset)
        self.bot.cl_stream._record(slot_start + 60.0, 90100.0, asset=asset)
        self.bot.cl_stream._record(now, 90200.0, asset=asset)

        # Orderbook spread is 0.08 > 0.05
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.60, ask=0.68, bid_sz=500, ask_sz=500)
        self.bot.jev.evaluate_dynamic = AsyncMock()

        await self.bot._tick_asset(slot, now)

        self.assertIsNone(slot.position)
        self.assertIsNone(slot.resting_order)

    async def test_free_ride_take_profit_exit(self):
        """For open positions, trigger early take-profit exit when market best bid reaches >= 0.95."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-take-profit",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 90000.0

        # Simulate existing open position bought at 0.70
        slot.position = {
            "asset": asset,
            "slot": market["slug"],
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "price": 0.70,
            "shares": 10.0,
            "cost_usd": 7.00,
            "strike": 90000.0,
        }

        self.bot.cl_stream._record(now, 90500.0, asset=asset)
        # Market best bid surges to 0.96 (>= 0.95)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.96, ask=0.98, bid_sz=500, ask_sz=500)

        # Run tick (120s remaining)
        await self.bot._tick_asset(slot, now)

        # Position should be closed via take-profit early exit
        self.assertIsNone(slot.position)
        self.assertIn(market["slug"], self.bot._slot_done)

    async def test_trailing_breakeven_exit_fires_cleanly(self):
        """When a position reaches 0.88 and pulls back to 0.61 (entry 0.60), trailing breakeven exit fires cleanly."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-trailing-breakeven-exit",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 90000.0

        # Simulate existing open position bought at 0.60
        slot.position = {
            "asset": asset,
            "slot": market["slug"],
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "price": 0.60,
            "shares": 10.0,
            "cost_usd": 6.00,
            "strike": 90000.0,
        }

        self.bot.cl_stream._record(now, 90500.0, asset=asset)

        # 1. Market best bid surges to 0.88 (>= 0.85 trigger, but < 0.95 TP target)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.88, ask=0.90, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now)

        # Position should still be active, with peak_bid=0.88 and breakeven_active=True
        self.assertIsNotNone(slot.position)
        self.assertEqual(slot.position.get("peak_bid"), 0.88)
        self.assertTrue(slot.position.get("breakeven_active"))

        # 2. Market pulls back to 0.61 (<= entry 0.60 + buffer 0.02, >= entry - 0.05)
        now_pullback = now + 1.0
        self.bot.cl_stream._record(now_pullback, 90450.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.61, ask=0.63, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now_pullback)

        # Position should now be cleanly closed via trailing breakeven exit
        self.assertIsNone(slot.position)
        self.assertIn(market["slug"], self.bot._slot_done)

    async def test_trailing_breakeven_does_not_fire_without_reaching_trigger(self):
        """Position reaching only 0.75 (< 0.85 trigger) does NOT activate breakeven stop-loss on pullback."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-no-trigger",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 90000.0

        slot.position = {
            "asset": asset,
            "slot": market["slug"],
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "price": 0.60,
            "shares": 10.0,
            "cost_usd": 6.00,
            "strike": 90000.0,
        }

        self.bot.cl_stream._record(now, 90200.0, asset=asset)
        # Bid reaches 0.75 (< 0.85)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.75, ask=0.77, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now)

        self.assertIsNotNone(slot.position)
        self.assertFalse(slot.position.get("breakeven_active", False))

        # Pullback to 0.61 should NOT trigger breakeven exit
        now2 = now + 1.0
        self.bot.cl_stream._record(now2, 90150.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.61, ask=0.63, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now2)

        self.assertIsNotNone(slot.position)

    async def test_trailing_breakeven_does_not_fire_when_disabled(self):
        """When TRAILING_BREAKEVEN_ENABLED is False, position does not trigger breakeven exit."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-disabled",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 90000.0

        slot.position = {
            "asset": asset,
            "slot": market["slug"],
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "price": 0.60,
            "shares": 10.0,
            "cost_usd": 6.00,
            "strike": 90000.0,
        }

        old_setting = config.TRAILING_BREAKEVEN_ENABLED
        try:
            config.TRAILING_BREAKEVEN_ENABLED = False
            self.bot.cl_stream._record(now, 90500.0, asset=asset)
            self.bot.book_stream._apply_update("tok_up_btc", bid=0.88, ask=0.90, bid_sz=500, ask_sz=500)
            await self.bot._tick_asset(slot, now)

            self.assertIsNotNone(slot.position)
            self.assertFalse(slot.position.get("breakeven_active", False))

            now2 = now + 1.0
            self.bot.cl_stream._record(now2, 90450.0, asset=asset)
            self.bot.book_stream._apply_update("tok_up_btc", bid=0.61, ask=0.63, bid_sz=500, ask_sz=500)
            await self.bot._tick_asset(slot, now2)

            self.assertIsNotNone(slot.position)
        finally:
            config.TRAILING_BREAKEVEN_ENABLED = old_setting

    async def test_trailing_breakeven_does_not_fire_when_bid_gaps_below_floor(self):
        """When bid gaps below entry - 0.05, trailing breakeven does not trigger (preserves option value)."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-gap-below-floor",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 90000.0

        slot.position = {
            "asset": asset,
            "slot": market["slug"],
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "price": 0.60,
            "shares": 10.0,
            "cost_usd": 6.00,
            "strike": 90000.0,
        }

        # 1. Bid reaches 0.88 -> activates breakeven
        self.bot.cl_stream._record(now, 90500.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.88, ask=0.90, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now)

        self.assertTrue(slot.position.get("breakeven_active"))

        # 2. Bid gaps down to 0.50 (< entry 0.60 - 0.05 = 0.55), but spot is still above strike (90100 > 90000)
        now2 = now + 1.0
        self.bot.cl_stream._record(now2, 90100.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.50, ask=0.55, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now2)

        # Position should still be retained (option value preserved)
        self.assertIsNotNone(slot.position)

    async def test_adverse_stop_loss_exit_on_salvageable_bid(self):
        """Trigger early stop-loss exit when spot crosses strike adversely and salvage bid >= 0.25."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-stop-loss-exit",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 90000.0

        # Position bought UP at 0.72
        slot.position = {
            "asset": asset,
            "slot": market["slug"],
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "price": 0.72,
            "shares": 10.0,
            "cost_usd": 7.20,
            "strike": 90000.0,
        }

        # Spot crashes to 89900 (< strike 90000)
        self.bot.cl_stream._record(now, 89900.0, asset=asset)
        # Salvage bid is 0.30 >= 0.25
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.30, ask=0.34, bid_sz=500, ask_sz=500)

        await self.bot._tick_asset(slot, now)

        # Capital preservation exit triggered
        self.assertIsNone(slot.position)
        self.assertIn(market["slug"], self.bot._slot_done)

    async def test_adverse_stop_loss_hold_on_low_bid(self):
        """Hold position rather than fire-selling when spot crosses strike adversely but salvage bid < 0.25."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-stop-loss-hold",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 90000.0

        slot.position = {
            "asset": asset,
            "slot": market["slug"],
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "price": 0.72,
            "shares": 10.0,
            "cost_usd": 7.20,
            "strike": 90000.0,
        }

        # Spot crashes to 89900 (< strike 90000)
        self.bot.cl_stream._record(now, 89900.0, asset=asset)
        # Salvage bid is only 0.12 (< 0.25)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.12, ask=0.18, bid_sz=500, ask_sz=500)

        await self.bot._tick_asset(slot, now)

        # Bot should HOLD the position (no fire sale at 0.12)
        self.assertIsNotNone(slot.position)

    async def test_take_profit_does_not_fire_sale_on_low_bid(self):
        """Take-profit must NOT dump position into a depressed bid when best_ask >= 0.96 but best_bid is low."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-tp-guard",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 90000.0

        slot.position = {
            "asset": asset,
            "slot": market["slug"],
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "price": 0.70,
            "shares": 10.0,
            "cost_usd": 7.00,
            "strike": 90000.0,
        }

        self.bot.cl_stream._record(now, 90500.0, asset=asset)
        # Ask is 0.96 (reaches tp_ask), but bid is only 0.20 (illiquid/wide)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.20, ask=0.96, bid_sz=500, ask_sz=500)

        await self.bot._tick_asset(slot, now)

        # Position MUST NOT be dumped at 0.20 (no fire sale disguised as take-profit)
        self.assertIsNotNone(slot.position)
        self.assertNotIn(market["slug"], self.bot._slot_done)

    async def test_spread_precision_at_exact_five_cents(self):
        """Exact 0.05 spread (e.g. 0.74 - 0.69) must pass spread gate without IEEE float failure."""
        asset = "btc"
        now = time.time()
        slot_start = now - 60.0

        market = {
            "slug": "btc-usd-5m-test-spread-precision",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 90000.0

        self.bot.cl_stream._record(slot_start, 90000.0, asset=asset)
        self.bot.cl_stream._record(now, 90150.0, asset=asset)

        # Bid 0.64, Ask 0.69 -> spread exactly 0.05 (in float: 0.69 - 0.64 = 0.050000000000000044)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.64, ask=0.69, bid_sz=500, ask_sz=500)

        mock_jev = JevDecision(
            action="proceed", passed=True, size_multiplier=1.0, conviction=2.5,
            reversal_risk=0.15, market_regime="clean_trend", confidence=0.88,
            reason="Clean breakout.", latency_ms=50.0, fallback_used=False,
        )
        self.bot.jev.evaluate_dynamic = AsyncMock(return_value=mock_jev)

        await self.bot._tick_asset(slot, now)

        self.assertIsNotNone(slot.position)
        self.assertEqual(slot.position["price"], 0.69)
        self.assertEqual(slot.position["strike"], 90000.0)

    async def test_adverse_stop_loss_preserved_position_strike(self):
        """Adverse stop-loss works using preserved position strike even if slot.strike is lost."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-lost-strike",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = None  # slot.strike cleared / ungrounded

        slot.position = {
            "asset": asset,
            "slot": market["slug"],
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "price": 0.72,
            "shares": 10.0,
            "cost_usd": 7.20,
            "strike": 90000.0,  # Preserved in position payload
        }

        # Spot crashes to 89900 (< strike 90000)
        self.bot.cl_stream._record(now, 89900.0, asset=asset)
        # Salvage bid is 0.30 >= 0.25
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.30, ask=0.34, bid_sz=500, ask_sz=500)

        await self.bot._tick_asset(slot, now)

        # Stop-loss triggered via preserved strike
        self.assertIsNone(slot.position)
        self.assertIn(market["slug"], self.bot._slot_done)

    async def test_avn_mas_strategy_alias_execution(self):
        """Bot runs dynamic momentum flow when STRATEGY='AVN_MAS'."""
        old_strat = config.STRATEGY
        try:
            config.STRATEGY = "AVN_MAS"
            asset = "btc"
            now = time.time()
            slot_start = now - 60.0

            market = {
                "slug": "btc-usd-5m-test-avn-alias",
                "slot_start_ts": slot_start,
                "start_time": slot_start,
                "end_ts": slot_start + 300.0,
                "end_time": slot_start + 300.0,
                "strike_price": 90000.0,
                "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
                "fee_rate": 0.0,
            }

            slot = self.bot.slots[asset]
            slot.market = market
            slot.strike = 90000.0

            self.bot.cl_stream._record(slot_start, 90000.0, asset=asset)
            self.bot.cl_stream._record(now, 90150.0, asset=asset)

            self.bot.book_stream._apply_update("tok_up_btc", bid=0.65, ask=0.70, bid_sz=500, ask_sz=500)

            mock_jev = JevDecision(
                action="proceed", passed=True, size_multiplier=1.0, conviction=2.5,
                reversal_risk=0.15, market_regime="clean_trend", confidence=0.88,
                reason="Clean breakout under AVN_MAS.", latency_ms=80.0, fallback_used=False,
            )
            self.bot.jev.evaluate_dynamic = AsyncMock(return_value=mock_jev)

            await self.bot._tick_asset(slot, now)

            self.assertIsNotNone(slot.position)
            self.assertEqual(slot.position["strategy"], "JEV_DYNAMIC_MOMENTUM")
            self.assertEqual(slot.position["price"], 0.70)
        finally:
            config.STRATEGY = old_strat


if __name__ == "__main__":
    unittest.main()
