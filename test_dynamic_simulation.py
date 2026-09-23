"""End-to-end paper simulation test for JEV_DYNAMIC mode in TWAPBot."""
from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import config
from bot import TWAPBot
from jev_advisor import JevDecision
from http_api import load_trades


class TestDynamicSimulation(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.old_mode = config.MODE
        self.old_strat = config.STRATEGY
        self.old_mom_min_ask = config.DYNAMIC_MOMENTUM_MIN_ASK
        self.old_mom_max_ask = config.DYNAMIC_MOMENTUM_MAX_ASK
        self.old_trailing_profit_enabled = config.TRAILING_PROFIT_ENABLED
        self.old_trailing_breakeven_enabled = config.TRAILING_BREAKEVEN_ENABLED

        config.MODE = "paper_live"
        config.STRATEGY = "JEV_DYNAMIC"
        config.TRAILING_PROFIT_ENABLED = True
        config.TRAILING_BREAKEVEN_ENABLED = True

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
        config.TRAILING_PROFIT_ENABLED = self.old_trailing_profit_enabled
        config.TRAILING_BREAKEVEN_ENABLED = self.old_trailing_breakeven_enabled
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
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-dynamic-veto",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 85000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.0,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 85000.0

        self.bot.cl_stream._record(slot_start, 85000.0, asset=asset)
        self.bot.cl_stream._record(slot_start + 60.0, 85050.0, asset=asset)
        self.bot.cl_stream._record(slot_start + 120.0, 84980.0, asset=asset)
        self.bot.cl_stream._record(now, 85060.0, asset=asset)

        self.bot.book_stream._apply_update("tok_up_btc", bid=0.68, ask=0.72, bid_sz=500, ask_sz=500)

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

        # Tier 1: 30s <= t < 90s: mult=2.0, max_ask=0.70
        allowed, min_move, max_ask, sigma_t = config.compute_dynamic_feasibility(30.0, spot, daily_vol)
        self.assertTrue(allowed)
        self.assertEqual(max_ask, 0.70)
        expected_sigma_30 = daily_vol * ((30.0 / 86400.0) ** 0.5) * spot
        self.assertAlmostEqual(sigma_t, expected_sigma_30, places=4)
        self.assertAlmostEqual(min_move, 2.0 * expected_sigma_30, places=4)

        allowed, min_move, max_ask, _ = config.compute_dynamic_feasibility(89.9, spot, daily_vol)
        self.assertTrue(allowed)
        self.assertEqual(max_ask, 0.70)

        # Tier 2: 90s <= t < 150s: mult=1.5, max_ask=0.76
        allowed, min_move, max_ask, sigma_t = config.compute_dynamic_feasibility(90.0, spot, daily_vol)
        self.assertTrue(allowed)
        self.assertEqual(max_ask, 0.76)
        expected_sigma_90 = daily_vol * ((90.0 / 86400.0) ** 0.5) * spot
        self.assertAlmostEqual(min_move, 1.5 * expected_sigma_90, places=4)

        # Tier 3: 150s <= t < 240s: mult=1.2, max_ask=0.80
        allowed, min_move, max_ask, sigma_t = config.compute_dynamic_feasibility(150.0, spot, daily_vol)
        self.assertTrue(allowed)
        self.assertEqual(max_ask, 0.80)
        expected_sigma_150 = daily_vol * ((150.0 / 86400.0) ** 0.5) * spot
        self.assertAlmostEqual(min_move, 1.2 * expected_sigma_150, places=4)

        # Tier 3 (continued at 210s): mult=1.2, max_ask=0.80
        allowed, min_move, max_ask, sigma_t = config.compute_dynamic_feasibility(210.0, spot, daily_vol)
        self.assertTrue(allowed)
        self.assertEqual(max_ask, 0.80)
        expected_sigma_210 = daily_vol * ((210.0 / 86400.0) ** 0.5) * spot
        self.assertAlmostEqual(min_move, 1.2 * expected_sigma_210, places=4)

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

    async def test_dynamic_trailing_profit_stop_exit(self):
        """Entry 0.60, peak 0.80, drops to 0.72 -> triggers trailing profit exit at 0.72 with positive PnL."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-trailing-profit-exit",
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

        # 1. Market best bid rises to 0.80 (>= TRAILING_PROFIT_MIN_PEAK 0.78)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.80, ask=0.82, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now)

        # Position should still be active, with peak_bid=0.80 and trailing_active=True
        self.assertIsNotNone(slot.position)
        self.assertEqual(slot.position.get("peak_bid"), 0.80)
        self.assertTrue(slot.position.get("trailing_active"))

        # Intermediate price 0.73 (> stop_price 0.72) does NOT trigger exit
        now_inter = now + 1.0
        self.bot.cl_stream._record(now_inter, 90480.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.73, ask=0.75, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now_inter)
        self.assertIsNotNone(slot.position)

        # 2. Market best bid drops to 0.72 (<= 0.80 - 0.08 = 0.72 stop_price) -> triggers trailing profit exit
        initial_bankroll = self.bot.risk.bankroll
        now_exit = now + 2.0
        self.bot.cl_stream._record(now_exit, 90450.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.72, ask=0.74, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now_exit)

        # Position should now be cleanly closed via trailing profit exit
        self.assertIsNone(slot.position)
        self.assertIn(market["slug"], self.bot._slot_done)

        # Verify positive PnL: entry 0.60, exit 0.72 on 10 shares -> +$1.20 profit
        expected_pnl = 10.0 * (0.72 - 0.60)
        self.assertAlmostEqual(self.bot.risk.bankroll, initial_bankroll + expected_pnl, places=2)
        self.assertGreater(self.bot.risk.bankroll, initial_bankroll)

    async def test_dynamic_trailing_profit_does_not_arm_below_min_peak(self):
        """When peak reaches 0.77 (< 0.78 min peak), trailing profit stop does not arm and does not exit on 8c drop."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-tp-below-peak",
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

        self.bot.cl_stream._record(now, 90500.0, asset=asset)
        # Bid reaches 0.77 (< 0.78 min peak)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.77, ask=0.79, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now)

        self.assertIsNotNone(slot.position)
        self.assertFalse(slot.position.get("trailing_active", False))

        # Drops 8c down to 0.69: should NOT exit because trailing stop was never armed
        now2 = now + 1.0
        self.bot.cl_stream._record(now2, 90450.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.69, ask=0.71, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now2)

        self.assertIsNotNone(slot.position)

    async def test_dynamic_trailing_profit_fee_aware_breakeven_protection(self):
        """When peak is 0.78 and entry is 0.72 with fees, stop_price guarantees breakeven+fees."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-tp-fee-protection",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.07,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 90000.0

        # Entry 0.72, fee_rate 0.07: round_trip_fee = 2 * 0.07 * 0.72 * 0.28 = 0.0282
        # entry + fee = 0.7482. Peak 0.78 - 0.08 = 0.70 < 0.7482.
        # stop_price clamped to 0.7482 (guaranteeing breakeven+fees).
        slot.position = {
            "asset": asset,
            "slot": market["slug"],
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "price": 0.72,
            "shares": 10.0,
            "cost_usd": 7.20,
            "fee": 0.141,
            "strike": 90000.0,
        }

        self.bot.cl_stream._record(now, 90500.0, asset=asset)
        # Peak reaches 0.78
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.78, ask=0.80, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now)

        self.assertIsNotNone(slot.position)
        self.assertTrue(slot.position.get("trailing_active"))

        # At bid 0.75 (> 0.7482 stop_price), position should NOT exit
        now2 = now + 1.0
        self.bot.cl_stream._record(now2, 90480.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.75, ask=0.77, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now2)
        self.assertIsNotNone(slot.position)

        # At bid 0.74 (<= 0.7482 stop_price), position exits to protect breakeven+fees
        now3 = now + 2.0
        self.bot.cl_stream._record(now3, 90450.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.74, ask=0.76, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now3)
        self.assertIsNone(slot.position)
        self.assertIn(market["slug"], self.bot._slot_done)

    async def test_dynamic_trailing_profit_high_entry_price(self):
        """For positions entered at high prices (e.g. 0.80), trailing profit only arms above breakeven+fees and does not deadlock."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-tp-high-entry",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.07,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 90000.0

        # Entry 0.80: round_trip_fee = 2 * 0.07 * 0.80 * 0.20 = 0.0224
        # min_breakeven = 0.8224. min_peak = max(0.78, 0.8324) = 0.8324.
        slot.position = {
            "asset": asset,
            "slot": market["slug"],
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "price": 0.80,
            "shares": 10.0,
            "cost_usd": 8.00,
            "fee": 0.112,
            "strike": 90000.0,
        }

        self.bot.cl_stream._record(now, 90500.0, asset=asset)
        # Bid reaches 0.81 (< min_peak 0.8324): should NOT arm trailing profit
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.81, ask=0.83, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now)
        self.assertIsNotNone(slot.position)
        self.assertFalse(slot.position.get("trailing_active", False))

        # Drops to 0.76: does not exit because trailing stop was not armed
        now2 = now + 1.0
        self.bot.cl_stream._record(now2, 90480.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.76, ask=0.78, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now2)
        self.assertIsNotNone(slot.position)

        # Surges to 0.86 (>= min_peak 0.8324): arms trailing profit
        now3 = now + 2.0
        self.bot.cl_stream._record(now3, 90550.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.86, ask=0.88, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now3)
        self.assertIsNotNone(slot.position)
        self.assertTrue(slot.position.get("trailing_active"))

        # Peak is 0.86: stop_price = max(0.86 - 0.08, 0.8224) = 0.8224
        # Bid 0.84 (> 0.8224): should hold
        now4 = now + 3.0
        self.bot.cl_stream._record(now4, 90530.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.84, ask=0.86, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now4)
        self.assertIsNotNone(slot.position)

        # Bid pulls back to 0.82 (<= stop_price 0.8224 and >= floor 0.75): triggers exit to protect breakeven
        now5 = now + 4.0
        self.bot.cl_stream._record(now5, 90500.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.82, ask=0.84, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now5)
        self.assertIsNone(slot.position)
        self.assertIn(market["slug"], self.bot._slot_done)

    async def test_dynamic_trailing_profit_disabled_by_config(self):
        """When TRAILING_PROFIT_ENABLED is False, position does not trigger trailing profit exit."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-tp-disabled-config",
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

        old_setting = config.TRAILING_PROFIT_ENABLED
        try:
            config.TRAILING_PROFIT_ENABLED = False
            self.bot.cl_stream._record(now, 90500.0, asset=asset)
            self.bot.book_stream._apply_update("tok_up_btc", bid=0.80, ask=0.82, bid_sz=500, ask_sz=500)
            await self.bot._tick_asset(slot, now)

            self.assertIsNotNone(slot.position)
            self.assertFalse(slot.position.get("trailing_active", False))

            # Pullback to 0.72 should NOT trigger exit when feature is disabled
            now2 = now + 1.0
            self.bot.cl_stream._record(now2, 90450.0, asset=asset)
            self.bot.book_stream._apply_update("tok_up_btc", bid=0.72, ask=0.74, bid_sz=500, ask_sz=500)
            await self.bot._tick_asset(slot, now2)

            self.assertIsNotNone(slot.position)
        finally:
            config.TRAILING_PROFIT_ENABLED = old_setting

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

    async def test_trailing_breakeven_fee_aware_exit(self):
        """Trailing breakeven dynamically accounts for round-trip Polymarket taker fees (0.07*P*(1-P))."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-fee-aware",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.07,  # Standard Polymarket crypto taker fee rate
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
            "fee": 0.168,  # Entry fee = 10 * 0.07 * 0.60 * 0.40 = 0.168
            "strike": 90000.0,
        }

        # 1. Surge to 0.88 -> activates breakeven
        self.bot.cl_stream._record(now, 90500.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.88, ask=0.90, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now)

        self.assertTrue(slot.position.get("breakeven_active"))

        # With entry=0.60 and fee_rate=0.07:
        # round_trip_fee = 2 * 0.07 * 0.60 * 0.40 = 0.0336
        # buffer = 0.0336 + 0.01 = 0.0436 -> trigger price = 0.6436
        # Pullback to 0.64 (<= 0.6436) MUST trigger early exit to preserve net capital after fees
        now2 = now + 1.0
        self.bot.cl_stream._record(now2, 90450.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.64, ask=0.66, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now2)

        self.assertIsNone(slot.position)
        self.assertIn(market["slug"], self.bot._slot_done)

    async def test_trailing_breakeven_live_partial_fill_handling(self):
        """In live mode, partial fills update position state without dropping remaining shares."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-partial-fill",
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
            "fee": 0.0,
            "strike": 90000.0,
        }

        # Mock live client with partial fill on first attempt (4.0 filled out of 10.0)
        mock_live = MagicMock()
        mock_live.place_order.side_effect = [
            {"success": True, "filled": True, "shares": 4.0, "status": "matched", "order_id": "ord_partial_1"},
            {"success": True, "filled": True, "shares": 6.0, "status": "matched", "order_id": "ord_full_2"},
        ]
        self.bot.live_client = mock_live
        old_mode = config.MODE
        config.MODE = "live"

        try:
            # 1. Activate breakeven at 0.88
            self.bot.cl_stream._record(now, 90500.0, asset=asset)
            self.bot.book_stream._apply_update("tok_up_btc", bid=0.88, ask=0.90, bid_sz=500, ask_sz=500)
            await self.bot._tick_asset(slot, now)
            self.assertTrue(slot.position.get("breakeven_active"))

            # 2. Pullback to 0.61 triggers exit, but book only fills 4 shares
            now2 = now + 1.0
            self.bot.cl_stream._record(now2, 90450.0, asset=asset)
            self.bot.book_stream._apply_update("tok_up_btc", bid=0.61, ask=0.63, bid_sz=500, ask_sz=500)
            await self.bot._tick_asset(slot, now2)

            # Position MUST NOT be None: 6 shares remain!
            self.assertIsNotNone(slot.position)
            self.assertEqual(slot.position["shares"], 6.0)
            self.assertEqual(slot.position["cost_usd"], 3.60)
            self.assertFalse(slot.position.get("exiting", False))

            # 3. Next tick exits remaining 6 shares
            now3 = now2 + 1.0
            self.bot.cl_stream._record(now3, 90450.0, asset=asset)
            await self.bot._tick_asset(slot, now3)

            # Now position is fully closed
            self.assertIsNone(slot.position)
            self.assertIn(market["slug"], self.bot._slot_done)
        finally:
            config.MODE = old_mode
            self.bot.live_client = None

    async def test_trailing_breakeven_runtime_disabled_after_activation(self):
        """Disabling TRAILING_BREAKEVEN_ENABLED at runtime prevents breakeven exit even if previously activated."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-runtime-disable",
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

        # 1. Activate breakeven at 0.88
        self.bot.cl_stream._record(now, 90500.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.88, ask=0.90, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now)
        self.assertTrue(slot.position.get("breakeven_active"))

        # 2. Disable TRAILING_BREAKEVEN_ENABLED at runtime
        old_setting = config.TRAILING_BREAKEVEN_ENABLED
        try:
            config.TRAILING_BREAKEVEN_ENABLED = False

            # Pullback to 0.61 should NOT trigger exit when feature is disabled
            now2 = now + 1.0
            self.bot.cl_stream._record(now2, 90450.0, asset=asset)
            self.bot.book_stream._apply_update("tok_up_btc", bid=0.61, ask=0.63, bid_sz=500, ask_sz=500)
            await self.bot._tick_asset(slot, now2)

            self.assertIsNotNone(slot.position)
        finally:
            config.TRAILING_BREAKEVEN_ENABLED = old_setting

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

        # Tick 1: Adverse condition detected, monitoring started (filters momentary wicks)
        await self.bot._tick_asset(slot, now)
        self.assertIsNotNone(slot.position)
        self.assertIsNotNone(slot.position.get("adverse_start_ts"))

        # Tick 2: Sustained past 10s persistence threshold -> confirms genuine reversal and triggers exit
        now2 = now + 11.0
        self.bot.cl_stream._record(now2, 89890.0, asset=asset)
        await self.bot._tick_asset(slot, now2)

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

        # Tick 1: Starts monitoring
        await self.bot._tick_asset(slot, now)
        self.assertIsNotNone(slot.position)

        # Tick 2: Sustained past 10s persistence hurdle -> triggers exit
        now2 = now + 11.0
        self.bot.cl_stream._record(now2, 89890.0, asset=asset)
        await self.bot._tick_asset(slot, now2)

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

    async def test_adverse_stop_loss_ignores_momentary_wick_and_resets(self):
        """A momentary wick below strike does NOT trigger stop-loss, and adverse timer resets when spot recovers."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-wick-filtering",
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

        # Spot momentarily dips to 89900 (< 90000)
        self.bot.cl_stream._record(now, 89900.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.30, ask=0.34, bid_sz=500, ask_sz=500)

        # Tick 1: Adverse condition detected, monitoring started (position NOT dumped)
        await self.bot._tick_asset(slot, now)
        self.assertIsNotNone(slot.position)
        self.assertIsNotNone(slot.position.get("adverse_start_ts"))

        # Spot recovers above strike to 90050 at now + 3s (before 10s persistence hurdle)
        now2 = now + 3.0
        self.bot.cl_stream._record(now2, 90050.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.65, ask=0.69, bid_sz=500, ask_sz=500)
        await self.bot._tick_asset(slot, now2)

        # Position is STILL retained and timer has reset to None!
        self.assertIsNotNone(slot.position)
        self.assertIsNone(slot.position.get("adverse_start_ts"))

    async def test_trailing_profit_disabled_by_default_lets_trade_reach_tp(self):
        """When trailing profit is disabled (production default), pullback from 0.88 to 0.77 does NOT exit, allowing trade to hit 0.96 Take-Profit."""
        asset = "btc"
        now = time.time()
        slot_start = now - 180.0

        market = {
            "slug": "btc-usd-5m-test-tp-reach-win",
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
            "price": 0.78,
            "shares": 10.0,
            "cost_usd": 7.80,
            "strike": 90000.0,
        }

        old_trailing = config.TRAILING_PROFIT_ENABLED
        old_breakeven = config.TRAILING_BREAKEVEN_ENABLED
        try:
            config.TRAILING_PROFIT_ENABLED = False
            config.TRAILING_BREAKEVEN_ENABLED = False

            # Peak reaches 0.88
            self.bot.cl_stream._record(now, 90300.0, asset=asset)
            self.bot.book_stream._apply_update("tok_up_btc", bid=0.88, ask=0.90, bid_sz=500, ask_sz=500)
            await self.bot._tick_asset(slot, now)
            self.assertIsNotNone(slot.position)

            # Bid pulls back to 0.77 (8c below peak, 1c below entry): with trailing stop disabled, trade is NOT choked
            now2 = now + 5.0
            self.bot.cl_stream._record(now2, 90200.0, asset=asset)
            self.bot.book_stream._apply_update("tok_up_btc", bid=0.77, ask=0.80, bid_sz=500, ask_sz=500)
            await self.bot._tick_asset(slot, now2)
            self.assertIsNotNone(slot.position)

            # Price surges towards expiry, best_bid reaches 0.96 -> Take-Profit triggers cleanly!
            now3 = now + 20.0
            self.bot.cl_stream._record(now3, 90500.0, asset=asset)
            self.bot.book_stream._apply_update("tok_up_btc", bid=0.96, ask=0.97, bid_sz=500, ask_sz=500)
            await self.bot._tick_asset(slot, now3)

            # Successfully exited at Take-Profit 0.96
            self.assertIsNone(slot.position)
            self.assertIn(market["slug"], self.bot._slot_done)
        finally:
            config.TRAILING_PROFIT_ENABLED = old_trailing
            config.TRAILING_BREAKEVEN_ENABLED = old_breakeven


    async def test_dynamic_feasibility_caps_at_80_cents(self):
        """Verify that dynamic feasibility never allows ask > 0.80 across any time tier."""
        spot = 90000.0
        daily_vol = 0.0106
        for t in [30.0, 60.0, 89.9, 90.0, 120.0, 149.9, 150.0, 180.0, 210.0, 239.9]:
            allowed, _, max_ask, _ = config.compute_dynamic_feasibility(t, spot, daily_vol)
            self.assertTrue(allowed)
            self.assertLessEqual(max_ask, 0.80)
            self.assertLessEqual(max_ask, config.DYNAMIC_MOMENTUM_MAX_ASK)

    async def test_stop_loss_triggers_when_spot_adverse_even_if_twap_lags(self):
        """In late round (t >= 180s, remaining <= 120s), spot governs expiry so stop-loss fires even if TWAP lags."""
        asset = "btc"
        now = time.time()
        slot_start = now - 240.0  # t=240s elapsed, 60s remaining

        market = {
            "slug": "btc-usd-5m-test-twap-lag-bypass",
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
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "price": 0.75,
            "shares": 10.0,
            "cost_usd": 7.50,
            "strike": 90000.0,
        }

        # Spot was up early, so TWAP is lagging high (~90100 > strike 90000)
        self.bot.cl_stream._record(slot_start, 90000.0, asset=asset)
        self.bot.cl_stream._record(slot_start + 60.0, 90150.0, asset=asset)
        self.bot.cl_stream._record(slot_start + 180.0, 90150.0, asset=asset)
        # Spot crashes to 89920 at t=240s (-80 below strike)
        self.bot.cl_stream._record(now, 89920.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.30, ask=0.34, bid_sz=500, ask_sz=500)

        # Tick 1: Starts adverse timer
        await self.bot._tick_asset(slot, now)
        self.assertIsNotNone(slot.position)
        self.assertIsNotNone(slot.position.get("adverse_start_ts"))

        # Tick 2: 11 seconds later, spot is still down at 89910. Stop loss must fire despite high TWAP!
        now2 = now + 11.0
        self.bot.cl_stream._record(now2, 89910.0, asset=asset)
        await self.bot._tick_asset(slot, now2)

        self.assertIsNone(slot.position)
        self.assertIn(market["slug"], self.bot._slot_done)

    async def test_stop_loss_timer_does_not_reset_on_minor_bounce_below_strike(self):
        """Minor bounces while spot is still deep in adverse territory must NOT reset the persistence timer."""
        asset = "btc"
        now = time.time()
        slot_start = now - 240.0

        market = {
            "slug": "btc-usd-5m-test-bounce-retention",
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
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "price": 0.75,
            "shares": 10.0,
            "cost_usd": 7.50,
            "strike": 90000.0,
        }

        # Spot crashes to 89925 (-75 below strike)
        self.bot.cl_stream._record(now, 89925.0, asset=asset)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.30, ask=0.34, bid_sz=500, ask_sz=500)

        # Tick 1: Timer starts
        await self.bot._tick_asset(slot, now)
        start_ts = slot.position.get("adverse_start_ts")
        self.assertIsNotNone(start_ts)

        # Tick 2: 5s later, spot bounces slightly to 89940 (-60 below strike). Still adverse!
        now2 = now + 5.0
        self.bot.cl_stream._record(now2, 89940.0, asset=asset)
        await self.bot._tick_asset(slot, now2)

        # Timer must NOT have been wiped to None!
        self.assertIsNotNone(slot.position)
        self.assertEqual(slot.position.get("adverse_start_ts"), start_ts)

        # Tick 3: 11s from start, spot is 89935. Timer has completed >= 10s -> early exit triggers!
        now3 = now + 11.0
        self.bot.cl_stream._record(now3, 89935.0, asset=asset)
        await self.bot._tick_asset(slot, now3)

        self.assertIsNone(slot.position)
        self.assertIn(market["slug"], self.bot._slot_done)

    async def test_late_round_urgent_salvage_in_final_45s(self):
        """In final 45s, adverse spot triggers urgent salvage exit after only 3s if salvage bid >= 0.20."""
        asset = "btc"
        now = time.time()
        slot_start = now - 260.0  # 260s elapsed, 40s remaining (<= 45s)

        market = {
            "slug": "btc-usd-5m-test-urgent-salvage",
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
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "price": 0.75,
            "shares": 10.0,
            "cost_usd": 7.50,
            "strike": 90000.0,
        }

        # Spot is adverse at 89910 (-90 below strike)
        self.bot.cl_stream._record(now, 89910.0, asset=asset)
        # Bid is 0.22 (below regular 0.25 salvage min, but >= 0.20 urgent salvage min!)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.22, ask=0.26, bid_sz=500, ask_sz=500)

        # Tick 1: Starts adverse timer
        await self.bot._tick_asset(slot, now)
        self.assertIsNotNone(slot.position)
        self.assertIsNotNone(slot.position.get("adverse_start_ts"))

        # Tick 2: 3.5s later (>= 3.0s urgent persistence hurdle) -> triggers urgent salvage!
        now2 = now + 3.5
        self.bot.cl_stream._record(now2, 89910.0, asset=asset)
        await self.bot._tick_asset(slot, now2)

        # Successfully salvages capital instead of taking total loss
        self.assertIsNone(slot.position)
        self.assertIn(market["slug"], self.bot._slot_done)

    async def test_window_timing_and_exit_records(self):
        """Verify entry_window_time, exit_window_time, exit_price, exit_reason and fees in records."""
        asset = "btc"
        now = time.time()
        slot_start = now - 250.0

        market = {
            "slug": "btc-usd-5m-test-timing-audit",
            "slot_start_ts": slot_start,
            "start_time": slot_start,
            "end_ts": slot_start + 300.0,
            "end_time": slot_start + 300.0,
            "strike_price": 90000.0,
            "tokens": {"UP": "tok_up_btc", "DOWN": "tok_down_btc"},
            "fee_rate": 0.05,
        }

        slot = self.bot.slots[asset]
        slot.market = market
        slot.strike = 90000.0
        slot.position = {
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "opp_token_id": "tok_down_btc",
            "price": 0.65,
            "shares": 5.0,
            "cost_usd": 3.25,
            "fee": 0.08,
            "total_cost": 3.33,
            "strike": 90000.0,
            "slot_start_ts": slot_start,
            "entry_time": slot_start + 42.0,
            "entry_elapsed_s": 42.0,
            "entry_window_time": "t=42s (00:42)",
        }

        # 1) Trigger early exit (Take-Profit @ 0.96)
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.96, ask=0.98, bid_sz=500, ask_sz=500)
        await self.bot._manage_open_momentum_position(
            slot, market, remaining_s=50.0, tokens=market["tokens"],
            spot=90100.0, strike=90000.0, elapsed_s=250.0, now=now,
        )

        self.assertIsNone(slot.position)
        # Check trades recorded
        loaded_trades = load_trades(self.bot._trades_log_path)
        self.assertTrue(len(loaded_trades) > 0)
        tp_trade = loaded_trades[-1]
        self.assertEqual(tp_trade["exit_price"], 0.96)
        self.assertEqual(tp_trade["exit_reason"], "take_profit")
        self.assertEqual(tp_trade["entry_elapsed_s"], 42.0)
        self.assertEqual(tp_trade["entry_window_time"], "t=42s (00:42)")
        self.assertIsNotNone(tp_trade.get("exit_elapsed_s"))
        self.assertIsNotNone(tp_trade.get("exit_window_time"))
        self.assertEqual(tp_trade["cost_usd"], 3.25)
        self.assertEqual(tp_trade["fee"], 0.08)
        self.assertAlmostEqual(tp_trade["total_cost"], 3.25 + 0.08 + tp_trade.get("exit_fee", 0.0), places=3)

        # 2) Test settlement record (loss settlement)
        loss_item = {
            "asset": "btc",
            "market": market,
            "strike": 90000.0,
            "position": {
                "strategy": "JEV_DYNAMIC_MOMENTUM",
                "side": "UP",
                "token_id": "tok_up_btc",
                "price": 0.65,
                "shares": 5.0,
                "cost_usd": 3.25,
                "fee": 0.08,
                "slot_start_ts": slot_start,
                "entry_time": slot_start + 30.0,
                "entry_elapsed_s": 30.0,
                "entry_window_time": "t=30s (00:30)",
            },
        }
        loss_res = {
            "side": "DOWN",
            "source": "polymarket_outcome_prices",
            "official": True,
            "price_to_beat": 90000.0,
            "final_price": 89900.0,
        }
        self.bot._settle_filled(loss_item, loss_res)

        loaded_trades2 = load_trades(self.bot._trades_log_path)
        settle_trade = loaded_trades2[-1]
        self.assertEqual(settle_trade["won"], False)
        self.assertEqual(settle_trade["exit_price"], 0.0)
        self.assertEqual(settle_trade["exit_reason"], "settlement_lost")
        self.assertEqual(settle_trade["entry_window_time"], "t=30s (00:30)")
        self.assertEqual(settle_trade["exit_window_time"], "t=300s (05:00 - expiry)")
        self.assertEqual(settle_trade["fee"], 0.08)
        self.assertEqual(settle_trade["total_cost"], 3.33)
        self.assertAlmostEqual(settle_trade["pnl"], -3.33, places=2)

    async def test_anti_settlement_zero_hurdle_urgent_salvage(self):
        """Verify that within 45s, a small adverse move (e.g. $5 on BTC) triggers urgent salvage at low bids (e.g. 0.12)."""
        asset = "btc"
        now = time.time()
        slot_start = now - 265.0  # 35s remaining
        market = {
            "slug": "btc-usd-5m-anti-settle-test-urgent",
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
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "opp_token_id": "tok_down_btc",
            "price": 0.65,
            "shares": 5.0,
            "cost_usd": 3.25,
            "fee": 0.08,
            "strike": 90000.0,
            "slot_start_ts": slot_start,
            "entry_time": slot_start + 30.0,
            "entry_elapsed_s": 30.0,
            "entry_window_time": "t=30s (00:30)",
            "adverse_start_ts": now - 4.0,  # sustained for 4s >= urgent persistence (3s)
        }

        # Spot is only $5 below strike (89,995 vs 90,000). Standard 0.50-sigma hurdle would be ~$80.
        # But remaining_s = 35s <= 45s, so zero-hurdle applies (min_adverse_usd = 0.0)!
        # Book bid is 0.12 (below old 0.20 floor, but above new 0.05 urgent salvage floor).
        self.bot.book_stream._apply_update("tok_up_btc", bid=0.12, ask=0.16, bid_sz=500, ask_sz=500)
        await self.bot._manage_open_momentum_position(
            slot, market, remaining_s=35.0, tokens=market["tokens"],
            spot=89995.0, strike=90000.0, elapsed_s=265.0, now=now,
        )

        self.assertIsNone(slot.position, "Position should have been cleanly salvaged")
        loaded = load_trades(self.bot._trades_log_path)
        trade = loaded[-1]
        self.assertEqual(trade["exit_reason"], "urgent_stop_loss")
        self.assertEqual(trade["exit_price"], 0.12)
        self.assertAlmostEqual(trade["pnl"], (5.0 * 0.12) - 3.25 - 0.08, places=2)

    async def test_anti_settlement_hard_flatten_panic_salvage(self):
        """Verify that within 20s, adverse move immediately flattens with 0 persistence even at 0.04 implied bid."""
        asset = "btc"
        now = time.time()
        slot_start = now - 285.0  # 15s remaining
        market = {
            "slug": "btc-usd-5m-anti-settle-test-hard-flatten",
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
            "strategy": "JEV_DYNAMIC_MOMENTUM",
            "side": "UP",
            "token_id": "tok_up_btc",
            "opp_token_id": "tok_down_btc",
            "price": 0.65,
            "shares": 5.0,
            "cost_usd": 3.25,
            "fee": 0.08,
            "strike": 90000.0,
            "slot_start_ts": slot_start,
            "entry_time": slot_start + 30.0,
            "entry_elapsed_s": 30.0,
            "entry_window_time": "t=30s (00:30)",
            # adverse_start_ts is None! Reversal just happened this very millisecond
        }

        # Spot reversed to 89,990.
        # Direct bid on UP is None (market makers pulled bids).
        # Opposite DOWN token ask is 0.96 -> implied bid on UP is 1.0 - 0.96 = 0.04!
        # With 15s remaining <= 20s (hard flatten), persistence is 0s and salvage min is 0.02.
        self.bot.book_stream._apply_update("tok_up_btc", bid=None, ask=None, bid_sz=0, ask_sz=0)
        self.bot.book_stream._apply_update("tok_down_btc", bid=0.94, ask=0.96, bid_sz=500, ask_sz=500)

        await self.bot._manage_open_momentum_position(
            slot, market, remaining_s=15.0, tokens=market["tokens"],
            spot=89990.0, strike=90000.0, elapsed_s=285.0, now=now,
        )

        self.assertIsNone(slot.position, "Position should have been immediately flattened before expiry")
        loaded = load_trades(self.bot._trades_log_path)
        trade = loaded[-1]
        self.assertEqual(trade["exit_reason"], "hard_flatten_expiry")
        self.assertEqual(trade["exit_price"], 0.04)


if __name__ == "__main__":
    unittest.main()

