"""Unit tests for maker_guard (JEV_DYNAMIC maker entry guards)."""
from __future__ import annotations
import maker_guard as mg


def test_check_quote_fallback_without_twap():
    ok, reason, z, ref = mg.check_quote("UP", 1.4, None, 0.25)
    assert ok and reason is None
    assert z == 1.4 and ref == mg.REF_SPOT_FALLBACK


def test_check_quote_direction_conflict_blocks():
    ok, reason, z, ref = mg.check_quote("UP", 1.2, -0.3, 0.25)
    assert not ok and reason == mg.REASON_DIRECTION_CONFLICT
    assert z == -0.3 and ref == mg.REF_TWAP


def test_check_quote_zero_twap_is_conflict():
    ok, reason, _, _ = mg.check_quote("UP", 1.2, 0.0, 0.25)
    assert not ok and reason == mg.REASON_DIRECTION_CONFLICT


def test_check_quote_twap_sigma_too_small_blocks():
    ok, reason, z, ref = mg.check_quote("UP", 1.5, 0.10, 0.25)
    assert not ok and reason == mg.REASON_TWAP_SIGMA
    assert z == 0.10 and ref == mg.REF_TWAP


def test_check_quote_passes_and_uses_less_favourable():
    ok, reason, z, ref = mg.check_quote("UP", 1.5, 0.8, 0.25)
    assert ok and reason is None and z == 0.8 and ref == mg.REF_TWAP
    ok, reason, z, ref = mg.check_quote("UP", 1.1, 1.6, 0.25)
    assert ok and z == 1.1 and ref == mg.REF_SPOT


def test_check_quote_down_side_signs():
    ok, reason, z, _ = mg.check_quote("DOWN", -1.3, -0.6, 0.25)
    assert ok and reason is None and abs(z - 0.6) < 1e-12
    ok, reason, _, _ = mg.check_quote("DOWN", -1.3, -0.2, 0.25)
    assert not ok and reason == mg.REASON_TWAP_SIGMA
    ok, reason, _, _ = mg.check_quote("DOWN", -1.3, 0.4, 0.25)
    assert not ok and reason == mg.REASON_DIRECTION_CONFLICT


def test_twap_cross_adverse():
    assert mg.twap_cross_adverse("UP", 99.0, 100.0)
    assert not mg.twap_cross_adverse("UP", 101.0, 100.0)
    assert mg.twap_cross_adverse("DOWN", 101.0, 100.0)
    assert not mg.twap_cross_adverse("DOWN", 99.0, 100.0)
    assert not mg.twap_cross_adverse("UP", 99.0, None)
    assert not mg.twap_cross_adverse("UP", 99.0, 0.0)


def test_mid_history_high_and_drop_window():
    h = mg.MidHistory(window_s=60)
    h.add("tok", 0.0, 0.70)
    h.add("tok", 30.0, 0.60)
    h.add("tok", 50.0, 0.52)
    hi, drop = h.drop("tok", 50.0, 0.52)
    assert hi == 0.70 and abs(drop - 0.18) < 1e-9
    assert mg.mid_drop_blocked(drop, 0.15)
    # 0.70 point ages out of the 60s window
    h.add("tok", 75.0, 0.50)
    hi, drop = h.drop("tok", 75.0, 0.50)
    assert hi == 0.60 and abs(drop - 0.10) < 1e-9
    assert not mg.mid_drop_blocked(drop, 0.15)


def test_mid_history_no_data_and_bad_input():
    h = mg.MidHistory(window_s=60)
    assert h.drop("tok", 10.0, 0.5) == (None, None)
    h.add(None, 1.0, 0.5)
    h.add("tok", 1.0, None)
    h.add("tok", 1.0, float("nan"))
    assert h.high("tok", 1.0) is None
    assert not mg.mid_drop_blocked(None, 0.15)


def test_mid_drop_threshold_edge_and_disabled():
    assert mg.mid_drop_blocked(0.15, 0.15)
    assert not mg.mid_drop_blocked(0.149, 0.15)
    assert not mg.mid_drop_blocked(0.5, 0.0)


def test_mid_history_rise_is_not_a_drop():
    h = mg.MidHistory(window_s=60)
    h.add("tok", 0.0, 0.45)
    h.add("tok", 10.0, 0.55)
    hi, drop = h.drop("tok", 10.0, 0.55)
    assert hi == 0.55 and drop == 0.0


# ---------------------------------------------------------------- bot integration (resting maker bid)
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

import config
from bot import TWAPBot
from jev_advisor import JevDecision

STRIKE = 84300.0


class TestMakerGuardBotIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.saved = {k: getattr(config, k) for k in (
            "MODE", "STRATEGY", "MAKER_TWAP_GUARD", "MAKER_MIN_TWAP_SIGMA", "MAKER_MAX_MID_DROP_60S")}
        config.MODE = "paper_live"
        config.STRATEGY = "JEV_DYNAMIC"
        config.MAKER_TWAP_GUARD = True
        config.MAKER_MIN_TWAP_SIGMA = 0.25
        config.MAKER_MAX_MID_DROP_60S = 0.15
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.temp_dir
        self.bot.risk.bankroll = 50.0
        self.bot._slot_done = set()
        self.bot._trades_log_path = self.temp_dir / "trades.json"
        self.bot._save_slot_state = lambda **kw: None
        self.bot.jev.evaluate_dynamic = AsyncMock(return_value=JevDecision(
            action="abstain", passed=False, size_multiplier=0.0, conviction=0.5, reversal_risk=0.5,
            market_regime="chop", confidence=0.2, reason="test veto", latency_ms=1.0, fallback_used=False,
        ))

    async def asyncTearDown(self):
        for k, v in self.saved.items():
            setattr(config, k, v)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def _run(self, spot: float, price_to_beat, seed_high: float | None = None):
        now = time.time()
        slot_start = now - 150.0
        slot = self.bot.slots["btc"]
        slot.market = {
            "slug": f"btc-updown-5m-test-maker-{spot}-{price_to_beat}-{seed_high}",
            "slot_start_ts": slot_start,
            "end_ts": slot_start + 300.0,
            "tokens": {"UP": "tok_up", "DOWN": "tok_down"},
            "fee_rate": 0.0,
            "price_to_beat": price_to_beat,
        }
        slot.strike = STRIKE
        slot.position = None
        cl = self.bot.cl_stream
        cl._record(slot_start, STRIKE, asset="btc")
        cl._record(slot_start + 60.0, spot, asset="btc")
        cl._record(slot_start + 120.0, spot, asset="btc")
        cl._record(now, spot, asset="btc")
        cl._last_tick_time["btc"] = time.time()
        # Ask trades down through our 0.50 bid -> CLOB paper fill unless a guard blocks it.
        self.bot.book_stream._apply_update("tok_up", bid=0.48, ask=0.50, bid_sz=500, ask_sz=500)
        self.bot.book_stream._apply_update("tok_down", bid=0.50, ask=0.52, bid_sz=500, ask_sz=500)
        if seed_high is not None:
            self.bot._mid_hist.add("tok_up", now - 20.0, seed_high)
        slot.resting_order = {
            "asset": "btc", "slot": slot.market["slug"], "side": "UP", "token_id": "tok_up",
            "price": 0.50, "shares": 6.0, "cost_usd": 3.0, "z_score": 1.2, "prob": 0.5,
            "rebate": 0.0, "placed_at": now - 30.0, "order_id": "maker_paper_test",
            "jev": {"conviction": 1.9}, "strike_spot": STRIKE, "strike_twap": None,
            "strike_twap_source": None, "move_ref_mode": "conservative", "move_ref_used": None,
        }
        await self.bot._tick_asset(slot, now)
        return slot

    async def test_fill_allowed_when_clear_of_twap_strike(self):
        slot = await self._run(spot=84420.0, price_to_beat=STRIKE)
        self.assertIsNotNone(slot.position)
        self.assertEqual(slot.position["strategy"], "JEV_DYNAMIC_MAKER")
        self.assertEqual(slot.position["strike_twap"], STRIKE)
        self.assertEqual(slot.position["strike_spot"], STRIKE)
        feats = slot.position["maker_features"]
        for k in ("strike_spot", "strike_twap", "twap_sigma", "spot_sigma", "mid_60s_high",
                  "mid_drop_60s", "elapsed_s", "jev_conviction"):
            self.assertIn(k, feats)
        self.assertGreaterEqual(feats["twap_sigma"], 0.25)
        self.assertEqual(feats["jev_conviction"], 1.9)

    async def test_twap_cross_cancels_and_blocks_fill(self):
        # Spot above the spot strike (UP) but below the TWAP strike -> adverse TWAP cross.
        slot = await self._run(spot=84340.0, price_to_beat=84360.0)
        self.assertIsNone(slot.position)
        self.assertIsNone(slot.resting_order)
        self.assertIn("resting:twap_cross", slot.maker_block_logged)

    async def test_twap_sigma_blocks_fill(self):
        # Same direction as the spot z, but below an (artificially high) TWAP-sigma floor.
        config.MAKER_MIN_TWAP_SIGMA = 1e6
        slot = await self._run(spot=84420.0, price_to_beat=STRIKE)
        self.assertIsNone(slot.position)
        self.assertIsNone(slot.resting_order)
        self.assertIn("resting:twap_sigma", slot.maker_block_logged)

    async def test_mid_drop_blocks_fill(self):
        slot = await self._run(spot=84420.0, price_to_beat=STRIKE, seed_high=0.70)
        self.assertIsNone(slot.position)
        self.assertIsNone(slot.resting_order)
        self.assertIn("resting:mid_drop", slot.maker_block_logged)

    async def test_guard_off_is_legacy(self):
        config.MAKER_TWAP_GUARD = False
        slot = await self._run(spot=84340.0, price_to_beat=84360.0, seed_high=0.70)
        self.assertIsNotNone(slot.position)
        self.assertFalse(slot.maker_block_logged)

    async def test_no_twap_strike_falls_back(self):
        # No priceToBeat and no pre-open ticks -> TWAP strike unavailable -> legacy fill.
        slot = await self._run(spot=84420.0, price_to_beat=None)
        self.assertIsNotNone(slot.position)
        self.assertIsNone(slot.position["maker_features"]["twap_sigma"])
