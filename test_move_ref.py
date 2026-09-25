"""Tests for the conservative momentum move reference (MOMENTUM_MOVE_REF)."""
from __future__ import annotations

import json
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

import config
import move_ref
from bot import TWAPBot
from feeds import ChainlinkRTDSStream
from jev_advisor import JevDecision

# btc-updown-5m-1790280000 (2026-09-24 20:00 UTC) forensic numbers
SPOT = 84307.5
STRIKE_SPOT = 84386.59
PRICE_TO_BEAT = 84353.97
THRESHOLD = 64.41


# ---------------------------------------------------------------- pure helpers
def test_a_blocks_when_twap_move_below_threshold_but_spot_move_clears():
    spot_move, ref_spot = move_ref.entry_move("spot", SPOT, STRIKE_SPOT, PRICE_TO_BEAT)
    assert ref_spot == move_ref.REF_SPOT_MODE
    assert move_ref.gate_move_ok(spot_move, THRESHOLD, "DOWN")  # -79.09 would have passed

    move, ref = move_ref.entry_move("conservative", SPOT, STRIKE_SPOT, PRICE_TO_BEAT)
    assert ref == move_ref.REF_TWAP
    assert abs(move - (SPOT - PRICE_TO_BEAT)) < 1e-9  # -46.47
    assert not move_ref.gate_move_ok(move, THRESHOLD, "DOWN")


def test_b_allows_when_both_clear():
    move, ref = move_ref.entry_move("conservative", SPOT, STRIKE_SPOT, 84380.0)
    assert ref == move_ref.REF_TWAP and abs(move - (SPOT - 84380.0)) < 1e-9  # -72.5
    assert move_ref.gate_move_ok(move, THRESHOLD, "DOWN")
    # spot move smaller than twap move -> spot is used
    move2, ref2 = move_ref.entry_move("conservative", SPOT, STRIKE_SPOT, 84400.0)
    assert ref2 == move_ref.REF_SPOT and abs(move2 - (SPOT - STRIKE_SPOT)) < 1e-9
    assert move_ref.gate_move_ok(move2, THRESHOLD, "DOWN")


def test_c_direction_disagreement_blocks():
    move, ref = move_ref.entry_move("conservative", SPOT, STRIKE_SPOT, 84300.0)  # twap says UP
    assert ref == move_ref.REF_DIRECTION_CONFLICT and move == 0.0
    assert not move_ref.gate_move_ok(move, THRESHOLD, "DOWN")
    assert not move_ref.gate_move_ok(move, THRESHOLD, "UP")
    # wrong-direction move never passes even if large
    assert not move_ref.gate_move_ok(-100.0, THRESHOLD, "UP")


def test_d_fallback_to_spot_when_twap_unavailable():
    move, ref = move_ref.entry_move("conservative", SPOT, STRIKE_SPOT, None)
    assert ref == move_ref.REF_SPOT_FALLBACK
    assert abs(move - (SPOT - STRIKE_SPOT)) < 1e-9
    assert move_ref.gate_move_ok(move, THRESHOLD, "DOWN")


def test_e_spot_mode_ignores_twap():
    for twap in (None, PRICE_TO_BEAT, 84300.0):
        move, ref = move_ref.entry_move("spot", SPOT, STRIKE_SPOT, twap)
        assert ref == move_ref.REF_SPOT_MODE and abs(move - (SPOT - STRIKE_SPOT)) < 1e-9


def test_normalize_mode():
    assert move_ref.normalize_mode("SPOT") == "spot"
    assert move_ref.normalize_mode(" conservative ") == "conservative"
    assert move_ref.normalize_mode(None) == "conservative"
    assert move_ref.normalize_mode("garbage") == "conservative"


def test_estimate_twap_strike_priority_and_coverage():
    s0 = 1_790_280_000.0
    cl = ChainlinkRTDSStream(assets=["btc"])
    # (1) official price-to-beat wins when known
    assert move_ref.estimate_twap_strike(cl, s0, 60, "btc", price_to_beat=PRICE_TO_BEAT) == (
        PRICE_TO_BEAT, move_ref.SOURCE_GAMMA)
    # no ticks -> unavailable
    assert move_ref.estimate_twap_strike(cl, s0, 60, "btc") == (None, move_ref.SOURCE_UNAVAILABLE)
    # ticks only after (restart mid-window) -> unavailable
    for i in range(0, 30):
        cl._record(s0 - 20 + i, 84400.0, asset="btc")
    assert move_ref.estimate_twap_strike(cl, s0, 60, "btc")[0] is None
    # full pre-open coverage: linear ramp 84340 -> 84400 over [s0-60, s0] => TWAP ~ 84370
    cl2 = ChainlinkRTDSStream(assets=["btc"])
    for i in range(0, 71):
        t = s0 - 70 + i
        cl2._record(t, 84330.0 + i, asset="btc")
    v, src = move_ref.estimate_twap_strike(cl2, s0, 60, "btc")
    assert src == move_ref.SOURCE_CHAINLINK_TWAP
    assert abs(v - 84370.0) < 0.6


# ---------------------------------------------------------------- bot integration
class TestMoveRefBotIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.saved = {k: getattr(config, k) for k in (
            "MODE", "STRATEGY", "MOMENTUM_MOVE_REF", "PAPER_FOK_REQUIRE_SIZE", "MAKER_TWAP_GUARD")}
        config.MODE = "paper_live"
        config.STRATEGY = "JEV_DYNAMIC"
        config.PAPER_FOK_REQUIRE_SIZE = False
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.temp_dir
        self.bot.risk.bankroll = 50.0
        self.bot._slot_done = set()
        self.bot._trades_log_path = self.temp_dir / "trades.json"
        self.bot._save_slot_state = lambda **kw: None
        self.bot.jev.evaluate_dynamic = AsyncMock(return_value=JevDecision(
            action="proceed", passed=True, size_multiplier=1.0, conviction=2.0, reversal_risk=0.2,
            market_regime="clean_trend", confidence=0.8, reason="ok", latency_ms=1.0, fallback_used=False,
        ))

    async def asyncTearDown(self):
        for k, v in self.saved.items():
            setattr(config, k, v)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def _run(self, mode: str, price_to_beat, pre_open_ticks: bool = False):
        config.MOMENTUM_MOVE_REF = mode
        now = time.time()
        slot_start = now - 160.0
        slot = self.bot.slots["btc"]
        slot.market = {
            "slug": f"btc-updown-5m-test-moveref-{mode}-{price_to_beat}",
            "slot_start_ts": slot_start,
            "end_ts": slot_start + 300.0,
            "tokens": {"UP": "tok_up", "DOWN": "tok_down"},
            "fee_rate": 0.0,
            "price_to_beat": price_to_beat,
        }
        slot.strike = STRIKE_SPOT
        slot.position = None
        slot.resting_order = None
        cl = self.bot.cl_stream
        if pre_open_ticks:
            for i in range(0, 61):
                cl._record(slot_start - 60 + i, STRIKE_SPOT, asset="btc")
        cl._record(slot_start, STRIKE_SPOT, asset="btc")
        cl._record(slot_start + 40.0, 84340.0, asset="btc")
        cl._record(slot_start + 90.0, 84320.0, asset="btc")
        cl._record(now, SPOT, asset="btc")
        cl._last_tick_time["btc"] = time.time()
        self.bot.book_stream._apply_update("tok_down", bid=0.68, ask=0.70, bid_sz=500, ask_sz=500)
        self.bot.book_stream._apply_update("tok_up", bid=0.30, ask=0.32, bid_sz=500, ask_sz=500)
        _, thr, _, _ = config.compute_dynamic_feasibility(elapsed_s=160.0, spot=SPOT, daily_vol=slot.params.daily_vol)
        await self.bot._tick_asset(slot, now)
        return slot, thr

    def _assert_precondition(self, thr):
        # The forensic case only makes sense if spot move clears but TWAP move does not.
        if not (abs(SPOT - PRICE_TO_BEAT) < thr <= abs(SPOT - STRIKE_SPOT)):
            self.skipTest(f"threshold {thr:.2f} outside forensic range")

    async def test_a_conservative_blocks_forensic_trade(self):
        slot, thr = await self._run("conservative", PRICE_TO_BEAT)
        self._assert_precondition(thr)
        self.assertIsNone(slot.position)
        self.assertEqual(slot.strike_twap, PRICE_TO_BEAT)
        self.assertEqual(slot.strike_twap_source, move_ref.SOURCE_GAMMA)
        self.assertEqual(slot.move_ref_used, move_ref.REF_TWAP)
        self.assertTrue(slot.move_ref_block_logged)
        calls = [c.kwargs.get("opportunity_type") for c in self.bot.jev.evaluate_dynamic.call_args_list]
        self.assertNotIn("MOMENTUM_TAKER", calls)

    async def test_b_conservative_allows_when_both_clear(self):
        slot, thr = await self._run("conservative", SPOT + thr_plus(self))
        self.assertIsNotNone(slot.position)
        self.assertEqual(slot.position["strategy"], "JEV_DYNAMIC_MOMENTUM")
        self.assertEqual(slot.position["side"], "DOWN")
        self.assertEqual(slot.position["strike_spot"], STRIKE_SPOT)
        self.assertIsNotNone(slot.position["strike_twap"])
        self.assertEqual(slot.position["move_ref_mode"], "conservative")
        self.assertFalse(slot.move_ref_block_logged)

    async def test_c_conservative_direction_conflict_blocks(self):
        slot, thr = await self._run("conservative", 84300.0)
        self.assertIsNone(slot.position)
        self.assertEqual(slot.move_ref_used, move_ref.REF_DIRECTION_CONFLICT)
        self.assertTrue(slot.move_ref_block_logged)

    async def test_d_conservative_falls_back_to_spot_without_twap(self):
        slot, thr = await self._run("conservative", None)
        self._assert_precondition(thr)
        self.assertEqual(slot.strike_twap_source, move_ref.SOURCE_UNAVAILABLE)
        self.assertIsNone(slot.strike_twap)
        self.assertEqual(slot.move_ref_used, move_ref.REF_SPOT_FALLBACK)
        self.assertIsNotNone(slot.position)
        self.assertEqual(slot.position["move_ref_used"], move_ref.REF_SPOT_FALLBACK)

    async def test_d2_conservative_uses_chainlink_ticks_when_no_price_to_beat(self):
        slot, thr = await self._run("conservative", None, pre_open_ticks=True)
        self.assertEqual(slot.strike_twap_source, move_ref.SOURCE_CHAINLINK_TWAP)
        self.assertAlmostEqual(slot.strike_twap, STRIKE_SPOT, places=4)

    async def test_e_spot_mode_unchanged(self):
        config.MAKER_TWAP_GUARD = False  # maker guard also resolves the TWAP strike; isolate spot mode
        slot, thr = await self._run("spot", PRICE_TO_BEAT)
        self._assert_precondition(thr)
        self.assertIsNotNone(slot.position)  # legacy behaviour: the forensic trade is taken
        self.assertEqual(slot.position["move_ref_used"], move_ref.REF_SPOT_MODE)
        self.assertIsNone(slot.strike_twap)  # TWAP strike not even computed in spot mode
        self.assertFalse(slot.move_ref_block_logged)


def thr_plus(tc) -> float:
    """TWAP strike offset that clears the dynamic threshold at t=160s (DOWN side)."""
    params = config.ASSET_PARAMS["btc"]
    _, thr, _, _ = config.compute_dynamic_feasibility(elapsed_s=160.0, spot=SPOT, daily_vol=params.daily_vol)
    off = max(thr * 1.1, abs(SPOT - STRIKE_SPOT) * 0.95)
    return off
