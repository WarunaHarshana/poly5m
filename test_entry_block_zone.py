"""Tests for the JEV_DYNAMIC entry dead zone (ENTRY_BLOCK_*)."""
from __future__ import annotations

import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import config
from bot import TWAPBot
from jev_advisor import JevDecision

STRIKE = 84300.0


def _approve():
    return JevDecision(
        action="proceed", passed=True, size_multiplier=1.0, conviction=2.5, reversal_risk=0.1,
        market_regime="clean_trend", confidence=0.9, reason="ok", latency_ms=1.0, fallback_used=False,
    )


class TestEntryBlockZone(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        keys = ("MODE", "STRATEGY", "MAKER_TWAP_GUARD", "MAKER_REQUIRE_TWAP", "ENTRY_BLOCK_ENABLED",
                "ENTRY_BLOCK_ELAPSED_LO", "ENTRY_BLOCK_ELAPSED_HI", "ENTRY_BLOCK_LEGS")
        self.saved = {k: getattr(config, k) for k in keys}
        config.MODE = "paper_live"
        config.STRATEGY = "JEV_DYNAMIC"
        config.MAKER_TWAP_GUARD = True
        config.MAKER_REQUIRE_TWAP = True
        config.ENTRY_BLOCK_ENABLED = True
        config.ENTRY_BLOCK_ELAPSED_LO = 120.0
        config.ENTRY_BLOCK_ELAPSED_HI = 180.0
        config.ENTRY_BLOCK_LEGS = ("momentum", "maker")
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.temp_dir
        self.bot.risk.bankroll = 50.0
        self.bot._slot_done = set()
        self.bot._trades_log_path = self.temp_dir / "trades.json"
        self.bot._save_slot_state = lambda **kw: None
        self.bot.jev.evaluate_dynamic = AsyncMock(return_value=_approve())
        self.jp = patch("bot.journal_write")
        self.journal = self.jp.start()

    async def asyncTearDown(self):
        self.jp.stop()
        for k, v in self.saved.items():
            setattr(config, k, v)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _slot(self, elapsed: float, spot: float = 84420.0, resting: bool = False):
        now = time.time()
        slot_start = now - elapsed
        slot = self.bot.slots["btc"]
        slot.market = {
            "slug": f"btc-updown-5m-test-zone-{elapsed}-{resting}", "slot_start_ts": slot_start,
            "end_ts": slot_start + 300.0, "tokens": {"UP": "tok_up", "DOWN": "tok_down"},
            "fee_rate": 0.0, "price_to_beat": STRIKE,
        }
        slot.strike = STRIKE
        slot.position = None
        cl = self.bot.cl_stream
        cl._record(slot_start, STRIKE, asset="btc")
        cl._record(slot_start + elapsed / 2, spot, asset="btc")
        cl._record(now, spot, asset="btc")
        cl._last_tick_time["btc"] = time.time()
        self.bot.book_stream._apply_update("tok_up", bid=0.48, ask=0.52, bid_sz=500, ask_sz=500)
        self.bot.book_stream._apply_update("tok_down", bid=0.48, ask=0.52, bid_sz=500, ask_sz=500)
        slot.resting_order = None if not resting else {
            "asset": "btc", "slot": slot.market["slug"], "side": "UP", "token_id": "tok_up",
            "price": 0.47, "shares": 6.0, "cost_usd": 2.82, "z_score": 1.2, "prob": 0.5,
            "rebate": 0.0, "placed_at": now - 1.0, "order_id": "maker_paper_test", "jev": None,
        }
        return slot, now

    def _any_consults(self):
        return len(self.bot.jev.evaluate_dynamic.await_args_list)

    def _consults(self, kind):
        return [c for c in self.bot.jev.evaluate_dynamic.await_args_list
                if c.kwargs.get("opportunity_type") == kind]

    def test_helper_bounds_and_legs(self):
        slot, _ = self._slot(150.0)
        z = self.bot._entry_zone_blocked
        self.assertFalse(z(slot, "s", "maker", 119.9))
        self.assertTrue(z(slot, "s", "maker", 120.0))
        self.assertTrue(z(slot, "s", "momentum", 179.9))
        self.assertFalse(z(slot, "s", "momentum", 180.0))
        self.assertFalse(z(slot, "s", "maker", None))
        config.ENTRY_BLOCK_LEGS = ("momentum",)
        self.assertFalse(z(slot, "s", "maker", 150.0))
        self.assertTrue(z(slot, "s", "momentum", 150.0))
        config.ENTRY_BLOCK_ELAPSED_HI = 0.0  # rollback switch
        self.assertFalse(z(slot, "s", "momentum", 150.0))
        config.ENTRY_BLOCK_ELAPSED_HI = 180.0
        config.ENTRY_BLOCK_ENABLED = False
        self.assertFalse(z(slot, "s", "momentum", 150.0))

    async def test_in_zone_no_maker_quote_and_logged_once(self):
        slot, now = self._slot(150.0)
        with self.assertLogs("twap-bot", level="INFO") as cm:
            await self.bot._tick_asset(slot, now)
            await self.bot._tick_asset(slot, now + 1.0)
        self.assertIsNone(slot.resting_order)
        self.assertIsNone(slot.position)
        self.assertEqual(self._consults("MAKER_BID"), [])
        self.assertEqual(self._consults("MOMENTUM_TAKER"), [])
        zone = [m for m in cm.output if "ENTRY_BLOCK_ZONE" in m]
        self.assertEqual(len([m for m in zone if "leg=maker" in m]), 1)
        self.assertEqual(len([m for m in zone if "leg=momentum" in m]), 1)
        events = [c.kwargs for c in self.journal.call_args_list if c.args and c.args[0] == "entry_block_zone"]
        self.assertEqual({e["leg"] for e in events}, {"maker", "momentum"})

    async def test_outside_zone_maker_quotes_normally(self):
        slot, now = self._slot(100.0)
        await self.bot._tick_asset(slot, now)
        self.assertGreaterEqual(self._any_consults(), 1)  # momentum or maker entry attempted
        self.assertTrue(slot.position is not None or slot.resting_order is not None)
        self.assertFalse(slot.entry_zone_logged)

    async def test_after_zone_maker_quotes_normally(self):
        slot, now = self._slot(185.0)
        await self.bot._tick_asset(slot, now)
        self.assertGreaterEqual(self._any_consults(), 1)
        self.assertFalse(slot.entry_zone_logged)

    async def test_resting_bid_cancelled_on_entering_zone(self):
        slot, now = self._slot(121.0, resting=True)
        with self.assertLogs("twap-bot", level="INFO") as cm:
            await self.bot._tick_asset(slot, now)
        self.assertIsNone(slot.resting_order)
        self.assertIsNone(slot.position)  # never filled
        self.assertTrue(any("entry_block_zone" in m and "cancel resting" in m for m in cm.output))
        self.assertIn("maker:resting", slot.entry_zone_logged)

    async def test_disabled_flag_is_legacy(self):
        config.ENTRY_BLOCK_ENABLED = False
        slot, now = self._slot(150.0)
        await self.bot._tick_asset(slot, now)
        self.assertGreaterEqual(self._any_consults(), 1)
        self.assertFalse(slot.entry_zone_logged)

    async def test_maker_only_scope_leaves_momentum_unblocked(self):
        config.ENTRY_BLOCK_LEGS = ("maker",)
        slot, now = self._slot(150.0)
        await self.bot._tick_asset(slot, now)
        self.assertEqual(self._consults("MAKER_BID"), [])
        self.assertNotIn("momentum:entry", slot.entry_zone_logged)
        self.assertIn("maker:entry", slot.entry_zone_logged)


def test_config_defaults():
    import importlib
    import os
    env = {k: os.environ.pop(k) for k in list(os.environ) if k.startswith("ENTRY_BLOCK_")}
    try:
        c = importlib.reload(config)
        assert c.ENTRY_BLOCK_ENABLED is True
        assert c.ENTRY_BLOCK_ELAPSED_LO == 120.0 and c.ENTRY_BLOCK_ELAPSED_HI == 180.0
        assert c.ENTRY_BLOCK_LEGS == ("momentum", "maker")
    finally:
        os.environ.update(env)
        importlib.reload(config)


class TestJevDisabledPlainPath(TestEntryBlockZone):
    """JEV_ENABLED=no: real (disabled) advisor passes everything; rules path still trades, zone still blocks."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        from jev_advisor import JevAdvisor
        self.bot.jev = JevAdvisor(enabled=False, shadow=True)
        self.spy = AsyncMock(wraps=self.bot.jev.evaluate_dynamic)
        self.bot.jev.evaluate_dynamic = self.spy

    async def test_disabled_jev_still_enters_outside_zone(self):
        slot, now = self._slot(100.0)
        await self.bot._tick_asset(slot, now)
        self.assertTrue(slot.position is not None or slot.resting_order is not None)
        st = self.bot.jev.stats()
        self.assertFalse(st["enabled"])
        self.assertFalse(st["shadow_mode"])
        self.assertEqual(st["vetoed"], 0)
