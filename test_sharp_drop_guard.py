"""SHARP_DROP_GUARD: live pre-post block when favoured mid >= threshold below its 60s high."""
import time
import unittest
from unittest import mock

import config
import maker_guard
from maker_guard import MidHistory
import shutil
import tempfile
from pathlib import Path


class _Base(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from bot import TWAPBot
        self.tmp = Path(tempfile.mkdtemp())
        self.saved = {k: getattr(config, k) for k in ("MODE", "MAKER_POST_GATE_SHADOW")}
        config.MODE = "paper_live"
        config.MAKER_POST_GATE_SHADOW = False
        self.bot = TWAPBot()
        self.bot.risk.bankroll = 50.0
        self.bot.risk.state_dir = self.tmp

    async def asyncTearDown(self):
        for k, v in self.saved.items():
            setattr(config, k, v)
        shutil.rmtree(self.tmp, ignore_errors=True)


class TestSharpDropPure(unittest.TestCase):
    def test_matches_shadow_definition(self):
        h = MidHistory(60.0)
        now = 1000.0
        for dt, m in [(70, 0.95), (50, 0.72), (20, 0.68), (1, 0.655)]:
            h.add("t", now - dt, m)
        mid, hi, drop = maker_guard.sharp_drop(h, "t", now, 60.0)
        rec = maker_guard.post_shadow_record(h, "t", now)
        self.assertEqual(rec["drop"], round(drop, 4))
        self.assertAlmostEqual(hi, 0.72)
        self.assertAlmostEqual(drop, 0.065)
        self.assertTrue(maker_guard.sharp_drop_blocked(drop, 0.06))
        self.assertFalse(maker_guard.sharp_drop_blocked(0.0599, 0.06))
        self.assertTrue(maker_guard.sharp_drop_blocked(0.06, 0.06))

    def test_fail_open(self):
        h = MidHistory(60.0)
        self.assertEqual(maker_guard.sharp_drop(h, "x", 0.0, 60.0), (None, None, None))
        self.assertFalse(maker_guard.sharp_drop_blocked(None, 0.06))
        self.assertFalse(maker_guard.sharp_drop_blocked(0.5, 0.0))


class TestSharpDropWiring(_Base):
    def test_default_off(self):
        self.assertFalse(config.SHARP_DROP_GUARD) if 'SHARP_DROP_GUARD' not in __import__('os').environ else None

    async def asyncSetUp(self):
        await super().asyncSetUp()
        for k in ("SHARP_DROP_GUARD", "SHARP_DROP_THRESHOLD", "SHARP_DROP_WINDOW_S"):
            self.saved.setdefault(k, getattr(config, k))
        config.SHARP_DROP_THRESHOLD = 0.06
        config.SHARP_DROP_WINDOW_S = 60.0

    async def _run(self, on, mids):
        config.MODE = "paper_live"
        config.SHARP_DROP_GUARD = on
        now = time.time()
        for dt, m in mids:
            self.bot._mid_hist.add("tok_up", now - dt, m)
        slot = self.bot.slots[next(iter(self.bot.slots))]
        slot.resting_order = None
        slot.guard_cooldown_until = 0.0
        market = {"slug": "btc-updown-5m-test", "tokens": {"UP": "tok_up", "DOWN": "tok_down"}}
        from types import SimpleNamespace
        sig = SimpleNamespace(side="UP", target_bid=0.50, edge_net=0.1, maker_rebate=0.0, z_score=1.2, model_prob=0.6)
        with mock.patch("bot.journal_write") as jw:
            await self.bot._place_maker_bid(slot, market, sig, strategy_tag="JEV_DYNAMIC_MAKER")
        blocks = [c for c in jw.call_args_list if c.args and c.args[0] == "maker_block"]
        return slot, blocks

    async def test_blocks_when_on(self):
        slot, blocks = await self._run(True, [(30, 0.70), (1, 0.63)])
        self.assertIsNone(slot.resting_order)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0].kwargs["reason"], "sharp_drop")
        self.assertAlmostEqual(blocks[0].kwargs["mid_drop"], 0.07, places=6)

    async def test_off_does_not_block(self):
        slot, blocks = await self._run(False, [(30, 0.70), (1, 0.63)])
        self.assertIsNotNone(slot.resting_order)
        self.assertEqual(blocks, [])

    async def test_small_drop_posts(self):
        slot, blocks = await self._run(True, [(30, 0.70), (1, 0.65)])
        self.assertIsNotNone(slot.resting_order)
        self.assertEqual(blocks, [])

    async def test_old_high_outside_window_ignored(self):
        slot, blocks = await self._run(True, [(59.5, 0.64), (1, 0.63)])
        self.assertIsNotNone(slot.resting_order)


if __name__ == "__main__":
    unittest.main()
