"""Pre-post falling-mid gate (option 1); default off."""
import config
import maker_guard as mg


def test_default_off():
    assert config.MAKER_POST_MAX_MID_DROP == 0.0
    assert config.MAKER_POST_STABLE_MAX_RANGE == 0.0
    assert not mg.post_falling_mid_blocked(0.5, 0.5, 0.0, 0.0)


def test_drop_threshold():
    assert mg.post_falling_mid_blocked(0.055, None, 0.05, 0.0)   # 2026-10-13 20:20:56 ETH: 0.645 -> 0.59
    assert mg.post_falling_mid_blocked(0.05, None, 0.05, 0.0)
    assert not mg.post_falling_mid_blocked(0.04, None, 0.05, 0.0)
    assert not mg.post_falling_mid_blocked(None, None, 0.05, 0.0)  # fail-open


def test_stability():
    h = mg.MidHistory(60)
    for t, m in [(0, 0.60), (98, 0.62), (99, 0.60), (100, 0.59)]:
        h.add("T", t, m)
    r = h.range_over("T", 100, 3, 0.59)
    assert abs(r - 0.03) < 1e-9
    assert mg.post_falling_mid_blocked(None, r, 0.0, 0.02)
    assert not mg.post_falling_mid_blocked(None, r, 0.0, 0.05)
    assert h.range_over("X", 100, 3, None) is None


# ---- shadow mode (MAKER_POST_GATE_SHADOW): logs only, never blocks ----
import shutil, tempfile, time, unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


def test_shadow_record_flags():
    h = mg.MidHistory(60)
    now = 1000.0
    for t, m in [(now - 40, 0.645), (now - 3, 0.62), (now - 2, 0.60), (now - 1, 0.59)]:
        h.add("T", t, m)
    rec = mg.post_shadow_record(h, "T", now)
    assert rec["mid"] == 0.59 and rec["mid_60s_high"] == 0.645
    assert abs(rec["drop"] - 0.055) < 1e-9
    wb = rec["would_block"]
    assert wb["drop_ge_0.03"] and wb["drop_ge_0.05"] and not wb["drop_ge_0.08"]
    assert wb["stable3s_gt_0.02"] and wb["stable5s_gt_0.02"]
    assert config.MAKER_POST_GATE_SHADOW is False  # default off


class TestShadowBot(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from bot import TWAPBot
        self.tmp = Path(tempfile.mkdtemp())
        self.saved = {k: getattr(config, k) for k in ("MODE", "MAKER_POST_GATE_SHADOW")}
        config.MODE = "paper_live"
        self.bot = TWAPBot()
        self.bot.risk.bankroll = 50.0
        self.bot.risk.state_dir = self.tmp

    async def asyncTearDown(self):
        for k, v in self.saved.items():
            setattr(config, k, v)
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def _post(self, shadow: bool):
        config.MODE = "paper_live"
        config.MAKER_POST_GATE_SHADOW = shadow
        now = time.time()
        # steep fall: every blocking flag would trip
        for dt, m in [(40, 0.70), (2, 0.62), (1, 0.55)]:
            self.bot._mid_hist.add("tok_up", now - dt, m)
        slot = self.bot.slots[next(iter(self.bot.slots))]
        slot.resting_order = None
        market = {"slug": "btc-updown-5m-test", "tokens": {"UP": "tok_up", "DOWN": "tok_down"}}
        sig = SimpleNamespace(side="UP", target_bid=0.50, edge_net=0.1, maker_rebate=0.0, z_score=1.2, model_prob=0.6)
        with mock.patch("bot.journal_write") as jw:
            await self.bot._place_maker_bid(slot, market, sig, strategy_tag="JEV_DYNAMIC_MAKER")
        return slot, [c for c in jw.call_args_list if c.args and c.args[0] == "maker_post_shadow"]

    async def test_shadow_logs_and_never_blocks(self):
        slot, calls = await self._post(True)
        self.assertIsNotNone(slot.resting_order)  # order still posted
        self.assertEqual(len(calls), 1)
        kw = calls[0].kwargs
        self.assertEqual(kw["slot"], "btc-updown-5m-test")
        self.assertEqual(kw["order_id"], slot.resting_order["order_id"])
        self.assertTrue(all(kw["would_block"].values()))
        self.assertIn("range_3s", kw)
        self.assertIn("range_5s", kw)

    async def test_shadow_off_no_log(self):
        slot, calls = await self._post(False)
        self.assertIsNotNone(slot.resting_order)
        self.assertEqual(calls, [])
