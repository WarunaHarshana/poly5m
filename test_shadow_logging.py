"""inv12 LOG-ONLY shadows: Rule A/B fields, rest ticks, post-fill path, order_id on settlements."""
import shutil, tempfile, time, unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import config
import shadow_log as sl


def test_twapdist_sign_and_threshold():
    assert sl.twapdist_bps(100_000, 99_980, "UP") == 2.0
    assert sl.twapdist_bps(100_000, 99_980, "DOWN") == -2.0
    assert sl.twapdist_fields(100_000, 99_980, "UP")["would_block_twapdist"] is True
    assert sl.twapdist_fields(100_000, 99_990, "UP")["would_block_twapdist"] is False
    assert sl.twapdist_fields(None, 1, "UP") == {"twapdist_bps": None, "would_block_twapdist": False}
    assert sl.twapdist_bps("x", 1, "UP") is None


def test_alt_bid():
    assert sl.alt_bid_047(0.06, None)
    assert sl.alt_bid_047(0.01, 1.5) and sl.alt_bid_047(None, 1.99)
    assert not sl.alt_bid_047(0.05, 2.0) and not sl.alt_bid_047(None, None)


def test_throttle():
    t = sl.Throttle(1.0)
    assert t.ready("a", 0.0) and not t.ready("a", 0.5) and t.ready("a", 1.0) and t.ready("b", 0.5)


def test_post_fill_tracer_checkpoints():
    p = sl.PostFillTracer()
    p.start("o", 100.0, 0.5, 0.49, 0.5)
    assert p.due("o", 103) == []
    assert p.due("o", 106) == [5]
    assert p.due("o", 131) == [15, 30]
    r = p.record("o", 30, 131, 0.6, 0.58)
    assert r["mid_move"] == 0.1 and r["bid_vs_fill"] == 0.08
    assert p.due("o", 165) == [60]
    p.record("o", 60, 165, 0.7, 0.7)
    assert p.due("o", 172) == [70] and p.due("o", 175) == []
    assert p.due("o", 205) == [100] and p.due("o", 209) == []
    p.stop("o")
    assert p.snapshot("o") is None
    assert p.due("missing", 1e9) == []


def test_flags_default_on():
    assert config.MAKER_TWAPDIST_SHADOW and config.MAKER_REST_TRACE and config.MAKER_POST_FILL_TRACE


class TestBotHooks(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from bot import TWAPBot
        self.tmp = Path(tempfile.mkdtemp())
        self.saved = {k: getattr(config, k) for k in ("MODE", "MAKER_POST_GATE_SHADOW")}
        config.MODE = "paper_live"
        self.bot = TWAPBot()
        self.bot.risk.bankroll = 50.0
        self.bot.risk.state_dir = self.tmp
        self.slot = self.bot.slots[next(iter(self.bot.slots))]

    async def asyncTearDown(self):
        for k, v in self.saved.items():
            setattr(config, k, v)
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_post_shadow_has_rule_a_b_and_still_posts(self):
        config.MAKER_POST_GATE_SHADOW = True
        self.slot.resting_order = None
        self.slot.strike_twap = 99_980.0
        market = {"slug": "btc-updown-5m-test", "tokens": {"UP": "tok_up", "DOWN": "tok_down"}}
        sig = SimpleNamespace(side="UP", target_bid=0.50, edge_net=0.1, maker_rebate=0.0, z_score=1.2,
                              model_prob=0.6, spot=100_000.0)
        with mock.patch("bot.journal_write") as jw:
            await self.bot._place_maker_bid(self.slot, market, sig, strategy_tag="JEV_DYNAMIC_MAKER")
        self.assertIsNotNone(self.slot.resting_order)
        kw = [c for c in jw.call_args_list if c.args[0] == "maker_post_shadow"][0].kwargs
        self.assertEqual(kw["twapdist_bps"], 2.0)
        self.assertTrue(kw["would_block_twapdist"])
        self.assertIn("alt_bid_047", kw)

    def test_rest_tick_throttled_and_never_raises(self):
        ro = {"order_id": "o1", "slot": "s", "side": "UP", "price": 0.5, "placed_at": 100.0}
        with mock.patch("bot.journal_write") as jw:
            self.bot._log_rest_tick(self.slot, ro, 101.0, 40.0, 0.52, 0.5, 0.54, {"bid_size": 30}, 0.02)
            self.bot._log_rest_tick(self.slot, ro, 101.4, 40.4, 0.52, 0.5, 0.54, {"bid_size": 30}, 0.02)
            self.bot._log_rest_tick(self.slot, ro, 102.1, 41.0, 0.52, 0.5, 0.54, {"bid_size": 30}, 0.02)
            kws = [c.kwargs for c in jw.call_args_list if c.args[0] == "maker_rest_tick"]
            self.assertEqual(len(kws), 2)
            self.assertEqual(kws[0]["size_at_our_price"], 30)
            # garbage inputs must not raise
            self.bot._log_rest_tick(self.slot, {"order_id": "o2", "price": "bad"}, 200.0, None, None, "x", None, None, None)
        with mock.patch("bot.journal_write", side_effect=RuntimeError("disk")):
            self.bot._log_rest_tick(self.slot, {"order_id": "o3", "price": 0.5}, 300.0, 1, 0.5, 0.5, 0.5, None, None)

    def test_post_fill_events(self):
        self.slot.position = {"order_id": "o9", "token_id": "tok_up", "opp_token_id": "tok_down", "slot": "s", "side": "UP"}
        self.bot._start_post_fill({"order_id": "o9", "price": 0.5}, 1000.0, 0.5, 0.5)
        with mock.patch.object(self.bot.book_stream, "get_top", return_value={"bid": 0.59, "ask": 0.61}) as gt, \
                mock.patch("bot.journal_write") as jw:
            self.bot._log_post_fill_tick(self.slot, 1003.0)
            self.bot._log_post_fill_tick(self.slot, 1006.0)
            self.bot._log_post_fill_tick(self.slot, 1007.0)
        kws = [c.kwargs for c in jw.call_args_list if c.args[0] == "maker_post_fill"]
        self.assertEqual(len(kws), 1)
        self.assertEqual(kws[0]["offset_s"], 5)
        self.assertEqual(kws[0]["mid_move"], 0.1)
        self.assertEqual(kws[0]["bid_vs_fill"], 0.09)
        gt.assert_called_with("tok_up", opp_token_id="tok_down")
        self.slot.position = None
        self.bot._log_post_fill_tick(self.slot, 2000.0)  # no position: no-op

    async def test_post_fill_hook_runs_while_position_open(self):
        """Regression: hook must be inside the open-position branch (which returns before entry logic)."""
        import inspect, bot
        src = inspect.getsource(bot.TWAPBot._tick_asset)
        i_pos = src.index("if slot.position:")
        i_hook = src.index("self._log_post_fill_tick(slot, now)")
        i_manage = src.index("_manage_open_momentum_position")
        self.assertTrue(i_pos < i_hook < i_manage)
