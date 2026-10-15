"""twapdist-midslot candidate: default OFF never blocks; ON blocks; shadow fields; post-fill fallback."""
import shutil, tempfile, time, unittest
from pathlib import Path
from unittest import mock

import config
import shadow_log as sl


def test_condition():
    assert sl.midslot_condition(45, 2.0)
    assert not sl.midslot_condition(39.9, 5) and not sl.midslot_condition(60, 5)
    assert not sl.midslot_condition(50, 1.99)
    assert not sl.midslot_condition(None, 5) and not sl.midslot_condition(50, None)


def test_defaults():
    assert config.MAKER_SKIP_TWAPDIST_MIDSLOT is False
    assert (config.MIDSLOT_LO, config.MIDSLOT_HI, config.MIDSLOT_TWAPDIST_BPS) == (40.0, 60.0, 2.0)


class TestBot(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from bot import TWAPBot
        self.tmp = Path(tempfile.mkdtemp())
        self.saved = {k: getattr(config, k) for k in ("MODE", "MAKER_SKIP_TWAPDIST_MIDSLOT")}
        config.MODE = "paper_live"
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.tmp
        self.slot = self.bot.slots[next(iter(self.bot.slots))]

    async def asyncTearDown(self):
        for k, v in self.saved.items():
            setattr(config, k, v)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_flag_off_never_blocks(self):
        config.MAKER_SKIP_TWAPDIST_MIDSLOT = False
        for el in (0, 40, 45, 59.9, 100):
            for spot in (100_000, 100_100, 99_900):
                for side in ("UP", "DOWN"):
                    self.assertFalse(self.bot._midslot_block_active(el, spot, 99_980, side))
        # but the shadow evaluation still reports it
        self.assertTrue(self.bot._midslot_would_block(45, 100_000, 99_980, "UP"))

    def test_flag_on_blocks_only_in_condition(self):
        config.MAKER_SKIP_TWAPDIST_MIDSLOT = True
        self.assertTrue(self.bot._midslot_block_active(45, 100_000, 99_980, "UP"))
        self.assertFalse(self.bot._midslot_block_active(45, 100_000, 99_980, "DOWN"))
        self.assertFalse(self.bot._midslot_block_active(65, 100_000, 99_980, "UP"))
        self.assertFalse(self.bot._midslot_block_active(45, None, 99_980, "UP"))

    def test_rest_tick_carries_midslot_flag(self):
        self.slot.strike_twap = 99_980.0
        ro = {"order_id": "m1", "slot": "s", "side": "UP", "price": 0.5, "placed_at": 100.0}
        with mock.patch("bot.journal_write") as jw:
            self.bot._log_rest_tick(self.slot, ro, 101.0, 45.0, 0.52, 0.5, 0.54, {}, 0.02, spot=100_000.0)
        kw = jw.call_args.kwargs
        self.assertTrue(kw["would_block_twapdist_midslot"])
        self.assertEqual(kw["twapdist_bps"], 2.0)

    def test_post_fill_falls_back_to_tp_cache(self):
        self.slot.position = {"order_id": "p1", "token_id": "tu", "opp_token_id": "td", "slot": "s", "side": "UP",
                              "_trace_last_bid": 0.55, "_trace_last_ask": 0.57}
        self.bot._start_post_fill({"order_id": "p1", "price": 0.5}, 1000.0, 0.5, 0.5)
        with mock.patch.object(self.bot.book_stream, "get_top", return_value=None), \
                mock.patch("bot.journal_write") as jw:
            self.bot._log_post_fill_tick(self.slot, 1005.5)
        kw = jw.call_args.kwargs
        self.assertEqual(kw["mid"], 0.56)
        self.assertEqual(kw["best_bid"], 0.55)
        self.assertEqual(kw["price_source"], "tp_check_cache")

    def test_post_fill_falls_back_to_mid_hist(self):
        self.slot.position = {"order_id": "p2", "token_id": "tu", "slot": "s", "side": "UP"}
        self.bot._start_post_fill({"order_id": "p2", "price": 0.5}, 1000.0, 0.5, 0.5)
        self.bot._mid_hist.add("tu", 1004.0, 0.61)
        with mock.patch.object(self.bot.book_stream, "get_top", return_value=None), \
                mock.patch("bot.journal_write") as jw:
            self.bot._log_post_fill_tick(self.slot, 1005.5)
        self.assertEqual(jw.call_args.kwargs["mid"], 0.61)
        self.assertEqual(jw.call_args.kwargs["price_source"], "mid_hist")
