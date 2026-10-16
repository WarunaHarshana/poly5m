"""maker_post_fill price fallback (TP-check book cache, mid hist)."""
import shutil, tempfile, time, unittest
from pathlib import Path
from unittest import mock

import config


class TestBot(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from bot import TWAPBot
        self.tmp = Path(tempfile.mkdtemp())
        self.saved = {k: getattr(config, k) for k in ("MODE",)}
        config.MODE = "paper_live"
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.tmp
        self.slot = self.bot.slots[next(iter(self.bot.slots))]

    async def asyncTearDown(self):
        for k, v in self.saved.items():
            setattr(config, k, v)
        shutil.rmtree(self.tmp, ignore_errors=True)

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
