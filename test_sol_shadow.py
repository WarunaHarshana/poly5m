"""LOG-ONLY sol_shadow: rule B virtual bid for SOL, journal events only, and NO order path for SOL."""
import shutil, tempfile, unittest
from pathlib import Path
from unittest import mock

import config


class TestSolShadow(unittest.TestCase):
    def setUp(self):
        from bot import TWAPBot
        self.tmp = Path(tempfile.mkdtemp())
        with mock.patch.object(config, "SOL_SHADOW", True), mock.patch.object(config, "ASSETS", ["btc"]):
            self.bot = TWAPBot()
        self.bot.risk.state_dir = self.tmp
        self.s = self.bot._sol_shadow
        self.s.market = {"slug": "sol-updown-5m-1000", "slot_start_ts": 1000, "end_ts": 1300,
                         "tokens": {"UP": "su", "DOWN": "sd"}}
        self.s.strike = 150.0; self.s.strike_twap = 150.0; self.s.strike_twap_resolved = True
        cl = self.bot.cl_stream
        self.p = [mock.patch.object(cl, "ready_for", return_value=True), mock.patch.object(cl, "is_stale", return_value=False),
                  mock.patch.object(cl, "price", return_value=150.5), mock.patch("bot._in_utc_windows", return_value=False)]
        for x in self.p: x.start()

    def tearDown(self):
        for x in self.p: x.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_sol_never_in_trading_slots_and_no_orders(self):
        self.assertNotIn("sol", self.bot.slots)
        live = mock.MagicMock()
        self.bot.live_client = live
        with mock.patch.object(self.bot, "_place_maker_bid") as pmb, mock.patch("bot.journal_write"), \
                mock.patch.object(self.bot, "_settle_z_now", return_value=1.2), \
                mock.patch.object(self.bot.book_stream, "get_top", return_value={"bid": 0.49, "ask": 0.50}):
            for t in range(1030, 1300, 5):
                self.bot._tick_sol_shadow(float(t))
            self.bot._finish_sol_shadow("slot_end")
        pmb.assert_not_called()
        self.assertEqual(live.method_calls, [])

    def test_post_fill_result(self):
        with mock.patch("bot.journal_write") as jw, mock.patch.object(self.bot, "_settle_z_now", return_value=0.7):
            with mock.patch.object(self.bot.book_stream, "get_top", return_value={"bid": 0.52, "ask": 0.54}):
                self.bot._tick_sol_shadow(1040.0)
                self.bot._tick_sol_shadow(1041.0)
            self.assertEqual(jw.call_count, 1)
            self.assertEqual(jw.call_args.args[0], "sol_shadow")
            self.assertEqual(jw.call_args.kwargs["side"], "UP")
            with mock.patch.object(self.bot.book_stream, "get_top", return_value={"bid": 0.47, "ask": 0.49}):
                self.bot._tick_sol_shadow(1050.0)
            with mock.patch.object(self.bot.book_stream, "get_top", return_value={"bid": 0.91, "ask": 0.93}):
                self.bot._tick_sol_shadow(1200.0)
            self.bot._finish_sol_shadow("slot_end"); self.bot._finish_sol_shadow("slot_end")
        self.assertEqual(jw.call_count, 2)
        kw = jw.call_args.kwargs
        self.assertEqual(jw.call_args.args[0], "sol_shadow_result")
        self.assertTrue(kw["filled"]); self.assertTrue(kw["ask_through"])
        self.assertEqual(kw["fill_elapsed_s"], 50.0); self.assertEqual(kw["max_bid_after_fill"], 0.91)

    def test_gates_and_cancel(self):
        with mock.patch("bot.journal_write") as jw:
            with mock.patch.object(self.bot, "_settle_z_now", return_value=0.4), \
                    mock.patch.object(self.bot.book_stream, "get_top", return_value={"bid": 0.52, "ask": 0.54}):
                self.bot._tick_sol_shadow(1040.0)          # |z| < 0.5
            with mock.patch.object(self.bot, "_settle_z_now", return_value=-0.9), \
                    mock.patch.object(self.bot.book_stream, "get_top", return_value={"bid": 0.70, "ask": 0.72}):
                self.bot._tick_sol_shadow(1040.0)          # DOWN mid out of band
            self.assertEqual(jw.call_count, 0)
            with mock.patch.object(self.bot, "_settle_z_now", return_value=-0.9), \
                    mock.patch.object(self.bot.book_stream, "get_top", return_value={"bid": 0.55, "ask": 0.57}):
                self.bot._tick_sol_shadow(1045.0)
                self.bot._tick_sol_shadow(1075.0)          # past 70s outside off-window -> cancel
        self.assertEqual([c.args[0] for c in jw.call_args_list], ["sol_shadow", "sol_shadow_result"])
        self.assertEqual(jw.call_args.kwargs["cancel_reason"], "entry_window_end")

    def test_no_post_logs_once(self):
        with mock.patch("bot.journal_write") as jw:
            self.bot._finish_sol_shadow("slot_end"); self.bot._finish_sol_shadow("slot_end")
        self.assertEqual(jw.call_count, 1)
        self.assertFalse(jw.call_args.kwargs["posted"])

    def test_flag_off(self):
        from bot import TWAPBot
        with mock.patch.object(config, "SOL_SHADOW", False):
            self.assertIsNone(TWAPBot()._sol_shadow)


if __name__ == "__main__":
    unittest.main()
