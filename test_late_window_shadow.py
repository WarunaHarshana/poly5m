"""inv17 LOG-ONLY late_window_shadow: one virtual bid per slot, fill/cancel tracking, max two events."""
import shutil, tempfile, unittest
from pathlib import Path
from unittest import mock

import config


class TestLateWindowShadow(unittest.TestCase):
    def setUp(self):
        from bot import TWAPBot
        self.tmp = Path(tempfile.mkdtemp())
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.tmp
        self.slot = self.bot.slots[next(iter(self.bot.slots))]
        self.slot.strike = 80000.0
        self.slot.strike_twap = 79990.0
        self.market = {"slug": "btc-updown-5m-1", "tokens": {"UP": "tu", "DOWN": "td"}}

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _start(self, elapsed=80.0, **kw):
        args = dict(side="UP", token="tu", mid=0.55, ask=0.56, z=1.3, z_twap=1.1, spot=80040.0, now=1000.0,
                    require_twap=True, guard_on=True)
        args.update(kw)
        self.bot._maybe_start_late_shadow(self.slot, self.market, elapsed, **args)

    def test_start_once_and_fill_then_result(self):
        with mock.patch("bot.journal_write") as jw:
            self._start()
            self._start(elapsed=90.0)
            self.assertEqual(jw.call_count, 1)
            self.assertEqual(jw.call_args.args[0], "late_window_shadow")
            self.assertEqual(jw.call_args.kwargs["bid"], 0.50)
            with mock.patch.object(self.bot.book_stream, "get_top", return_value={"bid": 0.48, "ask": 0.50}):
                self.bot._tick_late_shadow(self.slot, 95.0, 205.0, 80040.0, 80000.0, 1.0, 1015.0)
            with mock.patch.object(self.bot.book_stream, "get_top", return_value={"bid": 0.96, "ask": 0.97}):
                self.bot._tick_late_shadow(self.slot, 250.0, 50.0, 80080.0, 80000.0, 2.0, 1170.0)
            self.bot._finish_late_shadow(self.slot, "slot_end")
            self.bot._finish_late_shadow(self.slot, "slot_end")
        self.assertEqual(jw.call_count, 2)
        kw = jw.call_args.kwargs
        self.assertEqual(jw.call_args.args[0], "late_window_shadow_result")
        self.assertTrue(kw["filled"])
        self.assertEqual(kw["fill_elapsed_s"], 95.0)
        self.assertEqual(kw["max_bid_after_fill"], 0.96)
        self.assertIsNone(kw["cancel_reason"])

    def test_cancel_and_gates(self):
        with mock.patch("bot.journal_write") as jw:
            self._start(elapsed=65.0)            # outside [70,120)
            self._start(elapsed=125.0)
            self.slot.strike_twap = None
            self._start()                         # no TWAP strike
            self.slot.strike_twap = 79990.0
            self._start(z_twap=-0.5)              # maker_guard.check_quote fails
            self.assertEqual(jw.call_count, 0)
            with mock.patch.object(config, "LATE_WINDOW_SHADOW", False):
                self._start()
            self.assertEqual(jw.call_count, 0)
            self._start()
            with mock.patch.object(self.bot.book_stream, "get_top", return_value={"bid": 0.60, "ask": 0.62}):
                self.bot._tick_late_shadow(self.slot, 100.0, 200.0, 79980.0, 80000.0, 0.5, 1020.0)
        self.assertEqual(jw.call_count, 2)
        self.assertFalse(jw.call_args.kwargs["filled"])
        self.assertIn(jw.call_args.kwargs["cancel_reason"], ("maker_guard_twap_cross", "adverse_spot_cross"))
