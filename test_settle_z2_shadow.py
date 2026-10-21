"""LOG-ONLY settle_z2_shadow: retuned settle_z virtual maker bid; journal events only, NEVER orders."""
import inspect, shutil, tempfile, unittest
from pathlib import Path
from unittest import mock

import config


class TestSettleZ2Shadow(unittest.TestCase):
    def setUp(self):
        from bot import TWAPBot
        self.tmp = Path(tempfile.mkdtemp())
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.tmp
        self.slot = self.bot.slots[next(iter(self.bot.slots))]
        self.slot.strike = 80000.0; self.slot.strike_twap = 79990.0
        self.market = {"slug": "btc-updown-5m-1", "tokens": {"UP": "u", "DOWN": "d"}}
        self.p = mock.patch.object(config, "SETTLE_Z2_SHADOW", True); self.p.start()

    def tearDown(self):
        self.p.stop(); shutil.rmtree(self.tmp, ignore_errors=True)

    def _start(self, z, mid_up, bot_z=1.0, z_twap=1.0):
        self.slot.z2_shadow = None
        with mock.patch("bot.journal_write") as jw:
            self.bot._maybe_start_settle_z2(self.slot, self.market, 60.0, z, bot_z, z_twap, mid_up)
        return jw

    def test_post_fields_and_sizing(self):
        jw = self._start(1.0, 0.70)  # fair .881 -> cap .80; bid .67, shares floor(4/.67)=5
        kw = jw.call_args.kwargs
        self.assertEqual(jw.call_args.args[0], "settle_z2_shadow")
        self.assertEqual((kw["side"], kw["bid"], kw["shares"], kw["time_left_s"]), ("UP", 0.67, 5, 240.0))

    def test_gates(self):
        self.assertEqual(self._start(0.4, 0.5).call_count, 0)            # |z| < 0.5
        self.assertEqual(self._start(0.5, 0.70).call_count, 0)           # fair .722-.03=.692 < .70
        self.assertEqual(self._start(0.6, 0.70).call_count, 1)           # .761-.03=.731 >= .70
        self.assertEqual(self._start(1.0, 0.60, bot_z=1.0, z_twap=-0.5).call_count, 0)  # direction_conflict
        self.assertEqual(self._start(-2.0, 0.10, bot_z=-1, z_twap=-1).call_count, 0)    # DOWN mid .90 > .80
        jw = self._start(-2.0, 0.21, bot_z=-1, z_twap=-1)                # DOWN mid .79 -> bid .76
        self.assertEqual((jw.call_args.kwargs["side"], jw.call_args.kwargs["bid"]), ("DOWN", 0.76))
        jw = self._start(3.0, 0.95)                                       # mid .95 > .80
        self.assertEqual(jw.call_count, 0)

    def test_fill_and_pnl_never_orders(self):
        live = mock.MagicMock(); self.bot.live_client = live
        with mock.patch.object(self.bot, "_place_maker_bid") as pmb:
            self._start(1.0, 0.60)  # bid .57, shares min(6, 7)=6
            with mock.patch("bot.journal_write") as jw, mock.patch.object(self.bot, "_settle_z_now", return_value=0.9):
                with mock.patch.object(self.bot.book_stream, "get_top", return_value={"bid": 0.56, "ask": 0.57}):
                    self.bot._tick_settle_z2(self.slot, 70.0, 80020.0, 0)   # ask == bid: not strictly below
                self.assertFalse(self.slot.z2_shadow["filled"])
                with mock.patch.object(self.bot.book_stream, "get_top", return_value={"bid": 0.55, "ask": 0.56}):
                    self.bot._tick_settle_z2(self.slot, 80.0, 80020.0, 0)
                self.bot._finish_settle_z2(self.slot, "slot_end"); self.bot._finish_settle_z2(self.slot, "slot_end")
        pmb.assert_not_called(); self.assertEqual(live.method_calls, [])
        self.assertEqual(jw.call_count, 1)
        kw = jw.call_args.kwargs
        self.assertEqual(jw.call_args.args[0], "settle_z2_result")
        self.assertEqual(kw["shares"], 6)
        self.assertTrue(kw["filled"]); self.assertTrue(kw["won"]); self.assertEqual(kw["hypo_pnl"], round(6 * 0.43, 4))

    def test_cancel_mid_drop_and_t240(self):
        self._start(1.0, 0.60)
        with mock.patch.object(self.bot.book_stream, "get_top", return_value={"bid": 0.30, "ask": 0.58}), \
                mock.patch.object(self.bot, "_settle_z_now", return_value=-0.3), mock.patch("bot.journal_write") as jw:
            self.bot._tick_settle_z2(self.slot, 90.0, 0, 0)
            self.bot._finish_settle_z2(self.slot, "slot_end")
        kw = jw.call_args.kwargs
        self.assertFalse(kw["filled"]); self.assertEqual(kw["cancel_reason"], "mid_drop"); self.assertFalse(kw["won"])
        self.assertIsNone(kw["hypo_pnl"])
        self._start(1.0, 0.60)
        with mock.patch.object(self.bot.book_stream, "get_top", return_value={"bid": 0.50, "ask": 0.52}), \
                mock.patch("bot.journal_write") as jw:
            self.bot._tick_settle_z2(self.slot, 240.0, 0, 0)
            self.bot._tick_settle_z2(self.slot, 250.0, 0, 0)  # would fill, but too late
            self.bot._finish_settle_z2(self.slot, "slot_end")
        self.assertEqual(jw.call_args.kwargs["cancel_reason"], "t240")

    def test_flag_off_noop(self):
        with mock.patch.object(config, "SETTLE_Z2_SHADOW", False):
            self.assertEqual(self._start(1.0, 0.60).call_count, 0)
        self.assertIsNone(self.slot.z2_shadow)

    def test_no_order_path_in_shadow_code(self):
        import bot
        src = "".join(inspect.getsource(getattr(bot.TWAPBot, n)) for n in
                      ("_maybe_start_settle_z2", "_tick_settle_z2", "_finish_settle_z2"))
        for bad in ("live_client", "_place_maker_bid", "post_order", "create_order", "cancel_order", "await "):
            self.assertNotIn(bad, src)
