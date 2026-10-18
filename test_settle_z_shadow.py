"""inv15 LOG-ONLY settle_z_shadow: settlement-rule z + once-per-slot journal event."""
import math, shutil, tempfile, unittest
from pathlib import Path
from unittest import mock

import config
import move_ref


def test_settle_rule_z_formula_and_sign():
    v = 0.0135 ** 2 / 86400.0
    z = move_ref.settle_rule_z(80050.0, 80000.0, 60.0, 0.0135)
    assert math.isclose(z, 50.0 / (80050.0 * math.sqrt(v * (180.0 + 20.0))), rel_tol=1e-9)
    assert move_ref.settle_rule_z(79950.0, 80000.0, 60.0, 0.0135) < 0
    assert move_ref.settle_rule_z(80000.0, 0.0, 60.0, 0.0135) is None
    assert move_ref.settle_rule_z(80010.0, 80000.0, 60.0, 0.0135, min_sigma_usd=1e6) < 1e-4  # floor applies


class TestSettleZShadow(unittest.TestCase):
    def setUp(self):
        from bot import TWAPBot
        self.tmp = Path(tempfile.mkdtemp())
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.tmp
        self.slot = self.bot.slots[next(iter(self.bot.slots))]
        self.slot.strike = 80000.0
        self.slot.strike_twap = 79990.0
        self.slot.strike_twap_source = move_ref.SOURCE_GAMMA
        self.market = {"slug": "btc-updown-5m-1"}

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_logs_once_at_60s_with_fields(self):
        with mock.patch("bot.journal_write") as jw:
            self.bot._maybe_log_settle_z_shadow(self.slot, self.market, 45.0, 80020.0, 0.8, "UP", 0.6)
            self.assertEqual(jw.call_count, 0)
            self.slot.maker_posted = True
            self.bot._maybe_log_settle_z_shadow(self.slot, self.market, 60.4, 80020.0, 0.9, "UP", 0.6)
            self.bot._maybe_log_settle_z_shadow(self.slot, self.market, 61.0, 80020.0, 0.9, "UP", 0.6)
        self.assertEqual(jw.call_count, 1)
        self.assertEqual(jw.call_args.args[0], "settle_z_shadow")
        kw = jw.call_args.kwargs
        self.assertEqual(kw["settle_side"], "UP")
        self.assertGreater(kw["settle_z"], 0)
        self.assertEqual(kw["bot_z"], 0.9)
        self.assertEqual(kw["mid_up"], 0.6)
        self.assertEqual(kw["mid_down"], 0.4)
        self.assertEqual(kw["strike_ref"], "strike_twap")
        self.assertTrue(kw["posted"])

    def test_flag_off_and_spot_fallback(self):
        with mock.patch.object(config, "SETTLE_Z_SHADOW", False), mock.patch("bot.journal_write") as jw:
            self.bot._maybe_log_settle_z_shadow(self.slot, self.market, 60.0, 80020.0, 0.9, "UP", 0.6)
        self.assertEqual(jw.call_count, 0)
        self.slot.strike_twap = None
        with mock.patch("bot.journal_write") as jw:
            self.bot._maybe_log_settle_z_shadow(self.slot, self.market, 60.0, 79990.0, -0.9, "DOWN", 0.55)
        kw = jw.call_args.kwargs
        self.assertEqual(kw["strike_ref"], "spot_strike")
        self.assertEqual(kw["settle_side"], "DOWN")
        self.assertEqual(kw["mid_up"], 0.45)
        self.assertFalse(kw["posted"])
