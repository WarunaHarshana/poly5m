"""ENTRY_SIGNAL=settle_z: entry side/threshold from settle_z (rule B); legacy bot_z when unset or unavailable."""
import shutil, tempfile, unittest
from pathlib import Path
from unittest import mock

import config
import move_ref


class TestEntrySignal(unittest.TestCase):
    def setUp(self):
        from bot import TWAPBot
        self.tmp = Path(tempfile.mkdtemp())
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.tmp
        self.slot = self.bot.slots[next(iter(self.bot.slots))]
        self.slot.strike = 80000.0
        self.slot.strike_twap = 80010.0
        self.slot.strike_twap_source = move_ref.SOURCE_GAMMA

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_unset_is_legacy(self):
        with mock.patch.object(config, "ENTRY_SIGNAL", ""):
            self.assertEqual(self.bot._entry_signal(self.slot, 60.0, 80005.0, 0.9), (0.9, None))

    def test_settle_z_drives_side_and_threshold(self):
        # spot above spot strike (bot_z UP) but below TWAP strike -> settle_z DOWN
        with mock.patch.object(config, "ENTRY_SIGNAL", "settle_z"):
            z, mz = self.bot._entry_signal(self.slot, 60.0, 80005.0, 0.9)
        self.assertEqual(z, self.bot._settle_z_now(self.slot, 60.0, 80005.0))
        self.assertLess(z, 0)
        self.assertEqual(mz, 0.5)

    def test_settle_z_unavailable_falls_back(self):
        self.slot.strike = None
        self.slot.strike_twap = None
        with mock.patch.object(config, "ENTRY_SIGNAL", "settle_z"):
            self.assertEqual(self.bot._entry_signal(self.slot, 60.0, 80005.0, -1.2), (-1.2, None))

    def test_direction_conflict_still_uses_bot_z(self):
        import maker_guard
        ok, reason, _, _ = maker_guard.check_quote("DOWN", 0.9, -0.6, 0.25)
        self.assertFalse(ok)
        self.assertEqual(reason, maker_guard.REASON_DIRECTION_CONFLICT)
