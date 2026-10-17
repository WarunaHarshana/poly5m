"""inv14 LOG-ONLY slot_trace + chase_shadow."""
import shutil, tempfile, unittest
from pathlib import Path
from unittest import mock

import config
import shadow_log


class TestTracer(unittest.TestCase):
    def test_chase_once_in_band_before_70s(self):
        t = shadow_log.SlotTracer()
        t.start("btc", "s1", "UP", "tu", "td", "o1", 1000.0)
        st = t.get("btc", "s1")
        self.assertIsNone(t.chase(st, 10.0, 0.50, 0.52))   # below band
        self.assertIsNone(t.chase(st, 75.0, 0.58, 0.60))   # too late (not consumed)
        rec = t.chase(st, 20.0, 0.58, 0.60)
        self.assertEqual(rec["shares_at_3usd"], 5.0)
        self.assertEqual(rec["mid"], 0.59)
        self.assertIsNone(t.chase(st, 21.0, 0.58, 0.60))   # at most one per slot
        self.assertIsNone(t.get("btc", "s2"))              # new slot drops old state


class TestBot(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from bot import TWAPBot
        self.tmp = Path(tempfile.mkdtemp())
        self.saved = {k: getattr(config, k) for k in ("MODE", "SLOT_TRACE", "CHASE_SHADOW")}
        config.MODE = "paper_live"
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.tmp
        self.slot = self.bot.slots[next(iter(self.bot.slots))]
        self.slot.market = {"slug": "s1", "slot_start_ts": 1000.0}
        self.bot._slot_trace.start(self.slot.asset, "s1", "UP", "tu", "td", "o1", 1000.0)

    async def asyncTearDown(self):
        for k, v in self.saved.items():
            setattr(config, k, v)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _events(self, now, top):
        with mock.patch.object(self.bot.book_stream, "get_top", return_value=top), \
                mock.patch("bot.journal_write") as jw:
            self.bot._log_slot_trace(self.slot, now)
        return {c.args[0]: c.kwargs for c in jw.call_args_list}

    def test_trace_after_fill_and_chase(self):
        self.slot.resting_order = None
        self.slot.position = {"order_id": "o1"}
        ev = self._events(1030.0, {"bid": 0.58, "ask": 0.60})
        self.assertEqual(ev["slot_trace"]["order_state"], "filled")
        self.assertEqual(ev["slot_trace"]["mid"], 0.59)
        self.assertTrue(ev["chase_shadow"]["maker_filled"])
        self.slot.position = None  # exited: keep tracing until 300s, still 'filled'
        ev = self._events(1290.0, {"bid": 0.9, "ask": 0.92})
        self.assertEqual(ev["slot_trace"]["slot_elapsed_s"], 290.0)
        self.assertNotIn("chase_shadow", ev)
        self.assertEqual(self._events(1300.5, {"bid": 0.9, "ask": 0.92}), {})

    def test_flags_off_and_throttle(self):
        self.slot.resting_order = {"order_id": "o1"}
        ev = self._events(1010.0, {"bid": 0.49, "ask": 0.51})
        self.assertEqual(ev["slot_trace"]["order_state"], "resting")
        self.assertEqual(self._events(1012.0, {"bid": 0.49, "ask": 0.51}), {})  # 5s throttle
        config.SLOT_TRACE = config.CHASE_SHADOW = False
        self.assertEqual(self._events(1020.0, {"bid": 0.58, "ask": 0.60}), {})
