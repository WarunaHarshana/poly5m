"""LOG-ONLY late fair-value taker shadow: math, window/once-per-slot gating, scan rows, settlement, no orders."""
import asyncio, inspect, math, shutil, tempfile, unittest
from pathlib import Path
from unittest import mock

import config
import bot as botmod
import late_fv

S = 1_000_000.0  # slot start


def book(asks=(), bids=()):
    return {"asks": [{"price": str(p), "size": str(s)} for p, s in asks],
            "bids": [{"price": str(p), "size": str(s)} for p, s in bids], "timestamp": str(int((S + 230) * 1000))}


class FakeCL:
    def __init__(self, ticks):
        self.ticks = ticks

    def history_snapshot(self, asset=None):
        return list(self.ticks)

    def compute_realized_twap(self, start_ts, end_ts=None, asset=None):
        w = [p for t, p in self.ticks if start_ts <= t <= end_ts]
        return (sum(w) / len(w), end_ts - start_ts) if w else (0.0, 0.0)


class Boom:
    def __getattr__(self, name):
        raise AssertionError(f"order client touched: {name}")


class TestMath(unittest.TestCase):
    def test_settle_z_matches_hf_model(self):
        v = 1e-4
        z = late_fv.settle_z(100.1, 100.0, 220, v)
        self.assertAlmostEqual(z, math.log(100.1 / 100) / math.sqrt(v * v * 40), 9)
        z2 = late_fv.settle_z(100.1, 100.0, 250, v, [100.0] * 11)
        m = (11 * 100.0 + 49 * 100.1) / 60
        self.assertAlmostEqual(z2, math.log(m / 100) / math.sqrt(v * v * 50 ** 3 / 10800), 9)

    def test_edge_and_pick_larger(self):
        q_up = late_fv.side_quote(book(asks=[(0.30, 10)]), book(), 5)
        q_dn = late_fv.side_quote(book(asks=[(0.72, 10)]), book(), 5)
        pick, best = late_fv.evaluate(0.5, q_up, q_dn, 0.9, 0.03, 0.97)
        p = late_fv.phi(0.45)
        self.assertEqual(pick["side"], "UP")
        self.assertAlmostEqual(pick["edge"], round(p - 0.30 - 0.07 * 0.3 * 0.7, 4), 4)
        pick, _ = late_fv.evaluate(0.0, q_up, q_dn, 0.9, 0.03, 0.97)
        self.assertEqual(pick["side"], "UP")  # 0.5-0.3-fee > 0.03; DOWN negative

    def test_max_ask_and_depth(self):
        q = late_fv.side_quote(book(asks=[(0.98, 100)]), book(), 5)
        pick, best = late_fv.evaluate(5.0, q, {"ask": None}, 0.9, 0.03, 0.97)
        self.assertIsNone(pick)
        self.assertIsNotNone(best)
        q = late_fv.side_quote(book(asks=[(0.40, 2)]), book(bids=[(0.60, 2), (0.5, 50)]), 5)
        self.assertEqual(q["ask"], 0.40)
        self.assertEqual(q["ask_size"], 4.0)
        self.assertFalse(q["fok_fill"])

    def test_pnl_fee(self):
        self.assertAlmostEqual(late_fv.sim_pnl(True, 0.4, 5), 5 * 0.6 - 5 * 0.07 * 0.24, 6)
        self.assertAlmostEqual(late_fv.sim_pnl(False, 0.4, 5), -2.0 - 5 * 0.07 * 0.24, 6)


class TestBot(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        for x in (mock.patch.object(config, "STATE_DIR", self.tmp), mock.patch.object(config, "LATE_FV_SHADOW", True)):
            x.start()
        self.bot = botmod.TWAPBot()
        mock.patch.object(config, "MODE", "live").start()
        self.bot.live_client = Boom()
        self.place = mock.MagicMock()
        self.bot.pub_client.place_order = self.place
        ticks = [(S - 1900 + i, 100.0 * (1 + 1e-4 * math.sin(i))) for i in range(1900)]
        ticks += [(S + i, 100.05) for i in range(0, 300)]
        self.cl = FakeCL(ticks)
        self.bot.cl_stream = self.cl
        self.books = {"tu": book(asks=[(0.30, 10)], bids=[(0.29, 10)]), "td": book(asks=[(0.71, 10)], bids=[(0.70, 10)])}
        self.bot.pub_client.get_order_book = lambda tok: self.books[tok]
        self.bot.pub_client.get_resolution = lambda slug, now_ts=None, condition_id=None: {"side": "UP", "source": "x"}
        self.bot.book_stream.get_top = lambda tok, opp_token_id=None: {"ask": 0.3, "age_s": 0.1}
        self.jw = mock.patch("bot.journal_write").start()
        self.slot = self.bot.slots["btc"]
        self.market = {"slug": "btc-updown-5m-1", "tokens": {"UP": "tu", "DOWN": "td"}, "slot_start_ts": S, "end_ts": S + 300}

    def tearDown(self):
        mock.patch.stopall()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def ev(self, name):
        return [c.kwargs for c in self.jw.call_args_list if c.args and c.args[0] == name]

    def run_t(self, ts):
        for t in ts:
            asyncio.run(self.bot._tick_late_fv(self.slot, self.market, S + t))
            asyncio.run(self.bot._late_fv_settle(S + t))

    def test_no_market_scan_row(self):
        self.bot.pub_client.find_active_5m_slot = lambda now_ts=None, asset=None: None
        self.bot._load_slot_state = lambda: {}
        self.bot._discover_sol_shadow = lambda now: None
        asyncio.run(self.bot._discover_slot(int(S), S))
        scan = self.ev("late_fv_scan")
        self.assertEqual(scan[-1]["skips"], {"no_market": 1})

    def test_fires_once_in_window_and_logs_scan_and_result(self):
        self.run_t(range(200, 320))
        sh = self.ev("late_fv_shadow")
        self.assertEqual(len(sh), 1)
        r = sh[0]
        self.assertEqual(r["t"], 220.0)
        self.assertEqual(r["side"], "UP")
        for k in ("slot", "z", "model_prob", "best_ask", "ask_size", "fok_would_fill", "spread", "edge", "latency_s"):
            self.assertIn(k, r)
        self.assertTrue(r["fok_would_fill"])
        scan = self.ev("late_fv_scan")
        self.assertEqual(len(scan), 1)
        self.assertTrue(scan[0]["fired"])
        res = self.ev("late_fv_result")
        self.assertEqual(len(res), 1)
        self.assertTrue(res[0]["won"])
        self.assertAlmostEqual(res[0]["pnl"], late_fv.sim_pnl(True, 0.30, 5), 6)

    def test_no_fire_outside_window(self):
        self.run_t(list(range(0, 219)) + list(range(267, 300)))
        self.assertEqual(self.ev("late_fv_shadow"), [])

    def test_near_miss_scan_every_5s(self):
        self.books = {"tu": book(asks=[(0.97, 10)], bids=[(0.29, 10)]), "td": book(asks=[(0.97, 10)], bids=[(0.02, 10)])}
        self.run_t(range(215, 280))
        self.assertEqual(self.ev("late_fv_shadow"), [])
        scan = self.ev("late_fv_scan")
        self.assertEqual(len(scan), 1)
        self.assertEqual(scan[0]["n_checks"], 10)
        self.assertFalse(scan[0]["fired"])
        self.assertIsNotNone(scan[0]["best_edge"])

    def test_never_places_orders(self):
        self.run_t(range(200, 320))
        self.place.assert_not_called()
        self.assertEqual(len(self.ev("late_fv_shadow")), 1)
        for fn in (botmod.TWAPBot._tick_late_fv, botmod.TWAPBot._late_fv_check, botmod.TWAPBot._late_fv_settle,
                   botmod.TWAPBot._late_fv_flush_scan):
            src = inspect.getsource(fn)
            for bad in ("place_order", "live_client", "cancel_order", "_sz_taker_fok", "post_order"):
                self.assertNotIn(bad, src)
        self.assertFalse(hasattr(config, "LATE_FV_LIVE"))

    def test_flag_off_default(self):
        import importlib, os
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("LATE_FV_SHADOW", None)
            self.assertFalse(os.getenv("LATE_FV_SHADOW", "0").strip().lower() in ("yes", "y", "true", "1", "on"))


if __name__ == "__main__":
    unittest.main()
