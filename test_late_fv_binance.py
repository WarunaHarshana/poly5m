"""late_fv Binance spot source: all-Binance strike/spot/vol, stale_binance skip, both sources logged side by side."""
import asyncio, math, shutil, tempfile, unittest
from pathlib import Path
from unittest import mock

import config
import bot as botmod
import feeds
import late_fv
from test_late_fv_shadow import FakeCL, Boom, book, S


def fill_bn(bn, base, t_end, offset=0.0):
    for i in range(int(S) - 1900, int(t_end)):
        bn.on_trade(i + 0.5, base * (1 + 1e-4 * math.sin(i)) + offset if i < S else base + offset, recv_ts=i + 0.6)


class TestBinanceStream(unittest.TestCase):
    def test_bars_close_stamped_at_bar_end_and_last_trade(self):
        bn = feeds.BinanceSpotStream(ws_urls=["x"], rest_urls=[])
        bn.on_trade(10.1, 1.0); bn.on_trade(10.9, 2.0); bn.on_trade(11.2, 3.0)
        self.assertEqual(bn.history_snapshot(), [(11.0, 2.0), (11.2, 3.0)])
        self.assertAlmostEqual(bn.spot_age_s(11.7), 0.5)
        n = bn.seed_bars([(9.0, 0.5), (10.0, 0.7), (11.0, 99.0)])
        self.assertEqual(n, 2)  # bar 11.0 already live: not overwritten
        self.assertEqual(bn.history_snapshot()[:3], [(9.0, 0.5), (10.0, 0.7), (11.0, 2.0)])

    def test_twap_matches_chainlink_implementation(self):
        cl = feeds.ChainlinkRTDSStream(assets=["btc"])
        ticks = [(100.0 + i, 50.0 + (i % 7)) for i in range(120)]
        for ts, p in ticks:
            cl._record(ts, p, asset="btc")
        self.assertAlmostEqual(cl.compute_realized_twap(140, 200, "btc")[0], feeds.twap_trapezoid(ticks, 140, 200)[0], 9)


class TestBotBinance(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        for x in (mock.patch.object(config, "STATE_DIR", self.tmp), mock.patch.object(config, "LATE_FV_SHADOW", True),
                  mock.patch.object(config, "LATE_FV_SPOT_SOURCE", "binance"), mock.patch.object(config, "LATE_FV_LIVE", True)):
            x.start()
        self.bot = botmod.TWAPBot()
        mock.patch.object(config, "MODE", "live").start()
        self.bot.live_client = Boom()
        self.bot._late_fv_live = mock.AsyncMock()
        # Chainlink flat at 100.05 vs strike ~100 (z>0 => UP). Binance $80 lower level, and FLAT through the slot
        # (strike==spot => z~0). A mixed (Binance spot vs Chainlink strike) z would be hugely negative.
        cl = [(S - 1900 + i, 100.0 * (1 + 1e-4 * math.sin(i))) for i in range(1900)] + [(S + i, 100.05) for i in range(300)]
        self.bot.cl_stream = FakeCL(cl)
        self.bn = feeds.BinanceSpotStream(ws_urls=["x"], rest_urls=[])
        self.bot.bn_stream = self.bn
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

    def test_strike_spot_vol_all_binance_and_both_logged(self):
        fill_bn(self.bn, 99.0, S + 221, offset=0.0)
        asyncio.run(self.bot._tick_late_fv(self.slot, self.market, S + 220.9))
        row = self.ev("late_fv_tick")[0]
        b, c = row["binance"], row["chainlink"]
        self.assertEqual(row["primary"], "binance")
        ticks = self.bn.history_snapshot()
        exp_strike = feeds.twap_trapezoid(ticks, S - 60, S)[0]
        self.assertAlmostEqual(b["strike"], round(exp_strike, 4), 4)
        self.assertAlmostEqual(b["vol"], late_fv.vol_1s(ticks, S)[0], 12)
        self.assertEqual(b["spot"], 99.0)
        self.assertAlmostEqual(b["z"], round(late_fv.settle_z(99.0, exp_strike, 220.9, b["vol"]), 4), 3)
        self.assertLess(abs(b["z"]), 3)  # same-feed: no $-basis blowup
        self.assertGreater(c["z"], 0.5)
        self.assertLess(late_fv.settle_z(99.0, c["strike"], 220.9, b["vol"]), -10)  # what mixing would give
        self.assertNotEqual(b["strike"], c["strike"])
        self.assertAlmostEqual(b["spot_age_s"], 0.4, 3)
        self.assertIn("spot_age_s", c)
        self.assertIsNotNone(b["pick"]); self.assertIsNotNone(c["pick"])
        sh = self.ev("late_fv_shadow")
        self.assertEqual(sorted(r["spot_source"] for r in sh), ["binance", "chainlink"])
        self.assertEqual({r["spot_source"]: r["live_source"] for r in sh}, {"binance": True, "chainlink": False})
        self.assertEqual(self.bot._late_fv_live.await_count, 1)  # only the primary source may reach live
        for t in range(225, 320):
            asyncio.run(self.bot._tick_late_fv(self.slot, self.market, S + t))
            asyncio.run(self.bot._late_fv_settle(S + t))
        scan = self.ev("late_fv_scan")
        self.assertEqual(len(scan), 1)
        self.assertEqual(set(scan[0]["by_source"]), {"chainlink", "binance"})
        self.assertTrue(scan[0]["by_source"]["binance"]["fired"] and scan[0]["by_source"]["chainlink"]["fired"])
        res = self.ev("late_fv_result")
        self.assertEqual(sorted(r["spot_source"] for r in res), ["binance", "chainlink"])
        for r in res:
            self.assertAlmostEqual(r["pnl_at_limit_2c"], late_fv.sim_pnl(True, round(r["ask"] + 0.02, 2), r["shares"]), 6)

    def test_stale_binance_skips_and_chainlink_still_logged(self):
        fill_bn(self.bn, 99.0, S + 219)  # last trade at S+218.5 -> 2.4s old at t=220.9
        asyncio.run(self.bot._tick_late_fv(self.slot, self.market, S + 220.9))
        row = self.ev("late_fv_tick")[0]
        self.assertEqual(row["binance"], {"skip": "stale_binance"})
        self.assertIn("z", row["chainlink"])
        self.assertEqual(self.slot.late_fv["skips"].get("stale_binance"), 1)
        self.assertEqual([r["spot_source"] for r in self.ev("late_fv_shadow")], ["chainlink"])
        self.bot._late_fv_live.assert_not_awaited()  # chainlink is not primary -> never live

    def test_default_source_is_chainlink(self):
        with mock.patch.object(config, "LATE_FV_SPOT_SOURCE", "bogus"):
            self.assertEqual(self.bot._late_fv_primary(), "chainlink")


if __name__ == "__main__":
    unittest.main()
