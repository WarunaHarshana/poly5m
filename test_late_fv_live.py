"""LIVE late_fv taker: FOK via _sz_taker_fok, cap, gates, hold-to-settle, maker block, auto-stop, persistence."""
import asyncio, json, unittest
from unittest import mock

import config
import bot as botmod
from test_late_fv_shadow import TestBot as _Base, S, book


class FakeLive:
    def __init__(self, filled=True):
        self.filled, self.calls, self.max_usd = filled, [], None

    def place_order(self, token_id, side, price, size, order_type="GTC", max_usd=None):
        self.max_usd = max_usd
        self.calls.append((token_id, side, price, size, order_type))
        return {"success": True, "filled": self.filled, "order_id": "oid", "shares": size if self.filled else 0.0,
                "status": "matched" if self.filled else "unmatched"}


class TestLateFvLive(_Base):
    def setUp(self):
        super().setUp()
        mock.patch.object(config, "LATE_FV_LIVE", True).start()
        mock.patch.object(config, "LATE_FV_MAX_USD", 5.0).start()
        mock.patch.object(config, "LATE_FV_STOP_USD", 6.0).start()
        self.bot.live_client = FakeLive()
        self.bot.risk.can_trade = lambda open_notional=0.0: (True, "")

    def test_never_places_orders(self):
        pass  # shadow-only invariant; not applicable with LATE_FV_LIVE on

    def newbot(self):
        with mock.patch.object(config, "MODE", "paper"):
            return botmod.TWAPBot()

    def skips(self):
        return [e["reason"] for e in self.ev("late_fv_skip")]

    def test_fill_fok_at_ask_and_tags(self):
        self.run_t(range(215, 230))
        self.assertEqual(self.bot.live_client.calls, [("tu", "BUY", 0.30, 5.0, "FOK")])
        self.assertEqual(self.bot.live_client.max_usd, 5.0)
        p = self.slot.position
        self.assertEqual((p["strategy"], p["entry_type"]), ("late_fv", "late_fv"))
        self.assertTrue(self.slot.sz_taker_block)
        self.assertEqual(len(self.ev("late_fv_attempt")), 1)
        self.assertEqual(len(self.ev("late_fv_fill")), 1)
        self.assertEqual(len(self.ev("late_fv_shadow")), 1)

    def test_skip_reasons(self):
        for setup, reason in [
            (lambda: setattr(self.slot, "position", {"strategy": "H-004_MAKER", "side": "UP"}), "position_open"),
            (lambda: setattr(self.slot, "resting_order", {"id": "x"}), "maker_resting"),
            (lambda: self.bot.risk.__setattr__("can_trade", lambda open_notional=0.0: (False, "no bankroll")), "risk_manager"),
        ]:
            self.tearDown(); self.setUp()
            setup()
            self.run_t(range(215, 230))
            self.assertEqual(self.skips(), [reason])
            self.assertEqual(self.bot.live_client.calls, [])
            self.assertEqual(len(self.ev("late_fv_shadow")), 1)

    def test_cost_cap(self):
        mock.patch.object(config, "LATE_FV_MAX_USD", 1.0).start()
        self.run_t(range(215, 230))
        self.assertEqual(self.skips(), ["cost_above_max"])
        self.assertEqual(self.bot.live_client.calls, [])

    def test_vol_fallback_skip(self):
        self.cl.ticks = [(S - 100 + i, 100.0) for i in range(100)] + [(S + i, 100.05) for i in range(300)]
        self.run_t(range(215, 230))
        sh = self.ev("late_fv_shadow")
        self.assertTrue(sh and sh[0]["vol_fallback"])
        self.assertEqual(self.skips(), ["vol_fallback"])
        self.assertEqual(self.bot.live_client.calls, [])

    def test_unfilled_fok(self):
        self.bot.live_client = FakeLive(filled=False)
        self.run_t(range(215, 230))
        self.assertEqual(self.skips(), ["fok_unfilled"])
        self.assertIsNone(self.slot.position)
        self.assertTrue(self.slot.sz_taker_block)

    def test_maker_post_blocked_after_fill(self):
        self.run_t(range(215, 230))
        sig = mock.MagicMock(side="UP", target_bid=0.5)
        asyncio.run(self.bot._place_maker_bid(self.slot, self.market, sig))
        self.assertEqual(len(self.bot.live_client.calls), 1)

    def test_hold_to_settlement_no_exits(self):
        self.run_t(range(215, 230))
        self.bot.book_stream.get_top = lambda tok, opp_token_id=None: {"bid": 0.02, "ask": 0.04}
        ex = mock.AsyncMock()
        self.bot._exit_position_early = ex
        for rem in (60, 30, 10, 3):
            asyncio.run(self.bot._manage_open_momentum_position(self.slot, self.market, rem, self.market["tokens"],
                                                                elapsed_s=300 - rem, now=S + 300 - rem))
        ex.assert_not_called()
        self.assertIsNotNone(self.slot.position)

    def test_auto_stop_persists(self):
        for i, pnl in enumerate([-2.5, -2.0, -1.6]):
            self.bot._save_trade_record({"strategy": "late_fv", "slot": f"s{i}", "won": False, "pnl": pnl})
        st = json.load(open(self.tmp / "late_fv_state.json"))
        self.assertTrue(st["stopped"]); self.assertEqual(st["n"], 3); self.assertAlmostEqual(st["pnl"], -6.1, 6)
        self.assertEqual(len(self.ev("late_fv_stopped")), 1)
        b2 = self.newbot()
        b2.live_client = self.bot.live_client
        self.bot = b2; self.bot.cl_stream = self.cl
        self.bot.pub_client.get_order_book = lambda tok: self.books[tok]
        self.bot.book_stream.get_top = lambda tok, opp_token_id=None: {"ask": 0.3, "age_s": 0.1}
        self.bot.risk.can_trade = lambda open_notional=0.0: (True, "")
        self.slot = self.bot.slots["btc"]
        self.run_t(range(215, 230))
        self.assertEqual(self.skips(), ["stopped"])
        self.assertEqual(len(self.ev("late_fv_shadow")), 1)
        self.assertEqual(self.bot.live_client.calls, [])

    def test_dup_slot_not_double_counted(self):
        for _ in range(2):
            self.bot._save_trade_record({"strategy": "late_fv", "slot": "s1", "won": True, "pnl": 1.0})
        st = json.load(open(self.tmp / "late_fv_state.json"))
        self.assertEqual(st["n"], 1)

    def test_pending_survives_restart(self):
        self.bot.pub_client.get_resolution = lambda *a, **k: None
        self.run_t(range(215, 230))
        self.assertEqual(len(self.bot._late_fv_pending), 1)
        b2 = self.newbot()
        self.assertEqual(len(b2._late_fv_pending), 1)
        self.assertEqual(b2._late_fv_pending[0]["slot"], "btc-updown-5m-1")


del _Base
if __name__ == "__main__":
    unittest.main()


class TestLateFvMinNotional(unittest.TestCase):
    def test_sizes(self):
        from late_fv import live_shares
        n = live_shares(0.19, 5, 5.0)
        self.assertGreaterEqual(round(0.19, 2) * n, 1.0)
        self.assertEqual(n, 6.0)
        self.assertEqual(live_shares(0.05, 5, 5.0), 21.0)
        self.assertAlmostEqual(0.05 * 21, 1.05)
        self.assertEqual(live_shares(0.30, 5, 5.0), 5.0)
        self.assertEqual(live_shares(0.20, 5, 5.0), 5.0)  # exactly $1.00 ok
        self.assertEqual(live_shares(0.01, 5, 0.5), 5.0)  # cap blocks upsizing


def _low_ask_test(self):
    self.books = {"tu": book(asks=[(0.05, 30)], bids=[(0.04, 10)]), "td": book(asks=[(0.96, 10)], bids=[(0.94, 10)])}
    self.bot.pub_client.get_order_book = lambda tok: self.books[tok]
    self.run_t(range(215, 230))
    self.assertEqual(self.bot.live_client.calls, [("tu", "BUY", 0.05, 21.0, "FOK")])
    sh = self.ev("late_fv_shadow")[0]
    self.assertEqual(sh["shares"], 21.0)
    self.assertTrue(sh["fok_would_fill"])
    self.assertEqual(self.ev("late_fv_attempt")[0]["shares"], 21.0)
    self.assertEqual(self.slot.position["shares"], 21.0)


def _low_ask_thin(self):
    self.books = {"tu": book(asks=[(0.05, 10)], bids=[(0.04, 10)]), "td": book(asks=[(0.96, 10)], bids=[(0.94, 10)])}
    self.bot.pub_client.get_order_book = lambda tok: self.books[tok]
    self.run_t(range(215, 230))
    self.assertFalse(self.ev("late_fv_shadow")[0]["fok_would_fill"])


TestLateFvLive.test_low_ask_upsizes = _low_ask_test
TestLateFvLive.test_low_ask_thin_depth = _low_ask_thin


class FakeLiveAvg(FakeLive):
    def place_order(self, token_id, side, price, size, order_type="GTC", max_usd=None):
        r = super().place_order(token_id, side, price, size, order_type, max_usd)
        r["avg_price"] = 0.31
        return r


def _slip_limit(self):
    mock.patch.object(config, "LATE_FV_SLIPPAGE", 0.02).start()
    self.bot.live_client = FakeLiveAvg()
    self.run_t(range(215, 230))
    self.assertEqual(self.bot.live_client.calls, [("tu", "BUY", 0.32, 5.0, "FOK")])
    a = self.ev("late_fv_attempt")[0]
    self.assertEqual((a["ask"], a["limit_price"], a["slippage"]), (0.30, 0.32, 0.02))
    self.assertEqual(self.slot.position["price"], 0.31)  # actual fill price used for P&L
    sh = self.ev("late_fv_shadow")[0]
    self.assertNotIn("limit_price", sh); self.assertNotIn("ladder", sh)


def _slip_default_zero(self):
    self.run_t(range(215, 230))
    a = self.ev("late_fv_attempt")[0]
    self.assertEqual((a["limit_price"], a["slippage"]), (0.30, 0.0))


def _slip_edge_skip(self):
    mock.patch.object(config, "LATE_FV_SLIPPAGE", 0.95).start()  # limit capped at 0.97 -> edge gone
    self.run_t(range(215, 230))
    self.assertEqual(self.skips(), ["edge_after_slippage"])
    self.assertEqual(self.ev("late_fv_skip")[0]["limit_price"], 0.97)
    self.assertEqual(self.bot.live_client.calls, [])


def _slip_depth(self):
    mock.patch.object(config, "LATE_FV_SLIPPAGE", 0.02).start()
    self.books = {"tu": book(asks=[(0.05, 10), (0.06, 5), (0.08, 100)], bids=[(0.04, 10)]),
                  "td": book(asks=[(0.96, 10)], bids=[(0.94, 10)])}
    self.bot.pub_client.get_order_book = lambda tok: self.books[tok]
    self.run_t(range(215, 230))
    a = self.ev("late_fv_attempt")[0]
    self.assertEqual(a["limit_price"], 0.07)
    self.assertEqual(a["shares"], 15.0)  # ceil(1.01/0.07)
    self.assertEqual(a["depth_at_limit"], 25.0)  # 10@.05 + 5@.06 + complement 10@.06 (1-.94); .08 excluded
    self.assertTrue(a["fok_would_fill"])


def _ask_below_tick(self):
    rec = {"slot": "s", "side": "UP", "best_ask": 0.005, "shares": 5, "model_prob": 0.9, "t": 230}
    asyncio.run(self.bot._late_fv_live(self.slot, self.market, rec, None))
    self.assertEqual(self.skips(), ["ask_below_tick"])
    self.assertEqual(self.bot.live_client.calls, [])


TestLateFvLive.test_ask_below_tick = _ask_below_tick
TestLateFvLive.test_slippage_limit = _slip_limit
TestLateFvLive.test_slippage_default_zero = _slip_default_zero
TestLateFvLive.test_slippage_edge_skip = _slip_edge_skip
TestLateFvLive.test_slippage_depth_to_limit = _slip_depth


class TestLateFvLimit(unittest.TestCase):
    def test_limit(self):
        from late_fv import limit_price, depth_upto, edge_at
        self.assertEqual(limit_price(0.30, 0.02, 0.97), 0.32)
        self.assertEqual(limit_price(0.96, 0.02, 0.97), 0.97)
        self.assertEqual(limit_price(0.305, 0.0, 0.97), 0.31)
        self.assertEqual(limit_price(0.5, 0.0, 0.97), 0.5)
        self.assertEqual(depth_upto([(0.3, 2), (0.31, 3), (0.33, 9)], 0.32), 5)
        self.assertAlmostEqual(edge_at(0.9, 0.8), round(0.9 - 0.8 - 0.07 * 0.8 * 0.2, 4))



def _maxprice_skip_then_cheaper(self):
    mock.patch.object(config, "LATE_FV_MAX_PRICE", 0.25).start()
    self.run_t(range(215, 224))  # tick 220: ask 0.30 > 0.25 -> skipped, not done
    self.assertEqual(self.bot.live_client.calls, [])
    self.assertEqual(self.ev("late_fv_shadow"), [])
    self.assertFalse(self.slot.late_fv["done"])
    self.assertFalse(getattr(self.slot, "sz_taker_block", False))
    self.assertEqual(self.slot.late_fv["skips"].get("above_max_price"), 1)
    self.books["tu"] = book(asks=[(0.20, 10)], bids=[(0.19, 10)])
    self.run_t(range(224, 320))  # tick 225: 0.20 <= cap -> trades
    self.assertEqual(self.bot.live_client.calls, [("tu", "BUY", 0.20, 5.0, "FOK")])
    self.assertEqual(self.ev("late_fv_shadow")[0]["t"], 225.0)
    scan = self.ev("late_fv_scan")
    self.assertEqual(len(scan), 1)
    self.assertEqual(scan[0]["skips"].get("above_max_price"), 1)


def _maxprice_always_above(self):
    mock.patch.object(config, "LATE_FV_MAX_PRICE", 0.25).start()
    self.run_t(range(215, 320))
    self.assertEqual(self.bot.live_client.calls, [])
    self.assertEqual(self.ev("late_fv_shadow"), [])
    self.assertGreaterEqual(self.ev("late_fv_scan")[0]["skips"]["above_max_price"], 9)  # every tick 220..265


def _maxprice_default(self):
    self.assertEqual(config.LATE_FV_MAX_PRICE, 0.97)
    self.run_t(range(215, 230))
    self.assertEqual(self.bot.live_client.calls, [("tu", "BUY", 0.30, 5.0, "FOK")])


TestLateFvLive.test_max_price_skip_then_cheaper_tick_trades = _maxprice_skip_then_cheaper
TestLateFvLive.test_max_price_skips_every_tick = _maxprice_always_above
TestLateFvLive.test_max_price_default_unchanged = _maxprice_default
