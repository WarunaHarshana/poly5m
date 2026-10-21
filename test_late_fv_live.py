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
