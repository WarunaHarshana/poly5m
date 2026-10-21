"""LIVE settle_z taker: decision gates, sizing, maker interplay, hold-to-expiry, stop rule."""
import asyncio, shutil, tempfile, unittest
from pathlib import Path
from unittest import mock

import config
import bot as botmod


class FakeLive:
    def __init__(self, filled=True):
        self.filled = filled
        self.calls = []

    def place_order(self, token_id, side, price, size, order_type="GTC", max_usd=None):
        self.max_usd = max_usd
        self.calls.append((token_id, side, price, size, order_type))
        return {"success": True, "filled": self.filled, "order_id": "oid1", "shares": size if self.filled else 0.0,
                "status": "matched" if self.filled else "unmatched"}


class TestSettleZTaker(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.p = [mock.patch.object(config, "STATE_DIR", self.tmp), mock.patch.object(config, "SETTLE_Z_TAKER", True),
                  mock.patch.object(config, "ENTRY_BLOCK_OFF_UTC", "")]
        for x in self.p:
            x.start()
        self.bot = botmod.TWAPBot()
        mock.patch.object(config, "MODE", "live").start()
        self.bot.risk.state_dir = self.tmp
        self.bot.risk.can_trade = lambda open_notional=0.0: (True, "")
        self.bot.live_client = FakeLive()
        self.slot = self.bot.slots["btc"]
        self.market = {"slug": "btc-updown-5m-1", "tokens": {"UP": "tu", "DOWN": "td"}, "slot_start_ts": 1.0}
        self.z = 0.7
        self.bot._settle_z_now = lambda slot, e, spot=None: self.z
        self.top = {"ask": 0.75, "bid": 0.74}
        self.bot.book_stream.get_top = lambda tok, opp_token_id=None: dict(self.top)
        self.jw = mock.patch("bot.journal_write").start()

    def tearDown(self):
        mock.patch.stopall()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_(self, el=61.0):
        asyncio.run(self.bot._maybe_settle_z_taker(self.slot, self.market, el, 80000.0))

    def events(self, name):
        return [c.kwargs for c in self.jw.call_args_list if c.args and c.args[0] == name]

    def test_fill_sizes_5_shares_fok_and_holds(self):
        self.run_()
        self.assertEqual(self.bot.live_client.calls, [("tu", "BUY", 0.75, 5.0, "FOK")])
        self.assertEqual(self.bot.live_client.max_usd, 4.0)
        self.assertEqual(self.slot.position["strategy"], "settle_z_taker")
        self.assertEqual(self.slot.position["entry_type"], "settle_z_taker")
        self.assertTrue(self.slot.sz_taker_block)
        self.assertEqual(len(self.events("settle_z_taker_attempt")), 1)
        self.assertEqual(len(self.events("settle_z_taker_fill")), 1)

    def test_down_side(self):
        self.z = -0.6
        self.run_()
        self.assertEqual(self.bot.live_client.calls[0][0], "td")

    def test_skips(self):
        cases = [
            (dict(z=0.3), "z_below_min"),
            (dict(top={"ask": 0.81, "bid": 0.80}), "ask_above_max"),
            (dict(top={"ask": 0.70, "bid": 0.67}), "spread_above_max"),
        ]
        for kw, reason in cases:
            self.jw.reset_mock(); self.slot.position = None
            self.z = kw.get("z", 0.7); self.top = kw.get("top", {"ask": 0.75, "bid": 0.74})
            self.run_()
            self.assertEqual(self.events("settle_z_taker_skip")[-1]["reason"], reason)
        self.assertEqual(self.bot.live_client.calls, [])

    def test_cost_cap(self):
        with mock.patch.object(config, "SETTLE_Z_TAKER_MAX_USD", 3.5):
            self.run_()
        self.assertEqual(self.events("settle_z_taker_skip")[-1]["reason"], "cost_above_max")

    def test_flag_off_and_non_btc(self):
        with mock.patch.object(config, "SETTLE_Z_TAKER", False):
            self.run_()
        self.slot.asset = "eth"
        self.run_()
        self.slot.asset = "btc"
        self.assertEqual(self.bot.live_client.calls, [])
        self.assertEqual(self.jw.call_count, 0)

    def test_risk_manager_blocks(self):
        self.bot.risk.can_trade = lambda open_notional=0.0: (False, "daily loss")
        self.run_()
        self.assertEqual(self.events("settle_z_taker_skip")[-1]["reason"], "risk_manager")
        self.assertEqual(self.bot.live_client.calls, [])

    def test_existing_position_skips(self):
        self.slot.position = {"strategy": "JEV_DYNAMIC_MAKER"}
        self.run_()
        self.assertEqual(self.events("settle_z_taker_skip")[-1]["reason"], "position_exists")

    def test_resting_maker_cancelled_then_enter(self):
        self.slot.resting_order = {"order_id": "m1", "price": 0.45}
        fin = mock.patch.object(self.bot, "_live_finalize_resting").start()
        self.run_()
        fin.assert_called_once()
        self.assertIsNone(self.slot.resting_order)
        self.assertEqual(len(self.bot.live_client.calls), 1)

    def test_resting_maker_adopted_fill_skips(self):
        self.slot.resting_order = {"order_id": "m1"}
        def adopt(slot, reason):
            slot.position = {"strategy": "JEV_DYNAMIC_MAKER"}
        mock.patch.object(self.bot, "_live_finalize_resting", side_effect=adopt).start()
        self.run_()
        self.assertEqual(self.events("settle_z_taker_skip")[-1]["reason"], "maker_position_exists")
        self.assertEqual(self.bot.live_client.calls, [])

    def test_maker_post_blocked_after_taker(self):
        self.slot.sz_taker_block = True
        sig = mock.Mock(side="UP", target_bid=0.45)
        asyncio.run(self.bot._place_maker_bid(self.slot, self.market, sig))
        self.assertIsNone(self.slot.resting_order)

    def test_fok_unfilled(self):
        self.bot.live_client = FakeLive(filled=False)
        self.run_()
        self.assertIsNone(self.slot.position)
        self.assertEqual(self.events("settle_z_taker_skip")[-1]["reason"], "fok_unfilled")

    def test_60s_tick_not_in_entry_block_zone(self):
        self.assertLess(60.0, config.ENTRY_BLOCK_ELAPSED_LO)
        self.run_(el=61.0)
        self.assertEqual(len(self.bot.live_client.calls), 1)

    def test_late_tick_in_zone_skipped(self):
        with mock.patch.object(config, "ENTRY_BLOCK_ENABLED", True):
            self.run_(el=75.0)
        self.assertEqual(self.events("settle_z_taker_skip")[-1]["reason"], "entry_block_zone")

    def test_hold_no_take_profit(self):
        self.run_()
        self.top = {"ask": 0.99, "bid": 0.98}
        ex = mock.patch.object(self.bot, "_exit_position_early").start()
        asyncio.run(self.bot._manage_open_momentum_position(self.slot, self.market, 30.0, self.market["tokens"]))
        ex.assert_not_called()
        self.assertIsNotNone(self.slot.position)

    def test_stop_rule_wr_after_40_persists(self):
        for i in range(40):
            self.bot._sz_taker_note_result({"slot": f"s{i}", "won": i < 29, "pnl": 1.0 if i < 29 else -0.1})
        st = self.bot._sz_taker_state()
        self.assertTrue(st["stopped"])
        self.assertEqual(len(self.events("settle_z_taker_stopped")), 1)
        with mock.patch.object(config, "MODE", "paper"):
            bot2 = botmod.TWAPBot()  # restart keeps it disabled
        self.assertTrue(bot2._sz_taker_state()["stopped"])
        self.run_()
        self.assertEqual(self.events("settle_z_taker_skip")[-1]["reason"], "stopped")
        self.assertEqual(self.bot.live_client.calls, [])

    def test_stop_rule_early_pnl_and_dedup(self):
        for i in range(3):
            self.bot._sz_taker_note_result({"slot": f"s{i}", "won": False, "pnl": -3.0})
        self.bot._sz_taker_note_result({"slot": "s0", "won": False, "pnl": -3.0})
        self.assertFalse(self.bot._sz_taker_state()["stopped"])
        self.bot._sz_taker_note_result({"slot": "s3", "won": False, "pnl": -1.0})
        st = self.bot._sz_taker_state()
        self.assertEqual(st["n"], 4)
        self.assertTrue(st["stopped"])

    def test_no_stop_good_run(self):
        self.assertIsNone(botmod.sz_taker_stop_reason(40, 31, 5.0))
        self.assertIsNotNone(botmod.sz_taker_stop_reason(40, 29, 5.0))
        self.assertIsNone(botmod.sz_taker_stop_reason(39, 20, 5.0))

    def test_shadow_logs_ask_spread(self):
        self.slot.strike = 80000.0
        self.bot._maybe_log_settle_z_shadow(self.slot, self.market, 60.5, 80020.0, 0.9, "UP", 0.75)
        kw = self.events("settle_z_shadow")[0]
        self.assertEqual(kw["ask"], 0.75)
        self.assertEqual(kw["spread"], 0.01)

    def test_rest_fallback_when_ws_book_missing(self):
        self.bot.book_stream.get_top = lambda tok, opp_token_id=None: None
        with mock.patch("bot.extract_market_top", return_value=(0.77, 0.78, None)), \
                mock.patch.object(self.bot.pub_client, "get_order_book", return_value={}):
            self.run_()
        self.assertEqual(self.bot.live_client.calls, [("tu", "BUY", 0.78, 5.0, "FOK")])

    def test_trade_record_tagged(self):
        self.bot._save_trade_record({"strategy": "settle_z_taker", "slot": "x", "won": True, "pnl": 1.2})
        import json
        rows = json.load(open(self.bot._trades_log_path))
        self.assertEqual(rows[-1]["entry_type"], "settle_z_taker")
        self.assertEqual(self.bot._sz_taker_state()["n"], 1)


def test_client_cap_override():
    import polymarket_client as pc
    c = pc.PolymarketLiveClient.__new__(pc.PolymarketLiveClient)
    import config as cfg
    with mock.patch.object(cfg, "MAX_TRADE_USD", 3.0):
        r = c.place_order("t", "BUY", 0.80, 5, order_type="FOK")
        assert r["error"] == "exceeds MAX_TRADE_USD"
        r = c.place_order("t", "BUY", 0.81, 5, order_type="FOK", max_usd=4.0)
        assert r["error"] == "exceeds MAX_TRADE_USD"
