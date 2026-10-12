"""Live-readiness: exchange fill detection, cancel/confirm/adopt, pre-expiry exit, wallet bankroll,
explicit SIGNATURE_TYPE, persisted pending settlements, daily PnL reset. No network: fake CLOB client."""
from __future__ import annotations

import json
import shutil
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import config
from bot import TWAPBot
from jev_advisor import JevDecision
from polymarket_client import PolymarketLiveClient

STRIKE = 84300.0
LIVE_KEYS = ("MODE", "STRATEGY", "PRIVATE_KEY", "FUNDER_ADDRESS", "SIGNATURE_TYPE", "MAKER_TWAP_GUARD",
             "MAKER_MIN_TWAP_SIGMA", "MAKER_MAX_MID_DROP_60S", "MAKER_REQUIRE_TWAP", "MAKER_HOLD_TO_EXPIRY",
             "LIVE_ORDER_POLL_S", "LIVE_PRE_EXPIRY_EXIT_S", "LIVE_PRE_EXPIRY_MIN_BID", "TRAILING_PROFIT_ENABLED",
             "PRICE_STOP_ENABLED", "LIVE_BANKROLL_REFRESH_S")


class FakeClob:
    """Minimal stand-in for py_clob_client.ClobClient (no network, no keys)."""

    def __init__(self):
        self.orders: dict[str, dict] = {}
        self.post_responses: list = []
        self.cancel_calls: list[str] = []
        self.cancel_raises = False
        self.cancel_leaves_live = False
        self.fill_on_cancel = 0.0
        self.balance = "41890000"
        self.created = 0

    def create_order(self, args):
        self.created += 1
        return args

    def post_order(self, order, ot):
        r = self.post_responses.pop(0) if self.post_responses else {"success": True, "orderID": "o1", "status": "live"}
        if isinstance(r, Exception):
            raise r
        return r

    def get_order(self, oid):
        return dict(self.orders.get(oid) or {})

    def cancel(self, oid):
        self.cancel_calls.append(oid)
        if self.cancel_raises:
            raise RuntimeError("network")
        o = self.orders.setdefault(oid, {"status": "LIVE", "size_matched": "0", "original_size": "6"})
        if self.fill_on_cancel:
            o["size_matched"] = str(self.fill_on_cancel)
        if not self.cancel_leaves_live:
            o["status"] = "CANCELED"
        return {"canceled": [oid], "not_canceled": {}}

    def get_balance_allowance(self, params):
        return {"balance": self.balance, "allowances": {}}


def _live_env():
    config.PRIVATE_KEY = "0x" + "1" * 64  # dummy, never used for signing in tests
    config.FUNDER_ADDRESS = "0x" + "2" * 40
    config.SIGNATURE_TYPE = 1


class _LiveBase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.saved = {k: getattr(config, k) for k in LIVE_KEYS}
        config.MODE = "paper_live"
        config.STRATEGY = "JEV_DYNAMIC"
        config.MAKER_TWAP_GUARD = True
        config.MAKER_MIN_TWAP_SIGMA = 0.25
        config.MAKER_MAX_MID_DROP_60S = 0.15
        config.MAKER_REQUIRE_TWAP = True
        config.MAKER_HOLD_TO_EXPIRY = True
        config.LIVE_ORDER_POLL_S = 0.0
        config.TRAILING_PROFIT_ENABLED = False
        config.PRICE_STOP_ENABLED = False
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.temp_dir
        self.bot.risk.bankroll = 50.0
        self.bot._slot_done = set()
        self.bot._trades_log_path = self.temp_dir / "trades.json"
        self.bot._save_slot_state = lambda **kw: None
        self.bot.jev.evaluate_dynamic = AsyncMock(return_value=JevDecision(
            action="abstain", passed=False, size_multiplier=0.0, conviction=0.5, reversal_risk=0.5,
            market_regime="chop", confidence=0.2, reason="test", latency_ms=1.0, fallback_used=False,
        ))
        _live_env()
        config.MODE = "live"
        self.fake = FakeClob()
        self.bot.live_client = PolymarketLiveClient(client=self.fake)
        self.jp = patch("bot.journal_write")
        self.jp.start()

    async def asyncTearDown(self):
        self.jp.stop()
        for k, v in self.saved.items():
            setattr(config, k, v)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def _tick_resting(self, spot=84420.0, ask=0.52, bid=0.48, oid="L1", seed_high=None, shares=6.0):
        now = time.time()
        slot_start = now - 150.0
        slot = self.bot.slots["btc"]
        slot.market = {"slug": "btc-updown-5m-live-test", "slot_start_ts": slot_start, "end_ts": slot_start + 300.0,
                       "tokens": {"UP": "tok_up", "DOWN": "tok_down"}, "fee_rate": 0.0, "price_to_beat": STRIKE}
        slot.strike = STRIKE
        slot.position = None
        cl = self.bot.cl_stream
        for t in (slot_start, slot_start + 60.0, slot_start + 120.0):
            cl._record(t, STRIKE if t == slot_start else spot, asset="btc")
        cl._record(now, spot, asset="btc")
        cl._last_tick_time["btc"] = time.time()
        self.bot.book_stream._apply_update("tok_up", bid=bid, ask=ask, bid_sz=500, ask_sz=500)
        self.bot.book_stream._apply_update("tok_down", bid=round(1 - ask, 2), ask=round(1 - bid, 2), bid_sz=500, ask_sz=500)
        if seed_high is not None:
            self.bot._mid_hist.add("tok_up", now - 20.0, seed_high)
        if slot.resting_order is None:
            slot.resting_order = {
                "asset": "btc", "slot": slot.market["slug"], "side": "UP", "token_id": "tok_up", "price": 0.50,
                "shares": shares, "cost_usd": round(shares * 0.5, 4), "z_score": 1.2, "prob": 0.5, "rebate": 0.0,
                "placed_at": now - 30.0, "order_id": oid, "strike_spot": STRIKE, "strike_twap": None,
                "strike_twap_source": None, "move_ref_mode": "conservative", "move_ref_used": None,
            }
        await self.bot._tick_asset(slot, now)
        return slot


class TestLiveFillDetection(_LiveBase):
    async def test_book_cross_alone_is_not_a_live_fill(self):
        self.fake.orders["L1"] = {"status": "LIVE", "size_matched": "0", "original_size": "6"}
        slot = await self._tick_resting(ask=0.50, bid=0.48)  # ask touched our bid: paper would fill
        self.assertIsNone(slot.position)
        self.assertIsNotNone(slot.resting_order)

    async def test_exchange_full_fill_opens_position(self):
        self.fake.orders["L1"] = {"status": "MATCHED", "size_matched": "6", "original_size": "6"}
        slot = await self._tick_resting(ask=0.53, bid=0.49)  # book does NOT show a cross
        self.assertIsNotNone(slot.position)
        self.assertEqual(slot.position["shares"], 6.0)
        self.assertIsNone(slot.resting_order)

    async def test_partial_fill_keeps_resting_then_adopted_on_cancel(self):
        self.fake.orders["L1"] = {"status": "LIVE", "size_matched": "2.5", "original_size": "6"}
        slot = await self._tick_resting(ask=0.53, bid=0.49)
        self.assertIsNone(slot.position)
        self.assertAlmostEqual(slot.resting_order["live_matched"], 2.5)
        # adverse: TWAP strike crossed -> cancel; exchange shows 2.5 filled -> adopt 2.5
        slot = await self._tick_resting(spot=84250.0, ask=0.53, bid=0.49)
        self.assertEqual(self.fake.cancel_calls, ["L1"])
        self.assertIsNotNone(slot.position)
        self.assertAlmostEqual(slot.position["shares"], 2.5)
        self.assertAlmostEqual(slot.position["cost_usd"], 1.25)
        self.assertIsNone(slot.resting_order)


class TestLiveCancelConfirm(_LiveBase):
    async def test_cancel_confirmed_zero_fill_forgets_order(self):
        self.fake.orders["L1"] = {"status": "LIVE", "size_matched": "0", "original_size": "6"}
        slot = await self._tick_resting(spot=84250.0)  # twap cross -> cancel
        self.assertEqual(self.fake.cancel_calls, ["L1"])
        self.assertIsNone(slot.resting_order)
        self.assertIsNone(slot.position)

    async def test_fill_racing_the_cancel_is_adopted(self):
        self.fake.orders["L1"] = {"status": "LIVE", "size_matched": "0", "original_size": "6"}
        self.fake.fill_on_cancel = 6.0  # filled between our last poll and the cancel
        slot = await self._tick_resting(spot=84250.0)
        self.assertIsNotNone(slot.position)
        self.assertEqual(slot.position["shares"], 6.0)
        self.assertIsNone(slot.resting_order)

    async def test_unconfirmed_cancel_is_kept_and_retried(self):
        self.fake.orders["L1"] = {"status": "LIVE", "size_matched": "0", "original_size": "6"}
        self.fake.cancel_leaves_live = True
        slot = await self._tick_resting(spot=84250.0)
        self.assertIsNotNone(slot.resting_order)
        self.assertTrue(slot.resting_order["cancel_pending"])
        self.fake.cancel_leaves_live = False
        slot = await self._tick_resting(spot=84420.0)  # conditions cleared, but pending cancel is retried
        self.assertEqual(self.fake.cancel_calls, ["L1", "L1"])
        self.assertIsNone(slot.resting_order)

    async def test_window_rollover_finalizes_and_adopts(self):
        slot = self.bot.slots["btc"]
        slot.market = {"slug": "btc-updown-5m-1", "end_ts": time.time() - 1}
        slot.strike = STRIKE
        slot.position = None
        slot.resting_order = {"asset": "btc", "slot": "btc-updown-5m-1", "side": "UP", "token_id": "tok_up",
                              "price": 0.5, "shares": 6.0, "cost_usd": 3.0, "order_id": "L9"}
        self.fake.orders["L9"] = {"status": "LIVE", "size_matched": "0", "original_size": "6"}
        self.fake.fill_on_cancel = 6.0
        self.bot._live_finalize_resting(slot, "window_rollover")
        self.assertIsNone(slot.resting_order)
        self.assertEqual(slot.position["shares"], 6.0)
        self.bot._queue_settlement(slot)
        self.assertEqual(len(self.bot._pending_settles), 1)


class TestLiveClient(unittest.TestCase):
    def setUp(self):
        self.saved = {k: getattr(config, k) for k in ("PRIVATE_KEY", "FUNDER_ADDRESS", "SIGNATURE_TYPE")}
        _live_env()

    def tearDown(self):
        for k, v in self.saved.items():
            setattr(config, k, v)

    def test_signature_type_required_and_validated(self):
        for bad in (None, 4, -1):
            config.SIGNATURE_TYPE = bad
            with self.assertRaises(ValueError):
                PolymarketLiveClient(client=FakeClob())
        for ok in (0, 1, 2, 3):
            config.SIGNATURE_TYPE = ok
            self.assertEqual(PolymarketLiveClient(client=FakeClob()).signature_type, ok)

    def test_no_silent_signature_fallback(self):
        fake = FakeClob()
        fake.post_responses = [RuntimeError("invalid signature")]
        c = PolymarketLiveClient(client=fake)
        r = c.place_order("tok", "BUY", 0.5, 6, "GTC")
        self.assertFalse(r["success"])
        self.assertEqual(fake.created, 1)
        self.assertIs(c.client, fake)
        self.assertEqual(c.signature_type, 1)

    def test_post_response_parsing(self):
        fake = FakeClob()
        fake.post_responses = [
            {"success": True, "orderID": "s1", "status": "matched", "makingAmount": "6", "takingAmount": "5.7"},
            {"success": True, "orderID": "s2", "status": "unmatched", "makingAmount": "0", "takingAmount": "0"},
            {"success": True, "orderID": "b1", "status": "live"},
            {"success": False, "errorMsg": "not enough balance", "orderID": ""},
        ]
        c = PolymarketLiveClient(client=fake)
        sell = c.place_order("tok", "SELL", 0.93, 6, "FAK")
        self.assertTrue(sell["filled"]); self.assertEqual(sell["shares"], 6.0); self.assertAlmostEqual(sell["avg_price"], 0.95)
        miss = c.place_order("tok", "SELL", 0.93, 6, "FAK")
        self.assertFalse(miss["filled"]); self.assertEqual(miss["shares"], 0.0)
        gtc = c.place_order("tok", "BUY", 0.5, 6, "GTC")
        self.assertTrue(gtc["success"]); self.assertFalse(gtc["filled"]); self.assertEqual(gtc["order_id"], "b1")
        bad = c.place_order("tok", "BUY", 0.5, 6, "GTC")
        self.assertFalse(bad["success"])

    def test_cancel_and_confirm(self):
        fake = FakeClob()
        c = PolymarketLiveClient(client=fake)
        fake.orders["x"] = {"status": "LIVE", "size_matched": "1.5", "original_size": "6"}
        r = c.cancel_and_confirm("x")
        self.assertTrue(r["confirmed"]); self.assertEqual(r["size_matched"], 1.5)
        fake.cancel_raises = True
        fake.orders["y"] = {"status": "LIVE", "size_matched": "0"}
        self.assertFalse(c.cancel_and_confirm("y")["confirmed"])

    def test_wallet_balance(self):
        fake = FakeClob()
        c = PolymarketLiveClient(client=fake)
        self.assertAlmostEqual(c.get_wallet_balance(), 41.89)
        fake.get_balance_allowance = MagicMock(side_effect=RuntimeError("down"))
        self.assertIsNone(c.get_wallet_balance())


class TestLiveBankrollAndExpiry(_LiveBase):
    async def test_bankroll_read_from_wallet(self):
        self.bot._refresh_live_bankroll(time.time(), force=True)
        self.assertAlmostEqual(self.bot.risk.bankroll, 41.89)
        self.fake.balance = "30000000"
        self.bot._refresh_live_bankroll(time.time() + 1)  # throttled
        self.assertAlmostEqual(self.bot.risk.bankroll, 41.89)
        config.LIVE_BANKROLL_REFRESH_S = 0.0
        self.bot._refresh_live_bankroll(time.time() + 100)
        self.assertAlmostEqual(self.bot.risk.bankroll, 30.0)

    async def test_wallet_reserve_is_excluded(self):
        saved = config.WALLET_RESERVE_USD
        config.WALLET_RESERVE_USD = 10.0
        try:
            self.bot._refresh_live_bankroll(time.time(), force=True)
            self.assertAlmostEqual(self.bot.risk.bankroll, 31.89)
        finally:
            config.WALLET_RESERVE_USD = saved

    async def test_live_kill_switches_and_exposure_aware_daily_limit(self):
        saved = (config.DAILY_LOSS_LIMIT_USD, config.MAX_CONSECUTIVE_LOSSES, config.BASE_STAKE_USD)
        config.DAILY_LOSS_LIMIT_USD, config.MAX_CONSECUTIVE_LOSSES, config.BASE_STAKE_USD = 5.0, 3, 2.5
        try:
            r = self.bot.risk
            r.bankroll, r.daily_pnl, r.consecutive_losses = 40.0, 0.0, 0
            self.assertTrue(r.can_trade(open_notional=0.0)[0])
            r.daily_pnl = -2.5  # one 5-share loss; another 2.5 worst case would reach exactly -5
            self.assertTrue(r.can_trade(open_notional=0.0)[0])
            self.assertFalse(r.can_trade(open_notional=2.5)[0])  # open position + new stake breach
            r.daily_pnl = -5.0
            self.assertFalse(r.can_trade()[0])
            r.daily_pnl, r.consecutive_losses = 0.0, 3
            ok, why = r.can_trade()
            self.assertFalse(ok); self.assertIn("consecutive", why)
            config.MODE = "paper_live"  # paper keeps filling
            self.assertTrue(r.can_trade()[0])
        finally:
            config.DAILY_LOSS_LIMIT_USD, config.MAX_CONSECUTIVE_LOSSES, config.BASE_STAKE_USD = saved

    async def test_unreadable_wallet_at_startup_blocks_entries(self):
        self.fake.get_balance_allowance = MagicMock(side_effect=RuntimeError("down"))
        self.bot._refresh_live_bankroll(time.time(), force=True)
        self.assertEqual(self.bot.risk.bankroll, 0.0)
        ok, _ = self.bot.risk.can_trade()
        self.assertFalse(ok)

    async def _held(self, bid, remaining):
        now = time.time()
        slot_start = now - (300.0 - remaining)
        market = {"slug": "btc-updown-5m-live-hold", "slot_start_ts": slot_start, "end_ts": slot_start + 300.0,
                  "tokens": {"UP": "tok_up", "DOWN": "tok_down"}, "fee_rate": 0.0}
        slot = self.bot.slots["btc"]
        slot.market = market
        slot.strike = STRIKE
        slot.resting_order = None
        slot.position = {"asset": "btc", "slot": market["slug"], "strategy": "JEV_DYNAMIC_MAKER", "side": "UP",
                         "token_id": "tok_up", "opp_token_id": "tok_down", "price": 0.5, "shares": 6.0,
                         "cost_usd": 3.0, "fee": 0.0, "strike": STRIKE, "entry_time": now - 60,
                         "slot_start_ts": slot_start, "entry_time_utc": "2026-10-12T10:00:00+00:00"}
        book = {"bids": [{"price": f"{bid}", "size": "500"}], "asks": [{"price": f"{bid + 0.02:.2f}", "size": "500"}]}
        self.bot.pub_client.get_order_book = MagicMock(side_effect=lambda tok: book)
        self.bot.book_stream._apply_update("tok_up", bid=bid, ask=round(bid + 0.02, 2), bid_sz=500, ask_sz=500)
        for i in range(30):
            self.bot.cl_stream._record(now - 30 + i, STRIKE + 50, asset="btc")
        await self.bot._manage_open_momentum_position(slot, market, remaining, market["tokens"], spot=STRIKE + 50,
                                                      strike=STRIKE, realized_twap=STRIKE + 50,
                                                      elapsed_s=300.0 - remaining, now=now)
        return slot

    async def test_pre_expiry_exit_sells_likely_winner(self):
        self.fake.post_responses = [{"success": True, "orderID": "s", "status": "matched",
                                     "makingAmount": "6", "takingAmount": "5.1"}]
        slot = await self._held(bid=0.86, remaining=15.0)
        self.assertIsNone(slot.position)
        t = json.loads(self.bot._trades_log_path.read_text())[-1]
        self.assertEqual(t["exit_reason"], "live_pre_expiry_exit")
        self.assertAlmostEqual(t["exit_price"], 0.85, places=2)

    async def test_no_pre_expiry_exit_early_or_for_losers(self):
        slot = await self._held(bid=0.86, remaining=60.0)
        self.assertIsNotNone(slot.position)
        slot = await self._held(bid=0.10, remaining=10.0)
        self.assertIsNotNone(slot.position)
        self.assertEqual(self.fake.created, 0)

    async def test_paper_has_no_pre_expiry_exit(self):
        config.MODE = "paper_live"
        slot = await self._held(bid=0.86, remaining=10.0)
        self.assertIsNotNone(slot.position)

    async def test_live_settlement_win_flags_redeem(self):
        pos = {"asset": "btc", "slot": "btc-updown-5m-1", "strategy": "JEV_DYNAMIC_MAKER", "side": "UP",
               "token_id": "tok_up", "price": 0.5, "shares": 6.0, "cost_usd": 3.0, "fee": 0.0}
        self.bot._settle_filled({"position": pos, "market": {"slug": "btc-updown-5m-1"}, "asset": "btc"},
                                {"side": "UP", "source": "polymarket_outcome_prices", "official": True})
        t = json.loads(self.bot._trades_log_path.read_text())[-1]
        self.assertTrue(t["redeem_required"])
        q = json.loads((config.STATE_DIR / "redeem_queue.json").read_text())
        self.assertEqual(q[0]["token_id"], "tok_up")


class TestPendingPersistence(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.saved = config.MODE
        config.MODE = "paper_live"
        self.jp = patch("bot.journal_write")
        self.jp.start()

    async def asyncTearDown(self):
        self.jp.stop()
        config.MODE = self.saved

    async def test_pending_survives_restart_and_settles(self):
        bot = TWAPBot()
        bot._trades_log_path = config.STATE_DIR / "trades.json"
        slot = bot.slots["btc"]
        slot.market = {"slug": "btc-updown-5m-1791559500", "end_ts": time.time() - 30, "condition_id": "0xc"}
        slot.strike = STRIKE
        slot.position = {"asset": "btc", "slot": "btc-updown-5m-1791559500", "strategy": "JEV_DYNAMIC_MAKER",
                         "side": "UP", "price": 0.5, "shares": 6.0, "cost_usd": 3.0, "fee": 0.0,
                         "maker_features": {"twap_sigma": 0.4}}
        bot._queue_settlement(slot)
        self.assertTrue((config.STATE_DIR / "pending_settlements.json").exists())
        # "redeploy": a new process restores the queue from disk
        bot2 = TWAPBot()
        bot2._trades_log_path = config.STATE_DIR / "trades.json"
        bot2.risk.state_dir = config.STATE_DIR
        bot2.risk.bankroll = 50.0
        self.assertEqual(len(bot2._pending_settles), 1)
        self.assertEqual(bot2.snapshot()["pending_settlements"][0]["slug"], "btc-updown-5m-1791559500")
        bot2.pub_client.get_resolution = MagicMock(return_value={"side": "DOWN", "source": "polymarket_outcome_prices",
                                                                  "official": True})
        await bot2._flush_pending_settlements(time.time())
        self.assertEqual(bot2._pending_settles, [])
        self.assertEqual(json.loads((config.STATE_DIR / "pending_settlements.json").read_text()), [])
        self.assertAlmostEqual(json.loads(bot2._trades_log_path.read_text())[-1]["pnl"], -3.0)


class TestDailyPnlReset(unittest.TestCase):
    def test_reconcile_sets_today_only(self):
        saved = config.MODE
        config.MODE = "paper_live"
        try:
            bot = TWAPBot()
            d = Path(tempfile.mkdtemp())
            bot.risk.state_dir = d
            bot._trades_log_path = d / "trades.json"
            now = datetime.now(timezone.utc).isoformat()
            rows = [
                {"slot": "a", "won": False, "pnl": -30.0, "official": True, "exit_time_utc": "2026-10-01T10:00:00+00:00"},
                {"slot": "b", "won": True, "pnl": 2.72, "official": True, "exit_time_utc": now},
                {"slot": "c", "won": False, "pnl": -3.0, "official": True, "timestamp": now},
            ]
            bot._trades_log_path.write_text(json.dumps(rows))
            bot._reconcile_paper_blotter()
            self.assertAlmostEqual(bot.risk.bankroll, config.STARTING_BANKROLL - 30.28, places=2)
            self.assertAlmostEqual(bot.risk.daily_pnl, -0.28, places=2)
            shutil.rmtree(d, ignore_errors=True)
        finally:
            config.MODE = saved


if __name__ == "__main__":
    unittest.main()
