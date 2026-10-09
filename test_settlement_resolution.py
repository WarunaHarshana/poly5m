"""Late / delayed Polymarket resolution: keep polling past RESOLUTION_TIMEOUT_S, then book official PnL."""
from __future__ import annotations

import json
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import config
from bot import TWAPBot
from polymarket_client import PolymarketPublicClient, parse_clob_resolution

LOSS = {"side": "DOWN", "source": "polymarket_outcome_prices", "official": True,
        "final_price": 89900.0, "price_to_beat": 90000.0}
WIN = {"side": "UP", "source": "polymarket_outcome_prices", "official": True,
       "final_price": 90100.0, "price_to_beat": 90000.0}


class TestLateSettlement(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        keys = ("MODE", "STRATEGY", "MAKER_HOLD_TO_EXPIRY", "RESOLUTION_TIMEOUT_S", "RESOLUTION_MAX_WAIT_S",
                "RESOLUTION_POLL_INTERVAL_S", "TRAILING_PROFIT_ENABLED", "PRICE_STOP_ENABLED")
        self.saved = {k: getattr(config, k) for k in keys}
        config.MODE = "paper_live"
        config.STRATEGY = "JEV_DYNAMIC"
        config.MAKER_HOLD_TO_EXPIRY = True
        config.RESOLUTION_TIMEOUT_S = 180.0
        config.RESOLUTION_MAX_WAIT_S = 900.0
        config.RESOLUTION_POLL_INTERVAL_S = 15.0
        config.TRAILING_PROFIT_ENABLED = False
        config.PRICE_STOP_ENABLED = False
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.temp_dir
        self.bot.risk.bankroll = 50.0
        self.bot.risk.daily_pnl = 0.0
        self.bot.risk.consecutive_losses = 0
        self.bot._trades_log_path = self.temp_dir / "trades.json"
        self.bot._save_slot_state = lambda **kw: None
        self.jp = patch("bot.journal_write")
        self.jp.start()

    async def asyncTearDown(self):
        self.jp.stop()
        for k, v in self.saved.items():
            setattr(config, k, v)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _queue(self, end_ts: float, slug: str = "btc-updown-5m-1791559500"):
        market = {"slug": slug, "end_ts": end_ts, "condition_id": "0xabc", "tokens": {"UP": "u", "DOWN": "d"}}
        pos = {"asset": "btc", "slot": slug, "strategy": "JEV_DYNAMIC_MAKER", "side": "UP", "price": 0.5,
               "shares": 6.0, "cost_usd": 3.0, "fee": 0.0, "rebate": 0.0,
               "entry_time_utc": "2026-10-09T15:26:15+00:00"}
        self.bot._pending_settles.append({"asset": "btc", "market": market, "position": pos,
                                          "strike": 90000.0, "queued_at": end_ts, "end_ts": end_ts})

    def _trades(self):
        return json.loads(self.bot._trades_log_path.read_text()) if self.bot._trades_log_path.exists() else []

    # (1) Gamma resolves only after the 180s timeout -> still booked as an official loss
    async def test_resolution_after_timeout_books_loss(self):
        end = time.time() - 1000.0
        self._queue(end)
        answers = {"res": None}
        self.bot.pub_client.get_resolution = MagicMock(side_effect=lambda *a, **k: answers["res"])
        with self.assertLogs("twap-bot", level="INFO") as cm:
            for t in (5, 60, 179, 181, 190, 200, 300):
                await self.bot._flush_pending_settlements(end + t)
            self.assertEqual(len(self.bot._pending_settles), 1)
            self.assertEqual(self._trades(), [])
            answers["res"] = LOSS
            await self.bot._flush_pending_settlements(end + 420)
        self.assertEqual(self.bot._pending_settles, [])
        rows = self._trades()
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertIs(r["won"], False)
        self.assertEqual(r["exit_reason"], "settlement_lost")
        self.assertAlmostEqual(r["pnl"], -3.0, places=2)
        self.assertTrue(r["official"])
        self.assertTrue(any("still PENDING" in m for m in cm.output))
        self.assertTrue(any("Late Polymarket resolution" in m for m in cm.output))
        self.assertFalse(any("recording unresolved" in m for m in cm.output))
        # throttled polling after the timeout (not every second)
        self.assertLessEqual(self.bot.pub_client.get_resolution.call_count, 10)

    async def test_gives_up_only_after_max_wait(self):
        end = time.time() - 2000.0
        self._queue(end)
        self.bot.pub_client.get_resolution = MagicMock(return_value=None)
        await self.bot._flush_pending_settlements(end + 200)
        await self.bot._flush_pending_settlements(end + 899)
        self.assertEqual(len(self.bot._pending_settles), 1)
        await self.bot._flush_pending_settlements(end + 915)
        self.assertEqual(self.bot._pending_settles, [])
        r = self._trades()[0]
        self.assertEqual(r["settlement_source"], "unresolved")
        self.assertIsNone(r["won"])
        self.assertEqual(r["cost_usd"], 3.0)

    # (2) already resolved at the first check -> booked immediately
    async def test_resolved_at_first_check_books_immediately(self):
        end = time.time() - 10.0
        self._queue(end)
        self.bot.pub_client.get_resolution = MagicMock(return_value=WIN)
        await self.bot._flush_pending_settlements(end + 3)
        self.assertEqual(self.bot._pending_settles, [])
        r = self._trades()[0]
        self.assertIs(r["won"], True)
        self.assertEqual(r["exit_reason"], "settlement_won")
        self.assertAlmostEqual(r["pnl"], 3.0, places=2)
        self.assertEqual(self.bot.pub_client.get_resolution.call_count, 1)
        kwargs = self.bot.pub_client.get_resolution.call_args.kwargs
        self.assertEqual(kwargs.get("condition_id"), "0xabc")

    # (4) bankroll + consecutive-loss counter update on late booking
    async def test_late_booking_updates_bankroll_and_loss_counter(self):
        self.bot.risk.consecutive_losses = 2
        end = time.time() - 1000.0
        self._queue(end)
        answers = {"res": None}
        self.bot.pub_client.get_resolution = MagicMock(side_effect=lambda *a, **k: answers["res"])
        await self.bot._flush_pending_settlements(end + 200)
        self.assertEqual(self.bot.risk.bankroll, 50.0)
        answers["res"] = LOSS
        await self.bot._flush_pending_settlements(end + 600)
        self.assertAlmostEqual(self.bot.risk.bankroll, 47.0, places=2)
        self.assertAlmostEqual(self.bot.risk.daily_pnl, -3.0, places=2)
        self.assertEqual(self.bot.risk.consecutive_losses, 3)
        state = json.loads((self.temp_dir / "risk_state.json").read_text())
        self.assertEqual(state["consecutive_losses"], 3)
        self.assertAlmostEqual(state["bankroll"], 47.0, places=2)
        # a late WIN resets the streak
        self._queue(end + 300, slug="btc-updown-5m-1791559800")
        answers["res"] = WIN
        await self.bot._flush_pending_settlements(end + 300 + 400)
        self.assertEqual(self.bot.risk.consecutive_losses, 0)
        self.assertAlmostEqual(self.bot.risk.bankroll, 50.0, places=2)

    # (3) take-profit early exit is unchanged and never touches the settlement queue
    async def test_take_profit_unaffected(self):
        now = time.time()
        remaining = 100.0
        slot_start = now - (300.0 - remaining)
        market = {"slug": "btc-updown-5m-test-tp", "slot_start_ts": slot_start, "end_ts": slot_start + 300.0,
                  "tokens": {"UP": "tok_up", "DOWN": "tok_down"}, "fee_rate": 0.0}
        slot = self.bot.slots["btc"]
        slot.market = market
        slot.strike = 90000.0
        slot.position = {"asset": "btc", "slot": market["slug"], "strategy": "JEV_DYNAMIC_MAKER", "side": "UP",
                         "token_id": "tok_up", "opp_token_id": "tok_down", "price": 0.5, "shares": 6.0,
                         "cost_usd": 3.0, "fee": 0.0, "strike": 90000.0, "entry_time": now - 60,
                         "slot_start_ts": slot_start, "entry_time_utc": "2026-10-09T10:00:00+00:00"}
        for i in range(30):
            self.bot.cl_stream._record(now - 30 + i, 90080.0, asset="btc")
        book = {"bids": [{"price": "0.96", "size": "500"}], "asks": [{"price": "0.97", "size": "500"}]}
        self.bot.pub_client.get_order_book = MagicMock(side_effect=lambda tok: book)
        self.bot.pub_client.get_resolution = MagicMock(side_effect=AssertionError("TP must not query resolution"))
        self.bot.book_stream._apply_update("tok_up", bid=0.96, ask=0.97, bid_sz=500, ask_sz=500)
        await self.bot._manage_open_momentum_position(
            slot, market, remaining, market["tokens"], spot=90080.0, strike=90000.0,
            realized_twap=90080.0, elapsed_s=300.0 - remaining, now=now,
        )
        self.assertIsNone(slot.position)
        self.assertEqual(self.bot._pending_settles, [])
        r = self._trades()[-1]
        self.assertEqual(r["exit_reason"], "take_profit")
        self.assertIs(r["won"], True)
        self.assertGreater(r["pnl"], 0)
        self.assertGreater(self.bot.risk.bankroll, 50.0)
        await self.bot._flush_pending_settlements(now + 1000)
        self.bot.pub_client.get_resolution.assert_not_called()

    # Startup backfill of rows already recorded as unresolved
    async def test_backfill_unresolved_rows(self):
        rows = [
            {"timestamp": "2026-10-09T15:33:01+00:00", "asset": "btc", "slot": "btc-updown-5m-1791559500",
             "strategy": "JEV_DYNAMIC_MAKER", "side": "DOWN", "price": 0.5, "shares": 6.0, "cost_usd": 3.0,
             "outcome": None, "won": None, "pnl": 0.0, "bankroll": 79.0, "settlement_source": "unresolved",
             "early_exit": False},
            {"timestamp": "2026-10-09T15:38:01+00:00", "asset": "eth", "slot": "eth-updown-5m-1791559800",
             "strategy": "JEV_DYNAMIC_MAKER", "side": "UP", "price": 0.5, "shares": 6.0, "cost_usd": 3.0,
             "outcome": None, "won": None, "pnl": 0.0, "bankroll": 79.0, "settlement_source": "unresolved",
             "early_exit": False},
            {"timestamp": "2026-10-09T15:43:01+00:00", "asset": "btc", "slot": "btc-updown-5m-1791560100",
             "strategy": "JEV_DYNAMIC_MAKER", "side": "UP", "price": 0.5, "shares": 6.0, "cost_usd": 3.0,
             "outcome": None, "won": None, "pnl": 0.0, "bankroll": 79.0, "settlement_source": "unresolved",
             "early_exit": False},
            {"slot": "btc-updown-5m-1791560400", "side": "DOWN", "price": 0.5, "shares": 6.0, "cost_usd": 3.0,
             "won": True, "pnl": 2.72, "settlement_source": "early_exit_take_profit", "official": True,
             "exit_reason": "take_profit", "early_exit": True},
        ]
        self.bot._trades_log_path.write_text(json.dumps(rows))
        res = {"btc-updown-5m-1791559500": {**WIN},  # bot was DOWN -> loss
               "eth-updown-5m-1791559800": {**WIN},  # bot was UP -> win
               "btc-updown-5m-1791560100": None}     # still open -> left alone
        self.bot.pub_client.get_resolution = MagicMock(side_effect=lambda slug, **k: res[slug])
        summary = self.bot._repair_unresolved_records()
        self.assertEqual(summary["found"], 3)
        self.assertEqual(summary["booked"], 2)
        self.assertEqual((summary["wins"], summary["losses"], summary["still_unresolved"]), (1, 1, 1))
        out = self._trades()
        self.assertEqual(len(out), 4)
        self.assertEqual((out[0]["won"], out[0]["pnl"], out[0]["exit_reason"]), (False, -3.0, "settlement_lost"))
        self.assertEqual((out[1]["won"], out[1]["pnl"], out[1]["exit_reason"]), (True, 3.0, "settlement_won"))
        self.assertTrue(out[0]["backfilled"] and out[0]["official"])
        self.assertIsNone(out[2]["won"])
        self.assertEqual(out[3], rows[3])
        # paper bankroll follows the official blotter: 50 - 3 + 3 + 2.72
        self.bot._reconcile_paper_blotter()
        self.assertAlmostEqual(self.bot.risk.bankroll, 52.72, places=2)
        # idempotent
        again = self.bot._repair_unresolved_records()
        self.assertEqual(again["booked"], 0)
        self.assertEqual(self._trades()[:2], out[:2])


class TestNoJevOnTrades(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.temp_dir
        self.bot.risk.bankroll = 50.0
        self.bot._trades_log_path = self.temp_dir / "trades.json"
        self.jp = patch("bot.journal_write")
        self.jp.start()

    async def asyncTearDown(self):
        self.jp.stop()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_settled_trade_json_has_no_jev_keys(self):
        jev = {"action": "proceed", "shadow": True, "shadow_verdict": "veto", "conviction": 1.2}
        pos = {"asset": "btc", "slot": "btc-updown-5m-1791559500", "strategy": "JEV_DYNAMIC_MAKER", "side": "UP",
               "price": 0.5, "shares": 6.0, "cost_usd": 3.0, "fee": 0.0, "jev": jev,
               "jev_shadow_verdict": "veto", "jev_shadow_conviction": 1.2, "jev_extra": 1}
        market = {"slug": "btc-updown-5m-1791559500"}
        self.bot._settle_filled({"position": pos, "market": market, "asset": "btc"}, WIN)
        self.bot._save_trade_record({"slot": "x", "won": True, "pnl": 1.0, "jev": jev, "JEV_tag": 1})
        rows = json.loads(self.bot._trades_log_path.read_text())
        self.assertEqual(len(rows), 2)
        for r in rows:
            self.assertEqual([k for k in r if k.lower().startswith("jev")], [])
        self.assertEqual(rows[0]["strategy"], "JEV_DYNAMIC_MAKER")
        self.assertIs(rows[0]["won"], True)
        # /trades payload carries no Jev keys either, even for legacy rows that still store them
        from http_api import public_trade
        legacy = {**rows[0], "jev": jev, "jev_shadow_verdict": "veto", "jev_shadow_conviction": 1.2}
        self.assertEqual([k for k in public_trade(legacy) if k.lower().startswith("jev")], [])


class TestResolutionClient(unittest.TestCase):
    def test_parse_clob_winner(self):
        payload = {"closed": True, "tokens": [{"outcome": "Up", "winner": False}, {"outcome": "Down", "winner": True}]}
        r = parse_clob_resolution(payload)
        self.assertEqual((r["side"], r["official"], r["source"]), ("DOWN", True, "polymarket_clob_winner"))
        self.assertIsNone(parse_clob_resolution({"tokens": [{"outcome": "Up", "winner": False},
                                                            {"outcome": "Down", "winner": False}]}))
        self.assertIsNone(parse_clob_resolution(None))

    def test_get_resolution_falls_back_to_clob_when_gamma_stale(self):
        c = PolymarketPublicClient()
        stale_ev = {"slug": "btc-updown-5m-1000", "markets": [{"closed": False, "outcomes": '["Up","Down"]',
                    "outcomePrices": '["0.6","0.4"]', "conditionId": "0xcid"}]}
        c.get_event = MagicMock(return_value=stale_ev)
        c.get_clob_resolution = MagicMock(return_value={"side": "UP", "source": "polymarket_clob_winner",
                                                        "official": True})
        r = c.get_resolution("btc-updown-5m-1000", now_ts=2000.0)
        self.assertEqual(r["side"], "UP")
        c.get_clob_resolution.assert_called_once_with("0xcid")
        # before the window is over: no CLOB call, no result
        c.get_clob_resolution.reset_mock()
        self.assertIsNone(c.get_resolution("btc-updown-5m-1000", now_ts=1100.0))
        c.get_clob_resolution.assert_not_called()

    def test_get_resolution_prefers_gamma_when_resolved(self):
        c = PolymarketPublicClient()
        ev = {"slug": "btc-updown-5m-1000", "markets": [{"closed": True, "outcomes": '["Up","Down"]',
              "outcomePrices": '["0","1"]', "conditionId": "0xcid"}]}
        c.get_event = MagicMock(return_value=ev)
        c.get_clob_resolution = MagicMock()
        r = c.get_resolution("btc-updown-5m-1000", now_ts=2000.0)
        self.assertEqual(r["side"], "DOWN")
        c.get_clob_resolution.assert_not_called()


if __name__ == "__main__":
    unittest.main()
