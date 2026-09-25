"""Tests for JEV_SHADOW mode: Jev is consulted and logged but never enforced."""
from __future__ import annotations

import asyncio
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import config
import jev_advisor
from http_api import build_stats, jev_shadow_bucket
from jev_advisor import JevAdvisor, JevDecision

DYN_KW = dict(
    asset="btc", opportunity_type="MOMENTUM_TAKER", side="UP", spot=91500.0, strike=91400.0,
    realized_twap=91450.0, z_score=1.4, move_usd=100.0, remaining_s=180.0, elapsed_s=120.0,
    best_bid=0.70, best_ask=0.72, proposed_price=0.72, spread=0.02, chop_crosses=1, daily_vol=0.01,
)


def _veto() -> JevDecision:
    return JevDecision(
        action="abstain", passed=False, size_multiplier=0.0, conviction=1.2, reversal_risk=0.4,
        market_regime="mean_reverting_chop", confidence=0.7,
        reason="Vetoed by Jev: action=abstain, regime=mean_reverting_chop", latency_ms=812.0,
        fallback_used=False, fakeout_risk=0.2, evaluated_at=1.0,
    )


def _mock_res(regime="mean_reverting_chop", action="abstain", conviction=1.2, reversal=0.40):
    res = MagicMock()
    res.nouls = {"reversal_risk": MagicMock(noul=reversal), "fakeout_risk": MagicMock(noul=0.1)}
    res.choices = {"regime": MagicMock(choice=regime), "action": MagicMock(choice=action, confidence=0.8)}
    res.scores = {"conviction": MagicMock(score=conviction)}
    return res


def _advisor(shadow: bool) -> JevAdvisor:
    return JevAdvisor(api_key="test_key", enabled=True, timeout_s=1.0, shadow=shadow,
                      allow_downsizing=True, min_downsize_conviction=1.15, downsize_factor=0.5)


@unittest.skipUnless(jev_advisor.HAS_TYPESAFE, "typesafe-sdk not installed")
class TestJevShadowAdvisor(unittest.TestCase):
    @patch("jev_advisor.AsyncTypeSafeClient")
    def test_shadow_veto_proceeds_at_full_size(self, cls):
        cls.return_value.__aenter__.return_value.system_one = AsyncMock(return_value=_mock_res())
        adv = _advisor(shadow=True)
        dec = asyncio.run(adv.evaluate_dynamic(**DYN_KW))
        self.assertTrue(dec.passed)
        self.assertEqual(dec.size_multiplier, 1.0)
        self.assertTrue(dec.shadow)
        self.assertEqual(dec.shadow_verdict, "veto")
        self.assertFalse(dec.shadow_decision["passed"])
        self.assertIn("regime=mean_reverting_chop", dec.shadow_decision["reason"])
        self.assertAlmostEqual(dec.conviction, 1.2)
        st = adv.stats()
        self.assertTrue(st["shadow_mode"])
        self.assertEqual(st["shadow"]["veto"], 1)
        self.assertEqual(st["shadow"]["evaluations"], 1)
        self.assertEqual(st["vetoed"], 1)  # real verdict still counted

    @patch("jev_advisor.AsyncTypeSafeClient")
    def test_shadow_downsize_is_recorded_but_full_size(self, cls):
        cls.return_value.__aenter__.return_value.system_one = AsyncMock(
            return_value=_mock_res(regime="clean_trend", action="downsize", conviction=1.5, reversal=0.1))
        adv = _advisor(shadow=True)
        dec = asyncio.run(adv.evaluate_dynamic(**DYN_KW))
        self.assertTrue(dec.passed)
        self.assertEqual(dec.size_multiplier, 1.0)
        self.assertEqual(dec.shadow_verdict, "downsize")
        self.assertEqual(dec.shadow_decision["size_multiplier"], 0.5)

    @patch("jev_advisor.AsyncTypeSafeClient")
    def test_shadow_timeout_does_not_block(self, cls):
        cls.return_value.__aenter__.return_value.system_one = AsyncMock(side_effect=asyncio.TimeoutError())
        adv = _advisor(shadow=True)
        dec = asyncio.run(adv.evaluate_dynamic(**DYN_KW))
        self.assertTrue(dec.passed)
        self.assertEqual(dec.size_multiplier, 1.0)
        self.assertEqual(dec.shadow_verdict, "error")
        self.assertIn("shadow_error", dec.reason)
        self.assertEqual(adv.stats()["shadow"]["error"], 1)

    def test_shadow_unexpected_exception_does_not_block(self):
        adv = _advisor(shadow=True)
        adv._evaluate = AsyncMock(side_effect=RuntimeError("boom"))
        dec = asyncio.run(adv.evaluate_dynamic(**DYN_KW))
        self.assertTrue(dec.passed)
        self.assertEqual(dec.size_multiplier, 1.0)
        self.assertEqual(dec.shadow_verdict, "error")

    @patch("jev_advisor.AsyncTypeSafeClient")
    def test_shadow_off_keeps_enforced_veto(self, cls):
        cls.return_value.__aenter__.return_value.system_one = AsyncMock(return_value=_mock_res())
        adv = _advisor(shadow=False)
        dec = asyncio.run(adv.evaluate_dynamic(**DYN_KW))
        self.assertFalse(dec.passed)
        self.assertEqual(dec.size_multiplier, 0.0)
        self.assertFalse(dec.shadow)
        self.assertIsNone(dec.shadow_verdict)
        self.assertFalse(adv.stats()["shadow_mode"])
        self.assertEqual(adv.stats()["shadow"]["evaluations"], 0)

    @patch("jev_advisor.AsyncTypeSafeClient")
    def test_shadow_off_timeout_still_abstains(self, cls):
        cls.return_value.__aenter__.return_value.system_one = AsyncMock(side_effect=asyncio.TimeoutError())
        dec = asyncio.run(_advisor(shadow=False).evaluate_dynamic(**DYN_KW))
        self.assertFalse(dec.passed)

    def test_shadow_inactive_when_jev_disabled(self):
        adv = JevAdvisor(api_key="test_key", enabled=False, shadow=True)
        self.assertFalse(adv.shadow_active)
        dec = asyncio.run(adv.evaluate_dynamic(**DYN_KW))
        self.assertTrue(dec.passed)
        self.assertFalse(dec.shadow)

    def test_config_default_off(self):
        self.assertIn(config.JEV_SHADOW, (True, False))
        adv = JevAdvisor(api_key="test_key", enabled=True)
        self.assertEqual(adv.shadow, bool(config.JEV_SHADOW))


class TestJevShadowBot(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from bot import TWAPBot
        self.temp_dir = Path(tempfile.mkdtemp())
        self.saved = {k: getattr(config, k) for k in ("MODE", "STRATEGY", "PAPER_FOK_REQUIRE_SIZE")}
        config.MODE = "paper_live"
        config.STRATEGY = "JEV_DYNAMIC"
        config.PAPER_FOK_REQUIRE_SIZE = False
        self.bot = TWAPBot()
        self.bot.risk.state_dir = self.temp_dir
        self.bot.risk.bankroll = 57.0
        self.bot._trades_log_path = self.temp_dir / "trades.json"
        self.journal = MagicMock()
        self.jp = patch("bot.journal_write", self.journal)
        self.jp.start()
        adv = JevAdvisor(api_key="test_key", enabled=True, shadow=True)
        self.shadow_dec = adv._shadowize(_veto())

    async def asyncTearDown(self):
        self.jp.stop()
        for k, v in self.saved.items():
            setattr(config, k, v)
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _events(self, name):
        return [c.kwargs for c in self.journal.call_args_list if c.args and c.args[0] == name]

    def test_log_jev_shadow_line_and_journal(self):
        with self.assertLogs("twap-bot", level="INFO") as cm:
            self.bot._log_jev_shadow("BTC", "MOMENTUM", "btc", "UP", 0.72, self.shadow_dec,
                                     slot="btc-updown-5m-1", elapsed_s=120.0)
        self.assertTrue(any("JEV_SHADOW MOMENTUM UP" in m and "verdict=veto" in m for m in cm.output))
        ev = self._events("jev_shadow")
        self.assertEqual(len(ev), 1)
        self.assertEqual(ev[0]["verdict"], "veto")
        self.assertAlmostEqual(ev[0]["conviction"], 1.2)
        self.assertAlmostEqual(ev[0]["reversal_risk"], 0.4)
        self.assertEqual(ev[0]["regime"], "mean_reverting_chop")
        self.assertEqual(ev[0]["latency_ms"], 812.0)

    def test_log_noop_when_not_shadow(self):
        self.bot._log_jev_shadow("BTC", "MAKER", "btc", "UP", 0.48, _veto())
        self.assertEqual(self._events("jev_shadow"), [])

    async def test_vetoed_momentum_trades_full_size_and_tag_persists(self):
        slot = self.bot.slots["btc"]
        market = {"slug": "btc-updown-5m-1790000000", "slot_start_ts": 1790000000.0,
                  "tokens": {"UP": "tok_up", "DOWN": "tok_down"}}
        slot.market = market
        slot.strike = 91400.0
        sig = SimpleNamespace(side="UP", edge_net=0.2, fee_taker=0.0, z_score=1.4, model_prob=0.7, s_0=91400.0)
        await self.bot._place_taker_order(slot, market, sig, 0.72, strategy_tag="JEV_DYNAMIC_MOMENTUM",
                                          size_multiplier=self.shadow_dec.size_multiplier,
                                          jev_decision=self.shadow_dec)
        pos = slot.position
        self.assertIsNotNone(pos)
        # flat BASE_STAKE_USD sizing at 1.0x (not Jev's 0.5x downsize / not blocked)
        exp_shares = max(config.MIN_ORDER_SHARES, round(config.BASE_STAKE_USD / 0.72, 2))
        self.assertAlmostEqual(pos["shares"], exp_shares)
        self.assertEqual(self.shadow_dec.size_multiplier, 1.0)
        self.assertEqual(pos["jev_shadow_verdict"], "veto")
        self.assertAlmostEqual(pos["jev_shadow_conviction"], 1.2)
        self.assertEqual(self._events("paper_fill")[0]["jev_shadow_verdict"], "veto")
        self.bot._settle_filled({"position": pos, "market": market, "asset": "btc"},
                                {"side": "UP", "source": "polymarket_test", "official": True})
        trades = json.loads(self.bot._trades_log_path.read_text())
        self.assertEqual(trades[-1]["jev_shadow_verdict"], "veto")
        self.assertAlmostEqual(trades[-1]["jev_shadow_conviction"], 1.2)
        self.assertEqual(self._events("settlement")[-1]["jev_shadow_verdict"], "veto")
        stats = build_stats(trades)
        self.assertEqual(stats["by_jev_shadow"]["veto"]["trades"], 1)
        self.assertEqual(stats["by_jev_shadow"]["veto"]["wins"], 1)

    async def test_enforced_decision_has_no_shadow_tag(self):
        adv = JevAdvisor(enabled=False)
        dec = await adv.evaluate_dynamic(**DYN_KW)
        tags = self.bot._jev_shadow_tags(dec.to_dict())
        self.assertEqual(tags, {"jev_shadow_verdict": None, "jev_shadow_conviction": None})


def test_jev_shadow_bucket():
    assert jev_shadow_bucket("approve") == "approve"
    assert jev_shadow_bucket("veto") == "veto"
    assert jev_shadow_bucket("downsize") == "downsize"
    assert jev_shadow_bucket("error") == "error"
    assert jev_shadow_bucket("shadow_error") == "error"
    assert jev_shadow_bucket(None) == "none"
    assert jev_shadow_bucket("") == "none"


def test_build_stats_by_jev_shadow():
    base = {"asset": "btc", "slot": "btc-updown-5m-1", "cost_usd": 3.0, "fee": 0.0, "official": True}
    trades = [
        {**base, "won": True, "pnl": 1.2, "jev_shadow_verdict": "approve"},
        {**base, "won": False, "pnl": -3.0, "jev_shadow_verdict": "veto"},
        {**base, "won": True, "pnl": 1.0, "jev_shadow_verdict": "veto"},
        {**base, "won": True, "pnl": 0.5, "jev_shadow_verdict": "downsize"},
        {**base, "won": False, "pnl": -3.0, "jev_shadow_verdict": "error"},
        {**base, "won": True, "pnl": 2.0},  # pre-shadow trade
        # verdict only inside the stored jev dict (fallback path)
        {**base, "won": True, "pnl": 0.7, "jev": {"shadow": True, "shadow_verdict": "veto"}},
        {**base, "won": None, "pnl": None, "jev_shadow_verdict": "veto"},  # unresolved: excluded
    ]
    stats = build_stats(trades, {"jev": {"shadow_mode": True, "shadow": {"evaluations": 3}}})
    b = stats["by_jev_shadow"]
    assert list(b) == ["approve", "veto", "downsize", "error", "none"]
    assert b["veto"]["trades"] == 3 and b["veto"]["wins"] == 2 and b["veto"]["pnl"] == -1.3
    assert b["approve"]["trades"] == 1 and b["approve"]["pnl"] == 1.2
    assert b["error"]["trades"] == 1 and b["none"]["trades"] == 1
    assert stats["jev"]["shadow"]["evaluations"] == 3
