"""Unit and integration tests for JevAdvisor (TypeSafe AI System One model)."""
from __future__ import annotations

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from jev_advisor import JevAdvisor, JevDecision


class TestJevAdvisor(unittest.TestCase):
    def setUp(self):
        self.advisor = JevAdvisor(
            api_key="test_key",
            model="jev-latest",
            enabled=True,
            timeout_s=1.5,
            max_reversal_noul=0.35,
            min_conviction_score=1.50,
            downsize_factor=0.50,
        )

    def test_init_and_stats(self):
        stats = self.advisor.stats()
        self.assertTrue(stats["enabled"])
        self.assertEqual(stats["model"], "jev-latest")
        self.assertEqual(stats["total_evaluations"], 0)
        self.assertEqual(stats["approved"], 0)
        self.assertEqual(stats["downsized"], 0)
        self.assertEqual(stats["vetoed"], 0)

    def test_disabled_fallback(self):
        disabled_advisor = JevAdvisor(enabled=False)
        decision = asyncio.run(
            disabled_advisor.evaluate_maker(
                asset="btc",
                side="UP",
                spot=91000.0,
                strike=90950.0,
                realized_twap=90980.0,
                z_score=1.2,
                remaining_s=120.0,
                elapsed_s=180.0,
                best_bid=0.45,
                best_ask=0.49,
                target_bid=0.47,
                spread=0.04,
                chop_crosses=1,
                daily_vol=0.0106,
            )
        )
        self.assertTrue(decision.passed)
        self.assertEqual(decision.size_multiplier, 1.0)
        self.assertTrue(decision.fallback_used)
        self.assertEqual(disabled_advisor.total_approved, 1)

    @patch("jev_advisor.AsyncTypeSafeClient")
    def test_mocked_approve(self, mock_client_cls):
        mock_instance = MagicMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_instance

        mock_res = MagicMock()
        mock_res.nouls = {"reversal_risk": MagicMock(noul=0.15)}
        mock_res.choices = {
            "regime": MagicMock(choice="clean_trend"),
            "action": MagicMock(choice="proceed", confidence=0.88),
        }
        mock_res.scores = {"conviction": MagicMock(score=2.6)}
        mock_instance.system_one = AsyncMock(return_value=mock_res)

        decision = asyncio.run(
            self.advisor.evaluate_maker(
                asset="btc",
                side="UP",
                spot=91500.0,
                strike=91200.0,
                realized_twap=91350.0,
                z_score=1.5,
                remaining_s=120.0,
                elapsed_s=180.0,
                best_bid=0.44,
                best_ask=0.48,
                target_bid=0.46,
                spread=0.04,
                chop_crosses=0,
                daily_vol=0.0106,
            )
        )

        self.assertTrue(decision.passed)
        self.assertEqual(decision.action, "proceed")
        self.assertEqual(decision.size_multiplier, 1.0)
        self.assertEqual(decision.reversal_risk, 0.15)
        self.assertEqual(decision.market_regime, "clean_trend")
        self.assertFalse(decision.fallback_used)
        self.assertEqual(self.advisor.total_approved, 1)

    @patch("jev_advisor.AsyncTypeSafeClient")
    def test_mocked_veto_high_reversal(self, mock_client_cls):
        mock_instance = MagicMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_instance

        mock_res = MagicMock()
        mock_res.nouls = {"reversal_risk": MagicMock(noul=0.78)}  # > 0.60 threshold
        mock_res.choices = {
            "regime": MagicMock(choice="mean_reverting_chop"),
            "action": MagicMock(choice="abstain", confidence=0.72),
        }
        mock_res.scores = {"conviction": MagicMock(score=0.8)}
        mock_instance.system_one = AsyncMock(return_value=mock_res)

        decision = asyncio.run(
            self.advisor.evaluate_momentum(
                asset="sol",
                side="UP",
                spot=185.0,
                strike=184.5,
                move_usd=0.5,
                remaining_s=80.0,
                elapsed_s=220.0,
                best_bid=0.70,
                best_ask=0.78,
                spread=0.08,
                chop_crosses=4,
                daily_vol=0.0220,
            )
        )

        self.assertFalse(decision.passed)
        self.assertEqual(decision.action, "abstain")
        self.assertEqual(decision.size_multiplier, 0.0)
        self.assertEqual(decision.reversal_risk, 0.78)
        self.assertEqual(self.advisor.total_vetoed, 1)

    @patch("jev_advisor.AsyncTypeSafeClient")
    def test_mocked_downsize_marginal(self, mock_client_cls):
        mock_instance = MagicMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_instance

        mock_res = MagicMock()
        mock_res.nouls = {"reversal_risk": MagicMock(noul=0.28)}
        mock_res.choices = {
            "regime": MagicMock(choice="clean_trend"),
            "action": MagicMock(choice="downsize", confidence=0.75),
        }
        mock_res.scores = {"conviction": MagicMock(score=1.8)}
        mock_instance.system_one = AsyncMock(return_value=mock_res)

        decision = asyncio.run(
            self.advisor.evaluate_maker(
                asset="eth",
                side="DOWN",
                spot=3100.0,
                strike=3110.0,
                realized_twap=3108.0,
                z_score=0.85,
                remaining_s=150.0,
                elapsed_s=150.0,
                best_bid=0.46,
                best_ask=0.50,
                target_bid=0.48,
                spread=0.04,
                chop_crosses=2,
                daily_vol=0.0140,
            )
        )

        self.assertTrue(decision.passed)
        self.assertEqual(decision.action, "downsize")
        self.assertEqual(decision.size_multiplier, 0.50)
        self.assertEqual(self.advisor.total_downsized, 1)

    @patch("jev_advisor.AsyncTypeSafeClient")
    def test_mocked_veto_chop_regime(self, mock_client_cls):
        mock_instance = MagicMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_instance

        mock_res = MagicMock()
        mock_res.nouls = {"reversal_risk": MagicMock(noul=0.25)}
        mock_res.choices = {
            "regime": MagicMock(choice="mean_reverting_chop"),
            "action": MagicMock(choice="proceed", confidence=0.70),
        }
        mock_res.scores = {"conviction": MagicMock(score=2.2)}
        mock_instance.system_one = AsyncMock(return_value=mock_res)

        decision = asyncio.run(
            self.advisor.evaluate_maker(
                asset="xrp",
                side="UP",
                spot=1.52,
                strike=1.52,
                realized_twap=1.52,
                z_score=0.5,
                remaining_s=120.0,
                elapsed_s=180.0,
                best_bid=0.45,
                best_ask=0.49,
                target_bid=0.47,
                spread=0.04,
                chop_crosses=3,
                daily_vol=0.020,
            )
        )
        self.assertFalse(decision.passed)
        self.assertIn("mean_reverting_chop", decision.reason)
        self.assertEqual(self.advisor.total_vetoed, 1)

    @patch("jev_advisor.AsyncTypeSafeClient")
    def test_timeout_fallback(self, mock_client_cls):
        mock_instance = MagicMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_instance

        async def slow_call(*args, **kwargs):
            await asyncio.sleep(10.0)

        mock_instance.system_one = slow_call

        fast_advisor = JevAdvisor(api_key="key", timeout_s=0.05, enabled=True)
        decision = asyncio.run(
            fast_advisor.evaluate_maker(
                asset="btc",
                side="UP",
                spot=91000.0,
                strike=90900.0,
                realized_twap=90950.0,
                z_score=1.1,
                remaining_s=100.0,
                elapsed_s=200.0,
                best_bid=0.45,
                best_ask=0.49,
                target_bid=0.47,
                spread=0.04,
                chop_crosses=0,
                daily_vol=0.0106,
            )
        )

        self.assertFalse(decision.passed)
        self.assertTrue(decision.fallback_used)
        self.assertEqual(decision.action, "abstain")
        self.assertEqual(decision.size_multiplier, 0.0)
        self.assertEqual(fast_advisor.total_fallbacks, 1)
        self.assertEqual(fast_advisor.total_vetoed, 1)

    @patch("jev_advisor.AsyncTypeSafeClient")
    def test_mocked_dynamic_maker(self, mock_client_cls):
        mock_instance = MagicMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_instance

        mock_res = MagicMock()
        mock_res.nouls = {"reversal_risk": MagicMock(noul=0.20)}
        mock_res.choices = {
            "regime": MagicMock(choice="clean_trend"),
            "action": MagicMock(choice="proceed", confidence=0.85),
        }
        mock_res.scores = {"conviction": MagicMock(score=2.5)}
        mock_instance.system_one = AsyncMock(return_value=mock_res)

        decision = asyncio.run(
            self.advisor.evaluate_dynamic(
                asset="sol",
                opportunity_type="MAKER_BID",
                side="UP",
                spot=182.5,
                strike=182.0,
                realized_twap=182.2,
                z_score=0.45,
                move_usd=0.5,
                remaining_s=140.0,
                elapsed_s=160.0,
                best_bid=0.48,
                best_ask=0.52,
                proposed_price=0.47,
                spread=0.04,
                chop_crosses=1,
                daily_vol=0.022,
            )
        )
        self.assertTrue(decision.passed)
        self.assertEqual(decision.action, "proceed")
        self.assertEqual(decision.size_multiplier, 1.0)
        self.assertEqual(self.advisor.total_approved, 1)

    @patch("jev_advisor.AsyncTypeSafeClient")
    def test_mocked_dynamic_momentum(self, mock_client_cls):
        mock_instance = MagicMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_instance

        mock_res = MagicMock()
        mock_res.nouls = {"reversal_risk": MagicMock(noul=0.10)}
        mock_res.choices = {
            "regime": MagicMock(choice="clean_trend"),
            "action": MagicMock(choice="proceed", confidence=0.92),
        }
        mock_res.scores = {"conviction": MagicMock(score=2.8)}
        mock_instance.system_one = AsyncMock(return_value=mock_res)

        decision = asyncio.run(
            self.advisor.evaluate_dynamic(
                asset="doge",
                opportunity_type="MOMENTUM_TAKER",
                side="UP",
                spot=0.255,
                strike=0.250,
                realized_twap=0.252,
                z_score=0.85,
                move_usd=0.005,
                remaining_s=90.0,
                elapsed_s=210.0,
                best_bid=0.68,
                best_ask=0.72,
                proposed_price=0.72,
                spread=0.04,
                chop_crosses=0,
                daily_vol=0.035,
            )
        )
        self.assertTrue(decision.passed)
        self.assertEqual(decision.action, "proceed")
        self.assertEqual(decision.size_multiplier, 1.0)
        self.assertEqual(self.advisor.total_approved, 1)


class TestJevLiveIntegration(unittest.TestCase):
    def test_live_api_if_key_available(self):
        from dotenv import load_dotenv
        load_dotenv()

        api_key = os.getenv("TYPESAFE_API_KEY", "").strip()
        if not api_key:
            self.skipTest("TYPESAFE_API_KEY not set in environment; skipping live call.")

        advisor = JevAdvisor(api_key=api_key, enabled=True, timeout_s=3.0)
        # Test Maker evaluation
        decision = asyncio.run(
            advisor.evaluate_maker(
                asset="btc",
                side="UP",
                spot=91400.0,
                strike=91100.0,
                realized_twap=91250.0,
                z_score=1.35,
                remaining_s=120.0,
                elapsed_s=180.0,
                best_bid=0.45,
                best_ask=0.49,
                target_bid=0.47,
                spread=0.04,
                chop_crosses=1,
                daily_vol=0.0106,
            )
        )

        self.assertFalse(decision.fallback_used)
        self.assertIn(decision.action, ("proceed", "downsize", "abstain"))
        self.assertGreaterEqual(decision.conviction, 0.0)
        self.assertGreaterEqual(decision.reversal_risk, 0.0)
        self.assertLessEqual(decision.reversal_risk, 1.0)
        self.assertGreater(decision.latency_ms, 0.0)
        print(f"\n[LIVE JEV MAKER SUCCESS] latency={decision.latency_ms:.0f}ms action={decision.action} regime={decision.market_regime}")

        # Test Dynamic Hybrid evaluation
        dyn_decision = asyncio.run(
            advisor.evaluate_dynamic(
                asset="sol",
                opportunity_type="MOMENTUM_TAKER",
                side="UP",
                spot=182.5,
                strike=181.8,
                realized_twap=182.1,
                z_score=0.92,
                move_usd=0.7,
                remaining_s=85.0,
                elapsed_s=215.0,
                best_bid=0.68,
                best_ask=0.73,
                proposed_price=0.73,
                spread=0.05,
                chop_crosses=1,
                daily_vol=0.022,
            )
        )
        self.assertFalse(dyn_decision.fallback_used)
        self.assertIn(dyn_decision.action, ("proceed", "downsize", "abstain"))
        print(f"[LIVE JEV DYNAMIC SUCCESS] latency={dyn_decision.latency_ms:.0f}ms action={dyn_decision.action} regime={dyn_decision.market_regime}")


if __name__ == "__main__":
    unittest.main()
