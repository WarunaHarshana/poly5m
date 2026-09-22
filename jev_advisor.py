"""TypeSafe Jev System One AI Guardian for Polymarket 5m Trading Bot.

Evaluates real-time market micro-structure, TWAP trajectory, and candidate
trade setups using TypeSafe's Jev model (Choice, Score, Noul primitives).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, asdict
from typing import Any

import config

logger = logging.getLogger("jev-advisor")

try:
    from typesafe_sdk import AsyncTypeSafeClient, Choice, Noul, Score
    HAS_TYPESAFE = True
except ImportError:
    HAS_TYPESAFE = False


@dataclass
class JevDecision:
    """Structured decision returned by Jev System One evaluation."""

    action: str  # "proceed", "downsize", "abstain"
    passed: bool  # True if trade is allowed (proceed or downsize), False if blocked
    size_multiplier: float  # 1.0 for full stake, <1.0 for downsize, 0.0 for abstain
    conviction: float  # Score (0.0 to 3.0)
    reversal_risk: float  # Noul (0.0 to 1.0)
    market_regime: str  # "clean_trend", "mean_reverting_chop", "illiquid_or_spoofed", "exhaustion"
    confidence: float  # Model certainty in the primary decision
    reason: str
    latency_ms: float
    fallback_used: bool
    fakeout_risk: float = 0.0  # Noul (0.0 to 1.0)
    evaluated_at: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class JevAdvisor:
    """Asynchronous AI guardian using TypeSafe Jev."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        model: str | None = None,
        enabled: bool | None = None,
        timeout_s: float | None = None,
        max_reversal_noul: float | None = None,
        max_fakeout_noul: float | None = None,
        min_conviction_score: float | None = None,
        downsize_factor: float | None = None,
        allow_downsizing: bool | None = None,
    ):
        self.api_key = (api_key or config.TYPESAFE_API_KEY).strip()
        self.base_url = base_url or config.TYPESAFE_BASE_URL
        self.model = model or config.JEV_MODEL
        self.enabled = config.JEV_ENABLED if enabled is None else bool(enabled)
        self.timeout_s = config.JEV_TIMEOUT_S if timeout_s is None else float(timeout_s)
        self.max_reversal_noul = (
            config.JEV_MAX_REVERSAL_NOUL if max_reversal_noul is None else float(max_reversal_noul)
        )
        self.max_fakeout_noul = (
            config.JEV_MAX_FAKEOUT_NOUL if max_fakeout_noul is None else float(max_fakeout_noul)
        )
        self.min_conviction_score = (
            config.JEV_MIN_CONVICTION_SCORE if min_conviction_score is None else float(min_conviction_score)
        )
        self.downsize_factor = (
            config.JEV_DOWNSIZE_FACTOR if downsize_factor is None else float(downsize_factor)
        )
        self.allow_downsizing = (
            config.JEV_ALLOW_DOWNSIZING if allow_downsizing is None else bool(allow_downsizing)
        )

        # Telemetry
        self.total_evaluations: int = 0
        self.total_approved: int = 0
        self.total_downsized: int = 0
        self.total_vetoed: int = 0
        self.total_fallbacks: int = 0
        self.total_latency_ms: float = 0.0
        self.last_decision: JevDecision | None = None

        if not HAS_TYPESAFE:
            logger.warning("typesafe-sdk is not installed; JevAdvisor running in fallback mode.")
            self.enabled = False
        elif self.enabled and not self.api_key:
            logger.warning("TYPESAFE_API_KEY is not configured; JevAdvisor disabled.")
            self.enabled = False
        elif self.enabled:
            logger.info(f"TypeSafe Jev AI Guardian active (model={self.model}, timeout={self.timeout_s:.1f}s)")

    def stats(self) -> dict[str, Any]:
        """Return runtime telemetry snapshot for HTTP status reporting."""
        avg_lat = (
            round(self.total_latency_ms / self.total_evaluations, 1)
            if self.total_evaluations > 0
            else 0.0
        )
        return {
            "enabled": self.enabled,
            "model": self.model,
            "total_evaluations": self.total_evaluations,
            "approved": self.total_approved,
            "downsized": self.total_downsized,
            "vetoed": self.total_vetoed,
            "fallbacks": self.total_fallbacks,
            "avg_latency_ms": avg_lat,
            "last_decision": self.last_decision.to_dict() if self.last_decision else None,
        }

    async def evaluate_maker(
        self,
        *,
        asset: str,
        side: str,
        spot: float,
        strike: float,
        realized_twap: float,
        z_score: float,
        remaining_s: float,
        elapsed_s: float,
        best_bid: float | None,
        best_ask: float | None,
        target_bid: float,
        spread: float,
        chop_crosses: int,
        daily_vol: float,
    ) -> JevDecision:
        """Evaluate a candidate resting limit order for the MIDBAND_MAKER strategy."""
        state = {
            "strategy": "MIDBAND_MAKER",
            "asset": str(asset).upper(),
            "target_side": str(side).upper(),
            "time_elapsed_seconds": round(float(elapsed_s), 1),
            "time_remaining_seconds": round(float(remaining_s), 1),
            "spot_price": round(float(spot), 4),
            "strike_s0": round(float(strike), 4),
            "spot_vs_strike_delta": round(float(spot - strike), 4),
            "realized_twap": round(float(realized_twap), 4),
            "twap_z_score": round(float(z_score), 3),
            "strike_crossings_chop": int(chop_crosses),
            "clob_best_bid": round(float(best_bid), 3) if best_bid is not None else None,
            "clob_best_ask": round(float(best_ask), 3) if best_ask is not None else None,
            "proposed_limit_bid": round(float(target_bid), 3),
            "orderbook_spread": round(float(spread), 3),
            "daily_vol_prior": round(float(daily_vol), 4),
        }
        return await self._evaluate(state, side=side, spot=spot, strike=strike)

    async def evaluate_momentum(
        self,
        *,
        asset: str,
        side: str,
        spot: float,
        strike: float,
        move_usd: float,
        remaining_s: float,
        elapsed_s: float,
        best_bid: float | None,
        best_ask: float | None,
        spread: float,
        chop_crosses: int,
        daily_vol: float,
    ) -> JevDecision:
        """Evaluate a candidate taker market order for the MOMENTUM strategy."""
        state = {
            "strategy": "MOMENTUM_TAKER",
            "asset": str(asset).upper(),
            "target_side": str(side).upper(),
            "time_elapsed_seconds": round(float(elapsed_s), 1),
            "time_remaining_seconds": round(float(remaining_s), 1),
            "spot_price": round(float(spot), 4),
            "strike_s0": round(float(strike), 4),
            "spot_move_usd": round(float(move_usd), 4),
            "strike_crossings_chop": int(chop_crosses),
            "clob_best_bid": round(float(best_bid), 3) if best_bid is not None else None,
            "clob_best_ask": round(float(best_ask), 3) if best_ask is not None else None,
            "orderbook_spread": round(float(spread), 3),
            "daily_vol_prior": round(float(daily_vol), 4),
        }
        return await self._evaluate(state, side=side, spot=spot, strike=strike)

    async def evaluate_dynamic(
        self,
        *,
        asset: str,
        opportunity_type: str,
        side: str,
        spot: float,
        strike: float,
        realized_twap: float,
        z_score: float,
        move_usd: float,
        remaining_s: float,
        elapsed_s: float,
        best_bid: float | None,
        best_ask: float | None,
        proposed_price: float,
        spread: float,
        chop_crosses: int,
        daily_vol: float,
    ) -> JevDecision:
        """Evaluate a dynamic candidate opportunity (Maker or Momentum) under JEV_DYNAMIC mode."""
        state = {
            "mode": "JEV_DYNAMIC_HYBRID",
            "candidate_type": opportunity_type,
            "asset": str(asset).upper(),
            "target_side": str(side).upper(),
            "time_elapsed_seconds": round(float(elapsed_s), 1),
            "time_remaining_seconds": round(float(remaining_s), 1),
            "spot_price": round(float(spot), 4),
            "strike_s0": round(float(strike), 4),
            "spot_vs_strike_delta": round(float(spot - strike), 4),
            "spot_move_usd": round(float(move_usd), 4),
            "realized_twap": round(float(realized_twap), 4),
            "twap_z_score": round(float(z_score), 3),
            "strike_crossings_chop": int(chop_crosses),
            "clob_best_bid": round(float(best_bid), 3) if best_bid is not None else None,
            "clob_best_ask": round(float(best_ask), 3) if best_ask is not None else None,
            "proposed_entry_price": round(float(proposed_price), 3),
            "orderbook_spread": round(float(spread), 3),
            "daily_vol_prior": round(float(daily_vol), 4),
        }
        return await self._evaluate(state, side=side, spot=spot, strike=strike)

    async def _evaluate(
        self,
        state: dict[str, Any],
        *,
        side: str,
        spot: float,
        strike: float,
    ) -> JevDecision:
        t0 = time.time()
        self.total_evaluations += 1

        if not self.enabled or not HAS_TYPESAFE:
            decision = JevDecision(
                action="proceed",
                passed=True,
                size_multiplier=1.0,
                conviction=2.0,
                reversal_risk=0.0,
                market_regime="clean_trend",
                confidence=1.0,
                reason="Jev disabled or sdk missing; default pass",
                latency_ms=0.0,
                fallback_used=True,
                evaluated_at=t0,
            )
            self.total_approved += 1
            self.last_decision = decision
            return decision

        side_upper = str(side).upper()
        opp_side = "DOWN" if side_upper == "UP" else "UP"

        questions = {
            "reversal_risk": Noul(
                instructions=(
                    f"Is there a high risk that the spot price reverses across strike S0 "
                    f"({strike:.4f}) before expiration, causing the {side_upper} outcome to lose?"
                )
            ),
            "fakeout_risk": Noul(
                instructions=(
                    f"Is this move away from strike S0 ({strike:.4f}) a low-volume liquidity sweep, "
                    f"exhaustion wick, or bull/bear trap that is likely to snap back within 2-3 minutes?"
                )
            ),
            "regime": Choice(
                instructions="What best characterizes the current crypto market price action?",
                criteria={
                    "clean_trend": "Unidirectional drift away from strike with sustained momentum",
                    "mean_reverting_chop": "Frequent crossings or oscillations near strike with mean-reverting tendency",
                    "illiquid_or_spoofed": "Wide book spread, low depth, or erratic book liquidity",
                    "exhaustion": "Late breakout that is slowing down or showing momentum exhaustion",
                },
            ),
            "conviction": Score(
                instructions=(
                    f"How high is the probability that the {side_upper} position remains in the money "
                    f"through final settlement?"
                ),
                criteria=[
                    "low_probability_or_trap",
                    "marginal_speculative",
                    "solid_directional_bias",
                    "high_conviction_trend",
                ],
            ),
            "action": Choice(
                instructions=(
                    f"Considering the remaining time, spread, and price distance from strike, "
                    f"should the bot execute the {side_upper} order?"
                ),
                criteria={
                    "proceed": f"Favorable setup: enter {side_upper} with standard position sizing",
                    "downsize": f"Elevated uncertainty: enter {side_upper} with reduced (half) position size",
                    "abstain": f"Unfavorable setup: skip the trade to protect bankroll",
                },
            ),
        }

        try:
            client_kwargs: dict[str, Any] = {"api_key": self.api_key}
            if self.base_url:
                client_kwargs["base_url"] = self.base_url
            if self.model:
                client_kwargs["model"] = self.model

            async with AsyncTypeSafeClient(**client_kwargs) as client:
                res = await asyncio.wait_for(
                    client.system_one(state, questions),
                    timeout=self.timeout_s,
                )

            latency = (time.time() - t0) * 1000.0
            self.total_latency_ms += latency

            reversal_risk = float(res.nouls["reversal_risk"].noul)
            fakeout_risk = float(res.nouls["fakeout_risk"].noul) if "fakeout_risk" in res.nouls else 0.0
            regime = str(res.choices["regime"].choice)
            conviction_score = float(res.scores["conviction"].score)
            action_choice = str(res.choices["action"].choice)
            action_confidence = float(res.choices["action"].confidence)

            # Synthesize final decision gating
            is_toxic_regime = regime in ("mean_reverting_chop", "illiquid_or_spoofed", "exhaustion")
            is_downsize_veto = (action_choice == "downsize" and not self.allow_downsizing)

            if (
                action_choice == "abstain"
                or is_downsize_veto
                or reversal_risk > self.max_reversal_noul
                or fakeout_risk > self.max_fakeout_noul
                or is_toxic_regime
                or conviction_score < self.min_conviction_score
            ):
                passed = False
                size_mult = 0.0
                reasons = []
                if action_choice == "abstain":
                    reasons.append("action=abstain")
                elif is_downsize_veto:
                    reasons.append("action=downsize(uncertainty veto)")
                if is_toxic_regime:
                    reasons.append(f"regime={regime}")
                if reversal_risk > self.max_reversal_noul:
                    reasons.append(f"reversal={reversal_risk:.2f} (max {self.max_reversal_noul:.2f})")
                if fakeout_risk > self.max_fakeout_noul:
                    reasons.append(f"fakeout={fakeout_risk:.2f} (max {self.max_fakeout_noul:.2f})")
                if conviction_score < self.min_conviction_score:
                    reasons.append(f"conviction={conviction_score:.2f} (min {self.min_conviction_score:.2f})")
                reason = f"Vetoed by Jev: {', '.join(reasons)}"
                self.total_vetoed += 1
            elif action_choice == "downsize" and self.allow_downsizing:
                passed = True
                size_mult = self.downsize_factor
                reason = (
                    f"Downsized by Jev ({self.downsize_factor:.0%}): conviction={conviction_score:.2f}, "
                    f"reversal={reversal_risk:.2f}, fakeout={fakeout_risk:.2f}, regime={regime}"
                )
                self.total_downsized += 1
            else:
                passed = True
                size_mult = 1.0
                reason = (
                    f"Approved by Jev: conviction={conviction_score:.2f}, reversal={reversal_risk:.2f}, "
                    f"fakeout={fakeout_risk:.2f}, regime={regime}"
                )
                self.total_approved += 1

            decision = JevDecision(
                action=action_choice,
                passed=passed,
                size_multiplier=size_mult,
                conviction=conviction_score,
                reversal_risk=reversal_risk,
                market_regime=regime,
                confidence=action_confidence,
                reason=reason,
                latency_ms=round(latency, 1),
                fallback_used=False,
                fakeout_risk=fakeout_risk,
                evaluated_at=time.time(),
            )
            self.last_decision = decision
            return decision

        except asyncio.TimeoutError:
            latency = (time.time() - t0) * 1000.0
            self.total_latency_ms += latency
            self.total_fallbacks += 1
            logger.warning(f"Jev API timed out after {self.timeout_s:.2f}s; abstaining to protect capital.")
            decision = JevDecision(
                action="abstain",
                passed=False,
                size_multiplier=0.0,
                conviction=0.0,
                reversal_risk=1.0,
                market_regime="unknown_timeout",
                confidence=0.0,
                reason=f"Jev timeout ({self.timeout_s:.1f}s); abstaining",
                latency_ms=round(latency, 1),
                fallback_used=True,
                evaluated_at=time.time(),
            )
            self.total_vetoed += 1
            self.last_decision = decision
            return decision

        except Exception as e:
            latency = (time.time() - t0) * 1000.0
            self.total_latency_ms += latency
            self.total_fallbacks += 1
            logger.warning(f"Jev evaluation failed ({e}); abstaining to protect capital.")
            decision = JevDecision(
                action="abstain",
                passed=False,
                size_multiplier=0.0,
                conviction=0.0,
                reversal_risk=1.0,
                market_regime="error_fallback",
                confidence=0.0,
                reason=f"Jev error ({e}); abstaining",
                latency_ms=round(latency, 1),
                fallback_used=True,
                evaluated_at=time.time(),
            )
            self.total_vetoed += 1
            self.last_decision = decision
            return decision
