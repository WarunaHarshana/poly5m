"""Terminal diffusion Z-score and calibrated probability model.

Implements the empirical findings from quantitative hypothesis research:
- H-001: Terminal Diffusion Phase Transition (Z-score scaling over remaining horizon tau)
- H-002: Selective Chop Pin-Risk Filter on marginal moves
- H-003: Inter-Slot Micro-Volatility Clustering & Dynamic Conviction Multiplier
- H-004: Trading Session Regimes (London, US-Europe Overlap, Asian, US Afternoon)
- H-005: Verified RTDS Benchmark Strike Gating (Rejects exchange-proxy settlement skew)
"""
from __future__ import annotations

import math
from typing import NamedTuple


class MomentumSignal(NamedTuple):
    side: str                   # "UP" or "DOWN"
    z_score: float             # Standardized displacement z = |spot - strike| / sigma_tau
    signed_z: float            # Signed z (+ for UP, - for DOWN)
    sigma_tau: float           # Estimated terminal standard deviation in USD over tau
    model_prob: float          # Calibrated empirical win probability in [0.5, 0.999]
    edge: float                # model_prob - ask
    edge_net: float            # model_prob - ask - fee_per_share
    passed_gates: bool         # True if all filters (Z, chop, edge, band, strike) passed
    skip_reason: str | None    # Explanation if skipped
    session_name: str          # Current market trading session (H-004)
    regime_mult: float         # Inter-slot volatility conviction multiplier (H-003)
    strike_verified: bool      # True if strike is from official Chainlink Data Streams (H-005)


def classify_session(utc_hour: int | None) -> tuple[str, float]:
    """Classify the current UTC hour into market trading sessions and expected volatility baseline (H-004).

    Returns (session_name, expected_avg_move_usd).
    """
    if utc_hour is None:
        return "Unknown Session", 25.0
    h = int(utc_hour) % 24
    if 8 <= h < 12:
        return "London Opening (High Accuracy)", 22.8
    elif 12 <= h < 16:
        return "US-Europe Overlap (Peak Volatility)", 44.7
    elif 16 <= h < 21:
        return "US Afternoon (Moderate Volatility)", 28.3
    elif 0 <= h < 8:
        return "Asian Session (Low Volatility)", 25.2
    else:
        return "Pacific / Late US (Quiet)", 23.4


def calculate_regime_multiplier(prev_slot_abs_move: float | None) -> float:
    """Compute conviction sizing multiplier based on previous slot's displacement (H-003).

    Empirical evidence:
    - Prev move >= $30 -> 59.97% prob of next move >= $20 (High Volatility Cluster) -> 1.20x
    - Prev move < $10  -> 32.83% prob of next move >= $20 (Low Volatility Chop Cluster) -> 0.80x
    - Normal move ($10-$30) -> 1.00x
    """
    if prev_slot_abs_move is None:
        return 1.00
    m = float(prev_slot_abs_move)
    if m >= 30.0:
        return 1.20
    elif m < 10.0:
        return 0.80
    return 1.00


class DiffusionPricingModel:
    """Calibrated terminal diffusion model for short-horizon binary contracts."""

    DEFAULT_ALPHA = 1.75          # Logistic steepness calibrated on 2,439 historical slots
    DEFAULT_MIN_SIGMA_USD = 2.5   # Minimum USD diffusion floor over tau to prevent division-by-zero
    DEFAULT_MIN_Z = 1.8           # Minimum Z-score (~98%+ empirical win rate threshold)
    DEFAULT_HIGH_Z_BREAKOUT = 2.2 # Breakout threshold where chop pinning no longer degrades signal
    DEFAULT_MAX_CHOP_MARGINAL = 3 # Max strike crossings permitted when 1.8 <= Z < 2.2

    def __init__(
        self,
        alpha: float = DEFAULT_ALPHA,
        min_sigma_usd: float = DEFAULT_MIN_SIGMA_USD,
        min_z: float = DEFAULT_MIN_Z,
        high_z_breakout: float = DEFAULT_HIGH_Z_BREAKOUT,
        max_chop_marginal: int = DEFAULT_MAX_CHOP_MARGINAL,
    ):
        self.alpha = float(alpha)
        self.min_sigma_usd = float(min_sigma_usd)
        self.min_z = float(min_z)
        self.high_z_breakout = float(high_z_breakout)
        self.max_chop_marginal = int(max_chop_marginal)

    def estimate_sigma_tau(
        self,
        spot: float,
        remaining_s: float,
        realized_vol_daily: float | None = None,
        var_per_sec: float | None = None,
    ) -> float:
        """Estimate the standard deviation of spot displacement over remaining_s seconds in USD."""
        tau = max(1.0, min(300.0, float(remaining_s)))
        if var_per_sec is not None and var_per_sec > 0:
            v_sec = float(var_per_sec)
        elif realized_vol_daily is not None and realized_vol_daily > 0:
            v_sec = (float(realized_vol_daily) ** 2) / 86400.0
        else:
            # Default to ~0.0106 daily vol benchmark (historical median)
            v_sec = (0.0106 ** 2) / 86400.0

        sigma_tau = spot * math.sqrt(v_sec * tau)
        return max(self.min_sigma_usd, float(sigma_tau))

    def calculate_z_score(
        self,
        spot: float,
        strike: float,
        sigma_tau: float,
    ) -> tuple[float, float]:
        """Return (abs_z, signed_z)."""
        if sigma_tau <= 0:
            sigma_tau = self.min_sigma_usd
        move = spot - strike
        signed_z = move / sigma_tau
        abs_z = abs(signed_z)
        return abs_z, signed_z

    def calibrated_probability(self, abs_z: float) -> float:
        """Calibrated empirical probability of winning based on standardized Z-score.

        Maps Z >= 0 to [0.50, 0.999] via logistic S-curve: P(Z) = 1 / (1 + exp(-alpha * Z)).
        """
        z = max(0.0, float(abs_z))
        try:
            prob = 1.0 / (1.0 + math.exp(-self.alpha * z))
        except OverflowError:
            prob = 0.999
        return min(0.999, max(0.50, prob))

    def evaluate(
        self,
        spot: float,
        strike: float,
        remaining_s: float,
        ask: float | None,
        chop_crosses: int = 0,
        realized_vol_daily: float | None = None,
        var_per_sec: float | None = None,
        strike_src: str | None = None,
        require_verified_strike: bool = True,
        prev_slot_abs_move: float | None = None,
        utc_hour: int | None = None,
        min_entry: float = 0.50,
        max_entry: float = 0.995,
        min_edge: float = 0.00,
        max_edge: float | None = None,
        fee_rate: float = 0.00,
    ) -> MomentumSignal:
        """Evaluate a slot's late momentum features and return a structured MomentumSignal."""
        move = spot - strike
        side = "UP" if move >= 0 else "DOWN"

        session_name, _ = classify_session(utc_hour)
        regime_mult = calculate_regime_multiplier(prev_slot_abs_move)
        strike_verified = (strike_src == "chainlink-data-stream")

        sigma_tau = self.estimate_sigma_tau(
            spot=spot,
            remaining_s=remaining_s,
            realized_vol_daily=realized_vol_daily,
            var_per_sec=var_per_sec,
        )
        abs_z, signed_z = self.calculate_z_score(spot, strike, sigma_tau)
        model_prob = self.calibrated_probability(abs_z)

        if ask is None or ask <= 0 or ask >= 1.0:
            return MomentumSignal(
                side=side,
                z_score=abs_z,
                signed_z=signed_z,
                sigma_tau=sigma_tau,
                model_prob=model_prob,
                edge=0.0,
                edge_net=0.0,
                passed_gates=False,
                skip_reason="missing or invalid order book ask",
                session_name=session_name,
                regime_mult=regime_mult,
                strike_verified=strike_verified,
            )

        fee = ask * fee_rate if fee_rate > 0 else 0.0
        edge = model_prob - ask
        edge_net = edge - fee

        # 1. Check Verified RTDS Strike Guard (H-005)
        if require_verified_strike and not strike_verified:
            return MomentumSignal(
                side=side,
                z_score=abs_z,
                signed_z=signed_z,
                sigma_tau=sigma_tau,
                model_prob=model_prob,
                edge=edge,
                edge_net=edge_net,
                passed_gates=False,
                skip_reason=f"unverified strike source '{strike_src}' (Chainlink RTDS required to prevent settlement skew)",
                session_name=session_name,
                regime_mult=regime_mult,
                strike_verified=strike_verified,
            )

        # 2. Check Z-Score threshold (H-001)
        effective_min_z = self.min_z
        # If in high-volatility cluster (H-003), allow slightly lower min_z (1.70)
        if regime_mult > 1.0:
            effective_min_z = max(1.65, self.min_z - 0.10)

        if abs_z < effective_min_z:
            return MomentumSignal(
                side=side,
                z_score=abs_z,
                signed_z=signed_z,
                sigma_tau=sigma_tau,
                model_prob=model_prob,
                edge=edge,
                edge_net=edge_net,
                passed_gates=False,
                skip_reason=f"z-score {abs_z:.2f} < min {effective_min_z:.2f} (|move|=${abs(move):.2f}, sigma_tau=${sigma_tau:.2f})",
                session_name=session_name,
                regime_mult=regime_mult,
                strike_verified=strike_verified,
            )

        # 3. Check Chop Pin-Risk Filter (H-002)
        # If move is marginal (min_z <= Z < high_z_breakout), enforce strict chop threshold
        if abs_z < self.high_z_breakout and chop_crosses > self.max_chop_marginal:
            return MomentumSignal(
                side=side,
                z_score=abs_z,
                signed_z=signed_z,
                sigma_tau=sigma_tau,
                model_prob=model_prob,
                edge=edge,
                edge_net=edge_net,
                passed_gates=False,
                skip_reason=f"marginal z {abs_z:.2f} with choppy path ({chop_crosses} strike crosses > {self.max_chop_marginal})",
                session_name=session_name,
                regime_mult=regime_mult,
                strike_verified=strike_verified,
            )

        # 4. Entry price band check
        if ask < min_entry or ask > max_entry:
            return MomentumSignal(
                side=side,
                z_score=abs_z,
                signed_z=signed_z,
                sigma_tau=sigma_tau,
                model_prob=model_prob,
                edge=edge,
                edge_net=edge_net,
                passed_gates=False,
                skip_reason=f"{side} ask {ask:.3f} outside entry band [{min_entry:.2f}, {max_entry:.2f}]",
                session_name=session_name,
                regime_mult=regime_mult,
                strike_verified=strike_verified,
            )

        # 5. Net edge check
        if edge_net < min_edge:
            return MomentumSignal(
                side=side,
                z_score=abs_z,
                signed_z=signed_z,
                sigma_tau=sigma_tau,
                model_prob=model_prob,
                edge=edge,
                edge_net=edge_net,
                passed_gates=False,
                skip_reason=f"net edge {edge_net:+.3f} < min {min_edge:+.3f} (model {model_prob:.3f} vs ask {ask:.3f})",
                session_name=session_name,
                regime_mult=regime_mult,
                strike_verified=strike_verified,
            )

        if max_edge is not None and edge > max_edge:
            return MomentumSignal(
                side=side,
                z_score=abs_z,
                signed_z=signed_z,
                sigma_tau=sigma_tau,
                model_prob=model_prob,
                edge=edge,
                edge_net=edge_net,
                passed_gates=False,
                skip_reason=f"edge {edge:+.3f} > ceiling {max_edge:+.3f} (suspect quote/strike anomaly)",
                session_name=session_name,
                regime_mult=regime_mult,
                strike_verified=strike_verified,
            )

        return MomentumSignal(
            side=side,
            z_score=abs_z,
            signed_z=signed_z,
            sigma_tau=sigma_tau,
            model_prob=model_prob,
            edge=edge,
            edge_net=edge_net,
            passed_gates=True,
            skip_reason=None,
            session_name=session_name,
            regime_mult=regime_mult,
            strike_verified=strike_verified,
        )
