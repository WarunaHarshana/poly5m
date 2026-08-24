"""Terminal TWAP path-accumulation and diffusion probability model for Polymarket 5-minute markets.

Implements the empirical findings from quantitative hypothesis research:
- H-001: Realized TWAP Path-Accumulation & Cubic Variance Decay (tau^3 / 3)
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
    z_score: float             # Standardized displacement z = |spot - s_req| / sigma_twap
    signed_z: float            # Signed z (+ for UP, - for DOWN)
    sigma_tau: float           # Estimated remaining TWAP standard deviation in USD over tau
    model_prob: float          # Calibrated empirical win probability in [0.5, 0.999]
    edge: float                # model_prob - ask
    edge_net: float            # model_prob - ask - fee_per_share
    passed_gates: bool         # True if all filters (Z, chop, edge, band, strike) passed
    skip_reason: str | None    # Explanation if skipped
    session_name: str          # Current market trading session (H-004)
    regime_mult: float         # Inter-slot volatility conviction multiplier (H-003)
    strike_verified: bool      # True if strike is from official Chainlink Data Streams (H-005)
    realized_twap: float | None = None  # Realized TWAP over elapsed window
    s_req: float | None = None          # Required remaining spot average


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


class TWAPPricingModel:
    """Analytical TWAP path-accumulation and diffusion model for Polymarket 5-minute markets."""

    DEFAULT_ALPHA = 1.75          # Calibrated logistic steepness on binary outcome transitions
    DEFAULT_MIN_SIGMA_USD = 1.0   # Minimum USD diffusion floor to prevent division-by-zero
    DEFAULT_MIN_Z = 1.8           # High-conviction Z-score threshold (~96%+ win probability)
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
        """Estimate standard deviation of spot displacement in USD over tau remaining seconds."""
        tau = max(1.0, min(300.0, float(remaining_s)))
        if var_per_sec is not None and var_per_sec > 0:
            v_sec = float(var_per_sec)
        elif realized_vol_daily is not None and realized_vol_daily > 0:
            v_sec = (float(realized_vol_daily) ** 2) / 86400.0
        else:
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

    def estimate_sigma_twap_usd(
        self,
        spot: float,
        remaining_s: float,
        var_per_sec: float | None = None,
        realized_vol_daily: float | None = None,
    ) -> float:
        """Estimate standard deviation of the remaining TWAP integral in USD over tau remaining seconds.
        
        Formula: sigma_twap = S(t) * sqrt( (var_per_sec * tau) / 3 )
        """
        tau = max(1.0, min(300.0, float(remaining_s)))
        if var_per_sec is not None and var_per_sec > 0:
            v_sec = float(var_per_sec)
        elif realized_vol_daily is not None and realized_vol_daily > 0:
            v_sec = (float(realized_vol_daily) ** 2) / 86400.0
        else:
            # Default ~0.0106 daily vol benchmark (~25% annualized vol)
            v_sec = (0.0106 ** 2) / 86400.0

        # Remaining TWAP variance has integral factor tau / 3
        sigma_twap = spot * math.sqrt((v_sec * tau) / 3.0)
        return max(self.min_sigma_usd, float(sigma_twap))

    def calculate_twap_z_score(
        self,
        spot: float,
        strike: float,
        realized_twap: float | None,
        elapsed_s: float,
        remaining_s: float,
        sigma_twap_usd: float,
        total_window_s: float = 300.0,
    ) -> tuple[float, float, float, float]:
        """Calculate TWAP Z-score and required remaining spot price.
        
        Returns: (abs_z, signed_z, s_req, realized_twap_effective)
        """
        tau = max(1.0, float(remaining_s))
        t_el = max(0.0, float(elapsed_s))
        total_t = t_el + tau if (t_el + tau) > 0 else total_window_s

        r_twap = realized_twap if (realized_twap is not None and realized_twap > 0) else spot

        # Realized integral
        i_realized = t_el * r_twap
        # Target total integral
        i_target = total_t * strike
        # Required remaining average spot price to achieve TWAP >= strike
        s_req = (i_target - i_realized) / tau

        # Standardized cushion: positive when current spot exceeds required remaining average
        move = spot - s_req
        if sigma_twap_usd <= 0:
            sigma_twap_usd = self.min_sigma_usd

        signed_z = move / sigma_twap_usd
        abs_z = abs(signed_z)
        return abs_z, signed_z, s_req, r_twap

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
        realized_twap: float | None = None,
        elapsed_s: float | None = None,
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
        """Evaluate TWAP path accumulation features and return a structured MomentumSignal."""
        tau = max(1.0, min(300.0, float(remaining_s)))
        t_el = (300.0 - tau) if elapsed_s is None else max(0.0, float(elapsed_s))

        session_name, _ = classify_session(utc_hour)
        regime_mult = calculate_regime_multiplier(prev_slot_abs_move)
        strike_verified = (strike_src == "chainlink-data-stream")

        sigma_twap = self.estimate_sigma_twap_usd(
            spot=spot,
            remaining_s=tau,
            var_per_sec=var_per_sec,
            realized_vol_daily=realized_vol_daily,
        )

        abs_z, signed_z, s_req, eff_twap = self.calculate_twap_z_score(
            spot=spot,
            strike=strike,
            realized_twap=realized_twap,
            elapsed_s=t_el,
            remaining_s=tau,
            sigma_twap_usd=sigma_twap,
        )

        side = "UP" if signed_z >= 0 else "DOWN"
        model_prob = self.calibrated_probability(abs_z)

        if ask is None or ask <= 0 or ask >= 1.0:
            return MomentumSignal(
                side=side,
                z_score=abs_z,
                signed_z=signed_z,
                sigma_tau=sigma_twap,
                model_prob=model_prob,
                edge=0.0,
                edge_net=0.0,
                passed_gates=False,
                skip_reason="missing or invalid order book ask",
                session_name=session_name,
                regime_mult=regime_mult,
                strike_verified=strike_verified,
                realized_twap=eff_twap,
                s_req=s_req,
            )

        # Polymarket crypto feeSchedule: fee = rate * price * (1 - price)
        fee = (fee_rate * ask * (1.0 - ask)) if fee_rate > 0 else 0.0
        edge = model_prob - ask
        edge_net = edge - fee

        # 1. Check Verified RTDS Strike Guard (H-005)
        if require_verified_strike and not strike_verified:
            return MomentumSignal(
                side=side,
                z_score=abs_z,
                signed_z=signed_z,
                sigma_tau=sigma_twap,
                model_prob=model_prob,
                edge=edge,
                edge_net=edge_net,
                passed_gates=False,
                skip_reason=f"unverified strike source '{strike_src}' (Chainlink RTDS required to prevent settlement skew)",
                session_name=session_name,
                regime_mult=regime_mult,
                strike_verified=strike_verified,
                realized_twap=eff_twap,
                s_req=s_req,
            )

        # 2. Check Z-Score threshold (H-001)
        effective_min_z = self.min_z
        if regime_mult > 1.0:
            effective_min_z = max(1.65, self.min_z - 0.10)

        if abs_z < effective_min_z:
            return MomentumSignal(
                side=side,
                z_score=abs_z,
                signed_z=signed_z,
                sigma_tau=sigma_twap,
                model_prob=model_prob,
                edge=edge,
                edge_net=edge_net,
                passed_gates=False,
                skip_reason=f"twap z-score {abs_z:.2f} < min {effective_min_z:.2f} (spot=${spot:.2f}, s_req=${s_req:.2f}, sigma_twap=${sigma_twap:.2f})",
                session_name=session_name,
                regime_mult=regime_mult,
                strike_verified=strike_verified,
                realized_twap=eff_twap,
                s_req=s_req,
            )

        # 3. Check Chop Pin-Risk Filter (H-002)
        if abs_z < self.high_z_breakout and chop_crosses > self.max_chop_marginal:
            return MomentumSignal(
                side=side,
                z_score=abs_z,
                signed_z=signed_z,
                sigma_tau=sigma_twap,
                model_prob=model_prob,
                edge=edge,
                edge_net=edge_net,
                passed_gates=False,
                skip_reason=f"marginal twap z {abs_z:.2f} with choppy path ({chop_crosses} strike crosses > {self.max_chop_marginal})",
                session_name=session_name,
                regime_mult=regime_mult,
                strike_verified=strike_verified,
                realized_twap=eff_twap,
                s_req=s_req,
            )

        # 4. Entry price band check
        if ask < min_entry or ask > max_entry:
            return MomentumSignal(
                side=side,
                z_score=abs_z,
                signed_z=signed_z,
                sigma_tau=sigma_twap,
                model_prob=model_prob,
                edge=edge,
                edge_net=edge_net,
                passed_gates=False,
                skip_reason=f"{side} ask {ask:.3f} outside entry band [{min_entry:.2f}, {max_entry:.2f}]",
                session_name=session_name,
                regime_mult=regime_mult,
                strike_verified=strike_verified,
                realized_twap=eff_twap,
                s_req=s_req,
            )

        # 5. Net edge check
        if edge_net < min_edge:
            return MomentumSignal(
                side=side,
                z_score=abs_z,
                signed_z=signed_z,
                sigma_tau=sigma_twap,
                model_prob=model_prob,
                edge=edge,
                edge_net=edge_net,
                passed_gates=False,
                skip_reason=f"net edge {edge_net:+.3f} < min {min_edge:+.3f} (model {model_prob:.3f} vs ask {ask:.3f})",
                session_name=session_name,
                regime_mult=regime_mult,
                strike_verified=strike_verified,
                realized_twap=eff_twap,
                s_req=s_req,
            )

        if max_edge is not None and edge > max_edge:
            return MomentumSignal(
                side=side,
                z_score=abs_z,
                signed_z=signed_z,
                sigma_tau=sigma_twap,
                model_prob=model_prob,
                edge=edge,
                edge_net=edge_net,
                passed_gates=False,
                skip_reason=f"edge {edge:+.3f} > ceiling {max_edge:+.3f} (suspect quote anomaly)",
                session_name=session_name,
                regime_mult=regime_mult,
                strike_verified=strike_verified,
                realized_twap=eff_twap,
                s_req=s_req,
            )

        return MomentumSignal(
            side=side,
            z_score=abs_z,
            signed_z=signed_z,
            sigma_tau=sigma_twap,
            model_prob=model_prob,
            edge=edge,
            edge_net=edge_net,
            passed_gates=True,
            skip_reason=None,
            session_name=session_name,
            regime_mult=regime_mult,
            strike_verified=strike_verified,
            realized_twap=eff_twap,
            s_req=s_req,
        )


# Alias for backwards compatibility with existing callers
DiffusionPricingModel = TWAPPricingModel
