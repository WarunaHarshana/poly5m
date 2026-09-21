"""TWAP Path-Accumulation Pricing Engine (H-001).

Implements analytical TWAP settlement probability for Polymarket 5-Minute BTC Up/Down markets:
1. Realized TWAP path accumulation: I_realized(t) = integral_0^t S(u) du
2. Required remaining average spot price: S_req(tau) = (300 * S_0 - I_realized(t)) / tau
3. Cubic variance decay of remaining TWAP: sigma_twap(tau) = S(t) * sqrt(sigma^2 * tau / 3)
4. Standardized TWAP Z-Score: Z_twap = (S(t) - S_req(tau)) / sigma_twap(tau)
5. Calibrated win probability: P(Z) = 1 / (1 + exp(-alpha * |Z|))
6. Polymarket dynamic crypto taker fee: fee = rate * p * (1 - p)
7. Net edge calculation and decision gating.
"""
from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(slots=True)
class TWAPSignal:
    """Evaluation result from the TWAP Pricing Model (Taker)."""
    side: str                        # 'UP', 'DOWN', or 'FLAT'
    z_score: float                   # Standardized TWAP Z-score magnitude
    signed_z: float                  # Signed Z (+ for UP, - for DOWN)
    model_prob: float                # Calibrated win probability P in [0.5, 1.0)
    ask: float | None                # Order book best ask price
    edge_gross: float                # model_prob - ask
    fee_taker: float                 # Dynamic taker fee
    edge_net: float                  # edge_gross - fee_taker
    s_req: float                     # Required remaining average spot price
    s_0: float                       # Slot strike (price at t=0)
    spot: float                      # Current BTC spot price
    realized_twap: float             # Realized TWAP over elapsed time
    elapsed_s: float                 # Elapsed seconds in slot
    remaining_s: float               # Remaining seconds in slot (tau)
    sigma_twap_usd: float            # Dispersion of remaining TWAP in USD
    passed_gates: bool               # True if all entry conditions satisfied
    skip_reason: str = ""            # Explanation if passed_gates is False


@dataclass(slots=True)
class MakerSignal:
    """Evaluation result from the H-004 Passive Maker Limit Engine."""
    side: str                        # 'UP' or 'DOWN'
    z_score: float                   # Standardized TWAP Z-score magnitude
    signed_z: float                  # Signed Z (+ for UP, - for DOWN)
    model_prob: float                # Theoretical Fair Value P in [0.5, 1.0)
    target_bid: float                # Optimal resting limit bid price
    best_bid: float | None           # Current CLOB top bid
    best_ask: float | None           # Current CLOB top ask
    maker_rebate: float              # 20% fee rebate on taker volume
    edge_net: float                  # Net maker edge: model_prob - target_bid + rebate
    s_req: float                     # Required remaining average spot price
    s_0: float                       # Slot strike (price at t=0)
    spot: float                      # Current BTC spot price
    realized_twap: float             # Realized TWAP over elapsed time
    elapsed_s: float                 # Elapsed seconds in slot
    remaining_s: float               # Remaining seconds in slot (tau)
    sigma_twap_usd: float            # Dispersion of remaining TWAP in USD
    passed_gates: bool               # True if all maker entry conditions satisfied
    skip_reason: str = ""            # Explanation if passed_gates is False


class TWAPEngine:
    """Analytical engine for TWAP Mispricing and Passive Maker Limit Trading."""

    def __init__(
        self,
        alpha: float = 1.75,
        min_sigma_usd: float = 2.0,
        min_z: float = 1.45,
        min_edge: float = 0.05,
        min_entry: float = 0.50,
        max_entry: float = 0.95,
        min_maker_bid: float = 0.55,
        max_maker_bid: float = 0.90,
        min_maker_edge: float = 0.05,
        max_chop_crosses: int = 2,
        default_daily_vol: float = 0.0106,
    ):
        self.alpha = float(alpha)
        self.min_sigma_usd = float(min_sigma_usd)
        self.min_z = float(min_z)
        self.min_edge = float(min_edge)
        self.min_entry = float(min_entry)
        self.max_entry = float(max_entry)
        self.min_maker_bid = float(min_maker_bid)
        self.max_maker_bid = float(max_maker_bid)
        self.min_maker_edge = float(min_maker_edge)
        self.max_chop_crosses = int(max_chop_crosses)
        self.default_daily_vol = float(default_daily_vol)

    def estimate_sigma_twap_usd(
        self,
        spot: float,
        remaining_s: float,
        realized_vol_daily: float | None = None,
        var_per_sec: float | None = None,
        min_sigma_usd: float | None = None,
    ) -> float:
        """Calculate analytical std dev of remaining TWAP: sigma_twap = spot * sqrt(var_sec * tau / 3)."""
        tau = max(1.0, min(300.0, float(remaining_s)))
        if var_per_sec is not None and var_per_sec > 0:
            v_sec = float(var_per_sec)
        elif realized_vol_daily is not None and realized_vol_daily > 0:
            v_sec = (float(realized_vol_daily) ** 2) / 86400.0
        else:
            v_sec = (self.default_daily_vol ** 2) / 86400.0

        # Remaining TWAP integral variance is Var(1/tau * int_0^tau B_t dt) = sigma^2 * tau / 3
        sigma_twap = spot * math.sqrt(v_sec * tau / 3.0)
        floor = self.min_sigma_usd if min_sigma_usd is None else float(min_sigma_usd)
        return max(floor, float(sigma_twap))

    def compute_s_req_and_z(
        self,
        spot: float,
        strike: float,
        realized_twap: float,
        elapsed_s: float,
        remaining_s: float,
        sigma_twap_usd: float,
    ) -> tuple[float, float, float]:
        """Return (abs_z, signed_z, s_req)."""
        elapsed = max(0.0, min(300.0, float(elapsed_s)))
        remaining = max(1.0, min(300.0, float(remaining_s)))
        total_window = max(1.0, elapsed + remaining)

        # Realized path integral: I_realized = elapsed * realized_twap
        # Target path integral: I_target = total_window * strike
        target_integral = total_window * strike
        realized_integral = elapsed * realized_twap
        remaining_integral = target_integral - realized_integral
        s_req = remaining_integral / remaining

        dispersion = max(1e-12, float(sigma_twap_usd))
        signed_z = (spot - s_req) / dispersion
        abs_z = abs(signed_z)
        return abs_z, signed_z, s_req

    def calibrated_probability(self, z: float) -> float:
        """Map Z-score to calibrated win probability via logistic S-curve."""
        mag = abs(float(z))
        prob = 1.0 / (1.0 + math.exp(-self.alpha * mag))
        return max(0.50, min(0.999, prob))

    @staticmethod
    def calculate_taker_fee(price: float, fee_rate: float = 0.07) -> float:
        """Polymarket crypto taker fee formula: fee = rate * p * (1 - p)."""
        p = max(0.0, min(1.0, float(price)))
        return float(fee_rate) * p * (1.0 - p)

    def evaluate(
        self,
        spot: float,
        strike: float,
        remaining_s: float,
        elapsed_s: float,
        realized_twap: float,
        ask: float | None = None,
        chop_crosses: int = 0,
        realized_vol_daily: float | None = None,
        var_per_sec: float | None = None,
        fee_rate: float = 0.07,
        min_sigma_usd: float | None = None,
    ) -> TWAPSignal:
        """Evaluate TWAP mispricing and determine trade eligibility."""
        sigma_twap = self.estimate_sigma_twap_usd(
            spot=spot,
            remaining_s=remaining_s,
            realized_vol_daily=realized_vol_daily,
            var_per_sec=var_per_sec,
            min_sigma_usd=min_sigma_usd,
        )

        abs_z, signed_z, s_req = self.compute_s_req_and_z(
            spot=spot,
            strike=strike,
            realized_twap=realized_twap,
            elapsed_s=elapsed_s,
            remaining_s=remaining_s,
            sigma_twap_usd=sigma_twap,
        )

        model_prob = self.calibrated_probability(abs_z)
        side = "UP" if signed_z >= 0 else "DOWN"

        # Taker fee and edge
        if ask is not None and 0.0 < ask < 1.0:
            fee_taker = self.calculate_taker_fee(ask, fee_rate)
            edge_gross = model_prob - ask
            edge_net = edge_gross - fee_taker
        else:
            fee_taker = 0.0
            edge_gross = 0.0
            edge_net = 0.0

        # --- Decision Gatekeeper ---
        passed = True
        skip_reasons = []

        if abs_z < self.min_z:
            passed = False
            skip_reasons.append(f"Z-score {abs_z:.2f} < threshold {self.min_z:.2f}")

        if chop_crosses > self.max_chop_crosses and abs_z < 2.5:
            passed = False
            skip_reasons.append(f"chop crosses {chop_crosses} > max {self.max_chop_crosses}")

        if ask is None:
            passed = False
            skip_reasons.append("no ask price available")
        elif ask < self.min_entry:
            passed = False
            skip_reasons.append(f"ask {ask:.3f} < min entry {self.min_entry:.2f}")
        elif ask > self.max_entry:
            passed = False
            skip_reasons.append(f"ask {ask:.3f} > max entry {self.max_entry:.2f}")
        elif edge_net < self.min_edge:
            passed = False
            skip_reasons.append(f"net edge {edge_net:+.3f} < required {self.min_edge:+.3f}")

        return TWAPSignal(
            side=side,
            z_score=abs_z,
            signed_z=signed_z,
            model_prob=model_prob,
            ask=ask,
            edge_gross=edge_gross,
            fee_taker=fee_taker,
            edge_net=edge_net,
            s_req=s_req,
            s_0=strike,
            spot=spot,
            realized_twap=realized_twap,
            elapsed_s=elapsed_s,
            remaining_s=remaining_s,
            sigma_twap_usd=sigma_twap,
            passed_gates=passed,
            skip_reason="; ".join(skip_reasons),
        )

    @staticmethod
    def calculate_maker_rebate(price: float, fee_rate: float = 0.07, rebate_share: float = 0.20) -> float:
        """Polymarket crypto maker rebate: rebate = rebate_share * (fee_rate * p * (1 - p))."""
        p = max(0.0, min(1.0, float(price)))
        return float(rebate_share) * float(fee_rate) * p * (1.0 - p)

    def evaluate_maker(
        self,
        spot: float,
        strike: float,
        remaining_s: float,
        elapsed_s: float,
        realized_twap: float,
        best_bid: float | None = None,
        best_ask: float | None = None,
        chop_crosses: int = 0,
        realized_vol_daily: float | None = None,
        var_per_sec: float | None = None,
        fee_rate: float = 0.07,
        min_sigma_usd: float | None = None,
    ) -> MakerSignal:
        """Evaluate and price optimal resting limit bid for H-004 Passive Maker Strategy."""
        sigma_twap = self.estimate_sigma_twap_usd(
            spot=spot,
            remaining_s=remaining_s,
            realized_vol_daily=realized_vol_daily,
            var_per_sec=var_per_sec,
            min_sigma_usd=min_sigma_usd,
        )

        abs_z, signed_z, s_req = self.compute_s_req_and_z(
            spot=spot,
            strike=strike,
            realized_twap=realized_twap,
            elapsed_s=elapsed_s,
            remaining_s=remaining_s,
            sigma_twap_usd=sigma_twap,
        )

        model_prob = self.calibrated_probability(abs_z)
        side = "UP" if signed_z >= 0 else "DOWN"

        # --- Calculate Dynamic Fair-Value Limit Bid Price ---
        # 1. Target bid is pegged to Fair Value minus required maker margin (e.g. 0.92 - 0.06 = 0.86)
        max_allowed_bid = min(self.max_maker_bid, model_prob - self.min_maker_edge)
        target_bid = round(model_prob - self.min_maker_edge, 2)

        # 2. If a competitive top bid exists, step 1 tick above it to gain priority
        if best_bid is not None and 0.50 <= best_bid < max_allowed_bid:
            target_bid = max(target_bid, round(best_bid + 0.01, 2))

        # 3. Apply bounds: must not cross ask, must not exceed max_allowed_bid, must be >= min_maker_bid
        if best_ask is not None and best_ask > 0:
            target_bid = min(target_bid, round(best_ask - 0.01, 2))
        
        target_bid = min(target_bid, round(max_allowed_bid, 2))
        target_bid = max(target_bid, self.min_maker_bid)

        # Maker rebate and net edge
        maker_rebate = self.calculate_maker_rebate(target_bid, fee_rate=fee_rate)
        edge_net = (model_prob - target_bid) + maker_rebate

        # --- Decision Gatekeeper for Maker ---
        passed = True
        skip_reasons = []

        if abs_z < self.min_z:
            passed = False
            skip_reasons.append(f"Z-score {abs_z:.2f} < threshold {self.min_z:.2f}")

        if chop_crosses > self.max_chop_crosses and abs_z < 2.5:
            passed = False
            skip_reasons.append(f"chop crosses {chop_crosses} > max {self.max_chop_crosses}")

        if target_bid < self.min_maker_bid:
            passed = False
            skip_reasons.append(f"bid {target_bid:.2f} < min maker bid {self.min_maker_bid:.2f}")
        elif target_bid > self.max_maker_bid:
            passed = False
            skip_reasons.append(f"bid {target_bid:.2f} > max maker bid {self.max_maker_bid:.2f}")
        elif edge_net < self.min_maker_edge:
            passed = False
            skip_reasons.append(f"maker edge {edge_net:+.3f} < required {self.min_maker_edge:+.3f}")

        return MakerSignal(
            side=side,
            z_score=abs_z,
            signed_z=signed_z,
            model_prob=model_prob,
            target_bid=target_bid,
            best_bid=best_bid,
            best_ask=best_ask,
            maker_rebate=maker_rebate,
            edge_net=edge_net,
            s_req=s_req,
            s_0=strike,
            spot=spot,
            realized_twap=realized_twap,
            elapsed_s=elapsed_s,
            remaining_s=remaining_s,
            sigma_twap_usd=sigma_twap,
            passed_gates=passed,
            skip_reason="; ".join(skip_reasons),
        )


    def evaluate_midband(
        self,
        *,
        remaining_s: float,
        best_bid: float | None,
        best_ask: float | None,
        midband_lo: float = 0.40,
        midband_hi: float = 0.52,
        half_spread: float = 0.04,
        min_seconds_left: float = 75.0,
        max_seconds_left: float = 210.0,
        fee_rate: float = 0.07,
        rebate_share: float = 0.20,
        side: str = "UP",
        spot: float = 0.0,
        strike: float = 0.0,
        realized_twap: float = 0.0,
        elapsed_s: float = 0.0,
        min_z: float = 0.50,
        realized_vol_daily: float | None = None,
    ) -> MakerSignal:
        """Mid-band maker: rest a bid near mid only while mid is in [lo, hi].

        Uses TWAP Z-score for directional conviction — only passes gates if
        the requested `side` matches the direction spot is trending (signed_z).
        """
        skip_reasons: list[str] = []
        passed = True

        if best_bid is None or best_ask is None or best_bid <= 0 or best_ask <= 0:
            return MakerSignal(
                side=side,
                z_score=0.0,
                signed_z=0.0,
                model_prob=0.5,
                target_bid=0.0,
                best_bid=best_bid,
                best_ask=best_ask,
                maker_rebate=0.0,
                edge_net=0.0,
                s_req=0.0,
                s_0=strike,
                spot=spot,
                realized_twap=realized_twap,
                elapsed_s=elapsed_s,
                remaining_s=remaining_s,
                sigma_twap_usd=0.0,
                passed_gates=False,
                skip_reason="missing book",
            )

        mid = 0.5 * (float(best_bid) + float(best_ask))

        # --- TWAP Z-score for directional conviction ---
        sigma_twap = self.estimate_sigma_twap_usd(
            spot=spot,
            remaining_s=remaining_s,
            realized_vol_daily=realized_vol_daily,
        )
        abs_z, signed_z, s_req = self.compute_s_req_and_z(
            spot=spot,
            strike=strike,
            realized_twap=realized_twap,
            elapsed_s=elapsed_s,
            remaining_s=remaining_s,
            sigma_twap_usd=sigma_twap,
        )
        favored_side = "UP" if signed_z >= 0 else "DOWN"

        # Gate: side must match TWAP direction
        if side != favored_side:
            passed = False
            skip_reasons.append(f"side {side} != favored {favored_side} (Z={signed_z:+.2f})")

        # Gate: minimum Z-score for directional conviction
        if abs_z < min_z:
            passed = False
            skip_reasons.append(f"Z-score {abs_z:.2f} < min {min_z:.2f}")

        if remaining_s < min_seconds_left:
            passed = False
            skip_reasons.append(f"too late ({remaining_s:.0f}s < {min_seconds_left:.0f}s)")
        if remaining_s > max_seconds_left:
            passed = False
            skip_reasons.append(f"too early ({remaining_s:.0f}s > {max_seconds_left:.0f}s)")
        if not (midband_lo <= mid <= midband_hi):
            passed = False
            skip_reasons.append(f"mid {mid:.3f} outside [{midband_lo:.2f},{midband_hi:.2f}]")

        target_bid = round(mid - float(half_spread), 2)
        target_bid = max(midband_lo, min(midband_hi, target_bid))
        # Never cross the ask
        target_bid = min(target_bid, round(float(best_ask) - 0.01, 2))
        if target_bid < midband_lo or target_bid > midband_hi:
            passed = False
            skip_reasons.append(f"target bid {target_bid:.2f} outside band")
        if target_bid <= 0:
            passed = False
            skip_reasons.append("non-positive target bid")

        maker_rebate = self.calculate_maker_rebate(target_bid, fee_rate=fee_rate, rebate_share=rebate_share)
        model_prob = self.calibrated_probability(abs_z)
        # Fair value proxy = mid; edge is mid - bid + rebate
        edge_net = (mid - target_bid) + maker_rebate

        return MakerSignal(
            side=side,
            z_score=abs_z,
            signed_z=signed_z,
            model_prob=model_prob,
            target_bid=target_bid,
            best_bid=best_bid,
            best_ask=best_ask,
            maker_rebate=maker_rebate,
            edge_net=edge_net,
            s_req=s_req,
            s_0=strike,
            spot=spot,
            realized_twap=realized_twap,
            elapsed_s=elapsed_s,
            remaining_s=remaining_s,
            sigma_twap_usd=sigma_twap,
            passed_gates=passed,
            skip_reason="; ".join(skip_reasons),
        )

    def evaluate_momentum(
        self,
        spot: float,
        strike: float,
        remaining_s: float,
        elapsed_s: float,
        ask: float | None,
        fee_rate: float = 0.07,
        min_move_usd: float = 70.0,
        max_move_usd: float = 300.0,
        min_ask: float = 0.70,
        max_ask: float = 0.99,
        min_edge: float = 0.0,
        window_start_s: float = 150.0,
        window_end_s: float = 60.0,
    ) -> TWAPSignal:
        """Momentum-into-close gate (H-005): Novals/igus-style near-expiry follow-through.

        - Late window (default ~150s..60s remaining; target ~120s)
        - Require abs(spot - strike) >= min_move_usd (default 70 USD)
        - Side follows the move (never fade)
        - Buy only if ask in [min_ask, max_ask] (Novals threshold 0.70..0.99)
        - Edge gate only applies when min_edge > 0
        """
        spot = float(spot)
        strike = float(strike)
        remaining_s = float(remaining_s)
        elapsed_s = float(elapsed_s)
        move = spot - strike
        abs_move = abs(move)
        side = "UP" if move >= 0 else "DOWN"

        # Crude near-expiry confidence from move size (not TWAP math).
        # Scale the BTC-calibrated $50 / 0.04 logistic so alts with tiny USD moves still map.
        move_ref = max(1e-9, float(min_move_usd))
        offset = move_ref * (50.0 / 70.0)
        k = 0.04 * (70.0 / move_ref)
        model_prob = 1.0 / (1.0 + math.exp(-k * (abs_move - offset)))
        model_prob = min(0.97, max(0.55, model_prob))

        ask_v = float(ask) if ask is not None else None
        fee_taker = 0.0
        edge_gross = 0.0
        edge_net = 0.0
        if ask_v is not None and 0.0 < ask_v < 1.0:
            fee_taker = float(fee_rate) * ask_v * (1.0 - ask_v)
            edge_gross = model_prob - ask_v
            edge_net = edge_gross - fee_taker

        passed = True
        reasons: list[str] = []

        if not (window_end_s <= remaining_s <= window_start_s):
            passed = False
            reasons.append(
                f"outside momentum window ({remaining_s:.0f}s left; want {window_end_s:.0f}..{window_start_s:.0f})"
            )
        if abs_move < float(min_move_usd):
            passed = False
            reasons.append(f"|move| ${abs_move:.1f} < min ${float(min_move_usd):.1f}")
        if float(max_move_usd) > 0 and abs_move > float(max_move_usd):
            passed = False
            reasons.append(f"|move| ${abs_move:.1f} > max ${float(max_move_usd):.1f}")
        if ask_v is None:
            passed = False
            reasons.append("no ask")
        else:
            if ask_v < float(min_ask):
                passed = False
                reasons.append(f"ask {ask_v:.3f} < min {float(min_ask):.2f}")
            elif ask_v > float(max_ask):
                passed = False
                reasons.append(f"ask {ask_v:.3f} > max {float(max_ask):.2f}")
            elif float(min_edge) > 0 and edge_net < float(min_edge):
                passed = False
                reasons.append(f"net edge {edge_net:+.3f} < required {float(min_edge):+.3f}")

        return TWAPSignal(
            side=side,
            z_score=abs_move / max(offset, 1e-9),
            signed_z=(abs_move / max(offset, 1e-9)) if side == "UP" else -(abs_move / max(offset, 1e-9)),
            model_prob=model_prob,
            ask=ask_v,
            edge_gross=edge_gross,
            fee_taker=fee_taker,
            edge_net=edge_net,
            s_req=strike,
            s_0=strike,
            spot=spot,
            realized_twap=spot,
            elapsed_s=elapsed_s,
            remaining_s=remaining_s,
            sigma_twap_usd=abs_move,
            passed_gates=passed,
            skip_reason="; ".join(reasons),
        )
