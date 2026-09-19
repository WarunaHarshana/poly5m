"""Monkey-patch TWAPEngine with H-006 evaluate_midband (loaded from twap_engine)."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from twap_engine import MakerSignal, TWAPEngine


def _evaluate_midband(
    self,
    side: str,
    remaining_s: float,
    best_bid: float | None = None,
    best_ask: float | None = None,
    midband_lo: float = 0.40,
    midband_hi: float = 0.60,
    half_spread: float = 0.02,
    min_seconds_left: float = 75.0,
    max_seconds_left: float = 270.0,
    fee_rate: float = 0.07,
    rebate_share: float = 0.20,
    spot: float = 0.0,
    strike: float = 0.0,
    elapsed_s: float = 0.0,
    realized_twap: float = 0.0,
    tick: float = 0.01,
):
    """H-006 mid-band maker: quote near market mid when mid is in [lo, hi]. No Z-score gate."""
    from twap_engine import MakerSignal

    side_u = str(side).upper() if str(side).upper() in ("UP", "DOWN") else "UP"
    lo, hi, half = float(midband_lo), float(midband_hi), float(half_spread)
    tick = max(0.01, float(tick))
    remaining_s = float(remaining_s)
    passed, skip_reasons = True, []
    mid = target_bid = maker_rebate = edge_net = 0.0

    if best_bid is None or best_ask is None:
        passed = False
        skip_reasons.append("require bid+ask")
    else:
        bb, ba = float(best_bid), float(best_ask)
        if not (0.0 < bb < 1.0 and 0.0 < ba < 1.0) or ba <= bb:
            passed = False
            skip_reasons.append(f"invalid book bid={bb:.3f} ask={ba:.3f}")
        else:
            mid = 0.5 * (bb + ba)
            if not (lo <= mid <= hi):
                passed = False
                skip_reasons.append(f"mid {mid:.3f} outside band [{lo:.2f},{hi:.2f}]")
            if not (float(min_seconds_left) <= remaining_s <= float(max_seconds_left)):
                passed = False
                skip_reasons.append(
                    f"remaining_s {remaining_s:.0f} outside [{min_seconds_left:.0f},{max_seconds_left:.0f}]"
                )
            target_bid = round(mid - half, 2)
            target_bid = max(lo, min(hi, target_bid))
            target_bid = min(target_bid, round(ba - tick, 2))
            target_bid = round(target_bid, 2)
            if target_bid < lo or target_bid > hi:
                passed = False
                skip_reasons.append(f"target_bid {target_bid:.2f} outside band [{lo:.2f},{hi:.2f}]")
            if target_bid >= ba - (tick - 1e-9):
                passed = False
                skip_reasons.append(f"target_bid {target_bid:.2f} not >=1 tick below ask {ba:.2f}")
            maker_rebate = float(
                self.calculate_maker_rebate(target_bid, fee_rate=fee_rate, rebate_share=rebate_share)
            )
            edge_net = (mid - target_bid) + maker_rebate

    return MakerSignal(
        side=side_u,
        z_score=0.0,
        signed_z=0.0,
        model_prob=mid if mid > 0 else 0.5,
        target_bid=target_bid,
        best_bid=best_bid,
        best_ask=best_ask,
        maker_rebate=maker_rebate,
        edge_net=edge_net,
        s_req=float(strike) if strike else 0.0,
        s_0=float(strike) if strike else 0.0,
        spot=float(spot),
        realized_twap=float(realized_twap) if realized_twap else float(spot),
        elapsed_s=float(elapsed_s),
        remaining_s=float(remaining_s),
        sigma_twap_usd=0.0,
        passed_gates=passed,
        skip_reason="; ".join(skip_reasons),
    )


def patch_twap_engine(TWAPEngine):
    if getattr(TWAPEngine, "_midband_patched", False):
        return TWAPEngine
    TWAPEngine.evaluate_midband = _evaluate_midband
    TWAPEngine._midband_patched = True
    return TWAPEngine
