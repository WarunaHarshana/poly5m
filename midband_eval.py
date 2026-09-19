"""H-006 mid-band maker evaluation helper."""
from __future__ import annotations


def evaluate_midband_core(
    *,
    side: str,
    remaining_s: float,
    best_bid: float | None,
    best_ask: float | None,
    midband_lo: float = 0.40,
    midband_hi: float = 0.60,
    half_spread: float = 0.02,
    min_seconds_left: float = 75.0,
    max_seconds_left: float = 270.0,
    fee_rate: float = 0.07,
    rebate_share: float = 0.20,
    calculate_maker_rebate,
    tick: float = 0.01,
) -> dict:
    """Return kwargs for MakerSignal construction (no Z-score gate)."""
    side_u = str(side).upper()
    if side_u not in ("UP", "DOWN"):
        side_u = "UP"

    lo = float(midband_lo)
    hi = float(midband_hi)
    half = float(half_spread)
    tick = max(0.01, float(tick))
    remaining_s = float(remaining_s)

    passed = True
    skip_reasons: list[str] = []
    mid = 0.0
    target_bid = 0.0
    maker_rebate = 0.0
    edge_net = 0.0

    if best_bid is None or best_ask is None:
        passed = False
        skip_reasons.append("require bid+ask")
    else:
        bb = float(best_bid)
        ba = float(best_ask)
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
            max_bid_vs_ask = round(ba - tick, 2)
            target_bid = min(target_bid, max_bid_vs_ask)
            target_bid = round(target_bid, 2)

            if target_bid < lo or target_bid > hi:
                passed = False
                skip_reasons.append(f"target_bid {target_bid:.2f} outside band [{lo:.2f},{hi:.2f}]")
            if target_bid >= ba - (tick - 1e-9):
                passed = False
                skip_reasons.append(f"target_bid {target_bid:.2f} not >=1 tick below ask {ba:.2f}")

            maker_rebate = float(
                calculate_maker_rebate(target_bid, fee_rate=fee_rate, rebate_share=rebate_share)
            )
            edge_net = (mid - target_bid) + maker_rebate

    return {
        "side": side_u,
        "z_score": 0.0,
        "signed_z": 0.0,
        "model_prob": mid if mid > 0 else 0.5,
        "target_bid": target_bid,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "maker_rebate": maker_rebate,
        "edge_net": edge_net,
        "passed_gates": passed,
        "skip_reason": "; ".join(skip_reasons),
        "mid": mid,
    }
