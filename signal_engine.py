"""Signal engine: decide UP or DOWN for the next 5-minute BTC candle.

Primary model: fair-value diffusion CDF using the slot strike (window delta),
time left, and volatility (config + realized). Side follows window delta when
the move is significant; confidence scales with |Z| / sample quality.

Returns a model probability that the market resolves UP, the chosen side, and a
confidence scalar in (0, 1] used to haircut edge for risk sizing.
"""
from __future__ import annotations
import math
from dataclasses import dataclass


@dataclass
class Signal:
    side: str             # "UP" or "DOWN"
    model_prob_up: float  # model's estimated P(up)
    momentum: float
    confidence: float = 1.0  # 1.0 = full trust; lower when samples are noisy
    realized_vol_daily: float | None = None
    # Fair-value extras (Tier 1/2 research)
    fair_prob_up: float | None = None
    z_score: float | None = None
    sigma_usd: float | None = None
    min_move_usd: float | None = None
    window_delta_usd: float | None = None


@dataclass
class FairValue:
    """P(resolve UP) under pure diffusion from current spot vs strike."""
    fair_prob_up: float
    side: str
    z_score: float
    sigma_usd: float
    min_move_usd: float
    window_delta_usd: float
    used_vol_daily: float
    significant: bool


def _sigmoid(x: float) -> float:
    x = max(-60.0, min(60.0, x))
    return 1.0 / (1.0 + math.exp(-x))


def _realized_vol_daily(prices: list[float], sample_seconds: float) -> float | None:
    """Annualize-ish daily vol from successive log returns in the warmup window.

    sample_seconds is the spacing between consecutive samples. Returns None if
    there aren't enough clean returns.
    """
    if len(prices) < 4 or sample_seconds <= 0:
        return None
    rets = []
    for a, b in zip(prices, prices[1:]):
        if a > 0 and b > 0:
            rets.append(math.log(b / a))
    if len(rets) < 3:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / max(1, len(rets) - 1)
    # per-sample std -> per-second -> daily (86400s)
    std_sample = math.sqrt(max(var, 0.0))
    std_sec = std_sample / math.sqrt(sample_seconds)
    daily = std_sec * math.sqrt(86400.0)
    # BTC daily vol is rarely outside ~1%–15% for this use; clamp extremes so a
    # single noisy warmup can't make the model go mute or overconfident.
    return max(0.01, min(0.15, daily))


def _phi(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def count_strike_crossings(prices: list[float], strike: float) -> int:
    """How many times price crossed the strike in the sample window (chop)."""
    clean = [p for p in prices if isinstance(p, (int, float)) and p > 0]
    if len(clean) < 2 or strike is None or strike <= 0:
        return 0
    sides = [1 if p >= strike else -1 for p in clean]
    return sum(1 for a, b in zip(sides, sides[1:]) if a != b)


def fair_value(
    spot: float,
    strike: float,
    remaining_seconds: float,
    volatility_daily: float = 0.045,
    realized_vol_daily: float | None = None,
    vol_blend: float = 0.5,
    z_min_significant: float = 1.0,
) -> FairValue:
    """Window-delta fair value: P(end >= strike) ≈ Φ((spot-strike)/σ√t).

    Pure diffusion (no momentum drift) — this is the professional mid baseline.
    significant=True when |Z| >= z_min_significant (move large vs noise).
    """
    used_vol = volatility_daily
    if realized_vol_daily is not None and 0.0 <= vol_blend <= 1.0:
        used_vol = (1.0 - vol_blend) * volatility_daily + vol_blend * realized_vol_daily
    used_vol = max(0.01, min(0.15, used_vol))
    rem = max(float(remaining_seconds), 1.0)
    vol_sec = used_vol / math.sqrt(86400.0)
    sigma = max(spot * vol_sec * math.sqrt(rem), 1e-4)
    delta = float(spot) - float(strike)
    z = delta / sigma
    p_up = _phi(z)
    p_up = max(0.02, min(0.98, p_up))
    side = "UP" if delta >= 0 else "DOWN"
    min_move = abs(z_min_significant) * sigma
    return FairValue(
        fair_prob_up=p_up,
        side=side,
        z_score=z,
        sigma_usd=sigma,
        min_move_usd=min_move,
        window_delta_usd=delta,
        used_vol_daily=used_vol,
        significant=abs(z) >= z_min_significant,
    )


def compute_signal(
    prices: list[float],
    sensitivity: float = 900.0,
    slot_start_price: float | None = None,
    remaining_seconds: float | None = None,
    volatility_daily: float = 0.045,
    max_confidence: float = 0.98,
    sample_seconds: float = 5.0,
    vol_blend: float = 0.5,
    prefer_window_delta_side: bool = True,
    z_min_significant: float = 1.0,
) -> Signal:
    """prices: recent BTC samples, oldest -> newest.

    If slot_start_price and remaining_seconds are provided, applies fair-value
    diffusion CDF (window delta is king). Side follows spot vs strike when the
    move is significant; model_prob is fair P(up) with light confidence haircut.

    vol_blend: weight on realized vol vs configured vol
      (0 = pure config, 1 = pure realized when available).
    """
    clean = [p for p in prices if isinstance(p, (int, float)) and p > 0]
    if len(clean) < 3:
        return Signal(side="UP", model_prob_up=0.5, momentum=0.0, confidence=0.0)

    log_ret = math.log(clean[-1] / clean[0])
    momentum = log_ret / (len(clean) ** 0.5)

    # Sample quality: more samples + smaller return chaos -> higher confidence.
    rv = _realized_vol_daily(clean, sample_seconds)
    n_factor = min(1.0, (len(clean) - 2) / 10.0)
    if rv is not None:
        # Very high realized vol in a 1-min window => less trustworthy directional edge.
        vol_penalty = max(0.4, 1.0 - max(0.0, rv - volatility_daily) / 0.10)
    else:
        vol_penalty = 0.7
    confidence = max(0.15, min(1.0, n_factor * vol_penalty))

    if slot_start_price is None or remaining_seconds is None or remaining_seconds <= 0:
        model_prob_up = _sigmoid(momentum * sensitivity)
        side = "UP" if model_prob_up >= 0.5 else "DOWN"
        return Signal(
            side=side,
            model_prob_up=model_prob_up,
            momentum=momentum,
            confidence=confidence * 0.85,  # momentum-only is weaker
            realized_vol_daily=rv,
        )

    current_price = clean[-1]
    fv = fair_value(
        spot=current_price,
        strike=float(slot_start_price),
        remaining_seconds=float(remaining_seconds),
        volatility_daily=volatility_daily,
        realized_vol_daily=rv,
        vol_blend=vol_blend,
        z_min_significant=z_min_significant,
    )

    # Small optional drift from recent path (capped) so signal is not pure noise.
    used_vol = fv.used_vol_daily
    vol_sec = used_vol / math.sqrt(86400.0)
    std_dev = max(current_price * vol_sec * math.sqrt(max(remaining_seconds, 1.0)), 1e-4)
    window_secs = max(sample_seconds * (len(clean) - 1), sample_seconds)
    drift_rate_per_sec = max(-0.0001, min(0.0001, log_ret / window_secs))
    drift_amount = drift_rate_per_sec * remaining_seconds * current_price
    drift_cap = 0.35 * std_dev  # lighter than before — window delta dominates
    drift_amount = max(-drift_cap, min(drift_cap, drift_amount))

    diff = current_price - slot_start_price
    Z = (diff + drift_amount) / std_dev
    model_prob_up = _phi(Z)
    lo = 1.0 - max_confidence
    model_prob_up = max(lo, min(max_confidence, model_prob_up))

    # Blend toward pure fair value (no drift) — professional baseline.
    model_prob_up = 0.65 * fv.fair_prob_up + 0.35 * model_prob_up

    # Pull extreme probs slightly toward 0.5 when confidence is low.
    if confidence < 1.0:
        model_prob_up = 0.5 + (model_prob_up - 0.5) * (0.55 + 0.45 * confidence)

    # Side: window delta when significant; else model.
    if prefer_window_delta_side and fv.significant:
        side = fv.side
    else:
        side = "UP" if model_prob_up >= 0.5 else "DOWN"

    # Boost confidence when |Z| is large (clear vs noise).
    z_boost = min(1.0, abs(fv.z_score) / 2.0)
    confidence = max(0.15, min(1.0, 0.55 * confidence + 0.45 * z_boost))

    return Signal(
        side=side,
        model_prob_up=model_prob_up,
        momentum=momentum,
        confidence=confidence,
        realized_vol_daily=rv,
        fair_prob_up=fv.fair_prob_up,
        z_score=fv.z_score,
        sigma_usd=fv.sigma_usd,
        min_move_usd=fv.min_move_usd,
        window_delta_usd=fv.window_delta_usd,
    )
