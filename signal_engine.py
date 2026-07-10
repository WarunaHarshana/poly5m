"""Signal engine: decide UP or DOWN for the next 5-minute BTC candle.

Primary model: drift/diffusion random-walk CDF using the slot strike, time left,
and a volatility estimate that blends configured daily vol with realized
short-window vol. Falls back to short-term momentum when strike/time are missing.

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


def compute_signal(
    prices: list[float],
    sensitivity: float = 900.0,
    slot_start_price: float | None = None,
    remaining_seconds: float | None = None,
    volatility_daily: float = 0.045,
    max_confidence: float = 0.98,
    sample_seconds: float = 5.0,
    vol_blend: float = 0.5,
) -> Signal:
    """prices: recent BTC samples, oldest -> newest.

    If slot_start_price and remaining_seconds are provided, applies a
    drift/diffusion CDF. Otherwise falls back to pure short-term momentum.

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

    # Blend configured and realized daily vol for the diffusion scale.
    used_vol = volatility_daily
    if rv is not None and 0.0 <= vol_blend <= 1.0:
        used_vol = (1.0 - vol_blend) * volatility_daily + vol_blend * rv
    used_vol = max(0.01, min(0.15, used_vol))

    current_price = clean[-1]
    vol_sec = used_vol / math.sqrt(86400.0)
    std_dev = current_price * vol_sec * math.sqrt(remaining_seconds)

    # Warmup window is ~ (n-1)*sample_seconds; normalize drift to per-second.
    window_secs = max(sample_seconds * (len(clean) - 1), sample_seconds)
    drift_rate_per_sec = max(-0.0001, min(0.0001, log_ret / window_secs))
    drift_amount = drift_rate_per_sec * remaining_seconds * current_price
    # Cap momentum extrapolation so a ~60s trend can't pin confidence at ~1.00.
    drift_cap = 0.5 * std_dev
    drift_amount = max(-drift_cap, min(drift_cap, drift_amount))

    diff = current_price - slot_start_price
    denom = max(std_dev, 1e-4)
    Z = (diff + drift_amount) / denom

    def phi(z: float) -> float:
        return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))

    model_prob_up = phi(Z)
    lo = 1.0 - max_confidence
    model_prob_up = max(lo, min(max_confidence, model_prob_up))

    # Pull extreme probs slightly toward 0.5 when confidence is low (uncertainty
    # haircut on the *probability*, not only on stake size).
    if confidence < 1.0:
        model_prob_up = 0.5 + (model_prob_up - 0.5) * (0.55 + 0.45 * confidence)

    side = "UP" if model_prob_up >= 0.5 else "DOWN"
    return Signal(
        side=side,
        model_prob_up=model_prob_up,
        momentum=momentum,
        confidence=confidence,
        realized_vol_daily=rv,
    )
