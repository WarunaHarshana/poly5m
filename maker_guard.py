"""Maker-leg entry guards (JEV_DYNAMIC maker).

Two protections against the maker leg's structural adverse selection:

1. Settlement-strike check. Polymarket 5m markets settle on the 60s-TWAP price-to-beat, not the
   raw spot tick at slot open. The maker z is re-computed against the TWAP strike and the less
   favourable of the two z-scores is used; if the two disagree on direction the quote is blocked,
   and the TWAP-strike z must be at least MAKER_MIN_TWAP_SIGMA on our side.
2. Fast-drop guard. A resting bid near 0.50 is typically filled by a fast move against us. Keep a
   short mid history per token and block quoting / filling when the mid has dropped at least
   MAKER_MAX_MID_DROP_60S from its recent high.

Pure helpers only; no trading side effects.
"""
from __future__ import annotations

import math
from collections import deque
from typing import Any

REASON_TWAP_SIGMA = "twap_sigma"
REASON_DIRECTION_CONFLICT = "direction_conflict"
REASON_MID_DROP = "mid_drop"
REASON_POST_FALLING_MID = "post_falling_mid"
REASON_TWAP_CROSS = "twap_cross"
REASON_NO_TWAP_STRIKE = "no_twap_strike"

REF_SPOT_FALLBACK = "spot_fallback"  # no TWAP strike yet -> legacy spot-strike z
REF_SPOT = "spot"
REF_TWAP = "twap"


def _finite(x: Any) -> float | None:
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _sign(side: str) -> float:
    return 1.0 if str(side).upper() == "UP" else -1.0


def conservative_z(side: str, z_spot: float, z_twap: float | None) -> tuple[float, str]:
    """Directional z (positive = in favour of `side`) using the less favourable reference.

    Returns (z_on_side, ref). When the TWAP z is missing, falls back to the spot z.
    When the two z-scores disagree on direction the result is the non-positive one.
    """
    s = _sign(side)
    a = s * float(z_spot)
    b = _finite(z_twap)
    if b is None:
        return a, REF_SPOT_FALLBACK
    b = s * b
    if b < a:
        return b, REF_TWAP
    return a, REF_SPOT


def check_quote(
    side: str,
    z_spot: float,
    z_twap: float | None,
    min_twap_sigma: float,
) -> tuple[bool, str | None, float, str]:
    """Settlement-strike check for posting (or filling) a maker bid on `side`.

    Returns (ok, block_reason, z_used_on_side, ref).
    - No TWAP z (strike unknown): ok, legacy behaviour (ref=spot_fallback).
    - Spot z and TWAP z point in different directions: blocked (direction_conflict).
    - TWAP z on our side < min_twap_sigma: blocked (twap_sigma).
    """
    z_used, ref = conservative_z(side, z_spot, z_twap)
    zt = _finite(z_twap)
    if zt is None:
        return True, None, z_used, ref
    zs = float(z_spot)
    if zs == 0.0 or zt == 0.0 or (zs > 0) != (zt > 0):
        return False, REASON_DIRECTION_CONFLICT, z_used, ref
    if _sign(side) * zt < float(min_twap_sigma):
        return False, REASON_TWAP_SIGMA, z_used, ref
    return True, None, z_used, ref


def twap_cross_adverse(side: str, spot: float, strike_twap: float | None) -> bool:
    """True when spot sits on the losing side of the TWAP strike for `side`."""
    k = _finite(strike_twap)
    p = _finite(spot)
    if k is None or p is None or k <= 0:
        return False
    return p < k if str(side).upper() == "UP" else p > k


class MidHistory:
    """Rolling (ts, mid) history per token for the fast-drop guard."""

    def __init__(self, window_s: float = 60.0) -> None:
        self.window_s = float(window_s)
        self._h: dict[str, deque] = {}

    def add(self, token: str | None, ts: float, mid: float | None) -> None:
        m = _finite(mid)
        if not token or m is None:
            return
        dq = self._h.setdefault(token, deque())
        dq.append((float(ts), m))
        cutoff = float(ts) - self.window_s
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    def high(self, token: str | None, now: float) -> float | None:
        dq = self._h.get(token or "")
        if not dq:
            return None
        cutoff = float(now) - self.window_s
        vals = [m for ts, m in dq if ts >= cutoff]
        return max(vals) if vals else None

    def latest(self, token: str | None) -> float | None:
        dq = self._h.get(token or "")
        return dq[-1][1] if dq else None

    def range_over(self, token: str | None, now: float, lookback_s: float, mid: float | None = None) -> float | None:
        """max-min of mids within the last lookback_s (plus current mid). None if no history in that span."""
        dq = self._h.get(token or "")
        cutoff = float(now) - float(lookback_s)
        vals = [m for ts, m in (dq or ()) if ts >= cutoff]
        m = _finite(mid)
        if m is not None:
            vals.append(m)
        return (max(vals) - min(vals)) if len(vals) >= 2 else None

    def drop(self, token: str | None, now: float, mid: float | None) -> tuple[float | None, float | None]:
        """Return (window_high, drop_from_high); (None, None) when there is no history."""
        hi = self.high(token, now)
        m = _finite(mid)
        if hi is None or m is None:
            return hi, None
        return hi, max(0.0, hi - m)

    def clear(self) -> None:
        self._h.clear()


def mid_drop_blocked(drop: float | None, max_drop: float) -> bool:
    d = _finite(drop)
    if d is None or float(max_drop) <= 0:
        return False
    return d >= float(max_drop) - 1e-9


def post_falling_mid_blocked(
    drop: float | None,
    recent_range: float | None,
    max_drop: float,
    stable_max_range: float,
) -> bool:
    """Pre-post gate (option 1): don't post a maker bid into a falling/unstable mid.
    max_drop <= 0 disables the drop check; stable_max_range <= 0 disables the stability check.
    Missing data does not block (fail-open, matching mid_drop_blocked)."""
    if float(max_drop) > 0:
        d = _finite(drop)
        if d is not None and d >= float(max_drop) - 1e-9:
            return True
    if float(stable_max_range) > 0:
        r = _finite(recent_range)
        if r is not None and r > float(stable_max_range) + 1e-9:
            return True
    return False


SHADOW_DROP_THRESHOLDS = (0.03, 0.05, 0.08)
SHADOW_STABLE_MAX_RANGE = 0.02


def post_shadow_record(hist: "MidHistory", token: str | None, now: float, mid: float | None = None) -> dict:
    """Shadow evaluation of the pre-post gate. Pure; never blocks anything."""
    m = _finite(mid)
    if m is None:
        m = hist.latest(token)
    hi, drop = hist.drop(token, now, m)
    r3 = hist.range_over(token, now, 3.0, m)
    r5 = hist.range_over(token, now, 5.0, m)
    rd = lambda v: round(v, 4) if v is not None else None
    wb = {
        "stable3s_gt_0.02": bool(r3 is not None and r3 > SHADOW_STABLE_MAX_RANGE + 1e-9),
        "stable5s_gt_0.02": bool(r5 is not None and r5 > SHADOW_STABLE_MAX_RANGE + 1e-9),
    }
    for th in SHADOW_DROP_THRESHOLDS:
        wb[f"drop_ge_{th:.2f}"] = bool(drop is not None and drop >= th - 1e-9)
    return {"mid": rd(m), "mid_60s_high": rd(hi), "drop": rd(drop), "range_3s": rd(r3), "range_5s": rd(r5),
            "would_block": wb}
