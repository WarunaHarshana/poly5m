"""Log-only shadow helpers (inv12). Pure functions + tiny state; never affect trading."""
from __future__ import annotations

import math

TWAPDIST_BLOCK_BPS = 2.0
ALT_BID_RANGE5 = 0.05
ALT_BID_TWAPDIST_LO = 1.5
POST_FILL_OFFSETS = (5, 15, 30, 60)


def _f(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def twapdist_bps(spot, strike_twap, side) -> float | None:
    """(spot - strike_twap) * side_sign / spot * 1e4; +1 UP, -1 DOWN. None if inputs missing."""
    s, k = _f(spot), _f(strike_twap)
    if s is None or k is None or s <= 0 or k <= 0:
        return None
    sgn = 1.0 if str(side).upper() == "UP" else -1.0
    return round((s - k) * sgn / s * 1e4, 3)


def twapdist_fields(spot, strike_twap, side) -> dict:
    d = twapdist_bps(spot, strike_twap, side)
    return {"twapdist_bps": d, "would_block_twapdist": bool(d is not None and d >= TWAPDIST_BLOCK_BPS)}


def alt_bid_047(range_5s, twapdist) -> bool:
    r5, d = _f(range_5s), _f(twapdist)
    return bool((r5 is not None and r5 > ALT_BID_RANGE5) or
                (d is not None and ALT_BID_TWAPDIST_LO <= d < TWAPDIST_BLOCK_BPS))


class Throttle:
    """At most one emit per key per interval_s."""

    def __init__(self, interval_s: float = 1.0) -> None:
        self.interval_s = float(interval_s)
        self._last: dict = {}

    def ready(self, key, now: float) -> bool:
        last = self._last.get(key)
        if last is not None and now - last < self.interval_s:
            return False
        self._last[key] = now
        if len(self._last) > 500:  # bound memory
            for k in list(self._last)[:250]:
                self._last.pop(k, None)
        return True


class PostFillTracer:
    """Tracks fills and reports which +N s checkpoints are due (first tick at/after each offset)."""

    def __init__(self, offsets=POST_FILL_OFFSETS) -> None:
        self.offsets = tuple(offsets)
        self._f: dict = {}

    def start(self, order_id, fill_ts: float, fill_mid, fill_bid, price) -> None:
        if not order_id:
            return
        self._f[order_id] = {"ts": float(fill_ts), "mid": _f(fill_mid), "bid": _f(fill_bid),
                             "price": _f(price), "done": set()}
        if len(self._f) > 50:
            for k in list(self._f)[:25]:
                self._f.pop(k, None)

    def due(self, order_id, now: float):
        """Return list of offsets that are due now (marks them done)."""
        st = self._f.get(order_id)
        if not st:
            return []
        out = [o for o in self.offsets if o not in st["done"] and now - st["ts"] >= o]
        st["done"].update(out)
        return out

    def record(self, order_id, offset, now, mid, bid) -> dict:
        st = self._f.get(order_id) or {}
        m, b = _f(mid), _f(bid)
        fm, fb = st.get("mid"), st.get("price")
        if st and len(st.get("done", ())) == len(self.offsets) and offset == max(self.offsets):
            self._f.pop(order_id, None)
        return {"order_id": order_id, "offset_s": offset, "actual_s": round(now - st.get("ts", now), 2) if st else None,
                "mid": m, "best_bid": b, "fill_mid": fm, "fill_price": fb,
                "mid_move": round(m - fm, 4) if (m is not None and fm is not None) else None,
                "bid_vs_fill": round(b - fb, 4) if (b is not None and fb is not None) else None}

    def snapshot(self, order_id):
        return self._f.get(order_id)


def midslot_condition(elapsed_s, twapdist, lo=40.0, hi=60.0, bps=2.0) -> bool:
    """True when lo <= elapsed_s < hi and twapdist >= bps. Fail-open (False) on missing inputs."""
    e, d = _f(elapsed_s), _f(twapdist)
    return bool(e is not None and d is not None and float(lo) <= e < float(hi) and d >= float(bps))
