"""Log-only shadow helpers (inv12). Pure functions + tiny state; never affect trading."""
from __future__ import annotations

import math

POST_FILL_OFFSETS = (5, 15, 30, 60)
POST_FILL_STEP_S = 10  # after the last fixed offset, keep sampling every 10s (caller stops at slot end / exit)


def _f(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


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
        last = max(self.offsets)
        if now - st["ts"] >= last + POST_FILL_STEP_S:  # continuation: latest due 10s step only
            o = last + POST_FILL_STEP_S * int((now - st["ts"] - last) // POST_FILL_STEP_S)
            if o not in st["done"]:
                out.append(o)
        st["done"].update(out)
        return out

    def record(self, order_id, offset, now, mid, bid) -> dict:
        st = self._f.get(order_id) or {}
        m, b = _f(mid), _f(bid)
        fm, fb = st.get("mid"), st.get("price")
        return {"order_id": order_id, "offset_s": offset, "actual_s": round(now - st.get("ts", now), 2) if st else None,
                "mid": m, "best_bid": b, "fill_mid": fm, "fill_price": fb,
                "mid_move": round(m - fm, 4) if (m is not None and fm is not None) else None,
                "bid_vs_fill": round(b - fb, 4) if (b is not None and fb is not None) else None}

    def stop(self, order_id) -> None:
        self._f.pop(order_id, None)

    def snapshot(self, order_id):
        return self._f.get(order_id)
