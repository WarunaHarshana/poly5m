"""LOG-ONLY late fair-value taker shadow (BTC 5m). See /workspace/hf_search_REPORT.md.

From LATE_FV_T_LO..LATE_FV_T_HI seconds into the slot, every LATE_FV_STEP seconds, compute the settle-TWAP z
(same model as hf_search.zat) from the bot's own Chainlink feed (strike AND spot), price both sides off the REST
book and record ONE virtual FOK BUY per slot when edge >= LATE_FV_MIN_EDGE. Never places, cancels or signs
orders: this module has no order client and there is no live flag (LATE_FV_LIVE does not exist by design).
"""
from __future__ import annotations

import math
import time

FEE = 0.07


def phi(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def taker_fee(p: float) -> float:
    return FEE * p * (1.0 - p)


def grid_1s(ticks, t0: float, t1: float) -> list[float]:
    """Forward-filled 1s closes at integer seconds t0+1..t1 (needs a tick at/before t0+1)."""
    out = []
    i, last = 0, None
    ticks = sorted(ticks)
    n = len(ticks)
    t = int(t0) + 1
    while t <= int(t1):
        while i < n and ticks[i][0] <= t:
            last = ticks[i][1]
            i += 1
        if last is not None:
            out.append(float(last))
        t += 1
    return out


def vol_1s(ticks, slot_start: float, lookback_s: float = 1800.0, min_n: int = 600) -> tuple[float | None, int]:
    """Pstdev of 1s log returns over [slot_start-lookback, slot_start] (hf_search: max(sd,2e-5)); None if < min_n."""
    g = grid_1s(ticks, slot_start - lookback_s - 1, slot_start)
    r = [math.log(g[i] / g[i - 1]) for i in range(1, len(g)) if g[i] > 0 and g[i - 1] > 0]
    if len(r) < min_n:
        return None, len(r)
    m = sum(r) / len(r)
    sd = math.sqrt(sum((x - m) ** 2 for x in r) / len(r))
    return max(sd, 2e-5), len(r)


def settle_z(spot: float, strike: float, t: float, vol: float, obs_since_240: list[float] | None = None) -> float | None:
    """hf_search.zat: t<240 -> m=spot, var=vol^2*((240-t)+20); t>=240 -> m blends observed 1s TWAP points
    since 240 with spot for the rest, var=vol^2*(300-t)^3/(3*3600)."""
    if not spot or not strike or spot <= 0 or strike <= 0 or not vol:
        return None
    t = float(t)
    if t < 240:
        m = spot
        v = vol ** 2 * ((240.0 - t) + 20.0)
    else:
        obs = list(obs_since_240 or [])
        rem = max(0.0, 300.0 - t - 1.0)
        m = (sum(obs) + rem * spot) / (len(obs) + rem) if (len(obs) + rem) > 0 else spot
        v = vol ** 2 * max(0.0, 300.0 - t) ** 3 / (3 * 3600.0)
    return math.log(m / strike) / math.sqrt(max(v, 1e-12))


def _levels(book, key):
    out = []
    for lv in (book or {}).get(key) or []:
        try:
            p = float(lv.get("price") if isinstance(lv, dict) else lv[0])
            s = float(lv.get("size") if isinstance(lv, dict) else lv[1])
        except Exception:
            continue
        if 0.0 < p < 1.0 and s > 0:
            out.append((p, s))
    return out


def side_quote(book_tok, book_opp, shares: float) -> dict:
    """Effective ask ladder for buying a token: its own asks plus complement (1 - opp bid). Returns best ask,
    size at best, depth fillable at limit=best ask, would-FOK-fill flag, bid and spread."""
    asks = [(round(p, 4), s) for p, s in _levels(book_tok, "asks")]
    asks += [(round(1.0 - p, 4), s) for p, s in _levels(book_opp, "bids")]
    bids = [round(p, 4) for p, _ in _levels(book_tok, "bids")] + [round(1.0 - p, 4) for p, _ in _levels(book_opp, "asks")]
    if not asks:
        return {"ask": None}
    best = min(p for p, _ in asks)
    at_best = sum(s for p, s in asks if abs(p - best) < 1e-9)
    bid = max(bids) if bids else None
    ladder = sorted(asks)
    return {"ask": best, "ask_size": round(at_best, 4), "depth_at_ask": round(at_best, 4), "ladder": ladder,
            "fok_fill": at_best + 1e-9 >= shares, "bid": bid,
            "spread": round(best - bid, 4) if bid is not None else None}


def book_ts(book) -> float | None:
    try:
        v = float((book or {}).get("timestamp"))
        return v / 1000.0 if v > 1e11 else v
    except Exception:
        return None


def evaluate(z: float, q_up: dict, q_dn: dict, k: float, min_edge: float, max_ask: float):
    """Return (pick, best) where pick is the qualifying side dict (larger edge) or None, best the max-edge side."""
    cands = []
    for side, q, zs in (("UP", q_up, z), ("DOWN", q_dn, -z)):
        a = q.get("ask")
        if a is None:
            continue
        p = phi(k * zs)
        e = p - a - taker_fee(a)
        cands.append({"side": side, "prob": round(p, 4), "edge": round(e, 4), **q})
    if not cands:
        return None, None
    best = max(cands, key=lambda c: c["edge"])
    ok = [c for c in cands if c["edge"] >= min_edge - 1e-12 and c["ask"] <= max_ask + 1e-12]
    pick = max(ok, key=lambda c: c["edge"]) if ok else None
    return pick, best


MIN_MARKETABLE_USD = 1.00  # Polymarket rejects marketable BUY notional < $1 ("min size: 1")


def live_shares(ask: float, base: float, max_usd: float, min_usd: float = MIN_MARKETABLE_USD,
                buffer: float = 0.01) -> float:
    """Shares for a FOK BUY at `ask`: `base` unless base*ask < min_usd, then the smallest whole-share count with
    notional >= min_usd+buffer (place_order sends price at 2dp and size at 2dp; whole shares keep price*size exact).
    Never exceeds max_usd (falls back to base, which the caller's cost cap then judges)."""
    a = round(float(ask), 2)
    if a <= 0 or base * a >= min_usd - 1e-9:
        return float(base)
    n = float(math.ceil((min_usd + buffer) / a - 1e-9))
    if n * a > max_usd + 1e-9:
        return float(base)
    return n


def sim_pnl(won: bool, ask: float, shares: float) -> float:
    return round(shares * ((1.0 if won else 0.0) - ask) - shares * taker_fee(ask), 4)


def depth_upto(ladder, limit: float) -> float:
    """Total ask size at prices <= limit (what a FOK BUY at `limit` can consume)."""
    return round(sum(s for p, s in (ladder or []) if p <= float(limit) + 1e-9), 4)


def limit_price(ask: float, slippage: float, max_ask: float) -> float:
    """FOK BUY limit: ask + slippage floored to the cent grid, never below ask (ceil'd to a cent),
    capped at min(max_ask, 0.99)."""
    a = float(ask)
    lim = math.floor((a + max(0.0, float(slippage))) * 100 + 1e-9) / 100.0
    lim = max(lim, math.ceil(a * 100 - 1e-9) / 100.0)
    return round(min(lim, float(max_ask), 0.99), 2)


def edge_at(prob: float, price: float) -> float:
    return round(float(prob) - float(price) - taker_fee(float(price)), 4)
