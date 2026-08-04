"""Order-book recorder for market-making research (NO trading, NO risk).

We cannot backtest two-sided market-making without the full ask path for BOTH
tokens across each slot — and the bot only ever sampled the book at decision
moments (max ~4 snapshots/slot). This standalone recorder fixes that: it polls
UP and DOWN best bid/ask every SAMPLE_S seconds for the whole slot, records the
official outcome, and writes one JSON object per slot to
state/archive/book_recordings.jsonl for mm_backtest.py to replay.

It places no orders and needs no wallet. Run it alongside the paper bots:

    python record_books.py                 # BTC 5m
    $env:TIMEFRAME='15m'; python record_books.py

Stop with Ctrl+C; each completed slot is already flushed to disk.
"""
from __future__ import annotations
import json
import os
import time

import config
from polymarket_client import PolymarketPublic

def _state_dir() -> str:
    """Per-timeframe state dir, matching bot.py / app.py / analyze.py."""
    import paths
    return paths.state_dir()


OUT = os.path.join(_state_dir(), "archive", "book_recordings.jsonl")
SAMPLE_S = float(os.getenv("BOOK_SAMPLE_S", "3.0"))


def snapshot(pub: PolymarketPublic, market: dict) -> dict | None:
    """One reading of both sides. Returns None if either book is unusable."""
    try:
        up = pub.buy_quote(market["tokens"]["UP"])
        dn = pub.buy_quote(market["tokens"]["DOWN"])
    except Exception:
        return None
    return {
        "t": round(time.time(), 2),
        "up_bid": up.get("best_bid"), "up_ask": up.get("best_ask"),
        "up_ask_sz": up.get("ask_size"), "up_bid_sz": up.get("bid_size"),
        "dn_bid": dn.get("best_bid"), "dn_ask": dn.get("best_ask"),
        "dn_ask_sz": dn.get("ask_size"), "dn_bid_sz": dn.get("bid_size"),
    }


def _write_slot(slug: str, end_ts, snaps: list, resolved_up) -> None:
    rec = {
        "slug": slug,
        "end_ts": end_ts,
        "resolved_up": resolved_up,
        "snapshots": snaps,
        "recorded_at": time.time(),
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    tmp = OUT + ".tmp"
    # append-safe: read nothing, just append one line (atomic per line on POSIX;
    # on Windows a single write() of one line is effectively atomic for our size)
    with open(OUT, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, default=str) + "\n")
    print(f"  [rec] wrote {slug}: {len(snaps)} snapshots, "
          f"resolved={'UP' if resolved_up else ('DOWN' if resolved_up is False else '?')}")


def backfill_outcomes(pub: PolymarketPublic, path: str = OUT,
                      verbose: bool = True) -> int:
    """Fill resolved_up for slots recorded before Gamma had settled them.

    The inline poll after each slot is short (so the recorder can move on to
    the NEXT slot's opening book instead of blocking). Outcomes stay on Gamma
    indefinitely, so we backfill the None ones on a later pass. Rewrites the
    file atomically. Returns how many were filled.
    """
    if not os.path.isfile(path):
        return 0
    try:
        rows = [json.loads(ln) for ln in open(path, encoding="utf-8")
                if ln.strip()]
    except Exception:
        return 0
    filled = 0
    for r in rows:
        if r.get("resolved_up") is None and r.get("slug"):
            try:
                o = pub.get_official_btc_updown_outcome(r["slug"])
            except Exception:
                o = None
            if o in ("UP", "DOWN"):
                r["resolved_up"] = (o == "UP")
                filled += 1
    if filled:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, default=str) + "\n")
        os.replace(tmp, path)
        if verbose:
            print(f"  [rec] backfilled {filled} outcome(s)")
    return filled


def main() -> None:
    pub = PolymarketPublic()
    # Recover outcomes for anything recorded earlier but never resolved.
    try:
        n = backfill_outcomes(pub)
        if n:
            print(f"[book recorder] backfilled {n} pending outcome(s) on startup")
    except Exception as e:
        print(f"[book recorder] startup backfill skipped: {e}")
    print(f"[book recorder] {config.ASSET} {config.TIMEFRAME} -> {OUT}")
    print(f"  sampling every {SAMPLE_S:.0f}s; no orders, no wallet. Ctrl+C to stop.")
    seen_slugs: set = set()
    while True:
        try:
            market = pub.find_btc_5min_market()
        except Exception as e:
            print(f"  [rec] no market ({e}); retry 10s")
            time.sleep(10)
            continue
        slug = market.get("slug") or ""
        end_ts = market.get("end_ts")
        if not slug or slug in seen_slugs or not end_ts:
            time.sleep(min(SAMPLE_S, 5.0))
            continue
        seen_slugs.add(slug)
        print(f"\n[rec] slot {slug} ({round(end_ts - time.time())}s left)")
        snaps: list = []
        while time.time() < end_ts:
            s = snapshot(pub, market)
            if s is not None:
                s["rem"] = round(end_ts - s["t"], 1)
                snaps.append(s)
            time.sleep(SAMPLE_S)
        # slot ended — short outcome attempt only (~15s). Gamma often needs
        # longer, so we DON'T block here waiting (that would miss the next
        # slot's opening book). Unresolved ones get backfilled below.
        resolved_up = None
        for _ in range(5):
            try:
                o = pub.get_official_btc_updown_outcome(slug)
            except Exception:
                o = None
            if o in ("UP", "DOWN"):
                resolved_up = (o == "UP")
                break
            time.sleep(3)
        _write_slot(slug, end_ts, snaps, resolved_up)
        # Backfill outcomes for earlier slots Gamma has since settled.
        try:
            backfill_outcomes(pub, verbose=True)
        except Exception:
            pass
        # prune memory so a long run doesn't grow unbounded
        if len(seen_slugs) > 5000:
            seen_slugs = set(list(seen_slugs)[-1000:])


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[book recorder] stopped.")
