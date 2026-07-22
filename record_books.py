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
    here = os.path.dirname(__file__)
    tf = (os.getenv("TIMEFRAME") or "5m").strip().lower()
    sub = (os.getenv("STATE_SUBDIR") or ("" if tf == "5m" else tf)).strip()
    return os.path.join(here, "state", sub) if sub \
        else os.path.join(here, "state")


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


def main() -> None:
    pub = PolymarketPublic()
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
        # slot ended — fetch official outcome (best effort, ~90s)
        resolved_up = None
        for _ in range(30):
            try:
                o = pub.get_official_btc_updown_outcome(slug)
            except Exception:
                o = None
            if o in ("UP", "DOWN"):
                resolved_up = (o == "UP")
                break
            time.sleep(3)
        _write_slot(slug, end_ts, snaps, resolved_up)
        # prune memory so a long run doesn't grow unbounded
        if len(seen_slugs) > 5000:
            seen_slugs = set(list(seen_slugs)[-1000:])


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[book recorder] stopped.")
