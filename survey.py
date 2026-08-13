"""Read-only survey: does a REAL sum-to-one arb exist on any Polymarket series?

Watches every asset x timeframe Polymarket offers, prices both legs of each
pair from ONE WebSocket snapshot, and records how close the book actually gets
to a fillable arb. It never places an order, never imports the trading client,
and never touches the live bot's state.

WHY THIS EXISTS
---------------
The 5-minute bot spent days reporting arb signals that were not there. Every
one of 236 logged signals came from the REST fallback, which reads UP and then
DOWN in separate round trips: the price moves between the two calls, and when
it moves the right way the quotes describe a pair that never simultaneously
existed. The WebSocket, pricing both legs from a single instant, saw none of
them.

So before assuming a different series or coin would be better, measure them all
through an instrument that cannot manufacture the thing it is looking for.

WHAT IT RECORDS
---------------
One row per market per slot, holding the CLOSEST the book came:

    min_pair      lowest ask_UP + ask_DOWN seen in that slot (socket-priced)
    best_gap      1 - min_pair
    best_locked   best_gap minus both taker fees -- what you would keep
    would_trade   did any socket-priced pair clear ARB_MIN_LOCKED_EDGE
    ws / rest     how the scans were sourced, so a dead socket is visible
    phantom_rest  REST-priced pairs that LOOKED like arbs -- the false
                  positives, counted separately so the two never blur again

A market whose best_locked is always negative has no arb for a taker, however
many gaps the REST path imagines.

USAGE
    python survey.py                 # run until Ctrl-C, writing rows as it goes
    python survey.py --summary       # read the rows back and report
    python survey.py --minutes 480   # stop after 8 hours
"""
from __future__ import annotations

import json
import os
import sys
import time

# A higher tracking limit BEFORE book_feed is imported: the default of 8 holds
# only four markets, and this watches up to nine.
os.environ.setdefault("BOOK_FEED_MAX_TRACKED", "40")

import config
import paths
from book_feed import BookFeed
from polymarket_client import PolymarketPublic

# This module must never be able to trade. PolymarketClient is the only thing
# that can place an order and it is deliberately not imported.
assert "PolymarketClient" not in globals()

ASSETS = [a.strip() for a in
          (os.getenv("SURVEY_ASSETS") or "btc,eth,sol").split(",") if a.strip()]
# Verified live 2026-07-19: only these three series exist.
TIMEFRAMES = [t.strip() for t in
              (os.getenv("SURVEY_TIMEFRAMES") or "5m,15m,4h").split(",") if t.strip()]
TF_SECONDS = {"5m": 300, "15m": 900, "4h": 14400}

SCAN_INTERVAL_S = float(os.getenv("SURVEY_SCAN_INTERVAL_S", "1.0"))
REPORT_EVERY_S = float(os.getenv("SURVEY_REPORT_EVERY_S", "300"))
OUT_PATH = os.path.join(paths.archive_dir(), "market_survey.jsonl")


def _fees(ask_up: float, ask_down: float) -> float:
    """Both taker legs, on the venue's documented curve."""
    return (config.taker_fee_per_share(ask_up)
            + config.taker_fee_per_share(ask_down))


class Market:
    """One asset x timeframe series, and the best the book offered this slot."""

    def __init__(self, asset: str, tf: str):
        self.asset = asset
        self.tf = tf
        self.slot_seconds = TF_SECONDS.get(tf, 300)
        self.tokens = None
        self.slug = ""
        self.end_ts = None
        self.unavailable = False
        self.reset_slot()

    @property
    def name(self) -> str:
        return f"{self.asset}/{self.tf}"

    def reset_slot(self) -> None:
        self.scans = 0
        self.ws_scans = 0
        self.rest_scans = 0
        self.phantom_rest = 0
        self.min_pair = None
        self.best = None

    def discover(self, pub: PolymarketPublic) -> bool:
        """Point config at this series and fetch its current slot."""
        config.ASSET = self.asset
        config.TIMEFRAME = self.tf
        config.SLOT_SECONDS = self.slot_seconds
        config.reapply_asset_config()
        try:
            m = pub.find_btc_5min_market()
        except Exception as e:
            if not self.unavailable:
                print(f"  [{self.name}] no market ({str(e)[:70]})")
            self.unavailable = True
            self.tokens = None
            return False
        if not m or not m.get("tokens"):
            self.unavailable = True
            self.tokens = None
            return False
        self.unavailable = False
        self.tokens = m["tokens"]
        self.slug = m.get("slug") or ""
        self.end_ts = m.get("end_ts")
        return True

    def observe(self, feed: BookFeed) -> None:
        """One atomic look at this pair. Records, never trades."""
        if not self.tokens:
            return
        up, dn = self.tokens.get("UP"), self.tokens.get("DOWN")
        if not up or not dn:
            return
        self.scans += 1
        snap = feed.pair(up, dn)
        if snap is None:
            # No socket snapshot. Deliberately NOT falling back to REST: a
            # pair assembled from two round trips is the artifact this survey
            # exists to avoid counting as an opportunity.
            self.rest_scans += 1
            return
        self.ws_scans += 1
        pair = float(snap.ask_up) + float(snap.ask_down)
        if self.min_pair is None or pair < self.min_pair:
            self.min_pair = pair
            f = _fees(snap.ask_up, snap.ask_down)
            self.best = {
                "ask_up": round(float(snap.ask_up), 4),
                "ask_down": round(float(snap.ask_down), 4),
                "pair": round(pair, 4),
                "gap": round(1.0 - pair, 4),
                "fees": round(f, 4),
                "locked": round(1.0 - pair - f, 4),
                "age_ms": round(float(snap.age_s) * 1000, 1),
            }

    def row(self) -> dict:
        min_locked = float(getattr(config, "ARB_MIN_LOCKED_EDGE", 0.02) or 0.0)
        b = self.best or {}
        return {
            "ts": time.time(),
            "time_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
            "asset": self.asset,
            "timeframe": self.tf,
            "slug": self.slug,
            "scans": self.scans,
            "ws_scans": self.ws_scans,
            "rest_scans": self.rest_scans,
            "phantom_rest": self.phantom_rest,
            "min_pair": b.get("pair"),
            "best_gap": b.get("gap"),
            "best_fees": b.get("fees"),
            "best_locked": b.get("locked"),
            "best_quote": b or None,
            "would_trade": bool(b and b.get("locked", -1) >= min_locked),
        }


def _write(row: dict) -> None:
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, default=str) + "\n")


def run(minutes: float | None = None) -> None:
    pub = PolymarketPublic()
    feed = BookFeed(max_age_s=float(getattr(config, "BOOK_FEED_MAX_AGE_S", 2.0)))
    feed.start()

    markets = [Market(a, tf) for a in ASSETS for tf in TIMEFRAMES]
    print(f"[survey] watching {len(markets)} series: "
          f"{', '.join(m.name for m in markets)}")
    print(f"[survey] socket-priced pairs only; no orders are ever sent")
    print(f"[survey] writing {OUT_PATH}")

    deadline = (time.time() + minutes * 60) if minutes else None
    next_report = time.time() + REPORT_EVERY_S
    rows_written = 0

    try:
        while deadline is None or time.time() < deadline:
            now = time.time()
            # Roll any market whose slot has ended (or that has none yet).
            for m in markets:
                if m.tokens is None or (m.end_ts and now >= m.end_ts):
                    if m.tokens is not None and m.scans:
                        _write(m.row())
                        rows_written += 1
                    m.reset_slot()
                    m.discover(pub)
            toks = []
            for m in markets:
                if m.tokens:
                    toks += [m.tokens.get("UP"), m.tokens.get("DOWN")]
            if toks:
                try:
                    feed.watch(*[t for t in toks if t])
                except Exception:
                    pass

            for m in markets:
                try:
                    m.observe(feed)
                except Exception:
                    pass

            if time.time() >= next_report:
                next_report = time.time() + REPORT_EVERY_S
                st = feed.status()
                print(f"\n[survey] {time.strftime('%H:%M:%S')}  "
                      f"socket live={st.get('live')} books={st.get('books')}  "
                      f"rows={rows_written}")
                for m in markets:
                    if m.unavailable:
                        print(f"   {m.name:9} — series not available")
                        continue
                    b = m.best
                    if not b:
                        print(f"   {m.name:9} {m.scans:5} scans  "
                              f"(ws {m.ws_scans}/{m.scans})  no socket book yet")
                    else:
                        print(f"   {m.name:9} {m.scans:5} scans  "
                              f"(ws {m.ws_scans}/{m.scans})  "
                              f"best pair {b['pair']:.4f}  gap {b['gap']:+.4f}  "
                              f"fees {b['fees']:.4f}  locked {b['locked']:+.4f}")

            time.sleep(SCAN_INTERVAL_S)
    except KeyboardInterrupt:
        print("\n[survey] stopping")
    finally:
        for m in markets:
            if m.tokens is not None and m.scans:
                _write(m.row())
                rows_written += 1
        feed.stop()
        print(f"[survey] wrote {rows_written} slot rows -> {OUT_PATH}")


def summary() -> None:
    if not os.path.isfile(OUT_PATH):
        print(f"no survey data at {OUT_PATH} — run `python survey.py` first")
        return
    rows = []
    with open(OUT_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    continue
    if not rows:
        print("no rows yet")
        return

    by = {}
    for r in rows:
        by.setdefault((r["asset"], r["timeframe"]), []).append(r)

    min_locked = float(getattr(config, "ARB_MIN_LOCKED_EDGE", 0.02) or 0.0)
    print(f"{len(rows)} slot-observations across {len(by)} series"
          f"   (floor: locked edge >= {min_locked:+.4f})\n")
    print(f"{'series':10} {'slots':>6} {'scans':>8} {'ws%':>6} "
          f"{'best pair':>10} {'best gap':>9} {'best locked':>12} {'tradeable':>10}")
    for key in sorted(by):
        rs = by[key]
        scans = sum(r.get("scans") or 0 for r in rs)
        ws = sum(r.get("ws_scans") or 0 for r in rs)
        locked = [r["best_locked"] for r in rs if r.get("best_locked") is not None]
        pairs = [r["min_pair"] for r in rs if r.get("min_pair") is not None]
        gaps = [r["best_gap"] for r in rs if r.get("best_gap") is not None]
        n_trade = sum(1 for r in rs if r.get("would_trade"))
        print(f"{key[0] + '/' + key[1]:10} {len(rs):6} {scans:8} "
              f"{(100.0 * ws / scans if scans else 0):5.1f}% "
              f"{(min(pairs) if pairs else float('nan')):10.4f} "
              f"{(max(gaps) if gaps else float('nan')):+9.4f} "
              f"{(max(locked) if locked else float('nan')):+12.4f} "
              f"{n_trade:>6}/{len(rs)}")

    print()
    tradeable = [r for r in rows if r.get("would_trade")]
    if tradeable:
        print(f"{len(tradeable)} slot(s) had a socket-priced pair that clears "
              f"the floor:")
        for r in tradeable[:10]:
            q = r.get("best_quote") or {}
            print(f"  {r['time_utc']}  {r['asset']}/{r['timeframe']}  "
                  f"UP@{q.get('ask_up')}+DOWN@{q.get('ask_down')} "
                  f"locked {r.get('best_locked'):+.4f}")
    else:
        print("NO socket-priced pair cleared the floor in any series.")
        best = max((r for r in rows if r.get("best_locked") is not None),
                   key=lambda r: r["best_locked"], default=None)
        if best:
            print(f"  closest: {best['asset']}/{best['timeframe']} "
                  f"locked {best['best_locked']:+.4f} "
                  f"(needs {min_locked:+.4f}) — short by "
                  f"{min_locked - best['best_locked']:.4f}")
        print("  For a taker, that is the answer: the books are tight and the "
              "fee is the edge.")


if __name__ == "__main__":
    if "--summary" in sys.argv:
        summary()
    else:
        mins = None
        if "--minutes" in sys.argv:
            try:
                mins = float(sys.argv[sys.argv.index("--minutes") + 1])
            except (IndexError, ValueError):
                mins = None
        run(mins)
