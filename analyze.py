"""Self-serve performance report for the trade archive.

Usage:
    python analyze.py               # everything in the archive
    python analyze.py --hours 12    # only the last N hours
    python analyze.py --session 20260717T192051Z

Reads state/archive/trades_history.jsonl (permanent, survives --reset).
No dependencies. Interpret with the two rules that keep you honest:
  1) win rate means nothing without the entry price (breakeven = avg entry);
  2) one outlier can carry a whole session — always check trimmed PnL.
"""
from __future__ import annotations
import argparse
import json
import os
import time

HERE = os.path.dirname(os.path.abspath(__file__))
HISTORY = os.path.join(HERE, "state", "archive", "trades_history.jsonl")
STATS = os.path.join(HERE, "state", "stats.json")


def load_rows(hours: float | None, session: str | None) -> list[dict]:
    rows = []
    if not os.path.isfile(HISTORY):
        return rows
    cutoff = time.time() - hours * 3600 if hours else None
    with open(HISTORY, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            if session and d.get("session_id") != session:
                continue
            if cutoff and float(d.get("ts") or 0) < cutoff:
                continue
            rows.append(d)
    return rows


def fnum(row, key, default=0.0):
    try:
        return float(row.get(key) or default)
    except (TypeError, ValueError):
        return default


def block(title):
    print(f"\n=== {title} " + "=" * max(1, 60 - len(title)))


def summarize(label, rows):
    if not rows:
        print(f"{label:<22} n=0")
        return
    n = len(rows)
    wins = sum(1 for r in rows if r.get("won"))
    pnl = sum(fnum(r, "pnl") for r in rows)
    stake = sum(fnum(r, "stake_usd") for r in rows)
    entry = sum(fnum(r, "entry_price") for r in rows) / n
    fees = sum(fnum(r, "fee_usd") for r in rows)
    print(f"{label:<22} n={n:<4} wr={100*wins/n:5.1f}%  pnl={pnl:+8.2f}  "
          f"exp={pnl/n:+.3f}/trade  avg_entry={entry:.3f}  "
          f"avg_stake={stake/n:.2f}  fees={fees:.2f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=None)
    ap.add_argument("--session", type=str, default=None)
    args = ap.parse_args()

    rows = load_rows(args.hours, args.session)
    scope = (f"last {args.hours:g}h" if args.hours
             else (f"session {args.session}" if args.session else "all history"))
    if not rows:
        print(f"No settled trades found for {scope} in {HISTORY}")
        return

    block(f"TOTALS ({scope}, {len(rows)} trades)")
    summarize("ALL", rows)
    arb = [r for r in rows if r.get("side") == "ARB"]
    mom = [r for r in rows if r.get("side") in ("UP", "DOWN")]
    summarize("ARB (paired)", arb)
    summarize("MOMENTUM (UP+DOWN)", mom)
    summarize("  momentum UP", [r for r in mom if r.get("side") == "UP"])
    summarize("  momentum DOWN", [r for r in mom if r.get("side") == "DOWN"])

    if mom:
        block("MOMENTUM REALITY CHECK")
        n = len(mom)
        wins = sum(1 for r in mom if r.get("won"))
        avg_entry = sum(fnum(r, "entry_price") for r in mom) / n
        # Breakeven win rate for buying at avg entry (fee-adjusted approx.)
        fees = sum(fnum(r, "fee_usd") for r in mom)
        stake = sum(fnum(r, "stake_usd") for r in mom) or 1.0
        be = avg_entry + fees / stake * avg_entry
        print(f"achieved win rate : {100*wins/n:.1f}%  over n={n}")
        print(f"breakeven win rate: ~{100*be:.1f}%  (avg entry {avg_entry:.3f} "
              f"+ fees)")
        verdict = ("ABOVE breakeven" if wins / n > be else "BELOW breakeven")
        conf = ("(n>=100: take it seriously)" if n >= 100 else
                f"(n={n}: still small-sample noise — target 100+)")
        print(f"verdict           : {verdict} {conf}")

        block("MOMENTUM BY ENTRY BUCKET")
        for lo, hi in ((0.0, 0.70), (0.70, 0.80), (0.80, 0.85),
                       (0.85, 0.90), (0.90, 0.95), (0.95, 1.01)):
            b = [r for r in mom if lo <= fnum(r, "entry_price") < hi]
            if b:
                summarize(f"  entry {lo:.2f}-{hi:.2f}", b)

    block("OUTLIER SENSITIVITY")
    ordered = sorted(rows, key=lambda r: abs(fnum(r, "pnl")), reverse=True)
    top = ordered[0]
    total = sum(fnum(r, "pnl") for r in rows)
    rest = total - fnum(top, "pnl")
    print(f"biggest single trade: {top.get('side')} pnl={fnum(top,'pnl'):+.2f} "
          f"({top.get('time_utc','')}) — "
          f"{100*abs(fnum(top,'pnl'))/max(0.01,abs(total)):.0f}% of |total|")
    print(f"total without it    : {rest:+.2f}  (vs {total:+.2f} with)")
    if abs(fnum(top, "pnl")) > 0.5 * abs(total) and len(rows) > 5:
        print("WARNING: one trade dominates the result — treat the headline "
              "number as unproven.")

    block("RESOLUTION SOURCES")
    src: dict = {}
    for r in rows:
        k = r.get("resolve_source") or "?"
        src[k] = src.get(k, 0) + 1
    for k, v in sorted(src.items(), key=lambda x: -x[1]):
        flag = "  <-- unreliable" if "UNRELIABLE" in k else ""
        print(f"  {k}: {v}{flag}")

    if os.path.isfile(STATS):
        try:
            s = json.load(open(STATS, encoding="utf-8"))
            block(f"CURRENT SESSION SKIPS ({s.get('session_id')})")
            for k, v in (s.get("skips") or {}).items():
                if v:
                    print(f"  {k}: {v}")
            if (s.get("skips") or {}).get("proxy_strike", 0) > 10:
                print("  NOTE: high proxy_strike = stream missing slot opens "
                      "-> momentum starved. Check bot.log for reconnect gaps.")
        except Exception:
            pass

    print()


if __name__ == "__main__":
    main()
