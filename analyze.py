"""Self-serve performance report for the trade archive.

Usage:
    python analyze.py               # everything in the archive
    python analyze.py --hours 12    # only the last N hours
    python analyze.py --session 20260717T192051Z

Also powers the dashboard's Performance Analysis panel via
`compute_report()` (imported by app.py -> GET /api/analyze).

Reads state/archive/trades_history.jsonl (permanent, survives --reset).
Interpret with the two rules that keep you honest:
  1) win rate means nothing without the entry price (breakeven = avg entry);
  2) one outlier can carry a whole session — always check trimmed PnL.
"""
from __future__ import annotations
import argparse
import json
import os
import time

HERE = os.path.dirname(os.path.abspath(__file__))
# Follow the same per-timeframe layout as bot.py / app.py, so a 15m instance
# analyses its OWN trades instead of silently reporting the 5m archive.
_TF = (os.getenv("TIMEFRAME") or "5m").strip().lower()
_STATE_SUB = (os.getenv("STATE_SUBDIR") or ("" if _TF == "5m" else _TF)).strip()
_STATE = os.path.join(HERE, "state", _STATE_SUB) if _STATE_SUB \
    else os.path.join(HERE, "state")
HISTORY = os.path.join(_STATE, "archive", "trades_history.jsonl")
STATS = os.path.join(_STATE, "stats.json")

MOMENTUM_TARGET_N = 100  # sample size where the verdict starts to mean something

# When the CURRENT configuration came into force (2026-07-19 ~18:26 UTC:
# 0.88 entry cap + real fee model + maker-first + honest fills all live).
# Trades before this tested strategies/configs that no longer exist — they
# stay in the archive as the evidence base for today's gates, but they must
# not count toward validating the current bot.
CONFIG_ERA_START = 1784500000


def paths_for(state_dir: str | None = None) -> tuple[str, str]:
    """(history_jsonl, stats_json) for a given state dir (None = this one)."""
    st = state_dir or _STATE
    return (os.path.join(st, "archive", "trades_history.jsonl"),
            os.path.join(st, "stats.json"))


def load_rows(hours: float | None, session: str | None,
              state_dir: str | None = None, era: bool = False) -> list[dict]:
    rows = []
    HISTORY, _ = paths_for(state_dir)
    if not os.path.isfile(HISTORY):
        return rows
    cutoff = time.time() - hours * 3600 if hours else None
    if era:
        cutoff = max(cutoff or 0, CONFIG_ERA_START)
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


def net_pnl(row) -> float:
    """PnL with real taker fees applied.

    Trades archived before the fee model (2026-07-18) recorded fee_usd=0, so
    their stored pnl is OVERSTATED. Retro-charge them at the real rate —
    otherwise historical buckets look profitable when they were not (this
    exact illusion made 0.80-0.90 entries look like a small edge).
    """
    pnl = fnum(row, "pnl")
    fee = fnum(row, "fee_usd")
    if fee > 0:
        return pnl
    try:
        import config
        return pnl - fnum(row, "shares") * config.taker_fee_per_share(
            fnum(row, "entry_price"))
    except Exception:
        return pnl


def est_fee(row) -> float:
    """Fee actually charged, or the retro estimate for old rows."""
    fee = fnum(row, "fee_usd")
    if fee > 0:
        return fee
    try:
        import config
        return fnum(row, "shares") * config.taker_fee_per_share(
            fnum(row, "entry_price"))
    except Exception:
        return 0.0


def _agg(label: str, rows: list[dict]) -> dict:
    n = len(rows)
    if not n:
        return {"label": label, "n": 0}
    wins = sum(1 for r in rows if r.get("won"))
    pnl = sum(net_pnl(r) for r in rows)   # fee-adjusted (see net_pnl)
    stake = sum(fnum(r, "stake_usd") for r in rows)
    return {
        "label": label,
        "n": n,
        "wins": wins,
        "win_rate": round(100.0 * wins / n, 1),
        "pnl": round(pnl, 2),
        "expectancy": round(pnl / n, 3),
        "avg_entry": round(sum(fnum(r, "entry_price") for r in rows) / n, 3),
        "avg_stake": round(stake / n, 2),
        "fees": round(sum(est_fee(r) for r in rows), 2),
    }


def compute_report(hours: float | None = None,
                   session: str | None = None,
                   state_dir: str | None = None,
                   era: bool = False) -> dict:
    """Structured report used by both the CLI and the dashboard.

    state_dir lets the dashboard analyse any timeframe's archive without
    re-importing the module with a different TIMEFRAME.
    """
    rows = load_rows(hours, session, state_dir, era=era)
    _, STATS_P = paths_for(state_dir)
    scope = (f"last {hours:g}h" if hours
             else (f"session {session}" if session
                   else ("current config (since Jul 19)" if era
                         else "all history")))
    out: dict = {"ok": True, "scope": scope, "n": len(rows),
                 "generated_at": time.time()}
    if not rows:
        out["ok"] = False
        out["error"] = f"No settled trades found for {scope}"
        return out

    arb = [r for r in rows if r.get("side") == "ARB"]
    mom = [r for r in rows if r.get("side") in ("UP", "DOWN")]
    out["groups"] = [
        _agg("ALL", rows),
        _agg("ARB (paired)", arb),
        _agg("MOMENTUM", mom),
        _agg("momentum UP", [r for r in mom if r.get("side") == "UP"]),
        _agg("momentum DOWN", [r for r in mom if r.get("side") == "DOWN"]),
    ]

    if mom:
        n = len(mom)
        wins = sum(1 for r in mom if r.get("won"))
        avg_entry = sum(fnum(r, "entry_price") for r in mom) / n
        fees = sum(est_fee(r) for r in mom)
        stake = sum(fnum(r, "stake_usd") for r in mom) or 1.0
        breakeven = avg_entry + fees / stake * avg_entry
        achieved = wins / n
        out["momentum"] = {
            "n": n,
            "achieved_wr": round(100 * achieved, 1),
            "breakeven_wr": round(100 * breakeven, 1),
            "above_breakeven": achieved > breakeven,
            "sample_ok": n >= MOMENTUM_TARGET_N,
            "target_n": MOMENTUM_TARGET_N,
            "buckets": [
                _agg(f"{lo:.2f}-{hi:.2f}",
                     [r for r in mom if lo <= fnum(r, "entry_price") < hi])
                for lo, hi in ((0.0, 0.70), (0.70, 0.80), (0.80, 0.85),
                               (0.85, 0.90), (0.90, 0.95), (0.95, 1.01))
                if any(lo <= fnum(r, "entry_price") < hi for r in mom)
            ],
        }

    ordered = sorted(rows, key=lambda r: abs(net_pnl(r)), reverse=True)
    top = ordered[0]
    total = sum(net_pnl(r) for r in rows)
    top_pnl = net_pnl(top)
    out["outlier"] = {
        "side": top.get("side"),
        "pnl": round(top_pnl, 2),
        "time_utc": top.get("time_utc", ""),
        "pct_of_total": round(100 * abs(top_pnl) / max(0.01, abs(total))),
        "total": round(total, 2),
        "total_without": round(total - top_pnl, 2),
        "dominates": abs(top_pnl) > 0.5 * abs(total) and len(rows) > 5,
    }

    src: dict = {}
    for r in rows:
        k = r.get("resolve_source") or "?"
        src[k] = src.get(k, 0) + 1
    out["sources"] = src

    if os.path.isfile(STATS_P):
        try:
            _raw = open(STATS_P, encoding="utf-8").read()
            # Tolerate a torn / NUL-filled file left by a killed process.
            s = json.loads(_raw) if _raw and _raw[:1] != "\x00" else {}
            out["session_id"] = s.get("session_id")
            out["skips"] = {k: v for k, v in (s.get("skips") or {}).items() if v}
            out["stream_warning"] = (
                (s.get("skips") or {}).get("proxy_strike", 0) > 10)
        except Exception:
            pass
    return out


def _print_group(g: dict) -> None:
    if not g.get("n"):
        print(f"{g['label']:<22} n=0")
        return
    print(f"{g['label']:<22} n={g['n']:<4} wr={g['win_rate']:5.1f}%  "
          f"pnl={g['pnl']:+8.2f}  exp={g['expectancy']:+.3f}/trade  "
          f"avg_entry={g['avg_entry']:.3f}  avg_stake={g['avg_stake']:.2f}  "
          f"fees={g['fees']:.2f}")


def _block(title):
    print(f"\n=== {title} " + "=" * max(1, 60 - len(title)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=None)
    ap.add_argument("--session", type=str, default=None)
    ap.add_argument("--era", action="store_true",
                    help="only trades since the current config (the sample "
                         "that counts toward the n=100 verdict)")
    args = ap.parse_args()
    rep = compute_report(args.hours, args.session, era=args.era)
    if not rep.get("ok"):
        print(rep.get("error"))
        return

    _block(f"TOTALS ({rep['scope']}, {rep['n']} trades)")
    for g in rep["groups"]:
        _print_group(g)

    m = rep.get("momentum")
    if m:
        _block("MOMENTUM REALITY CHECK")
        print(f"achieved win rate : {m['achieved_wr']:.1f}%  over n={m['n']}")
        print(f"breakeven win rate: ~{m['breakeven_wr']:.1f}%  "
              f"(avg entry + fees)")
        verdict = "ABOVE breakeven" if m["above_breakeven"] else "BELOW breakeven"
        conf = ("(n>=%d: take it seriously)" % m["target_n"] if m["sample_ok"]
                else f"(n={m['n']}: small-sample noise — target {m['target_n']}+)")
        print(f"verdict           : {verdict} {conf}")
        _block("MOMENTUM BY ENTRY BUCKET")
        for b in m["buckets"]:
            _print_group({**b, "label": "  entry " + b["label"]})

    o = rep["outlier"]
    _block("OUTLIER SENSITIVITY")
    print(f"biggest single trade: {o['side']} pnl={o['pnl']:+.2f} "
          f"({o['time_utc']}) — {o['pct_of_total']}% of |total|")
    print(f"total without it    : {o['total_without']:+.2f}  "
          f"(vs {o['total']:+.2f} with)")
    if o["dominates"]:
        print("WARNING: one trade dominates the result — treat the headline "
              "number as unproven.")

    _block("RESOLUTION SOURCES")
    for k, v in sorted(rep["sources"].items(), key=lambda x: -x[1]):
        flag = "  <-- unreliable" if "UNRELIABLE" in k else ""
        print(f"  {k}: {v}{flag}")

    if rep.get("skips"):
        _block(f"CURRENT SESSION SKIPS ({rep.get('session_id')})")
        for k, v in rep["skips"].items():
            print(f"  {k}: {v}")
        if rep.get("stream_warning"):
            print("  NOTE: high proxy_strike = stream missing slot opens "
                  "-> momentum starved. Check bot.log for reconnect gaps.")
    print()


if __name__ == "__main__":
    main()
