"""Replay recorded order books through the market-making core.

Reads state/archive/book_recordings.jsonl (produced by record_books.py) and,
for each recorded slot, rests two-sided bids at (best_ask - EDGE) and reports
what two-sided quoting would ACTUALLY have captured — honest fills (market must
trade through the bid), honest inventory risk, maker fee = 0, rebates not
booked.

    python mm_backtest.py                 # default 2c under ask, 5 shares
    python mm_backtest.py --edge 0.03 --size 5

This answers one question before any live MM work: does resting both sides even
capture spread on these markets, or does inventory risk eat it? A tiny sample
proves nothing — the recorder needs to run first.
"""
from __future__ import annotations
import argparse
import json
import os

from market_maker import MMQuote, simulate_slot, MMStats

REC = os.path.join(os.path.dirname(__file__), "state", "archive",
                   "book_recordings.jsonl")


def load_slots() -> list[dict]:
    out = []
    if not os.path.isfile(REC):
        return out
    with open(REC, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            if d.get("resolved_up") in (True, False) and d.get("snapshots"):
                out.append(d)
    return out


def ask_path(snaps: list, key: str) -> list[float]:
    path = []
    for s in snaps:
        v = s.get(key)
        if v is not None:
            try:
                path.append(float(v))
            except (TypeError, ValueError):
                pass
    return path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--edge", type=float, default=0.02,
                    help="how far under best ask to rest each bid")
    ap.add_argument("--size", type=float, default=5.0,
                    help="shares per side")
    ap.add_argument("--rebate", type=float, default=0.0,
                    help="per-share rebate estimate (reported, NOT booked)")
    args = ap.parse_args()

    slots = load_slots()
    if not slots:
        print(f"No usable recordings in {REC}")
        print("Run `python record_books.py` for a few hours first — a real MM")
        print("backtest needs full two-sided book paths, which we don't have yet.")
        return

    st = MMStats()
    skipped = 0
    for d in slots:
        snaps = d["snapshots"]
        up = ask_path(snaps, "up_ask")
        dn = ask_path(snaps, "dn_ask")
        if not up or not dn:
            skipped += 1
            continue
        # Rest each bid `edge` under the FIRST observed ask (our quote at entry).
        up_bid = round(up[0] - args.edge, 2)
        dn_bid = round(dn[0] - args.edge, 2)
        st.add(simulate_slot(
            MMQuote(up_bid=up_bid, down_bid=dn_bid, size=args.size),
            up_ask_path=up, down_ask_path=dn,
            resolved_up=bool(d["resolved_up"]), slot_id=d.get("slug", ""),
            fee_rebate_share=args.rebate))

    s = st.summary()
    print(f"=== MM BACKTEST ({s['slots']} slots, edge {args.edge:.2f}, "
          f"size {args.size:.0f}, {skipped} skipped) ===")
    print(f"  fills : both {s['both_filled_pct']}%  one-sided "
          f"{s['one_filled_pct']}%  none {s['none_filled_pct']}%")
    print(f"  PnL   : {s['total_pnl']:+.2f}  ({s['pnl_per_slot']:+.4f}/slot, "
          f"{s['return_on_deployed_pct']:+.2f}% on deployed)")
    print(f"  RISK  : {s['gross_directional_shares']} gross directional "
          f"shares from one-sided fills")
    if s.get("rebate_upside_not_booked"):
        print(f"  upside: +{s['rebate_upside_not_booked']:.2f} rebates "
              f"(NOT in PnL above)")
    print()
    if s["slots"] < 100:
        print(f"NOTE: only {s['slots']} slots — noise, not a verdict. The "
              "one-sided-fill % and its direction risk matter more than PnL "
              "at this sample size. Keep the recorder running.")
    else:
        verdict = ("captures spread" if s["pnl_per_slot"] > 0
                   else "inventory risk eats the spread")
        print(f"VERDICT (n>=100): two-sided quoting {verdict} here "
              f"({s['pnl_per_slot']:+.4f}/slot).")


if __name__ == "__main__":
    main()
