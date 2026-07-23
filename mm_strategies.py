"""Compare market-making side-selection strategies on recorded books.

Naive both-sides quoting loses because it accumulates the side that gets
dumped into it (adverse selection). This asks the real question: is there a
side-selection rule that BEATS adverse selection on these markets?

Strategies tested per slot (rest each enabled bid `edge` under its first ask):
  both       — quote UP and DOWN (the naive baseline that lost)
  favorite   — quote only the higher-priced side (the market's pick)
  underdog   — quote only the lower-priced side (contrarian)
  inventory  — quote to rebalance: if net-long UP, quote only DOWN, and vice
               versa; quote both when flat. Keeps exposure bounded.

Run:
    python mm_strategies.py                 # 2c under ask, 5 shares
    python mm_strategies.py --edge 0.03

Honest, like the rest: fills only on trade-through, maker fee = 0, rebates
not booked, no verdict under n=100. A profitable-looking row at small n is
noise — the point is to see which rule's SHAPE (fill mix, directional risk)
looks least eaten by adverse selection.
"""
from __future__ import annotations
import argparse

from market_maker import MMQuote, simulate_slot, MMStats
from mm_backtest import load_slots, ask_path, _rec_path


def _sides_for(mode: str, up_fav: bool, net_inv: float):
    """(quote_up, quote_down) for a strategy given the slot + running inventory."""
    if mode == "both":
        return True, True
    if mode == "favorite":
        return (up_fav, not up_fav)
    if mode == "underdog":
        return (not up_fav, up_fav)
    if mode == "inventory":
        # Rebalance toward flat: skip the side we're already long.
        if net_inv > 1e-9:      # net long UP -> only quote DOWN
            return False, True
        if net_inv < -1e-9:     # net long DOWN -> only quote UP
            return True, False
        return True, True       # flat -> quote both
    return True, True


def run_mode(slots: list[dict], mode: str, edge: float, size: float) -> dict:
    st = MMStats()
    net_inv = 0.0
    for d in slots:
        up = ask_path(d["snapshots"], "up_ask")
        dn = ask_path(d["snapshots"], "dn_ask")
        if not up or not dn:
            continue
        up_fav = up[0] >= dn[0]
        qu, qd = _sides_for(mode, up_fav, net_inv)
        r = simulate_slot(
            MMQuote(up_bid=round(up[0] - edge, 2),
                    down_bid=round(dn[0] - edge, 2), size=size),
            up_ask_path=up, down_ask_path=dn,
            resolved_up=bool(d["resolved_up"]),
            quote_up=qu, quote_down=qd, slot_id=d.get("slug", ""))
        st.add(r)
        net_inv += r.net_inventory
    s = st.summary()
    s["mode"] = mode
    s["end_net_inventory"] = round(net_inv, 1)  # leftover directional exposure
    return s


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--edge", type=float, default=0.02)
    ap.add_argument("--size", type=float, default=5.0)
    args = ap.parse_args()

    slots = load_slots(_rec_path())
    slots = [d for d in slots if d.get("resolved_up") in (True, False)
             and d.get("snapshots")]
    if not slots:
        print("No usable recordings. Run the recorder first "
              "(dashboard: Start recorder).")
        return

    print(f"=== MM STRATEGY COMPARISON ({len(slots)} slots, "
          f"edge {args.edge:.2f}, size {args.size:.0f}) ===")
    print(f"{'strategy':<10} {'pnl/slot':>9} {'total':>8} {'both%':>6} "
          f"{'1-sided%':>8} {'dir.risk':>8} {'end_inv':>7}")
    for mode in ("both", "favorite", "underdog", "inventory"):
        s = run_mode(slots, mode, args.edge, args.size)
        print(f"{mode:<10} {s['pnl_per_slot']:>+9.4f} {s['total_pnl']:>+8.2f} "
              f"{s['both_filled_pct']:>6} {s['one_filled_pct']:>8} "
              f"{s['gross_directional_shares']:>8} {s['end_net_inventory']:>7}")
    print()
    if len(slots) < 100:
        print(f"n={len(slots)} — NOISE, not a verdict. Read the SHAPE: which "
              "rule has the least one-sided% and directional risk while "
              "staying near-flat (end_inv ~ 0)? Keep recording toward 100+.")


if __name__ == "__main__":
    main()
