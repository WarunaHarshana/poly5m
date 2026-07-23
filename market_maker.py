"""Market-making core (PAPER simulation, standalone + unit-tested).

The strategy the verified +$95k wallet runs: rest bids on BOTH sides of a 5m
market below mid, let takers come to you, earn the spread + maker rebates +
liquidity rewards instead of predicting direction. Makers pay ZERO fee here
(feeSchedule.takerOnly=True).

This module is the SUBTLE, RISKY core kept separate so it can be tested in
isolation before touching the live loop. It models the three things naive
market-making code gets silently wrong:

  1. Honest fills — a resting bid fills ONLY when the market trades through
     it (best ask drops to <= our bid). Waiting alone never fills. This is
     an UPPER bound because queue position is not modelled.
  2. Inventory risk — if only one side's bid fills you are left NET
     DIRECTIONAL, holding a position you did not want. This is the real risk
     of market-making and it is tracked explicitly, not hidden.
  3. Rebates NOT credited — the liquidity-reward/rebate pool is real but
     proportional and not guaranteed, so PnL here is spread-capture only.
     Counting speculative rebates is the exact optimism the fee bug taught
     us to avoid; they are reported separately as an upside, never in PnL.

Nothing here places a real order. Wiring into bot.py is a SEPARATE step,
gated behind a proven paper fill rate.
"""
from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class MMQuote:
    """One slot's two resting bids."""
    up_bid: float           # price we rest a BUY on the UP token
    down_bid: float         # price we rest a BUY on the DOWN token
    size: float             # shares per side


@dataclass
class MMResult:
    slot_id: str
    filled_up: float = 0.0      # shares actually bought on UP
    filled_down: float = 0.0    # shares actually bought on DOWN
    cost_up: float = 0.0        # $ spent on UP fills
    cost_down: float = 0.0      # $ spent on DOWN fills
    resolved_up: bool | None = None
    payout: float = 0.0         # $ returned at resolution
    pnl: float = 0.0            # payout - cost (maker fee = 0)
    net_inventory: float = 0.0  # filled_up - filled_down (directional exposure)
    hedged_pairs: float = 0.0   # min(up, down): the riskless matched part
    rebate_upside: float = 0.0  # NOT in pnl — reported separately

    @property
    def cost(self) -> float:
        return self.cost_up + self.cost_down


def simulate_slot(
    quote: MMQuote,
    up_ask_path: list[float],
    down_ask_path: list[float],
    resolved_up: bool,
    slot_id: str = "",
    fee_rebate_share: float = 0.0,
    quote_up: bool = True,
    quote_down: bool = True,
) -> MMResult:
    """Simulate one 5-minute slot of quoting.

    `up_ask_path` / `down_ask_path` are the observed best-ask series for each
    token over the slot (e.g. one sample per second). A resting bid on a side
    fills the first time that side's ask trades DOWN to our bid or below — we
    then hold `size` shares on that side. Only one fill per side per slot
    (a single resting order).

    quote_up / quote_down: whether we actually rest a bid on that side. This
    is how the strategy variants (favorite-only, underdog-only, inventory-
    managed) express which side(s) to quote — a disabled side never fills.

    resolved_up: which side pays $1/share at settlement.
    fee_rebate_share: optional, for the SEPARATE upside estimate only.
    """
    r = MMResult(slot_id=slot_id, resolved_up=resolved_up)

    # UP bid: filled when the UP ask trades through our bid (if we quote it).
    if quote_up and any(a <= quote.up_bid + 1e-9 for a in up_ask_path):
        r.filled_up = quote.size
        r.cost_up = round(quote.size * quote.up_bid, 6)
    # DOWN bid: same, independently.
    if quote_down and any(a <= quote.down_bid + 1e-9 for a in down_ask_path):
        r.filled_down = quote.size
        r.cost_down = round(quote.size * quote.down_bid, 6)

    # Resolution: the winning side pays $1/share, the other pays $0.
    r.payout = round(
        (r.filled_up if resolved_up else r.filled_down) * 1.0, 6)
    r.pnl = round(r.payout - r.cost, 6)  # maker fee = 0 on this venue

    r.net_inventory = round(r.filled_up - r.filled_down, 6)
    r.hedged_pairs = min(r.filled_up, r.filled_down)

    # Rebate upside (reported, NOT booked): a share of taker fees, proportional
    # to your resting size. Conservative flat estimate for research only.
    if fee_rebate_share > 0:
        r.rebate_upside = round(
            (r.filled_up + r.filled_down) * fee_rebate_share, 6)
    return r


@dataclass
class MMStats:
    slots: int = 0
    both_filled: int = 0        # both bids hit -> spread capture, low risk
    one_filled: int = 0         # only one -> naked directional this slot
    none_filled: int = 0
    total_cost: float = 0.0
    total_pnl: float = 0.0
    total_rebate_upside: float = 0.0
    gross_directional: float = 0.0   # sum |net_inventory| — risk accumulated
    results: list = field(default_factory=list)

    def add(self, r: MMResult) -> None:
        self.slots += 1
        up, dn = r.filled_up > 0, r.filled_down > 0
        if up and dn:
            self.both_filled += 1
        elif up or dn:
            self.one_filled += 1
        else:
            self.none_filled += 1
        self.total_cost += r.cost
        self.total_pnl += r.pnl
        self.total_rebate_upside += r.rebate_upside
        self.gross_directional += abs(r.net_inventory)
        self.results.append(r)

    def summary(self) -> dict:
        n = self.slots or 1
        deployed = self.total_cost or 1.0
        return {
            "slots": self.slots,
            "both_filled_pct": round(100 * self.both_filled / n, 1),
            "one_filled_pct": round(100 * self.one_filled / n, 1),
            "none_filled_pct": round(100 * self.none_filled / n, 1),
            "total_pnl": round(self.total_pnl, 2),
            "pnl_per_slot": round(self.total_pnl / n, 4),
            "return_on_deployed_pct": round(100 * self.total_pnl / deployed, 3),
            "rebate_upside_not_booked": round(self.total_rebate_upside, 2),
            # The honest risk number: how much naked directional exposure the
            # one-sided fills piled up relative to what was hedged.
            "gross_directional_shares": round(self.gross_directional, 1),
        }
