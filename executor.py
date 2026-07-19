"""Order execution.

PaperExecutor simulates fills locally (no money, no network) and resolves each
5-minute round against the realized BTC move. Live orders are sent through
`PolymarketClient.buy` directly from the bot loop (tokens change every slot).
Both paths record trades with the same `Trade` shape so the dashboard doesn't
care which one ran.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import time

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

ET_ZONE = None
if ZoneInfo is not None:
    try:
        ET_ZONE = ZoneInfo("America/New_York")
    except Exception:
        ET_ZONE = None

POLYMARKET_EVENT_URL = "https://polymarket.com/event/"


def format_trade_times(ts: float) -> tuple[str, str]:
    """Return (time_utc, time_et) human-readable strings for a unix timestamp."""
    if not ts:
        return "", ""
    dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
    time_utc = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    time_et = ""
    if ET_ZONE is not None:
        time_et = dt.astimezone(ET_ZONE).strftime("%Y-%m-%d %I:%M:%S %p ET")
    else:
        # Fallback without zoneinfo: show UTC only labeled
        time_et = time_utc.replace("UTC", "ET~")
    return time_utc, time_et


def stamp_trade_times(trade: "Trade", when: float | None = None) -> "Trade":
    """Fill entry time fields from trade.ts (or `when`)."""
    ts = float(when if when is not None else trade.ts or time.time())
    trade.ts = ts
    trade.time_utc, trade.time_et = format_trade_times(ts)
    if trade.slug and not trade.event_url:
        trade.event_url = f"{POLYMARKET_EVENT_URL}{trade.slug}"
    return trade


def stamp_resolved_times(trade: "Trade", when: float | None = None) -> "Trade":
    """Fill resolution timestamps after the slot settles."""
    ts = float(when if when is not None else time.time())
    trade.resolved_ts = ts
    trade.resolved_time_utc, trade.resolved_time_et = format_trade_times(ts)
    return trade


@dataclass
class Trade:
    ts: float
    round_id: int
    side: str            # UP / DOWN
    entry_price: float   # cost per contract (0-1)
    shares: float
    stake_usd: float
    model_prob: float
    edge: float
    outcome: str = ""    # UP / DOWN once resolved
    won: bool | None = None
    pnl: float = 0.0
    bankroll_after: float = 0.0
    # Human-readable + Polymarket cross-check fields
    time_utc: str = ""
    time_et: str = ""            # Polymarket titles use ET
    resolved_ts: float | None = None
    resolved_time_utc: str = ""
    resolved_time_et: str = ""
    slug: str = ""
    question: str = ""
    slot_start_ts: float | None = None
    end_ts: float | None = None
    event_url: str = ""
    resolve_source: str = ""     # polymarket-official / chainlink-stream / ...
    fee_usd: float = 0.0         # taker fee (crypto_fees_v2); deducted from pnl
    is_maker: bool = False       # resting fill -> zero fee (takerOnly venue)

    def to_dict(self):
        # Ensure times are populated even if older code paths skipped stamping.
        if self.ts and not self.time_utc:
            self.time_utc, self.time_et = format_trade_times(self.ts)
        if self.resolved_ts and not self.resolved_time_utc:
            self.resolved_time_utc, self.resolved_time_et = format_trade_times(
                self.resolved_ts)
        if self.slug and not self.event_url:
            self.event_url = f"{POLYMARKET_EVENT_URL}{self.slug}"
        return asdict(self)


class PaperExecutor:
    """Simulated fills. Buys the chosen side at the market price; pays $1 per
    winning contract, $0 per losing one.

    side="ARB" is a paired position (equal shares of UP and DOWN bought when
    ask_UP + ask_DOWN < 1): exactly one leg pays $1/share whichever way the
    slot resolves, so payout = shares regardless of direction. entry_price
    stores the SUM of both asks; won = the pair cost less than it paid."""

    def open_and_resolve(self, trade: Trade, resolved_up: bool) -> Trade:
        trade.outcome = "UP" if resolved_up else "DOWN"
        fee = float(trade.fee_usd or 0.0)
        if trade.side == "ARB":
            payout = trade.shares  # winning leg pays $1/share, other pays $0
            trade.pnl = round(payout - trade.stake_usd - fee, 4)
            trade.won = trade.pnl > 0
        else:
            trade.won = (trade.side == trade.outcome)
            payout = trade.shares * (1.0 if trade.won else 0.0)
            trade.pnl = round(payout - trade.stake_usd - fee, 4)
        stamp_resolved_times(trade)
        return trade
