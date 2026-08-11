"""Order lifecycle: what the venue actually said, at each step.

The failure this exists to prevent is treating three different things as one:

    we decided to trade  !=  the venue accepted an order  !=  the order filled

Every bug of consequence in this bot has been a collapse of that distinction.
The live fill check used to "default to full size" when the lookup errored,
booking a position off a failed API call. Resolution used to fall back to a
local guess and book it permanently, which fabricated a win on R55. Both looked
identical to success from the outside, because nothing recorded WHICH step had
actually been confirmed.

So each transition is recorded with the venue's own response attached, and the
terminal state says exactly how much is known:

    FILLED       the venue confirmed a fill, with a size
    PARTIAL      the venue confirmed a smaller fill than requested
    REJECTED     the venue refused the order
    CANCELLED    we pulled it before it filled
    UNVERIFIED   we could not confirm - treated as NOT filled
    SIMULATED    paper mode: no order was ever sent to Polymarket

SIMULATED is deliberately not a success state. Paper fills are modelled against
real books, but nothing was submitted and nothing was matched, and the UI must
never let that read as a venue-confirmed trade.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import paths

ARCHIVE_DIR = paths.archive_dir()
ORDER_EVENTS = os.path.join(ARCHIVE_DIR, "order_events.jsonl")

# --- states ---------------------------------------------------------------
SIGNAL = "SIGNAL"            # decided to trade; nothing sent yet
SUBMITTED = "SUBMITTED"      # request sent, no response yet
ACCEPTED = "ACCEPTED"        # venue acknowledged, order id issued
OPEN = "OPEN"                # resting on the book
PARTIAL = "PARTIALLY_FILLED"
FILLED = "FILLED"
CANCELLED = "CANCELLED"
REJECTED = "REJECTED"
EXPIRED = "EXPIRED"
UNVERIFIED = "UNVERIFIED"    # could not confirm -> treated as unfilled
SIMULATED = "SIMULATED"      # paper: never sent to the venue

# States in which real size is actually held.
HOLDS_POSITION = (PARTIAL, FILLED)
# States that mean "no venue confirmation of a fill".
NOT_CONFIRMED = (SIGNAL, SUBMITTED, ACCEPTED, OPEN, CANCELLED, REJECTED,
                 EXPIRED, UNVERIFIED, SIMULATED)

MAX_ROWS = int(os.getenv("ORDER_EVENTS_MAX_ROWS", "50000"))


def _utc(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC")


def record(state: str, *, round_id=None, slug: str = "", side: str = "",
           token_id: str = "", order_id: str = "", requested_shares=None,
           filled_shares=None, price=None, mode: str = "",
           venue_response=None, note: str = "") -> dict:
    """Append one lifecycle transition. Never raises — logging must not trade."""
    ev = {
        "ts": time.time(),
        "state": state,
        "round_id": round_id,
        "slug": slug,
        "side": side,
        "token_id": (token_id or "")[:12],   # identifier only, never a secret
        "order_id": order_id or "",
        "requested_shares": requested_shares,
        "filled_shares": filled_shares,
        "price": price,
        "mode": mode,
        "note": note,
    }
    ev["time_utc"] = _utc(ev["ts"])
    if venue_response is not None:
        # Keep the venue's own words, truncated — this is the evidence that
        # distinguishes "we think it filled" from "the venue said it filled".
        try:
            ev["venue_response"] = json.dumps(venue_response, default=str)[:600]
        except Exception:
            ev["venue_response"] = str(venue_response)[:600]
    try:
        with open(ORDER_EVENTS, "a", encoding="utf-8") as f:
            f.write(json.dumps(ev, ensure_ascii=True, default=str) + "\n")
    except OSError as e:
        print(f"  [order_state] write failed: {e}")
    return ev


def load(limit: int | None = None, round_id=None) -> list[dict]:
    rows: list[dict] = []
    if not os.path.isfile(ORDER_EVENTS):
        return rows
    try:
        with open(ORDER_EVENTS, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except ValueError:
                    continue
                if round_id is not None and d.get("round_id") != round_id:
                    continue
                rows.append(d)
    except OSError:
        pass
    return rows[-limit:] if limit else rows


def trail(round_id) -> list[dict]:
    """Every state this order passed through, oldest first."""
    return sorted(load(round_id=round_id), key=lambda e: e.get("ts") or 0)


def summary(limit: int = 500) -> dict:
    """Counts by terminal state — the honest headline for the dashboard."""
    rows = load(limit=limit * 6)
    latest: dict = {}
    for e in rows:
        key = (e.get("round_id"), e.get("slug"), e.get("side"))
        if key[0] is None:
            continue
        prev = latest.get(key)
        if prev is None or (e.get("ts") or 0) >= (prev.get("ts") or 0):
            latest[key] = e
    counts: dict = {}
    for e in latest.values():
        counts[e["state"]] = counts.get(e["state"], 0) + 1
    return {
        "orders": len(latest),
        "by_state": counts,
        "venue_confirmed_fills": sum(
            v for k, v in counts.items() if k in HOLDS_POSITION),
        "simulated": counts.get(SIMULATED, 0),
        "unverified": counts.get(UNVERIFIED, 0),
        "rejected": counts.get(REJECTED, 0),
    }


def rotate() -> None:
    """Keep the event log bounded — a full volume crash-loops the bot."""
    rows = load()
    if len(rows) <= MAX_ROWS:
        return
    keep = rows[-MAX_ROWS:]
    tmp = ORDER_EVENTS + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            for r in keep:
                f.write(json.dumps(r, ensure_ascii=True, default=str) + "\n")
        os.replace(tmp, ORDER_EVENTS)
    except OSError:
        pass


if __name__ == "__main__":
    print(json.dumps(summary(), indent=2))
