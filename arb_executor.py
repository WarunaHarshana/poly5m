"""Two-leg sum-to-one arb execution that is safe to run with real money.

WHY ARB AND NOT THE OTHER STRATEGIES
------------------------------------
Measured over the observation dataset this bot collected (740 priced, labeled
slots) plus the recorded order books:

    momentum        no entry-price band shows a significant positive net edge.
                    Across every move bucket the win rate matches the ask to
                    within ~1pp BEFORE fees — the book prices these moves
                    correctly, so a taker just pays the fee. Nothing to tune.
    market making   -0.244/slot over 130 recorded slots; adverse selection
                    (20% one-sided fills) eats the spread.
    sum-to-one arb  15/15 wins, +4.57% ROI, t=+5.58.

Arb is the only one that works, and it is the only one that does not predict
anything: when ask_UP + ask_DOWN < $1, one leg pays $1/share at resolution
whichever way the slot lands, so the profit is locked at entry.

THE LEG RISK, AND WHY FOK SOLVES IT
-----------------------------------
The whole edge depends on owning BOTH sides. Fill one leg and you hold a naked
directional position in a market that resolves in minutes — strictly worse than
the momentum strategy that was just shown to have no edge. That risk is why arb
was execution-disabled in live mode.

Polymarket's CLOB supports FOK (fill-or-kill): the entire size fills
immediately or nothing does. That removes partial fills, leaving exactly one
residual risk — leg A fills and leg B is killed. This module bounds that:

  1. Quote both sides and verify the pair is profitable after fees.
  2. Send the HARDER leg first (thinner depth), as FOK. If it is killed,
     nothing has been bought and the attempt costs nothing.
  3. Send the second leg FOK. If it fills, profit is locked.
  4. If the second leg is killed, immediately unwind the first at the bid
     (FAK sell). The loss is the spread crossed twice on one leg — small,
     bounded, and recorded so the real unwind rate can be measured.

Nothing here decides to trade real money. It executes what the caller asked
for, refuses when the economics do not hold, and reports what happened.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

import config
import paths

ARCHIVE_DIR = paths.archive_dir()
ARB_LOG = os.path.join(ARCHIVE_DIR, "arb_executions.jsonl")


@dataclass
class LegResult:
    side: str
    token_id: str
    price: float
    size: float
    filled: bool
    order_id: str | None = None
    error: str | None = None


@dataclass
class ArbResult:
    ok: bool
    reason: str
    pair_cost: float = 0.0
    locked_edge: float = 0.0
    shares: float = 0.0
    legs: list = field(default_factory=list)
    unwound: bool = False
    unwind_pnl: float = 0.0
    realized_pnl: float = 0.0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["legs"] = [asdict(l) if hasattr(l, "__dataclass_fields__") else l
                     for l in self.legs]
        return d


def _log(entry: dict) -> None:
    entry.setdefault("ts", time.time())
    entry.setdefault("time_utc", datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"))
    try:
        with open(ARB_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=True, default=str) + "\n")
    except OSError:
        pass


def taker_fee_per_share(price: float) -> float:
    """Delegates to config so there is ONE fee curve in the codebase.

    This module used to carry its own copy, which meant a fix in config would
    silently leave the arb gate pricing on the old (wrong) curve — the gate
    that decides whether real money goes in.
    """
    return config.taker_fee_per_share(price)


def evaluate(ask_up: float, ask_down: float, shares: float,
             min_locked_edge: float | None = None) -> tuple[bool, float, str]:
    """Is this pair still worth taking once both legs pay taker fees?

    Returns (viable, locked_edge_per_share, reason). The pair must pay for
    itself AFTER fees on both legs — a raw sum below $1 is not enough, which is
    why several live signals were correctly skipped.
    """
    if min_locked_edge is None:
        min_locked_edge = float(getattr(config, "ARB_MIN_LOCKED_EDGE", 0.02))
    if ask_up <= 0 or ask_down <= 0:
        return False, 0.0, "missing quote on one side"
    pair = float(ask_up) + float(ask_down)
    fees = taker_fee_per_share(ask_up) + taker_fee_per_share(ask_down)
    # One leg pays $1/share at resolution whichever way it lands.
    locked = 1.0 - pair - fees
    if locked < min_locked_edge:
        return False, locked, (
            f"locked {locked:+.4f}/share < {min_locked_edge:.4f} "
            f"(pair ${pair:.3f} + fees ${fees:.3f})")
    max_edge = float(getattr(config, "ARB_MAX_EDGE", 0.15) or 0)
    if max_edge > 0 and (1.0 - pair) > max_edge:
        return False, locked, (
            f"raw gap {1.0 - pair:.3f} > {max_edge:.2f} — a book that wide is "
            f"makers pulling quotes, not a fillable arb")
    return True, locked, "ok"


def _verify_filled(client, order_id, shares) -> bool | None:
    """Ask the venue whether `order_id` actually matched. None = unknowable.

    Used only when a FOK response cannot be interpreted. Costs a round trip,
    which is exactly the right trade against acting on a guess about whether we
    are holding a position.
    """
    if not order_id:
        return False
    try:
        c = client._client()
        trades = c.get_trades()
    except Exception:
        return None
    if not isinstance(trades, list):
        return None
    total = 0.0
    for tr in trades:
        try:
            if tr.get("taker_order_id") == order_id:
                total += float(tr.get("size") or 0.0)
            else:
                for mo in (tr.get("maker_orders") or []):
                    if mo.get("order_id") == order_id:
                        total += float(mo.get("matched_amount") or 0.0)
        except (TypeError, ValueError, AttributeError):
            continue
    return total + 1e-9 >= float(shares)


def _read_fill(client, resp, shares) -> tuple[bool, object, str | None]:
    """Decide, conservatively, whether an order response means MATCHED.

    Shared by the entry legs and the unwind, because the question and the
    danger are identical: believing an order filled when it did not. On the way
    IN that sends the second leg against a hedge that does not exist; on the way
    OUT it reports a naked position as closed and clears the alert that was the
    last line of defence.

    Returns ``(filled, order_id, error)``. A fill must be POSITIVELY confirmed;
    anything ambiguous is resolved against the venue, and anything still
    unresolved after that counts as NOT filled.
    """
    oid = None
    status = ""
    matched = None
    if isinstance(resp, dict):
        oid = resp.get("orderID") or resp.get("orderId") or resp.get("id")
        status = str(resp.get("status") or resp.get("state") or "").lower()
        # Only fields whose UNITS are known. `makingAmount` and friends are
        # conventionally raw token base units (6-decimal USDC), so a $5 order
        # would report 5000000 and `5000000 >= 5 shares` would read as a full
        # fill on an order that never matched. matched_amount is the one
        # corroborated elsewhere in this codebase (bot.py's live fill check
        # reads it from get_trades), and a value that cannot be a plausible
        # share count is discarded rather than trusted.
        for k in ("matched_amount", "size_matched", "filled_size"):
            if resp.get(k) is not None:
                try:
                    val = float(resp[k])
                except (TypeError, ValueError):
                    continue
                if 0 <= val <= max(float(shares) * 100.0, 1000.0):
                    matched = val
                    break
        if resp.get("success") is False or resp.get("errorMsg"):
            return False, oid, str(resp.get("errorMsg") or "rejected")

    # Only words that unambiguously mean MATCHED. "success" and "complete" were
    # here and had to go: on most order APIs "success" means the request was
    # accepted, not that anything traded — which is precisely the assume-a-fill
    # bug this function exists to prevent. Anything else falls through to
    # _verify_filled, which asks the venue.
    POSITIVE = ("matched", "filled")
    NEGATIVE = ("unmatched", "cancelled", "canceled", "killed", "rejected",
                "expired", "failed")
    if matched is not None:
        return (matched + 1e-9 >= float(shares)), oid, None
    if status in NEGATIVE:
        return False, oid, None
    if status in POSITIVE:
        return True, oid, None
    # Unknown shape. Ask the venue what actually happened rather than picking
    # the convenient answer.
    filled = _verify_filled(client, oid, shares)
    if filled is None:
        return False, oid, (f"unverifiable response (status={status!r}); "
                            f"treated as UNFILLED")
    return bool(filled), oid, None


def execute(client, market: dict, ask_up: float, ask_down: float,
            shares: float, depth_up: float | None = None,
            depth_down: float | None = None,
            bid_up: float | None = None, bid_down: float | None = None,
            dry_run: bool = False, context: dict | None = None) -> ArbResult:
    """Buy both legs atomically, or end up holding nothing.

    ``client`` needs .buy(token_id, price, size, condition_id, order_type) and
    .sell(token_id, price, size, condition_id, order_type) for the unwind.

    ``context`` is merged into every log line — the caller knows things this
    module does not (how much slot was left, whether the quote came from the
    socket or REST) and the tuner cannot learn a gate whose value was never
    recorded next to the outcome it produced.
    """
    ctx = dict(context or {})

    def _emit(entry: dict) -> None:
        _log({**ctx, **entry})
    viable, locked, why = evaluate(ask_up, ask_down, shares)
    pair = float(ask_up) + float(ask_down)
    if not viable:
        res = ArbResult(ok=False, reason=why, pair_cost=pair,
                        locked_edge=locked, shares=shares)
        _emit({"action": "reject", **res.to_dict()})
        return res

    tokens = market.get("tokens") or {}
    cond = market.get("conditionId") or ""

    # Send the leg least likely to fill FIRST. If it is killed we have bought
    # nothing and the attempt is free; discovering the problem on the SECOND
    # leg is what costs an unwind.
    legs = [("UP", tokens.get("UP"), float(ask_up), depth_up, bid_up),
            ("DOWN", tokens.get("DOWN"), float(ask_down), depth_down, bid_down)]
    legs.sort(key=lambda l: (l[3] if l[3] is not None else float("inf")))
    first, second = legs[0], legs[1]

    if dry_run:
        res = ArbResult(ok=True, reason="dry-run", pair_cost=pair,
                        locked_edge=locked, shares=shares,
                        legs=[LegResult(first[0], first[1], first[2], shares, True),
                              LegResult(second[0], second[1], second[2], shares, True)])
        return res

    def _fok_buy(side, token, price):
        """Send one FOK leg and decide, conservatively, whether it filled."""
        try:
            resp = client.buy(token, price, shares, cond, order_type="FOK")
        except Exception as e:
            return LegResult(side, token, price, shares, False, error=str(e))
        filled, oid, err = _read_fill(client, resp, shares)
        return LegResult(side, token, price, shares, filled,
                         order_id=oid, error=err)

    leg1 = _fok_buy(first[0], first[1], first[2])
    if not leg1.filled:
        res = ArbResult(ok=False, reason=f"first leg ({first[0]}) not filled; "
                                         f"nothing bought, no risk taken",
                        pair_cost=pair, locked_edge=locked, shares=shares,
                        legs=[leg1])
        _emit({"action": "first_leg_killed", **res.to_dict()})
        return res

    leg2 = _fok_buy(second[0], second[1], second[2])
    if leg2.filled:
        realized = round(shares * locked, 4)
        res = ArbResult(ok=True, reason="both legs filled — profit locked",
                        pair_cost=pair, locked_edge=locked, shares=shares,
                        legs=[leg1, leg2], realized_pnl=realized)
        _emit({"action": "locked", **res.to_dict()})
        return res

    # Second leg killed: we are holding a naked directional position. Get out
    # now — an unhedged position in a market resolving in minutes is exactly
    # the risk this design exists to avoid.
    # Sell into the real bid when the caller quoted one. The fallback below is
    # a GUESS at where the book is, and a FAK priced above the true bid matches
    # nothing — so it is a last resort, not a default.
    unwind_px = first[4] if first[4] is not None else max(0.01, first[2] - 0.02)
    px_src = "book bid" if first[4] is not None else "ESTIMATED (no bid quoted)"
    unwind_pnl = 0.0
    unwound = False

    def _naked(why: str) -> ArbResult:
        r = ArbResult(ok=False,
                      reason=f"second leg killed AND unwind {why} — "
                             f"HOLDING NAKED {first[0]} {shares} shares",
                      pair_cost=pair, locked_edge=locked, shares=shares,
                      legs=[leg1, leg2])
        _emit({"action": "unwind_failed", "severity": "critical", **r.to_dict()})
        return r

    try:
        sell_resp = client.sell(first[1], float(unwind_px), shares, cond,
                                order_type="FAK")
    except Exception as e:
        return _naked(f"failed ({e})")

    # A FAK that matches nothing returns NORMALLY — no exception, no error.
    # Reading "the call returned" as "the position is closed" is the same
    # assume-a-fill bug the entry legs guard against, on the side where it is
    # worst: it books a loss, clears the alert, and leaves a real naked leg
    # riding into a resolution minutes away.
    sold, _sell_oid, sell_err = _read_fill(client, sell_resp, shares)
    if not sold:
        return _naked(f"did not fill at {unwind_px:.3f} [{px_src}]"
                      + (f" ({sell_err})" if sell_err else ""))

    unwound = True
    # Bought at the ask, sold at the bid: the loss is the spread plus the
    # fee already paid on the way in.
    unwind_pnl = round(shares * (float(unwind_px) - first[2])
                       - shares * taker_fee_per_share(first[2]), 4)

    res = ArbResult(ok=False,
                    reason=f"second leg ({second[0]}) killed; unwound {first[0]}",
                    pair_cost=pair, locked_edge=locked, shares=shares,
                    legs=[leg1, leg2], unwound=unwound,
                    unwind_pnl=unwind_pnl, realized_pnl=unwind_pnl)
    _emit({"action": "unwound", **res.to_dict()})
    return res


def open_alerts() -> list[dict]:
    """Unresolved unwind failures — each is a naked position, or was.

    This is the only path in the design that can leave real directional
    exposure on the book: the second leg was killed AND the unwind order also
    failed. It must never be something you discover later in a log file.
    """
    rows = []
    if os.path.isfile(ARB_LOG):
        try:
            with open(ARB_LOG, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            rows.append(json.loads(line))
                        except ValueError:
                            continue
        except OSError:
            return []
    acked = {r.get("acked_ts") for r in rows if r.get("action") == "ack"}
    return [r for r in rows
            if r.get("action") == "unwind_failed" and r.get("ts") not in acked]


def acknowledge(ts: float) -> None:
    """Mark one unwind failure as handled (position closed by hand)."""
    _log({"action": "ack", "acked_ts": ts,
          "note": "operator confirmed the naked position is resolved"})


def stats() -> dict:
    """Measured execution record — the numbers that decide if this is fundable."""
    rows = []
    if os.path.isfile(ARB_LOG):
        with open(ARB_LOG, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except ValueError:
                        continue
    attempts = [r for r in rows if r.get("action") in
                ("locked", "unwound", "first_leg_killed", "unwind_failed")]
    locked = [r for r in attempts if r["action"] == "locked"]
    unwound = [r for r in attempts if r["action"] == "unwound"]
    killed = [r for r in attempts if r["action"] == "first_leg_killed"]
    failed = [r for r in attempts if r["action"] == "unwind_failed"]
    pnl = sum(float(r.get("realized_pnl") or 0.0) for r in attempts)
    return {
        "attempts": len(attempts),
        "both_legs_locked": len(locked),
        "second_leg_killed_and_unwound": len(unwound),
        "first_leg_killed_no_risk": len(killed),
        "unwind_failures": len(failed),
        "lock_rate_pct": round(100.0 * len(locked) / len(attempts), 1)
        if attempts else 0.0,
        "net_pnl": round(pnl, 4),
    }


if __name__ == "__main__":
    print(json.dumps(stats(), indent=2))
