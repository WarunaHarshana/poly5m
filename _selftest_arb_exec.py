"""Offline self-test for live-safe arb execution (no network).

The happy path is the easy part. What decides whether this is safe to fund is
the failure paths, so those are what this exercises hardest:

  T1  economics   a pair that does not clear fees after BOTH legs is refused
  T2  ordering    the thinner leg is attempted first, so a kill costs nothing
  T3  first kill  nothing bought, nothing risked
  T4  second kill the filled leg is unwound immediately, loss bounded
  T5  unwind fail reported as CRITICAL rather than quietly swallowed
  T6  happy path  both legs fill, profit locked at entry
  T7  accounting  the measured record adds up
"""
import json
import os
import tempfile

os.environ["STATE_ROOT"] = tempfile.mkdtemp(prefix="selftest_arb_")

import config  # noqa: E402

config.TAKER_FEE_RATE = 0.07
config.ARB_MIN_LOCKED_EDGE = 0.02
config.ARB_MAX_EDGE = 0.15

import arb_executor as ax  # noqa: E402

FAIL = []


def check(name, cond, detail=""):
    if cond:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}  {detail}")
        FAIL.append(name)


MARKET = {"tokens": {"UP": "TOK_UP", "DOWN": "TOK_DN"}, "conditionId": "0xc"}


class FakeClient:
    """Scriptable CLOB. `fills` maps token -> whether its FOK matches."""
    def __init__(self, fills, sell_raises=False):
        self.fills = fills
        self.sell_raises = sell_raises
        self.buys = []
        self.sells = []

    def buy(self, token_id, price, size, condition_id, order_type="GTC"):
        self.buys.append((token_id, price, size, order_type))
        if not self.fills.get(token_id, True):
            return {"success": True, "status": "unmatched"}
        return {"success": True, "status": "matched", "orderID": f"o-{token_id}"}

    def sell(self, token_id, price, size, condition_id, order_type="FAK"):
        self.sells.append((token_id, price, size, order_type))
        if self.sell_raises:
            raise RuntimeError("book vanished")
        return {"success": True, "status": "matched"}


# ---------------------------------------------------------------------------
print("T1 economics")
# Pair 0.98 looks free, but two taker fees on it cost more than the 2c gap.
ok, locked, why = ax.evaluate(0.49, 0.49, 5.0)
check("a pair that fees eat is refused", not ok, f"locked={locked:.4f} {why}")
check("the refusal explains itself", "locked" in why, why)

ok, locked, _ = ax.evaluate(0.44, 0.44, 5.0)
check("a genuinely profitable pair is accepted", ok, f"locked={locked:.4f}")
# Polymarket's documented curve: rate * p * (1 - p) per share, per leg.
FEE_44 = 0.07 * 0.44 * 0.56
check("locked edge is net of both fees",
      abs(locked - (1 - 0.88 - 2 * FEE_44)) < 1e-9, f"{locked}")

ok, _, why = ax.evaluate(0.40, 0.40, 5.0)
check("an implausibly wide gap is refused as unfillable", not ok, why)

# ---------------------------------------------------------------------------
print("T2 ordering: hardest leg first")
c = FakeClient({"TOK_UP": True, "TOK_DN": True})
ax.execute(c, MARKET, 0.44, 0.44, 5.0, depth_up=500.0, depth_down=7.0,
           bid_up=0.43, bid_down=0.43)
check("the thinner side is attempted first",
      c.buys[0][0] == "TOK_DN", str(c.buys))
check("both legs go out as FOK",
      all(b[3] == "FOK" for b in c.buys), str(c.buys))

# ---------------------------------------------------------------------------
print("T3 first leg killed")
c = FakeClient({"TOK_UP": True, "TOK_DN": False})
r = ax.execute(c, MARKET, 0.44, 0.44, 5.0, depth_up=500.0, depth_down=7.0,
               bid_up=0.43, bid_down=0.43)
check("the attempt reports failure", not r.ok, r.reason)
check("only one order was ever sent", len(c.buys) == 1, str(c.buys))
check("nothing was sold (nothing was held)", not c.sells, str(c.sells))
check("no pnl either way", r.realized_pnl == 0.0, str(r.realized_pnl))

# ---------------------------------------------------------------------------
print("T4 second leg killed -> unwind")
c = FakeClient({"TOK_DN": True, "TOK_UP": False})
r = ax.execute(c, MARKET, 0.44, 0.44, 5.0, depth_up=500.0, depth_down=7.0,
               bid_up=0.43, bid_down=0.43)
check("reported as not locked", not r.ok, r.reason)
check("the stranded leg was unwound", r.unwound and len(c.sells) == 1,
      str(c.sells))
check("the unwind sells the leg that actually filled",
      c.sells[0][0] == "TOK_DN", str(c.sells))
check("the unwind is FAK, not a resting order",
      c.sells[0][3] == "FAK", str(c.sells))
# Bought 5 @ 0.44, sold 5 @ 0.43, plus the entry fee already paid.
# The executor rounds booked figures to 4dp, so compare on the same basis.
expected = round(5 * (0.43 - 0.44) - 5 * FEE_44, 4)
check("the loss is the spread plus the entry fee",
      abs(r.unwind_pnl - expected) < 1e-9,
      f"got {r.unwind_pnl}, expected {expected}")
check("the bounded loss is small versus the locked edge it chased",
      abs(r.unwind_pnl) < 1.0, str(r.unwind_pnl))

# ---------------------------------------------------------------------------
print("T5 unwind failure is loud")
c = FakeClient({"TOK_DN": True, "TOK_UP": False}, sell_raises=True)
r = ax.execute(c, MARKET, 0.44, 0.44, 5.0, depth_up=500.0, depth_down=7.0,
               bid_up=0.43, bid_down=0.43)
check("failure to unwind is reported", not r.ok and not r.unwound, r.reason)
check("the reason names the naked position", "NAKED" in r.reason, r.reason)
rows = [json.loads(l) for l in open(ax.ARB_LOG, encoding="utf-8") if l.strip()]
check("it is journalled as critical",
      any(x.get("severity") == "critical" for x in rows), "no critical entry")

# ---------------------------------------------------------------------------
print("T6 happy path")
c = FakeClient({"TOK_UP": True, "TOK_DN": True})
r = ax.execute(c, MARKET, 0.44, 0.44, 5.0, depth_up=500.0, depth_down=7.0,
               bid_up=0.43, bid_down=0.43)
check("both legs filled", r.ok, r.reason)
check("profit is locked at entry, not predicted",
      abs(r.realized_pnl - round(5.0 * r.locked_edge, 4)) < 1e-9,
      f"{r.realized_pnl} vs {round(5.0 * r.locked_edge, 4)}")
check("the locked profit is positive", r.realized_pnl > 0, str(r.realized_pnl))
check("no unwind was needed", not c.sells or len(c.sells) == 1, str(c.sells))

# ---------------------------------------------------------------------------
print("T7 measured record")
s = ax.stats()
check("attempts are counted", s["attempts"] >= 4, str(s))
check("locked and unwound are tracked separately",
      s["both_legs_locked"] >= 1 and s["second_leg_killed_and_unwound"] >= 1,
      str(s))
check("a no-risk first-leg kill is not counted as a loss",
      s["first_leg_killed_no_risk"] >= 1, str(s))
check("unwind failures are surfaced", s["unwind_failures"] >= 1, str(s))

# ---------------------------------------------------------------------------
print("T8 naked-position alerting")
alerts = ax.open_alerts()
check("the unwind failure raises an open alert", len(alerts) == 1, str(len(alerts)))
a = alerts[0]
held = [l for l in (a.get("legs") or []) if l.get("filled")]
check("the alert says which leg is actually held", len(held) == 1, str(a.get("legs")))
check("it carries the size and price to close",
      held[0].get("size") == 5.0 and held[0].get("price") == 0.44, str(held))

# A resolved one must stop nagging, but only once acknowledged.
still = ax.open_alerts()
check("it keeps alerting until acknowledged", len(still) == 1, str(len(still)))
ax.acknowledge(a["ts"])
check("acknowledging clears it", ax.open_alerts() == [], str(ax.open_alerts()))

# A clean run must never raise a false alarm.
c = FakeClient({"TOK_UP": True, "TOK_DN": True})
ax.execute(c, MARKET, 0.44, 0.44, 5.0, depth_up=500.0, depth_down=7.0,
           bid_up=0.43, bid_down=0.43)
check("a successful lock raises no alert", ax.open_alerts() == [],
      str(ax.open_alerts()))

# ---------------------------------------------------------------------------
print("T9 a fill must be POSITIVELY confirmed")
# Believing an unfilled leg filled is the worst error available here: it sends
# the second leg against a hedge that does not exist. An earlier version used
# bool(resp.get("success", True)) plus a deny-list of statuses, so any
# unanticipated response shape counted as a fill.
class _RespClient(FakeClient):
    """Returns a scripted response shape for the first leg."""
    def __init__(self, resp, trades=None):
        super().__init__({})
        self.resp = resp
        self._trades = trades

    def buy(self, token_id, price, size, condition_id, order_type="GTC"):
        self.buys.append((token_id, price, size, order_type))
        return self.resp

    def _client(self):
        outer = self

        class _C:
            def get_trades(self):
                if outer._trades is None:
                    raise RuntimeError("CLOB unreachable")
                return outer._trades
        return _C()


def first_leg_filled(resp, trades=None):
    c = _RespClient(resp, trades)
    r = ax.execute(c, MARKET, 0.44, 0.44, 5.0, depth_up=500.0, depth_down=7.0,
                   bid_up=0.43, bid_down=0.43)
    return (r.legs[0].filled if r.legs else None), r


f1, _ = first_leg_filled({"status": "matched", "orderID": "o1"})
check("an explicitly matched order counts as filled", f1 is True, str(f1))

f2, _ = first_leg_filled({"status": "unmatched", "orderID": "o2"})
check("an explicitly unmatched order does NOT", f2 is False, str(f2))

f3, _ = first_leg_filled({"success": False, "errorMsg": "insufficient balance"})
check("an explicit rejection does NOT", f3 is False, str(f3))

# The dangerous one: a shape nobody anticipated.
f4, r4 = first_leg_filled({"orderID": "o4", "status": "pending_new"},
                          trades=[])
check("an UNKNOWN status is not assumed filled", f4 is False, str(f4))

# ...but if the venue confirms the match, believe the venue.
f5, _ = first_leg_filled({"orderID": "o5", "status": "weird_new_status"},
                         trades=[{"taker_order_id": "o5", "size": 5.0}])
check("an unknown status IS filled when the venue confirms it", f5 is True,
      str(f5))

# ...and if the venue cannot be reached, refuse to guess.
f6, r6 = first_leg_filled({"orderID": "o6", "status": "???"}, trades=None)
check("an unverifiable response is treated as unfilled", f6 is False, str(f6))
check("...and says so", "unverifiable" in (r6.legs[0].error or ""),
      str(r6.legs[0].error))

# A partial match on a FOK is not a fill.
f7, _ = first_leg_filled({"orderID": "o7", "matched_amount": 3.0})
check("a partial match is not a fill", f7 is False, str(f7))
f8, _ = first_leg_filled({"orderID": "o8", "matched_amount": 5.0})
check("a full match is a fill", f8 is True, str(f8))

# "success" means the REQUEST succeeded on most order APIs, not that anything
# traded. Treating it as a fill is the exact bug this function prevents.
f9, _ = first_leg_filled({"orderID": "o9", "status": "success"}, trades=[])
check("'success' is NOT taken as a fill", f9 is False, str(f9))

# A raw-token-units amount (6-decimal USDC) must not read as a share count:
# 5000000 >= 5 would otherwise fake a fill on an order that never matched.
f10, _ = first_leg_filled({"orderID": "o10", "matched_amount": 5000000.0},
                          trades=[])
check("an implausible matched size is rejected, not trusted", f10 is False,
      str(f10))

# ---------------------------------------------------------------------------
print("T10 an unwind that matches nothing is NOT a successful unwind")
# A FAK that matches nothing returns NORMALLY. If "the call returned" counts as
# "the position is closed", the bot books a small loss, clears its own alert,
# and rides a naked directional leg into a resolution minutes away — the exact
# risk this module exists to prevent.
class _SellRespClient(FakeClient):
    """Fills the first leg, kills the second, and scripts the unwind reply."""
    def __init__(self, sell_resp, trades=None):
        super().__init__({"TOK_DN": True, "TOK_UP": False})
        self.sell_resp = sell_resp
        self._trades = trades

    def sell(self, token_id, price, size, condition_id, order_type="FAK"):
        self.sells.append((token_id, price, size, order_type))
        return self.sell_resp

    def _client(self):
        outer = self

        class _C:
            def get_trades(self):
                if outer._trades is None:
                    raise RuntimeError("CLOB unreachable")
                return outer._trades
        return _C()


def _ack_all():
    for a in ax.open_alerts():
        ax.acknowledge(a["ts"])


c = _SellRespClient({"success": True, "status": "unmatched"})
r = ax.execute(c, MARKET, 0.44, 0.44, 5.0, depth_up=500.0, depth_down=7.0,
               bid_up=0.43, bid_down=0.43)
check("a killed unwind is not reported as unwound", not r.unwound, str(r.unwound))
check("it is reported as a naked position", "NAKED" in r.reason, r.reason)
check("no pnl is booked for a sale that never happened",
      r.unwind_pnl == 0.0 and r.realized_pnl == 0.0,
      f"{r.unwind_pnl} / {r.realized_pnl}")
check("the reason names the price it tried", "0.430" in r.reason, r.reason)
check("it raises an open alert", len(ax.open_alerts()) == 1,
      str(len(ax.open_alerts())))
_ack_all()

# An unwind the venue will not confirm is naked until proven otherwise.
c = _SellRespClient({"orderID": "s1", "status": "pending_new"}, trades=[])
r = ax.execute(c, MARKET, 0.44, 0.44, 5.0, depth_up=500.0, depth_down=7.0,
               bid_up=0.43, bid_down=0.43)
check("an unconfirmed unwind is treated as naked, not closed",
      not r.unwound and "NAKED" in r.reason, r.reason)
_ack_all()

# ...but a genuine confirmation from the venue still closes it cleanly.
c = _SellRespClient({"orderID": "s2", "status": "odd_shape"},
                    trades=[{"taker_order_id": "s2", "size": 5.0}])
r = ax.execute(c, MARKET, 0.44, 0.44, 5.0, depth_up=500.0, depth_down=7.0,
               bid_up=0.43, bid_down=0.43)
check("an unwind the venue confirms IS closed", r.unwound, r.reason)
check("no alert is raised for a real unwind", ax.open_alerts() == [],
      str(ax.open_alerts()))

# ---------------------------------------------------------------------------
print("T11 a missing bid is a guess, and must not masquerade as the book")
c = _SellRespClient({"success": True, "status": "unmatched"})
r = ax.execute(c, MARKET, 0.44, 0.44, 5.0, depth_up=500.0, depth_down=7.0)
check("the fallback price is flagged as estimated", "ESTIMATED" in r.reason,
      r.reason)
check("the estimate sits below the ask it bought at", "0.420" in r.reason,
      r.reason)
_ack_all()

print()
if FAIL:
    raise SystemExit(f"SELFTEST_FAILED arb_exec: {FAIL}")
print("SELFTEST_OK arb_exec")
