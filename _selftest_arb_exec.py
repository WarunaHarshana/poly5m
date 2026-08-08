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
check("locked edge is net of both fees",
      abs(locked - (1 - 0.88 - 2 * 0.07 * 0.44)) < 1e-9, f"{locked}")

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
expected = 5 * (0.43 - 0.44) - 5 * 0.07 * 0.44
check("the loss is the spread plus the entry fee",
      abs(r.unwind_pnl - expected) < 1e-6,
      f"got {r.unwind_pnl}, expected {expected:.4f}")
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
      abs(r.realized_pnl - 5.0 * r.locked_edge) < 1e-6, str(r.realized_pnl))
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

print()
if FAIL:
    raise SystemExit(f"SELFTEST_FAILED arb_exec: {FAIL}")
print("SELFTEST_OK arb_exec")
