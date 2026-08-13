"""Offline self-test for the arb tuner (no network, isolated state).

The tests that matter here are the negative ones. Any optimiser will find a
better parameter set on data with a real effect; the question is whether this
one keeps its hands off data that is pure noise, because that is the failure
mode that quietly loses money.

  T1  refused orders   -> a venue refusal is NOT evidence about the edge
  T2  signal present   -> the tuner moves toward the profitable region
  T3  pure noise       -> the tuner overwhelmingly declines to change anything
  T4  step limit       -> no parameter jumps more than one ladder notch
  T5  sample floors    -> too little data is a wait, never a guess
  T6  naked position   -> an unwind failure is scored as the disaster it is
"""
import json
import os
import random
import tempfile

os.environ["STATE_ROOT"] = tempfile.mkdtemp(prefix="selftest_tuner_")

import tuner  # noqa: E402  (must follow STATE_ROOT)

FAIL = []


def check(name, cond, detail=""):
    if cond:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}  {detail}")
        FAIL.append(name)


def attempt(ts, pair_cost, locked_edge, action, shares=5.0, remaining=40.0,
            realized=0.0, unwind=0.0, leg_error=None, leg_price=None):
    """One row of arb_executions.jsonl as arb_executor would write it."""
    px = leg_price if leg_price is not None else pair_cost / 2.0
    return {
        "ts": ts, "action": action, "pair_cost": pair_cost,
        "locked_edge": locked_edge, "shares": shares, "remaining_s": remaining,
        "realized_pnl": realized, "unwind_pnl": unwind,
        "legs": [{"side": "UP", "price": px, "size": shares, "filled": False,
                  "error": leg_error}],
    }


def write_attempts(rows):
    os.makedirs(os.path.dirname(tuner.ARB_LOG), exist_ok=True)
    with open(tuner.ARB_LOG, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def reset_tuned():
    for p in (tuner.TUNED_PATH, tuner.TUNING_LOG):
        if os.path.exists(p):
            os.remove(p)


# ---------------------------------------------------------------------------
print("T1 an order the venue REFUSED is not evidence about the edge")
# Every live attempt up to 2026-08-13 was an HTTP 403 "trading restricted in
# your region" — eleven orders, none of which reached the book. Scored naively
# that reads as "this edge never fills", and the tuner's confident response
# would be to raise the edge floor until it stopped trading, on a dataset
# containing no market information at all.
GEO = ("PolyApiException[status_code=403, error_message={'error': 'Trading "
       "restricted in your region'}]")
write_attempts([attempt(1000 + i, 0.92, 0.045, "first_leg_killed",
                        leg_error=GEO) for i in range(11)])
rows, census = tuner.load_attempts()
check("refused attempts are excluded from the dataset", rows == [], str(len(rows)))
check("...and counted, so the operator can see why", census["refused_by_venue"] == 11,
      str(census))

reset_tuned()
res = tuner.run(verbose=False)
check("the tuner refuses to tune on them", res["action"] == "wait", str(res))
check("...and says the orders never reached the book",
      "never reached the book" in res.get("reason", ""), res.get("reason", ""))
check("no parameters were written", tuner.load_tuned() == {},
      str(tuner.load_tuned()))

# A leg with NO error reached the book and was genuinely killed — that IS data.
write_attempts([attempt(2000 + i, 0.92, 0.045, "first_leg_killed")
                for i in range(11)])
rows, census = tuner.load_attempts()
check("a killed leg (no error) IS scorable", len(rows) == 11, str(len(rows)))
check("and is not counted as refused", census["refused_by_venue"] == 0, str(census))

# ---------------------------------------------------------------------------
print("T2 a real effect must be found")
# Wide gaps are makers pulling quotes and never fill; modest gaps lock. The
# tuner should pull ARB_MAX_EDGE down to exclude the junk.
def gen_signal(n, seed):
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        raw = rng.choice([0.03, 0.05, 0.07, 0.12, 0.18, 0.25])
        pair = 1.0 - raw
        locked = raw - 0.035           # both taker fees
        if raw <= 0.07:
            # fills reliably: profit is arithmetic once locked
            rows.append(attempt(1000 + i, pair, locked, "locked",
                                realized=round(5.0 * locked, 4)))
        else:
            # degenerate book: the second leg goes, we unwind at a loss
            rows.append(attempt(1000 + i, pair, locked, "unwound",
                                unwind=-0.35))
    return rows


reset_tuned()
write_attempts(gen_signal(400, 11))
res = tuner.run(verbose=False)
check("a real effect produces a change", res["action"] == "apply", str(res)[:400])
if res["action"] == "apply":
    check("it tightened the too-good-to-be-true ceiling",
          res["params"]["ARB_MAX_EDGE"] <= tuner.DEFAULTS["ARB_MAX_EDGE"],
          str(res["params"]))

# ---------------------------------------------------------------------------
print("T3 pure noise must NOT produce a change")
# This is the test the whole design exists to pass. Before the t-stat gate,
# 25% of noise runs produced a "improvement" on both slices.
def gen_noise(n, seed):
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        raw = rng.choice([0.03, 0.05, 0.07, 0.12, 0.18, 0.25])
        pair = 1.0 - raw
        locked = raw - 0.035
        if rng.random() < 0.5:
            rows.append(attempt(1000 + i, pair, locked, "locked",
                                realized=round(5.0 * locked, 4)))
        else:
            rows.append(attempt(1000 + i, pair, locked, "unwound",
                                unwind=-round(5.0 * locked, 4)))
    return rows


changed = 0
TRIALS = 40
for s in range(TRIALS):
    reset_tuned()
    write_attempts(gen_noise(400, 500 + s))
    r = tuner.run(verbose=False)
    if r["action"] == "apply":
        changed += 1
check(f"noise almost never moves parameters ({changed}/{TRIALS})",
      changed <= 1, f"{changed} of {TRIALS} noise datasets produced a change")

# ---------------------------------------------------------------------------
print("T4 one notch per run, never a leap")
reset_tuned()
write_attempts(gen_signal(400, 11))
res = tuner.run(verbose=False)
if res.get("action") == "apply":
    for key, val in res["params"].items():
        ladder = tuner.LADDERS[key]
        moved = abs(ladder.index(val) - ladder.index(tuner.DEFAULTS[key]))
        check(f"{key} moved at most one notch", moved <= 1, f"moved {moved}")

# ---------------------------------------------------------------------------
print("T5 too little data is a wait, never a guess")
reset_tuned()
write_attempts(gen_signal(20, 3))
res = tuner.run(verbose=False)
check("a thin dataset does not tune", res["action"] == "wait", str(res)[:200])
check("no parameters were written", tuner.load_tuned() == {},
      str(tuner.load_tuned()))

# ---------------------------------------------------------------------------
print("T6 a naked position is scored as the disaster it is")
# The one outcome the whole execution design exists to avoid. If the tuner
# treats it as roughly free, it will happily learn to court it.
naked = attempt(1, 0.90, 0.05, "unwind_failed", shares=5.0, leg_price=0.45)
killed = attempt(2, 0.90, 0.05, "first_leg_killed")
locked = attempt(3, 0.90, 0.05, "locked", realized=0.25)
check("an unwind failure costs the whole leg",
      abs(tuner._attempt_pnl(naked) - (-5.0 * 0.45)) < 1e-9,
      str(tuner._attempt_pnl(naked)))
check("a first-leg kill costs nothing", tuner._attempt_pnl(killed) == 0.0,
      str(tuner._attempt_pnl(killed)))
check("a lock books its realised profit", tuner._attempt_pnl(locked) == 0.25,
      str(tuner._attempt_pnl(locked)))
check("the disaster is scored worse than the miss",
      tuner._attempt_pnl(naked) < tuner._attempt_pnl(killed), "")

print()
if FAIL:
    raise SystemExit(f"SELFTEST_FAILED tuner: {FAIL}")
print("SELFTEST_OK tuner")
