"""Offline self-test for the tuner (no network, isolated state).

The tests that matter here are the negative ones. Any optimiser will find a
better parameter set on data with a real effect; the question is whether this
one keeps its hands off data that is pure noise, because that is the failure
mode that quietly loses money.

  T1  signal present   -> the tuner moves toward the profitable region
  T2  pure noise       -> the tuner overwhelmingly declines to change anything
  T3  step limit       -> no parameter jumps more than one ladder notch
  T4  sample floors    -> too little data is a hold, never a guess
  T5  auto-revert      -> a change followed by real losses is rolled back
"""
import json
import os
import random
import tempfile

os.environ["STATE_ROOT"] = tempfile.mkdtemp(prefix="selftest_tuner_")

import observations  # noqa: E402  (must follow STATE_ROOT)
import tuner  # noqa: E402

FAIL = []


def check(name, cond, detail=""):
    if cond:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}  {detail}")
        FAIL.append(name)


def make_row(ts, ask, move, won, remaining=20.0, edge=0.0, chop=0):
    """One labeled observation. `won` is the counterfactual outcome."""
    side = "UP" if move >= 0 else "DOWN"
    return {
        "ts": ts, "slug": f"slot-{int(ts)}", "end_ts": ts + 30,
        "remaining_s": remaining, "move_usd": move, "abs_move": abs(move),
        "side": side, "ask": ask, "edge": edge, "edge_net": edge - 0.02,
        "chop_crosses": chop, "decision": "skip", "skip_reason": "synthetic",
        "outcome": side if won else ("DOWN" if side == "UP" else "UP"),
        "won_if_taken": won,
    }


def write_rows(rows):
    if os.path.exists(observations.OBSERVATIONS_JSONL):
        os.remove(observations.OBSERVATIONS_JSONL)
    for r in rows:
        observations.record(r)


def reset_tuned():
    for p in (tuner.TUNED_PATH, tuner.TUNING_LOG):
        if os.path.exists(p):
            os.remove(p)


# ---------------------------------------------------------------------------
# T1: a real effect must be found
# ---------------------------------------------------------------------------
# Cheap entries win often, expensive ones are coin flips. At ask 0.86 a 50%
# win rate loses badly, so the tuner should pull MAX_ENTRY down.
def gen_signal(n, seed):
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        ask = rng.choice([0.58, 0.62, 0.66, 0.70, 0.78, 0.82, 0.86])
        p_win = 0.88 if ask <= 0.70 else 0.50
        rows.append(make_row(1_000_000 + i * 300, ask,
                             rng.choice([15.0, -15.0, 22.0, -22.0]),
                             rng.random() < p_win))
    return rows


print("T1 signal present")
reset_tuned()
write_rows(gen_signal(1200, seed=7))
res = tuner.run(verbose=False)
check("tuner applies a change when the effect is real",
      res["action"] == "apply", json.dumps(res, default=str)[:300])
if res["action"] == "apply":
    check("it tightened the entry cap toward the profitable region",
          res["params"]["LATE_MOMENTUM_MAX_ENTRY"] < 0.88,
          str(res["params"]))
    check("holdout expectancy is positive after the change",
          res["holdout"]["expectancy"] > 0, str(res["holdout"]))

# ---------------------------------------------------------------------------
# T3: step limit (uses the same run as T1)
# ---------------------------------------------------------------------------
print("T3 step limit")
if res.get("action") == "apply":
    ok = True
    detail = ""
    for key, (old, new) in res["changes"].items():
        ladder = tuner.LADDERS[key]
        if old in ladder and new in ladder:
            if abs(ladder.index(new) - ladder.index(old)) > tuner.MAX_STEPS_PER_RUN:
                ok = False
                detail = f"{key} {old}->{new} jumped too far"
    check("no parameter moved more than one notch", ok, detail)
else:
    check("no parameter moved more than one notch", False, "T1 did not apply")

# ---------------------------------------------------------------------------
# T2: noise must not move anything (the one that really matters)
# ---------------------------------------------------------------------------
def gen_noise(n, seed):
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        ask = rng.choice([0.52, 0.58, 0.64, 0.70, 0.76, 0.82, 0.86, 0.90])
        # Outcome independent of every feature, and priced fairly: the win
        # probability IS the ask, so no threshold can create edge.
        rows.append(make_row(1_000_000 + i * 300, ask,
                             rng.choice([12.0, -12.0, 18.0, -18.0]),
                             rng.random() < ask))
    return rows


print("T2 pure noise (20 seeds)")
applies = []
for seed in range(20):
    reset_tuned()
    write_rows(gen_noise(1200, seed=100 + seed))
    r = tuner.run(verbose=False)
    if r["action"] == "apply":
        applies.append((seed, r.get("changes")))
rate = len(applies) / 20.0
check(f"false-positive rate {rate:.0%} is at most 10%", rate <= 0.10,
      f"applied on seeds {[s for s, _ in applies]}")

# ---------------------------------------------------------------------------
# T4: not enough data is a hold
# ---------------------------------------------------------------------------
print("T4 sample floors")
reset_tuned()
write_rows(gen_signal(50, seed=3))
r = tuner.run(verbose=False)
check("too few labeled slots -> wait, not a guess", r["action"] == "wait",
      str(r))

# A dataset large enough to pass MIN_LABELED_ROWS but whose candidates cannot
# reach the per-slice trade floors must still hold.
reset_tuned()
rows = gen_signal(400, seed=4)
for row in rows:          # every slot below any tradeable move threshold
    row["move_usd"] = 0.5
    row["abs_move"] = 0.5
write_rows(rows)
r = tuner.run(verbose=False)
# Starved of trades is "wait", NOT "hold". They look identical from outside —
# both leave parameters untouched — but only "hold" is a real judgement, and
# reporting a starved run as one made the supervisor sleep a full day on a
# holdout of 9 trades.
check("no eligible candidate -> wait, not a considered hold",
      r["action"] == "wait", str(r)[:200])
check("the starved result says why", "not enough trades" in (r.get("reason") or ""),
      str(r.get("reason"))[:160])

# ---------------------------------------------------------------------------
# T5: auto-revert
# ---------------------------------------------------------------------------
print("T5 auto-revert")
reset_tuned()
prev = dict(tuner.DEFAULTS)
bad = dict(prev)
bad["LATE_MOMENTUM_MAX_ENTRY"] = 0.90
tuner._write_tuned(bad, {"previous_params": prev, "reason": "selftest"})
tuned = tuner.load_tuned()

# 60 real trades since the change, all losers at a rich entry.
changed_at = float(tuned["updated_at"])
losing = []
for i in range(60):
    row = make_row(changed_at + 60 + i * 300, 0.89, 20.0, won=False)
    row["decision"] = "trade"
    losing.append(row)
write_rows(losing)
rev = tuner.check_revert(min_trades=25)
check("a losing change is rolled back", rev is not None and rev["action"] == "revert",
      str(rev)[:200])
check("parameters returned to the previous set",
      tuner.active_params()["LATE_MOMENTUM_MAX_ENTRY"] == prev["LATE_MOMENTUM_MAX_ENTRY"],
      str(tuner.active_params()))

# A profitable change must NOT be reverted.
reset_tuned()
tuner._write_tuned(bad, {"previous_params": prev, "reason": "selftest"})
changed_at = float(tuner.load_tuned()["updated_at"])
winning = []
for i in range(60):
    row = make_row(changed_at + 60 + i * 300, 0.60, 20.0, won=(i % 4 != 0))
    row["decision"] = "trade"
    winning.append(row)
write_rows(winning)
check("a profitable change is left alone", tuner.check_revert(min_trades=25) is None)

print()
if FAIL:
    raise SystemExit(f"SELFTEST_FAILED tuner: {FAIL}")
print("SELFTEST_OK tuner")
