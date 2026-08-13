"""Nightly walk-forward tuner for the arb gates.

Reads the live execution log, replays the arb gate stack over it under candidate
parameter sets, and — only when the evidence clears every guardrail — writes new
parameters to ``state/tuned_params.json``, which ``config.py`` loads at import so
the next bot restart picks them up.

WHAT CHANGED WHEN MOMENTUM WAS RETIRED
--------------------------------------
The momentum tuner learned from *counterfactual* slots: it knew the outcome of
every slot it declined, because the outcome was just "did BTC go up". An arb has
no such luxury and does not need one. Once both legs fill, the payout is
arithmetic — one leg pays $1/share whichever way the slot lands — so the only
thing worth learning is **at what locked edge both legs actually fill**. That is
measured directly, per attempt, in ``archive/arb_executions.jsonl``.

So the dataset is attempts, not slots, and the question is fill rate, not
direction.

REFUSED ORDERS ARE NOT EVIDENCE
-------------------------------
An attempt whose leg carries an ``error`` was REFUSED by the venue — wrong
region, bad auth, bad size, bad tick — and never reached the matching engine. A
leg that was *killed* reaches the book, finds nothing to match, and comes back
with no error at all. Only the second kind says anything about whether an edge
was capturable.

This distinction is not academic. Every live attempt up to 2026-08-13 was an
HTTP 403 "trading restricted in your region": eleven orders, none of which ever
reached the book. Scored naively that reads as "this edge never fills at any
level", and the tuner's correct-looking response would have been to drive
ARB_MIN_LOCKED_EDGE up until it stopped trading entirely — a confident
conclusion drawn from a dataset containing no market information whatsoever.
``load_attempts`` drops them and reports the count instead.

WHY THE REST IS BUILT THE WAY IT IS
-----------------------------------
Optimising parameters on your own history is the classic way to build a bot that
looks brilliant on the data it was fitted to and loses money live. Every design
choice here exists to make that harder:

* **Chronological holdout.** Parameters are fitted on the older portion and must
  *also* win on a newer slice they never saw.
* **Coordinate ascent, not a full grid.** One dimension at a time over a handful
  of passes. A deep search on a few hundred rows finds noise.
* **Step limits.** No parameter moves more than one notch per night.
* **Minimum sample sizes.** A configuration that attempts 4 arbs in the holdout
  is not evidence, however good those 4 look.
* **Significance, not just sign.** The holdout mean must stand clear of its own
  standard error.
* **Auto-revert.** Every change is journalled; if realised results after a change
  are worse and negative, it is rolled back.

The honest limitation: this tunes *thresholds on an existing edge*. It cannot
invent one. If sum-to-one arbs do not fill at any threshold, a well-behaved
tuner's correct output is "no change" — and it will keep saying that.
"""
from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime, timezone

import paths

STATE_DIR = paths.state_dir()
ARCHIVE_DIR = paths.archive_dir()
TUNED_PATH = os.path.join(STATE_DIR, "tuned_params.json")
TUNING_LOG = os.path.join(ARCHIVE_DIR, "tuning_log.jsonl")
ARB_LOG = os.path.join(ARCHIVE_DIR, "arb_executions.jsonl")

# --- guardrails ------------------------------------------------------------
# NOTE: these count ATTEMPTS that reached the book, not slots scanned. Arbs are
# far rarer than slots, so these floors are correspondingly lower than the
# momentum tuner's — but they are still floors, and lowering them to "get a
# result" is how you get a result that means nothing.
MIN_USABLE_ROWS = int(os.getenv("TUNER_MIN_ROWS", "100"))
MIN_TRAIN_TRADES = int(os.getenv("TUNER_MIN_TRAIN_TRADES", "40"))
MIN_HOLDOUT_TRADES = int(os.getenv("TUNER_MIN_HOLDOUT_TRADES", "15"))
HOLDOUT_FRAC = float(os.getenv("TUNER_HOLDOUT_FRAC", "0.30"))
# Required improvement in expectancy ($/attempt) over the incumbent, on BOTH
# slices, before anything is written.
MIN_IMPROVEMENT = float(os.getenv("TUNER_MIN_IMPROVEMENT", "0.02"))
# How many standard errors the holdout expectancy must clear zero by. The
# coordinate search takes dozens of looks at the data, so "positive on both
# slices" happens by chance far more often than intuition suggests — measured at
# 25% of runs on synthetic pure noise with this gate absent. See
# _selftest_tuner.py, which holds the false-positive rate to the stated bound;
# lowering this without re-running that test re-opens the hole.
MIN_T_STAT = float(os.getenv("TUNER_MIN_T_STAT", "2.0"))
PASSES = int(os.getenv("TUNER_PASSES", "2"))
FEE_RATE = float(os.getenv("TUNER_FEE_RATE", "0.07"))

# The knobs the tuner is allowed to touch, each with an ordered ladder of
# candidate values. Ordered so "one notch per night" is well defined, and
# deliberately bounded — the tuner cannot propose a value not on this ladder.
LADDERS: dict[str, list] = {
    # Raw gap (1 - pair cost) required before an attempt is even considered.
    "ARB_MIN_EDGE":        [0.010, 0.015, 0.020, 0.030, 0.040, 0.050],
    # Edge left AFTER both taker fees — the number that actually has to be
    # positive for the trade to be worth doing.
    "ARB_MIN_LOCKED_EDGE": [0.005, 0.010, 0.020, 0.030, 0.040, 0.050],
    # Too-good-to-be-true ceiling: a very wide "free" gap is makers pulling
    # quotes, not a fillable arb.
    "ARB_MAX_EDGE":        [0.08, 0.10, 0.15, 0.20, 0.30],
    # Seconds of slot that must remain: below this there is not enough time to
    # get two legs done.
    "ARB_MIN_REMAINING":   [10, 15, 20, 30, 45, 60],
}
MAX_STEPS_PER_RUN = int(os.getenv("TUNER_MAX_STEPS", "1"))

DEFAULTS = {
    "ARB_MIN_EDGE": 0.02,
    "ARB_MIN_LOCKED_EDGE": 0.02,
    "ARB_MAX_EDGE": 0.15,
    "ARB_MIN_REMAINING": 20,
}


# ---------------------------------------------------------------------------
# Tuned-parameter store (read by config.py at import)
# ---------------------------------------------------------------------------
def load_tuned() -> dict:
    """Current tuned parameters, or {} when nothing has been learned yet."""
    try:
        with open(TUNED_PATH, "r", encoding="utf-8") as f:
            data = f.read()
        if not data or data[:1] == "\x00":
            return {}
        obj = json.loads(data)
    except (OSError, ValueError):
        return {}
    return obj if isinstance(obj, dict) else {}


def active_params() -> dict:
    """Effective parameter set: tuned values layered over the defaults."""
    out = dict(DEFAULTS)
    tuned = (load_tuned().get("params") or {})
    for k, v in tuned.items():
        if k in LADDERS:
            out[k] = v
    return out


def _write_tuned(params: dict, meta: dict) -> None:
    payload = {
        "updated_at": time.time(),
        "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "params": params,
        **meta,
    }
    tmp = TUNED_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, TUNED_PATH)


def _journal(entry: dict) -> None:
    entry = dict(entry)
    entry.setdefault("ts", time.time())
    entry.setdefault("time_utc", datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"))
    try:
        with open(TUNING_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=True, default=str) + "\n")
    except OSError:
        pass


def read_journal() -> list[dict]:
    rows = []
    if not os.path.isfile(TUNING_LOG):
        return rows
    try:
        with open(TUNING_LOG, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except ValueError:
                        continue
    except OSError:
        pass
    return rows


# ---------------------------------------------------------------------------
# The dataset: attempts that actually reached the book
# ---------------------------------------------------------------------------
SCORABLE_ACTIONS = ("locked", "first_leg_killed", "unwound", "unwind_failed")


def _was_refused(row: dict) -> bool:
    """True when the VENUE refused the order rather than failing to match it.

    arb_executor records the venue's own words on a leg it could not place, and
    records nothing at all on a leg that reached the book and was killed. So a
    non-empty leg error is the signal that this attempt never became a real
    market interaction — and therefore carries no information about whether the
    edge was capturable.
    """
    for leg in (row.get("legs") or []):
        if leg.get("error"):
            return True
    return False


def load_attempts(since_ts: float | None = None) -> tuple[list[dict], dict]:
    """Scorable attempts (oldest first) plus a census of what was dropped.

    The census is not decoration: when the tuner holds, the operator needs to
    know whether it held because the market said no or because the orders never
    got there.
    """
    census = {"total": 0, "refused_by_venue": 0, "not_an_attempt": 0,
              "usable": 0, "by_action": {}}
    rows: list[dict] = []
    if not os.path.isfile(ARB_LOG):
        return rows, census
    try:
        with open(ARB_LOG, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                census["total"] += 1
                if since_ts is not None and float(r.get("ts") or 0) < since_ts:
                    continue
                act = r.get("action")
                if act not in SCORABLE_ACTIONS:
                    # "reject" = our own economics gate refused before sending.
                    census["not_an_attempt"] += 1
                    continue
                if _was_refused(r):
                    census["refused_by_venue"] += 1
                    continue
                census["usable"] += 1
                census["by_action"][act] = census["by_action"].get(act, 0) + 1
                rows.append(r)
    except OSError:
        return rows, census
    rows.sort(key=lambda r: float(r.get("ts") or 0))
    return rows, census


def _taker_fee_per_share(price: float, rate: float = FEE_RATE) -> float:
    """Polymarket's documented shape: rate * price * (1 - price) per share.

    Delegates to config so the tuner prices trades exactly the way the live gate
    does. A tuner that scores candidates on a different fee curve than the bot
    pays is optimising the wrong problem.
    """
    import config as _c
    return _c.taker_fee_per_share(price, rate)


def _attempt_pnl(row: dict) -> float:
    """What this attempt actually cost or made."""
    act = row.get("action")
    shares = float(row.get("shares") or 0.0)
    if act == "locked":
        return float(row.get("realized_pnl") or 0.0)
    if act == "unwound":
        return float(row.get("unwind_pnl") or row.get("realized_pnl") or 0.0)
    if act == "first_leg_killed":
        return 0.0            # nothing bought, nothing risked
    if act == "unwind_failed":
        # A naked leg riding into resolution is worth $1/share or $0, and we do
        # not know which. Score the worst case rather than the average: this is
        # the one outcome the whole design exists to avoid, and letting the
        # tuner treat it as roughly free is how it would learn to court it.
        legs = row.get("legs") or []
        px = float((legs[0] or {}).get("price") or 0.0) if legs else 0.0
        return -(shares * px)
    return 0.0


# ---------------------------------------------------------------------------
# Gate replay
# ---------------------------------------------------------------------------
def would_attempt(row: dict, p: dict) -> bool:
    """Would this parameter set have tried this arb?"""
    pair = row.get("pair_cost")
    locked = row.get("locked_edge")
    if pair is None or locked is None:
        return False
    raw_edge = 1.0 - float(pair)
    if raw_edge < float(p["ARB_MIN_EDGE"]):
        return False
    max_edge = float(p["ARB_MAX_EDGE"])
    if max_edge > 0 and raw_edge > max_edge:
        return False
    if float(locked) < float(p["ARB_MIN_LOCKED_EDGE"]):
        return False
    rem = row.get("remaining_s")
    if rem is not None and float(rem) < float(p["ARB_MIN_REMAINING"]):
        return False
    return True


def simulate(rows: list[dict], p: dict) -> dict:
    """Replay a parameter set over recorded attempts.

    ``expectancy`` is $/attempt — including the attempts that cost nothing,
    because a gate that admits ten free misses to catch one lock is a different
    proposition from one that catches the lock alone, and the average is where
    that shows up.
    """
    n = locks = 0
    pnl = 0.0
    staked = 0.0
    per_attempt: list[float] = []
    for r in rows:
        if not would_attempt(r, p):
            continue
        v = _attempt_pnl(r)
        n += 1
        if r.get("action") == "locked":
            locks += 1
        pnl += v
        staked += float(r.get("shares") or 0.0) * float(r.get("pair_cost") or 0.0)
        per_attempt.append(v)
    exp = pnl / n if n else 0.0
    if n > 1:
        var = sum((x - exp) ** 2 for x in per_attempt) / (n - 1)
        stderr = math.sqrt(var / n)
    else:
        stderr = float("inf") if n else 0.0
    return {
        # Key names match the momentum tuner's so the dashboard's learning
        # panel keeps rendering; "wins" is a LOCK here, not a direction call.
        "trades": n,
        "wins": locks,
        "win_rate": round(100.0 * locks / n, 2) if n else 0.0,
        "pnl": round(pnl, 4),
        "expectancy": round(exp, 4),
        "stderr": round(stderr, 4) if stderr != float("inf") else None,
        "t_stat": round(exp / stderr, 3) if stderr not in (0.0, float("inf")) else 0.0,
        "roi_pct": round(100.0 * pnl / staked, 3) if staked else 0.0,
    }


def _score(train: dict, holdout: dict) -> float:
    """Rank candidates by the WORSE of the two slices.

    Optimising the average lets a spectacular in-sample fit carry a losing
    holdout. Optimising the minimum means a candidate is only as good as its
    weakest evidence, which is the property we actually want.
    """
    return min(train["expectancy"], holdout["expectancy"])


def _eligible(train: dict, holdout: dict) -> bool:
    return (train["trades"] >= MIN_TRAIN_TRADES
            and holdout["trades"] >= MIN_HOLDOUT_TRADES)


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------
def _neighbours(key: str, current, max_steps: int) -> list:
    """Values within `max_steps` notches of the current one on this ladder."""
    ladder = LADDERS[key]
    try:
        idx = ladder.index(current)
    except ValueError:
        # Current value is off-ladder (hand-edited .env): snap to the nearest.
        idx = min(range(len(ladder)),
                  key=lambda i: abs(float(ladder[i]) - float(current)))
    lo = max(0, idx - max_steps)
    hi = min(len(ladder) - 1, idx + max_steps)
    return [ladder[i] for i in range(lo, hi + 1) if ladder[i] != current]


def search(train_rows: list[dict], holdout_rows: list[dict],
           start: dict) -> tuple[dict, dict]:
    """Coordinate ascent from `start`. Returns (best_params, best_metrics)."""
    best = dict(start)
    best_train = simulate(train_rows, best)
    best_holdout = simulate(holdout_rows, best)
    best_score = _score(best_train, best_holdout) if _eligible(
        best_train, best_holdout) else float("-inf")

    for _ in range(PASSES):
        improved = False
        for key in LADDERS:
            # Neighbours are measured from the values this run STARTED with,
            # not from the running best. Measuring from the running best let a
            # second pass step another notch, so "one notch per night" silently
            # became "one notch per pass".
            for value in _neighbours(key, start[key], MAX_STEPS_PER_RUN):
                if value == best[key]:
                    continue
                cand = dict(best)
                cand[key] = value
                tr = simulate(train_rows, cand)
                ho = simulate(holdout_rows, cand)
                if not _eligible(tr, ho):
                    continue
                sc = _score(tr, ho)
                if sc > best_score + 1e-9:
                    best, best_score = cand, sc
                    best_train, best_holdout = tr, ho
                    improved = True
        if not improved:
            break

    return best, {"train": best_train, "holdout": best_holdout,
                  "score": best_score}


# ---------------------------------------------------------------------------
# Auto-revert
# ---------------------------------------------------------------------------
def check_revert(min_trades: int = 25) -> dict | None:
    """Roll back the last change if realised results since it are bad.

    "Bad" deliberately requires BOTH a worse expectancy than the parameters it
    replaced *and* an outright negative one — a change that merely underperforms
    a lucky predecessor while still making money is not a reason to thrash.
    """
    tuned = load_tuned()
    prev = tuned.get("previous_params")
    changed_at = tuned.get("updated_at")
    if not prev or not changed_at:
        return None

    rows, _census = load_attempts(since_ts=float(changed_at))
    if len(rows) < min_trades:
        return None

    now_params = active_params()
    after = simulate(rows, now_params)
    before = simulate(rows, {**DEFAULTS, **prev})
    if after["expectancy"] < 0 and after["expectancy"] < before["expectancy"]:
        _write_tuned(prev, {
            "reason": "auto-revert: realised expectancy since the change was "
                      "negative and worse than the parameters it replaced",
            "reverted_from": now_params,
            "realised_after": after,
            "realised_counterfactual_before": before,
        })
        entry = {"action": "revert", "to": prev, "from": now_params,
                 "realised_after": after, "realised_before": before}
        _journal(entry)
        return entry
    return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def run(verbose: bool = True) -> dict:
    """One tuning cycle. Safe to call on a schedule; a no-op is a valid result."""
    def say(msg):
        if verbose:
            print(f"[tuner] {msg}")

    reverted = check_revert()
    if reverted:
        say("rolled back the last parameter change (see tuning_log.jsonl)")
        return {"action": "revert", "detail": reverted}

    rows, census = load_attempts()

    # Say WHY there is no data, when there is no data. "0 usable attempts"
    # because the venue refused every order is a completely different problem
    # from "0 usable attempts" because no arb ever appeared, and only one of
    # them is fixed by waiting.
    if census["refused_by_venue"] and not rows:
        msg = (f"{census['refused_by_venue']} of {census['total']} attempts were "
               f"REFUSED by the venue and never reached the book, and none "
               f"remain to learn from. That is a setup problem (region, auth, "
               f"size or tick), not a verdict on the edge — check the leg "
               f"errors in arb_executions.jsonl. Tuning nothing.")
        say(msg)
        return {"action": "wait", "reason": msg, "census": census,
                "params": active_params()}

    if len(rows) < MIN_USABLE_ROWS:
        msg = (f"only {len(rows)} usable attempt(s); need {MIN_USABLE_ROWS} "
               f"before tuning anything"
               + (f" ({census['refused_by_venue']} more were refused by the "
                  f"venue and cannot be scored)"
                  if census["refused_by_venue"] else ""))
        say(msg)
        return {"action": "wait", "usable_rows": len(rows), "reason": msg,
                "census": census, "params": active_params()}

    split = int(len(rows) * (1.0 - HOLDOUT_FRAC))
    train_rows, holdout_rows = rows[:split], rows[split:]
    current = active_params()

    cur_train = simulate(train_rows, current)
    cur_holdout = simulate(holdout_rows, current)
    say(f"{len(rows)} usable attempts  (train {len(train_rows)} / holdout "
        f"{len(holdout_rows)})  [{census['refused_by_venue']} refused, "
        f"not scored]")
    say(f"current: train exp ${cur_train['expectancy']:+.4f} "
        f"(n={cur_train['trades']}, {cur_train['win_rate']}% locked)  "
        f"holdout exp ${cur_holdout['expectancy']:+.4f} "
        f"(n={cur_holdout['trades']}, {cur_holdout['win_rate']}% locked)")

    best, metrics = search(train_rows, holdout_rows, current)
    tr, ho = metrics["train"], metrics["holdout"]

    changes = {k: (current[k], best[k]) for k in best if best[k] != current[k]}
    if not changes:
        # Distinguish "nothing was better" from "nothing could be judged".
        # With too few attempts in a slice, _eligible() rejects every candidate
        # and the search returns the incumbent unchanged — which looks exactly
        # like a considered decision but is not one.
        starved = not _eligible(cur_train, cur_holdout)
        if starved:
            msg = (f"not enough attempts to judge yet "
                   f"(train {cur_train['trades']}/{MIN_TRAIN_TRADES}, "
                   f"holdout {cur_holdout['trades']}/{MIN_HOLDOUT_TRADES})")
            say(msg)
            return {"action": "wait", "params": current, "reason": msg,
                    "usable_rows": len(rows), "census": census,
                    "train": cur_train, "holdout": cur_holdout}
        say("no parameter change clears the guardrails — keeping current set")
        _journal({"action": "hold", "params": current,
                  "train": cur_train, "holdout": cur_holdout})
        return {"action": "hold", "params": current, "census": census,
                "train": cur_train, "holdout": cur_holdout}

    # Final gates, applied to the winner only.
    gain_train = tr["expectancy"] - cur_train["expectancy"]
    gain_holdout = ho["expectancy"] - cur_holdout["expectancy"]
    reasons = []
    if ho["expectancy"] <= 0:
        reasons.append(f"holdout expectancy {ho['expectancy']:+.4f} is not positive")
    if gain_train < MIN_IMPROVEMENT:
        reasons.append(f"train gain {gain_train:+.4f} < {MIN_IMPROVEMENT}")
    if gain_holdout < MIN_IMPROVEMENT:
        reasons.append(f"holdout gain {gain_holdout:+.4f} < {MIN_IMPROVEMENT}")
    # Significance, not just sign. The search takes dozens of looks at the data,
    # so on noise alone some candidate will beat the incumbent on both slices —
    # measured at 25% of runs before this gate existed.
    if (ho.get("t_stat") or 0) < MIN_T_STAT:
        reasons.append(
            f"holdout t-stat {ho.get('t_stat')} < {MIN_T_STAT} "
            f"(expectancy {ho['expectancy']:+.4f} +/- {ho.get('stderr')} "
            f"per attempt — indistinguishable from luck)")
    if (tr.get("t_stat") or 0) < MIN_T_STAT:
        reasons.append(f"train t-stat {tr.get('t_stat')} < {MIN_T_STAT}")
    if reasons:
        say("rejected: " + "; ".join(reasons))
        _journal({"action": "reject", "candidate": best, "rejected_for": reasons,
                  "train": tr, "holdout": ho})
        return {"action": "reject", "candidate": best, "reasons": reasons}

    _write_tuned(best, {
        "previous_params": current,
        "reason": "walk-forward improvement on both train and holdout",
        "train": tr, "holdout": ho,
        "usable_rows": len(rows),
    })
    _journal({"action": "apply", "params": best, "previous": current,
              "changes": {k: {"from": v[0], "to": v[1]}
                          for k, v in changes.items()},
              "train": tr, "holdout": ho, "usable_rows": len(rows)})
    for k, (old, new) in changes.items():
        say(f"  {k}: {old} -> {new}")
    say(f"applied. holdout expectancy ${cur_holdout['expectancy']:+.4f} -> "
        f"${ho['expectancy']:+.4f} (n={ho['trades']})")
    return {"action": "apply", "params": best, "changes": changes,
            "train": tr, "holdout": ho}


if __name__ == "__main__":
    import sys
    if "--status" in sys.argv:
        _rows, _census = load_attempts()
        print(json.dumps({"tuned": load_tuned(),
                          "active": active_params(),
                          "dataset": _census}, indent=2, default=str))
    elif "--revert" in sys.argv:
        print(json.dumps(check_revert(min_trades=0) or {"action": "none"},
                         indent=2, default=str))
    else:
        print(json.dumps(run(), indent=2, default=str))
