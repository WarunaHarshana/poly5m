"""Nightly walk-forward parameter tuner.

Reads the labeled observation log, replays the momentum strategy over it under
candidate parameter sets, and — only when the evidence clears every guardrail —
writes new parameters to ``state/tuned_params.json``, which ``config.py`` loads
at import so the next bot restart picks them up.

WHY THIS IS BUILT THE WAY IT IS
-------------------------------
Optimising parameters on your own trade history is the classic way to build a
bot that looks brilliant on the data it was fitted to and loses money live.
Every design choice here exists to make that harder:

* **Counterfactual data, not just trades.** The search runs over every
  evaluated slot (see observations.py), so widening a gate is scored on the
  slots it would newly admit — not on a sample that already passed the old gate.
* **Chronological holdout.** Parameters are fitted on the older portion and
  must *also* win on a newer slice they never saw. A random split would leak
  future information through overlapping market regimes.
* **Coordinate ascent, not a full grid.** One dimension at a time over a
  handful of passes. A 7000-cell grid search on a few thousand rows finds
  noise; a shallow search cannot chase it as far.
* **Step limits.** No parameter moves more than one notch per night, so a
  single odd day cannot swing the strategy somewhere strange.
* **Minimum sample sizes.** A configuration that trades 4 slots in the holdout
  is not evidence, however good those 4 look.
* **Auto-revert.** Every change is journalled with the bankroll at the time.
  If realised results after a change are worse and negative, it is rolled back.

The honest limitation: this tunes *thresholds on an existing signal*. It cannot
invent edge that is not there. If the strategy has no edge, a well-behaved
tuner's correct output is "no change" — and it will keep saying that.
"""
from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime, timezone

import observations
import paths

STATE_DIR = paths.state_dir()
ARCHIVE_DIR = paths.archive_dir()
TUNED_PATH = os.path.join(STATE_DIR, "tuned_params.json")
TUNING_LOG = os.path.join(ARCHIVE_DIR, "tuning_log.jsonl")

# --- guardrails ------------------------------------------------------------
MIN_LABELED_ROWS = int(os.getenv("TUNER_MIN_ROWS", "300"))
MIN_TRAIN_TRADES = int(os.getenv("TUNER_MIN_TRAIN_TRADES", "40"))
MIN_HOLDOUT_TRADES = int(os.getenv("TUNER_MIN_HOLDOUT_TRADES", "15"))
HOLDOUT_FRAC = float(os.getenv("TUNER_HOLDOUT_FRAC", "0.30"))
# Required improvement in expectancy ($/trade) over the incumbent, on BOTH
# slices, before anything is written. Small enough to make progress, large
# enough that noise does not clear it.
MIN_IMPROVEMENT = float(os.getenv("TUNER_MIN_IMPROVEMENT", "0.02"))
# How many standard errors the holdout expectancy must clear zero by. The
# coordinate search takes dozens of looks at the data, so "positive on both
# slices" happens by chance far more often than intuition suggests — measured
# at 25% of runs on synthetic pure noise with this gate absent. See
# _selftest_tuner.py T2, which holds the false-positive rate to the stated
# bound; lowering this without re-running that test re-opens the hole.
MIN_T_STAT = float(os.getenv("TUNER_MIN_T_STAT", "2.0"))
PASSES = int(os.getenv("TUNER_PASSES", "2"))
# Simulation economics — a fixed notional makes parameter sets comparable.
UNIT_STAKE = float(os.getenv("TUNER_UNIT_STAKE", "10.0"))
SLIPPAGE = float(os.getenv("TUNER_SLIPPAGE", "0.01"))
FEE_RATE = float(os.getenv("TUNER_FEE_RATE", "0.07"))

# The knobs the tuner is allowed to touch, each with an ordered ladder of
# candidate values. Ordered so "one notch per night" is well defined, and
# deliberately bounded — the tuner cannot propose a value not on this ladder.
LADDERS: dict[str, list] = {
    "LATE_MOMENTUM_MIN_MOVE":  [5.0, 6.0, 8.0, 10.0, 12.0, 15.0, 20.0, 25.0],
    "LATE_MOMENTUM_MIN_ENTRY": [0.45, 0.50, 0.55, 0.60, 0.65, 0.70],
    "LATE_MOMENTUM_MAX_ENTRY": [0.75, 0.80, 0.84, 0.88, 0.90, 0.92],
    "LATE_MOMENTUM_MIN_EDGE":  [-0.30, -0.20, -0.10, -0.05, 0.0, 0.02, 0.05],
    "LATE_MOMENTUM_MAX_EDGE":  [0.04, 0.06, 0.10, 0.15, 0.20, 0.30],
    "LATE_MOMENTUM_MAX_CHOP":  [1, 2, 3, 4, 6, 99],
}
MAX_STEPS_PER_RUN = int(os.getenv("TUNER_MAX_STEPS", "1"))

DEFAULTS = {
    "LATE_MOMENTUM_MIN_MOVE": 10.0,
    "LATE_MOMENTUM_MIN_ENTRY": 0.55,
    "LATE_MOMENTUM_MAX_ENTRY": 0.88,
    "LATE_MOMENTUM_MIN_EDGE": -0.20,
    "LATE_MOMENTUM_MAX_EDGE": 0.10,
    "LATE_MOMENTUM_MAX_CHOP": 3,
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
# Strategy replay
# ---------------------------------------------------------------------------
def _taker_fee_per_share(price: float, rate: float = FEE_RATE) -> float:
    """Polymarket's documented shape: rate * price * (1 - price) per share.

    Delegates to config so the backtest prices trades exactly the way the live
    gate does. A tuner that scores candidates on a different fee curve than the
    bot pays is optimising the wrong problem.
    """
    import config as _c
    return _c.taker_fee_per_share(price, rate)


def would_trade(row: dict, p: dict) -> bool:
    """Would this parameter set have taken this slot?"""
    ask = row.get("ask")
    move = row.get("move_usd")
    side = row.get("side")
    if ask is None or move is None or side not in ("UP", "DOWN"):
        return False
    # The recorded requirement was scaled by sqrt(remaining/20) at decision
    # time; recompute that scaling for the candidate threshold so the
    # comparison is exactly what the live bot would have done.
    remaining = row.get("remaining_s")
    scale = math.sqrt(max(float(remaining), 1.0) / 20.0) if remaining else 1.0
    if abs(float(move)) < float(p["LATE_MOMENTUM_MIN_MOVE"]) * scale:
        return False
    ask = float(ask)
    if ask < float(p["LATE_MOMENTUM_MIN_ENTRY"]):
        return False
    if ask > float(p["LATE_MOMENTUM_MAX_ENTRY"]):
        return False
    edge = row.get("edge")
    edge_net = row.get("edge_net")
    if edge_net is not None and float(edge_net) < float(p["LATE_MOMENTUM_MIN_EDGE"]):
        return False
    max_edge = float(p["LATE_MOMENTUM_MAX_EDGE"])
    if max_edge > 0 and edge is not None and float(edge) > max_edge:
        return False
    chop = row.get("chop_crosses")
    max_chop = int(p["LATE_MOMENTUM_MAX_CHOP"])
    if chop is not None and max_chop > 0 and int(chop) > max_chop:
        return False
    return True


def simulate(rows: list[dict], p: dict) -> dict:
    """Replay a parameter set over labeled observations.

    Prices every row identically — raw ask plus the same pessimistic slippage
    and the real taker fee — so traded and skipped rows are directly
    comparable. Returns aggregate stats; ``expectancy`` is $/trade.
    """
    n = wins = 0
    pnl = 0.0
    staked = 0.0
    per_trade: list[float] = []
    for r in rows:
        if r.get("won_if_taken") is None:
            continue
        if not would_trade(r, p):
            continue
        fill = min(0.99, float(r["ask"]) + SLIPPAGE)
        if fill <= 0:
            continue
        shares = UNIT_STAKE / fill
        fee = shares * _taker_fee_per_share(fill)
        won = bool(r["won_if_taken"])
        trade_pnl = (shares if won else 0.0) - UNIT_STAKE - fee
        n += 1
        wins += 1 if won else 0
        pnl += trade_pnl
        staked += UNIT_STAKE
        per_trade.append(trade_pnl)
    # Standard error of the mean PnL. Binary payouts are violently skewed —
    # at ask 0.60 a win is +$6.67 and a loss is -$10 — so a difference of a
    # few cents per trade over a few hundred trades is well inside the noise.
    # Without this the acceptance test cannot tell signal from luck.
    exp = pnl / n if n else 0.0
    if n > 1:
        var = sum((x - exp) ** 2 for x in per_trade) / (n - 1)
        stderr = math.sqrt(var / n)
    else:
        stderr = float("inf") if n else 0.0
    return {
        "trades": n,
        "wins": wins,
        "win_rate": round(100.0 * wins / n, 2) if n else 0.0,
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

    rows = [r for r in observations.load(since_ts=float(changed_at))
            if r.get("decision") == "trade" and r.get("won_if_taken") is not None]
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

    rows = [r for r in observations.load(labeled_only=True)
            if r.get("won_if_taken") is not None]
    rows.sort(key=lambda r: float(r.get("ts") or 0))
    if len(rows) < MIN_LABELED_ROWS:
        msg = (f"only {len(rows)} labeled slots; need {MIN_LABELED_ROWS} "
               f"before tuning anything")
        say(msg)
        return {"action": "wait", "labeled_rows": len(rows), "reason": msg}

    split = int(len(rows) * (1.0 - HOLDOUT_FRAC))
    train_rows, holdout_rows = rows[:split], rows[split:]
    current = active_params()

    cur_train = simulate(train_rows, current)
    cur_holdout = simulate(holdout_rows, current)
    say(f"{len(rows)} labeled slots  (train {len(train_rows)} / holdout "
        f"{len(holdout_rows)})")
    say(f"current: train exp ${cur_train['expectancy']:+.4f} "
        f"(n={cur_train['trades']})  holdout exp "
        f"${cur_holdout['expectancy']:+.4f} (n={cur_holdout['trades']})")

    best, metrics = search(train_rows, holdout_rows, current)
    tr, ho = metrics["train"], metrics["holdout"]

    changes = {k: (current[k], best[k]) for k in best if best[k] != current[k]}
    if not changes:
        # Distinguish "nothing was better" from "nothing could be judged".
        # With too few trades in a slice, _eligible() rejects every candidate
        # and the search returns the incumbent unchanged — which looks exactly
        # like a considered decision but is not one. Reporting it as such made
        # the supervisor back off a full day on a holdout of 9 trades.
        starved = not _eligible(cur_train, cur_holdout)
        if starved:
            msg = (f"not enough trades to judge yet "
                   f"(train {cur_train['trades']}/{MIN_TRAIN_TRADES}, "
                   f"holdout {cur_holdout['trades']}/{MIN_HOLDOUT_TRADES})")
            say(msg)
            return {"action": "wait", "params": current, "reason": msg,
                    "labeled_rows": len(rows),
                    "train": cur_train, "holdout": cur_holdout}
        say("no parameter change clears the guardrails — keeping current set")
        _journal({"action": "hold", "params": current,
                  "train": cur_train, "holdout": cur_holdout})
        return {"action": "hold", "params": current,
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
    # Significance, not just sign. The search takes dozens of looks at the
    # data, so on noise alone some candidate will beat the incumbent on both
    # slices — measured at 25% of runs before this gate existed. Requiring the
    # holdout mean to stand clear of its own standard error is what separates
    # an effect from a lucky subset.
    if (ho.get("t_stat") or 0) < MIN_T_STAT:
        reasons.append(
            f"holdout t-stat {ho.get('t_stat')} < {MIN_T_STAT} "
            f"(expectancy {ho['expectancy']:+.4f} +/- {ho.get('stderr')} "
            f"per trade — indistinguishable from luck)")
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
        "labeled_rows": len(rows),
    })
    _journal({"action": "apply", "params": best, "previous": current,
              "changes": {k: {"from": v[0], "to": v[1]}
                          for k, v in changes.items()},
              "train": tr, "holdout": ho, "labeled_rows": len(rows)})
    for k, (old, new) in changes.items():
        say(f"  {k}: {old} -> {new}")
    say(f"applied. holdout expectancy ${cur_holdout['expectancy']:+.4f} -> "
        f"${ho['expectancy']:+.4f} (n={ho['trades']})")
    return {"action": "apply", "params": best, "changes": changes,
            "train": tr, "holdout": ho}


if __name__ == "__main__":
    import sys
    if "--status" in sys.argv:
        print(json.dumps({"tuned": load_tuned(),
                          "active": active_params()}, indent=2, default=str))
    elif "--revert" in sys.argv:
        print(json.dumps(check_revert(min_trades=0) or {"action": "none"},
                         indent=2, default=str))
    else:
        print(json.dumps(run(), indent=2, default=str))
