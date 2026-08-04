"""Labeled slot observations — the dataset the tuner learns from.

The trade log only contains slots the bot actually traded. That is a tiny,
*selected* sample: roughly 5 rows a day out of 288 slots, and every row already
passed the current gates. You cannot ask "would a wider entry band have made
money?" from it, because the slots a wider band would have taken were never
recorded.

So this module records one row per *evaluated* slot regardless of the decision:
the full feature vector at decision time plus, once the slot settles, the
outcome. Traded or skipped, the row is there. That turns ~5 samples/day into
~288 and makes honest walk-forward tuning possible.

Two phases, because the label does not exist yet at decision time:

  1. ``record()``  — append the features with ``outcome: null``.
  2. ``backfill_outcomes()`` — later, look up Polymarket's official resolution
     for slots whose ``end_ts`` has passed and fill in the label.

Files (all under the archive dir, never cleared by ``--reset``):
  observations.jsonl   one JSON object per evaluated slot
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import paths

ARCHIVE_DIR = paths.archive_dir()
OBSERVATIONS_JSONL = os.path.join(ARCHIVE_DIR, "observations.jsonl")

# Keep the file bounded. 288 slots/day means ~105k rows a year; the tuner only
# ever looks at a recent window, so older rows are rotated out rather than
# reread forever.
MAX_ROWS = int(os.getenv("OBSERVATIONS_MAX_ROWS", "200000"))

# Fields written for every row. Anything the tuner might want to slice on has
# to be here — a feature not recorded today can never be studied tomorrow.
FIELDS = (
    "ts", "time_utc", "slug", "end_ts", "slot_start_ts", "hour_utc",
    "mode", "timeframe", "asset", "session_id",
    # signal features
    "strike", "strike_src", "spot", "move_usd", "abs_move", "min_move_req",
    "remaining_s", "chop_crosses", "realized_vol_daily",
    "side", "model_prob", "edge", "edge_net", "fee_per_share",
    # book features — `ask` is always the RAW best ask, on traded and skipped
    # rows alike, so a backtest can price every row the same way.
    "ask", "bid", "spread", "ask_size", "ask_up", "ask_down", "arb_sum",
    # what the bot did
    "decision", "skip_reason", "stake_usd", "fill_price", "is_maker",
    # label (filled by backfill_outcomes)
    "outcome", "won_if_taken", "outcome_src", "labeled_at",
)


def _utc(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC")


def _clean(row: dict) -> dict:
    out = {}
    for k in FIELDS:
        v = row.get(k)
        if isinstance(v, float):
            v = round(v, 6)
        out[k] = v
    return out


def record(row: dict) -> None:
    """Append one evaluated slot. Never raises — logging must not stop trading."""
    try:
        rec = dict(row or {})
        rec.setdefault("ts", time.time())
        rec.setdefault("time_utc", _utc(rec["ts"]))
        if rec.get("hour_utc") is None:
            rec["hour_utc"] = datetime.fromtimestamp(
                float(rec["ts"]), tz=timezone.utc).hour
        move = rec.get("move_usd")
        if move is not None and rec.get("abs_move") is None:
            rec["abs_move"] = abs(float(move))
        rec.setdefault("outcome", None)
        with open(OBSERVATIONS_JSONL, "a", encoding="utf-8") as f:
            f.write(json.dumps(_clean(rec), ensure_ascii=True) + "\n")
    except Exception as e:  # pragma: no cover - defensive
        print(f"  [obs] write failed: {e}")


def load(since_ts: float | None = None, labeled_only: bool = False) -> list[dict]:
    """Read observations, optionally limited to a recent window."""
    rows: list[dict] = []
    if not os.path.isfile(OBSERVATIONS_JSONL):
        return rows
    try:
        with open(OBSERVATIONS_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except ValueError:
                    continue
                if since_ts is not None and float(d.get("ts") or 0) < since_ts:
                    continue
                if labeled_only and d.get("outcome") not in ("UP", "DOWN"):
                    continue
                rows.append(d)
    except OSError as e:
        print(f"  [obs] read failed: {e}")
    return rows


def _rewrite(rows: list[dict]) -> None:
    """Atomically replace the log (used by backfill and rotation)."""
    if len(rows) > MAX_ROWS:
        rows = rows[-MAX_ROWS:]
    tmp = OBSERVATIONS_JSONL + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(_clean(r), ensure_ascii=True) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, OBSERVATIONS_JSONL)


def backfill_outcomes(pub=None, max_lookups: int = 400,
                      min_age_s: float = 90.0,
                      max_age_s: float = 7 * 24 * 3600.0) -> dict:
    """Label settled slots with Polymarket's official outcome.

    ``min_age_s`` gives the venue time to publish the resolution; ``max_age_s``
    stops us retrying ancient rows forever when a slug 404s permanently.

    Returns a small summary dict for logging.
    """
    rows = load()
    if not rows:
        return {"labeled": 0, "pending": 0, "checked": 0}

    if pub is None:
        from polymarket_client import PolymarketPublic
        pub = PolymarketPublic()

    now = time.time()
    # One lookup per slug, not per row — several rows can share a slot.
    by_slug: dict[str, list[dict]] = {}
    for r in rows:
        if r.get("outcome") in ("UP", "DOWN"):
            continue
        end_ts = r.get("end_ts")
        slug = r.get("slug")
        if not slug or not end_ts:
            continue
        age = now - float(end_ts)
        if age < min_age_s or age > max_age_s:
            continue
        by_slug.setdefault(slug, []).append(r)

    labeled = 0
    checked = 0
    # Oldest first: those are the ones most likely to be resolved already.
    for slug in sorted(by_slug, key=lambda s: by_slug[s][0].get("end_ts") or 0):
        if checked >= max_lookups:
            break
        checked += 1
        try:
            outcome = pub.get_official_btc_updown_outcome(slug)
        except Exception:
            continue
        if outcome not in ("UP", "DOWN"):
            continue
        for r in by_slug[slug]:
            r["outcome"] = outcome
            r["outcome_src"] = "polymarket-official"
            r["labeled_at"] = now
            side = r.get("side")
            # The counterfactual: would the side momentum pointed at have won?
            r["won_if_taken"] = (side == outcome) if side in ("UP", "DOWN") else None
            labeled += 1

    if labeled:
        _rewrite(rows)
    pending = sum(1 for r in rows if r.get("outcome") not in ("UP", "DOWN"))
    return {"labeled": labeled, "pending": pending, "checked": checked,
            "total": len(rows)}


if __name__ == "__main__":  # manual backfill / inspection
    import sys
    if "--backfill" in sys.argv:
        print(json.dumps(backfill_outcomes(), indent=2))
    else:
        rows = load()
        lab = sum(1 for r in rows if r.get("outcome") in ("UP", "DOWN"))
        print(f"{OBSERVATIONS_JSONL}\n  rows={len(rows)}  labeled={lab}  "
              f"unlabeled={len(rows) - lab}")
        if rows:
            print("  latest:", json.dumps(rows[-1], indent=2))
