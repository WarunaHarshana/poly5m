"""Permanent decision journal for research (Tier 1).

Appends one JSON line per mid/late decision (trade or skip) so we can measure:
  - does edge predict wins?
  - which skip reasons kill false positives?
  - paper vs would-be live fill realism

File: state/archive/decisions.jsonl  (never deleted by --reset)
"""
from __future__ import annotations
import json
import os
import time
from datetime import datetime, timezone

ARCHIVE_DIR = os.path.join(os.path.dirname(__file__), "state", "archive")
os.makedirs(ARCHIVE_DIR, exist_ok=True)
DECISIONS_JSONL = os.path.join(ARCHIVE_DIR, "decisions.jsonl")
DECISIONS_CSV = os.path.join(ARCHIVE_DIR, "decisions.csv")

_CSV_COLS = [
    "ts", "time_utc", "session_id", "mode", "strategy", "phase",
    "action", "reason", "slug", "question", "side",
    "spot", "strike", "strike_src", "move_usd", "remaining_s",
    "model_prob", "fair_prob", "z_score", "sigma_usd", "min_move_usd",
    "ask", "edge", "fair_edge", "conf", "stake", "fill_price",
    "chop_crosses", "ask_size", "spread", "arb_sum", "shadow",
]


def _utc(ts: float | None = None) -> str:
    t = float(ts if ts is not None else time.time())
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def log_decision(row: dict) -> None:
    """Append a decision row to jsonl (+ light CSV mirror)."""
    rec = dict(row or {})
    rec.setdefault("ts", time.time())
    rec.setdefault("time_utc", _utc(rec["ts"]))
    # Round floats for readability
    for k, v in list(rec.items()):
        if isinstance(v, float):
            rec[k] = round(v, 6)
    try:
        with open(DECISIONS_JSONL, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=True) + "\n")
    except Exception as e:
        print(f"  [journal] write failed: {e}")
        return
    try:
        need_header = not os.path.exists(DECISIONS_CSV) or os.path.getsize(DECISIONS_CSV) == 0
        with open(DECISIONS_CSV, "a", encoding="utf-8") as f:
            if need_header:
                f.write(",".join(_CSV_COLS) + "\n")
            vals = []
            for c in _CSV_COLS:
                v = rec.get(c, "")
                if v is None:
                    v = ""
                s = str(v).replace(",", ";").replace("\n", " ")
                vals.append(s)
            f.write(",".join(vals) + "\n")
    except Exception:
        pass
