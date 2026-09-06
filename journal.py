"""Append-only JSONL event journal (ported from poly15m)."""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger("journal")

_path: str | None = None
_last: dict[str, Any] = {}
_STDOUT_EVENTS = {
    "fill",
    "paper_fill",
    "settlement",
    "decision",
    "skip_round",
    "round_start",
    "round_end",
    "strike",
    "risk_block",
}


def configure(path: str | None) -> None:
    global _path
    _path = path
    if _path:
        directory = os.path.dirname(_path)
        if directory:
            os.makedirs(directory, exist_ok=True)


def last() -> dict[str, Any]:
    return dict(_last)


def write(event: str, **fields: Any) -> dict[str, Any]:
    rec: dict[str, Any] = {"ts": time.time(), "event": event}
    for key, val in fields.items():
        if val is None:
            continue
        rec[key] = val
    global _last
    _last = rec

    if event in _STDOUT_EVENTS:
        try:
            logger.info("[JOURNAL] %s", json.dumps(rec, default=str, separators=(",", ":")))
        except Exception:
            logger.info("[JOURNAL] %s %s", event, fields)

    if not _path:
        return rec
    try:
        with open(_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(rec, default=str, separators=(",", ":")) + "\n")
    except Exception as exc:
        logger.warning("journal write failed: %s", exc)
    return rec
