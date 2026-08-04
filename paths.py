"""Single source of truth for where runtime state lives.

Locally that is ``<repo>/state``. On Railway the repo filesystem is ephemeral —
every deploy or restart wipes it — so the trade archive, the observation log and
the tuner's learned parameters have to live on a mounted volume instead.
Set ``STATE_ROOT=/data`` (matching the volume mount path) and every module that
imports this writes there.

Parallel timeframe instances still get their own subdirectory, exactly as
before: 5m keeps the root paths, 15m/4h live under ``<root>/<timeframe>``.
"""
from __future__ import annotations

import os

HERE = os.path.dirname(os.path.abspath(__file__))


def state_root() -> str:
    """Base directory for all runtime state (volume-aware)."""
    root = (os.getenv("STATE_ROOT") or "").strip()
    return os.path.abspath(root) if root else os.path.join(HERE, "state")


def state_subdir() -> str:
    """The per-timeframe subdirectory name ('' for the 5m default)."""
    sub = (os.getenv("STATE_SUBDIR") or "").strip()
    if sub:
        return sub
    tf = (os.getenv("TIMEFRAME") or "5m").strip().lower()
    return "" if tf == "5m" else tf


def state_dir(sub: str | None = None, create: bool = True) -> str:
    """State directory for a timeframe. ``None`` means "use the environment"."""
    if sub is None:
        sub = state_subdir()
    path = os.path.join(state_root(), sub) if sub else state_root()
    if create:
        os.makedirs(path, exist_ok=True)
    return path


def archive_dir(sub: str | None = None, create: bool = True) -> str:
    """Permanent history directory — never cleared by ``--reset``."""
    path = os.path.join(state_dir(sub, create=create), "archive")
    if create:
        os.makedirs(path, exist_ok=True)
    return path
