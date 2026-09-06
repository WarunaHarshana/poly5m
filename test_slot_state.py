from __future__ import annotations
import json
import tempfile
from pathlib import Path

import config
from bot import TWAPBot


def test_slot_state_roundtrip(monkeypatch=None):
    d = Path(tempfile.mkdtemp())
    # Point STATE_DIR via monkeypatch of instance methods after construct is heavy;
    # call helpers on a lightweight stand-in.
    class _T:
        _slot_done = {"btc-updown-5m-1"}
        def _slot_state_path(self):
            return d / "slot_state.json"
        _load_slot_state = TWAPBot._load_slot_state
        _save_slot_state = TWAPBot._save_slot_state

    t = _T()
    TWAPBot._save_slot_state(t, slot_ts=123, slug="btc-updown-5m-1", strike=79551.8)
    data = TWAPBot._load_slot_state(t)
    assert data["slot_ts"] == 123
    assert abs(float(data["strike"]) - 79551.8) < 1e-6
    assert "btc-updown-5m-1" in data["slot_done"]
    # None strike keeps previous
    TWAPBot._save_slot_state(t, slot_ts=123, slug="btc-updown-5m-1", strike=None)
    data2 = TWAPBot._load_slot_state(t)
    assert abs(float(data2["strike"]) - 79551.8) < 1e-6
