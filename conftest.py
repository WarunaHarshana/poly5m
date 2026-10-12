"""Shared pytest config.

Most bot-integration tests simulate ticks at t=120-180s into the slot to exercise maker/momentum
logic. The ENTRY_BLOCK dead zone (default on, [120,180)) would block those, so it is disabled by
default for tests; test_entry_block_zone.py re-enables it explicitly.
"""
import pytest

import config


@pytest.fixture(autouse=True)
def _entry_block_zone_off_by_default(monkeypatch):
    monkeypatch.setattr(config, "ENTRY_BLOCK_ENABLED", False, raising=False)
    yield


@pytest.fixture(autouse=True)
def _isolated_state_dir(monkeypatch, tmp_path):
    """Bot side files (pending_settlements.json, redeem_queue.json, live_orphans.json, ...) go to a temp dir."""
    d = tmp_path / "state"
    d.mkdir()
    monkeypatch.setattr(config, "STATE_DIR", d, raising=False)
    yield
