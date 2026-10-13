import config
from risk_manager import cap_order_shares
from polymarket_client import PolymarketLiveClient


def _set(mp, cap=3.0, mn=5.0):
    mp.setattr(config, "MAX_TRADE_USD", cap)
    mp.setattr(config, "MIN_ORDER_SHARES", mn)


def test_disabled_passthrough(monkeypatch):
    _set(monkeypatch, cap=0.0)
    assert cap_order_shares(10.0, 0.9) == 10.0


def test_skip_when_min_shares_exceed_cap(monkeypatch):
    _set(monkeypatch)
    assert cap_order_shares(5.0, 0.61) is None
    assert cap_order_shares(5.0, 0.90) is None


def test_reduces_to_floor_cap(monkeypatch):
    _set(monkeypatch)
    s = cap_order_shares(30.0, 0.10)
    assert s == 30.0 and s * 0.10 <= 3.0
    s = cap_order_shares(12.0, 0.40)
    assert s == 7.0 and s * 0.40 <= 3.0
    assert cap_order_shares(5.0, 0.60) == 5.0


def test_client_hard_guard_blocks_oversized_buy(monkeypatch):
    _set(monkeypatch)
    c = PolymarketLiveClient.__new__(PolymarketLiveClient)
    r = c.place_order("tok", "BUY", 0.90, 5.0)
    assert r["success"] is False and "MAX_TRADE_USD" in r["error"]


def test_cancel_confirm_repolls_lagging_state(monkeypatch):
    monkeypatch.setattr(config, "CANCEL_CONFIRM_POLL_S", 0.0)
    c = PolymarketLiveClient.__new__(PolymarketLiveClient)
    states = iter(["live", "live", "canceled"])
    monkeypatch.setattr(c, "cancel_order", lambda oid: {"canceled": [oid]})
    monkeypatch.setattr(c, "get_order_state", lambda oid: {"status": next(states), "size_matched": 0.0,
                                                           "original_size": 5.0, "price": 0.01})
    r = c.cancel_and_confirm("x")
    assert r["confirmed"] is True and r["status"] == "canceled"
