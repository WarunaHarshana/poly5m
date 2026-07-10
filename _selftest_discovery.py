"""Offline test for the REAL market-discovery parsing in polymarket_client.

The end-to-end self-test (_selftest_realtime.py) stubs out PolymarketPublic
entirely, so it can't catch bugs in the real Gamma/CLOB parsing. This test
injects a fake HTTP layer into the real PolymarketPublic and asserts that
find_btc_5min_market() returns the exact shape bot.run_realtime expects:
{question, tokens{UP,DOWN}, up, down, conditionId, end_ts, slug}.

Run: python3 _selftest_discovery.py
"""
import json

import config
from polymarket_client import PolymarketPublic


class Resp:
    def __init__(self, payload):
        self._p = payload
    def json(self):
        return self._p


class FakeRequests:
    """Mimics the Gamma/CLOB REST responses (JSON-encoded string fields)."""
    def get(self, url, params=None, timeout=None):
        if url.endswith("/events"):
            slug = (params or {}).get("slug", "")
            event = {
                "slug": slug,
                "title": "Bitcoin Up or Down?",
                "markets": [{
                    "question": "Will BTC be up or down over the next 5 minutes?",
                    "closed": False,
                    "acceptingOrders": True,
                    "conditionId": "0xabc123",
                    # Gamma sends these as JSON-encoded strings, outcome order
                    # matching the token order:
                    "clobTokenIds": json.dumps(["TOK_UP", "TOK_DOWN"]),
                    "outcomes": json.dumps(["Up", "Down"]),
                }],
            }
            return Resp([event])
        if url.endswith("/midpoint"):
            return Resp({"mid": "0.53"})
        if url.endswith("/book"):
            return Resp({"bids": [], "asks": []})
        return Resp([])


# Build without __init__ so we don't need the `requests` package installed.
pub = PolymarketPublic.__new__(PolymarketPublic)
pub.timeout = 12.0
pub.requests = FakeRequests()

m = pub.find_btc_5min_market()
assert m["tokens"]["UP"] == "TOK_UP", m
assert m["tokens"]["DOWN"] == "TOK_DOWN", m
assert m["up"] == "TOK_UP" and m["down"] == "TOK_DOWN", m
assert m["conditionId"] == "0xabc123", m
assert m["end_ts"] and m["end_ts"] % config.SLOT_SECONDS == 0, m
assert m["slug"].startswith(config.BTC_SLUG_PREFIX), m

# The bot indexes market["tokens"][side] then calls midpoint(token_id):
mid = pub.midpoint(m["tokens"]["UP"])
assert abs(mid - 0.53) < 1e-9, mid

# Positional fallback when outcome labels are unfamiliar (e.g. Yes/No missing):
tokens = PolymarketPublic._map_tokens(["A", "B"], ["Maybe", "Perhaps"])
assert tokens == {"UP": "A", "DOWN": "B"}, tokens

print("DISCOVERY_OK", json.dumps(m))
