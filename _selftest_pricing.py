"""Unit test for the quote-validation fix (the $2000 bankroll bug).

Proves midpoint() never returns a degenerate ~0 price: it falls back to the
real order-book mid, and raises when the book is empty/one-sided so the trade
loop skips the slot instead of booking an absurd 2000x fill.
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
from polymarket_client import PolymarketPublic


class Resp:
    def __init__(self, payload):
        self._p = payload

    def json(self):
        return self._p


class FakeRequests:
    def __init__(self, midpoint_payload, book_payload):
        self.midpoint_payload = midpoint_payload
        self.book_payload = book_payload

    def get(self, url, params=None, timeout=None):
        if url.endswith("/midpoint"):
            return Resp(self.midpoint_payload)
        if url.endswith("/book"):
            return Resp(self.book_payload)
        return Resp({})


def make_pub(midpoint_payload, book_payload):
    pub = PolymarketPublic.__new__(PolymarketPublic)
    pub.requests = FakeRequests(midpoint_payload, book_payload)
    pub.timeout = 5
    return pub


# 1) a valid midpoint passes straight through
p = make_pub({"mid": "0.53"}, {})
assert abs(p.midpoint("t") - 0.53) < 1e-9, "valid midpoint failed"

# 2) zero midpoint -> fall back to the two-sided book mid (0.48/0.52 -> 0.50)
p = make_pub({"mid": "0"},
             {"bids": [{"price": "0.48", "size": "10"}],
              "asks": [{"price": "0.52", "size": "9"}]})
assert abs(p.midpoint("t") - 0.50) < 1e-9, "book fallback failed"

# 3) zero midpoint + empty book -> must raise (loop will skip the slot)
p = make_pub({"mid": "0"}, {"bids": [], "asks": []})
try:
    p.midpoint("t")
    raise SystemExit("FAIL: degenerate ~0 quote was not rejected")
except ValueError:
    pass

# 4) one-sided book -> no mid -> must raise
p = make_pub({}, {"bids": [{"price": "0.9", "size": "1"}], "asks": []})
try:
    p.midpoint("t")
    raise SystemExit("FAIL: one-sided book was not rejected")
except ValueError:
    pass

# 5) the run-loop range guard rejects extreme-but-valid quotes, keeps ~0.5
assert not (config.MIN_QUOTE <= 0.004 <= config.MAX_QUOTE), "range guard too loose"
assert (config.MIN_QUOTE <= 0.5 <= config.MAX_QUOTE), "range guard too tight"

# 6) buy_quote prefers best ask (executable price) over mid
p = make_pub({"mid": "0.50"},
             {"bids": [{"price": "0.48", "size": "10"}],
              "asks": [{"price": "0.54", "size": "9"}]})
q = p.buy_quote("t")
assert abs(q["price"] - 0.54) < 1e-9, f"buy_quote should use ask, got {q}"
assert abs(q["spread"] - 0.06) < 1e-9, q
assert abs(q["mid"] - 0.50) < 1e-9, q
assert abs(q["ask_size"] - 9.0) < 1e-9, q

# 7) buy_quote falls back to mid when book has no ask
p = make_pub({"mid": "0.51"}, {"bids": [], "asks": []})
q = p.buy_quote("t")
assert abs(q["price"] - 0.51) < 1e-9, q

print("PRICING_OK")
