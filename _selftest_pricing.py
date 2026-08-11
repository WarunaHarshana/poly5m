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

# --- taker fee must match Polymarket's DOCUMENTED formula -------------------
# fee = shares * rate * price * (1 - price), corroborated by the venue's own
# feeSchedule {"rate": 0.07, "exponent": 1, "takerOnly": true}. The bot used to
# charge rate * min(p, 1-p): double the real fee at 50/50, ~15% too much at
# 0.87. Wrong in the conservative direction, but it sits inside the arb gate
# that decides whether real money goes in, so it gets pinned here.
import config as _cfg

_cfg.TAKER_FEE_RATE = 0.07
# Polymarket's published worked example.
_got = 100 * _cfg.taker_fee_per_share(0.50)
assert abs(_got - 1.75) < 1e-9, f"100 shares @0.50 should cost $1.75, got ${_got}"

for _p in (0.05, 0.25, 0.5, 0.75, 0.95):
    _expect = 0.07 * _p * (1 - _p)
    _actual = _cfg.taker_fee_per_share(_p)
    assert abs(_actual - _expect) < 1e-12, f"p={_p}: {_actual} != {_expect}"

# Symmetric about 0.5, and zero at the boundaries where there is nothing to win.
assert abs(_cfg.taker_fee_per_share(0.3) - _cfg.taker_fee_per_share(0.7)) < 1e-12
assert _cfg.taker_fee_per_share(1.0) == 0.0
assert _cfg.taker_fee_per_share(0.0) == 0.0
# Peak at 50/50.
assert _cfg.taker_fee_per_share(0.5) > _cfg.taker_fee_per_share(0.6) > \
    _cfg.taker_fee_per_share(0.9)

# Every module must price on the SAME curve as the live gate.
import arb_executor as _ax
import tuner as _tn
for _p in (0.44, 0.5, 0.87):
    assert abs(_ax.taker_fee_per_share(_p) - _cfg.taker_fee_per_share(_p)) < 1e-12
    assert abs(_tn._taker_fee_per_share(_p) - _cfg.taker_fee_per_share(_p)) < 1e-12

print("FEE_OK  (matches the documented 100 x 0.50 -> $1.75, one curve everywhere)")

print("PRICING_OK")
