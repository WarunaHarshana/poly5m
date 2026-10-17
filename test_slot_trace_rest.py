import time
import shadow_log


def _wait(c, asset, tok, opp):
    for _ in range(100):
        r = c.get(asset, tok, opp, time.time())
        if r != (None, None):
            return r
        time.sleep(0.01)
    return (None, None)


def test_rest_cache_fetches_async_and_rate_limits():
    calls = []
    def fetch(t, o):
        calls.append(t)
        return (0.58, 0.60)
    c = shadow_log.RestTopCache(fetch, min_interval_s=5.0)
    assert _wait(c, "eth", "T", "O") == (0.58, 0.60)
    for _ in range(5):
        c.get("eth", "T", "O", time.time())
    time.sleep(0.05)
    assert len(calls) == 1


def test_rest_cache_survives_errors_and_token_change():
    def bad(t, o):
        raise RuntimeError("boom")
    c = shadow_log.RestTopCache(bad, min_interval_s=0.0)
    assert c.get("eth", "T", None, time.time()) == (None, None)
    time.sleep(0.05)
    assert c.get("eth", "T", None, time.time()) == (None, None)
    c._fetch = lambda t, o: (0.4, 0.42)
    assert _wait(c, "eth", "T2", None) == (0.4, 0.42)


def test_rest_cache_stale_ignored():
    c = shadow_log.RestTopCache(lambda t, o: (0.5, 0.52), min_interval_s=1000, max_age_s=10)
    assert _wait(c, "eth", "T", None) == (0.5, 0.52)
    assert c.get("eth", "T", None, time.time() + 60) == (None, None)
