"""Pre-post falling-mid gate (option 1); default off."""
import config
import maker_guard as mg


def test_default_off():
    assert config.MAKER_POST_MAX_MID_DROP == 0.0
    assert config.MAKER_POST_STABLE_MAX_RANGE == 0.0
    assert not mg.post_falling_mid_blocked(0.5, 0.5, 0.0, 0.0)


def test_drop_threshold():
    assert mg.post_falling_mid_blocked(0.055, None, 0.05, 0.0)   # 2026-10-13 20:20:56 ETH: 0.645 -> 0.59
    assert mg.post_falling_mid_blocked(0.05, None, 0.05, 0.0)
    assert not mg.post_falling_mid_blocked(0.04, None, 0.05, 0.0)
    assert not mg.post_falling_mid_blocked(None, None, 0.05, 0.0)  # fail-open


def test_stability():
    h = mg.MidHistory(60)
    for t, m in [(0, 0.60), (98, 0.62), (99, 0.60), (100, 0.59)]:
        h.add("T", t, m)
    r = h.range_over("T", 100, 3, 0.59)
    assert abs(r - 0.03) < 1e-9
    assert mg.post_falling_mid_blocked(None, r, 0.0, 0.02)
    assert not mg.post_falling_mid_blocked(None, r, 0.0, 0.05)
    assert h.range_over("X", 100, 3, None) is None
