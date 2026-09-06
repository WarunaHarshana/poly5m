from twap_engine import TWAPEngine

def test_pass():
    eng = TWAPEngine()
    sig = eng.evaluate_momentum(100_080.0, 100_000.0, 120.0, 180.0, 0.88)
    assert sig.side == "UP" and sig.passed_gates, sig.skip_reason

def test_small_move():
    eng = TWAPEngine()
    sig = eng.evaluate_momentum(100_020.0, 100_000.0, 120.0, 180.0, 0.88)
    assert not sig.passed_gates and "min" in sig.skip_reason

def test_window():
    eng = TWAPEngine()
    sig = eng.evaluate_momentum(100_100.0, 100_000.0, 200.0, 100.0, 0.88)
    assert not sig.passed_gates and "window" in sig.skip_reason

def test_down():
    eng = TWAPEngine()
    sig = eng.evaluate_momentum(99_900.0, 100_000.0, 120.0, 180.0, 0.90)
    assert sig.side == "DOWN" and sig.passed_gates, sig.skip_reason

if __name__ == "__main__":
    test_pass(); test_small_move(); test_window(); test_down()
    print("H-005 smoke OK")
