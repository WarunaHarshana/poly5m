"""Multi-asset 5m coin config, feed isolation, and per-coin positions."""
from __future__ import annotations

import tempfile
from pathlib import Path

import config
from bot import CoinSlot, TWAPBot, _engine_for
from feeds import ChainlinkRTDSStream
from risk_manager import RiskManager


def test_known_5m_assets():
    assert config.KNOWN_5M_ASSETS == ("btc", "eth", "sol", "bnb", "xrp", "doge", "hype")
    assert set(config.ASSETS).issubset(set(config.KNOWN_5M_ASSETS))
    assert config._parse_asset_list("btc, ETH, sol, no-coin, btc") == ["btc", "eth", "sol"]


def test_per_coin_sigma_and_chop_scale_with_spot():
    btc = config.ASSET_PARAMS["btc"] if "btc" in config.ASSET_PARAMS else config._load_asset_params("btc")
    eth = config.ASSET_PARAMS["eth"] if "eth" in config.ASSET_PARAMS else config._load_asset_params("eth")
    xrp = config.ASSET_PARAMS["xrp"] if "xrp" in config.ASSET_PARAMS else config._load_asset_params("xrp")
    doge = config.ASSET_PARAMS["doge"] if "doge" in config.ASSET_PARAMS else config._load_asset_params("doge")

    sol = config.ASSET_PARAMS["sol"] if "sol" in config.ASSET_PARAMS else config._load_asset_params("sol")

    assert abs(btc.min_sigma_usd(78000.0) - 2.0) < 1e-6
    assert abs(btc.chop_noise_usd(78000.0) - 3.0) < 1e-6
    assert abs(btc.momentum_min_move_usd(78000.0) - 70.0) < 1e-6

    # Same bps, much smaller dollars on alts
    assert eth.min_sigma_usd(2500.0) < 0.1
    assert xrp.min_sigma_usd(1.33) < 0.001
    assert doge.min_z >= btc.min_z
    assert doge.max_buy_price <= btc.max_buy_price
    assert eth.daily_vol > btc.daily_vol
    # Altcoins require higher midband conviction than BTC
    assert doge.midband_min_z > btc.midband_min_z
    assert sol.midband_min_z > btc.midband_min_z
    assert btc.midband_min_z == 0.50
    assert doge.midband_min_z == 1.20


def test_feed_isolates_assets():
    stream = ChainlinkRTDSStream(assets=["btc", "eth"])
    stream._record(1000.0, 78000.0, asset="btc")
    stream._record(1000.0, 2500.0, asset="eth")
    stream._record(1100.0, 78100.0, asset="btc")
    stream._record(1100.0, 2510.0, asset="eth")
    assert abs(stream.price("btc") - 78100.0) < 1e-9
    assert abs(stream.price("eth") - 2510.0) < 1e-9
    btc_twap, _ = stream.compute_realized_twap(1000.0, 1100.0, asset="btc")
    eth_twap, _ = stream.compute_realized_twap(1000.0, 1100.0, asset="eth")
    assert abs(btc_twap - 78050.0) < 1e-6
    assert abs(eth_twap - 2505.0) < 1e-6


def test_slot_state_per_asset():
    d = Path(tempfile.mkdtemp())

    class _T:
        _slot_done = {"btc-updown-5m-1", "eth-updown-5m-1"}

        def _slot_state_path(self):
            return d / "slot_state.json"

        _load_slot_state = TWAPBot._load_slot_state
        _save_slot_state = TWAPBot._save_slot_state

    t = _T()
    TWAPBot._save_slot_state(t, slot_ts=123, slug="btc-updown-5m-1", strike=79551.8, asset="btc")
    TWAPBot._save_slot_state(t, slot_ts=123, slug="eth-updown-5m-1", strike=2501.5, asset="eth")
    data = TWAPBot._load_slot_state(t)
    assert abs(float(data["assets"]["btc"]["strike"]) - 79551.8) < 1e-6
    assert abs(float(data["assets"]["eth"]["strike"]) - 2501.5) < 1e-6
    TWAPBot._save_slot_state(t, slot_ts=123, slug="eth-updown-5m-1", strike=None, asset="eth")
    data2 = TWAPBot._load_slot_state(t)
    assert abs(float(data2["assets"]["eth"]["strike"]) - 2501.5) < 1e-6


def test_one_position_per_coin_independent():
    btc_p = config._load_asset_params("btc")
    eth_p = config._load_asset_params("eth")
    btc = CoinSlot(asset="btc", params=btc_p, engine=_engine_for(btc_p), position={"cost_usd": 7.5})
    eth = CoinSlot(asset="eth", params=eth_p, engine=_engine_for(eth_p))
    assert btc.position is not None
    assert eth.position is None


def test_risk_accounts_for_open_notional():
    d = Path(tempfile.mkdtemp())
    old_mode = config.MODE
    try:
        config.MODE = "paper_live"
        risk = RiskManager(state_dir=d)
        risk.bankroll = 50.0
        ok, _ = risk.can_trade(open_notional=0.0)
        assert ok
        ok2, reason = risk.can_trade(open_notional=46.0)
        assert not ok2
        assert "free bankroll" in reason
        stake = risk.calculate_stake(0.04, 0.80, open_notional=40.0)
        assert 0 < stake <= 10.0
    finally:
        config.MODE = old_mode


def test_max_concurrent_entries_count():
    class DummyBot:
        _slot_entries_count = TWAPBot._slot_entries_count

    b = DummyBot()
    b.slots = {
        "btc": CoinSlot(asset="btc", params=config._load_asset_params("btc"), engine=None, position={"shares": 5.0}),
        "eth": CoinSlot(asset="eth", params=config._load_asset_params("eth"), engine=None, position={"shares": 5.0}),
        "sol": CoinSlot(asset="sol", params=config._load_asset_params("sol"), engine=None, position={"shares": 5.0}),
        "bnb": CoinSlot(asset="bnb", params=config._load_asset_params("bnb"), engine=None, position=None),
    }
    assert b._slot_entries_count() == 3
    assert b._slot_entries_count() >= config.MAX_CONCURRENT_ENTRIES




if __name__ == "__main__":
    test_known_5m_assets()
    test_per_coin_sigma_and_chop_scale_with_spot()
    test_feed_isolates_assets()
    test_slot_state_per_asset()
    test_one_position_per_coin_independent()
    test_risk_accounts_for_open_notional()
    print("[PASS] test_multi_asset")
