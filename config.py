"""Configuration for Polymarket 5m bot (default: mid-band maker)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(override=False)

# --- Trading Mode & Asset ---
MODE = os.getenv("MODE", "paper_live").strip().lower()  # 'paper_live' (real data, paper fills) or 'live'
LIVE_CONFIRM = os.getenv("LIVE_CONFIRM", os.getenv("CONFIRM_LIVE", "no")).strip().lower()
TIMEFRAME = os.getenv("TIMEFRAME", "5m").strip().lower()
SLOT_SECONDS = int(os.getenv("SLOT_SECONDS", "300"))
MAX_ORACLE_STALE_S = float(os.getenv("MAX_ORACLE_STALE_S", os.getenv("ORACLE_STALE_S", os.getenv("MAX_STALE_S", "6.0"))))

# Polymarket 5-minute Up/Down coins (Chainlink TWAP settlement).
KNOWN_5M_ASSETS = ("btc", "eth", "sol", "bnb", "xrp", "doge", "hype")
# Default assets list excludes HYPE (3.5% daily vol, 33% win rate, high noise)
DEFAULT_5M_ASSETS = ("btc", "eth", "sol", "bnb", "xrp", "doge")


def _parse_asset_list(raw: str) -> list[str]:
    out: list[str] = []
    for tok in (raw or "").split(","):
        a = tok.strip().lower()
        if a in KNOWN_5M_ASSETS and a not in out:
            out.append(a)
    return out


_assets_env = os.getenv("ASSETS", "").strip()
if _assets_env:
    ASSETS = _parse_asset_list(_assets_env)
else:
    ASSETS = list(DEFAULT_5M_ASSETS)

_asset_env = os.getenv("ASSET", "").strip().lower()
if _asset_env in KNOWN_5M_ASSETS and not _assets_env:
    # ASSET=btc with no ASSETS list still means "all 5m coins" unless ASSETS is set.
    # Keep ASSET as the primary/display coin.
    pass
if not ASSETS:
    ASSETS = [_asset_env] if _asset_env in KNOWN_5M_ASSETS else ["btc"]
ASSET = _asset_env if _asset_env in ASSETS else ASSETS[0]

# --- WebSocket Endpoints & Hosts ---
CHAINLINK_RTDS_URL = os.getenv("CHAINLINK_RTDS_URL", "wss://ws-live-data.polymarket.com").strip()
CLOB_WS_URL = os.getenv("CLOB_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market").strip()
GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com").strip()
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com").strip()
HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0").strip() or "0.0.0.0"
HTTP_PORT = int(os.getenv("PORT") or os.getenv("HTTP_PORT") or "8080")
CHAIN_ID = int(os.getenv("CHAIN_ID", "137"))  # Polygon PoS

# --- Polymarket Authentication (for MODE=live) ---
PRIVATE_KEY = os.getenv("PRIVATE_KEY", os.getenv("POLYMARKET_PRIVATE_KEY", "")).strip()
FUNDER_ADDRESS = os.getenv("FUNDER_ADDRESS", os.getenv("POLYMARKET_FUNDER_ADDRESS", "")).strip()
SIGNATURE_TYPE = int(os.getenv("SIGNATURE_TYPE", os.getenv("POLYMARKET_SIGNATURE_TYPE", "3")))  # 3 = Polymarket Proxy Wallet

# --- Strategy Selection ---
STRATEGY = os.getenv("STRATEGY", "MIDBAND_MAKER").strip().upper()
# --- H-005 Momentum-Into-Close (Novals/igus-style) ---
MOMENTUM_WINDOW_START_S = float(os.getenv('MOMENTUM_WINDOW_START_S', '150.0'))
MOMENTUM_WINDOW_END_S = float(os.getenv('MOMENTUM_WINDOW_END_S', '60.0'))
MOMENTUM_MIN_MOVE_USD = float(os.getenv('MOMENTUM_MIN_MOVE_USD', '70.0'))
MOMENTUM_MAX_MOVE_USD = float(os.getenv('MOMENTUM_MAX_MOVE_USD', '300.0'))
MOMENTUM_MIN_ASK = float(os.getenv('MOMENTUM_MIN_ASK', '0.70'))
MOMENTUM_MAX_ASK = float(os.getenv('MOMENTUM_MAX_ASK', '0.92'))
MOMENTUM_MIN_EDGE = float(os.getenv('MOMENTUM_MIN_EDGE', '0.00'))
# Risk overlays for momentum (Novals conservative defaults)
MOMENTUM_EXIT_BEFORE_S = float(os.getenv("MOMENTUM_EXIT_BEFORE_S", "20.0"))
MOMENTUM_STOP_LOSS_PCT = float(os.getenv("MOMENTUM_STOP_LOSS_PCT", "0.25"))
MOMENTUM_STOP_LOSS_ENABLED = os.getenv("MOMENTUM_STOP_LOSS_ENABLED", "no").strip().lower() in ("yes", "y", "true", "1")
MOMENTUM_HEDGE_ENABLED = os.getenv("MOMENTUM_HEDGE_ENABLED", "yes").strip().lower() in ("yes", "y", "true", "1")
MOMENTUM_HEDGE_TRIGGER_ASK = float(os.getenv("MOMENTUM_HEDGE_TRIGGER_ASK", "0.95"))
MOMENTUM_HEDGE_TRIGGER_SECONDS_LEFT = float(os.getenv("MOMENTUM_HEDGE_TRIGGER_SECONDS_LEFT", "45.0"))
MOMENTUM_HEDGE_SHARE_PCT = float(os.getenv("MOMENTUM_HEDGE_SHARE_PCT", "3.0"))
MOMENTUM_HEDGE_NOTIONAL_MIN = float(os.getenv("MOMENTUM_HEDGE_NOTIONAL_MIN", "1.0"))
MOMENTUM_HEDGE_NOTIONAL_MAX = float(os.getenv("MOMENTUM_HEDGE_NOTIONAL_MAX", "2.0"))


# --- Mid-band maker (default) ---
# Resting post-only bids near mid while the market is still uncertain (~40-60c).
# Not a late Z-score taker sniper.
MIDBAND_LO = float(os.getenv("MIDBAND_LO", "0.40"))
MIDBAND_HI = float(os.getenv("MIDBAND_HI", "0.60"))
MIDBAND_HALF_SPREAD = float(os.getenv("MIDBAND_HALF_SPREAD", "0.02"))
MIDBAND_MIN_SECONDS_LEFT = float(os.getenv("MIDBAND_MIN_SECONDS_LEFT", "75"))
MIDBAND_MAX_SECONDS_LEFT = float(os.getenv("MIDBAND_MAX_SECONDS_LEFT", "270"))
MIDBAND_REQUOTE_SECONDS = float(os.getenv("MIDBAND_REQUOTE_SECONDS", "3"))
MIDBAND_MAX_SKEW_SHARES = float(os.getenv("MIDBAND_MAX_SKEW_SHARES", "40"))
# Paper: require the ask to trade down through our bid (no phantom fills).
MIDBAND_PAPER_REQUIRE_CROSS = os.getenv("MIDBAND_PAPER_REQUIRE_CROSS", "yes").strip().lower() in ("yes", "y", "true", "1")

# --- Legacy H-001 sniper params (retired; kept for reference only) ---
MIN_Z_SCORE = float(os.getenv("MIN_Z_SCORE", "1.65"))                # Require >=95.0% mathematical certainty (Z >= 1.65)
ALPHA = float(os.getenv("ALPHA", "1.75"))
MIN_SIGMA_USD = float(os.getenv("MIN_SIGMA_USD", "2.0"))
MAX_CHOP_CROSSES = int(os.getenv("MAX_CHOP_CROSSES", "2"))
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.035"))                     # Minimum +3.5% net profit edge
MIN_BUY_PRICE = float(os.getenv("MIN_BUY_PRICE", "0.20"))            # Capture discount mispricings down to $0.20
MAX_BUY_PRICE = float(os.getenv("MAX_BUY_PRICE", "0.92"))            # Strict ceiling: NEVER pay > $0.92
EVAL_WINDOW_START_S = float(os.getenv("EVAL_WINDOW_START_S", "270.0"))  # Start evaluating at t=30s (270s left)
EVAL_WINDOW_END_S = float(os.getenv("EVAL_WINDOW_END_S", "40.0"))        # Stop evaluating at t=260s (40s left)
TAKER_FEE_RATE = float(os.getenv("TAKER_FEE_RATE", "0.07"))

# --- H-004 Passive Maker Limit Model Parameters (Optional) ---
MIN_MAKER_EDGE = float(os.getenv("MIN_MAKER_EDGE", "0.05"))
MIN_MAKER_BID = float(os.getenv("MIN_MAKER_BID", "0.55"))
MAX_MAKER_BID = float(os.getenv("MAX_MAKER_BID", "0.90"))
MAKER_DWELL_SECONDS = float(os.getenv("MAKER_DWELL_SECONDS", "8.0"))
MAKER_REBATE_SHARE = float(os.getenv("MAKER_REBATE_SHARE", "0.20"))

# --- Risk & Sizing Parameters ---
STARTING_BANKROLL = float(os.getenv("STARTING_BANKROLL", "50.0"))
BASE_STAKE_USD = float(os.getenv("BASE_STAKE_USD", "5.0"))
MAX_STAKE_USD = float(os.getenv("MAX_STAKE_USD", "10.0"))
DAILY_LOSS_LIMIT_USD = float(os.getenv("DAILY_LOSS_LIMIT_USD", "15.0"))
MAX_CONSECUTIVE_LOSSES = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "4"))
MIN_ORDER_SHARES = float(os.getenv("MIN_ORDER_SHARES", "5.0"))
# Paper FOK must be fully fillable from the live book (no phantom size).
PAPER_FOK_REQUIRE_SIZE = os.getenv("PAPER_FOK_REQUIRE_SIZE", "yes").strip().lower() in ("yes", "y", "true", "1")
# Settle from Polymarket Gamma, not our local TWAP. Wait this long after the window.
RESOLUTION_TIMEOUT_S = float(os.getenv("RESOLUTION_TIMEOUT_S", "180.0"))
RESOLUTION_PRICE_MIN = float(os.getenv("RESOLUTION_PRICE_MIN", "0.99"))
# Cap how many coins can open new positions in a single 5-minute slot.
# Prevents correlated multi-asset blowups when all crypto trends together.
MAX_CONCURRENT_ENTRIES = int(os.getenv("MAX_CONCURRENT_ENTRIES", "3"))


def _fenv(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    return float(raw)


def _ienv(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return int(default)
    return int(raw)


# BTC-calibrated dollar gates expressed as bps of spot so alts scale.
# 2 USD / 78k, 3 USD / 78k, 70 USD / 78k, 300 USD / 78k
_BTC_REF_SPOT = 78000.0
DEFAULT_MIN_SIGMA_BPS = _fenv("MIN_SIGMA_BPS", 2.0 / _BTC_REF_SPOT * 1e4)
DEFAULT_CHOP_NOISE_BPS = _fenv("CHOP_NOISE_BPS", 3.0 / _BTC_REF_SPOT * 1e4)
DEFAULT_MOMENTUM_MIN_MOVE_BPS = _fenv("MOMENTUM_MIN_MOVE_BPS", 70.0 / _BTC_REF_SPOT * 1e4)
DEFAULT_MOMENTUM_MAX_MOVE_BPS = _fenv("MOMENTUM_MAX_MOVE_BPS", 300.0 / _BTC_REF_SPOT * 1e4)

# Per-coin realized-vol prior (alts chop more than BTC).
_DAILY_VOL = {
    "btc": 0.0106,
    "eth": 0.0140,
    "sol": 0.0220,
    "bnb": 0.0160,
    "xrp": 0.0200,
    "doge": 0.0300,
    "hype": 0.0350,
}
# Tighter ask ceiling on thinner books.
_MAX_BUY = {
    "btc": 0.92,
    "eth": 0.92,
    "sol": 0.88,   # was 0.92 — force cheaper entries (SOL lost at 0.91)
    "bnb": 0.92,
    "xrp": 0.88,   # was 0.90
    "doge": 0.85,   # was 0.88
    "hype": 0.85,   # was 0.90
}
# Higher Z on noisier coins to prevent coin-flip entries.
_MIN_Z = {
    "btc": 1.65,
    "eth": 1.65,
    "sol": 2.00,    # was 1.70 — lost at Z=1.94
    "bnb": 1.70,
    "xrp": 2.20,    # was 1.75 — lost at Z=7.5 but asset is too noisy
    "doge": 2.50,   # was 1.80 — lost at Z=4.1
    "hype": 2.50,   # was 1.80 — 33% WR, toxic
}


@dataclass(frozen=True, slots=True)
class AssetParams:
    """Per-coin sniper/maker/momentum gates. Dollar floors are bps of live spot."""

    symbol: str
    min_z: float
    min_edge: float
    min_buy_price: float
    max_buy_price: float
    min_sigma_bps: float
    chop_noise_bps: float
    max_chop_crosses: int
    daily_vol: float
    momentum_min_move_bps: float
    momentum_max_move_bps: float
    min_maker_edge: float
    min_maker_bid: float
    max_maker_bid: float

    def min_sigma_usd(self, spot: float) -> float:
        return max(1e-12, self.min_sigma_bps / 1e4 * float(spot))

    def chop_noise_usd(self, spot: float) -> float:
        return max(1e-12, self.chop_noise_bps / 1e4 * float(spot))

    def momentum_min_move_usd(self, spot: float) -> float:
        return self.momentum_min_move_bps / 1e4 * float(spot)

    def momentum_max_move_usd(self, spot: float) -> float:
        return self.momentum_max_move_bps / 1e4 * float(spot)

    def fmt_px(self, px: float) -> str:
        p = float(px)
        if p >= 1000:
            return f"{p:,.1f}"
        if p >= 10:
            return f"{p:,.2f}"
        if p >= 1:
            return f"{p:,.3f}"
        if p >= 0.1:
            return f"{p:.4f}"
        return f"{p:.6f}"


def _load_asset_params(symbol: str) -> AssetParams:
    u = symbol.upper()
    return AssetParams(
        symbol=symbol,
        min_z=_fenv(f"{u}_MIN_Z", _MIN_Z.get(symbol, MIN_Z_SCORE)),
        min_edge=_fenv(f"{u}_MIN_EDGE", MIN_EDGE),
        min_buy_price=_fenv(f"{u}_MIN_BUY_PRICE", MIN_BUY_PRICE),
        max_buy_price=_fenv(f"{u}_MAX_BUY_PRICE", _MAX_BUY.get(symbol, MAX_BUY_PRICE)),
        min_sigma_bps=_fenv(f"{u}_MIN_SIGMA_BPS", DEFAULT_MIN_SIGMA_BPS),
        chop_noise_bps=_fenv(f"{u}_CHOP_NOISE_BPS", DEFAULT_CHOP_NOISE_BPS),
        max_chop_crosses=_ienv(f"{u}_MAX_CHOP_CROSSES", MAX_CHOP_CROSSES),
        daily_vol=_fenv(f"{u}_DAILY_VOL", _DAILY_VOL.get(symbol, 0.0200)),
        momentum_min_move_bps=_fenv(f"{u}_MOMENTUM_MIN_MOVE_BPS", DEFAULT_MOMENTUM_MIN_MOVE_BPS),
        momentum_max_move_bps=_fenv(f"{u}_MOMENTUM_MAX_MOVE_BPS", DEFAULT_MOMENTUM_MAX_MOVE_BPS),
        min_maker_edge=_fenv(f"{u}_MIN_MAKER_EDGE", MIN_MAKER_EDGE),
        min_maker_bid=_fenv(f"{u}_MIN_MAKER_BID", MIN_MAKER_BID),
        max_maker_bid=_fenv(f"{u}_MAX_MAKER_BID", MAX_MAKER_BID),
    )


ASSET_PARAMS: dict[str, AssetParams] = {a: _load_asset_params(a) for a in ASSETS}

# --- State Directory ---
STATE_ROOT = os.getenv("STATE_ROOT", "").strip()
# Prefer explicit STATE_ROOT (e.g. Railway volume at /data) even before first mkdir
if STATE_ROOT:
    STATE_DIR = Path(STATE_ROOT)
else:
    STATE_DIR = Path("state")
STATE_DIR.mkdir(parents=True, exist_ok=True)
# --- Event journal (JSONL) ---
JOURNAL_PATH = os.getenv("JOURNAL_PATH", "").strip()
if not JOURNAL_PATH:
    JOURNAL_PATH = str(STATE_DIR / "journal.jsonl")
