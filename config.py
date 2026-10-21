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
# Default assets list excludes HYPE, XRP, and DOGE (high noise / thin CLOB liquidity)
DEFAULT_5M_ASSETS = ("btc", "eth", "sol", "bnb")


def _parse_asset_list(raw: str) -> list[str]:
    out: list[str] = []
    # Support both comma-separated and space-separated lists
    normalized = (raw or "").replace(",", " ")
    for tok in normalized.split():
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
CLOB_HOST = os.getenv("CLOB_HOST", os.getenv("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com")).strip()
HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0").strip() or "0.0.0.0"
HTTP_PORT = int(os.getenv("PORT") or os.getenv("HTTP_PORT") or "8080")
CHAIN_ID = int(os.getenv("CHAIN_ID", os.getenv("POLYMARKET_CHAIN_ID", "137")))  # Polygon PoS

# --- Polymarket Authentication (for MODE=live) ---
PRIVATE_KEY = os.getenv("PRIVATE_KEY", os.getenv("POLYMARKET_PRIVATE_KEY", "")).strip()
FUNDER_ADDRESS = os.getenv("FUNDER_ADDRESS", os.getenv("POLYMARKET_FUNDER_ADDRESS", "")).strip()
# Must be set explicitly for MODE=live: 0 = EOA, 1 = Polymarket proxy (email/Magic), 2 = Gnosis Safe,
# 3 = POLY_1271 deposit wallet (CLOB V2; needs py-clob-client-v2 - the 1c bot's wallet uses 3).
# No default and no silent fallback; the live client refuses to start without a valid value.
_SIG_RAW = os.getenv("SIGNATURE_TYPE", os.getenv("POLYMARKET_SIGNATURE_TYPE", "")).strip()
try:
    SIGNATURE_TYPE = int(_SIG_RAW) if _SIG_RAW != "" else None
except ValueError:
    SIGNATURE_TYPE = None
VALID_SIGNATURE_TYPES = (0, 1, 2, 3)

# --- Strategy Selection ---
STRATEGY = os.getenv("STRATEGY", "JEV_DYNAMIC").strip().upper()

# --- JEV_DYNAMIC Hybrid Adaptive Strategy Parameters ---
# Trades nearly every 5-minute round by continuously evaluating both maker and momentum
# opportunities across all active assets, filtered & sized by TypeSafe Jev.
DYNAMIC_MIDBAND_LO = float(os.getenv("DYNAMIC_MIDBAND_LO", "0.35"))
DYNAMIC_MIDBAND_HI = float(os.getenv("DYNAMIC_MIDBAND_HI", "0.65"))
DYNAMIC_HALF_SPREAD = float(os.getenv("DYNAMIC_HALF_SPREAD", "0.03"))
DYNAMIC_MIN_Z = float(os.getenv("DYNAMIC_MIN_Z", "1.00"))
DYNAMIC_WINDOW_START_S = float(os.getenv("DYNAMIC_WINDOW_START_S", "270.0"))
DYNAMIC_WINDOW_END_S = float(os.getenv("DYNAMIC_WINDOW_END_S", "60.0"))
DYNAMIC_MOMENTUM_MIN_MOVE_BPS = float(os.getenv("DYNAMIC_MOMENTUM_MIN_MOVE_BPS", "25.0"))
DYNAMIC_MOMENTUM_MIN_ASK = float(os.getenv("DYNAMIC_MOMENTUM_MIN_ASK", "0.38"))
DYNAMIC_MOMENTUM_MAX_ASK = float(os.getenv("DYNAMIC_MOMENTUM_MAX_ASK", "0.72"))
DYNAMIC_MAX_BOOK_SPREAD = float(os.getenv("DYNAMIC_MAX_BOOK_SPREAD", "0.05"))
DYNAMIC_ENTRY_COOLDOWN_S = float(os.getenv("DYNAMIC_ENTRY_COOLDOWN_S", "10.0"))

# --- AVN-MAS Dynamic Price-Time Feasibility Matrix Parameters ---
# Adaptive scaling with elapsed time t:
# - t < 30s: Entry prohibited (noise & initial spread formation)
# - 30s <= t < 150s: Move >= 2.0*sigma, max ask P_max = 0.72
# - 150s <= t < 200s: Move >= 1.6*sigma, max ask P_max = 0.72
# - 200s <= t < 240s: Move >= 1.3*sigma, max ask P_max = 0.72
# - t >= 240s (remaining <= 60s): Prohibit new entries
# Volatility normalization: sigma(t) = daily_vol * sqrt(t / 86400.0) * spot
DYNAMIC_MIN_ELAPSED_S = float(os.getenv("DYNAMIC_MIN_ELAPSED_S", "30.0"))
# Momentum taker only — maker mid-band may still quote earlier in the window.
DYNAMIC_MOMENTUM_MIN_ELAPSED_S = float(os.getenv("DYNAMIC_MOMENTUM_MIN_ELAPSED_S", "90.0"))
# --- Entry dead zone (JEV_DYNAMIC) ---
# No new entries / maker quotes for the listed legs while slot elapsed is in [LO, HI); resting maker bids
# are cancelled when the slot enters the zone. /trades (120): 120-180s lost on both legs
# (maker 13 tr -8.26, momentum 22 tr -11.92). Disable: ENTRY_BLOCK_ENABLED=0 or ENTRY_BLOCK_ELAPSED_HI=0.
ENTRY_BLOCK_ENABLED = os.getenv("ENTRY_BLOCK_ENABLED", "1").strip().lower() in ("yes", "y", "true", "1", "on")
# 2026-10-15: live default 70-300s (block all entries from 70s). Roll back: ENTRY_BLOCK_ELAPSED_LO=120 ENTRY_BLOCK_ELAPSED_HI=180, or git tag pre-70s-cutoff.
ENTRY_BLOCK_ELAPSED_LO = float(os.getenv("ENTRY_BLOCK_ELAPSED_LO", "70"))
ENTRY_BLOCK_ELAPSED_HI = float(os.getenv("ENTRY_BLOCK_ELAPSED_HI", "300"))
# 2026-10-15: no entry block during these UTC windows (18:30-00:30 UTC = 00:00-06:00 SLT, where late fills did better).
# Comma-separated HH:MM-HH:MM in UTC; wraps past midnight. Roll back (block all day): ENTRY_BLOCK_OFF_UTC=""
ENTRY_BLOCK_OFF_UTC = os.getenv("ENTRY_BLOCK_OFF_UTC", "18:30-00:30").strip()
ENTRY_BLOCK_LEGS = tuple(
    x.strip().lower() for x in os.getenv("ENTRY_BLOCK_LEGS", "momentum,maker").split(",") if x.strip()
)
# Momentum entry move reference. Markets settle on Chainlink 60s-TWAP (finalPrice vs priceToBeat),
# not the raw spot tick at slot open. "conservative" measures the entry move against both the spot
# strike and a TWAP strike estimate and uses the smaller magnitude (0 if they disagree on direction).
# "spot" restores the legacy spot-strike-only behaviour exactly.
MOMENTUM_MOVE_REF = os.getenv("MOMENTUM_MOVE_REF", "conservative").strip().lower()
# Window (seconds before slot open) averaged to estimate the TWAP-based price-to-beat.
SETTLEMENT_TWAP_WINDOW_S = float(os.getenv("SETTLEMENT_TWAP_WINDOW_S", "60"))
# --- JEV_DYNAMIC maker entry guards (see maker_guard.py) ---
# Maker z is also computed against the TWAP (settlement) strike; the bid is blocked when spot z and
# TWAP z disagree on direction or the TWAP z on our side is < MAKER_MIN_TWAP_SIGMA, and a resting bid
# is cancelled on an adverse TWAP-strike cross. Falls back to legacy behaviour without a TWAP strike.
MAKER_TWAP_GUARD = os.getenv("MAKER_TWAP_GUARD", "1").strip().lower() in ("yes", "y", "true", "1", "on")
MAKER_MIN_TWAP_SIGMA = float(os.getenv("MAKER_MIN_TWAP_SIGMA", "0.25"))
# After a maker guard (mid_drop, twap_cross, direction_conflict, adverse_spot_cross, twap_sigma) cancels or
# blocks a bid, no maker bid is posted in that slot for this many seconds (0 = off).
GUARD_COOLDOWN_S = float(os.getenv("GUARD_COOLDOWN_S", "15"))
# With the guard on, require a TWAP strike for the maker leg: no maker bids in a slot whose TWAP
# strike is unavailable (e.g. first slot after a restart) and cancel a resting bid if it goes missing.
# Off = legacy spot-strike fallback. Only effective when MAKER_TWAP_GUARD is on. Momentum unaffected.
MAKER_REQUIRE_TWAP = os.getenv("MAKER_REQUIRE_TWAP", "1").strip().lower() in ("yes", "y", "true", "1", "on")
# Adverse-selection guard: skip quoting / cancel resting bid (blocking the fill) when the favoured
# token's mid has dropped >= MAKER_MAX_MID_DROP_60S from its high over the last MAKER_MID_WINDOW_S.
MAKER_MAX_MID_DROP_60S = float(os.getenv("MAKER_MAX_MID_DROP_60S", "0.15"))
MAKER_MID_WINDOW_S = float(os.getenv("MAKER_MID_WINDOW_S", "60"))
# Pre-post falling-mid gate (default OFF = live behaviour unchanged). Before posting a NEW maker bid:
# block if the favoured mid is >= MAKER_POST_MAX_MID_DROP below its MAKER_MID_WINDOW_S high (0 = off),
# or if its range over the last MAKER_POST_STABLE_S seconds exceeds MAKER_POST_STABLE_MAX_RANGE (0 = off).
MAKER_POST_MAX_MID_DROP = float(os.getenv("MAKER_POST_MAX_MID_DROP", "0"))
MAKER_POST_STABLE_S = float(os.getenv("MAKER_POST_STABLE_S", "3"))
MAKER_POST_STABLE_MAX_RANGE = float(os.getenv("MAKER_POST_STABLE_MAX_RANGE", "0"))
# LIVE sharp-drop guard (default OFF): at maker post time skip the post if the favoured mid is
# >= SHARP_DROP_THRESHOLD below its max mid over the last SHARP_DROP_WINDOW_S (same 'drop' as maker_post_shadow).
SHARP_DROP_GUARD = os.getenv("SHARP_DROP_GUARD", "0").strip().lower() in ("yes", "y", "true", "1", "on")
SHARP_DROP_THRESHOLD = float(os.getenv("SHARP_DROP_THRESHOLD", "0.06"))
SHARP_DROP_WINDOW_S = float(os.getenv("SHARP_DROP_WINDOW_S", "60"))
# Shadow-only logging of the pre-post gate (never blocks): journal event maker_post_shadow on each maker bid post.
MAKER_POST_GATE_SHADOW = os.getenv("MAKER_POST_GATE_SHADOW", "1").strip().lower() in ("yes", "y", "true", "1", "on")
# inv12 LOG-ONLY traces (never change trading): resting-path ticks, post-fill path.
MAKER_REST_TRACE = os.getenv("MAKER_REST_TRACE", "1").strip().lower() in ("yes", "y", "true", "1", "on")
MAKER_POST_FILL_TRACE = os.getenv("MAKER_POST_FILL_TRACE", "1").strip().lower() in ("yes", "y", "true", "1", "on")
# inv14 LOG-ONLY: slot_trace (our side book every 5s to slot end) + chase_shadow (first ask in [.55,.65] <70s).
SLOT_TRACE = os.getenv("SLOT_TRACE", "1").strip().lower() in ("yes", "y", "true", "1", "on")
CHASE_SHADOW = os.getenv("CHASE_SHADOW", "1").strip().lower() in ("yes", "y", "true", "1", "on")
# inv15 LOG-ONLY: settle_z_shadow (z under the real settlement rule: 60s TWAP at end vs 60s TWAP before start), 1/slot/asset at ~60s.
SETTLE_Z_SHADOW = os.getenv("SETTLE_Z_SHADOW", "1").strip().lower() in ("yes", "y", "true", "1", "on")
# inv17 LOG-ONLY: late_window_shadow (one virtual maker bid per slot when ONLY the entry block zone stops a 70-120s maker entry).
LATE_WINDOW_SHADOW = os.getenv("LATE_WINDOW_SHADOW", "1").strip().lower() in ("yes", "y", "true", "1", "on")
# LOG-ONLY: settle_z2_shadow (retuned settle_z entry, virtual bid only, never orders; default OFF).
SETTLE_Z2_SHADOW = os.getenv("SETTLE_Z2_SHADOW", "0").strip().lower() in ("yes", "y", "true", "1", "on")
# LIVE: settle_z taker (default OFF). At the settle_z_shadow tick (first tick >=60s), if |settle_z|>=MIN_Z buy the
# settle_z side FOK at best ask (ask<=MAX_ASK, spread<=MAX_SPREAD), fixed SHARES (skip if SHARES*ask>MAX_USD), 1/slot,
# hold to expiry. Auto stop rule persisted in STATE_DIR/settle_z_taker_state.json. Roll back: SETTLE_Z_TAKER=0.
SETTLE_Z_TAKER = os.getenv("SETTLE_Z_TAKER", "0").strip().lower() in ("yes", "y", "true", "1", "on")
SETTLE_Z_TAKER_MIN_Z = float(os.getenv("SETTLE_Z_TAKER_MIN_Z", "0.5"))
SETTLE_Z_TAKER_MAX_ASK = float(os.getenv("SETTLE_Z_TAKER_MAX_ASK", "0.80"))
SETTLE_Z_TAKER_MAX_SPREAD = float(os.getenv("SETTLE_Z_TAKER_MAX_SPREAD", "0.02"))
SETTLE_Z_TAKER_SHARES = float(os.getenv("SETTLE_Z_TAKER_SHARES", "5"))
SETTLE_Z_TAKER_MAX_USD = float(os.getenv("SETTLE_Z_TAKER_MAX_USD", "4"))
SETTLE_Z_TAKER_USD = float(os.getenv("SETTLE_Z_TAKER_USD", "3"))  # legacy/unused (fixed-share sizing)
SETTLE_Z_TAKER_STOP_N = int(os.getenv("SETTLE_Z_TAKER_STOP_N", "40"))
SETTLE_Z_TAKER_STOP_MIN_WR = float(os.getenv("SETTLE_Z_TAKER_STOP_MIN_WR", "0.75"))
SETTLE_Z_TAKER_STOP_PNL = float(os.getenv("SETTLE_Z_TAKER_STOP_PNL", "-10"))
# LOG-ONLY: late fair-value taker shadow (BTC 5m, default OFF). Virtual FOK BUY only; live orders only via
# LATE_FV_LIVE below. Journals late_fv_shadow / late_fv_result / late_fv_scan.
LATE_FV_SHADOW = os.getenv("LATE_FV_SHADOW", "0").strip().lower() in ("yes", "y", "true", "1", "on")
LATE_FV_T_LO = float(os.getenv("LATE_FV_T_LO", "220"))
LATE_FV_T_HI = float(os.getenv("LATE_FV_T_HI", "265"))
LATE_FV_STEP = float(os.getenv("LATE_FV_STEP", "5"))
LATE_FV_K = float(os.getenv("LATE_FV_K", "0.9"))
LATE_FV_MIN_EDGE = float(os.getenv("LATE_FV_MIN_EDGE", "0.03"))
LATE_FV_MAX_ASK = float(os.getenv("LATE_FV_MAX_ASK", "0.97"))
LATE_FV_SHARES = float(os.getenv("LATE_FV_SHARES", "5"))
# LIVE late_fv (default OFF): when LATE_FV_SHADOW fires and LATE_FV_LIVE=1, place a real FOK BUY at the observed best
# ask via the settle_z taker FOK path, cap LATE_FV_MAX_USD per order, hold to settlement. Auto-stop when cumulative
# live P&L <= -LATE_FV_STOP_USD (persisted STATE_DIR/late_fv_state.json). Roll back: LATE_FV_LIVE=0.
LATE_FV_LIVE = os.getenv("LATE_FV_LIVE", "0").strip().lower() in ("yes", "y", "true", "1", "on")
LATE_FV_MAX_USD = float(os.getenv("LATE_FV_MAX_USD", "5"))
LATE_FV_STOP_USD = float(os.getenv("LATE_FV_STOP_USD", "6"))
# FOK BUY limit = min(ask + LATE_FV_SLIPPAGE, LATE_FV_MAX_ASK, 0.99); edge re-checked at the limit (default 0 = at ask).
LATE_FV_SLIPPAGE = float(os.getenv("LATE_FV_SLIPPAGE", "0"))
# Max entry price (ask) for a late_fv pick. A pick above it is not taken on that tick; later ticks keep scanning.
LATE_FV_MAX_PRICE = float(os.getenv("LATE_FV_MAX_PRICE", "0.97"))
# LOG-ONLY: sol_shadow (rule B virtual maker bid at 0.50 on the settle_z side for SOL 5m; SOL is never traded).
SOL_SHADOW = os.getenv("SOL_SHADOW", "1").strip().lower() in ("yes", "y", "true", "1", "on")
DYNAMIC_MAX_ELAPSED_S = float(os.getenv("DYNAMIC_MAX_ELAPSED_S", "240.0"))

# --- AVN-MAS Take-Profit & Adverse Stop-Loss Parameters ---
TAKE_PROFIT_BID = float(os.getenv("TAKE_PROFIT_BID", "0.95"))
TAKE_PROFIT_ASK = float(os.getenv("TAKE_PROFIT_ASK", "0.96"))
STOP_LOSS_SALVAGE_MIN_BID = float(os.getenv("STOP_LOSS_SALVAGE_MIN_BID", "0.10"))

# Urgent Late-Round Capital Salvage (remaining <= 45s)
STOP_LOSS_URGENT_SALVAGE_MIN_BID = float(os.getenv("STOP_LOSS_URGENT_SALVAGE_MIN_BID", "0.05"))
STOP_LOSS_PANIC_SALVAGE_MIN_BID = float(os.getenv("STOP_LOSS_PANIC_SALVAGE_MIN_BID", "0.02"))
STOP_LOSS_URGENT_PERSISTENCE_SECONDS = float(os.getenv("STOP_LOSS_URGENT_PERSISTENCE_SECONDS", "3.0"))
STOP_LOSS_URGENT_REMAINING_SECONDS = float(os.getenv("STOP_LOSS_URGENT_REMAINING_SECONDS", "45.0"))

# Anti-Settlement-Loss Zero Hurdle & Hard Expiry Flatten:
# If remaining <= 45s, noise hurdle drops to 0 (any adverse move across strike is actionable)
STOP_LOSS_ZERO_HURDLE_REMAINING_S = float(os.getenv("STOP_LOSS_ZERO_HURDLE_REMAINING_S", "45.0"))
# If remaining <= 20s, persistence drops to 0 (immediate panic flatten before 0.00 expiry)
STOP_LOSS_HARD_FLATTEN_REMAINING_S = float(os.getenv("STOP_LOSS_HARD_FLATTEN_REMAINING_S", "20.0"))

# --- Adverse Strike Stop-Loss Protection Filters ---
# Hysteresis hurdle: Spot must cross strike against position by >= STOP_LOSS_MIN_ADVERSE_SIGMA * sigma(t)
STOP_LOSS_MIN_ADVERSE_SIGMA = float(os.getenv("STOP_LOSS_MIN_ADVERSE_SIGMA", "0.50"))
# Persistence hurdle: Adverse cross must hold continuously for >= 10s to filter momentary wicks
STOP_LOSS_PERSISTENCE_SECONDS = float(os.getenv("STOP_LOSS_PERSISTENCE_SECONDS", "10.0"))
# Dual agreement: Realized TWAP must also cross strike against position
STOP_LOSS_REQUIRE_TWAP = os.getenv("STOP_LOSS_REQUIRE_TWAP", "yes").strip().lower() in ("yes", "y", "true", "1")

# --- Bid-based max-loss stop (price_stop), independent of spot/TWAP/sigma conditions ---
# Exit when the position token's own best bid (no complement inference) stays <= entry * PRICE_STOP_FRACTION
# for >= PRICE_STOP_PERSISTENCE_S. Paper exits sell into the real bid book (VWAP over bid levels for our size).
# Default OFF: a replay of 43 trades on Polymarket trade prints showed it stops out more winners than it saves
# at every fraction tested (0.45-0.70). Enable with PRICE_STOP_ENABLED=1.
PRICE_STOP_ENABLED = os.getenv("PRICE_STOP_ENABLED", "no").strip().lower() in ("yes", "y", "true", "1", "on")
PRICE_STOP_FRACTION = float(os.getenv("PRICE_STOP_FRACTION", "0.50"))
PRICE_STOP_PERSISTENCE_S = float(os.getenv("PRICE_STOP_PERSISTENCE_S", "5.0"))

# --- Maker hold-to-expiry ---
# When on, maker positions (strategy contains "MAKER") skip trailing profit, the price stop, the adverse
# stop-loss / urgent / hard-flatten block and the failsafe orderbook flatten, and settle at expiry against the
# official outcome. Take-profit (TAKE_PROFIT_BID/ASK, 0.95) still fires. Momentum is unchanged.
# Replay (110 maker trades, both accounts): hold-to-expiry +$28.25 vs actual exits (weak evidence, paper only).
# Rollback: unset MAKER_HOLD_TO_EXPIRY or set it to 0.
MAKER_HOLD_TO_EXPIRY = os.getenv("MAKER_HOLD_TO_EXPIRY", "no").strip().lower() in ("yes", "y", "true", "1", "on")

# --- Dynamic Trailing Profit Stop Parameters ---
# Armed when peak bid reaches >= 0.83, trails by 0.08 to lock in profit before expiry
TRAILING_PROFIT_ENABLED = os.getenv("TRAILING_PROFIT_ENABLED", os.getenv("TRAILING_BREAKEVEN_ENABLED", "yes")).strip().lower() in ("yes", "y", "true", "1")
TRAILING_PROFIT_DISTANCE = float(os.getenv("TRAILING_PROFIT_DISTANCE", "0.08"))
TRAILING_PROFIT_MIN_PEAK = float(os.getenv("TRAILING_PROFIT_MIN_PEAK", "0.83"))

# Legacy aliases for backward compatibility
TRAILING_BREAKEVEN_ENABLED = TRAILING_PROFIT_ENABLED
TRAILING_BREAKEVEN_TRIGGER = float(os.getenv("TRAILING_BREAKEVEN_TRIGGER", "0.85"))
TRAILING_BREAKEVEN_BUFFER = float(os.getenv("TRAILING_BREAKEVEN_BUFFER", "0.01"))


def compute_dynamic_feasibility(
    elapsed_s: float, spot: float, daily_vol: float
) -> tuple[bool, float, float, float]:
    """
    Compute AVN-MAS Dynamic Price-Time Feasibility.
    Returns:
        (is_allowed: bool, min_move_usd: float, max_allowed_ask: float, sigma_t_usd: float)
    """
    t = float(elapsed_s)
    s = float(spot)
    vol = float(daily_vol)

    if t < DYNAMIC_MIN_ELAPSED_S or t >= DYNAMIC_MAX_ELAPSED_S or s <= 0.0 or vol <= 0.0:
        return False, 0.0, 0.0, 0.0

    sigma_t = vol * ((t / 86400.0) ** 0.5) * s

    if t < 150.0:
        mult, max_ask = 2.0, 0.72
    elif t < 200.0:
        mult, max_ask = 1.6, 0.72
    else:  # 200.0 <= t < 240.0
        mult, max_ask = 1.3, 0.72

    max_ask = min(max_ask, DYNAMIC_MOMENTUM_MAX_ASK)
    min_move_usd = mult * sigma_t
    return True, min_move_usd, max_ask, sigma_t


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
# Uses TWAP Z-score for directional bias — only bids the side spot is trending toward.
MIDBAND_LO = float(os.getenv("MIDBAND_LO", "0.40"))
MIDBAND_HI = float(os.getenv("MIDBAND_HI", "0.52"))             # Ceiling at 52c so we never pay > 48c (positive payout asymmetry)
MIDBAND_HALF_SPREAD = float(os.getenv("MIDBAND_HALF_SPREAD", "0.04"))
MIDBAND_MIN_Z = float(os.getenv("MIDBAND_MIN_Z", "0.50"))       # Minimum Z-score for directional conviction
MIDBAND_MIN_SECONDS_LEFT = float(os.getenv("MIDBAND_MIN_SECONDS_LEFT", "75"))
MIDBAND_MAX_SECONDS_LEFT = float(os.getenv("MIDBAND_MAX_SECONDS_LEFT", "210")) # Wait >=90s of TWAP path before quoting
MIDBAND_REQUOTE_SECONDS = float(os.getenv("MIDBAND_REQUOTE_SECONDS", "3"))
MIDBAND_MAX_SKEW_SHARES = float(os.getenv("MIDBAND_MAX_SKEW_SHARES", "40"))
# Paper: require the ask to trade down through our bid (no phantom fills).
MIDBAND_PAPER_REQUIRE_CROSS = os.getenv("MIDBAND_PAPER_REQUIRE_CROSS", "yes").strip().lower() in ("yes", "y", "true", "1")
# Reject entry when CLOB is illiquid (bid=0.01, ask=0.99 → spread=0.98 >> threshold).
# Set to 0.20 so any book with real two-sided depth (ask-bid ≤ 20¢) is accepted.
MIDBAND_MAX_BOOK_SPREAD = float(os.getenv("MIDBAND_MAX_BOOK_SPREAD", "0.20"))

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
BASE_STAKE_USD = float(os.getenv("BASE_STAKE_USD", "3.0"))
MAX_STAKE_USD = float(os.getenv("MAX_STAKE_USD", "3.0"))
DAILY_LOSS_LIMIT_USD = float(os.getenv("DAILY_LOSS_LIMIT_USD", os.getenv("MAX_DAILY_LOSS_USD", "18.0")))
MAX_CONSECUTIVE_LOSSES = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "1000"))
MIN_ORDER_SHARES = float(os.getenv("MIN_ORDER_SHARES", os.getenv("POLYMARKET_MIN_SHARES", "5.0")))
# Live: USDC kept untouched in the (possibly shared) wallet; bankroll = wallet balance - reserve.
WALLET_RESERVE_USD = float(os.getenv("WALLET_RESERVE_USD", "12.8"))
# Hard per-order USD cap for BUYs (0 = disabled). If MIN_ORDER_SHARES x price exceeds it, the trade is skipped.
CANCEL_CONFIRM_RETRIES = int(os.getenv("CANCEL_CONFIRM_RETRIES", "6"))
CANCEL_CONFIRM_POLL_S = float(os.getenv("CANCEL_CONFIRM_POLL_S", "0.5"))
MAX_TRADE_USD = float(os.getenv("MAX_TRADE_USD", "3.0"))
# Paper FOK must be fully fillable from the live book (no phantom size).
PAPER_FOK_REQUIRE_SIZE = os.getenv("PAPER_FOK_REQUIRE_SIZE", "yes").strip().lower() in ("yes", "y", "true", "1")
# Settle from Polymarket Gamma, not our local TWAP. Wait this long after the window.
RESOLUTION_TIMEOUT_S = float(os.getenv("RESOLUTION_TIMEOUT_S", "180.0"))
RESOLUTION_PRICE_MIN = float(os.getenv("RESOLUTION_PRICE_MIN", "0.99"))
# After RESOLUTION_TIMEOUT_S keep polling (Gamma + CLOB winner flag) every RESOLUTION_POLL_INTERVAL_S
# until RESOLUTION_MAX_WAIT_S; only then record the fill as unresolved (repaired on next startup).
RESOLUTION_MAX_WAIT_S = float(os.getenv("RESOLUTION_MAX_WAIT_S", "900.0"))
RESOLUTION_POLL_INTERVAL_S = float(os.getenv("RESOLUTION_POLL_INTERVAL_S", "15.0"))
# On startup, re-check trades.json rows recorded as "unresolved" and book their official PnL.
BACKFILL_UNRESOLVED_ON_START = os.getenv("BACKFILL_UNRESOLVED_ON_START", "yes").strip().lower() in ("yes", "y", "true", "1")
# --- live-mode safety (no effect in paper) ---
# Poll the exchange for resting-order status (fills / partials) at most this often.
LIVE_ORDER_POLL_S = float(os.getenv("LIVE_ORDER_POLL_S", "2.0"))
# Live: sell a held position this many seconds before expiry when its bid is >= LIVE_PRE_EXPIRY_MIN_BID,
# so winners are cashed on the CLOB instead of needing an on-chain redeem. Losers (low bid) settle at 0.
LIVE_PRE_EXPIRY_EXIT_S = float(os.getenv("LIVE_PRE_EXPIRY_EXIT_S", "20.0"))
LIVE_PRE_EXPIRY_MIN_BID = float(os.getenv("LIVE_PRE_EXPIRY_MIN_BID", "0.90"))
# Dip-bounce exit (maker positions): once the held token's best bid has been <= DIP_BOUNCE_DIP (on a sane
# book, spread <= 0.15), sell the whole position via the normal early-exit path when its own-book best bid
# recovers to >= DIP_BOUNCE_SELL. Take-profit (0.95) is checked first and is unchanged. Default off.
DIP_BOUNCE_EXIT = os.getenv("DIP_BOUNCE_EXIT", "0").strip().lower() in ("yes", "y", "true", "1", "on")
DIP_BOUNCE_DIP = float(os.getenv("DIP_BOUNCE_DIP", "0.35"))
DIP_BOUNCE_SELL = float(os.getenv("DIP_BOUNCE_SELL", "0.80"))
DIP_BOUNCE_MAX_ATTEMPTS = int(os.getenv("DIP_BOUNCE_MAX_ATTEMPTS", "3"))
# Live: refresh bankroll from the wallet's CLOB collateral balance this often (only while flat).
LIVE_BANKROLL_REFRESH_S = float(os.getenv("LIVE_BANKROLL_REFRESH_S", "60.0"))
# Cap how many coins can open new positions in a single 5-minute slot.
# Increased to 3 for JEV_DYNAMIC mode to participate actively across coins.
MAX_CONCURRENT_ENTRIES = int(os.getenv("MAX_CONCURRENT_ENTRIES", "1"))


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

# Per-coin realized-vol prior (alts chop more than BTC; BTC tightened from 0.0106 to prevent sub-5bps noise triggers).
_DAILY_VOL = {
    "btc": 0.0135,
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
    "btc": 1.95,    # was 1.65 — BTC 5m chop requires higher directional conviction
    "eth": 1.65,
    "sol": 2.00,    # was 1.70 — lost at Z=1.94
    "bnb": 1.70,
    "xrp": 2.20,    # was 1.75 — lost at Z=7.5 but asset is too noisy
    "doge": 2.50,   # was 1.80 — lost at Z=4.1
    "hype": 2.50,   # was 1.80 — 33% WR, toxic
}
# Per-coin minimum Z-score for mid-band maker (higher conviction on noisy alts).
_MIDBAND_MIN_Z = {
    "btc": 1.00,    # solid major
    "eth": 1.00,    # solid major (75% WR on live trades)
    "sol": 1.25,    # altcoin, filter weak flips
    "bnb": 1.50,    # altcoin, high noise hurdle
    "xrp": 1.50,    # altcoin, high noise hurdle
    "doge": 1.50,   # meme coin, high noise hurdle
    "hype": 1.50,
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
    midband_min_z: float = 0.50

    def min_sigma_usd(self, spot: float) -> float:
        return max(1e-12, self.min_sigma_bps / 1e4 * float(spot))

    def chop_noise_usd(self, spot: float) -> float:
        return max(1e-12, self.chop_noise_bps / 1e4 * float(spot))

    def momentum_min_move_usd(self, spot: float) -> float:
        return self.momentum_min_move_bps / 1e4 * float(spot)

    def dynamic_momentum_min_move_usd(self, spot: float, elapsed_s: float | None = None) -> float:
        if elapsed_s is not None:
            _, min_move, _, _ = compute_dynamic_feasibility(elapsed_s, spot, self.daily_vol)
            return min_move
        return DYNAMIC_MOMENTUM_MIN_MOVE_BPS / 1e4 * float(spot)

    def dynamic_feasibility(self, spot: float, elapsed_s: float) -> tuple[bool, float, float, float]:
        return compute_dynamic_feasibility(elapsed_s, spot, self.daily_vol)

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
        midband_min_z=_fenv(f"{u}_MIDBAND_MIN_Z", _MIDBAND_MIN_Z.get(symbol, MIDBAND_MIN_Z)),
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

# --- TypeSafe Jev System One AI Guardian ---
TYPESAFE_API_KEY = os.getenv("TYPESAFE_API_KEY", "").strip()
TYPESAFE_BASE_URL = os.getenv("TYPESAFE_BASE_URL", "").strip() or None
JEV_ENABLED = os.getenv("JEV_ENABLED", "yes").strip().lower() in ("yes", "y", "true", "1") and bool(TYPESAFE_API_KEY)
JEV_MODEL = os.getenv("JEV_MODEL", "jev-latest").strip()
JEV_TIMEOUT_S = _fenv("JEV_TIMEOUT_S", 1.2)
JEV_MAX_REVERSAL_NOUL = _fenv("JEV_MAX_REVERSAL_NOUL", 0.32)
JEV_MAX_FAKEOUT_NOUL = _fenv("JEV_MAX_FAKEOUT_NOUL", 0.35)
JEV_MIN_CONVICTION_SCORE = _fenv("JEV_MIN_CONVICTION_SCORE", 1.65)
JEV_MIN_DOWNSIZE_CONVICTION = _fenv("JEV_MIN_DOWNSIZE_CONVICTION", 1.15)
JEV_DOWNSIZE_FACTOR = _fenv("JEV_DOWNSIZE_FACTOR", 0.50)
JEV_ALLOW_DOWNSIZING = os.getenv("JEV_ALLOW_DOWNSIZING", "true").strip().lower() in ("true", "1", "yes")
# JEV_SHADOW: consult Jev and log/journal/tag its real verdict, but never enforce it
# (every consult proceeds as an approve at size 1.0; timeouts/errors never block).
JEV_SHADOW = os.getenv("JEV_SHADOW", "no").strip().lower() in ("yes", "y", "true", "1", "on")
# late_fv spot source: chainlink (default) | binance. With binance, strike, spot AND vol all come from the Binance
# BTCUSDT feed (never mixed with Chainlink). Both sources are always computed and logged side by side in shadow.
LATE_FV_SPOT_SOURCE = (os.getenv("LATE_FV_SPOT_SOURCE", "chainlink").strip().lower() or "chainlink")
if LATE_FV_SPOT_SOURCE not in ("chainlink", "binance"):
    LATE_FV_SPOT_SOURCE = "chainlink"
LATE_FV_BINANCE_MAX_AGE_S = float(os.getenv("LATE_FV_BINANCE_MAX_AGE_S", "1.0"))
BINANCE_WS_URLS = [u.strip() for u in os.getenv(
    "BINANCE_WS_URLS",
    "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker,"
    "wss://data-stream.binance.vision/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker").split(",") if u.strip()]
BINANCE_REST_URLS = [u.strip() for u in os.getenv(
    "BINANCE_REST_URLS", "https://data-api.binance.vision,https://api.binance.com").split(",") if u.strip()]
