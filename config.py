"""Central configuration for the Polymarket BTC 5-minute up/down bot.

All live-trading secrets come from environment variables (.env). Nothing
sensitive is hard-coded. See .env.example.

MODES
-----
- paper       : offline random-walk data + simulated fills (no network, no money)
- paper_live  : REAL BTC prices + REAL Polymarket order-book quotes, but
                simulated fills (no wallet/private key needed, no money at risk)
- live        : real data + REAL orders on Polymarket (needs funded wallet)

The target market's token IDs are discovered from the Gamma API at the start of
every round, so the bot always trades the current 5-minute slot automatically.

LIVE SAFETY
-----------
Mode=live automatically applies a conservative risk profile (tiny fixed stakes,
strict edge/EV, Chainlink-first prices, trade-window limits) unless you set
LIVE_AGGRESSIVE=1. You must also set LIVE_CONFIRM=yes.
"""
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:  # python-dotenv optional for the offline simulator
    pass


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


# --- Mode ---
MODE = os.getenv("MODE", "paper")  # paper | paper_live | live
# Strategy:
#   mid         = mid-slot value band (default; edge vs strike + model)
#   late_scalp  = selective late favorites (stricter gates)
#   late_every  = late scalp EVERY 5m slot (relaxed; more activity/risk)
#   hybrid      = late preferred; mid only if exceptional, else late same slot
STRATEGY = (os.getenv("STRATEGY") or "mid").strip().lower()

# --- Polymarket / chain ---
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")
CHAIN_ID = int(os.getenv("CHAIN_ID", "137"))  # Polygon mainnet
# Authenticated CLOB httpx timeout (py-clob-client-v2 default is too tight).
CLOB_HTTP_TIMEOUT = float(os.getenv("CLOB_HTTP_TIMEOUT", "30"))
# Min seconds between update_balance_allowance calls (dashboard polls often).
CLOB_BALANCE_REFRESH_S = float(os.getenv("CLOB_BALANCE_REFRESH_S", "60"))
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
FUNDER_ADDRESS = os.getenv("FUNDER_ADDRESS")
SIGNATURE_TYPE = int(os.getenv("SIGNATURE_TYPE", "3"))  # 0=EOA, 1=POLY_PROXY, 2=Safe, 3=deposit/EIP-1271
# Optional: pre-created CLOB API creds (skips create/derive if all three set).
POLY_API_KEY = (os.getenv("POLY_API_KEY") or "").strip() or None
POLY_API_SECRET = (os.getenv("POLY_API_SECRET") or "").strip() or None
POLY_API_PASSPHRASE = (os.getenv("POLY_API_PASSPHRASE") or "").strip() or None
# Hard gate for real-money mode: must be "yes" in .env
LIVE_CONFIRM = (os.getenv("LIVE_CONFIRM") or "").strip().lower()
# If 1, skip auto-tightening of live risk defaults (not recommended).
LIVE_AGGRESSIVE = _env_bool("LIVE_AGGRESSIVE", False)

# --- Market discovery (deterministic) ---
# Polymarket's 5-minute BTC markets are events whose slug encodes the slot's
# Unix start time, e.g. "btc-updown-5m-1771582500" (always a multiple of 300).
# The bot computes the current slot as floor(now/SLOT_SECONDS)*SLOT_SECONDS and
# fetches that event by slug. Override the prefix only if Polymarket renames it.
SLOT_SECONDS = int(os.getenv("SLOT_SECONDS", "300"))  # 5-minute slots
BTC_SLUG_PREFIX = os.getenv("BTC_SLUG_PREFIX", "btc-updown-5m-")

# --- Market discovery (fuzzy fallback only) ---
BTC_MARKET_ASSET_TERMS = [s.strip().lower() for s in os.getenv(
    "BTC_MARKET_ASSET_TERMS", "btc,bitcoin").split(",") if s.strip()]
BTC_MARKET_UPDOWN_TERMS = [s.strip().lower() for s in os.getenv(
    "BTC_MARKET_UPDOWN_TERMS",
    "up or down,up-or-down,updown,5 min,5-min,5min,next 5").split(",") if s.strip()]
BTC_MARKET_MATCH = os.getenv("BTC_MARKET_MATCH", "").strip().lower()
SLOT_MIN_SECONDS = int(os.getenv("SLOT_MIN_SECONDS", "180"))
SLOT_MAX_SECONDS = int(os.getenv("SLOT_MAX_SECONDS", "420"))
# Skip slots with too little time left (need room to decide + place).
MIN_SLOT_SECONDS = int(os.getenv("MIN_SLOT_SECONDS", "90"))
# Also skip if TOO much time left (opening books are often empty/noisy).
# Trade only when remaining is in [MIN_SLOT_SECONDS, MAX_TRADE_REMAINING].
MAX_TRADE_REMAINING = int(os.getenv("MAX_TRADE_REMAINING", "240"))
# Discovery: only prefer the *next* slot when current remaining is below this.
# Must stay LOW (≤ late window) or hybrid never reaches late_scalp (was wrongly
# tied to MIN_SLOT_SECONDS=90, which abandoned slots ~100s early).
MARKET_ADVANCE_REMAINING = int(os.getenv("MARKET_ADVANCE_REMAINING", "5"))

# --- BTC price feed (real data) ---
# Polymarket 5m BTC markets resolve on Chainlink BTC/USD — put chainlink first
# for live accuracy. Public endpoints, no key.
BTC_PRICE_SOURCES = [s.strip() for s in os.getenv(
    "BTC_PRICE_SOURCES",
    "chainlink_stream,chainlink,pyth,coinbase,binance,kraken").split(",") if s.strip()]

# --- Strategy (stricter mid defaults — fewer, higher-conviction mid entries) ---
ROUND_SECONDS = int(os.getenv("ROUND_SECONDS", "300"))
MOMENTUM_LOOKBACK = int(os.getenv("MOMENTUM_LOOKBACK", "12"))
SAMPLE_SECONDS = float(os.getenv("SAMPLE_SECONDS", "5"))
EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "0.04"))
MIN_EV = float(os.getenv("MIN_EV", "0.02"))
VOL_BLEND = float(os.getenv("VOL_BLEND", "0.5"))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.08"))
# Assumed / buffer for fees + adverse selection (subtracted from model edge).
FEE_BUFFER = float(os.getenv("FEE_BUFFER", "0.01"))
# Only buy contracts in this ask band (avoid expensive favorites / lottery tickets).
MIN_ENTRY_PRICE = float(os.getenv("MIN_ENTRY_PRICE", "0.22"))
MAX_ENTRY_PRICE = float(os.getenv("MAX_ENTRY_PRICE", "0.72"))
# Require signal confidence and a clear move vs strike (in $) before trading.
MIN_CONFIDENCE = float(os.getenv("MIN_CONFIDENCE", "0.40"))
MIN_MOVE_USD = float(os.getenv("MIN_MOVE_USD", "5.0"))  # |spot - strike|
# Minimum size sitting on the best ask (shares); 0 = disabled (paper-friendly).
MIN_ASK_SIZE = float(os.getenv("MIN_ASK_SIZE", "0"))

# Mid-only quality guards (STRATEGY=mid). Archive mid was ~44% WR / flat PnL;
# skip overconfident cheap tickets and soft edges.
MID_MIN_MODEL = float(os.getenv("MID_MIN_MODEL", "0.52"))
MID_MAX_MODEL = float(os.getenv("MID_MAX_MODEL", "0.95"))
# If ask is this cheap AND model above this -> skip (0 / 1 = disabled).
MID_CHEAP_ASK = float(os.getenv("MID_CHEAP_ASK", "0.0"))
MID_CHEAP_MAX_MODEL = float(os.getenv("MID_CHEAP_MAX_MODEL", "1.0"))
MID_REQUOTE = _env_bool("MID_REQUOTE", True)
MID_REQUOTE_SLEEP_S = float(os.getenv("MID_REQUOTE_SLEEP_S", "0.40"))
MID_MAX_ASK_JUMP = float(os.getenv("MID_MAX_ASK_JUMP", "0.02"))
MID_PAUSE_SLOTS_AFTER_LOSS = int(os.getenv("MID_PAUSE_SLOTS_AFTER_LOSS", "1"))
MID_MAX_STAKE_USD = float(os.getenv("MID_MAX_STAKE_USD", "4.0"))
# Fair-value: used for edge sizing/logging. Noise floor off when CAP=0.
MID_USE_FAIR_VALUE = _env_bool("MID_USE_FAIR_VALUE", True)
MID_Z_MIN = float(os.getenv("MID_Z_MIN", "0.15"))
# Cap on Z*sigma floor (USD). 0 = do not raise min move above MIN_MOVE_USD.
MID_NOISE_MOVE_CAP = float(os.getenv("MID_NOISE_MOVE_CAP", "0"))
MID_REQUIRE_SIGNIFICANT = _env_bool("MID_REQUIRE_SIGNIFICANT", False)
MID_MAX_CHOP_CROSSES = int(os.getenv("MID_MAX_CHOP_CROSSES", "6"))
# Paper default: allow proxy strike (Chainlink open often missing if bot joins late)
MID_REQUIRE_CHAINLINK_STRIKE = _env_bool("MID_REQUIRE_CHAINLINK_STRIKE", False)
# Pessimistic paper fills: add this to ask for simulated entry (Tier 1 realism).
PAPER_FILL_SLIPPAGE = float(os.getenv("PAPER_FILL_SLIPPAGE", "0.01"))
# Book quality (Tier 2)
MID_MIN_BID_ASK_RATIO = float(os.getenv("MID_MIN_BID_ASK_RATIO", "0.0"))  # 0=off
# Both-side arb scanner: log when ask_UP + ask_DOWN < 1 - fee (Tier 2)
ARB_SCAN_ENABLED = _env_bool("ARB_SCAN_ENABLED", True)
ARB_MIN_EDGE = float(os.getenv("ARB_MIN_EDGE", "0.02"))  # 1 - sum asks
# Shadow journal: log every mid decision even when not trading
DECISION_JOURNAL = _env_bool("DECISION_JOURNAL", True)

# --- Late-scalp strategy (STRATEGY=late_scalp / hybrid late path) ---
# Tighter defaults: harder to lose (stronger favorites, later window) + better
# payoff than ultra-rich 0.97+ asks. R13-style 0.89 losses are filtered out.
# Enter only when remaining seconds in [LATE_SCALP_MIN_REMAINING, LATE_SCALP_MAX_REMAINING]
# and the favorite ask is in [LATE_SCALP_MIN_ENTRY, LATE_SCALP_MAX_ENTRY].
LATE_SCALP_MIN_REMAINING = int(os.getenv("LATE_SCALP_MIN_REMAINING", "8"))
LATE_SCALP_MAX_REMAINING = int(os.getenv("LATE_SCALP_MAX_REMAINING", "50"))
# Default band wide enough for every-slot mode (clamp if needed).
# Min favorite ask — below this is near coin-flip (R5 lost @ 0.57).
LATE_SCALP_MIN_ENTRY = float(os.getenv("LATE_SCALP_MIN_ENTRY", "0.60"))
LATE_SCALP_MAX_ENTRY = float(os.getenv("LATE_SCALP_MAX_ENTRY", "0.95"))
# Prefer entries at/under this ask (ignored when LATE_SCALP_EVERY_SLOT=1).
LATE_SCALP_PREFER_MAX = float(os.getenv("LATE_SCALP_PREFER_MAX", "0.95"))
# Minimum gap between favorite ask and underdog ask (favorite clearly leading).
LATE_SCALP_MIN_FAVORITE_LEAD = float(os.getenv("LATE_SCALP_MIN_FAVORITE_LEAD", "0.05"))
# Underdog ask must be this low (0 = off in every-slot).
LATE_SCALP_MAX_UNDERDOG_ASK = float(os.getenv("LATE_SCALP_MAX_UNDERDOG_ASK", "0.42"))
# Require spot vs strike to agree with the favorite side (skip book/spot fights).
LATE_SCALP_REQUIRE_SPOT_ALIGN = _env_bool("LATE_SCALP_REQUIRE_SPOT_ALIGN", False)
LATE_SCALP_SPOT_ALIGN_EPS_USD = float(os.getenv("LATE_SCALP_SPOT_ALIGN_EPS_USD", "8.0"))
# Re-check book after a short pause; skip if favorite flips or ask jumps.
LATE_SCALP_REQUOTE = _env_bool("LATE_SCALP_REQUOTE", False)
LATE_SCALP_REQUOTE_SLEEP_S = float(os.getenv("LATE_SCALP_REQUOTE_SLEEP_S", "0.25"))
LATE_SCALP_MAX_ASK_JUMP = float(os.getenv("LATE_SCALP_MAX_ASK_JUMP", "0.10"))
# If requote spikes higher, keep original limit when it is still in the sweet band.
LATE_SCALP_KEEP_FIRST_ASK = _env_bool("LATE_SCALP_KEEP_FIRST_ASK", True)
# After a late loss, skip this many subsequent late windows (cooldown).
LATE_SCALP_PAUSE_SLOTS_AFTER_LOSS = int(os.getenv("LATE_SCALP_PAUSE_SLOTS_AFTER_LOSS", "0"))
# Cap stake even after min-share bump (5 * 0.99 = 4.95).
LATE_SCALP_MAX_STAKE_USD = float(os.getenv("LATE_SCALP_MAX_STAKE_USD", "5.00"))
# Prefer slightly wider late max-spread than mid (books thin near end).
LATE_SCALP_MAX_SPREAD = float(os.getenv("LATE_SCALP_MAX_SPREAD", "0.15"))
# 1 = try to place a late trade every 5m slot (relaxed gates; more risk).
LATE_SCALP_EVERY_SLOT = _env_bool("LATE_SCALP_EVERY_SLOT", True)

# --- Late-scalp v2 improvements (smart filters) ---
# Minimum |spot − strike| move in USD before entering. Skip coin-flip slots.
LATE_SCALP_MIN_MOVE_USD = float(os.getenv("LATE_SCALP_MIN_MOVE_USD", "8.0"))
# Require Chainlink Data Stream tick for strike (refuse exchange-proxy strikes).
LATE_SCALP_REQUIRE_CHAINLINK = _env_bool("LATE_SCALP_REQUIRE_CHAINLINK", True)
# Minimum model edge (model_prob − market_ask) to enter. 0 = off (legacy).
LATE_SCALP_MIN_EDGE = float(os.getenv("LATE_SCALP_MIN_EDGE", "0.02"))
# Enable edge-scaled/Kelly sizing for late-scalping.
LATE_SCALP_USE_EDGE_SIZING = _env_bool("LATE_SCALP_USE_EDGE_SIZING", True)

# --- Late momentum strategy (STRATEGY=late_momentum) ---
LATE_MOMENTUM_MIN_MOVE = float(os.getenv("LATE_MOMENTUM_MIN_MOVE", "10.0"))
LATE_MOMENTUM_TIME_LIMIT = int(os.getenv("LATE_MOMENTUM_TIME_LIMIT", "40"))
LATE_MOMENTUM_MIN_REMAINING = int(os.getenv("LATE_MOMENTUM_MIN_REMAINING", "8"))
LATE_MOMENTUM_BIDIRECTIONAL = _env_bool("LATE_MOMENTUM_BIDIRECTIONAL", True)

# --- Hybrid (STRATEGY=hybrid): prefer late; mid only if exceptional ---
# When true (default), mid must clear HYBRID_MID_* bars on top of normal mid filters;
# otherwise the slot is queued for late_scalp. Set HYBRID_PREFER_LATE=0 for old
# mid-first behavior (still uses normal EDGE_THRESHOLD etc. for mid).
HYBRID_PREFER_LATE = _env_bool("HYBRID_PREFER_LATE", True)
HYBRID_MID_MIN_EDGE = float(os.getenv("HYBRID_MID_MIN_EDGE", "0.10"))
HYBRID_MID_MIN_MOVE_USD = float(os.getenv("HYBRID_MID_MIN_MOVE_USD", "15.0"))
HYBRID_MID_MIN_MODEL = float(os.getenv("HYBRID_MID_MIN_MODEL", "0.65"))
HYBRID_MID_MIN_CONFIDENCE = float(os.getenv("HYBRID_MID_MIN_CONFIDENCE", "0.55"))

# CLOB limit orders usually need ~5 shares. Stake is bumped to max(bet, shares*price).
MIN_ORDER_SHARES = float(os.getenv("MIN_ORDER_SHARES", "5"))
ENFORCE_MIN_SHARES = _env_bool("ENFORCE_MIN_SHARES", True)

# --- Risk controls ---
STARTING_BANKROLL = float(os.getenv("STARTING_BANKROLL", "100.0"))
BASE_TRADE_USD = float(os.getenv("BASE_TRADE_USD", "1.0"))
MAX_TRADE_USD = float(os.getenv("MAX_TRADE_USD", "3.0"))
MAX_EXPOSURE_USD = float(os.getenv("MAX_EXPOSURE_USD", "3.0"))
DAILY_LOSS_LIMIT_USD = float(os.getenv("DAILY_LOSS_LIMIT_USD", "5.0"))
# Halt at the TIGHTER of absolute $ and this % of starting bankroll (0 = ignore %).
DAILY_LOSS_LIMIT_PCT = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "0.05"))
# 0 = unlimited (paper). Live safety profile forces a small cap.
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "0"))
MAX_CONSECUTIVE_LOSSES = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "3"))
KELLY_FRACTION = float(os.getenv("KELLY_FRACTION", "0.10"))
# If > 0, every qualifying trade uses exactly this USD stake (live profile sets $1).
FIXED_STAKE_USD = float(os.getenv("FIXED_STAKE_USD", "0"))

MIN_QUOTE = float(os.getenv("MIN_QUOTE", "0.02"))
MAX_QUOTE = float(os.getenv("MAX_QUOTE", "0.98"))

VOLATILITY_DAILY = float(os.getenv("VOLATILITY_DAILY", "0.045"))


def apply_live_safety_profile() -> list[str]:
    """Tighten risk for MODE=live. Returns list of applied overrides for logging.

    Does nothing if LIVE_AGGRESSIVE=1. Env values that are already stricter than
    the profile floor are kept (we only tighten, never loosen).
    """
    if LIVE_AGGRESSIVE:
        return ["LIVE_AGGRESSIVE=1 (profile skipped)"]

    notes = []
    global EDGE_THRESHOLD, MIN_EV, MAX_SPREAD, FEE_BUFFER
    global MIN_ENTRY_PRICE, MAX_ENTRY_PRICE, MIN_CONFIDENCE, MIN_MOVE_USD
    global BASE_TRADE_USD, MAX_TRADE_USD, MAX_EXPOSURE_USD, FIXED_STAKE_USD
    global DAILY_LOSS_LIMIT_USD, DAILY_LOSS_LIMIT_PCT, MAX_TRADES_PER_DAY
    global MAX_CONSECUTIVE_LOSSES, KELLY_FRACTION, BTC_PRICE_SOURCES
    global MIN_SLOT_SECONDS, MAX_TRADE_REMAINING, MIN_ASK_SIZE

    def tighten_max(name, current, ceiling):
        """Values where lower is safer (trade size, loss limit, etc.)."""
        if current > ceiling:
            notes.append(f"{name}: {current} -> {ceiling}")
            return ceiling
        return current

    def tighten_min(name, current, floor):
        """Values where higher is safer (edge, min confidence, etc.)."""
        if current < floor:
            notes.append(f"{name}: {current} -> {floor}")
            return floor
        return current

    # Live mid floors align with stricter mid defaults (hybrid late path separate).
    EDGE_THRESHOLD = tighten_min("EDGE_THRESHOLD", EDGE_THRESHOLD, 0.08)
    MIN_EV = tighten_min("MIN_EV", MIN_EV, 0.05)
    MAX_SPREAD = tighten_max("MAX_SPREAD", MAX_SPREAD, 0.05)
    FEE_BUFFER = tighten_min("FEE_BUFFER", FEE_BUFFER, 0.015)
    MAX_ENTRY_PRICE = tighten_max("MAX_ENTRY_PRICE", MAX_ENTRY_PRICE, 0.58)
    MIN_ENTRY_PRICE = tighten_min("MIN_ENTRY_PRICE", MIN_ENTRY_PRICE, 0.25)
    MIN_CONFIDENCE = tighten_min("MIN_CONFIDENCE", MIN_CONFIDENCE, 0.50)
    MIN_MOVE_USD = tighten_min("MIN_MOVE_USD", MIN_MOVE_USD, 12.0)
    MIN_ASK_SIZE = tighten_min("MIN_ASK_SIZE", MIN_ASK_SIZE, 5.0)

    FIXED_STAKE_USD = tighten_max("FIXED_STAKE_USD", FIXED_STAKE_USD or 1.0, 1.0)
    if FIXED_STAKE_USD <= 0:
        FIXED_STAKE_USD = 1.0
        notes.append("FIXED_STAKE_USD -> 1.0")
    BASE_TRADE_USD = min(BASE_TRADE_USD, FIXED_STAKE_USD)
    MAX_TRADE_USD = min(MAX_TRADE_USD, FIXED_STAKE_USD)
    MAX_EXPOSURE_USD = min(MAX_EXPOSURE_USD, FIXED_STAKE_USD)
    notes.append(f"fixed stake ${FIXED_STAKE_USD:.2f}, max exposure ${MAX_EXPOSURE_USD:.2f}")

    DAILY_LOSS_LIMIT_USD = tighten_max("DAILY_LOSS_LIMIT_USD", DAILY_LOSS_LIMIT_USD, 5.0)
    # PCT=0 means "use absolute $ limit only" — do NOT force 5% back on
    # (that made daily_loss=$0.10 on a $2 start balance and halted after one $1 loss).
    if DAILY_LOSS_LIMIT_PCT > 0.05:
        notes.append(f"DAILY_LOSS_LIMIT_PCT: {DAILY_LOSS_LIMIT_PCT} -> 0.05")
        DAILY_LOSS_LIMIT_PCT = 0.05
    elif DAILY_LOSS_LIMIT_PCT <= 0:
        notes.append("DAILY_LOSS_LIMIT_PCT: 0 (absolute $ cap only)")
    MAX_TRADES_PER_DAY = int(tighten_max("MAX_TRADES_PER_DAY",
                                         float(MAX_TRADES_PER_DAY or 12), 10))
    if MAX_TRADES_PER_DAY <= 0:
        MAX_TRADES_PER_DAY = 10
        notes.append("MAX_TRADES_PER_DAY -> 10")
    MAX_CONSECUTIVE_LOSSES = int(tighten_max("MAX_CONSECUTIVE_LOSSES",
                                            float(MAX_CONSECUTIVE_LOSSES or 3), 3))
    KELLY_FRACTION = tighten_max("KELLY_FRACTION", KELLY_FRACTION, 0.10)

    MIN_SLOT_SECONDS = int(tighten_min("MIN_SLOT_SECONDS", float(MIN_SLOT_SECONDS), 90))
    MAX_TRADE_REMAINING = int(tighten_max("MAX_TRADE_REMAINING",
                                          float(MAX_TRADE_REMAINING), 220))

    # Prefer Chainlink first (resolution source).
    # Prefer Polymarket RTDS Chainlink Data Streams (matches 5m market rules).
    preferred = "chainlink_stream"
    if preferred not in BTC_PRICE_SOURCES:
        # Legacy: Arbitrum aggregator named "chainlink" is only a fallback.
        if BTC_PRICE_SOURCES and BTC_PRICE_SOURCES[0] != "chainlink":
            rest = [s for s in BTC_PRICE_SOURCES if s != "chainlink"]
            BTC_PRICE_SOURCES = ["chainlink"] + rest
            notes.append("BTC_PRICE_SOURCES: chainlink first (legacy aggregator)")
    elif BTC_PRICE_SOURCES[0] != preferred:
        rest = [s for s in BTC_PRICE_SOURCES if s != preferred]
        BTC_PRICE_SOURCES = [preferred] + rest
        notes.append("BTC_PRICE_SOURCES: chainlink_stream first (Data Streams)")

    return notes


def apply_late_scalp_profile() -> list[str]:
    """Override timing + entry band for late-window favorite scalping.

    Designed for paper_live testing of Bone Reaper–style convergence:
    trade only in the last ~35s–8s when one side is already ~90%+ priced.
    Still uses limit buys (never market). Does not loosen live stake caps.
    """
    global MIN_SLOT_SECONDS, MAX_TRADE_REMAINING
    global MIN_ENTRY_PRICE, MAX_ENTRY_PRICE
    global EDGE_THRESHOLD, MIN_EV, MIN_CONFIDENCE, MIN_MOVE_USD
    global FEE_BUFFER, MAX_SPREAD, MIN_ASK_SIZE
    global LATE_SCALP_MIN_REMAINING, LATE_SCALP_MAX_REMAINING
    global LATE_SCALP_MIN_ENTRY, LATE_SCALP_MAX_ENTRY

    notes = []
    # Window: last 35s down to 8s remaining
    lo = max(5, int(LATE_SCALP_MIN_REMAINING))
    hi = max(lo + 1, int(LATE_SCALP_MAX_REMAINING))
    LATE_SCALP_MIN_REMAINING = lo
    LATE_SCALP_MAX_REMAINING = hi
    if MIN_SLOT_SECONDS != lo:
        notes.append(f"MIN_SLOT_SECONDS: {MIN_SLOT_SECONDS} -> {lo} (late_scalp)")
        MIN_SLOT_SECONDS = lo
    if MAX_TRADE_REMAINING != hi:
        notes.append(f"MAX_TRADE_REMAINING: {MAX_TRADE_REMAINING} -> {hi} (late_scalp)")
        MAX_TRADE_REMAINING = hi

    # High-price favorites only (strict band for lower loss rate)
    MIN_ENTRY_PRICE = float(LATE_SCALP_MIN_ENTRY)
    MAX_ENTRY_PRICE = float(LATE_SCALP_MAX_ENTRY)
    notes.append(
        f"entry band -> [{MIN_ENTRY_PRICE:.2f}, {MAX_ENTRY_PRICE:.2f}] "
        f"(late favorites from ~0.90; skip micro-payout 0.97+)"
    )

    # Book/signal gates: late scalp is book-driven, not model-edge driven
    EDGE_THRESHOLD = 0.0
    MIN_EV = 0.0
    MIN_CONFIDENCE = 0.0
    MIN_MOVE_USD = max(MIN_MOVE_USD, LATE_SCALP_MIN_MOVE_USD)
    FEE_BUFFER = min(FEE_BUFFER, 0.01)
    MAX_SPREAD = max(MAX_SPREAD, 0.08) if MAX_SPREAD > 0 else 0.08
    # Paper-friendly depth; live safety may have raised MIN_ASK_SIZE already
    notes.append(
        f"window {lo}-{hi}s left; edge/EV/move gates off (favorite-convergence mode)"
    )
    notes.append(
        "WARNING: late_scalp buys expensive favorites - high win-rate, brutal "
        "loss size when wrong. Prefer paper_live first."
    )
    return notes


def is_late_every() -> bool:
    """True for every-5m-slot late mode (menu: late_every)."""
    return (STRATEGY or "mid").strip().lower() in (
        "late_every", "late_scalp_every", "every_slot", "every", "late_all",
    )


def is_late_scalp() -> bool:
    """True for pure late strategies (selective or every-slot), not hybrid."""
    if is_late_every():
        return True
    return (STRATEGY or "mid").strip().lower() in (
        "late_scalp", "latescalp", "late", "scalp", "bone_reaper", "convergence",
    )


def is_hybrid() -> bool:
    return (STRATEGY or "mid").strip().lower() in (
        "hybrid", "mid_then_late", "both", "late_then_mid", "prefer_late",
    )


def is_late_momentum() -> bool:
    """True for late momentum breakout strategy."""
    return (STRATEGY or "mid").strip().lower() in (
        "late_momentum", "momentum_late", "late_min_move", "momentum",
    )


def apply_strategy_flags() -> None:
    """Normalize STRATEGY aliases and set LATE_SCALP_EVERY_SLOT from menu choice.

    Call after CLI/env STRATEGY is set so dashboard menu options behave:
      late_every  -> late path + EVERY_SLOT on
      late_scalp  -> late path + EVERY_SLOT off (unless env already forced)
    """
    global STRATEGY, LATE_SCALP_EVERY_SLOT
    s = (STRATEGY or "mid").strip().lower()
    if s in ("late_every", "late_scalp_every", "every_slot", "every", "late_all"):
        STRATEGY = "late_every"
        LATE_SCALP_EVERY_SLOT = True
    elif s in ("late_scalp", "latescalp", "late", "scalp", "bone_reaper", "convergence"):
        STRATEGY = "late_scalp"
        # Menu "late_scalp" = selective; only keep EVERY_SLOT if explicitly late_every
        LATE_SCALP_EVERY_SLOT = False
    elif s in ("hybrid", "mid_then_late", "both", "late_then_mid", "prefer_late"):
        STRATEGY = "hybrid"
    elif s in ("late_momentum", "momentum_late", "late_min_move", "momentum"):
        STRATEGY = "late_momentum"
    elif s in ("mid", "middle", "value"):
        STRATEGY = "mid"


def hybrid_prefer_late() -> bool:
    """Hybrid default: wait for late unless mid is exceptional."""
    return bool(is_hybrid() and HYBRID_PREFER_LATE)


def mid_exceptional_ok(
    edge: float,
    move_usd: float,
    model_side_prob: float,
    confidence: float,
) -> tuple[bool, str]:
    """Return (ok, reason) for hybrid mid-only exceptional gate.

    Used when HYBRID_PREFER_LATE so weak mid signals defer to late_scalp.
    """
    need_edge = float(HYBRID_MID_MIN_EDGE)
    need_move = float(HYBRID_MID_MIN_MOVE_USD)
    need_model = float(HYBRID_MID_MIN_MODEL)
    need_conf = float(HYBRID_MID_MIN_CONFIDENCE)
    fails: list[str] = []
    if edge < need_edge:
        fails.append(f"edge {edge:+.3f}<{need_edge:.3f}")
    if move_usd < need_move:
        fails.append(f"move ${move_usd:.1f}<${need_move:.1f}")
    if model_side_prob < need_model:
        fails.append(f"model {model_side_prob:.2f}<{need_model:.2f}")
    if confidence < need_conf:
        fails.append(f"conf {confidence:.2f}<{need_conf:.2f}")
    if fails:
        return False, "; ".join(fails)
    return True, "exceptional mid"


def strategy_name() -> str:
    if is_hybrid():
        return "hybrid"
    if is_late_every():
        return "late_every"
    if is_late_scalp():
        return "late_scalp"
    if is_late_momentum():
        return "late_momentum"
    return "mid"
