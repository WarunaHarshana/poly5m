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
#   mid        = mid-slot value band (default; edge vs strike + model)
#   late_scalp = last ~35s–8s favorites only (Bone Reaper–style; paper first)
#   hybrid     = try mid first; if no trade, try late_scalp on the SAME slot
STRATEGY = (os.getenv("STRATEGY") or "mid").strip().lower()

# --- Polymarket / chain ---
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")
CHAIN_ID = int(os.getenv("CHAIN_ID", "137"))  # Polygon mainnet
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

# --- BTC price feed (real data) ---
# Polymarket 5m BTC markets resolve on Chainlink BTC/USD — put chainlink first
# for live accuracy. Public endpoints, no key.
BTC_PRICE_SOURCES = [s.strip() for s in os.getenv(
    "BTC_PRICE_SOURCES",
    "chainlink_stream,chainlink,pyth,coinbase,binance,kraken").split(",") if s.strip()]

# --- Strategy ---
ROUND_SECONDS = int(os.getenv("ROUND_SECONDS", "300"))
MOMENTUM_LOOKBACK = int(os.getenv("MOMENTUM_LOOKBACK", "12"))
SAMPLE_SECONDS = float(os.getenv("SAMPLE_SECONDS", "5"))
EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "0.05"))
MIN_EV = float(os.getenv("MIN_EV", "0.04"))
VOL_BLEND = float(os.getenv("VOL_BLEND", "0.5"))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.06"))
# Assumed / buffer for fees + adverse selection (subtracted from model edge).
FEE_BUFFER = float(os.getenv("FEE_BUFFER", "0.015"))
# Only buy contracts in this ask band (avoid expensive favorites / lottery tickets).
MIN_ENTRY_PRICE = float(os.getenv("MIN_ENTRY_PRICE", "0.22"))
MAX_ENTRY_PRICE = float(os.getenv("MAX_ENTRY_PRICE", "0.60"))
# Require signal confidence and a clear move vs strike (in $) before trading.
MIN_CONFIDENCE = float(os.getenv("MIN_CONFIDENCE", "0.45"))
MIN_MOVE_USD = float(os.getenv("MIN_MOVE_USD", "5.0"))  # |spot - strike|
# Minimum size sitting on the best ask (shares); 0 = disabled (paper-friendly).
MIN_ASK_SIZE = float(os.getenv("MIN_ASK_SIZE", "0"))

# --- Late-scalp strategy (STRATEGY=late_scalp) ---
# Enter only when remaining seconds in [LATE_SCALP_MIN_REMAINING, LATE_SCALP_MAX_REMAINING]
# and the favorite ask is in [LATE_SCALP_MIN_ENTRY, LATE_SCALP_MAX_ENTRY].
LATE_SCALP_MIN_REMAINING = int(os.getenv("LATE_SCALP_MIN_REMAINING", "8"))
LATE_SCALP_MAX_REMAINING = int(os.getenv("LATE_SCALP_MAX_REMAINING", "35"))
LATE_SCALP_MIN_ENTRY = float(os.getenv("LATE_SCALP_MIN_ENTRY", "0.88"))
LATE_SCALP_MAX_ENTRY = float(os.getenv("LATE_SCALP_MAX_ENTRY", "0.97"))
# Minimum gap between favorite ask and underdog ask (favorite clearly leading).
LATE_SCALP_MIN_FAVORITE_LEAD = float(os.getenv("LATE_SCALP_MIN_FAVORITE_LEAD", "0.50"))

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

    # Slightly looser than original ultra-tight live band after paper_live validation,
    # but still stricter than paper_live (which may use ~0.18–0.72).
    EDGE_THRESHOLD = tighten_min("EDGE_THRESHOLD", EDGE_THRESHOLD, 0.06)
    MIN_EV = tighten_min("MIN_EV", MIN_EV, 0.05)
    MAX_SPREAD = tighten_max("MAX_SPREAD", MAX_SPREAD, 0.05)
    FEE_BUFFER = tighten_min("FEE_BUFFER", FEE_BUFFER, 0.015)
    MAX_ENTRY_PRICE = tighten_max("MAX_ENTRY_PRICE", MAX_ENTRY_PRICE, 0.65)
    MIN_ENTRY_PRICE = tighten_min("MIN_ENTRY_PRICE", MIN_ENTRY_PRICE, 0.22)
    MIN_CONFIDENCE = tighten_min("MIN_CONFIDENCE", MIN_CONFIDENCE, 0.45)
    MIN_MOVE_USD = tighten_min("MIN_MOVE_USD", MIN_MOVE_USD, 5.0)
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

    # High-price favorites only
    MIN_ENTRY_PRICE = float(LATE_SCALP_MIN_ENTRY)
    MAX_ENTRY_PRICE = float(LATE_SCALP_MAX_ENTRY)
    notes.append(
        f"entry band -> [{MIN_ENTRY_PRICE:.2f}, {MAX_ENTRY_PRICE:.2f}] (late favorite)"
    )

    # Book/signal gates: late scalp is book-driven, not model-edge driven
    EDGE_THRESHOLD = 0.0
    MIN_EV = 0.0
    MIN_CONFIDENCE = 0.0
    MIN_MOVE_USD = 0.0
    FEE_BUFFER = min(FEE_BUFFER, 0.01)
    MAX_SPREAD = max(MAX_SPREAD, 0.08) if MAX_SPREAD > 0 else 0.08
    # Paper-friendly depth; live safety may have raised MIN_ASK_SIZE already
    notes.append(
        f"window {lo}–{hi}s left; edge/EV/move gates off (favorite-convergence mode)"
    )
    notes.append(
        "WARNING: late_scalp buys expensive favorites — high win-rate, brutal "
        "loss size when wrong. Prefer paper_live first."
    )
    return notes


def is_late_scalp() -> bool:
    """True only for pure late_scalp (not hybrid)."""
    return (STRATEGY or "mid").strip().lower() in (
        "late_scalp", "latescalp", "late", "scalp", "bone_reaper", "convergence",
    )


def is_hybrid() -> bool:
    return (STRATEGY or "mid").strip().lower() in ("hybrid", "mid_then_late", "both")


def strategy_name() -> str:
    if is_hybrid():
        return "hybrid"
    if is_late_scalp():
        return "late_scalp"
    return "mid"
