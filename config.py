"""Configuration for High-Conviction TWAP Mispricing Bot (H-001)."""
from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(override=False)

# --- Trading Mode & Asset ---
MODE = os.getenv("MODE", "paper_live").strip().lower()  # 'paper_live' (real data, paper fills) or 'live'
LIVE_CONFIRM = os.getenv("LIVE_CONFIRM", "no").strip().lower()
ASSET = os.getenv("ASSET", "btc").strip().lower()
TIMEFRAME = os.getenv("TIMEFRAME", "5m").strip().lower()
SLOT_SECONDS = int(os.getenv("SLOT_SECONDS", "300"))

# --- WebSocket Endpoints & Hosts ---
CHAINLINK_RTDS_URL = os.getenv("CHAINLINK_RTDS_URL", "wss://ws-live-data.polymarket.com").strip()
CLOB_WS_URL = os.getenv("CLOB_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market").strip()
GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com").strip()
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com").strip()
CHAIN_ID = int(os.getenv("CHAIN_ID", "137"))  # Polygon PoS

# --- Polymarket Authentication (for MODE=live) ---
PRIVATE_KEY = os.getenv("PRIVATE_KEY", os.getenv("POLYMARKET_PRIVATE_KEY", "")).strip()
FUNDER_ADDRESS = os.getenv("FUNDER_ADDRESS", os.getenv("POLYMARKET_FUNDER_ADDRESS", "")).strip()
SIGNATURE_TYPE = int(os.getenv("SIGNATURE_TYPE", os.getenv("POLYMARKET_SIGNATURE_TYPE", "3")))  # 3 = Polymarket Proxy Wallet

# --- Strategy Selection ---
STRATEGY = os.getenv("STRATEGY", "H-004_MAKER").strip().upper()

# --- H-004 Passive Maker Limit Model Parameters ---
MIN_Z_SCORE = float(os.getenv("MIN_Z_SCORE", "1.45"))                # Require >=93.0% conviction (Z >= 1.45)
ALPHA = float(os.getenv("ALPHA", "1.75"))
MIN_SIGMA_USD = float(os.getenv("MIN_SIGMA_USD", "2.0"))
MAX_CHOP_CROSSES = int(os.getenv("MAX_CHOP_CROSSES", "2"))
MIN_MAKER_EDGE = float(os.getenv("MIN_MAKER_EDGE", "0.05"))          # Minimum +5.0% expected net edge
MIN_MAKER_BID = float(os.getenv("MIN_MAKER_BID", "0.55"))            # Floor resting bid price at $0.55
MAX_MAKER_BID = float(os.getenv("MAX_MAKER_BID", "0.90"))            # Cap resting bid price at $0.90 (+11% to +81% ROI)
EVAL_WINDOW_START_S = float(os.getenv("EVAL_WINDOW_START_S", "270.0"))  # Start quoting at t=30s (270s left)
EVAL_WINDOW_END_S = float(os.getenv("EVAL_WINDOW_END_S", "20.0"))        # Stop quoting at t=280s (20s left)
MAKER_DWELL_SECONDS = float(os.getenv("MAKER_DWELL_SECONDS", "8.0"))    # Require 8s sustained queue dwell
TAKER_FEE_RATE = float(os.getenv("TAKER_FEE_RATE", "0.07"))
MAKER_REBATE_SHARE = float(os.getenv("MAKER_REBATE_SHARE", "0.20"))

# --- Risk & Sizing Parameters ---
STARTING_BANKROLL = float(os.getenv("STARTING_BANKROLL", "50.0"))
BASE_STAKE_USD = float(os.getenv("BASE_STAKE_USD", "5.0"))
MAX_STAKE_USD = float(os.getenv("MAX_STAKE_USD", "10.0"))
DAILY_LOSS_LIMIT_USD = float(os.getenv("DAILY_LOSS_LIMIT_USD", "15.0"))
MAX_CONSECUTIVE_LOSSES = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "4"))
MIN_ORDER_SHARES = float(os.getenv("MIN_ORDER_SHARES", "5.0"))

# --- State Directory ---
STATE_ROOT = os.getenv("STATE_ROOT", "").strip()
if STATE_ROOT and os.path.exists(STATE_ROOT):
    STATE_DIR = Path(STATE_ROOT)
else:
    STATE_DIR = Path("state")
STATE_DIR.mkdir(parents=True, exist_ok=True)
