"""Polymarket 5m bot — default MIDBAND_MAKER (H-001 sniper retired).

Trades Polymarket 5-Minute Up/Down markets (BTC, ETH, SOL, BNB, XRP, DOGE, HYPE):
- Polymarket RTDS Chainlink Stream
- Polymarket CLOB Level 2 Orderbook Stream
- Exact Analytical TWAP Integral & Cubic Variance Decay Engine
- One open position per coin in the same 5-minute window
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import config
from twap_engine import TWAPEngine, MakerSignal, TWAPSignal
from feeds import ChainlinkRTDSStream, PolymarketBookStream
from polymarket_client import (
    PolymarketPublicClient,
    PolymarketLiveClient,
    fillable_ask_shares,
)
from risk_manager import RiskManager
from journal import configure as journal_configure
from journal import write as journal_write
from http_api import StatusServer, is_official_trade, load_trades

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("twap-bot")
