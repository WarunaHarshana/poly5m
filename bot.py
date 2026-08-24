"""High-Conviction TWAP Mispricing Trading Bot (H-001).

Trades Polymarket 5-Minute BTC Up/Down markets using real-time WebSockets:
- Polymarket RTDS Chainlink BTC/USD Stream
- Polymarket CLOB Level 2 Orderbook Stream
- Exact Analytical TWAP Integral & Cubic Variance Decay Engine
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import config
from twap_engine import TWAPEngine, TWAPSignal
from feeds import ChainlinkRTDSStream, PolymarketBookStream
from polymarket_client import PolymarketPublicClient, PolymarketLiveClient
from risk_manager import RiskManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("twap-bot")


class TWAPBot:
    """Trading Bot executing the High-Conviction TWAP Mispricing Strategy."""

    def __init__(self):
        self.engine = TWAPEngine(
            alpha=config.ALPHA,
            min_sigma_usd=config.MIN_SIGMA_USD,
            min_z=config.MIN_Z_SCORE,
            min_edge=config.MIN_EDGE,
            min_entry=config.MIN_ENTRY_PRICE,
            max_entry=config.MAX_ENTRY_PRICE,
            max_chop_crosses=config.MAX_CHOP_CROSSES,
        )
        self.cl_stream = ChainlinkRTDSStream(asset=config.ASSET)
        self.book_stream = PolymarketBookStream()
        self.pub_client = PolymarketPublicClient()
        self.risk = RiskManager()
        self.live_client = None
        if config.MODE == "live":
            if config.LIVE_CONFIRM not in ("yes", "y", "true", "1"):
                logger.error("MODE=live is blocked! Set LIVE_CONFIRM=yes in .env to confirm live funds risk.")
                sys.exit(1)
            self.live_client = PolymarketLiveClient()

        self._stop = False
        self._current_position: dict | None = None
        self._trades_log_path = config.STATE_DIR / "trades.json"

    def stop(self) -> None:
        logger.info("Stopping bot...")
        self._stop = True
        self.cl_stream.stop()
        self.book_stream.stop()

    async def start(self) -> None:
        logger.info("=" * 70)
        logger.info("STARTING HIGH-CONVICTION TWAP MISPRICING BOT (H-001)")
        logger.info(f"Mode: {config.MODE.upper()} | Asset: {config.ASSET.upper()} | Timeframe: {config.TIMEFRAME}")
        logger.info(f"Parameters: min_z={config.MIN_Z_SCORE} | min_edge={config.MIN_EDGE:.1%} | alpha={config.ALPHA}")
        logger.info(f"Window: [{config.EVAL_WINDOW_END_S:.0f}s .. {config.EVAL_WINDOW_START_S:.0f}s remaining]")
        logger.info(f"Bankroll: ${self.risk.bankroll:.2f} | Base Stake: ${config.BASE_STAKE_USD:.2f}")
        logger.info("=" * 70)

        # Start feeds
        logger.info("Connecting to Chainlink RTDS WebSocket (wss://ws-live-data.polymarket.com)...")
        self.cl_stream.start()
        self.book_stream.start()

        # Non-blocking async wait for stream ready
        for _ in range(30):
            if self.cl_stream.ready:
                break
            await asyncio.sleep(0.5)

        spot = self.cl_stream.price()
        logger.info(f"Chainlink BTC Spot: ${spot:,.2f} | Clock Offset: {self.cl_stream.clock_offset_s():+.2f}s")

        active_slot_ts = None
        current_market = None

        while not self._stop:
            try:
                now = time.time()
                current_window = int(now // config.SLOT_SECONDS * config.SLOT_SECONDS)

                # Discover new slot if needed
                if active_slot_ts != current_window:
                    active_slot_ts = current_window
                    current_market = self.pub_client.find_active_5m_slot(now_ts=now)
                    if current_market:
                        logger.info(f"\n[SLOT {active_slot_ts}] Discovered market: {current_market['slug']}")
                        tokens = current_market["tokens"]
                        self.book_stream.watch(tokens.get("UP", ""), tokens.get("DOWN", ""))
                    else:
                        logger.warning(f"Could not discover market for slot {active_slot_ts}, retrying in 2s...")
                        await asyncio.sleep(2.0)
                        continue

                if not current_market:
                    await asyncio.sleep(2.0)
                    continue

                end_ts = current_market["end_ts"]
                slot_start_ts = current_market["slot_start_ts"]
                remaining_s = max(0.0, end_ts - now)
                elapsed_s = max(0.0, now - slot_start_ts)

                # Latch slot strike S_0
                strike = self.cl_stream.get_slot_strike(slot_start_ts)
                if strike is None:
                    strike = self.cl_stream.price()

                spot = self.cl_stream.price()
                realized_twap, _ = self.cl_stream.compute_realized_twap(start_ts=slot_start_ts, end_ts=now)
                chop = self.cl_stream.strike_crossings_since(slot_start_ts, strike)

                # Evaluate strategy within entry window
                in_eval_window = (config.EVAL_WINDOW_END_S <= remaining_s <= config.EVAL_WINDOW_START_S)

                if in_eval_window and not self._current_position:
                    # Check orderbook for UP and DOWN
                    tokens = current_market["tokens"]
                    prelim_side = "UP" if spot >= strike else "DOWN"
                    token_id = tokens.get(prelim_side)
                    
                    top = self.book_stream.get_top(token_id) if token_id else None
                    ask = top.get("ask") if top else None

                    # If WebSocket had no quote, fallback to REST
                    if ask is None and token_id:
                        bk = self.pub_client.get_order_book(token_id)
                        if bk and bk.get("asks"):
                            ask = float(bk["asks"][0]["price"])

                    signal = self.engine.evaluate(
                        spot=spot,
                        strike=strike,
                        remaining_s=remaining_s,
                        elapsed_s=elapsed_s,
                        realized_twap=realized_twap,
                        ask=ask,
                        chop_crosses=chop,
                        fee_rate=current_market.get("fee_rate", config.TAKER_FEE_RATE),
                    )

                    # Diagnostic tick log
                    logger.info(
                        f"[{remaining_s:3.0f}s left] BTC: ${spot:,.1f} | S0: ${strike:,.1f} | "
                        f"rTWAP: ${realized_twap:,.1f} | S_req: ${signal.s_req:,.1f} | "
                        f"Z: {signal.z_score:4.2f} | P: {signal.model_prob:.1%} | "
                        f"Ask({signal.side}): {ask or 0:.2f} | Edge: {signal.edge_net:+.1%} | "
                        f"Pass: {signal.passed_gates}"
                    )

                    # Execute if gates passed
                    if signal.passed_gates:
                        can_trade, risk_reason = self.risk.can_trade()
                        if can_trade:
                            await self._execute_trade(current_market, signal)
                        else:
                            logger.warning(f"Trade blocked by risk manager: {risk_reason}")

                # Handle settlement at slot end
                if remaining_s <= 0.5 and self._current_position:
                    await self._settle_position(current_market, strike)

                await asyncio.sleep(1.0)

            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                await asyncio.sleep(2.0)

    async def _execute_trade(self, market: dict, signal: TWAPSignal) -> None:
        """Place order on high-conviction signal."""
        token_id = market["tokens"].get(signal.side)
        if not token_id or not signal.ask:
            return

        stake_usd = self.risk.calculate_stake(signal.edge_net, signal.ask)
        shares = max(config.MIN_ORDER_SHARES, round(stake_usd / signal.ask, 2))
        cost_usd = round(shares * signal.ask, 2)

        logger.info("=" * 60)
        logger.info(f"🚨 [H-001 SIGNAL TRIGGERED] BUY {signal.side} @ {signal.ask:.3f}")
        logger.info(f"   Z-Score: {signal.z_score:.2f} | Win Prob: {signal.model_prob:.1%} | Net Edge: {signal.edge_net:+.1%}")
        logger.info(f"   Position Size: {shares:.1f} shares (${cost_usd:.2f})")
        logger.info("=" * 60)

        order_id = f"paper_{int(time.time()*1000)}"
        if config.MODE == "live" and self.live_client:
            try:
                resp = self.live_client.place_order(token_id, "BUY", signal.ask, shares)
                order_id = resp.get("orderID", order_id)
                logger.info(f"Live order placed: {resp}")
            except Exception as e:
                logger.error(f"Live order placement failed: {e}")
                return

        self._current_position = {
            "slot": market["slug"],
            "side": signal.side,
            "token_id": token_id,
            "ask": signal.ask,
            "shares": shares,
            "cost_usd": cost_usd,
            "z_score": signal.z_score,
            "prob": signal.model_prob,
            "entry_time": time.time(),
            "order_id": order_id,
        }

    async def _settle_position(self, market: dict, strike: float) -> None:
        """Settle open position against final Chainlink TWAP."""
        pos = self._current_position
        self._current_position = None
        if not pos:
            return

        # Compute official 300s TWAP over slot window
        final_twap, _ = self.cl_stream.compute_realized_twap(
            start_ts=market["slot_start_ts"],
            end_ts=market["end_ts"],
        )
        resolved_side = "UP" if final_twap >= strike else "DOWN"
        won = (pos["side"] == resolved_side)

        if won:
            revenue = pos["shares"] * 1.0
            pnl = revenue - pos["cost_usd"]
        else:
            pnl = -pos["cost_usd"]

        self.risk.record_trade_result(pnl)

        logger.info("=" * 60)
        logger.info(f"🏁 [SLOT SETTLEMENT] {market['slug']}")
        logger.info(f"   Strike (S0): ${strike:,.2f} | Final TWAP: ${final_twap:,.2f}")
        logger.info(f"   Outcome: {resolved_side} | Trade: {pos['side']} -> {'WON' if won else 'LOST'}")
        logger.info(f"   Trade PnL: ${pnl:+.2f} | New Bankroll: ${self.risk.bankroll:.2f}")
        logger.info("=" * 60)

        # Record to trades.json
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "slot": market["slug"],
            "side": pos["side"],
            "ask": pos["ask"],
            "shares": pos["shares"],
            "cost_usd": pos["cost_usd"],
            "z_score": pos["z_score"],
            "strike": strike,
            "final_twap": final_twap,
            "outcome": resolved_side,
            "won": won,
            "pnl": round(pnl, 2),
            "bankroll": round(self.risk.bankroll, 2),
        }
        self._save_trade_record(record)

    def _save_trade_record(self, record: dict) -> None:
        trades = []
        if self._trades_log_path.exists():
            try:
                with open(self._trades_log_path, "r") as f:
                    trades = json.load(f)
            except Exception:
                trades = []
        trades.append(record)
        try:
            with open(self._trades_log_path, "w") as f:
                json.dump(trades, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save trade record: {e}")


def main():
    bot = TWAPBot()

    def handle_sig(sig, frame):
        bot.stop()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    asyncio.run(bot.start())


if __name__ == "__main__":
    main()
