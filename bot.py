"""High-Conviction TWAP Mispricing & Passive Maker Trading Bot (H-004).

Trades Polymarket 5-Minute BTC Up/Down markets using real-time WebSockets:
- Polymarket RTDS Chainlink BTC/USD Stream
- Polymarket CLOB Level 2 Orderbook Stream
- Exact Analytical TWAP Integral & Cubic Variance Decay Engine
- Passive Limit Maker Execution with 0% fees, 20% rebates, and adverse selection cancel guards.
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
from twap_engine import TWAPEngine, MakerSignal, TWAPSignal
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
    """Trading Bot executing the H-004 Passive Maker Strategy."""

    def __init__(self):
        self.engine = TWAPEngine(
            alpha=config.ALPHA,
            min_sigma_usd=config.MIN_SIGMA_USD,
            min_z=config.MIN_Z_SCORE,
            min_maker_bid=config.MIN_MAKER_BID,
            max_maker_bid=config.MAX_MAKER_BID,
            min_maker_edge=config.MIN_MAKER_EDGE,
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
        self._resting_order: dict | None = None
        self._current_position: dict | None = None
        self._trades_log_path = config.STATE_DIR / "trades.json"

    def stop(self) -> None:
        logger.info("Stopping bot...")
        self._stop = True
        self.cl_stream.stop()
        self.book_stream.stop()

    async def start(self) -> None:
        logger.info("=" * 70)
        logger.info("STARTING H-004 PASSIVE MAKER LIMIT TRADING BOT")
        logger.info(f"Mode: {config.MODE.upper()} | Asset: {config.ASSET.upper()} | Timeframe: {config.TIMEFRAME}")
        logger.info(f"Parameters: min_z={config.MIN_Z_SCORE} | min_maker_edge={config.MIN_MAKER_EDGE:.1%} | bid_range=[${config.MIN_MAKER_BID:.2f} .. ${config.MAX_MAKER_BID:.2f}]")
        logger.info(f"Quoting Window: [{config.EVAL_WINDOW_END_S:.0f}s .. {config.EVAL_WINDOW_START_S:.0f}s remaining]")
        logger.info(f"Bankroll: ${self.risk.bankroll:.2f} | Base Stake: ${config.BASE_STAKE_USD:.2f}")
        if self._trades_log_path.exists():
            try:
                with open(self._trades_log_path, "r") as f:
                    all_trades = json.load(f)
                    total_wins = sum(1 for t in all_trades if t.get("won"))
                    total_losses = len(all_trades) - total_wins
                    total_pnl = sum(float(t.get("pnl", 0.0)) for t in all_trades)
                    logger.info(f"Historical Blotter: {len(all_trades)} trades ({total_wins}W / {total_losses}L) | Net PnL: ${total_pnl:+.2f}")
            except Exception:
                pass
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
        slot_strike = None

        while not self._stop:
            try:
                now = time.time()
                current_window = int(now // config.SLOT_SECONDS * config.SLOT_SECONDS)

                # Discover new slot if needed
                if active_slot_ts != current_window:
                    active_slot_ts = current_window
                    slot_strike = None
                    self._resting_order = None
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

                # Latch slot strike S_0 once at or near slot open
                if slot_strike is None or slot_strike <= 0:
                    slot_strike = self.cl_stream.get_slot_strike(slot_start_ts)
                    if slot_strike is None or slot_strike <= 0:
                        slot_strike = self.cl_stream.price()

                strike = slot_strike
                spot = self.cl_stream.price()

                if self.cl_stream.is_stale(max_age_s=6.0):
                    logger.warning(f"[FEED] Chainlink tick stream waiting for live tick... (last: ${spot:,.2f})")
                    await asyncio.sleep(1.0)
                    continue

                realized_twap, _ = self.cl_stream.compute_realized_twap(start_ts=slot_start_ts, end_ts=now)
                chop = self.cl_stream.strike_crossings_since(slot_start_ts, strike)

                # Evaluate strategy within entry window
                in_eval_window = (config.EVAL_WINDOW_END_S <= remaining_s <= config.EVAL_WINDOW_START_S)

                # Fetch top of book for UP and DOWN
                tokens = current_market["tokens"]
                prelim_side = "UP" if spot >= strike else "DOWN"
                token_id = tokens.get(prelim_side)

                top = self.book_stream.get_top(token_id) if token_id else None
                best_bid = top.get("bid") if top else None
                best_ask = top.get("ask") if top else None

                # Fallback to REST if WebSocket had no quote
                if best_bid is None and token_id:
                    bk = self.pub_client.get_order_book(token_id)
                    if bk:
                        if bk.get("bids"):
                            best_bid = float(bk["bids"][0]["price"])
                        if bk.get("asks"):
                            best_ask = float(bk["asks"][0]["price"])

                is_sniper = (config.STRATEGY in ("H-001_SNIPER", "SNIPER", "H-001"))

                if is_sniper:
                    signal = self.engine.evaluate(
                        spot=spot,
                        strike=strike,
                        remaining_s=remaining_s,
                        elapsed_s=elapsed_s,
                        realized_twap=realized_twap,
                        ask=best_ask,
                        chop_crosses=chop,
                        fee_rate=current_market.get("fee_rate", config.TAKER_FEE_RATE),
                    )

                    if in_eval_window:
                        logger.info(
                            f"[{remaining_s:3.0f}s left] BTC: ${spot:,.1f} | S0: ${strike:,.1f} | "
                            f"Z: {signal.z_score:4.2f} | P: {signal.model_prob:.1%} | "
                            f"Ask({signal.side}): {best_ask or 0:.2f} | "
                            f"Edge: {signal.edge_net:+.1%} | Pass: {signal.passed_gates}"
                        )

                        if signal.passed_gates and not self._current_position:
                            can_trade, risk_reason = self.risk.can_trade()
                            if can_trade:
                                await self._place_taker_order(current_market, signal, best_ask)
                            else:
                                logger.warning(f"Sniper trade blocked by risk manager: {risk_reason}")
                else:
                    signal = self.engine.evaluate_maker(
                        spot=spot,
                        strike=strike,
                        remaining_s=remaining_s,
                        elapsed_s=elapsed_s,
                        realized_twap=realized_twap,
                        best_bid=best_bid,
                        best_ask=best_ask,
                        chop_crosses=chop,
                        fee_rate=current_market.get("fee_rate", config.TAKER_FEE_RATE),
                    )

                    if in_eval_window:
                        logger.info(
                            f"[{remaining_s:3.0f}s left] BTC: ${spot:,.1f} | S0: ${strike:,.1f} | "
                            f"Z: {signal.z_score:4.2f} | P: {signal.model_prob:.1%} | "
                            f"TopBid: {best_bid or 0:.2f} | TargetBid({signal.side}): {signal.target_bid:.2f} | "
                            f"Edge: {signal.edge_net:+.1%} | Pass: {signal.passed_gates}"
                        )

                        # Place Resting Maker Bid if not already active or positioned
                        if signal.passed_gates and not self._resting_order and not self._current_position:
                            can_trade, risk_reason = self.risk.can_trade()
                            if can_trade:
                                await self._place_maker_bid(current_market, signal)
                            else:
                                logger.warning(f"Maker trade blocked by risk manager: {risk_reason}")

                    # Check Fill Simulation / Matching on Active Resting Maker Bid
                    if self._resting_order and not self._current_position:
                        ro = self._resting_order
                        cur_bid = best_bid or 0.0
                        cur_ask = best_ask or 1.0
                        elapsed_resting = time.time() - ro.get("placed_at", time.time())

                        # In paper mode: simulate queue priority fill after sustained dwell while conviction holds
                        is_paper_fill = (config.MODE != "live" and elapsed_resting >= config.MAKER_DWELL_SECONDS and signal.z_score >= config.MIN_Z_SCORE)
                        is_clob_fill = (cur_bid >= ro["price"] or cur_ask <= ro["price"])

                        if is_paper_fill or is_clob_fill:
                            self._current_position = {
                                "slot": ro["slot"],
                                "strategy": "H-004_MAKER",
                                "side": ro["side"],
                                "token_id": ro["token_id"],
                                "price": ro["price"],
                                "shares": ro["shares"],
                                "cost_usd": ro["cost_usd"],
                                "z_score": ro["z_score"],
                                "prob": ro["prob"],
                                "fee": 0.0,
                                "rebate": ro["rebate"],
                                "entry_time": time.time(),
                                "order_id": ro["order_id"],
                            }
                            logger.info("=" * 60)
                            logger.info(f"✅ [H-004 MAKER BID FILLED] BUY {ro['side']} @ {ro['price']:.2f}")
                            logger.info(f"   Filled: {ro['shares']:.1f} shares (${ro['cost_usd']:.2f}) | Rebate: ${ro['rebate']:.3f}")
                            logger.info("=" * 60)
                            self._resting_order = None

                        # Adverse Selection / Reversal Protection Cancellation
                        elif signal.z_score < 0.80 or remaining_s < config.EVAL_WINDOW_END_S:
                            reason = "spot reversal (Z < 0.80)" if signal.z_score < 0.80 else "end of quoting window"
                            logger.info(f"🚫 [H-004 MAKER BID CANCELLED] Cancelled resting bid @ {ro['price']:.2f} ({reason})")
                            if config.MODE == "live" and self.live_client:
                                try:
                                    self.live_client.cancel_order(ro["order_id"])
                                except Exception as e:
                                    logger.error(f"Error cancelling live order: {e}")
                            self._resting_order = None

                # Handle settlement at slot end
                if remaining_s <= 0.5:
                    if self._resting_order:
                        self._resting_order = None
                    if self._current_position:
                        await self._settle_position(current_market, strike)

                await asyncio.sleep(1.0)

            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                await asyncio.sleep(2.0)

    async def _place_taker_order(self, market: dict, signal: TWAPSignal, ask: float) -> None:
        """Execute immediate taker sniper order against available ask."""
        token_id = market["tokens"].get(signal.side)
        if not token_id or not ask:
            return

        stake_usd = self.risk.calculate_stake(signal.edge_net, ask)
        shares = max(config.MIN_ORDER_SHARES, round(stake_usd / ask, 2))
        cost_usd = round(shares * ask, 2)
        fee_usd = round(signal.fee_taker * shares, 4)

        logger.info("=" * 60)
        logger.info(f"🎯 [H-001 SNIPER ORDER EXECUTED] BUY {signal.side} @ {ask:.3f}")
        logger.info(f"   Z-Score: {signal.z_score:.2f} | Win Prob: {signal.model_prob:.1%} | Net Edge: {signal.edge_net:+.1%}")
        logger.info(f"   Size: {shares:.1f} shares (${cost_usd:.2f}) | Fee: ${fee_usd:.3f}")
        logger.info("=" * 60)

        order_id = f"sniper_paper_{int(time.time()*1000)}"
        if config.MODE == "live" and self.live_client:
            try:
                resp = self.live_client.place_order(token_id, "BUY", ask, shares, order_type="FOK")
                order_id = resp.get("orderID", order_id)
                logger.info(f"Live taker order placed: {resp}")
            except Exception as e:
                logger.error(f"Live taker order placement failed: {e}")
                return

        self._current_position = {
            "slot": market["slug"],
            "strategy": "H-001_SNIPER",
            "side": signal.side,
            "token_id": token_id,
            "price": ask,
            "shares": shares,
            "cost_usd": cost_usd,
            "z_score": signal.z_score,
            "prob": signal.model_prob,
            "fee": fee_usd,
            "rebate": 0.0,
            "entry_time": time.time(),
            "order_id": order_id,
        }

    async def _place_maker_bid(self, market: dict, signal: MakerSignal) -> None:
        """Post resting limit bid inside spread as Maker."""
        token_id = market["tokens"].get(signal.side)
        if not token_id or not signal.target_bid:
            return

        stake_usd = self.risk.calculate_stake(signal.edge_net, signal.target_bid)
        shares = max(config.MIN_ORDER_SHARES, round(stake_usd / signal.target_bid, 2))
        cost_usd = round(shares * signal.target_bid, 2)
        total_rebate = round(signal.maker_rebate * shares, 4)

        logger.info("=" * 60)
        logger.info(f"📥 [H-004 MAKER BID POSTED] RESTING BUY {signal.side} @ {signal.target_bid:.2f}")
        logger.info(f"   Z-Score: {signal.z_score:.2f} | Fair Value: {signal.model_prob:.1%} | Net Edge: {signal.edge_net:+.1%}")
        logger.info(f"   Target Size: {shares:.1f} shares (${cost_usd:.2f}) | Expected Rebate: ${total_rebate:.3f}")
        logger.info("=" * 60)

        order_id = f"maker_paper_{int(time.time()*1000)}"
        if config.MODE == "live" and self.live_client:
            try:
                resp = self.live_client.place_order(token_id, "BUY", signal.target_bid, shares, order_type="GTC")
                order_id = resp.get("orderID", order_id)
                logger.info(f"Live limit order placed: {resp}")
            except Exception as e:
                logger.error(f"Live limit order placement failed: {e}")
                return

        self._resting_order = {
            "slot": market["slug"],
            "side": signal.side,
            "token_id": token_id,
            "price": signal.target_bid,
            "shares": shares,
            "cost_usd": cost_usd,
            "z_score": signal.z_score,
            "prob": signal.model_prob,
            "rebate": total_rebate,
            "placed_at": time.time(),
            "order_id": order_id,
        }

    async def _settle_position(self, market: dict, strike: float) -> None:
        """Settle filled position against final Chainlink TWAP."""
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

        strategy_name = pos.get("strategy", "H-001_SNIPER")
        fee = pos.get("fee", 0.0)
        rebate = pos.get("rebate", 0.0)

        if won:
            revenue = pos["shares"] * 1.0
            pnl = (revenue - pos["cost_usd"]) - fee + rebate
        else:
            pnl = -pos["cost_usd"] - fee + rebate

        self.risk.record_trade_result(pnl)

        logger.info("=" * 60)
        logger.info(f"🏁 [SLOT SETTLEMENT] {market['slug']} ({strategy_name})")
        logger.info(f"   Strike (S0): ${strike:,.2f} | Final TWAP: ${final_twap:,.2f}")
        logger.info(f"   Outcome: {resolved_side} | Trade: {pos['side']} -> {'WON' if won else 'LOST'}")
        logger.info(f"   Trade PnL: ${pnl:+.2f} | New Bankroll: ${self.risk.bankroll:.2f}")
        logger.info("=" * 60)

        # Record to trades.json
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "slot": market["slug"],
            "strategy": strategy_name,
            "side": pos["side"],
            "price": pos["price"],
            "shares": pos["shares"],
            "cost_usd": pos["cost_usd"],
            "fee": fee,
            "rebate": rebate,
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
