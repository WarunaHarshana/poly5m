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
from pathlib import Path

import config
from twap_engine import TWAPEngine, MakerSignal, TWAPSignal
from feeds import ChainlinkRTDSStream, PolymarketBookStream
from polymarket_client import PolymarketPublicClient, PolymarketLiveClient
from risk_manager import RiskManager
from journal import configure as journal_configure
from journal import write as journal_write

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
            min_edge=config.MIN_EDGE,
            min_entry=config.MIN_BUY_PRICE,
            max_entry=config.MAX_BUY_PRICE,
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
        self._slot_done: set[str] = set()  # slugs we already traded/exited this process
        self._risk_block_logged = False
        self._trades_log_path = config.STATE_DIR / "trades.json"
        for _slug in (self._load_slot_state().get("slot_done") or []):
            if _slug:
                self._slot_done.add(str(_slug))


    def _slot_state_path(self) -> Path:
        return config.STATE_DIR / "slot_state.json"

    def _load_slot_state(self) -> dict:
        p = self._slot_state_path()
        if not p.exists():
            return {}
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception as e:
            logger.warning(f"Failed to load slot state: {e}")
            return {}

    def _save_slot_state(
        self,
        *,
        slot_ts: int | float | None,
        slug: str | None,
        strike: float | None,
    ) -> None:
        """Persist S0 + slot identity so redeploys keep impulse continuity."""
        payload = {
            "slot_ts": int(slot_ts) if slot_ts is not None else None,
            "slug": slug,
            "strike": float(strike) if strike is not None else None,
            "slot_done": sorted(self._slot_done),
            "updated_at": time.time(),
        }
        if payload["strike"] is None:
            prev = self._load_slot_state()
            if prev.get("slug") == slug or (
                slot_ts is not None and int(prev.get("slot_ts") or 0) == int(slot_ts)
            ):
                if prev.get("strike") is not None:
                    payload["strike"] = float(prev["strike"])
        try:
            with open(self._slot_state_path(), "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save slot state: {e}")

    def stop(self) -> None:
        logger.info("Stopping bot...")
        self._stop = True
        self.cl_stream.stop()
        self.book_stream.stop()

    async def start(self) -> None:
        logger.info("=" * 70)
        logger.info(f"STARTING POLY 5M BOT | STRATEGY={config.STRATEGY}")
        logger.info(f"Mode: {config.MODE.upper()} | Asset: {config.ASSET.upper()} | Timeframe: {config.TIMEFRAME}")
        if config.STRATEGY in ("H-005_MOMENTUM", "MOMENTUM", "H-005", "LATE_MOMENTUM"):
            logger.info(
                f"Momentum: move>=${config.MOMENTUM_MIN_MOVE_USD:.0f} | ask=[{config.MOMENTUM_MIN_ASK:.2f}..{config.MOMENTUM_MAX_ASK:.2f}] | "
                f"window=[{config.MOMENTUM_WINDOW_END_S:.0f}s..{config.MOMENTUM_WINDOW_START_S:.0f}s left]"
            )
        elif config.STRATEGY in ("H-001_SNIPER", "SNIPER", "H-001"):
            logger.info(
                f"Sniper: min_z={config.MIN_Z_SCORE} | edge>={config.MIN_EDGE:.1%} | "
                f"ask=[{config.MIN_BUY_PRICE:.2f}..{config.MAX_BUY_PRICE:.2f}]"
            )
            logger.info(f"Eval Window: [{config.EVAL_WINDOW_END_S:.0f}s .. {config.EVAL_WINDOW_START_S:.0f}s remaining]")
        else:
            logger.info(
                f"Maker: min_z={config.MIN_Z_SCORE} | min_maker_edge={config.MIN_MAKER_EDGE:.1%} | "
                f"bid_range=[${config.MIN_MAKER_BID:.2f} .. ${config.MAX_MAKER_BID:.2f}]"
            )
            logger.info(f"Quoting Window: [{config.EVAL_WINDOW_END_S:.0f}s .. {config.EVAL_WINDOW_START_S:.0f}s remaining]")
        journal_configure(config.JOURNAL_PATH)
        logger.info(f"Journal: {config.JOURNAL_PATH}")
        logger.info(f"Bankroll: ${self.risk.bankroll:.2f} | Base Stake: ${config.BASE_STAKE_USD:.2f}")
        if config.MODE != "live":
            logger.info("Daily-loss / consecutive-loss kill switches: DISABLED (paper mode keeps filling)")
        else:
            logger.info(f"Daily loss limit: ${config.DAILY_LOSS_LIMIT_USD:.2f} | Max consecutive losses: {config.MAX_CONSECUTIVE_LOSSES}")
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
                        saved = self._load_slot_state()
                        same_slot = (
                            int(saved.get("slot_ts") or 0) == int(active_slot_ts)
                            or saved.get("slug") == current_market.get("slug")
                        )
                        if same_slot and saved.get("strike") is not None:
                            try:
                                restored = float(saved["strike"])
                                if restored > 0:
                                    slot_strike = restored
                                    logger.info(
                                        f"[SLOT {active_slot_ts}] Restored S0 ${slot_strike:,.2f} from volume "
                                        f"(survived redeploy)"
                                    )
                            except (TypeError, ValueError):
                                slot_strike = None
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

                # Latch slot strike S_0 once at or near slot open (persist to volume)
                if slot_strike is None or slot_strike <= 0:
                    slot_strike = self.cl_stream.get_slot_strike(slot_start_ts)
                    if slot_strike is None or slot_strike <= 0:
                        slot_strike = self.cl_stream.price()
                    if slot_strike and slot_strike > 0:
                        self._save_slot_state(
                            slot_ts=active_slot_ts,
                            slug=current_market.get("slug"),
                            strike=slot_strike,
                        )

                strike = slot_strike
                spot = self.cl_stream.price()

                if self.cl_stream.is_stale(max_age_s=6.0):
                    logger.warning(f"[FEED] Chainlink tick stream waiting for live tick... (last: ${spot:,.2f})")
                    await asyncio.sleep(1.0)
                    continue

                realized_twap, _ = self.cl_stream.compute_realized_twap(start_ts=slot_start_ts, end_ts=now)
                chop = self.cl_stream.strike_crossings_since(slot_start_ts, strike)

                # Strategy flags
                is_momentum = (config.STRATEGY in ("H-005_MOMENTUM", "MOMENTUM", "H-005", "LATE_MOMENTUM"))
                is_sniper = (config.STRATEGY in ("H-001_SNIPER", "SNIPER", "H-001"))

                # Evaluate strategy within entry window
                if is_momentum:
                    in_eval_window = (
                        config.MOMENTUM_WINDOW_END_S <= remaining_s <= config.MOMENTUM_WINDOW_START_S
                    )
                else:
                    in_eval_window = (
                        config.EVAL_WINDOW_END_S <= remaining_s <= config.EVAL_WINDOW_START_S
                    )

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

                if is_momentum:
                    can_trade_now, risk_reason_now = self.risk.can_trade()
                    if not can_trade_now:
                        # Avoid per-second spam once daily loss / consecutive-loss lock is hit
                        if not getattr(self, "_risk_block_logged", False):
                            logger.warning(f"Momentum paused by risk manager: {risk_reason_now}")
                            journal_write("risk_block", reason=risk_reason_now, mode=config.MODE)
                            self._risk_block_logged = True
                        await asyncio.sleep(2.0)
                        continue
                    self._risk_block_logged = False
                    # Stronger-side check (Novals): also peek opposite ask when available
                    opp_side = "DOWN" if prelim_side == "UP" else "UP"
                    opp_token = tokens.get(opp_side)
                    opp_ask = None
                    if opp_token:
                        opp_top = self.book_stream.get_top(opp_token)
                        opp_ask = opp_top.get("ask") if opp_top else None
                        if opp_ask is None:
                            bk = self.pub_client.get_order_book(opp_token)
                            if bk and bk.get("asks"):
                                opp_ask = float(bk["asks"][0]["price"])
                    # Prefer stronger ask >= threshold; still require with-trend side
                    trade_side = prelim_side
                    trade_ask = best_ask
                    if best_ask is not None and opp_ask is not None:
                        if opp_ask > best_ask and ((spot >= strike and opp_side == "UP") or (spot < strike and opp_side == "DOWN")):
                            trade_side = opp_side
                            trade_ask = opp_ask
                        elif best_ask >= config.MOMENTUM_MIN_ASK or opp_ask >= config.MOMENTUM_MIN_ASK:
                            # pick stronger among same-direction-compatible; default keep trend side
                            if trade_side == prelim_side and (best_ask or 0) < config.MOMENTUM_MIN_ASK and (opp_ask or 0) >= config.MOMENTUM_MIN_ASK:
                                # opposite meets threshold but would be a fade — skip via gate later
                                pass

                    signal = self.engine.evaluate_momentum(
                        spot=spot,
                        strike=strike,
                        remaining_s=remaining_s,
                        elapsed_s=elapsed_s,
                        ask=trade_ask if trade_side == prelim_side else best_ask,
                        fee_rate=current_market.get("fee_rate", config.TAKER_FEE_RATE),
                        min_move_usd=config.MOMENTUM_MIN_MOVE_USD,
                        max_move_usd=config.MOMENTUM_MAX_MOVE_USD,
                        min_ask=config.MOMENTUM_MIN_ASK,
                        max_ask=config.MOMENTUM_MAX_ASK,
                        min_edge=config.MOMENTUM_MIN_EDGE,
                        window_start_s=config.MOMENTUM_WINDOW_START_S,
                        window_end_s=config.MOMENTUM_WINDOW_END_S,
                    )

                    if in_eval_window:
                        move = spot - strike
                        logger.info(
                            f"[{remaining_s:3.0f}s left] BTC: ${spot:,.1f} | S0: ${strike:,.1f} | "
                            f"Move: ${move:+.1f} | Ask({signal.side}): {best_ask or 0:.2f} | "
                            f"P: {signal.model_prob:.1%} | Edge: {signal.edge_net:+.1%} | Pass: {signal.passed_gates}"
                            + (f" | Skip: {signal.skip_reason}" if (not signal.passed_gates and signal.skip_reason) else "")
                        )

                        if signal.passed_gates and not self._current_position:
                            slot_key = current_market.get("slug", "")
                            if slot_key and slot_key in self._slot_done:
                                pass  # already traded/exited this slot
                            else:
                                can_trade, risk_reason = self.risk.can_trade()
                                if can_trade:
                                    await self._place_taker_order(
                                        current_market, signal, best_ask, strategy_tag="H-005_MOMENTUM"
                                    )
                                    if slot_key:
                                        self._slot_done.add(slot_key)
                                        self._save_slot_state(
                                            slot_ts=active_slot_ts,
                                            slug=slot_key,
                                            strike=strike,
                                        )
                                else:
                                    logger.warning(f"Momentum trade blocked by risk manager: {risk_reason}")

                elif is_sniper:
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

                # Momentum overlays: stop-loss / early exit / micro-hedge
                if self._current_position and self._current_position.get("strategy") in (
                    "H-005_MOMENTUM", "LATE_MOMENTUM", "MOMENTUM", "H-005"
                ):
                    await self._manage_open_momentum_position(
                        current_market, remaining_s, tokens
                    )

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

    async def _place_taker_order(self, market: dict, signal: TWAPSignal, ask: float, strategy_tag: str = "H-001_SNIPER") -> None:
        """Execute immediate taker sniper order against available ask."""
        token_id = market["tokens"].get(signal.side)
        if not token_id or not ask:
            return

        stake_usd = self.risk.calculate_stake(signal.edge_net, ask)
        shares = max(config.MIN_ORDER_SHARES, round(stake_usd / ask, 2))
        cost_usd = round(shares * ask, 2)
        fee_usd = round(signal.fee_taker * shares, 4)

        logger.info("=" * 60)
        logger.info(f"[{strategy_tag} ORDER EXECUTED] BUY {signal.side} @ {ask:.3f}")
        logger.info(f"   Z-Score: {signal.z_score:.2f} | Win Prob: {signal.model_prob:.1%} | Net Edge: {signal.edge_net:+.1%}")
        logger.info(f"   Size: {shares:.1f} shares (${cost_usd:.2f}) | Fee: ${fee_usd:.3f}")
        logger.info("=" * 60)

        order_id = f"{strategy_tag.lower()}_paper_{int(time.time()*1000)}"
        is_paper = config.MODE != "live"
        if config.MODE == "live" and self.live_client:
            try:
                resp = self.live_client.place_order(token_id, "BUY", ask, shares, order_type="FOK")
                order_id = resp.get("order_id") or resp.get("orderID") or order_id
                logger.info(f"Live taker order placed: {resp}")
                if not resp.get("filled"):
                    logger.warning(
                        f"Live FOK not filled (post!=fill). status={resp.get('status')} — skipping position"
                    )
                    journal_write(
                        "skip_round",
                        reason="fok_unfilled",
                        side=signal.side,
                        ask=ask,
                        response=str(resp)[:300],
                    )
                    return
                if resp.get("shares"):
                    shares = float(resp["shares"])
                    cost_usd = round(shares * ask, 2)
            except Exception as e:
                logger.error(f"Live taker order placement failed: {e}")
                return

        journal_write(
            "paper_fill" if is_paper else "fill",
            strategy=strategy_tag,
            side=signal.side,
            price=ask,
            shares=shares,
            cost_usd=cost_usd,
            slot=market.get("slug"),
            order_id=order_id,
            z_score=signal.z_score,
            edge=signal.edge_net,
        )

        self._current_position = {
            "slot": market["slug"],
            "strategy": strategy_tag,
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
                order_id = resp.get("order_id") or resp.get("orderID") or order_id
                logger.info(f"Live limit order placed: {resp}")
                if not resp.get("success"):
                    logger.warning(f"Live GTC post failed: {resp}")
                    return
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


    async def _manage_open_momentum_position(self, market: dict, remaining_s: float, tokens: dict) -> None:
        """Stop-loss, exit-before-close, and optional micro-hedge for H-005 positions."""
        pos = self._current_position
        if not pos:
            return

        side = pos["side"]
        token_id = pos.get("token_id") or tokens.get(side)
        best_bid = None
        best_ask = None
        if token_id:
            top = self.book_stream.get_top(token_id)
            best_bid = top.get("bid") if top else None
            best_ask = top.get("ask") if top else None
            if best_bid is None:
                bk = self.pub_client.get_order_book(token_id)
                if bk:
                    if bk.get("bids"):
                        best_bid = float(bk["bids"][0]["price"])
                    if bk.get("asks"):
                        best_ask = float(bk["asks"][0]["price"])

        entry = float(pos["price"])
        # Mark carefully: raw best bid on thin CLOB books is often garbage (e.g. 0.28 vs 0.87 ask).
        mark = None
        if best_bid is not None and best_ask is not None:
            spread = float(best_ask) - float(best_bid)
            if 0 <= spread <= 0.15:
                mark = (float(best_bid) + float(best_ask)) / 2.0
            else:
                # Wide/absurd book — do not stop-loss on thin bid alone
                mark = None
        elif best_ask is not None:
            mark = float(best_ask)

        # 1) Stop-loss vs entry (mid only when spread is sane)
        if (
            config.MOMENTUM_STOP_LOSS_ENABLED
            and mark is not None
            and entry > 0
            and mark <= entry * (1.0 - float(config.MOMENTUM_STOP_LOSS_PCT))
        ):
            logger.warning(
                f"[H-005 STOP-LOSS] mark {mark:.3f} <= entry {entry:.3f} * "
                f"(1-{config.MOMENTUM_STOP_LOSS_PCT:.0%})"
            )
            await self._exit_position_early(market, mark, reason="stop_loss")
            return

        # 2) Exit before final seconds (avoid last-second oracle chop)
        if remaining_s <= float(config.MOMENTUM_EXIT_BEFORE_S) and remaining_s > 0.5:
            exit_px = mark if mark is not None else entry
            logger.info(
                f"[H-005 EXIT-BEFORE] {remaining_s:.1f}s left <= {config.MOMENTUM_EXIT_BEFORE_S:.0f}s; "
                f"flatten @ {exit_px:.3f}"
            )
            await self._exit_position_early(market, exit_px, reason="exit_before")
            return

        # 3) Micro-hedge when main side ask is extremely rich / skew extreme
        if (
            config.MOMENTUM_HEDGE_ENABLED
            and not pos.get("hedge_placed")
            and remaining_s <= float(config.MOMENTUM_HEDGE_TRIGGER_SECONDS_LEFT)
        ):
            main_ask = float(best_ask) if best_ask is not None else None
            # Extreme conviction on our side (crowd piled in) -> tiny opposite hedge
            if main_ask is not None and main_ask >= float(config.MOMENTUM_HEDGE_TRIGGER_ASK):
                await self._place_micro_hedge(market, tokens, pos, main_ask)

    async def _place_micro_hedge(self, market: dict, tokens: dict, pos: dict, main_ask: float) -> None:
        """Buy a small opposite-side ticket when skew is extreme (Novals-style)."""
        hedge_side = "DOWN" if pos["side"] == "UP" else "UP"
        hedge_token = tokens.get(hedge_side)
        if not hedge_token:
            return

        top = self.book_stream.get_top(hedge_token)
        hedge_ask = top.get("ask") if top else None
        if hedge_ask is None:
            bk = self.pub_client.get_order_book(hedge_token)
            if bk and bk.get("asks"):
                hedge_ask = float(bk["asks"][0]["price"])
        if hedge_ask is None or hedge_ask <= 0:
            return

        main_cost = float(pos.get("cost_usd", 0.0))
        notional = main_cost * (float(config.MOMENTUM_HEDGE_SHARE_PCT) / 100.0)
        notional = max(float(config.MOMENTUM_HEDGE_NOTIONAL_MIN), notional)
        notional = min(float(config.MOMENTUM_HEDGE_NOTIONAL_MAX), notional)
        shares = max(1.0, round(notional / hedge_ask, 2))
        cost = round(shares * hedge_ask, 2)

        logger.info("=" * 60)
        logger.info(
            f"[H-005 MICRO-HEDGE] BUY {hedge_side} @ {hedge_ask:.3f} | "
            f"{shares:.1f} sh (${cost:.2f}) | trigger main_ask={main_ask:.3f}"
        )
        logger.info("=" * 60)

        order_id = f"h005_hedge_paper_{int(time.time()*1000)}"
        if config.MODE == "live" and self.live_client:
            try:
                resp = self.live_client.place_order(hedge_token, "BUY", hedge_ask, shares, order_type="FOK")
                order_id = resp.get("orderID", order_id)
            except Exception as e:
                logger.error(f"Live micro-hedge failed: {e}")
                return

        pos["hedge_placed"] = True
        pos["hedge_side"] = hedge_side
        pos["hedge_token_id"] = hedge_token
        pos["hedge_price"] = hedge_ask
        pos["hedge_shares"] = shares
        pos["hedge_cost_usd"] = cost
        pos["hedge_order_id"] = order_id

    async def _exit_position_early(self, market: dict, exit_price: float, reason: str) -> None:
        """Flatten main (+ optional hedge) before TWAP settlement; paper marks at exit_price."""
        pos = self._current_position
        if not pos:
            return
        self._current_position = None
        slot_key = market.get("slug") or pos.get("slot")
        if slot_key:
            self._slot_done.add(slot_key)
            self._save_slot_state(
                slot_ts=market.get("slot_start_ts"),
                slug=slot_key,
                strike=None,
            )

        shares = float(pos["shares"])
        cost = float(pos["cost_usd"])
        fee = float(pos.get("fee", 0.0))
        rebate = float(pos.get("rebate", 0.0))
        exit_price = float(exit_price)

        # Paper / live flatten of main
        if config.MODE == "live" and self.live_client:
            try:
                self.live_client.place_order(
                    pos["token_id"], "SELL", max(0.01, exit_price), shares, order_type="FOK"
                )
            except Exception as e:
                logger.error(f"Live early exit sell failed ({reason}): {e}")

        main_pnl = (shares * exit_price) - cost - fee + rebate

        hedge_pnl = 0.0
        if pos.get("hedge_placed"):
            h_shares = float(pos.get("hedge_shares", 0.0))
            h_cost = float(pos.get("hedge_cost_usd", 0.0))
            h_token = pos.get("hedge_token_id")
            # Mark hedge at its best bid if available, else assume worthless residual
            h_exit = 0.0
            if h_token:
                top = self.book_stream.get_top(h_token)
                if top and top.get("bid") is not None:
                    h_exit = float(top["bid"])
            if config.MODE == "live" and self.live_client and h_token and h_shares > 0:
                try:
                    self.live_client.place_order(h_token, "SELL", max(0.01, h_exit or 0.01), h_shares, order_type="FOK")
                except Exception as e:
                    logger.error(f"Live hedge exit failed: {e}")
            hedge_pnl = (h_shares * h_exit) - h_cost

        pnl = main_pnl + hedge_pnl
        self.risk.record_trade_result(pnl)

        logger.info("=" * 60)
        logger.info(f"[H-005 EARLY EXIT:{reason}] {market['slug']}")
        logger.info(
            f"   Main {pos['side']} entry={pos['price']:.3f} exit={exit_price:.3f} | "
            f"PnL ${pnl:+.2f} | Bankroll ${self.risk.bankroll:.2f}"
        )
        logger.info("=" * 60)

        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "slot": market["slug"],
            "strategy": pos.get("strategy", "H-005_MOMENTUM"),
            "side": pos["side"],
            "price": pos["price"],
            "exit_price": exit_price,
            "exit_reason": reason,
            "shares": shares,
            "cost_usd": cost,
            "fee": fee,
            "rebate": rebate,
            "hedge_placed": bool(pos.get("hedge_placed")),
            "hedge_side": pos.get("hedge_side"),
            "hedge_cost_usd": pos.get("hedge_cost_usd", 0.0),
            "hedge_pnl": round(hedge_pnl, 2),
            "z_score": pos.get("z_score"),
            "won": pnl > 0,
            "pnl": round(pnl, 2),
            "bankroll": round(self.risk.bankroll, 2),
            "early_exit": True,
        }
        journal_write(
            "settlement",
            slot=record.get("slot"),
            strategy=record.get("strategy"),
            side=record.get("side"),
            won=record.get("won"),
            pnl=record.get("pnl"),
            bankroll=record.get("bankroll"),
            early_exit=True,
        )
        self._save_trade_record(record)

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

        # If a micro-hedge was on, it resolves opposite the main outcome
        if pos.get("hedge_placed"):
            h_shares = float(pos.get("hedge_shares", 0.0))
            h_cost = float(pos.get("hedge_cost_usd", 0.0))
            hedge_won = not won
            if hedge_won:
                pnl += (h_shares * 1.0) - h_cost
            else:
                pnl += -h_cost

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
        journal_write(
            "settlement",
            slot=market["slug"],
            strategy=strategy_name,
            side=pos["side"],
            outcome=resolved_side,
            won=won,
            pnl=round(pnl, 2),
            bankroll=round(self.risk.bankroll, 2),
            early_exit=False,
        )
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
