"""High-Conviction TWAP Mispricing & Passive Maker Trading Bot (H-004).

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


def _engine_for(params: config.AssetParams) -> TWAPEngine:
    return TWAPEngine(
        alpha=config.ALPHA,
        min_sigma_usd=params.min_sigma_usd(config._BTC_REF_SPOT if params.symbol == "btc" else 1.0),
        min_z=params.min_z,
        min_edge=params.min_edge,
        min_entry=params.min_buy_price,
        max_entry=params.max_buy_price,
        min_maker_bid=params.min_maker_bid,
        max_maker_bid=params.max_maker_bid,
        min_maker_edge=params.min_maker_edge,
        max_chop_crosses=params.max_chop_crosses,
        default_daily_vol=params.daily_vol,
    )


@dataclass
class CoinSlot:
    """Per-coin state for one 5-minute window."""

    asset: str
    params: config.AssetParams
    engine: TWAPEngine
    market: dict | None = None
    strike: float | None = None
    position: dict | None = None
    resting_order: dict | None = None
    risk_block_logged: bool = False


class TWAPBot:
    """Trading Bot executing H-001 / H-004 / H-005 across 5-minute Up/Down coins."""

    def __init__(self):
        self.slots: dict[str, CoinSlot] = {
            a: CoinSlot(asset=a, params=config.ASSET_PARAMS[a], engine=_engine_for(config.ASSET_PARAMS[a]))
            for a in config.ASSETS
        }
        # Back-compat alias used by older tests/helpers
        self.engine = next(iter(self.slots.values())).engine if self.slots else TWAPEngine()
        self.cl_stream = ChainlinkRTDSStream(assets=config.ASSETS)
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
        self._started_at = time.time()
        self._http: StatusServer | None = None
        self._slot_done: set[str] = set()  # slugs we already traded/exited this process
        self._pending_settles: list[dict] = []
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
        asset: str | None = None,
    ) -> None:
        """Persist S0 + slot identity so redeploys keep impulse continuity."""
        coin = str(asset or "btc").lower()
        prev = self._load_slot_state()
        assets_map = dict(prev.get("assets") or {})
        if not assets_map and prev.get("slug"):
            assets_map["btc"] = {"slug": prev.get("slug"), "strike": prev.get("strike")}
        entry = dict(assets_map.get(coin) or {})
        if slug:
            entry["slug"] = slug
        if strike is not None:
            entry["strike"] = float(strike)
        elif entry.get("strike") is None:
            if prev.get("slug") == slug or (
                slot_ts is not None and int(prev.get("slot_ts") or 0) == int(slot_ts)
            ):
                if prev.get("strike") is not None:
                    entry["strike"] = float(prev["strike"])
        assets_map[coin] = entry
        payload = {
            "slot_ts": int(slot_ts) if slot_ts is not None else prev.get("slot_ts"),
            "slug": entry.get("slug", slug),
            "strike": entry.get("strike"),
            "asset": coin,
            "assets": assets_map,
            "slot_done": sorted(self._slot_done),
            "updated_at": time.time(),
        }
        try:
            with open(self._slot_state_path(), "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save slot state: {e}")

    def _reconcile_paper_blotter(self) -> None:
        """Paper bankroll follows official Polymarket settlements only."""
        if config.MODE == "live":
            return
        trades = load_trades(self._trades_log_path)
        official = [t for t in trades if is_official_trade(t)]
        ghost = len(trades) - len(official)
        pnl = sum(float(t.get("pnl") or 0.0) for t in official if t.get("won") is not None)
        self.risk.bankroll = round(config.STARTING_BANKROLL + pnl, 2)
        self.risk.daily_pnl = round(
            sum(float(t.get("pnl") or 0.0) for t in official if t.get("won") is not None),
            2,
        )
        self.risk.save_state()
        if ghost:
            logger.warning(
                f"Paper blotter: {ghost} unofficial local-TWAP rows ignored. "
                f"Bankroll reset to ${self.risk.bankroll:.2f} from official PnL only."
            )

    def _open_notional(self) -> float:
        total = 0.0
        for slot in self.slots.values():
            if slot.position:
                total += float(slot.position.get("cost_usd") or 0.0)
            elif slot.resting_order:
                total += float(slot.resting_order.get("cost_usd") or 0.0)
        # Include capital tied up in positions awaiting Polymarket resolution
        for item in self._pending_settles:
            pos = item.get("position") or {}
            total += float(pos.get("cost_usd") or 0.0)
        return total

    def _slot_entries_count(self) -> int:
        """Count how many coins already have open positions in this window."""
        count = 0
        for slot in self.slots.values():
            if slot.position:
                count += 1
        return count

    def _restored_strike(self, saved: dict, asset: str, slug: str | None, slot_ts: int) -> float | None:
        assets_map = saved.get("assets") or {}
        entry = assets_map.get(asset) or {}
        if not entry and asset == "btc":
            if saved.get("slug") == slug or int(saved.get("slot_ts") or 0) == int(slot_ts):
                entry = {"strike": saved.get("strike")}
        raw = entry.get("strike")
        try:
            val = float(raw)
            return val if val > 0 else None
        except (TypeError, ValueError):
            return None

    def snapshot(self) -> dict:
        open_positions = []
        for slot in self.slots.values():
            if not slot.position:
                continue
            pos = slot.position
            open_positions.append({
                "asset": slot.asset,
                "slug": pos.get("slot") or (slot.market or {}).get("slug"),
                "side": pos.get("side"),
                "price": pos.get("price"),
                "shares": pos.get("shares"),
                "cost_usd": pos.get("cost_usd"),
                "strategy": pos.get("strategy"),
            })
        pending = []
        for item in self._pending_settles:
            pos = item.get("position") or {}
            pending.append({
                "asset": item.get("asset"),
                "slug": (item.get("market") or {}).get("slug"),
                "side": pos.get("side"),
                "price": pos.get("price"),
                "shares": pos.get("shares"),
                "status": "awaiting_polymarket_resolution",
            })
        return {
            "mode": config.MODE,
            "strategy": config.STRATEGY,
            "assets": list(config.ASSETS),
            "bankroll": round(float(self.risk.bankroll), 2),
            "daily_pnl": round(float(self.risk.daily_pnl), 2),
            "open_positions": open_positions,
            "pending_settlements": pending,
            "uptime_seconds": round(time.time() - self._started_at, 1),
        }

    def stop(self) -> None:
        logger.info("Stopping bot...")
        self._stop = True
        self.cl_stream.stop()
        self.book_stream.stop()
        if self._http:
            self._http.stop()

    async def start(self) -> None:
        logger.info("=" * 70)
        logger.info(f"STARTING POLY 5M BOT | STRATEGY={config.STRATEGY}")
        coins = ",".join(a.upper() for a in config.ASSETS)
        logger.info(f"Mode: {config.MODE.upper()} | Assets: {coins} | Timeframe: {config.TIMEFRAME}")
        logger.info("One position per coin per 5-minute window")
        for a in config.ASSETS:
            p = config.ASSET_PARAMS[a]
            logger.info(
                f"  {a.upper()}: min_z={p.min_z} edge>={p.min_edge:.1%} "
                f"ask=[{p.min_buy_price:.2f}..{p.max_buy_price:.2f}] "
                f"vol={p.daily_vol:.2%} sigma={p.min_sigma_bps:.2f}bps chop={p.chop_noise_bps:.2f}bps"
            )
        if config.STRATEGY in ("H-005_MOMENTUM", "MOMENTUM", "H-005", "LATE_MOMENTUM"):
            logger.info(
                f"Momentum: move>=${config.MOMENTUM_MIN_MOVE_USD:.0f} | ask=[{config.MOMENTUM_MIN_ASK:.2f}..{config.MOMENTUM_MAX_ASK:.2f}] | "
                f"window=[{config.MOMENTUM_WINDOW_END_S:.0f}s..{config.MOMENTUM_WINDOW_START_S:.0f}s left]"
            )
            logger.info(
                f"Overlays: SL={'on' if config.MOMENTUM_STOP_LOSS_ENABLED else 'off'} "
                f"exit_before={config.MOMENTUM_EXIT_BEFORE_S:g}s "
                f"hedge={'on' if config.MOMENTUM_HEDGE_ENABLED else 'off'}"
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
        self._reconcile_paper_blotter()
        if self._trades_log_path.exists():
            try:
                all_trades = load_trades(self._trades_log_path)
                official = [t for t in all_trades if is_official_trade(t)]
                shown = official if config.MODE != "live" else all_trades
                total_wins = sum(1 for t in shown if t.get("won"))
                total_losses = sum(1 for t in shown if t.get("won") is False)
                total_pnl = sum(float(t.get("pnl", 0.0)) for t in shown)
                ghost = len(all_trades) - len(official)
                logger.info(
                    f"Historical Blotter: {len(shown)} official trades ({total_wins}W / {total_losses}L) "
                    f"| Net PnL: ${total_pnl:+.2f}"
                    + (f" | ignored {ghost} unofficial local-TWAP rows" if ghost else "")
                )
            except Exception:
                pass
        logger.info("=" * 70)

        try:
            self._http = StatusServer(
                snapshot=self.snapshot,
                trades_path=self._trades_log_path,
                host=config.HTTP_HOST,
                port=config.HTTP_PORT,
            )
            self._http.start()
        except Exception as e:
            logger.warning(f"Status HTTP server failed to bind: {e}")

        # Start feeds
        logger.info("Connecting to Chainlink RTDS WebSocket (wss://ws-live-data.polymarket.com)...")
        self.cl_stream.start()
        self.book_stream.start()

        # Non-blocking async wait for stream ready
        for _ in range(30):
            if self.cl_stream.ready:
                break
            await asyncio.sleep(0.5)

        for a in config.ASSETS:
            px = self.cl_stream.price(a)
            live = self.cl_stream.ready_for(a)
            p = config.ASSET_PARAMS[a]
            if live and px > 0:
                logger.info(f"Chainlink {a.upper()} Spot: ${p.fmt_px(px)}")
            else:
                logger.warning(f"Chainlink {a.upper()} not ready yet")
        logger.info(f"Clock Offset: {self.cl_stream.clock_offset_s():+.2f}s")

        active_slot_ts = None

        while not self._stop:
            try:
                now = time.time()
                current_window = int(now // config.SLOT_SECONDS * config.SLOT_SECONDS)

                if active_slot_ts != current_window:
                    if active_slot_ts is not None:
                        for slot in self.slots.values():
                            self._queue_settlement(slot)
                            slot.resting_order = None
                    active_slot_ts = current_window
                    await self._discover_slot(current_window, now)

                await self._flush_pending_settlements(now)

                for slot in self.slots.values():
                    await self._tick_asset(slot, now)

                await asyncio.sleep(1.0)

            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                await asyncio.sleep(2.0)

    async def _discover_slot(self, window_ts: int, now: float) -> None:
        saved = self._load_slot_state()
        same_slot = int(saved.get("slot_ts") or 0) == int(window_ts)
        for slot in self.slots.values():
            slot.market = None
            slot.strike = None
            market = self.pub_client.find_active_5m_slot(now_ts=now, asset=slot.asset)
            if not market:
                logger.warning(f"[{slot.asset.upper()}] Could not discover market for slot {window_ts}")
                continue
            slot.market = market
            logger.info(f"[SLOT {window_ts}] {slot.asset.upper()} {market['slug']}")
            tokens = market["tokens"]
            self.book_stream.watch(tokens.get("UP", ""), tokens.get("DOWN", ""))
            restored = self._restored_strike(saved, slot.asset, market.get("slug"), window_ts) if same_slot else None
            if restored:
                slot.strike = restored
                logger.info(
                    f"[{slot.asset.upper()}] Restored S0 ${slot.params.fmt_px(slot.strike)} from volume "
                    f"(survived redeploy)"
                )

    async def _tick_asset(self, slot: CoinSlot, now: float) -> None:
        market = slot.market
        if not market:
            return
        params = slot.params
        asset = slot.asset
        end_ts = market["end_ts"]
        slot_start_ts = market["slot_start_ts"]
        remaining_s = max(0.0, end_ts - now)
        elapsed_s = max(0.0, now - slot_start_ts)

        if not self.cl_stream.ready_for(asset):
            return
        if self.cl_stream.is_stale(max_age_s=config.MAX_ORACLE_STALE_S, asset=asset):
            logger.warning(f"[FEED] {asset.upper()} Chainlink waiting for live tick...")
            return

        if slot.strike is None or slot.strike <= 0:
            slot.strike = self.cl_stream.get_slot_strike(slot_start_ts, asset=asset)
            if slot.strike is None or slot.strike <= 0:
                slot.strike = self.cl_stream.price(asset)
            if slot.strike and slot.strike > 0:
                self._save_slot_state(
                    slot_ts=slot_start_ts,
                    slug=market.get("slug"),
                    strike=slot.strike,
                    asset=asset,
                )

        strike = slot.strike
        spot = self.cl_stream.price(asset)
        if not strike or strike <= 0 or not spot or spot <= 0:
            return

        realized_twap, _ = self.cl_stream.compute_realized_twap(
            start_ts=slot_start_ts, end_ts=now, asset=asset
        )
        chop = self.cl_stream.strike_crossings_since(
            slot_start_ts, strike, noise_buffer_usd=params.chop_noise_usd(spot), asset=asset
        )
        min_sigma = params.min_sigma_usd(spot)

        is_momentum = (config.STRATEGY in ("H-005_MOMENTUM", "MOMENTUM", "H-005", "LATE_MOMENTUM"))
        is_sniper = (config.STRATEGY in ("H-001_SNIPER", "SNIPER", "H-001"))

        if is_momentum:
            in_eval_window = (
                config.MOMENTUM_WINDOW_END_S <= remaining_s <= config.MOMENTUM_WINDOW_START_S
            )
        else:
            in_eval_window = (
                config.EVAL_WINDOW_END_S <= remaining_s <= config.EVAL_WINDOW_START_S
            )

        tokens = market["tokens"]
        prelim_side = "UP" if spot >= strike else "DOWN"
        token_id = tokens.get(prelim_side)

        top = self.book_stream.get_top(token_id) if token_id else None
        best_bid = top.get("bid") if top else None
        best_ask = top.get("ask") if top else None

        if best_bid is None and token_id:
            bk = self.pub_client.get_order_book(token_id)
            if bk:
                if bk.get("bids"):
                    best_bid = float(bk["bids"][0]["price"])
                if bk.get("asks"):
                    best_ask = float(bk["asks"][0]["price"])

        open_notional = self._open_notional()
        tag = asset.upper()
        px = params.fmt_px

        if is_momentum:
            can_trade_now, risk_reason_now = self.risk.can_trade(open_notional=open_notional)
            if not can_trade_now:
                if not slot.risk_block_logged:
                    logger.warning(f"[{tag}] Momentum paused by risk manager: {risk_reason_now}")
                    journal_write("risk_block", reason=risk_reason_now, mode=config.MODE, asset=asset)
                    slot.risk_block_logged = True
                return
            slot.risk_block_logged = False
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
            trade_side = prelim_side
            trade_ask = best_ask
            if best_ask is not None and opp_ask is not None:
                if opp_ask > best_ask and ((spot >= strike and opp_side == "UP") or (spot < strike and opp_side == "DOWN")):
                    trade_side = opp_side
                    trade_ask = opp_ask
                elif best_ask >= config.MOMENTUM_MIN_ASK or opp_ask >= config.MOMENTUM_MIN_ASK:
                    if trade_side == prelim_side and (best_ask or 0) < config.MOMENTUM_MIN_ASK and (opp_ask or 0) >= config.MOMENTUM_MIN_ASK:
                        pass

            signal = slot.engine.evaluate_momentum(
                spot=spot,
                strike=strike,
                remaining_s=remaining_s,
                elapsed_s=elapsed_s,
                ask=trade_ask if trade_side == prelim_side else best_ask,
                fee_rate=market.get("fee_rate", config.TAKER_FEE_RATE),
                min_move_usd=params.momentum_min_move_usd(spot),
                max_move_usd=params.momentum_max_move_usd(spot),
                min_ask=config.MOMENTUM_MIN_ASK,
                max_ask=config.MOMENTUM_MAX_ASK,
                min_edge=config.MOMENTUM_MIN_EDGE,
                window_start_s=config.MOMENTUM_WINDOW_START_S,
                window_end_s=config.MOMENTUM_WINDOW_END_S,
            )

            if in_eval_window:
                move = spot - strike
                logger.info(
                    f"[{remaining_s:3.0f}s left] {tag}: ${px(spot)} | S0: ${px(strike)} | "
                    f"Move: ${move:+.4g} | Ask({signal.side}): {best_ask or 0:.2f} | "
                    f"P: {signal.model_prob:.1%} | Edge: {signal.edge_net:+.1%} | Pass: {signal.passed_gates}"
                    + (f" | Skip: {signal.skip_reason}" if (not signal.passed_gates and signal.skip_reason) else "")
                )

                if signal.passed_gates and not slot.position:
                    slot_key = market.get("slug", "")
                    if slot_key and slot_key in self._slot_done:
                        pass
                    elif self._slot_entries_count() >= config.MAX_CONCURRENT_ENTRIES:
                        logger.warning(
                            f"[{tag}] Momentum trade skipped: slot entry cap ({config.MAX_CONCURRENT_ENTRIES}) reached"
                        )
                    else:
                        can_trade, risk_reason = self.risk.can_trade(open_notional=open_notional)
                        if can_trade:
                            await self._place_taker_order(
                                slot, market, signal, best_ask, strategy_tag="H-005_MOMENTUM"
                            )
                            # Only mark done if order actually filled (position populated)
                            if slot.position and slot_key:
                                self._slot_done.add(slot_key)
                                self._save_slot_state(
                                    slot_ts=slot_start_ts,
                                    slug=slot_key,
                                    strike=strike,
                                    asset=asset,
                                )
                        else:
                            logger.warning(f"[{tag}] Momentum trade blocked by risk manager: {risk_reason}")

        elif is_sniper:
            signal = slot.engine.evaluate(
                spot=spot,
                strike=strike,
                remaining_s=remaining_s,
                elapsed_s=elapsed_s,
                realized_twap=realized_twap,
                ask=best_ask,
                chop_crosses=chop,
                fee_rate=market.get("fee_rate", config.TAKER_FEE_RATE),
                realized_vol_daily=params.daily_vol,
                min_sigma_usd=min_sigma,
            )

            if in_eval_window:
                logger.info(
                    f"[{remaining_s:3.0f}s left] {tag}: ${px(spot)} | S0: ${px(strike)} | "
                    f"Z: {signal.z_score:4.2f} | P: {signal.model_prob:.1%} | "
                    f"Ask({signal.side}): {best_ask or 0:.2f} | "
                    f"Edge: {signal.edge_net:+.1%} | Pass: {signal.passed_gates}"
                )

                if signal.passed_gates and not slot.position:
                    if self._slot_entries_count() >= config.MAX_CONCURRENT_ENTRIES:
                        logger.warning(
                            f"[{tag}] Sniper trade skipped: slot entry cap ({config.MAX_CONCURRENT_ENTRIES}) reached"
                        )
                    else:
                        can_trade, risk_reason = self.risk.can_trade(open_notional=open_notional)
                        if can_trade:
                            await self._place_taker_order(slot, market, signal, best_ask)
                        else:
                            logger.warning(f"[{tag}] Sniper trade blocked by risk manager: {risk_reason}")
        else:
            signal = slot.engine.evaluate_maker(
                spot=spot,
                strike=strike,
                remaining_s=remaining_s,
                elapsed_s=elapsed_s,
                realized_twap=realized_twap,
                best_bid=best_bid,
                best_ask=best_ask,
                chop_crosses=chop,
                fee_rate=market.get("fee_rate", config.TAKER_FEE_RATE),
                realized_vol_daily=params.daily_vol,
                min_sigma_usd=min_sigma,
            )

            if in_eval_window:
                logger.info(
                    f"[{remaining_s:3.0f}s left] {tag}: ${px(spot)} | S0: ${px(strike)} | "
                    f"Z: {signal.z_score:4.2f} | P: {signal.model_prob:.1%} | "
                    f"TopBid: {best_bid or 0:.2f} | TargetBid({signal.side}): {signal.target_bid:.2f} | "
                    f"Edge: {signal.edge_net:+.1%} | Pass: {signal.passed_gates}"
                )

                if signal.passed_gates and not slot.resting_order and not slot.position:
                    if self._slot_entries_count() >= config.MAX_CONCURRENT_ENTRIES:
                        logger.warning(
                            f"[{tag}] Maker trade skipped: slot entry cap ({config.MAX_CONCURRENT_ENTRIES}) reached"
                        )
                    else:
                        can_trade, risk_reason = self.risk.can_trade(open_notional=open_notional)
                        if can_trade:
                            await self._place_maker_bid(slot, market, signal)
                        else:
                            logger.warning(f"[{tag}] Maker trade blocked by risk manager: {risk_reason}")

            if slot.resting_order and not slot.position:
                ro = slot.resting_order
                cur_bid = best_bid or 0.0
                cur_ask = best_ask or 1.0
                elapsed_resting = time.time() - ro.get("placed_at", time.time())

                # Paper must not invent a fill: only count it if the book would lift the bid.
                is_clob_fill = (cur_ask <= ro["price"])
                is_paper_fill = (
                    config.MODE != "live"
                    and elapsed_resting >= config.MAKER_DWELL_SECONDS
                    and signal.z_score >= params.min_z
                    and is_clob_fill
                )

                if is_paper_fill or (config.MODE == "live" and is_clob_fill):
                    slot.position = {
                        "asset": asset,
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
                    logger.info(f"✅ [{tag} H-004 MAKER BID FILLED] BUY {ro['side']} @ {ro['price']:.2f}")
                    logger.info(f"   Filled: {ro['shares']:.1f} shares (${ro['cost_usd']:.2f}) | Rebate: ${ro['rebate']:.3f}")
                    logger.info("=" * 60)
                    slot.resting_order = None

                elif signal.z_score < 0.80 or remaining_s < config.EVAL_WINDOW_END_S:
                    reason = "spot reversal (Z < 0.80)" if signal.z_score < 0.80 else "end of quoting window"
                    logger.info(f"🚫 [{tag} H-004 MAKER BID CANCELLED] Cancelled resting bid @ {ro['price']:.2f} ({reason})")
                    if config.MODE == "live" and self.live_client:
                        try:
                            self.live_client.cancel_order(ro["order_id"])
                        except Exception as e:
                            logger.error(f"Error cancelling live order: {e}")
                    slot.resting_order = None

        if slot.position and slot.position.get("strategy") in (
            "H-005_MOMENTUM", "LATE_MOMENTUM", "MOMENTUM", "H-005"
        ):
            await self._manage_open_momentum_position(slot, market, remaining_s, tokens)

        if remaining_s <= 0.0:
            if slot.resting_order:
                slot.resting_order = None
            if slot.position:
                self._queue_settlement(slot)

    async def _place_taker_order(
        self, slot: CoinSlot, market: dict, signal: TWAPSignal, ask: float, strategy_tag: str = "H-001_SNIPER"
    ) -> None:
        """Execute immediate taker sniper order against available ask."""
        token_id = market["tokens"].get(signal.side)
        if not token_id or not ask:
            return

        stake_usd = self.risk.calculate_stake(signal.edge_net, ask, open_notional=self._open_notional())
        if stake_usd < config.BASE_STAKE_USD:
            logger.warning(f"[{slot.asset.upper()}] Stake ${stake_usd:.2f} below base; skip")
            return
        shares = max(config.MIN_ORDER_SHARES, round(stake_usd / ask, 2))
        cost_usd = round(shares * ask, 2)
        fee_usd = round(signal.fee_taker * shares, 4)
        is_paper = config.MODE != "live"

        if is_paper and config.PAPER_FOK_REQUIRE_SIZE:
            book = self.pub_client.get_order_book(token_id)
            fillable, vwap = fillable_ask_shares(book, ask)
            if fillable + 1e-9 < shares:
                top = self.book_stream.get_top(token_id) or {}
                ws_sz = float(top.get("ask_size") or 0.0)
                ws_ask = top.get("ask")
                if ws_sz + 1e-9 >= shares and ws_ask is not None and float(ws_ask) <= float(ask) + 1e-9:
                    fillable = ws_sz
                    vwap = float(ws_ask)
            if fillable + 1e-9 < shares:
                logger.warning(
                    f"[{slot.asset.upper()}] Paper FOK unfilled: need {shares:.2f} sh @ {ask:.3f} "
                    f"but book only has {fillable:.2f} sh"
                )
                journal_write(
                    "skip_round",
                    reason="paper_fok_unfilled",
                    asset=slot.asset,
                    side=signal.side,
                    ask=ask,
                    shares=shares,
                    fillable=round(fillable, 4),
                    slot=market.get("slug"),
                )
                return
            if vwap > 0:
                ask = min(float(ask), float(vwap))
                cost_usd = round(shares * ask, 2)
                fee_usd = round(signal.fee_taker * shares, 4)

        logger.info("=" * 60)
        logger.info(f"[{slot.asset.upper()} {strategy_tag} ORDER EXECUTED] BUY {signal.side} @ {ask:.3f}")
        logger.info(f"   Z-Score: {signal.z_score:.2f} | Win Prob: {signal.model_prob:.1%} | Net Edge: {signal.edge_net:+.1%}")
        logger.info(f"   Size: {shares:.1f} shares (${cost_usd:.2f}) | Fee: ${fee_usd:.3f}")
        logger.info("=" * 60)

        order_id = f"{strategy_tag.lower()}_paper_{int(time.time()*1000)}"
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
                        asset=slot.asset,
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
            asset=slot.asset,
            order_id=order_id,
            z_score=signal.z_score,
            edge=signal.edge_net,
        )

        slot.position = {
            "asset": slot.asset,
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

    async def _place_maker_bid(self, slot: CoinSlot, market: dict, signal: MakerSignal) -> None:
        """Post resting limit bid inside spread as Maker."""
        token_id = market["tokens"].get(signal.side)
        if not token_id or not signal.target_bid:
            return

        stake_usd = self.risk.calculate_stake(signal.edge_net, signal.target_bid, open_notional=self._open_notional())
        if stake_usd < config.BASE_STAKE_USD:
            logger.warning(f"[{slot.asset.upper()}] Stake ${stake_usd:.2f} below base; skip maker")
            return
        shares = max(config.MIN_ORDER_SHARES, round(stake_usd / signal.target_bid, 2))
        cost_usd = round(shares * signal.target_bid, 2)
        total_rebate = round(signal.maker_rebate * shares, 4)

        logger.info("=" * 60)
        logger.info(f"📥 [{slot.asset.upper()} H-004 MAKER BID POSTED] RESTING BUY {signal.side} @ {signal.target_bid:.2f}")
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

        slot.resting_order = {
            "asset": slot.asset,
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


    async def _manage_open_momentum_position(
        self, slot: CoinSlot, market: dict, remaining_s: float, tokens: dict
    ) -> None:
        """Stop-loss, exit-before-close, and optional micro-hedge for H-005 positions."""
        pos = slot.position
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
            await self._exit_position_early(slot, market, mark, reason="stop_loss")
            return

        # 2) Exit before final seconds (avoid last-second oracle chop)
        if remaining_s <= float(config.MOMENTUM_EXIT_BEFORE_S) and remaining_s > 0.5:
            exit_px = mark if mark is not None else entry
            logger.info(
                f"[{slot.asset.upper()} H-005 EXIT-BEFORE] {remaining_s:.1f}s left <= {config.MOMENTUM_EXIT_BEFORE_S:.0f}s; "
                f"flatten @ {exit_px:.3f}"
            )
            await self._exit_position_early(slot, market, exit_px, reason="exit_before")
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
                await self._place_micro_hedge(slot, market, tokens, pos, main_ask)

    async def _place_micro_hedge(
        self, slot: CoinSlot, market: dict, tokens: dict, pos: dict, main_ask: float
    ) -> None:
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
            f"[{slot.asset.upper()} H-005 MICRO-HEDGE] BUY {hedge_side} @ {hedge_ask:.3f} | "
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

    async def _exit_position_early(self, slot: CoinSlot, market: dict, exit_price: float, reason: str) -> None:
        """Flatten main (+ optional hedge) before TWAP settlement; paper marks at exit_price."""
        pos = slot.position
        if not pos:
            return

        shares = float(pos["shares"])
        cost = float(pos["cost_usd"])
        fee = float(pos.get("fee", 0.0))
        rebate = float(pos.get("rebate", 0.0))
        exit_price = float(exit_price)

        # Paper / live flatten of main
        if config.MODE == "live" and self.live_client:
            try:
                resp = self.live_client.place_order(
                    pos["token_id"], "SELL", max(0.01, exit_price), shares, order_type="FOK"
                )
                if not resp or not resp.get("filled"):
                    logger.error(f"Live early exit SELL not filled ({reason}), retaining position for retry")
                    return  # keep slot.position alive so bot retries next tick
            except Exception as e:
                logger.error(f"Live early exit sell failed ({reason}): {e} — retaining position for retry")
                return  # keep slot.position alive so bot retries next tick

        # Only clear position AFTER confirming exit (paper always succeeds, live confirmed above)
        slot.position = None
        slot_key = market.get("slug") or pos.get("slot")
        if slot_key:
            self._slot_done.add(slot_key)
            self._save_slot_state(
                slot_ts=market.get("slot_start_ts"),
                slug=slot_key,
                strike=None,
                asset=slot.asset,
            )


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
        logger.info(f"[{slot.asset.upper()} H-005 EARLY EXIT:{reason}] {market['slug']}")
        logger.info(
            f"   Main {pos['side']} entry={pos['price']:.3f} exit={exit_price:.3f} | "
            f"PnL ${pnl:+.2f} | Bankroll ${self.risk.bankroll:.2f}"
        )
        logger.info("=" * 60)

        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "asset": slot.asset,
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
            "official": True,
            "settlement_source": f"early_exit_{reason}",
        }
        journal_write(
            "settlement",
            slot=record.get("slot"),
            asset=slot.asset,
            strategy=record.get("strategy"),
            side=record.get("side"),
            won=record.get("won"),
            pnl=record.get("pnl"),
            bankroll=record.get("bankroll"),
            early_exit=True,
        )
        self._save_trade_record(record)

    def _queue_settlement(self, slot: CoinSlot) -> None:
        """Hold a fill until Polymarket posts the official Up/Down result."""
        if not slot.position or not slot.market:
            slot.position = None
            return
        item = {
            "asset": slot.asset,
            "market": slot.market,
            "position": slot.position,
            "strike": slot.strike,
            "queued_at": time.time(),
            "end_ts": slot.market.get("end_ts"),
        }
        self._pending_settles.append(item)
        logger.info(
            f"[{slot.asset.upper()}] Queued for Polymarket resolution: {slot.market.get('slug')} "
            f"(do not book PnL yet)"
        )
        slot.position = None

    async def _flush_pending_settlements(self, now: float) -> None:
        if not self._pending_settles:
            return
        still: list[dict] = []
        for item in self._pending_settles:
            slug = (item.get("market") or {}).get("slug")
            end_ts = float(item.get("end_ts") or 0.0)
            if end_ts and now < end_ts + 2.0:
                still.append(item)
                continue
            resolution = self.pub_client.get_resolution(slug, now_ts=now) if slug else None
            waited = now - float(item.get("queued_at") or now)
            if resolution is None and waited < config.RESOLUTION_TIMEOUT_S:
                still.append(item)
                continue
            if resolution is None:
                logger.warning(
                    f"[{str(item.get('asset') or '').upper()}] No Polymarket resolution after "
                    f"{waited:.0f}s for {slug}; recording unresolved (PnL not booked)"
                )
                self._record_unresolved(item)
                continue
            self._settle_filled(item, resolution)
        self._pending_settles = still

    def _record_unresolved(self, item: dict) -> None:
        pos = item.get("position") or {}
        market = item.get("market") or {}
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "asset": item.get("asset"),
            "slot": market.get("slug"),
            "strategy": pos.get("strategy", "H-001_SNIPER"),
            "side": pos.get("side"),
            "price": pos.get("price"),
            "shares": pos.get("shares"),
            "cost_usd": pos.get("cost_usd"),
            "z_score": pos.get("z_score"),
            "strike": item.get("strike"),
            "outcome": None,
            "won": None,
            "pnl": 0.0,
            "bankroll": round(self.risk.bankroll, 2),
            "settlement_source": "unresolved",
            "early_exit": False,
        }
        journal_write(
            "settlement",
            slot=market.get("slug"),
            asset=item.get("asset"),
            won=None,
            pnl=0.0,
            settlement_source="unresolved",
        )
        self._save_trade_record(record)

    def _settle_filled(self, item: dict, resolution: dict) -> None:
        """Settle a paper/live fill against Polymarket's official outcome only."""
        pos = item.get("position") or {}
        market = item.get("market") or {}
        asset = str(item.get("asset") or "")
        resolved_side = resolution["side"]
        won = pos.get("side") == resolved_side
        strategy_name = pos.get("strategy", "H-001_SNIPER")
        fee = pos.get("fee", 0.0)
        rebate = pos.get("rebate", 0.0)

        if won:
            revenue = float(pos.get("shares") or 0.0) * 1.0
            pnl = (revenue - float(pos.get("cost_usd") or 0.0)) - fee + rebate
        else:
            pnl = -float(pos.get("cost_usd") or 0.0) - fee + rebate

        if pos.get("hedge_placed"):
            h_shares = float(pos.get("hedge_shares", 0.0))
            h_cost = float(pos.get("hedge_cost_usd", 0.0))
            if not won:
                pnl += (h_shares * 1.0) - h_cost
            else:
                pnl += -h_cost

        self.risk.record_trade_result(pnl)
        strike = resolution.get("price_to_beat") if resolution.get("price_to_beat") is not None else item.get("strike")
        final_twap = resolution.get("final_price")

        logger.info("=" * 60)
        logger.info(f"🏁 [{asset.upper()} SLOT SETTLEMENT] {market.get('slug')} ({strategy_name})")
        logger.info(
            f"   Source: {resolution.get('source')} | official={resolution.get('official')} "
            f"| S0: {strike} | Final: {final_twap}"
        )
        logger.info(f"   Outcome: {resolved_side} | Trade: {pos.get('side')} -> {'WON' if won else 'LOST'}")
        logger.info(f"   Trade PnL: ${pnl:+.2f} | New Bankroll: ${self.risk.bankroll:.2f}")
        logger.info("=" * 60)

        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "asset": asset,
            "slot": market.get("slug"),
            "strategy": strategy_name,
            "side": pos.get("side"),
            "price": pos.get("price"),
            "shares": pos.get("shares"),
            "cost_usd": pos.get("cost_usd"),
            "fee": fee,
            "rebate": rebate,
            "z_score": pos.get("z_score"),
            "strike": strike,
            "final_twap": final_twap,
            "outcome": resolved_side,
            "won": won,
            "pnl": round(pnl, 2),
            "bankroll": round(self.risk.bankroll, 2),
            "settlement_source": resolution.get("source"),
            "official": bool(resolution.get("official")),
            "early_exit": False,
        }
        journal_write(
            "settlement",
            slot=market.get("slug"),
            asset=asset,
            strategy=strategy_name,
            side=pos.get("side"),
            outcome=resolved_side,
            won=won,
            pnl=round(pnl, 2),
            bankroll=round(self.risk.bankroll, 2),
            settlement_source=resolution.get("source"),
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
