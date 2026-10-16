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
import os
import shutil
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import config
from twap_engine import TWAPEngine, MakerSignal, TWAPSignal
from feeds import ChainlinkRTDSStream, PolymarketBookStream
from polymarket_client import (
    PolymarketPublicClient,
    PolymarketLiveClient,
    fillable_ask_shares,
    fillable_bid_shares,
    extract_best_bid_ask,
    extract_market_top,
)
from risk_manager import RiskManager, cap_order_shares
from journal import configure as journal_configure
from journal import write as journal_write
from http_api import StatusServer, is_official_trade, load_trades
from jev_advisor import JevAdvisor, JevDecision
import move_ref
import maker_guard
import shadow_log

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


def _extract_slot_start_ts(market: dict | None = None, slot_str: str | None = None) -> float | None:
    """Extract 5-minute round start Unix timestamp from market or slug."""
    if isinstance(market, dict):
        st = market.get("slot_start_ts") or market.get("start_time")
        if st is not None:
            try:
                return float(st)
            except Exception:
                pass
        slug = market.get("slug")
        if slug and isinstance(slug, str) and "-" in slug:
            try:
                return float(slug.rsplit("-", 1)[-1])
            except Exception:
                pass
    if slot_str and isinstance(slot_str, str) and "-" in slot_str:
        try:
            return float(slot_str.rsplit("-", 1)[-1])
        except Exception:
            pass
    return None


def _format_window_time(elapsed_s: float | None) -> str | None:
    """Format elapsed seconds within the 5m round into human-readable window timing."""
    if elapsed_s is None:
        return None
    el = max(0.0, min(300.0, float(elapsed_s)))
    m = int(el) // 60
    s = int(el) % 60
    return f"t={int(el)}s ({m:02d}:{s:02d})"



def _in_utc_windows(spec: str, ts: float) -> bool:
    """True if UTC time-of-day of ts falls in any 'HH:MM-HH:MM' window in spec (windows may wrap midnight)."""
    if not spec:
        return False
    g = time.gmtime(ts)
    m = g.tm_hour * 60 + g.tm_min
    for part in spec.split(","):
        part = part.strip()
        if "-" not in part:
            continue
        try:
            a, b = (int(x.split(":")[0]) * 60 + int(x.split(":")[1]) for x in part.split("-", 1))
        except (ValueError, IndexError):
            continue
        if (a <= m < b) if a < b else (m >= a or m < b):
            return True
    return False


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
    # Conservative momentum move reference (per slot, cached once resolved)
    strike_twap: float | None = None
    strike_twap_source: str | None = None
    strike_twap_resolved: bool = False
    move_ref_used: str | None = None
    move_ref_block_logged: bool = False
    # Maker guard / momentum elapsed-gate logging (once per slot per reason)
    maker_block_logged: set = field(default_factory=set)
    momentum_gate_logged: bool = False
    entry_zone_logged: set = field(default_factory=set)


class TWAPBot:
    """Trading Bot — default MIDBAND_MAKER; H-001 sniper retired."""

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
        self.jev = JevAdvisor()
        self._mid_hist = maker_guard.MidHistory(float(getattr(config, "MAKER_MID_WINDOW_S", 60.0)))
        self._rest_throttle = shadow_log.Throttle(1.0)
        self._post_fill = shadow_log.PostFillTracer()
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
        self._pending_path = config.STATE_DIR / "pending_settlements.json"
        self._pending_settles = self._load_pending()
        self._last_bankroll_refresh = 0.0
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
        if slot_ts is not None:
            entry["slot_ts"] = int(slot_ts)
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

    def _check_reset_trade_data(self) -> None:
        """Archive and reset old trades, journal, slot states, or prune historical trades prior to cutoff."""
        prune_before = os.getenv("PRUNE_TRADES_BEFORE", "").strip()
        if prune_before and self._trades_log_path.exists():
            logger.info("=" * 70)
            logger.info(f"✂️ PRUNE_TRADES_BEFORE={prune_before}: pruning trades prior to cutoff...")
            ts = int(time.time())
            archive_path = config.STATE_DIR / f"trades_pre_prune_{ts}.json"
            try:
                shutil.copy2(self._trades_log_path, archive_path)
                logger.info(f"   Archived original trades.json -> {archive_path.name}")
            except Exception as e:
                logger.error(f"   Failed to archive trades: {e}")

            trades = load_trades(self._trades_log_path)
            kept_trades = []
            for t in trades:
                t_time = str(t.get("entry_time_utc") or t.get("timestamp") or "")
                if t_time >= prune_before:
                    kept_trades.append(t)

            logger.info(f"   Pruned {len(trades) - len(kept_trades)} old trades; kept {len(kept_trades)} post-fix trades.")
            try:
                with open(self._trades_log_path, "w", encoding="utf-8") as f:
                    json.dump(kept_trades, f, indent=2)
                logger.info(f"   Saved {len(kept_trades)} trades to trades.json")
            except Exception as e:
                logger.error(f"   Failed to save pruned trades: {e}")
            logger.info("=" * 70)

        reset_marker = config.STATE_DIR / ".clean_reset_done_v2"
        env_reset = os.getenv("RESET_TRADE_DATA", "false").strip().lower() in ("true", "1", "yes")
        should_reset = env_reset or not reset_marker.exists()
        if not should_reset:
            return

        logger.info("=" * 70)
        logger.info("🧹 Clean reset: archiving and resetting historical trades...")
        ts = int(time.time())

        # 1. Archive & reset trades.json
        if self._trades_log_path.exists():
            archive_path = config.STATE_DIR / f"trades_archive_{ts}.json"
            try:
                shutil.copy2(self._trades_log_path, archive_path)
                logger.info(f"   Archived trades.json -> {archive_path.name}")
            except Exception as e:
                logger.error(f"   Failed to archive trades.json: {e}")
            try:
                with open(self._trades_log_path, "w", encoding="utf-8") as f:
                    json.dump([], f)
                logger.info("   Reset trades.json to []")
            except Exception as e:
                logger.error(f"   Failed to reset trades.json: {e}")

        # 2. Archive & reset journal.jsonl
        journal_p = Path(config.JOURNAL_PATH)
        if journal_p.exists():
            j_archive = config.STATE_DIR / f"journal_archive_{ts}.jsonl"
            try:
                shutil.copy2(journal_p, j_archive)
                logger.info(f"   Archived journal.jsonl -> {j_archive.name}")
                with open(journal_p, "w", encoding="utf-8") as f:
                    pass
                logger.info("   Truncated journal.jsonl")
            except Exception as e:
                logger.error(f"   Failed to archive/reset journal: {e}")

        # 3. Reset slot state
        slot_p = self._slot_state_path()
        if slot_p.exists():
            try:
                slot_p.unlink()
                logger.info("   Cleared slot_state.json")
            except Exception as e:
                logger.error(f"   Failed to delete slot_state.json: {e}")
        self._slot_done.clear()

        # 4. Reset risk manager state
        self.risk.bankroll = config.STARTING_BANKROLL
        self.risk.daily_pnl = 0.0
        self.risk.consecutive_losses = 0
        self.risk.trades_today = 0
        self.risk.save_state()
        logger.info(f"   Risk state reset to STARTING_BANKROLL (${config.STARTING_BANKROLL:.2f})")
        try:
            reset_marker.touch()
        except Exception:
            pass
        logger.info("=" * 70)

    def _reconcile_paper_blotter(self) -> None:
        """Paper bankroll follows official Polymarket settlements only."""
        if config.MODE == "live":
            return
        trades = load_trades(self._trades_log_path)
        official = [t for t in trades if is_official_trade(t)]
        ghost = len(trades) - len(official)
        pnl = sum(float(t.get("pnl") or 0.0) for t in official if t.get("won") is not None)
        self.risk.bankroll = round(config.STARTING_BANKROLL + pnl, 2)
        # daily_pnl = today's (UTC risk day) booked PnL only, not the all-time total
        self.risk._check_day_rollover()
        self.risk.daily_pnl = round(
            sum(
                float(t.get("pnl") or 0.0) for t in official
                if t.get("won") is not None and _trade_day(t) == int(self.risk.day_timestamp)
            ),
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
        """Count coins with an open position OR a resting maker bid (both can fill on the same move)."""
        count = 0
        for slot in self.slots.values():
            if slot.position or getattr(slot, "resting_order", None):
                count += 1
        return count

    def _restored_strike(self, saved: dict, asset: str, slug: str | None, slot_ts: int) -> float | None:
        assets_map = saved.get("assets") or {}
        entry = assets_map.get(asset) or {}
        if not entry and asset == "btc":
            if saved.get("slug") == slug or int(saved.get("slot_ts") or 0) == int(slot_ts):
                entry = {"strike": saved.get("strike"), "slug": saved.get("slug"), "slot_ts": saved.get("slot_ts")}

        # Critical guard: Stored strike MUST belong to the exact same slot slug and slot_ts!
        entry_slug = entry.get("slug")
        entry_ts = entry.get("slot_ts") or saved.get("slot_ts")
        if slug and entry_slug and entry_slug != slug:
            logger.warning(
                f"[{asset.upper()}] Stored strike slug '{entry_slug}' != current slug '{slug}'; rejecting stale strike."
            )
            return None
        if slot_ts and entry_ts and int(entry_ts) != int(slot_ts):
            logger.warning(
                f"[{asset.upper()}] Stored strike slot_ts {entry_ts} != current slot_ts {slot_ts}; rejecting stale strike."
            )
            return None

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
            "jev": self.jev.stats(),
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
                f"  {a.upper()}: min_z={p.min_z} midband_z={p.midband_min_z} edge>={p.min_edge:.1%} "
                f"ask=[{p.min_buy_price:.2f}..{p.max_buy_price:.2f}] "
                f"vol={p.daily_vol:.2%} sigma={p.min_sigma_bps:.2f}bps chop={p.chop_noise_bps:.2f}bps"
            )
        if config.STRATEGY in ("JEV_DYNAMIC", "DYNAMIC", "HYBRID", "ADAPTIVE", "AVN_MAS", "AVN-MAS", "AVN_MOMENTUM", "AVN"):
            logger.info(
                f"JEV_DYNAMIC Hybrid: band=[{config.DYNAMIC_MIDBAND_LO:.2f}..{config.DYNAMIC_MIDBAND_HI:.2f}] "
                f"min_z={config.DYNAMIC_MIN_Z:.2f} time_left=[{config.DYNAMIC_WINDOW_END_S:.0f}..{config.DYNAMIC_WINDOW_START_S:.0f}]s "
                f"concurrency_cap={config.MAX_CONCURRENT_ENTRIES} base_stake=${config.BASE_STAKE_USD:.2f}"
            )
            if self.jev.shadow_active:
                logger.info("JEV_SHADOW=on: Jev is consulted and logged but NOT enforced (all consults proceed at 1.0x)")
            elif getattr(config, "JEV_SHADOW", False):
                logger.info("JEV_SHADOW set but Jev is not active (disabled / no key / no SDK); shadow has no effect")
            logger.info(
                "MAKER_HOLD_TO_EXPIRY="
                + ("on: maker positions hold to settlement (TP 0.95 only; no trailing/price stop/stop-loss/hard flatten/failsafe)"
                   if getattr(config, "MAKER_HOLD_TO_EXPIRY", False) else "off (maker uses the normal exit stack)")
            )
        elif config.STRATEGY in ("MIDBAND_MAKER", "MIDBAND", "H-006_MIDBAND_MAKER", "H-006"):
            logger.info(
                f"Mid-band maker: band=[{config.MIDBAND_LO:.2f}..{config.MIDBAND_HI:.2f}] "
                f"half_spread={config.MIDBAND_HALF_SPREAD:.2f} "
                f"time_left=[{config.MIDBAND_MIN_SECONDS_LEFT:.0f}..{config.MIDBAND_MAX_SECONDS_LEFT:.0f}]s"
            )
        elif config.STRATEGY in ("H-005_MOMENTUM", "MOMENTUM", "H-005", "LATE_MOMENTUM"):
            logger.info(
                f"Momentum: move>=${config.MOMENTUM_MIN_MOVE_USD:.0f} | ask=[{config.MOMENTUM_MIN_ASK:.2f}..{config.MOMENTUM_MAX_ASK:.2f}] | "
                f"window=[{config.MOMENTUM_WINDOW_END_S:.0f}s..{config.MOMENTUM_WINDOW_START_S:.0f}s left]"
            )
            logger.info(
                f"Overlays: SL={'on' if config.MOMENTUM_STOP_LOSS_ENABLED else 'off'} "
                f"exit_before={config.MOMENTUM_EXIT_BEFORE_S:g}s "
                f"hedge={'on' if config.MOMENTUM_HEDGE_ENABLED else 'off'}"
            )
        elif False and config.STRATEGY in ("H-001_SNIPER", "SNIPER", "H-001"):  # retired
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
        self._check_reset_trade_data()
        journal_configure(config.JOURNAL_PATH)
        logger.info(f"Journal: {config.JOURNAL_PATH}")
        logger.info(f"Bankroll: ${self.risk.bankroll:.2f} | Base Stake: ${config.BASE_STAKE_USD:.2f}")
        if config.MODE != "live":
            logger.info("Daily-loss / consecutive-loss kill switches: DISABLED (paper mode keeps filling)")
        else:
            logger.info(f"Daily loss limit: ${config.DAILY_LOSS_LIMIT_USD:.2f} | Max consecutive losses: {config.MAX_CONSECUTIVE_LOSSES}")
        if config.BACKFILL_UNRESOLVED_ON_START:
            try:
                self._repair_unresolved_records()
            except Exception as e:
                logger.error(f"Unresolved backfill failed: {e}")
        self._reconcile_paper_blotter()
        self._refresh_live_bankroll(time.time(), force=True)
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
                            self._live_finalize_resting(slot, "window_rollover")
                            self._queue_settlement(slot)
                            slot.resting_order = None
                    active_slot_ts = current_window
                    await self._discover_slot(current_window, now)

                await self._flush_pending_settlements(now)
                self._refresh_live_bankroll(now)

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
            slot.strike_twap = None
            slot.strike_twap_source = None
            slot.strike_twap_resolved = False
            slot.move_ref_used = None
            slot.move_ref_block_logged = False
            slot.maker_block_logged = set()
            slot.momentum_gate_logged = False
            slot.entry_zone_logged = set()
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

        tokens = market.get("tokens") or {}

        # ---------------------------------------------------------------------
        # 1. PRIORITY POSITION MANAGEMENT: NEVER SKIP OPEN POSITIONS!
        # Even during transient oracle staleness or WebSocket reconnections,
        # open positions with real capital at risk MUST be managed every second.
        # ---------------------------------------------------------------------
        if slot.position:
            self._log_post_fill_tick(slot, now)
            spot_pos = self.cl_stream.price(asset)
            strike_pos = slot.strike or (float(slot.position["strike"]) if slot.position.get("strike") else None)
            twap_pos = None
            if slot_start_ts:
                twap_pos, _ = self.cl_stream.compute_realized_twap(start_ts=slot_start_ts, end_ts=now, asset=asset)

            await self._manage_open_momentum_position(
                slot, market, remaining_s, tokens,
                spot=spot_pos, strike=strike_pos,
                realized_twap=twap_pos, elapsed_s=elapsed_s,
                now=now,
            )

            if remaining_s <= 0.0:
                if slot.resting_order:
                    self._live_finalize_resting(slot, "expiry")
                    slot.resting_order = None
                if slot.position:
                    self._queue_settlement(slot)
                return

            # If position remains open, do not evaluate new entries on this coin
            if slot.position:
                return

        # ---------------------------------------------------------------------
        # 2. NEW ENTRY GATES: REQUIRE ACTIVE & FRESH ORACLE TICKS
        # ---------------------------------------------------------------------
        if not self.cl_stream.ready_for(asset):
            return
        if self.cl_stream.is_stale(max_age_s=config.MAX_ORACLE_STALE_S, asset=asset):
            logger.warning(f"[FEED] {asset.upper()} Chainlink waiting for live tick...")
            return

        if slot.strike is None or slot.strike <= 0:
            if slot.position and slot.position.get("strike"):
                slot.strike = float(slot.position["strike"])
            else:
                slot.strike = self.cl_stream.get_slot_strike(slot_start_ts, max_dist_s=5.0, asset=asset)
                if slot.strike is None or slot.strike <= 0:
                    if elapsed_s <= 10.0:
                        slot.strike = self.cl_stream.price(asset)
                    else:
                        logger.warning(
                            f"[{asset.upper()}] No strike tick within 5s of slot start (elapsed {elapsed_s:.1f}s); "
                            f"skipping ungrounded slot to avoid strike drift."
                        )
                        return
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

        is_dynamic = (config.STRATEGY in ("JEV_DYNAMIC", "DYNAMIC", "HYBRID", "ADAPTIVE", "AVN_MAS", "AVN-MAS", "AVN_MOMENTUM", "AVN"))
        is_momentum = (config.STRATEGY in ("H-005_MOMENTUM", "MOMENTUM", "H-005", "LATE_MOMENTUM"))
        is_midband = (config.STRATEGY in ("MIDBAND_MAKER", "MIDBAND", "H-006_MIDBAND_MAKER", "H-006"))
        is_sniper = False  # H-001 retired

        if is_dynamic:
            in_eval_window = (
                config.DYNAMIC_WINDOW_END_S <= remaining_s <= config.DYNAMIC_WINDOW_START_S
            )
        elif is_momentum:
            in_eval_window = (
                config.MOMENTUM_WINDOW_END_S <= remaining_s <= config.MOMENTUM_WINDOW_START_S
            )
        else:
            in_eval_window = (
                config.EVAL_WINDOW_END_S <= remaining_s <= config.EVAL_WINDOW_START_S
            )

        tokens = market["tokens"]
        prelim_side = "UP" if spot >= strike else "DOWN"
        prelim_opp = "DOWN" if prelim_side == "UP" else "UP"
        token_id = tokens.get(prelim_side)
        opp_token_id = tokens.get(prelim_opp)

        top = self.book_stream.get_top(token_id, opp_token_id=opp_token_id) if token_id else None
        best_bid = top.get("bid") if top else None
        best_ask = top.get("ask") if top else None

        if (best_bid is None or best_ask is None) and token_id:
            bk_fav = self.pub_client.get_order_book(token_id)
            bk_opp = self.pub_client.get_order_book(opp_token_id) if opp_token_id else None
            r_bid, r_ask, _ = extract_market_top(bk_fav, bk_opp)
            if best_bid is None:
                best_bid = r_bid
            if best_ask is None:
                best_ask = r_ask

        open_notional = self._open_notional()
        tag = asset.upper()
        px = params.fmt_px

        if is_dynamic:
            slot_key = market.get("slug", "")
            can_trade_now, risk_reason_now = self.risk.can_trade(open_notional=open_notional)
            if not can_trade_now and not slot.position and not slot.resting_order:
                if not slot.risk_block_logged:
                    logger.warning(f"[{tag}] Dynamic trading paused by risk manager: {risk_reason_now}")
                    journal_write("risk_block", reason=risk_reason_now, mode=config.MODE, asset=asset)
                    slot.risk_block_logged = True
                return
            slot.risk_block_logged = False

            sigma_twap_dyn = slot.engine.estimate_sigma_twap_usd(
                spot=spot, remaining_s=remaining_s, realized_vol_daily=params.daily_vol
            )
            _, signed_z_dyn, _ = slot.engine.compute_s_req_and_z(
                spot=spot, strike=strike, realized_twap=realized_twap,
                elapsed_s=elapsed_s, remaining_s=remaining_s, sigma_twap_usd=sigma_twap_dyn
            )
            favored_side = "UP" if signed_z_dyn >= 0 else "DOWN"
            fav_opp_side = "DOWN" if favored_side == "UP" else "UP"
            fav_token = tokens.get(favored_side)
            opp_token = tokens.get(fav_opp_side)

            q_bid = None
            q_ask = None
            if fav_token:
                q_top = self.book_stream.get_top(fav_token, opp_token_id=opp_token)
                q_bid = q_top.get("bid") if q_top else None
                q_ask = q_top.get("ask") if q_top else None
                if q_bid is None or q_ask is None:
                    bk_fav = self.pub_client.get_order_book(fav_token)
                    bk_opp = self.pub_client.get_order_book(opp_token) if opp_token else None
                    r_bid, r_ask, _ = extract_market_top(bk_fav, bk_opp)
                    if q_bid is None:
                        q_bid = r_bid
                    if q_ask is None:
                        q_ask = r_ask

            mid_px = 0.5 * (float(q_bid) + float(q_ask)) if (q_bid is not None and q_ask is not None) else None
            spread = round(float(q_ask) - float(q_bid), 4) if (q_bid is not None and q_ask is not None) else 1.0
            move_usd = spot - strike
            abs_z = abs(signed_z_dyn)

            # Momentum entry move reference (spot strike vs settlement-TWAP strike).
            move_ref_mode = move_ref.normalize_mode(getattr(config, "MOMENTUM_MOVE_REF", "conservative"))
            maker_guard_on = bool(getattr(config, "MAKER_TWAP_GUARD", False))
            maker_require_twap = maker_guard_on and bool(getattr(config, "MAKER_REQUIRE_TWAP", True))
            if move_ref_mode == move_ref.MODE_CONSERVATIVE or maker_guard_on:
                self._ensure_twap_strike(slot, market, slot_start_ts, elapsed_s)
            gate_move_usd, move_ref_used = move_ref.entry_move(move_ref_mode, spot, strike, slot.strike_twap)
            slot.move_ref_used = move_ref_used

            # Maker guard inputs: mid history per token and z against the TWAP (settlement) strike.
            if mid_px is not None:
                self._mid_hist.add(fav_token, now, mid_px)
                self._mid_hist.add(opp_token, now, 1.0 - mid_px)
            z_twap_dyn = None
            if maker_guard_on and slot.strike_twap:
                try:
                    _, z_twap_dyn, _ = slot.engine.compute_s_req_and_z(
                        spot=spot, strike=slot.strike_twap, realized_twap=realized_twap,
                        elapsed_s=elapsed_s, remaining_s=remaining_s, sigma_twap_usd=sigma_twap_dyn
                    )
                except Exception:
                    z_twap_dyn = None

            allowed_feas, min_move_usd, max_allowed_ask, sigma_t = config.compute_dynamic_feasibility(
                elapsed_s=elapsed_s, spot=spot, daily_vol=params.daily_vol
            )
            effective_max_ask = min(max_allowed_ask, getattr(config, "DYNAMIC_MOMENTUM_MAX_ASK", 0.90))

            directional_coherence = (
                (spot > strike and realized_twap > strike and favored_side == "UP")
                or (spot < strike and realized_twap < strike and favored_side == "DOWN")
            )

            # Seek new entry across both Momentum and Maker plays
            if in_eval_window and not slot.position and not slot.resting_order:
                if slot_key and slot_key in self._slot_done:
                    pass
                elif self._slot_entries_count() >= config.MAX_CONCURRENT_ENTRIES:
                    pass
                elif (
                    q_bid is not None
                    and q_ask is not None
                    and 0.0 < float(q_bid) < float(q_ask) < 1.0
                    and spread <= config.DYNAMIC_MAX_BOOK_SPREAD
                ):
                    momentum_min_elapsed = float(
                        getattr(config, "DYNAMIC_MOMENTUM_MIN_ELAPSED_S", 90.0)
                    )
                    if move_ref_mode == move_ref.MODE_SPOT:
                        move_gate_ok = abs(move_usd) >= min_move_usd
                    else:
                        move_gate_ok = move_ref.gate_move_ok(gate_move_usd, min_move_usd, favored_side)
                    momentum_zone_blocked = self._entry_zone_blocked(slot, slot_key, "momentum", elapsed_s)
                    is_momentum_candidate = (
                        not momentum_zone_blocked
                        and allowed_feas
                        and float(elapsed_s) >= momentum_min_elapsed
                        and directional_coherence
                        and move_gate_ok
                        and config.DYNAMIC_MOMENTUM_MIN_ASK <= float(q_ask) <= effective_max_ask
                    )
                    if (
                        not is_momentum_candidate
                        and not move_gate_ok
                        and move_ref_mode == move_ref.MODE_CONSERVATIVE
                        and allowed_feas
                        and float(elapsed_s) >= momentum_min_elapsed
                        and directional_coherence
                        and abs(move_usd) >= min_move_usd
                        and config.DYNAMIC_MOMENTUM_MIN_ASK <= float(q_ask) <= effective_max_ask
                    ):
                        # Spot-only gate would have allowed this momentum entry; conservative ref blocks it.
                        if not slot.move_ref_block_logged:
                            twap_move = (spot - slot.strike_twap) if slot.strike_twap else None
                            logger.warning(
                                f"[{tag}] MOVE_REF_BLOCK asset={asset} side={favored_side} spot_move={move_usd:+.2f} "
                                f"twap_move={(f'{twap_move:+.2f}' if twap_move is not None else 'n/a')} "
                                f"used_move={gate_move_usd:+.2f} threshold={min_move_usd:.2f} ref={move_ref_used} "
                                f"strike_spot={strike:.4f} strike_twap={(f'{slot.strike_twap:.4f}' if slot.strike_twap else 'n/a')} "
                                f"src={slot.strike_twap_source} ask={float(q_ask):.2f} t={elapsed_s:.0f}s"
                            )
                            journal_write(
                                "move_ref_block",
                                asset=asset,
                                slot=slot_key,
                                side=favored_side,
                                spot=spot,
                                spot_move=round(move_usd, 4),
                                twap_move=(round(twap_move, 4) if twap_move is not None else None),
                                used_move=round(gate_move_usd, 4),
                                threshold=round(min_move_usd, 4),
                                move_ref_used=move_ref_used,
                                strike_spot=strike,
                                strike_twap=slot.strike_twap,
                                strike_twap_source=slot.strike_twap_source,
                                ask=float(q_ask),
                                elapsed_s=round(float(elapsed_s), 1),
                            )
                            slot.move_ref_block_logged = True
                    if (
                        not slot.momentum_gate_logged
                        and allowed_feas
                        and float(elapsed_s) < momentum_min_elapsed
                        and directional_coherence
                        and abs(move_usd) >= min_move_usd
                        and config.DYNAMIC_MOMENTUM_MIN_ASK <= float(q_ask) <= effective_max_ask
                    ):
                        # Debug: momentum candidate rejected only by the elapsed gate (once per slot).
                        logger.info(
                            f"[{tag}] MOMENTUM_GATE_EARLY asset={asset} side={favored_side} t={elapsed_s:.0f}s "
                            f"< {momentum_min_elapsed:.0f}s z={abs_z:.2f} ask={float(q_ask):.2f} "
                            f"spot_move={move_usd:+.2f} threshold={min_move_usd:.2f} "
                            f"move_ref={move_ref_used} move_ref_ok={move_gate_ok} used_move={gate_move_usd:+.2f}"
                        )
                        journal_write(
                            "momentum_elapsed_gate",
                            asset=asset,
                            slot=slot_key,
                            side=favored_side,
                            elapsed_s=round(float(elapsed_s), 1),
                            min_elapsed_s=momentum_min_elapsed,
                            z=round(abs_z, 3),
                            ask=float(q_ask),
                            spot_move=round(move_usd, 4),
                            used_move=round(gate_move_usd, 4),
                            threshold=round(min_move_usd, 4),
                            move_ref_used=move_ref_used,
                            move_ref_ok=bool(move_gate_ok),
                        )
                        slot.momentum_gate_logged = True
                    dyn_min_z = max(config.DYNAMIC_MIN_Z, params.midband_min_z)
                    is_maker_candidate = (
                        mid_px is not None
                        and config.DYNAMIC_MIDBAND_LO <= mid_px <= config.DYNAMIC_MIDBAND_HI
                        and abs_z >= dyn_min_z
                    )
                    maker_feats = None
                    maker_zone_blocked = self._entry_zone_blocked(slot, slot_key, "maker", elapsed_s)
                    if is_maker_candidate and maker_zone_blocked:
                        is_maker_candidate = False
                    if is_maker_candidate and maker_require_twap and not slot.strike_twap:
                        # No settlement (TWAP) strike for this slot (e.g. first slot after a restart):
                        # don't quote the maker leg at all. Logged once per slot.
                        is_maker_candidate = False
                        self._log_maker_block(
                            slot, slot_key, maker_guard.REASON_NO_TWAP_STRIKE, stage="quote",
                            side=favored_side, spot=spot, z_spot=signed_z_dyn, z_twap=None, mid=mid_px,
                            elapsed_s=elapsed_s,
                        )
                    if is_maker_candidate and maker_guard_on:
                        mk_ok, mk_reason, mk_z_used, mk_ref = maker_guard.check_quote(
                            favored_side, signed_z_dyn, z_twap_dyn,
                            float(getattr(config, "MAKER_MIN_TWAP_SIGMA", 0.25)),
                        )
                        mk_hi, mk_drop = self._mid_hist.drop(fav_token, now, mid_px)
                        if mk_ok and maker_guard.mid_drop_blocked(
                            mk_drop, float(getattr(config, "MAKER_MAX_MID_DROP_60S", 0.15))
                        ):
                            mk_ok, mk_reason = False, maker_guard.REASON_MID_DROP
                        if mk_ok and maker_guard.post_falling_mid_blocked(
                            mk_drop,
                            self._mid_hist.range_over(
                                fav_token, now, float(getattr(config, "MAKER_POST_STABLE_S", 3.0)), mid_px
                            ),
                            float(getattr(config, "MAKER_POST_MAX_MID_DROP", 0.0)),
                            float(getattr(config, "MAKER_POST_STABLE_MAX_RANGE", 0.0)),
                        ):
                            mk_ok, mk_reason = False, maker_guard.REASON_POST_FALLING_MID
                        if not mk_ok:
                            is_maker_candidate = False
                            self._log_maker_block(
                                slot, slot_key, mk_reason, stage="quote", side=favored_side, spot=spot,
                                z_spot=signed_z_dyn, z_twap=z_twap_dyn, mid=mid_px, mid_high=mk_hi,
                                mid_drop=mk_drop, elapsed_s=elapsed_s,
                            )
                        else:
                            maker_feats = {
                                "twap_sigma": (round(z_twap_dyn, 4) if z_twap_dyn is not None else None),
                                "spot_sigma": round(float(signed_z_dyn), 4),
                                "maker_z_used": round(float(mk_z_used), 4),
                                "maker_z_ref": mk_ref,
                            }

                    if is_momentum_candidate:
                        logger.info(
                            f"[{tag}] 🤖 Consulting Jev on MOMENTUM ({favored_side} @ {q_ask:.2f} | spot: {px(spot)} | S0: {px(strike)} | "
                            f"move: {move_usd:+.2f} >= {min_move_usd:.2f} | max_ask: {max_allowed_ask:.2f} | t={elapsed_s:.0f}s | "
                            f"ref: {move_ref_used} gate_move: {gate_move_usd:+.2f})..."
                        )
                        jev_dec = await self.jev.evaluate_dynamic(
                            asset=asset,
                            opportunity_type="MOMENTUM_TAKER",
                            side=favored_side,
                            spot=spot,
                            strike=strike,
                            realized_twap=realized_twap,
                            z_score=abs_z,
                            move_usd=move_usd,
                            remaining_s=remaining_s,
                            elapsed_s=elapsed_s,
                            best_bid=float(q_bid),
                            best_ask=float(q_ask),
                            proposed_price=float(q_ask),
                            spread=spread,
                            chop_crosses=chop,
                            daily_vol=params.daily_vol,
                            sigma_t=sigma_t,
                            min_move_usd=min_move_usd,
                            max_allowed_ask=max_allowed_ask,
                        )
                        self._log_jev_shadow(tag, "MOMENTUM", asset, favored_side, float(q_ask), jev_dec,
                                             slot=slot_key, elapsed_s=elapsed_s)
                        if jev_dec.passed:
                            logger.info(
                                f"[{tag}] ⚡ JEV_DYNAMIC MOMENTUM ({favored_side} @ {q_ask:.2f} x {jev_dec.size_multiplier:.0%}): {jev_dec.reason}"
                            )
                            taker_fee = market.get("fee_rate", config.TAKER_FEE_RATE) * float(q_ask) * (1.0 - float(q_ask))
                            fake_signal = TWAPSignal(
                                side=favored_side,
                                z_score=abs_z,
                                signed_z=signed_z_dyn,
                                model_prob=mid_px or 0.5,
                                ask=float(q_ask),
                                edge_gross=max(0.01, 1.0 - float(q_ask)),
                                fee_taker=taker_fee,
                                edge_net=max(0.01, (1.0 - float(q_ask)) - taker_fee),
                                s_req=spot,
                                s_0=strike,
                                spot=spot,
                                realized_twap=realized_twap,
                                elapsed_s=elapsed_s,
                                remaining_s=remaining_s,
                                sigma_twap_usd=sigma_twap_dyn,
                                passed_gates=True,
                                skip_reason="",
                            )
                            await self._place_taker_order(
                                slot,
                                market,
                                fake_signal,
                                float(q_ask),
                                strategy_tag="JEV_DYNAMIC_MOMENTUM",
                                size_multiplier=jev_dec.size_multiplier,
                                jev_decision=jev_dec,
                            )
                            if slot.position and slot_key:
                                self._slot_done.add(slot_key)
                                self._save_slot_state(
                                    slot_ts=slot_start_ts,
                                    slug=slot_key,
                                    strike=strike,
                                    asset=asset,
                                )
                        else:
                            logger.info(f"[{tag}] 🛑 Jev VETOED momentum: {jev_dec.reason}")
                            journal_write(
                                "jev_veto",
                                strategy="JEV_DYNAMIC_MOMENTUM",
                                asset=asset,
                                side=favored_side,
                                reason=jev_dec.reason,
                                jev=jev_dec.to_dict(),
                            )

                    # If no position open/filled and no resting order, evaluate maker spread opportunity
                    if not slot.position and not slot.resting_order and is_maker_candidate and mid_px is not None:
                        target_bid = round(mid_px - config.DYNAMIC_HALF_SPREAD, 2)
                        target_bid = max(config.DYNAMIC_MIDBAND_LO, min(config.DYNAMIC_MIDBAND_HI, target_bid))
                        target_bid = min(target_bid, round(float(q_ask) - 0.01, 2))
                        # Never bid > 0.50 on maker (guarantees positive payout asymmetry >= 1.0x)
                        target_bid = min(0.50, target_bid)

                        if target_bid < float(q_ask):
                            logger.info(
                                f"[{tag}] 🤖 Consulting Jev on MAKER ({favored_side} @ {target_bid:.2f} | mid: {mid_px:.2f} | spread: {spread:.2f})..."
                            )
                            jev_dec = await self.jev.evaluate_dynamic(
                                asset=asset,
                                opportunity_type="MAKER_BID",
                                side=favored_side,
                                spot=spot,
                                strike=strike,
                                realized_twap=realized_twap,
                                z_score=abs_z,
                                move_usd=move_usd,
                                remaining_s=remaining_s,
                                elapsed_s=elapsed_s,
                                best_bid=float(q_bid),
                                best_ask=float(q_ask),
                                proposed_price=target_bid,
                                spread=spread,
                                chop_crosses=chop,
                                daily_vol=params.daily_vol,
                            )
                            self._log_jev_shadow(tag, "MAKER", asset, favored_side, float(target_bid), jev_dec,
                                                 slot=slot_key, elapsed_s=elapsed_s)
                            if jev_dec.passed:
                                logger.info(
                                    f"[{tag}] 📥 JEV_DYNAMIC MAKER ({favored_side} @ {target_bid:.2f} x {jev_dec.size_multiplier:.0%}): {jev_dec.reason}"
                                )
                                rebate_rate = slot.engine.calculate_maker_rebate(
                                    target_bid,
                                    fee_rate=market.get("fee_rate", config.TAKER_FEE_RATE),
                                    rebate_share=config.MAKER_REBATE_SHARE,
                                )
                                msig = MakerSignal(
                                    side=favored_side,
                                    z_score=abs_z,
                                    signed_z=signed_z_dyn,
                                    model_prob=mid_px or 0.5,
                                    target_bid=target_bid,
                                    best_bid=float(q_bid),
                                    best_ask=float(q_ask),
                                    maker_rebate=rebate_rate,
                                    edge_net=(mid_px - target_bid) + rebate_rate,
                                    s_req=spot,
                                    s_0=strike,
                                    spot=spot,
                                    realized_twap=realized_twap,
                                    elapsed_s=elapsed_s,
                                    remaining_s=remaining_s,
                                    sigma_twap_usd=sigma_twap_dyn,
                                    passed_gates=True,
                                    skip_reason="",
                                )
                                await self._place_maker_bid(
                                    slot,
                                    market,
                                    msig,
                                    strategy_tag="JEV_DYNAMIC_MAKER",
                                    size_multiplier=jev_dec.size_multiplier,
                                    jev_decision=jev_dec,
                                )
                                if slot.resting_order is not None and maker_feats:
                                    slot.resting_order["quote_features"] = maker_feats
                            else:
                                logger.info(f"[{tag}] 🛑 Jev VETOED maker bid: {jev_dec.reason}")
                                journal_write(
                                    "jev_veto",
                                    strategy="JEV_DYNAMIC_MAKER",
                                    asset=asset,
                                    side=favored_side,
                                    reason=jev_dec.reason,
                                    jev=jev_dec.to_dict(),
                                )

            # Manage resting orders in dynamic mode
            if slot.resting_order and not slot.position:
                ro = slot.resting_order
                ro_token = ro.get("token_id")
                ro_side = ro.get("side")
                ro_opp_side = "DOWN" if ro_side == "UP" else "UP"
                ro_opp_token = tokens.get(ro_opp_side)
                ro_top = self.book_stream.get_top(ro_token, opp_token_id=ro_opp_token) if ro_token else None
                cur_bid = (ro_top or {}).get("bid")
                cur_ask = (ro_top or {}).get("ask")
                if ro_token and (cur_bid is None or cur_ask is None):
                    bk_ro = self.pub_client.get_order_book(ro_token)
                    bk_opp = self.pub_client.get_order_book(ro_opp_token) if ro_opp_token else None
                    r_bid, r_ask, _ = extract_market_top(bk_ro, bk_opp)
                    if cur_bid is None:
                        cur_bid = r_bid
                    if cur_ask is None:
                        cur_ask = r_ask
                cur_bid = cur_bid or 0.0
                cur_ask = cur_ask or 1.0
                elapsed_resting = time.time() - ro.get("placed_at", time.time())
                is_clob_fill = (cur_ask <= ro["price"])
                # Live: a fill is what the EXCHANGE reports (order status / size_matched), never book inference.
                live_state = self._live_poll_order(ro, now) if config.MODE == "live" else None
                live_matched = float((live_state or {}).get("size_matched") or ro.get("live_matched") or 0.0)
                is_live_fill = bool(
                    config.MODE == "live" and live_state is not None
                    and (live_matched >= float(ro["shares"]) - 1e-6
                         or (live_state.get("status") in ("matched", "filled") and live_matched > 0))
                )

                # Adverse price move: spot crossed strike against resting order
                adverse_cross = (
                    (ro_side == "UP" and spot < strike)
                    or (ro_side == "DOWN" and spot > strike)
                )
                # Adverse Z flip: momentum reversed against resting order
                adverse_z = (
                    (ro_side == "UP" and signed_z_dyn < -0.20)
                    or (ro_side == "DOWN" and signed_z_dyn > 0.20)
                )

                # Realistic paper fill: filled if ask crossed bid, OR if bid is competitive at market top,
                # book is liquid, and order rested >= 5 seconds without adverse price move.
                competitive_at_top = (
                    ro["price"] >= float(cur_bid) - 0.01
                    and (float(cur_ask) - float(cur_bid)) <= config.DYNAMIC_MAX_BOOK_SPREAD
                )
                paper_passive_fill = (
                    competitive_at_top
                    and elapsed_resting >= max(5.0, config.MIDBAND_REQUOTE_SECONDS)
                    and not adverse_cross
                    and not adverse_z
                )
                is_paper_fill = (
                    config.MODE != "live"
                    and (is_clob_fill or paper_passive_fill)
                )

                mid_now = 0.5 * (float(cur_bid) + float(cur_ask)) if (cur_bid and cur_ask) else None
                if mid_now is not None and ro_token:
                    self._mid_hist.add(ro_token, now, mid_now)
                    if ro_opp_token:
                        self._mid_hist.add(ro_opp_token, now, 1.0 - mid_now)
                ro_hi, ro_drop = self._mid_hist.drop(ro_token, now, mid_now)
                self._log_rest_tick(slot, ro, now, elapsed_s, mid_now, cur_bid, cur_ask, ro_top, ro_drop, spot=spot)
                guard_reason = None
                ro_z_twap = None
                if maker_guard_on:
                    if maker_require_twap and not slot.strike_twap:
                        guard_reason = maker_guard.REASON_NO_TWAP_STRIKE
                    elif maker_guard.twap_cross_adverse(ro_side, spot, slot.strike_twap):
                        guard_reason = maker_guard.REASON_TWAP_CROSS
                    elif maker_guard.mid_drop_blocked(
                        ro_drop, float(getattr(config, "MAKER_MAX_MID_DROP_60S", 0.15))
                    ):
                        guard_reason = maker_guard.REASON_MID_DROP
                    elif is_paper_fill:
                        # Re-check the settlement-strike z before counting a (paper) fill.
                        ro_z_twap = z_twap_dyn
                        g_ok, g_reason, _, _ = maker_guard.check_quote(
                            ro_side, signed_z_dyn, ro_z_twap,
                            float(getattr(config, "MAKER_MIN_TWAP_SIGMA", 0.25)),
                        )
                        if not g_ok:
                            guard_reason = g_reason
                cancel = False
                cancel_reason = "band/time"
                if self._entry_zone_blocked(slot, slot_key, "maker", elapsed_s, stage="resting"):
                    cancel = True
                    cancel_reason = (
                        f"entry_block_zone (t={float(elapsed_s):.0f}s in "
                        f"[{config.ENTRY_BLOCK_ELAPSED_LO:.0f},{config.ENTRY_BLOCK_ELAPSED_HI:.0f}))"
                    )
                elif remaining_s < config.DYNAMIC_WINDOW_END_S:
                    cancel = True
                    cancel_reason = f"window_end ({remaining_s:.0f}s left)"
                elif mid_now is not None and not (config.DYNAMIC_MIDBAND_LO <= mid_now <= config.DYNAMIC_MIDBAND_HI):
                    cancel = True
                    cancel_reason = f"mid_left_band (mid={mid_now:.2f})"
                elif guard_reason:
                    cancel = True
                    cancel_reason = f"maker_guard_{guard_reason}"
                    self._log_maker_block(
                        slot, slot_key, guard_reason, stage="resting", side=ro_side, spot=spot,
                        z_spot=signed_z_dyn, z_twap=z_twap_dyn, mid=mid_now, mid_high=ro_hi,
                        mid_drop=ro_drop, elapsed_s=elapsed_s, bid=ro.get("price"),
                        would_fill=bool(is_paper_fill or is_live_fill),
                    )
                elif adverse_cross:
                    cancel = True
                    cancel_reason = f"adverse_spot_cross (spot={spot:.2f} vs strike={strike:.2f})"
                elif adverse_z:
                    cancel = True
                    cancel_reason = f"adverse_z_flip (z={signed_z_dyn:.2f})"

                if ro.get("cancel_pending") and not is_live_fill:
                    cancel = True
                    cancel_reason = ro.get("cancel_reason") or cancel_reason
                if is_live_fill:
                    cancel = False  # the exchange already filled it: adopt, never forget
                adopt_live = False
                if cancel and config.MODE == "live":
                    # cancel -> read back -> adopt any fill; unconfirmed cancels stay tracked and are retried
                    outcome, ro_after = self._live_cancel_resting(tag, ro, cancel_reason, live_matched)
                    slot.resting_order = ro_after
                    if outcome == "adopt":
                        ro = ro_after
                        adopt_live = True
                        cancel = False
                if cancel and config.MODE == "live":
                    pass  # "gone" (cleared) or "pending" (kept with cancel_pending, retried next tick)
                elif cancel:
                    logger.info(f"[{tag}] JEV_DYNAMIC cancel resting @{ro['price']:.2f} ({cancel_reason})")
                    slot.resting_order = None
                elif is_paper_fill or is_live_fill or adopt_live:
                    if is_live_fill and live_matched > 0 and abs(live_matched - float(ro["shares"])) > 1e-6:
                        ro = {**ro, "shares": round(live_matched, 4),
                              "cost_usd": round(live_matched * float(ro["price"]), 4)}
                    st_ts = _extract_slot_start_ts(market, ro.get("slot"))
                    now_fill = time.time()
                    el_s = max(0.0, min(300.0, round(now_fill - st_ts, 1))) if st_ts else None
                    slot.position = {
                        "asset": asset,
                        "slot": ro["slot"],
                        "strategy": "JEV_DYNAMIC_MAKER",
                        "side": ro["side"],
                        "token_id": ro["token_id"],
                        "opp_token_id": (market.get("tokens") or {}).get("DOWN" if ro["side"] == "UP" else "UP"),
                        "price": ro["price"],
                        "shares": ro["shares"],
                        "cost_usd": ro["cost_usd"],
                        "z_score": ro.get("z_score", 0.0),
                        "prob": ro.get("prob", ro["price"]),
                        "fee": 0.0,
                        "rebate": ro.get("rebate", 0.0),
                        "order_id": ro.get("order_id"),
                        "slot_start_ts": st_ts,
                        "entry_time": now_fill,
                        "entry_time_utc": datetime.now(timezone.utc).isoformat(),
                        "entry_elapsed_s": el_s,
                        "entry_window_time": _format_window_time(el_s),
                        "opened_at": time.time(),
                        "strike": strike,
                        "official": False,
                        "jev": ro.get("jev"),
                        **self._jev_shadow_tags(ro.get("jev")),
                        **{k: ro.get(k) for k in ("strike_spot", "strike_twap", "strike_twap_source", "move_ref_mode", "move_ref_used")},
                    }
                    if not slot.position.get("strike_twap") and slot.strike_twap:
                        slot.position["strike_twap"] = slot.strike_twap
                        slot.position["strike_twap_source"] = slot.strike_twap_source
                    if not slot.position.get("strike_spot"):
                        slot.position["strike_spot"] = slot.strike
                    feats = self._maker_fill_features(
                        slot, ro, spot=spot, z_spot=signed_z_dyn, z_twap=z_twap_dyn,
                        mid_high=ro_hi, mid_drop=ro_drop, elapsed_s=el_s,
                    )
                    slot.position["maker_features"] = feats
                    slot.resting_order = None
                    logger.info(
                        f"[{tag}] ✅ JEV_DYNAMIC MAKER FILL {ro['side']} @{ro['price']:.3f} x {ro['shares']:.2f}"
                    )
                    logger.info(f"[{tag}] MAKER_FILL_FEATURES {json.dumps(feats, default=str)}")
                    journal_write(
                        "maker_fill",
                        asset=asset,
                        slot=ro.get("slot"),
                        order_id=ro.get("order_id"),
                        side=ro["side"],
                        price=ro["price"],
                        shares=ro["shares"],
                        **self._jev_shadow_tags(ro.get("jev")),
                        **feats,
                    )
                    self._start_post_fill(ro, now, mid_now, cur_bid)

        elif is_momentum:
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
                opp_top = self.book_stream.get_top(opp_token, opp_token_id=token_id)
                opp_ask = opp_top.get("ask") if opp_top else None
                if opp_ask is None:
                    bk_opp = self.pub_client.get_order_book(opp_token)
                    bk_prelim = self.pub_client.get_order_book(token_id) if token_id else None
                    _, r_ask, _ = extract_market_top(bk_opp, bk_prelim)
                    opp_ask = r_ask
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
                            trade_ask_eval = trade_ask if trade_side == prelim_side else best_ask
                            bb_eval = float(best_bid) if best_bid is not None else None
                            ba_eval = float(trade_ask_eval) if trade_ask_eval is not None else None
                            sp_eval = round(ba_eval - bb_eval, 4) if (ba_eval is not None and bb_eval is not None) else 0.0
                            jev_decision = await self.jev.evaluate_momentum(
                                asset=asset,
                                side=signal.side,
                                spot=spot,
                                strike=strike,
                                move_usd=spot - strike,
                                remaining_s=remaining_s,
                                elapsed_s=elapsed_s,
                                best_bid=bb_eval,
                                best_ask=ba_eval,
                                spread=sp_eval,
                                chop_crosses=chop,
                                daily_vol=params.daily_vol,
                            )
                            if not jev_decision.passed:
                                logger.warning(f"[{tag}] Momentum trade vetoed by Jev: {jev_decision.reason}")
                                journal_write(
                                    "jev_veto",
                                    strategy="H-005_MOMENTUM",
                                    asset=asset,
                                    side=signal.side,
                                    reason=jev_decision.reason,
                                    jev=jev_decision.to_dict(),
                                )
                            else:
                                if jev_decision.size_multiplier < 1.0:
                                    logger.info(f"[{tag}] Jev downsized momentum ({jev_decision.size_multiplier:.0%}): {jev_decision.reason}")
                                else:
                                    logger.info(f"[{tag}] Jev approved momentum: {jev_decision.reason}")
                                await self._place_taker_order(
                                    slot, market, signal, trade_ask_eval,
                                    strategy_tag="H-005_MOMENTUM",
                                    size_multiplier=jev_decision.size_multiplier,
                                    jev_decision=jev_decision,
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

        elif is_midband:
            # Directional mid-band maker: use TWAP Z-score to pick ONE side.
            # Compute favored direction from spot vs required TWAP level.
            sigma_twap_mb = slot.engine.estimate_sigma_twap_usd(
                spot=spot, remaining_s=remaining_s,
                realized_vol_daily=params.daily_vol,
            )
            _, signed_z_mb, _ = slot.engine.compute_s_req_and_z(
                spot=spot, strike=strike,
                realized_twap=realized_twap, elapsed_s=elapsed_s,
                remaining_s=remaining_s, sigma_twap_usd=sigma_twap_mb,
            )
            favored_side = "UP" if signed_z_mb >= 0 else "DOWN"

            # Only quote the TWAP-favored side (eliminates BUG #4 UP-bias)
            for quote_side in (favored_side,):
                q_token = tokens.get(quote_side)
                opp_quote_side = "DOWN" if quote_side == "UP" else "UP"
                opp_q_token = tokens.get(opp_quote_side)
                if not q_token:
                    continue
                q_top = self.book_stream.get_top(q_token, opp_token_id=opp_q_token)
                q_bid = q_top.get("bid") if q_top else None
                q_ask = q_top.get("ask") if q_top else None
                if q_bid is None or q_ask is None:
                    bk = self.pub_client.get_order_book(q_token)
                    bk_opp = self.pub_client.get_order_book(opp_q_token) if opp_q_token else None
                    r_bid, r_ask, _ = extract_market_top(bk, bk_opp)
                    if q_bid is None:
                        q_bid = r_bid
                    if q_ask is None:
                        q_ask = r_ask
                msig = slot.engine.evaluate_midband(
                    remaining_s=remaining_s,
                    best_bid=q_bid,
                    best_ask=q_ask,
                    midband_lo=config.MIDBAND_LO,
                    midband_hi=config.MIDBAND_HI,
                    half_spread=config.MIDBAND_HALF_SPREAD,
                    min_seconds_left=config.MIDBAND_MIN_SECONDS_LEFT,
                    max_seconds_left=config.MIDBAND_MAX_SECONDS_LEFT,
                    fee_rate=market.get("fee_rate", config.TAKER_FEE_RATE),
                    rebate_share=config.MAKER_REBATE_SHARE,
                    side=quote_side,
                    spot=spot,
                    strike=strike,
                    realized_twap=realized_twap,
                    elapsed_s=elapsed_s,
                    min_z=params.midband_min_z,
                    realized_vol_daily=params.daily_vol,
                    max_book_spread=config.MIDBAND_MAX_BOOK_SPREAD,
                )
                mid_px = None
                if q_bid is not None and q_ask is not None:
                    mid_px = 0.5 * (float(q_bid) + float(q_ask))
                if config.EVAL_WINDOW_END_S <= remaining_s <= config.EVAL_WINDOW_START_S:
                    logger.info(
                        f"[{remaining_s:3.0f}s left] {tag} {quote_side}: mid={mid_px or 0:.3f} "
                        f"bid={q_bid or 0:.2f} ask={q_ask or 0:.2f} target={msig.target_bid:.2f} "
                        f"Z={msig.z_score:.2f} Pass={msig.passed_gates}"
                        + (f" | Skip: {msig.skip_reason}" if not msig.passed_gates else "")
                    )
                # One resting order / position per asset slot
                if (
                    msig.passed_gates
                    and not slot.resting_order
                    and not slot.position
                ):
                    if self._slot_entries_count() >= config.MAX_CONCURRENT_ENTRIES:
                        logger.warning(
                            f"[{tag}] Midband skipped: slot entry cap ({config.MAX_CONCURRENT_ENTRIES}) reached"
                        )
                        break
                    can_trade, risk_reason = self.risk.can_trade(open_notional=open_notional)
                    if can_trade:
                        q_bid_f = float(q_bid) if q_bid is not None else None
                        q_ask_f = float(q_ask) if q_ask is not None else None
                        spread_f = round(q_ask_f - q_bid_f, 4) if (q_ask_f is not None and q_bid_f is not None) else 0.0
                        jev_decision = await self.jev.evaluate_maker(
                            asset=asset,
                            side=quote_side,
                            spot=spot,
                            strike=strike,
                            realized_twap=realized_twap,
                            z_score=msig.z_score,
                            remaining_s=remaining_s,
                            elapsed_s=elapsed_s,
                            best_bid=q_bid_f,
                            best_ask=q_ask_f,
                            target_bid=msig.target_bid,
                            spread=spread_f,
                            chop_crosses=chop,
                            daily_vol=params.daily_vol,
                        )
                        if not jev_decision.passed:
                            logger.warning(f"[{tag}] Midband maker bid vetoed by Jev: {jev_decision.reason}")
                            journal_write(
                                "jev_veto",
                                strategy="MIDBAND_MAKER",
                                asset=asset,
                                side=quote_side,
                                reason=jev_decision.reason,
                                jev=jev_decision.to_dict(),
                            )
                            break
                        if jev_decision.size_multiplier < 1.0:
                            logger.info(f"[{tag}] Jev downsized maker ({jev_decision.size_multiplier:.0%}): {jev_decision.reason}")
                        else:
                            logger.info(f"[{tag}] Jev approved maker: {jev_decision.reason}")
                        # Reuse maker placement with strategy tag override via signal side
                        await self._place_maker_bid(
                            slot, market, msig, strategy_tag="MIDBAND_MAKER",
                            size_multiplier=jev_decision.size_multiplier,
                            jev_decision=jev_decision,
                        )
                        break
                    else:
                        logger.warning(f"[{tag}] Midband blocked by risk manager: {risk_reason}")
                        break

            if slot.resting_order and not slot.position:
                ro = slot.resting_order
                ro_token = ro.get("token_id")
                ro_side = ro.get("side")
                ro_opp_side = "DOWN" if ro_side == "UP" else "UP"
                ro_opp_token = tokens.get(ro_opp_side)
                ro_top = self.book_stream.get_top(ro_token, opp_token_id=ro_opp_token) if ro_token else None
                cur_bid = (ro_top or {}).get("bid")
                cur_ask = (ro_top or {}).get("ask")
                if ro_token and (cur_bid is None or cur_ask is None):
                    bk = self.pub_client.get_order_book(ro_token)
                    bk_opp = self.pub_client.get_order_book(ro_opp_token) if ro_opp_token else None
                    r_bid, r_ask, _ = extract_market_top(bk, bk_opp)
                    if cur_bid is None:
                        cur_bid = r_bid
                    if cur_ask is None:
                        cur_ask = r_ask
                cur_bid = cur_bid or 0.0
                cur_ask = cur_ask or 1.0
                elapsed_resting = time.time() - ro.get("placed_at", time.time())
                is_clob_fill = (cur_ask <= ro["price"])

                adverse_cross = (
                    (ro_side == "UP" and spot < strike)
                    or (ro_side == "DOWN" and spot > strike)
                )

                competitive_at_top = (
                    ro["price"] >= float(cur_bid) - 0.01
                    and (float(cur_ask) - float(cur_bid)) <= config.MIDBAND_MAX_BOOK_SPREAD
                )
                paper_passive_fill = (
                    competitive_at_top
                    and elapsed_resting >= max(5.0, config.MIDBAND_REQUOTE_SECONDS)
                    and not adverse_cross
                )
                is_paper_fill = (
                    config.MODE != "live"
                    and (is_clob_fill or paper_passive_fill)
                )

                # Cancel if mid left the band or time ran out or adverse price cross
                mid_now = 0.5 * (float(cur_bid) + float(cur_ask)) if cur_bid and cur_ask else None
                cancel = False
                cancel_reason = "band/time"
                if remaining_s < config.MIDBAND_MIN_SECONDS_LEFT:
                    cancel = True
                    cancel_reason = f"window_end ({remaining_s:.0f}s left)"
                elif mid_now is not None and not (config.MIDBAND_LO <= mid_now <= config.MIDBAND_HI):
                    cancel = True
                    cancel_reason = f"mid_left_band (mid={mid_now:.2f})"
                elif adverse_cross:
                    cancel = True
                    cancel_reason = f"adverse_spot_cross (spot={spot:.2f} vs strike={strike:.2f})"

                if cancel:
                    logger.info(f"[{tag}] Midband cancel resting @{ro['price']:.2f} ({cancel_reason})")
                    # BUG #1 fix: cancel live CLOB order too, not just in-memory state
                    if config.MODE == "live" and ro.get("order_id"):
                        try:
                            self.live_client.cancel_order(ro["order_id"])
                        except Exception as e:
                            logger.warning(f"[{tag}] Failed to cancel live order {ro['order_id']}: {e}")
                    slot.resting_order = None
                elif is_paper_fill or (config.MODE == "live" and is_clob_fill):
                    st_ts = _extract_slot_start_ts(market, ro.get("slot"))
                    now_fill = time.time()
                    el_s = max(0.0, min(300.0, round(now_fill - st_ts, 1))) if st_ts else None
                    slot.position = {
                        "asset": asset,
                        "slot": ro["slot"],
                        "strategy": "MIDBAND_MAKER",
                        "side": ro["side"],
                        "token_id": ro["token_id"],
                        "opp_token_id": (market.get("tokens") or {}).get("DOWN" if ro["side"] == "UP" else "UP"),
                        "price": ro["price"],
                        "shares": ro["shares"],
                        "cost_usd": ro["cost_usd"],
                        "z_score": ro.get("z_score", 0.0),
                        "prob": ro.get("prob", ro["price"]),
                        "fee": 0.0,
                        "rebate": ro.get("rebate", 0.0),
                        "order_id": ro.get("order_id"),
                        "slot_start_ts": st_ts,
                        "entry_time": now_fill,
                        "entry_time_utc": datetime.now(timezone.utc).isoformat(),
                        "entry_elapsed_s": el_s,
                        "entry_window_time": _format_window_time(el_s),
                        "opened_at": time.time(),
                        "strike": strike,
                        "official": False,
                        "jev": ro.get("jev"),
                        **self._jev_shadow_tags(ro.get("jev")),
                    }
                    slot.resting_order = None
                    logger.info(
                        f"[{tag}] MIDBAND FILL {ro['side']} @{ro['price']:.3f} x {ro['shares']:.2f}"
                    )

        elif is_sniper:
            logger.warning(f"[{tag}] H-001 sniper is retired — no trade")
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
                    st_ts = _extract_slot_start_ts(market, ro.get("slot"))
                    now_fill = time.time()
                    el_s = max(0.0, min(300.0, round(now_fill - st_ts, 1))) if st_ts else None
                    slot.position = {
                        "asset": asset,
                        "slot": ro["slot"],
                        "strategy": strategy_tag,
                        "side": ro["side"],
                        "token_id": ro["token_id"],
                        "opp_token_id": (market.get("tokens") or {}).get("DOWN" if ro["side"] == "UP" else "UP"),
                        "price": ro["price"],
                        "shares": ro["shares"],
                        "cost_usd": ro["cost_usd"],
                        "z_score": ro["z_score"],
                        "prob": ro["prob"],
                        "fee": 0.0,
                        "rebate": ro["rebate"],
                        "slot_start_ts": st_ts,
                        "entry_time": now_fill,
                        "entry_time_utc": datetime.now(timezone.utc).isoformat(),
                        "entry_elapsed_s": el_s,
                        "entry_window_time": _format_window_time(el_s),
                        "order_id": ro["order_id"],
                        "jev": ro.get("jev"),
                        **self._jev_shadow_tags(ro.get("jev")),
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

        if slot.position and (
            slot.position.get("strategy") in (
                "H-005_MOMENTUM", "LATE_MOMENTUM", "MOMENTUM", "H-005",
                "JEV_DYNAMIC_MOMENTUM", "JEV_DYNAMIC_MAKER",
                "AVN_MAS", "AVN-MAS", "AVN_MOMENTUM",
            )
            or "DYNAMIC" in str(slot.position.get("strategy", ""))
            or "AVN" in str(slot.position.get("strategy", ""))
        ):
            await self._manage_open_momentum_position(
                slot, market, remaining_s, tokens,
                spot=spot, strike=strike,
                realized_twap=realized_twap, elapsed_s=elapsed_s,
                now=now,
            )

        if remaining_s <= 0.0:
            if slot.resting_order:
                slot.resting_order = None
            if slot.position:
                self._queue_settlement(slot)

    def _ensure_twap_strike(self, slot: CoinSlot, market: dict, slot_start_ts: float, elapsed_s: float) -> None:
        """Resolve the TWAP-based strike estimate once per slot and cache it on the slot."""
        if slot.strike_twap_resolved:
            return
        # Let late-arriving ticks stamped just before slot open land before we average.
        if elapsed_s is not None and float(elapsed_s) < 3.0:
            return
        window_s = float(getattr(config, "SETTLEMENT_TWAP_WINDOW_S", 60.0))
        value, source = move_ref.estimate_twap_strike(
            self.cl_stream,
            slot_start_ts,
            window_s,
            asset=slot.asset,
            price_to_beat=(market or {}).get("price_to_beat"),
        )
        slot.strike_twap = value
        slot.strike_twap_source = source
        slot.strike_twap_resolved = True
        tag = slot.asset.upper()
        if value is not None:
            diff = (float(slot.strike) - value) if slot.strike else None
            logger.info(
                f"[{tag}] MOVE_REF strike_spot={(f'{slot.strike:.4f}' if slot.strike else 'n/a')} "
                f"strike_twap={value:.4f} src={source} window={window_s:.0f}s "
                f"spot_minus_twap={(f'{diff:+.4f}' if diff is not None else 'n/a')} slot={(market or {}).get('slug')}"
            )
        else:
            logger.warning(
                f"[{tag}] MOVE_REF TWAP strike unavailable for {(market or {}).get('slug')} "
                f"(insufficient pre-open ticks over {window_s:.0f}s); momentum gate falls back to spot strike"
            )

    def _entry_zone_blocked(
        self, slot: CoinSlot, slot_key: str | None, leg: str, elapsed_s, stage: str = "entry",
    ) -> bool:
        """True when `leg` is in the entry dead zone [ENTRY_BLOCK_ELAPSED_LO, HI). Logs once per slot/leg/stage."""
        if not getattr(config, "ENTRY_BLOCK_ENABLED", True):
            return False
        lo = float(getattr(config, "ENTRY_BLOCK_ELAPSED_LO", 120.0))
        hi = float(getattr(config, "ENTRY_BLOCK_ELAPSED_HI", 180.0))
        legs = tuple(getattr(config, "ENTRY_BLOCK_LEGS", ("momentum", "maker")))
        if hi <= lo or leg not in legs or elapsed_s is None:
            return False
        if _in_utc_windows(getattr(config, "ENTRY_BLOCK_OFF_UTC", ""), time.time()):
            return False
        try:
            e = float(elapsed_s)
        except (TypeError, ValueError):
            return False
        if not (lo <= e < hi):
            return False
        key = f"{leg}:{stage}"
        if key not in slot.entry_zone_logged:
            slot.entry_zone_logged.add(key)
            logger.info(
                f"[{slot.asset.upper()}] ENTRY_BLOCK_ZONE leg={leg} stage={stage} t={e:.0f}s in [{lo:.0f},{hi:.0f}) "
                f"slot={slot_key} -> no new {leg} entries until t={hi:.0f}s"
                + (" (cancelling resting maker bid)" if stage == "resting" else "")
            )
            journal_write(
                "entry_block_zone", asset=slot.asset, slot=slot_key, leg=leg, stage=stage,
                elapsed_s=round(e, 1), lo=lo, hi=hi,
            )
        return True

    def _log_maker_block(self, slot: CoinSlot, slot_key: str, reason: str | None, stage: str, **kw) -> None:
        """MAKER_BLOCK log + journal; quote-stage blocks log once per slot per reason."""
        reason = reason or "unknown"
        key = f"{stage}:{reason}"
        if stage == "quote" and key in slot.maker_block_logged:
            return
        slot.maker_block_logged.add(key)

        def _f(v, fmt):
            return format(v, fmt) if isinstance(v, (int, float)) else "n/a"

        logger.warning(
            f"[{slot.asset.upper()}] MAKER_BLOCK reason={reason} stage={stage} side={kw.get('side')} "
            f"z_spot={_f(kw.get('z_spot'), '+.2f')} z_twap={_f(kw.get('z_twap'), '+.2f')} "
            f"strike_spot={_f(slot.strike, '.4f')} strike_twap={_f(slot.strike_twap, '.4f')} "
            f"spot={_f(kw.get('spot'), '.4f')} mid={_f(kw.get('mid'), '.3f')} "
            f"mid_60s_high={_f(kw.get('mid_high'), '.3f')} mid_drop={_f(kw.get('mid_drop'), '.3f')} "
            f"t={_f(kw.get('elapsed_s'), '.0f')}s"
            + (f" bid={_f(kw.get('bid'), '.2f')} would_fill={kw.get('would_fill')}" if stage == "resting" else "")
        )
        payload = {
            k: (round(v, 6) if isinstance(v, float) else v)
            for k, v in kw.items()
        }
        journal_write(
            "maker_block",
            asset=slot.asset,
            slot=slot_key,
            reason=reason,
            stage=stage,
            strike_spot=slot.strike,
            strike_twap=slot.strike_twap,
            strike_twap_source=slot.strike_twap_source,
            **payload,
        )

    @staticmethod
    def _jev_shadow_tags(jev: dict | None) -> dict:
        """Persistable JEV_SHADOW tags derived from the stored Jev decision dict."""
        jev = jev or {}
        verdict = jev.get("shadow_verdict") if jev.get("shadow") else None
        real = jev.get("shadow_decision") or {}
        conv = real.get("conviction", jev.get("conviction")) if verdict else None
        return {"jev_shadow_verdict": verdict, "jev_shadow_conviction": conv}

    def _log_jev_shadow(
        self, tag: str, leg: str, asset: str, side: str, price: float, jev_dec: JevDecision,
        slot: str | None = None, elapsed_s: float | None = None,
    ) -> None:
        """JEV_SHADOW: log + journal the real (non-enforced) Jev verdict. Never raises."""
        if not getattr(jev_dec, "shadow", False):
            return
        try:
            real = jev_dec.shadow_decision or {}
            verdict = jev_dec.shadow_verdict or "error"
            label = "shadow_error" if verdict == "error" else verdict

            def _f(v, nd=2):
                try:
                    return f"{float(v):.{nd}f}"
                except (TypeError, ValueError):
                    return "na"

            logger.info(
                f"[{tag}] 👻 JEV_SHADOW {leg} {side} @ {price:.2f} verdict={label} "
                f"conv={_f(real.get('conviction'))} rev={_f(real.get('reversal_risk'))} "
                f"fake={_f(real.get('fakeout_risk'))} regime={real.get('market_regime')} "
                f"lat={_f(real.get('latency_ms'), 0)}ms | {real.get('reason')} -> proceeding at 100% (not enforced)"
            )
            journal_write(
                "jev_shadow",
                strategy=f"JEV_DYNAMIC_{leg}",
                asset=asset,
                side=side,
                price=price,
                slot=slot,
                elapsed_s=elapsed_s,
                verdict=label,
                conviction=real.get("conviction"),
                reversal_risk=real.get("reversal_risk"),
                fakeout_risk=real.get("fakeout_risk"),
                regime=real.get("market_regime"),
                action=real.get("action"),
                reason=real.get("reason"),
                latency_ms=real.get("latency_ms"),
                jev=real,
            )
        except Exception as e:  # logging must never block the trade
            logger.warning(f"[{tag}] JEV_SHADOW log failed: {e}")

    def _maker_fill_features(
        self, slot: CoinSlot, ro: dict, spot=None, z_spot=None, z_twap=None,
        mid_high=None, mid_drop=None, elapsed_s=None,
    ) -> dict:
        jev = ro.get("jev") or {}
        side = ro.get("side")
        sgn = 1.0 if str(side).upper() == "UP" else -1.0
        return {
            "strike_spot": slot.strike,
            "strike_twap": slot.strike_twap,
            "strike_twap_source": slot.strike_twap_source,
            "spot": spot,
            "twap_sigma": (round(sgn * float(z_twap), 4) if z_twap is not None else None),
            "spot_sigma": (round(sgn * float(z_spot), 4) if z_spot is not None else None),
            "mid_60s_high": (round(mid_high, 4) if mid_high is not None else None),
            "mid_drop_60s": (round(mid_drop, 4) if mid_drop is not None else None),
            "elapsed_s": elapsed_s,
            "jev_conviction": jev.get("conviction"),
            "maker_guard": bool(getattr(config, "MAKER_TWAP_GUARD", False)),
            "quote_features": ro.get("quote_features"),
        }

    def _strike_meta(self, slot: CoinSlot) -> dict:
        return {
            "strike_spot": slot.strike,
            "strike_twap": slot.strike_twap,
            "strike_twap_source": slot.strike_twap_source,
            "move_ref_mode": move_ref.normalize_mode(getattr(config, "MOMENTUM_MOVE_REF", "conservative")),
            "move_ref_used": slot.move_ref_used,
        }

    async def _place_taker_order(
        self,
        slot: CoinSlot,
        market: dict,
        signal: TWAPSignal,
        ask: float,
        strategy_tag: str = "H-001_SNIPER",
        size_multiplier: float = 1.0,
        jev_decision: JevDecision | None = None,
    ) -> None:
        """Execute immediate taker sniper order against available ask."""
        token_id = market["tokens"].get(signal.side)
        if not token_id or not ask:
            return

        stake_usd = (
            self.risk.calculate_stake(signal.edge_net, ask, open_notional=self._open_notional())
            * max(0.1, float(size_multiplier))
        )
        min_allowed_stake = min(config.BASE_STAKE_USD, max(1.0, config.BASE_STAKE_USD * float(size_multiplier)))
        if stake_usd < min_allowed_stake:
            logger.warning(f"[{slot.asset.upper()}] Stake ${stake_usd:.2f} below allowed minimum (${min_allowed_stake:.2f}); skip")
            return
        shares = max(config.MIN_ORDER_SHARES, round(stake_usd / ask, 2))
        shares = cap_order_shares(shares, ask)
        if shares is None:
            logger.warning(f"[{slot.asset.upper()}] {config.MIN_ORDER_SHARES:.0f} sh @ {ask:.3f} exceeds MAX_TRADE_USD ${config.MAX_TRADE_USD:.2f}; skip")
            return
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
        if jev_decision:
            logger.info(f"   Jev Guardian: {jev_decision.reason} (lat: {jev_decision.latency_ms:.0f}ms)")
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

        st_ts = _extract_slot_start_ts(market)
        now_fill = time.time()
        el_s = max(0.0, min(300.0, round(now_fill - st_ts, 1))) if st_ts else None
        entry_window_time = _format_window_time(el_s)
        entry_time_utc = datetime.now(timezone.utc).isoformat()
        total_cost = round(cost_usd + fee_usd, 4)

        journal_write(
            "paper_fill" if is_paper else "fill",
            strategy=strategy_tag,
            side=signal.side,
            price=ask,
            shares=shares,
            cost_usd=cost_usd,
            fee=fee_usd,
            total_cost=total_cost,
            slot=market.get("slug"),
            asset=slot.asset,
            order_id=order_id,
            z_score=signal.z_score,
            edge=signal.edge_net,
            entry_time_utc=entry_time_utc,
            entry_elapsed_s=el_s,
            entry_window_time=entry_window_time,
            jev=jev_decision.to_dict() if jev_decision else None,
            **self._jev_shadow_tags(jev_decision.to_dict() if jev_decision else None),
            **self._strike_meta(slot),
        )

        slot.position = {
            "asset": slot.asset,
            "slot": market["slug"],
            "strategy": strategy_tag,
            "side": signal.side,
            "token_id": token_id,
            "opp_token_id": (market.get("tokens") or {}).get("DOWN" if signal.side == "UP" else "UP"),
            "price": ask,
            "shares": shares,
            "cost_usd": cost_usd,
            "fee": fee_usd,
            "total_cost": total_cost,
            "strike": slot.strike or signal.s_0,
            "z_score": signal.z_score,
            "prob": signal.model_prob,
            "rebate": 0.0,
            "slot_start_ts": st_ts,
            "entry_time": now_fill,
            "entry_time_utc": entry_time_utc,
            "entry_elapsed_s": el_s,
            "entry_window_time": entry_window_time,
            "order_id": order_id,
            "jev": jev_decision.to_dict() if jev_decision else None,
            **self._jev_shadow_tags(jev_decision.to_dict() if jev_decision else None),
            **self._strike_meta(slot),
        }

    def _log_rest_tick(self, slot, ro, now, elapsed_s, mid, bid, ask, top, drop, spot=None) -> None:
        """~1/s resting-path trace (LOG-ONLY). Uses only data already fetched. Never raises."""
        try:
            if not bool(getattr(config, "MAKER_REST_TRACE", True)):
                return
            oid = ro.get("order_id")
            if not oid or not self._rest_throttle.ready(oid, now):
                return
            px = ro.get("price")
            bsz = (top or {}).get("bid_size")
            at_px = bool(bid is not None and px is not None and abs(float(bid) - float(px)) < 1e-6)
            journal_write(
                "maker_rest_tick", order_id=oid, asset=slot.asset, slot=ro.get("slot"), side=ro.get("side"),
                bid_price=px, elapsed_s=(round(float(elapsed_s), 1) if elapsed_s is not None else None),
                resting_s=round(max(0.0, now - float(ro.get("placed_at", now))), 1),
                mid=(round(mid, 4) if mid is not None else None), best_bid=bid, best_ask=ask,
                best_bid_size=bsz, best_bid_is_our_price=at_px, size_at_our_price=(bsz if at_px else None),
                drop_60s=(round(drop, 4) if drop is not None else None),
            )
        except Exception as e:
            logger.warning(f"maker_rest_tick log failed: {e}")

    def _start_post_fill(self, ro, now, mid, bid) -> None:
        try:
            if bool(getattr(config, "MAKER_POST_FILL_TRACE", True)):
                self._post_fill.start(ro.get("order_id"), now, mid, bid, ro.get("price"))
        except Exception as e:
            logger.warning(f"post_fill start failed: {e}")

    def _log_post_fill_tick(self, slot, now) -> None:
        """maker_post_fill at +5/+15/+30/+60s, then every 10s until slot end (300s) or exit. LOG-ONLY, never raises.
        Reads only the in-memory book stream cache (no API calls)."""
        try:
            if not bool(getattr(config, "MAKER_POST_FILL_TRACE", True)):
                return
            pos = slot.position
            if not pos:
                return
            oid = pos.get("order_id")
            if not oid or not self._post_fill.snapshot(oid):
                return
            st_ts = (getattr(slot, "market", None) or {}).get("slot_start_ts")
            slot_el = round(now - float(st_ts), 1) if st_ts else None
            if slot_el is not None and slot_el >= 300:
                self._post_fill.stop(oid)
                return
            due = self._post_fill.due(oid, now)
            if not due:
                return
            tok = pos.get("token_id")
            top = self.book_stream.get_top(tok, opp_token_id=pos.get("opp_token_id")) if tok else None
            bid = (top or {}).get("bid")
            ask = (top or {}).get("ask")
            src = "book_stream"
            if bid is None or ask is None:
                # Fallback: the book the TP check used on the previous tick (cached on the position).
                bid = pos.get("_trace_last_bid") if bid is None else bid
                ask = pos.get("_trace_last_ask") if ask is None else ask
                src = "tp_check_cache"
            mid = 0.5 * (float(bid) + float(ask)) if (bid is not None and ask is not None) else None
            if mid is None:
                mid = self._mid_hist.latest(tok)
                src = "mid_hist" if mid is not None else "none"
            for off in due:
                journal_write("maker_post_fill", asset=slot.asset, slot=pos.get("slot"), side=pos.get("side"),
                              best_ask=ask, price_source=src, slot_elapsed_s=slot_el, **self._post_fill.record(oid, off, now, mid, bid))
        except Exception as e:
            logger.warning(f"maker_post_fill log failed: {e}")

    def _log_maker_post_shadow(self, slot, market, side, token_id, bid, shares, order_id, spot=None) -> None:
        """Shadow-only pre-post gate log (MAKER_POST_GATE_SHADOW). Never raises, never blocks."""
        if not bool(getattr(config, "MAKER_POST_GATE_SHADOW", False)):
            return
        try:
            rec = maker_guard.post_shadow_record(self._mid_hist, token_id, time.time())
            st0 = market.get("slot_start_ts")
            rec["elapsed_s"] = round(time.time() - float(st0), 1) if st0 else None
            journal_write(
                "maker_post_shadow", slot=market.get("slug"), asset=slot.asset, side=side, bid=bid,
                shares=shares, order_id=order_id, mode=config.MODE, **rec,
            )
        except Exception as e:
            logger.warning(f"[{slot.asset.upper()}] maker_post_shadow log failed: {e}")

    async def _place_maker_bid(
        self,
        slot: CoinSlot,
        market: dict,
        signal: MakerSignal,
        strategy_tag: str = "H-004_MAKER",
        size_multiplier: float = 1.0,
        jev_decision: JevDecision | None = None,
    ) -> None:
        """Post resting limit bid inside spread as Maker."""
        token_id = market["tokens"].get(signal.side)
        if not token_id or not signal.target_bid:
            return

        stake_usd = (
            self.risk.calculate_stake(signal.edge_net, signal.target_bid, open_notional=self._open_notional())
            * max(0.1, float(size_multiplier))
        )
        min_allowed_stake = min(config.BASE_STAKE_USD, max(1.0, config.BASE_STAKE_USD * float(size_multiplier)))
        if stake_usd < min_allowed_stake:
            logger.warning(f"[{slot.asset.upper()}] Stake ${stake_usd:.2f} below allowed minimum (${min_allowed_stake:.2f}); skip maker")
            return
        shares = max(config.MIN_ORDER_SHARES, round(stake_usd / signal.target_bid, 2))
        shares = cap_order_shares(shares, signal.target_bid)
        if shares is None:
            logger.warning(f"[{slot.asset.upper()}] {config.MIN_ORDER_SHARES:.0f} sh @ {signal.target_bid:.3f} exceeds MAX_TRADE_USD ${config.MAX_TRADE_USD:.2f}; skip maker")
            return
        cost_usd = round(shares * signal.target_bid, 2)
        total_rebate = round(signal.maker_rebate * shares, 4)

        logger.info("=" * 60)
        logger.info(f"📥 [{slot.asset.upper()} H-004 MAKER BID POSTED] RESTING BUY {signal.side} @ {signal.target_bid:.2f}")
        logger.info(f"   Z-Score: {signal.z_score:.2f} | Fair Value: {signal.model_prob:.1%} | Net Edge: {signal.edge_net:+.1%}")
        logger.info(f"   Target Size: {shares:.1f} shares (${cost_usd:.2f}) | Expected Rebate: ${total_rebate:.3f}")
        if jev_decision:
            logger.info(f"   Jev Guardian: {jev_decision.reason} (lat: {jev_decision.latency_ms:.0f}ms)")
        logger.info("=" * 60)

        order_id = f"maker_paper_{int(time.time()*1000)}"
        if config.MODE == "live" and self.live_client:
            try:
                resp = self.live_client.place_order(token_id, "BUY", signal.target_bid, shares, order_type="GTC")
                live_id = resp.get("order_id") or resp.get("orderID")
                if not resp.get("success") or not live_id:
                    logger.warning(f"Live GTC post failed (no order id / not accepted): status={resp.get('status')} "
                                   f"error={resp.get('error')}")
                    return
                order_id = live_id
                logger.info(f"Live limit order placed: id={order_id} status={resp.get('status')} "
                            f"matched_now={resp.get('shares')}")
            except Exception as e:
                logger.error(f"Live limit order placement failed: {e}")
                return

        self._log_maker_post_shadow(slot, market, signal.side, token_id, signal.target_bid, shares, order_id,
                                    spot=getattr(signal, "spot", None))
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
            "jev": jev_decision.to_dict() if jev_decision else None,
            **self._jev_shadow_tags(jev_decision.to_dict() if jev_decision else None),
            **self._strike_meta(slot),
        }


    async def _manage_open_momentum_position(
        self,
        slot: CoinSlot,
        market: dict,
        remaining_s: float,
        tokens: dict,
        spot: float | None = None,
        strike: float | None = None,
        realized_twap: float | None = None,
        elapsed_s: float | None = None,
        now: float | None = None,
    ) -> None:
        """Stop-loss, exit-before-close, and optional micro-hedge for open positions."""
        pos = slot.position
        if not pos:
            return

        side = pos["side"]
        opp_pos_side = "DOWN" if side == "UP" else "UP"
        token_id = pos.get("token_id") or tokens.get(side)
        opp_token_id = tokens.get(opp_pos_side) or pos.get("opp_token_id")
        best_bid = None
        best_ask = None
        if token_id:
            top = self.book_stream.get_top(token_id, opp_token_id=opp_token_id)
            best_bid = top.get("bid") if top else None
            best_ask = top.get("ask") if top else None
            if best_bid is None or best_ask is None:
                bk = self.pub_client.get_order_book(token_id)
                bk_opp = self.pub_client.get_order_book(opp_token_id) if opp_token_id else None
                r_bid, r_ask, _ = extract_market_top(bk, bk_opp)
                if best_bid is None:
                    best_bid = r_bid
                if best_ask is None:
                    best_ask = r_ask
        try:  # LOG-ONLY: cache the book used by the TP check for maker_post_fill tracing
            if isinstance(slot.position, dict):
                slot.position["_trace_last_bid"] = best_bid
                slot.position["_trace_last_ask"] = best_ask
        except Exception:
            pass

        entry = float(pos["price"])

        # Track peak market bid & activate dynamic trailing profit stop if enabled
        fee_rate = float(
            market.get("fee_rate")
            if market.get("fee_rate") is not None
            else getattr(config, "TAKER_FEE_RATE", 0.07)
        )
        # Round-trip taker fee (entry + exit legs): approx 2 * fee_rate * p * (1 - p)
        round_trip_fee = 2.0 * fee_rate * entry * (1.0 - entry)
        min_breakeven = round(entry + round_trip_fee, 4)
        min_lock_profit = round(min_breakeven + 0.01, 4)
        min_peak = max(
            float(getattr(config, "TRAILING_PROFIT_MIN_PEAK", 0.83)),
            min_lock_profit,
        )

        trailing_enabled = (
            getattr(config, "TRAILING_PROFIT_ENABLED", False)
            and getattr(config, "TRAILING_BREAKEVEN_ENABLED", False)
        )

        if best_bid is not None and trailing_enabled:
            peak_bid = max(float(pos.get("peak_bid", entry)), float(best_bid))
            pos["peak_bid"] = peak_bid
            if peak_bid >= min_peak:
                pos["trailing_active"] = True
                pos["breakeven_active"] = True

        # Mark carefully: raw best bid on thin CLOB books is often garbage (e.g. 0.28 vs 0.87 ask).
        mark = None
        if best_bid is not None and best_ask is not None:
            spread = float(best_ask) - float(best_bid)
            if 0 <= spread <= 0.15:
                mark = (float(best_bid) + float(best_ask)) / 2.0
            else:
                # Wide/absurd book — do not stop-loss on thin bid alone
                mark = None

        # 1) "Free-Ride" Take-Profit Limit Exit:
        # If current market best bid reaches >= 0.95 (or TAKE_PROFIT_ASK = 0.96), trigger early take-profit exit.
        # CRITICAL: We only exit if best_bid >= tp_target and best_bid > entry to guarantee locking in >= +15¢ profit.
        # Never sell into a depressed bid when best_bid < tp_target!
        tp_bid = getattr(config, "TAKE_PROFIT_BID", 0.95)
        tp_ask = getattr(config, "TAKE_PROFIT_ASK", 0.96)
        tp_target = min(tp_bid, tp_ask)
        if best_bid is not None and float(best_bid) >= tp_target and float(best_bid) > entry:
            exit_px = float(best_bid)
            logger.info(
                f"[{slot.asset.upper()} TAKE-PROFIT] Free-ride take-profit triggered: "
                f"best_bid={float(best_bid):.3f} >= target {tp_target:.2f} (entry={entry:.3f}) | "
                f"locking in profit @ {exit_px:.3f}"
            )
            await self._exit_position_early(slot, market, exit_px, reason="take_profit")
            return

        # MAKER_HOLD_TO_EXPIRY: after the take-profit check, maker positions hold to settlement
        # (no trailing, price stop, adverse stop-loss / urgent / hard flatten, or failsafe flatten).
        if self._maker_hold_to_expiry(pos):
            # Live: cash a likely winner on the CLOB shortly before expiry so it never needs an on-chain redeem.
            if (
                config.MODE == "live"
                and remaining_s is not None and float(remaining_s) <= float(config.LIVE_PRE_EXPIRY_EXIT_S)
                and best_bid is not None and float(best_bid) >= float(config.LIVE_PRE_EXPIRY_MIN_BID)
            ):
                logger.info(
                    f"[{slot.asset.upper()}] LIVE pre-expiry exit: bid={float(best_bid):.3f} >= "
                    f"{config.LIVE_PRE_EXPIRY_MIN_BID:.2f} with {float(remaining_s):.0f}s left (avoid redeem)"
                )
                await self._exit_position_early(slot, market, float(best_bid), reason="live_pre_expiry_exit")
                return
            if not pos.get("hold_to_expiry_logged"):
                pos["hold_to_expiry_logged"] = True
                logger.info(
                    f"[{slot.asset.upper()}] MAKER_HOLD_TO_EXPIRY holding {pos.get('strategy')} {side} "
                    f"entry={entry:.3f} to settlement (only TP >= {tp_target:.2f} exits early)"
                )
            return

        # 2) Dynamic Trailing Profit Stop (Target 3):
        # Optional: only active when TRAILING_PROFIT_ENABLED is explicitly enabled.
        # If enabled and peak_bid >= TRAILING_PROFIT_MIN_PEAK, trails peak bid by TRAILING_PROFIT_DISTANCE.
        trailing_enabled = (
            getattr(config, "TRAILING_PROFIT_ENABLED", False)
            and getattr(config, "TRAILING_BREAKEVEN_ENABLED", False)
        )
        peak_bid = float(pos.get("peak_bid", entry))
        if trailing_enabled and (pos.get("trailing_active") or pos.get("breakeven_active") or peak_bid >= min_peak) and best_bid is not None:
            trail_distance = getattr(config, "TRAILING_PROFIT_DISTANCE", 0.08)
            stop_price = round(peak_bid - trail_distance, 4)
            min_lock_profit = round(entry + round_trip_fee + 0.01, 4)
            if min_lock_profit < peak_bid:
                stop_price = max(stop_price, min_lock_profit)
            # Trailing profit must not scratch below entry; deep crashes belong to adverse SL.
            floor = round(entry, 4)
            if float(best_bid) <= stop_price and float(best_bid) >= floor:
                reason = f"trailing_profit (peak: {peak_bid:.2f}, exit: {float(best_bid):.2f}, entry: {entry:.2f})"
                logger.warning(
                    f"[{slot.asset.upper()} TRAILING PROFIT] Peak {peak_bid:.2f} >= {min_peak:.2f}, "
                    f"pulled back to {float(best_bid):.2f} <= stop_price {stop_price:.2f} "
                    f"(entry {entry:.2f}, fees: {round_trip_fee:.3f}) — triggering early exit"
                )
                await self._exit_position_early(slot, market, float(best_bid), reason=reason)
                return

        # 2b) Bid-based max-loss stop (PRICE_STOP_*): independent of spot/TWAP/sigma conditions.
        if getattr(config, "PRICE_STOP_ENABLED", False) and entry > 0 and token_id:
            if await self._check_price_stop(slot, market, pos, token_id, entry, remaining_s, now):
                return

        # 3) Adverse Stop-Loss with Hysteresis Buffer, TWAP Coherence & Persistence Filter:
        # A momentary 1-tick dip across strike must NOT trigger an instantaneous fire sale.
        # Live audit proved 1-tick stop-outs caused -$5.78 in profit leaks on winning trades.
        if spot is None:
            spot = self.cl_stream.price(slot.asset)
        if strike is None:
            strike = slot.strike or pos.get("strike")
        if elapsed_s is None:
            elapsed_s = max(0.0, 300.0 - remaining_s)

        if spot is not None and strike is not None and spot > 0 and strike > 0:
            adverse_dist = (strike - spot) if side == "UP" else (spot - strike)

            # Volatility-normalized distance hurdle
            params = slot.params
            daily_vol = getattr(params, "daily_vol", 0.035) if params else 0.035
            t_eff = max(float(elapsed_s), 30.0)
            sigma_t = daily_vol * ((t_eff / 86400.0) ** 0.5) * spot
            min_adverse_mult = float(getattr(config, "STOP_LOSS_MIN_ADVERSE_SIGMA", 0.50))

            # Anti-Settlement-Loss Zero Hurdle:
            # If remaining_s <= STOP_LOSS_ZERO_HURDLE_REMAINING_S (<= 45s), noise hurdle drops to 0.0!
            # With <= 45s left, ANY spot crossing strike against us will resolve to 0.00 at expiry.
            zero_hurdle_s = float(getattr(config, "STOP_LOSS_ZERO_HURDLE_REMAINING_S", 45.0))
            if remaining_s <= zero_hurdle_s:
                min_adverse_usd = 0.0
            else:
                min_adverse_usd = min_adverse_mult * sigma_t

            # Spot must breach strike beyond noise buffer (or simply cross strike if min_adverse_usd == 0)
            is_adverse_move = (adverse_dist > min_adverse_usd if min_adverse_usd == 0.0 else adverse_dist >= min_adverse_usd)
            now_ts = float(now) if now is not None else time.time()

            if not is_adverse_move:
                # Reset adverse persistence timer ONLY if spot genuinely recovered back into the noise buffer
                if pos.get("adverse_start_ts") is not None:
                    logger.info(
                        f"[{slot.asset.upper()} ADVERSE RESET] Spot {spot:.2f} recovered towards strike {strike:.2f} "
                        f"(dist={adverse_dist:.2f} < min={min_adverse_usd:.2f}) — resetting stop-loss persistence timer"
                    )
                    pos["adverse_start_ts"] = None

                # Fail-safe orderbook collapse defense:
                # If spot appears favorable or inside noise buffer, but Polymarket orderbook quotes have
                # collapsed into the final 35 seconds (market bid <= 0.22 and <= 45% of entry),
                # the market makers/arbitrageurs are pricing an expiry loss. Do NOT hold to 0.00!
                if remaining_s <= 35.0:
                    salvage_candidate = float(best_bid) if best_bid is not None else 0.0
                    if salvage_candidate < 0.05 and opp_token_id:
                        opp_top = self.book_stream.get_top(opp_token_id) if hasattr(self, "book_stream") else None
                        opp_ask = opp_top.get("ask") if opp_top else None
                        if opp_ask is None:
                            bk_opp = self.pub_client.get_order_book(opp_token_id)
                            _, opp_ask = extract_best_bid_ask(bk_opp)
                        if opp_ask is not None and 0.0 < opp_ask < 1.0:
                            salvage_candidate = max(salvage_candidate, round(1.0 - opp_ask, 4))

                    if 0.02 <= salvage_candidate <= 0.22 and salvage_candidate <= (entry * 0.45):
                        logger.warning(
                            f"[{slot.asset.upper()} FAILSAFE CRASH FLATTEN] remaining={remaining_s:.1f}s <= 35s, "
                            f"market bid {salvage_candidate:.3f} <= 0.22 (entry={entry:.3f}) — emergency salvage exit to prevent 0.00 expiry loss"
                        )
                        await self._exit_position_early(slot, market, salvage_candidate, reason="failsafe_orderbook_flatten")
                        return
            else:
                # Spot is adversely across strike beyond noise hurdle: track/continue persistence timer
                if pos.get("adverse_start_ts") is None:
                    pos["adverse_start_ts"] = now_ts
                adverse_duration = now_ts - float(pos["adverse_start_ts"])

                # Urgent and Hard-Flatten tiers
                urgent_remaining = float(getattr(config, "STOP_LOSS_URGENT_REMAINING_SECONDS", 45.0))
                hard_flatten_remaining = float(getattr(config, "STOP_LOSS_HARD_FLATTEN_REMAINING_S", 20.0))
                is_hard_flatten = (remaining_s <= hard_flatten_remaining)
                is_urgent = (remaining_s <= urgent_remaining)

                # Dual agreement with realized TWAP:
                # Enforced only in early/mid round (remaining_s > 45s).
                # In urgent and hard-flatten windows, spot directly governs binary expiry resolution.
                require_twap = getattr(config, "STOP_LOSS_REQUIRE_TWAP", True)
                twap_adverse = True
                if not is_urgent and require_twap and (elapsed_s is not None and elapsed_s < 180.0 and remaining_s > 120.0):
                    twap_val = realized_twap
                    if twap_val is None and market.get("slot_start_ts"):
                        twap_val, _ = self.cl_stream.compute_realized_twap(
                            start_ts=market["slot_start_ts"], end_ts=time.time(), asset=slot.asset
                        )
                    if twap_val is not None and twap_val > 0:
                        twap_adverse = (twap_val < strike) if side == "UP" else (twap_val > strike)

                # Catastrophic move override (>= 1.5 sigma_t)
                is_catastrophic = (adverse_dist >= 1.5 * sigma_t)
                is_adverse_confirmed = (is_urgent or twap_adverse or is_catastrophic)

                if is_hard_flatten:
                    salvage_min = float(getattr(config, "STOP_LOSS_PANIC_SALVAGE_MIN_BID", 0.02))
                    persistence_s = 0.0
                elif is_urgent:
                    salvage_min = float(getattr(config, "STOP_LOSS_URGENT_SALVAGE_MIN_BID", 0.05))
                    persistence_s = float(getattr(config, "STOP_LOSS_URGENT_PERSISTENCE_SECONDS", 3.0))
                else:
                    salvage_min = float(getattr(config, "STOP_LOSS_SALVAGE_MIN_BID", 0.10))
                    persistence_s = float(getattr(config, "STOP_LOSS_PERSISTENCE_SECONDS", 10.0))

                # Final 30s: accept any scrap of liquidity via complement / panic floor.
                if remaining_s <= 30.0:
                    salvage_min = min(salvage_min, 0.01)

                if is_adverse_confirmed:
                    if adverse_duration >= persistence_s or (is_catastrophic and persistence_s == 0.0):
                        if (best_bid is None or float(best_bid) < salvage_min) and opp_token_id:
                            # 1) Try realtime orderbook stream first (zero latency)
                            opp_top = self.book_stream.get_top(opp_token_id) if hasattr(self, "book_stream") else None
                            opp_ask = opp_top.get("ask") if opp_top else None
                            # 2) Fallback to REST orderbook if cache missing
                            if opp_ask is None:
                                bk_opp = self.pub_client.get_order_book(opp_token_id)
                                _, opp_ask = extract_best_bid_ask(bk_opp)
                            if opp_ask is not None and 0.0 < opp_ask < 1.0:
                                implied = round(1.0 - opp_ask, 4)
                                if best_bid is None or implied > float(best_bid):
                                    best_bid = implied

                        if best_bid is not None and float(best_bid) >= salvage_min:
                            if is_hard_flatten:
                                exit_reason = "hard_flatten_expiry"
                            elif is_urgent:
                                exit_reason = "urgent_stop_loss"
                            else:
                                exit_reason = "stop_loss"

                            logger.warning(
                                f"[{slot.asset.upper()} {exit_reason.upper()}] Spot {spot:.2f} crossed strike {strike:.2f} against {side} "
                                f"(adverse_dist={adverse_dist:.2f} >= min={min_adverse_usd:.2f}, "
                                f"duration={adverse_duration:.1f}s >= {persistence_s:.1f}s, remaining={remaining_s:.1f}s, TWAP={realized_twap or 0:.2f}); "
                                f"salvage bid {float(best_bid):.3f} >= {salvage_min:.2f} — triggering early capital preservation exit"
                            )
                            await self._exit_position_early(slot, market, float(best_bid), reason=exit_reason)
                            return
                        elif is_hard_flatten:
                            # Never ride to $0.00 in the final hard-flatten window.
                            force_px = max(0.01, float(best_bid) if best_bid is not None else 0.01)
                            logger.warning(
                                f"[{slot.asset.upper()} HARD_FLATTEN_EXPIRY] Spot {spot:.2f} crossed strike {strike:.2f} against {side} "
                                f"(adverse_dist={adverse_dist:.2f}, remaining={remaining_s:.1f}s); "
                                f"forcing flatten @ {force_px:.3f} (book illiquid / below salvage floor)"
                            )
                            await self._exit_position_early(slot, market, force_px, reason="hard_flatten_expiry")
                            return
                        else:
                            logger.info(
                                f"[{slot.asset.upper()} ADVERSE CONFIRMED] Spot {spot:.2f} crossed strike {strike:.2f} against {side} "
                                f"(dist={adverse_dist:.2f}, dur={adverse_duration:.1f}s, remaining={remaining_s:.1f}s), "
                                f"salvage bid {best_bid} < {salvage_min:.2f} (orderbook illiquid, waiting for fillable quote)"
                            )
                    else:
                        logger.info(
                            f"[{slot.asset.upper()} ADVERSE MONITORING] Spot {spot:.2f} adverse vs strike {strike:.2f} "
                            f"(dist={adverse_dist:.2f} >= min={min_adverse_usd:.2f}), "
                            f"duration={adverse_duration:.1f}s < {persistence_s:.1f}s hurdle (filtering momentary noise)"
                        )
                else:
                    logger.info(
                        f"[{slot.asset.upper()} ADVERSE UNCONFIRMED] Spot {spot:.2f} adverse vs strike {strike:.2f} "
                        f"(dist={adverse_dist:.2f} >= min={min_adverse_usd:.2f}), but TWAP has not confirmed and not catastrophic "
                        f"(dur={adverse_duration:.1f}s, continuing to monitor)"
                    )
        else:
            # Spot or strike is missing/ungrounded.
            # If in final 35s and orderbook has collapsed, flatten as fail-safe!
            if remaining_s <= 35.0:
                salvage_candidate = float(best_bid) if best_bid is not None else 0.0
                if salvage_candidate < 0.05 and opp_token_id:
                    opp_top = self.book_stream.get_top(opp_token_id) if hasattr(self, "book_stream") else None
                    opp_ask = opp_top.get("ask") if opp_top else None
                    if opp_ask is None:
                        bk_opp = self.pub_client.get_order_book(opp_token_id)
                        _, opp_ask = extract_best_bid_ask(bk_opp)
                    if opp_ask is not None and 0.0 < opp_ask < 1.0:
                        salvage_candidate = max(salvage_candidate, round(1.0 - opp_ask, 4))
                if 0.02 <= salvage_candidate <= 0.22 and salvage_candidate <= (entry * 0.45):
                    logger.warning(
                        f"[{slot.asset.upper()} FAILSAFE CRASH FLATTEN] remaining={remaining_s:.1f}s <= 35s (no spot), "
                        f"market bid {salvage_candidate:.3f} <= 0.22 (entry={entry:.3f}) — emergency salvage exit to prevent 0.00 expiry loss"
                    )
                    await self._exit_position_early(slot, market, salvage_candidate, reason="failsafe_orderbook_flatten")
                    return

        # 3) Legacy H-005 Stop-loss vs entry (mid only when spread is sane) - applies only to legacy H-005
        is_legacy_h005 = pos.get("strategy") in ("H-005_MOMENTUM", "H-005", "LATE_MOMENTUM", "MOMENTUM")
        if (
            is_legacy_h005
            and config.MOMENTUM_STOP_LOSS_ENABLED
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

        # 4) Legacy H-005 Exit before final seconds (avoid last-second oracle chop) - applies only to legacy H-005
        if is_legacy_h005 and remaining_s <= float(config.MOMENTUM_EXIT_BEFORE_S) and remaining_s > 0.5:
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

    @staticmethod
    def _maker_hold_to_expiry(pos: dict | None) -> bool:
        """True when MAKER_HOLD_TO_EXPIRY is on and the position is a maker-leg fill."""
        if not pos or not getattr(config, "MAKER_HOLD_TO_EXPIRY", False):
            return False
        return "MAKER" in str(pos.get("strategy") or "").upper()

    def _real_best_bid(self, token_id: str) -> float | None:
        """Best bid from the token's OWN book only (no 1-opp_ask complement inference)."""
        top = self.book_stream.get_top(token_id)
        bid = top.get("bid") if top else None
        if bid is None:
            try:
                bid, _ = extract_best_bid_ask(self.pub_client.get_order_book(token_id))
            except Exception:
                bid = None
        return float(bid) if bid is not None else None

    def _paper_sell_price(self, token_id: str, shares: float, best_bid: float) -> tuple[float, str]:
        """Paper exit price when selling `shares` into the real bid book: VWAP over bid levels.

        Unfilled remainder (book too thin) is marked at 0.01. Never better than the best bid.
        """
        try:
            book = self.pub_client.get_order_book(token_id)
        except Exception:
            book = None
        filled, vwap = fillable_bid_shares(book, shares)
        if filled <= 0:
            return float(best_bid), "ws_best_bid"
        px = (filled * vwap + max(0.0, shares - filled) * 0.01) / shares
        src = "book_vwap" if filled + 1e-9 >= shares else f"book_vwap_partial({filled:.2f}/{shares:.2f})"
        return round(min(float(best_bid), px), 4), src

    async def _check_price_stop(
        self, slot: CoinSlot, market: dict, pos: dict, token_id: str, entry: float,
        remaining_s: float, now: float | None,
    ) -> bool:
        """Bid-based max-loss stop. Returns True when the position was exited."""
        tag = slot.asset.upper()
        frac = float(getattr(config, "PRICE_STOP_FRACTION", 0.50))
        pers = float(getattr(config, "PRICE_STOP_PERSISTENCE_S", 5.0))
        thr = round(entry * frac, 4)
        now_ts = float(now) if now is not None else time.time()
        bid = self._real_best_bid(token_id)
        if bid is None or bid > thr:
            if pos.get("price_stop_start_ts") is not None:
                logger.info(
                    f"[{tag}] PRICE_STOP reset bid={(f'{bid:.3f}' if bid is not None else 'n/a')} > thr={thr:.3f} "
                    f"(entry={entry:.3f} x {frac:.2f})"
                )
                pos["price_stop_start_ts"] = None
            return False
        if pos.get("price_stop_start_ts") is None:
            pos["price_stop_start_ts"] = now_ts
            logger.info(
                f"[{tag}] PRICE_STOP armed bid={bid:.3f} <= thr={thr:.3f} (entry={entry:.3f} x {frac:.2f}), "
                f"need {pers:.1f}s persistence, remaining={remaining_s:.1f}s"
            )
        held = now_ts - float(pos["price_stop_start_ts"])
        if held + 1e-9 < pers:
            return False
        shares = float(pos.get("shares") or 0.0)
        if config.MODE == "live":
            exit_px, src = bid, "best_bid"
        else:
            exit_px, src = self._paper_sell_price(token_id, shares, bid)
        logger.warning(
            f"[{tag}] PRICE_STOP exit {pos.get('strategy')} {pos.get('side')} entry={entry:.3f} bid={bid:.3f} "
            f"<= thr={thr:.3f} for {held:.1f}s >= {pers:.1f}s | sell {shares:.2f} sh @ {exit_px:.3f} ({src}) "
            f"remaining={remaining_s:.1f}s"
        )
        journal_write(
            "price_stop", asset=slot.asset, slot=pos.get("slot"), order_id=pos.get("order_id"), strategy=pos.get("strategy"),
            side=pos.get("side"), entry=entry, bid=bid, threshold=thr, fraction=frac, held_s=round(held, 2),
            exit_price=exit_px, exit_price_source=src, shares=shares, remaining_s=round(remaining_s, 1),
        )
        await self._exit_position_early(slot, market, exit_px, reason="price_stop")
        return True

    async def _place_micro_hedge(
        self, slot: CoinSlot, market: dict, tokens: dict, pos: dict, main_ask: float
    ) -> None:
        """Buy a small opposite-side ticket when skew is extreme (Novals-style)."""
        hedge_side = "DOWN" if pos["side"] == "UP" else "UP"
        hedge_token = tokens.get(hedge_side)
        main_token = tokens.get(pos["side"])
        if not hedge_token:
            return

        top = self.book_stream.get_top(hedge_token, opp_token_id=main_token)
        hedge_ask = top.get("ask") if top else None
        if hedge_ask is None:
            bk = self.pub_client.get_order_book(hedge_token)
            bk_main = self.pub_client.get_order_book(main_token) if main_token else None
            _, r_ask, _ = extract_market_top(bk, bk_main)
            hedge_ask = r_ask
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
        if not pos or pos.get("exiting"):
            return
        pos["exiting"] = True

        shares = float(pos["shares"])
        cost = float(pos["cost_usd"])
        fee = float(pos.get("fee", 0.0))
        rebate = float(pos.get("rebate", 0.0))
        exit_price = float(exit_price)

        # Cancel any resting maker orders on this slot to prevent orphan fills (live: confirm + report)
        if slot.resting_order:
            ro = slot.resting_order
            if config.MODE == "live" and self.live_client and ro.get("order_id"):
                res = self.live_client.cancel_and_confirm(ro["order_id"])
                if not res.get("confirmed") or float(res.get("size_matched") or 0.0) > 0:
                    logger.error(
                        f"[{slot.asset.upper()}] Resting order {ro['order_id']} during early exit: "
                        f"cancel confirmed={res.get('confirmed')} matched={res.get('size_matched')} "
                        f"status={res.get('status')} - CHECK WALLET (extra shares not managed)"
                    )
                    self._record_orphan(slot.asset, ro, res, "early_exit")
            slot.resting_order = None

        # Paper / live flatten of main
        if config.MODE == "live" and self.live_client:
            try:
                # Realistic limit price allowing for 2¢ slippage during relayer latency
                min_price = max(0.10, round(exit_price - 0.02, 2)) if exit_price >= 0.12 else max(0.01, round(exit_price - 0.02, 2))
                # Use FAK (Fill-And-Kill / IOC) to sweep book liquidity without all-or-nothing rejection
                resp = self.live_client.place_order(
                    pos["token_id"], "SELL", min_price, shares, order_type="FAK"
                )
                if not resp or not resp.get("success"):
                    pos["exiting"] = False
                    logger.error(f"Live early exit SELL failed ({reason}), retaining position for retry")
                    return

                filled_shares = float(resp.get("shares", 0.0))
                if resp.get("avg_price"):
                    exit_price = float(resp["avg_price"])
                if not resp.get("filled") or filled_shares <= 0:
                    pos["exiting"] = False
                    logger.error(f"Live early exit SELL not filled ({reason}), retaining position for retry")
                    return

                if filled_shares < shares - 1e-4:
                    logger.warning(
                        f"Live early exit SELL partially filled: {filled_shares:.2f}/{shares:.2f} shares ({reason})"
                    )
                    fee_rate = float(
                        market.get("fee_rate")
                        if market.get("fee_rate") is not None
                        else getattr(config, "TAKER_FEE_RATE", 0.07)
                    )
                    exit_fee = fee_rate * exit_price * (1.0 - exit_price) * filled_shares
                    filled_cost = round(cost * (filled_shares / shares), 2)
                    entry_fee_portion = round(fee * (filled_shares / shares), 4)
                    partial_pnl = (filled_shares * exit_price) - filled_cost - entry_fee_portion - exit_fee
                    self.risk.record_trade_result(partial_pnl)

                    pos["shares"] = round(shares - filled_shares, 2)
                    pos["cost_usd"] = round(cost - filled_cost, 2)
                    pos["fee"] = round(fee - entry_fee_portion, 4)
                    pos["exiting"] = False
                    return  # keep remaining shares in slot.position for retry on next tick
            except Exception as e:
                pos["exiting"] = False
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

        fee_rate = float(
            market.get("fee_rate")
            if market.get("fee_rate") is not None
            else getattr(config, "TAKER_FEE_RATE", 0.07)
        )
        exit_fee = fee_rate * exit_price * (1.0 - exit_price) * shares if (config.MODE == "live" or fee > 0) else 0.0
        main_pnl = (shares * exit_price) - cost - fee - exit_fee + rebate

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
        logger.info(f"[{slot.asset.upper()} EARLY EXIT:{reason}] {market['slug']}")
        logger.info(
            f"   Main {pos['side']} entry={pos['price']:.3f} exit={exit_price:.3f} | "
            f"PnL ${pnl:+.2f} | Bankroll ${self.risk.bankroll:.2f}"
        )
        logger.info("=" * 60)

        now_ts = time.time()
        slot_start_ts = _extract_slot_start_ts(market, pos.get("slot")) or pos.get("slot_start_ts")
        exit_elapsed_s = max(0.0, min(300.0, round(now_ts - float(slot_start_ts), 1))) if slot_start_ts else None
        exit_window_time = _format_window_time(exit_elapsed_s)
        exit_time_utc = datetime.now(timezone.utc).isoformat()

        entry_time_utc = pos.get("entry_time_utc") or (
            datetime.fromtimestamp(pos["entry_time"], tz=timezone.utc).isoformat()
            if pos.get("entry_time") else None
        )
        entry_elapsed_s = pos.get("entry_elapsed_s")
        entry_window_time = pos.get("entry_window_time")
        if entry_elapsed_s is None and slot_start_ts and pos.get("entry_time"):
            try:
                entry_elapsed_s = max(0.0, min(300.0, round(float(pos["entry_time"]) - float(slot_start_ts), 1)))
                entry_window_time = _format_window_time(entry_elapsed_s)
            except Exception:
                pass

        total_cost = round(cost + fee + exit_fee, 4)

        record = {
            "timestamp": exit_time_utc,
            "asset": slot.asset,
            "slot": market["slug"],
            "strategy": pos.get("strategy", "H-005_MOMENTUM"),
            "side": pos["side"],
            "price": pos["price"],
            "exit_price": exit_price,
            "exit_reason": reason,
            "entry_time_utc": entry_time_utc,
            "exit_time_utc": exit_time_utc,
            "entry_elapsed_s": entry_elapsed_s,
            "entry_window_time": entry_window_time,
            "exit_elapsed_s": exit_elapsed_s,
            "exit_window_time": exit_window_time,
            "shares": shares,
            "cost_usd": cost,
            "fee": fee,
            "exit_fee": round(exit_fee, 4),
            "total_cost": total_cost,
            "rebate": rebate,
            "hedge_placed": bool(pos.get("hedge_placed")),
            "hedge_side": pos.get("hedge_side"),
            "hedge_cost_usd": pos.get("hedge_cost_usd", 0.0),
            "hedge_pnl": round(hedge_pnl, 2),
            "z_score": pos.get("z_score"),
            "strike": slot.strike,
            "won": pnl > 0,
            "pnl": round(pnl, 2),
            "bankroll": round(self.risk.bankroll, 2),
            "early_exit": True,
            "official": True,
            "settlement_source": f"early_exit_{reason}",
            "order_id": pos.get("order_id"),
            **{k: pos.get(k) for k in ("strike_spot", "strike_twap", "strike_twap_source", "move_ref_mode", "move_ref_used")},
            "maker_features": pos.get("maker_features"),
        }
        journal_write(
            "settlement",
            slot=record.get("slot"),
            asset=slot.asset,
            order_id=record.get("order_id"),
            strategy=record.get("strategy"),
            side=record.get("side"),
            outcome=f"EARLY_EXIT_{reason}",
            won=record.get("won"),
            pnl=record.get("pnl"),
            bankroll=record.get("bankroll"),
            settlement_source=record.get("settlement_source"),
            early_exit=True,
            exit_price=exit_price,
            exit_reason=reason,
            entry_time_utc=entry_time_utc,
            exit_time_utc=exit_time_utc,
            entry_elapsed_s=entry_elapsed_s,
            entry_window_time=entry_window_time,
            exit_elapsed_s=exit_elapsed_s,
            exit_window_time=exit_window_time,
            fee=round(fee + exit_fee, 4),
            total_cost=total_cost,
            jev=pos.get("jev"),
            **self._jev_shadow_tags(pos.get("jev")),
            **{k: pos.get(k) for k in ("strike_spot", "strike_twap", "strike_twap_source", "move_ref_mode", "move_ref_used")},
            maker_features=pos.get("maker_features"),
        )
        self._save_trade_record(record)

    # ------------------------------------------------------------------ live order helpers
    def _live_poll_order(self, ro: dict, now: float) -> dict | None:
        """Throttled exchange read of a resting order (status + size_matched). Live only."""
        if config.MODE != "live" or not self.live_client or not ro.get("order_id"):
            return None
        last = float(ro.get("_last_poll") or 0.0)
        if now - last < float(config.LIVE_ORDER_POLL_S) and ro.get("_last_state") is not None:
            return ro["_last_state"]
        state = self.live_client.get_order_state(ro["order_id"])
        ro["_last_poll"] = now
        if state is not None:
            ro["_last_state"] = state
            matched = float(state.get("size_matched") or 0.0)
            if matched > float(ro.get("live_matched") or 0.0) + 1e-9:
                logger.info(
                    f"[{str(ro.get('asset') or '').upper()}] Live order {ro['order_id']} matched "
                    f"{matched:.2f}/{float(ro['shares']):.2f} (status={state.get('status')})"
                )
            ro["live_matched"] = matched
        return state

    def _live_cancel_resting(self, tag: str, ro: dict, reason: str, known_matched: float = 0.0):
        """Cancel a live resting order and read it back.
        Returns (outcome, ro): "gone" (cancelled, nothing filled; ro None), "adopt" (some/all filled;
        ro carries the matched shares), "pending" (cancel not confirmed; ro kept with cancel_pending)."""
        if not self.live_client or not ro.get("order_id"):
            return "gone", None
        res = self.live_client.cancel_and_confirm(ro["order_id"])
        matched = max(float(res.get("size_matched") or 0.0), float(known_matched or 0.0))
        if not res.get("confirmed"):
            tries = int(ro.get("cancel_tries") or 0) + 1
            logger.warning(
                f"[{tag}] Live cancel NOT confirmed for {ro['order_id']} ({reason}); status={res.get('status')} "
                f"try {tries} - keeping order tracked and retrying"
            )
            return "pending", {**ro, "cancel_pending": True, "cancel_reason": reason, "cancel_tries": tries,
                               "live_matched": matched}
        if matched > 1e-9:
            logger.warning(
                f"[{tag}] Live order {ro['order_id']} filled {matched:.2f}/{float(ro['shares']):.2f} before cancel "
                f"({reason}); adopting as position"
            )
            return "adopt", {**ro, "shares": round(matched, 4), "cost_usd": round(matched * float(ro["price"]), 4),
                             "cancel_pending": False}
        logger.info(f"[{tag}] JEV_DYNAMIC cancel resting @{ro['price']:.2f} ({reason}) - confirmed, 0 filled")
        return "gone", None

    def _live_finalize_resting(self, slot: CoinSlot, reason: str) -> None:
        """Before forgetting a resting order at window end: live cancel + adopt any fill into slot.position."""
        ro = slot.resting_order
        if config.MODE != "live" or not ro or not ro.get("order_id"):
            return
        outcome, ro_after = "pending", ro
        for _ in range(3):
            outcome, ro_after = self._live_cancel_resting(
                slot.asset.upper(), ro_after or ro, reason, float((ro_after or ro).get("live_matched") or 0.0)
            )
            if outcome != "pending":
                break
        if outcome == "adopt" and not slot.position:
            slot.position = {
                "asset": slot.asset, "slot": ro_after["slot"], "strategy": "JEV_DYNAMIC_MAKER",
                "side": ro_after["side"], "token_id": ro_after["token_id"], "price": ro_after["price"],
                "shares": ro_after["shares"], "cost_usd": ro_after["cost_usd"], "fee": 0.0,
                "rebate": 0.0, "order_id": ro_after.get("order_id"), "entry_time": time.time(),
                "entry_time_utc": datetime.now(timezone.utc).isoformat(), "strike": slot.strike,
                "official": False, "adopted_at": reason,
            }
        elif outcome != "gone":
            self._record_orphan(slot.asset, ro_after or ro, {"status": outcome}, reason)
        slot.resting_order = None

    def _record_redeem_needed(self, record: dict, pos: dict) -> None:
        """Live winner held through settlement: tokens must be redeemed (claimed) on Polymarket."""
        path = config.STATE_DIR / "redeem_queue.json"
        try:
            rows = json.loads(path.read_text()) if path.exists() else []
        except Exception:
            rows = []
        rows.append({"ts": record.get("timestamp"), "slot": record.get("slot"), "side": record.get("side"),
                     "token_id": pos.get("token_id"), "shares": record.get("shares"), "redeemed": False})
        try:
            path.write_text(json.dumps(rows, indent=2, default=_json_default))
        except Exception as e:
            logger.error(f"Failed to write redeem_queue.json: {e}")
        logger.warning(
            f"LIVE REDEEM NEEDED: {record.get('slot')} {record.get('side')} x{record.get('shares')} won at settlement; "
            f"claim it on Polymarket (bot does not redeem on-chain). Logged to redeem_queue.json"
        )

    def _record_orphan(self, asset: str, ro: dict, res: dict, reason: str) -> None:
        """Persist an order we could not reconcile so a human can check the wallet."""
        path = config.STATE_DIR / "live_orphans.json"
        try:
            rows = json.loads(path.read_text()) if path.exists() else []
        except Exception:
            rows = []
        rows.append({"ts": datetime.now(timezone.utc).isoformat(), "asset": asset, "reason": reason,
                     "order_id": ro.get("order_id"), "slot": ro.get("slot"), "side": ro.get("side"),
                     "price": ro.get("price"), "shares": ro.get("shares"), "result": res})
        try:
            path.write_text(json.dumps(rows, indent=2, default=str))
        except Exception as e:
            logger.error(f"Failed to write live_orphans.json: {e}")
        logger.error(f"[{asset.upper()}] LIVE ORPHAN recorded ({reason}): order {ro.get('order_id')} - check wallet")

    def _load_pending(self) -> list[dict]:
        """Restore positions awaiting Polymarket resolution (survives redeploys)."""
        path = getattr(self, "_pending_path", None)
        if not path or not path.exists():
            return []
        try:
            rows = json.loads(path.read_text())
        except Exception as e:
            logger.error(f"pending_settlements.json unreadable ({e}); starting with an empty queue")
            return []
        rows = [r for r in rows if isinstance(r, dict) and (r.get("market") or {}).get("slug")]
        for r in rows:
            r.pop("next_check", None)
        if rows:
            logger.warning(
                f"Restored {len(rows)} pending settlement(s) from disk: "
                + ", ".join(str((r.get('market') or {}).get('slug')) for r in rows)
            )
        return rows

    def _save_pending(self) -> None:
        path = getattr(self, "_pending_path", None)
        if not path:
            return
        try:
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(self._pending_settles, default=_json_default))
            os.replace(tmp, path)
        except Exception as e:
            logger.error(f"Failed to persist pending settlements: {e}")

    def _refresh_live_bankroll(self, now: float, force: bool = False) -> None:
        """Live: bankroll = wallet USDC collateral (CLOB view). Only while flat unless forced."""
        if config.MODE != "live" or not self.live_client:
            return
        if not force:
            if now - self._last_bankroll_refresh < float(config.LIVE_BANKROLL_REFRESH_S):
                return
            if self._pending_settles or any(sl.position or sl.resting_order for sl in self.slots.values()):
                return
        self._last_bankroll_refresh = now
        bal = self.live_client.get_wallet_balance()
        if bal is None:
            if force:
                self.risk.bankroll = 0.0
                self.risk.save_state()
                logger.error("LIVE: wallet balance unreadable at startup - bankroll set to $0 (no new entries)")
            else:
                logger.warning("LIVE: wallet balance refresh failed; keeping last bankroll")
            return
        if abs(float(self.risk.bankroll) - bal) >= 0.01:
            logger.info(f"LIVE bankroll from wallet: ${bal:.2f} (was ${float(self.risk.bankroll):.2f})")
        self.risk.bankroll = round(max(0.0, bal - float(config.WALLET_RESERVE_USD)), 2)
        self.risk.save_state()

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
        self._save_pending()
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
            market = item.get("market") or {}
            slug = market.get("slug")
            tag = str(item.get("asset") or "").upper()
            end_ts = float(item.get("end_ts") or 0.0)
            if end_ts and now < end_ts + 2.0:
                still.append(item)
                continue
            next_check = item.get("next_check")
            if next_check and now < float(next_check):
                still.append(item)
                continue
            resolution = (
                self.pub_client.get_resolution(slug, now_ts=now, condition_id=market.get("condition_id"))
                if slug else None
            )
            waited = now - float(item.get("queued_at") or now)
            if resolution is None:
                if waited < config.RESOLUTION_TIMEOUT_S:
                    still.append(item)
                    continue
                if slug and waited < config.RESOLUTION_MAX_WAIT_S:
                    if not item.get("pending_warned"):
                        logger.warning(
                            f"[{tag}] Polymarket resolution still PENDING after {waited:.0f}s for {slug}; "
                            f"polling every {config.RESOLUTION_POLL_INTERVAL_S:.0f}s up to "
                            f"{config.RESOLUTION_MAX_WAIT_S:.0f}s before giving up"
                        )
                        item["pending_warned"] = True
                    item["next_check"] = now + float(config.RESOLUTION_POLL_INTERVAL_S)
                    still.append(item)
                    continue
                logger.error(
                    f"[{tag}] No Polymarket resolution after {waited:.0f}s for {slug}; recording unresolved "
                    f"(PnL not booked yet; will be re-checked and booked on next startup)"
                )
                self._record_unresolved(item)
                continue
            if item.get("pending_warned"):
                logger.info(
                    f"[{tag}] Late Polymarket resolution for {slug} after {waited:.0f}s "
                    f"({resolution.get('source')}): {resolution.get('side')} -> booking PnL now"
                )
            self._settle_filled(item, resolution)
        changed = [id(x) for x in still] != [id(x) for x in self._pending_settles] or any(
            x.get("pending_warned") and not x.get("_persisted_warned") for x in still
        )
        self._pending_settles = still
        if changed:
            for x in still:
                if x.get("pending_warned"):
                    x["_persisted_warned"] = True
            self._save_pending()

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
            "fee": pos.get("fee", 0.0),
            "rebate": pos.get("rebate", 0.0),
            "entry_time_utc": pos.get("entry_time_utc"),
            "entry_elapsed_s": pos.get("entry_elapsed_s"),
            "entry_window_time": pos.get("entry_window_time"),
            "maker_features": pos.get("maker_features"),
        }
        journal_write(
            "settlement",
            slot=market.get("slug"),
            asset=item.get("asset"),
            order_id=pos.get("order_id"),
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

        exit_price = 1.0 if won else 0.0
        exit_reason = "settlement_won" if won else "settlement_lost"
        exit_time_utc = datetime.now(timezone.utc).isoformat()
        exit_elapsed_s = 300.0
        exit_window_time = "t=300s (05:00 - expiry)"

        slot_start_ts = _extract_slot_start_ts(market, pos.get("slot")) or pos.get("slot_start_ts")
        entry_time_utc = pos.get("entry_time_utc") or (
            datetime.fromtimestamp(pos["entry_time"], tz=timezone.utc).isoformat()
            if pos.get("entry_time") else None
        )
        entry_elapsed_s = pos.get("entry_elapsed_s")
        entry_window_time = pos.get("entry_window_time")
        if entry_elapsed_s is None and slot_start_ts and pos.get("entry_time"):
            try:
                entry_elapsed_s = max(0.0, min(300.0, round(float(pos["entry_time"]) - float(slot_start_ts), 1)))
                entry_window_time = _format_window_time(entry_elapsed_s)
            except Exception:
                pass

        cost_usd = float(pos.get("cost_usd") or 0.0)
        total_cost = round(cost_usd + float(fee), 4)

        record = {
            "timestamp": exit_time_utc,
            "asset": asset,
            "slot": market.get("slug"),
            "strategy": strategy_name,
            "side": pos.get("side"),
            "price": pos.get("price"),
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "entry_time_utc": entry_time_utc,
            "exit_time_utc": exit_time_utc,
            "entry_elapsed_s": entry_elapsed_s,
            "entry_window_time": entry_window_time,
            "exit_elapsed_s": exit_elapsed_s,
            "exit_window_time": exit_window_time,
            "shares": pos.get("shares"),
            "cost_usd": cost_usd,
            "fee": fee,
            "total_cost": total_cost,
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
            **{k: pos.get(k) for k in ("strike_spot", "strike_twap", "strike_twap_source", "move_ref_mode", "move_ref_used")},
            "maker_features": pos.get("maker_features"),
        }
        if config.MODE == "live" and won:
            record["redeem_required"] = True
            self._record_redeem_needed(record, pos)
        journal_write(
            "settlement",
            slot=market.get("slug"),
            asset=asset,
            strategy=strategy_name,
            order_id=pos.get("order_id"),
            side=pos.get("side"),
            outcome=resolved_side,
            won=won,
            pnl=round(pnl, 2),
            bankroll=round(self.risk.bankroll, 2),
            settlement_source=resolution.get("source"),
            early_exit=False,
            exit_price=exit_price,
            exit_reason=exit_reason,
            entry_time_utc=entry_time_utc,
            exit_time_utc=exit_time_utc,
            entry_elapsed_s=entry_elapsed_s,
            entry_window_time=entry_window_time,
            exit_elapsed_s=exit_elapsed_s,
            exit_window_time=exit_window_time,
            fee=fee,
            total_cost=total_cost,
            jev=pos.get("jev"),
            **self._jev_shadow_tags(pos.get("jev")),
            **{k: pos.get(k) for k in ("strike_spot", "strike_twap", "strike_twap_source", "move_ref_mode", "move_ref_used")},
            maker_features=pos.get("maker_features"),
        )
        self._save_trade_record(record)

    def _repair_unresolved_records(self) -> dict:
        """Book official PnL for trades.json rows previously recorded as unresolved ($0, won=None).

        Idempotent: rows are only rewritten once Polymarket reports an official winner. Paper bankroll
        follows via _reconcile_paper_blotter; in live mode the PnL is applied to the risk bankroll.
        """
        summary = {"found": 0, "booked": 0, "wins": 0, "losses": 0, "pnl": 0.0, "still_unresolved": 0}
        if not self._trades_log_path.exists():
            return summary
        try:
            with open(self._trades_log_path, "r") as f:
                trades = json.load(f)
        except Exception as e:
            logger.error(f"Unresolved backfill: cannot read trades.json ({e}); skipping")
            return summary
        if not isinstance(trades, list):
            return summary
        changed = False
        for i, row in enumerate(trades):
            if not isinstance(row, dict):
                continue
            if row.get("settlement_source") != "unresolved" or row.get("won") is not None:
                continue
            summary["found"] += 1
            slug = row.get("slot") or row.get("slug")
            try:
                res = self.pub_client.get_resolution(slug, now_ts=time.time()) if slug else None
            except Exception:
                res = None
            if not res or not res.get("official") or res.get("side") not in ("UP", "DOWN"):
                summary["still_unresolved"] += 1
                logger.warning(f"Unresolved backfill: {slug} still has no official Polymarket result; left as-is")
                continue
            shares = float(row.get("shares") or 0.0)
            cost = float(row.get("cost_usd") or 0.0) or round(shares * float(row.get("price") or 0.0), 4)
            fee = float(row.get("fee") or 0.0)
            rebate = float(row.get("rebate") or 0.0)
            won = row.get("side") == res["side"]
            pnl = (shares - cost - fee + rebate) if won else (-cost - fee + rebate)
            exit_time = row.get("exit_time_utc") or row.get("timestamp")
            new = dict(row)
            new.update({
                "outcome": res["side"],
                "won": won,
                "pnl": round(pnl, 2),
                "exit_price": 1.0 if won else 0.0,
                "exit_reason": "settlement_won" if won else "settlement_lost",
                "exit_time_utc": exit_time,
                "exit_elapsed_s": 300.0,
                "exit_window_time": "t=300s (05:00 - expiry)",
                "cost_usd": cost,
                "total_cost": round(cost + fee, 4),
                "final_twap": res.get("final_price"),
                "strike": res.get("price_to_beat") if res.get("price_to_beat") is not None else row.get("strike"),
                "settlement_source": res.get("source"),
                "official": True,
                "early_exit": False,
                "backfilled": True,
                "backfilled_at": datetime.now(timezone.utc).isoformat(),
            })
            trades[i] = new
            changed = True
            summary["booked"] += 1
            summary["wins" if won else "losses"] += 1
            summary["pnl"] = round(summary["pnl"] + pnl, 2)
            if config.MODE == "live":
                self.risk.bankroll += pnl
            logger.info(
                f"Unresolved backfill: {slug} {row.get('side')} -> official {res['side']} "
                f"({'WON' if won else 'LOST'}) PnL ${pnl:+.2f} [{res.get('source')}]"
            )
        if changed:
            tmp = self._trades_log_path.with_suffix(".json.tmp")
            try:
                with open(tmp, "w") as f:
                    json.dump(trades, f, indent=2)
                os.replace(tmp, self._trades_log_path)
            except Exception as e:
                logger.error(f"Unresolved backfill: failed to write trades.json ({e}); nothing changed")
                return {**summary, "booked": 0}
            if config.MODE == "live":
                self.risk.save_state()
        if summary["found"]:
            logger.info(
                f"Unresolved backfill: {summary['found']} unresolved rows | booked {summary['booked']} "
                f"({summary['wins']}W / {summary['losses']}L, PnL ${summary['pnl']:+.2f}) | "
                f"still unresolved {summary['still_unresolved']}"
            )
        return summary

    @staticmethod
    def _strip_jev_fields(record: dict) -> dict:
        """Trade records carry no Jev blobs (jev, jev_shadow_*, any jev_* key)."""
        return {k: v for k, v in record.items() if not str(k).lower().startswith("jev")}

    def _save_trade_record(self, record: dict) -> None:
        record = self._strip_jev_fields(record)
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


def _trade_day(row: dict) -> int | None:
    """UTC day index (epoch // 86400) of a trade's exit/settlement time."""
    for key in ("exit_time_utc", "timestamp"):
        raw = row.get(key)
        if not raw:
            continue
        try:
            return int(datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp() // 86400)
        except Exception:
            continue
    return None


def _json_default(o):
    try:
        return float(o)
    except Exception:
        return str(o)


def main():
    bot = TWAPBot()

    def handle_sig(sig, frame):
        bot.stop()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    asyncio.run(bot.start())


if __name__ == "__main__":
    main()
