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
