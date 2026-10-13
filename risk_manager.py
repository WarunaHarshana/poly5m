import os
import json
import logging
import time
from pathlib import Path

import config

logger = logging.getLogger("risk")


class RiskManager:
    """Enforces bankroll preservation, daily loss limits, and consecutive loss cooldowns."""

    def __init__(self, state_dir: Path = config.STATE_DIR):
        self.state_dir = state_dir
        self.bankroll = config.STARTING_BANKROLL
        self.daily_pnl = 0.0
        self.consecutive_losses = 0
        self.trades_today = 0
        self.day_timestamp = int(time.time() // 86400)
        self.load_state()

    def _check_day_rollover(self) -> None:
        current_day = int(time.time() // 86400)
        if current_day != self.day_timestamp:
            self.day_timestamp = current_day
            self.daily_pnl = 0.0
            self.consecutive_losses = 0
            self.trades_today = 0
            self.save_state()

    def _enforce_live_kill_switches(self) -> bool:
        """Daily-loss / consecutive-loss caps are live-only; paper must keep filling."""
        return str(config.MODE).strip().lower() == "live"

    def can_trade(self, open_notional: float = 0.0) -> tuple[bool, str]:
        """Check if trading is allowed under current risk limits."""
        self._check_day_rollover()

        if self._enforce_live_kill_switches():
            # Daily loss limit check (live only)
            if self.daily_pnl <= -config.DAILY_LOSS_LIMIT_USD:
                return False, f"daily loss limit hit (-${abs(self.daily_pnl):.2f} <= -${config.DAILY_LOSS_LIMIT_USD:.2f})"

            # Exposure-aware: today's realized PnL minus capital already at risk minus this trade's
            # worst case must stay within the daily limit (a held 5-min position can lose its full cost).
            worst = float(self.daily_pnl) - max(0.0, float(open_notional)) - float(config.BASE_STAKE_USD)
            if worst < -config.DAILY_LOSS_LIMIT_USD - 1e-9:
                return False, (
                    f"daily loss limit (exposure-aware): ${float(self.daily_pnl):+.2f} realized, "
                    f"${float(open_notional):.2f} open, ${config.BASE_STAKE_USD:.2f} new > "
                    f"-${config.DAILY_LOSS_LIMIT_USD:.2f}"
                )

            # Consecutive loss check (live only)
            if self.consecutive_losses >= config.MAX_CONSECUTIVE_LOSSES:
                return False, f"hit {self.consecutive_losses} consecutive losses (max {config.MAX_CONSECUTIVE_LOSSES})"

        free = float(self.bankroll) - max(0.0, float(open_notional))
        if free < config.BASE_STAKE_USD:
            return False, (
                f"insufficient free bankroll (${free:.2f} < ${config.BASE_STAKE_USD:.2f}; "
                f"open ${float(open_notional):.2f})"
            )

        return True, ""

    def calculate_stake(self, edge: float, price: float, open_notional: float = 0.0) -> float:
        """Calculate dollar stake for trade."""
        free = max(0.0, float(self.bankroll) - max(0.0, float(open_notional)))
        stake = config.BASE_STAKE_USD
        # Maintain uniform base stake (Jev advisor manages downsizing to 50% when needed)
        # Ensure bankroll constraint against capital already tied in open positions
        stake = min(stake, free * 0.20, free)
        if stake < config.BASE_STAKE_USD:
            return round(max(0.0, min(free, config.BASE_STAKE_USD)), 2) if free >= config.BASE_STAKE_USD else 0.0
        return round(stake, 2)

    def record_trade_result(self, pnl: float) -> None:
        """Update risk metrics upon trade settlement."""
        self._check_day_rollover()
        self.bankroll += pnl
        self.daily_pnl += pnl
        self.trades_today += 1

        if pnl < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0

        self.save_state()

    def save_state(self) -> None:
        state = {
            "bankroll": round(self.bankroll, 2),
            "daily_pnl": round(self.daily_pnl, 2),
            "consecutive_losses": self.consecutive_losses,
            "trades_today": self.trades_today,
            "day_timestamp": self.day_timestamp,
            "updated_at": time.time(),
        }
        try:
            with open(self.state_dir / "risk_state.json", "w") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save risk state: {e}")

    def load_state(self) -> None:
        reset_flag = os.getenv("RESET_RISK_STATE", "false").strip().lower() in ("true", "1", "yes")
        if reset_flag:
            self.bankroll = config.STARTING_BANKROLL
            self.daily_pnl = 0.0
            self.consecutive_losses = 0
            self.trades_today = 0
            self.save_state()
            logger.info(f"Risk state cleanly reset via RESET_RISK_STATE (Bankroll: ${self.bankroll:.2f}).")
            return

        path = self.state_dir / "risk_state.json"
        current_day = int(time.time() // 86400)
        if path.exists():
            try:
                with open(path, "r") as f:
                    state = json.load(f)
                    self.bankroll = float(state.get("bankroll", self.bankroll))
                    if state.get("day_timestamp") == current_day:
                        self.daily_pnl = float(state.get("daily_pnl", 0.0))
                        self.consecutive_losses = int(state.get("consecutive_losses", 0))
                        self.trades_today = int(state.get("trades_today", 0))
                    else:
                        self.daily_pnl = 0.0
                        self.consecutive_losses = 0
                        self.trades_today = 0
            except Exception as e:
                logger.error(f"Failed to load risk state: {e}")



def cap_order_shares(shares: float, price: float) -> float | None:
    """Apply MAX_TRADE_USD: returns shares (possibly reduced, floored to whole shares) or None to skip.
    Never returns a size whose cost exceeds the cap or that is below MIN_ORDER_SHARES."""
    cap = float(getattr(config, "MAX_TRADE_USD", 0.0) or 0.0)
    if cap <= 0 or not price or price <= 0:
        return shares
    min_sh = float(config.MIN_ORDER_SHARES)
    if min_sh * float(price) > cap + 1e-9:
        return None
    if float(shares) * float(price) > cap + 1e-9:
        import math
        shares = float(math.floor(cap / float(price) + 1e-9))
        if shares < min_sh or shares * float(price) > cap + 1e-9:
            return None
    return shares
