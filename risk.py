"""Risk controls: position sizing, exposure caps, and circuit breakers.

Sizing uses fractional Kelly on the *executable* price (ask for buys), scaled by
model confidence, with an explicit fee buffer. Live mode typically uses a fixed
tiny stake (FIXED_STAKE_USD) so a bad streak cannot scale up.
"""
from __future__ import annotations
import config


class RiskManager:
    def __init__(self, bankroll: float):
        self.bankroll = bankroll
        self.starting_bankroll = bankroll
        self.open_exposure = 0.0
        self.realized_pnl_today = 0.0
        self.halted = False
        self.halt_reason = ""
        self.trades_today = 0
        self.consecutive_losses = 0
        self.last_skip_reason = ""

    def _daily_loss_limit(self) -> float:
        """Tighter of absolute $ cap and bankroll % (minimal risk)."""
        limit = config.DAILY_LOSS_LIMIT_USD
        pct = getattr(config, "DAILY_LOSS_LIMIT_PCT", 0.0) or 0.0
        if pct > 0:
            pct_limit = self.starting_bankroll * pct
            # Use the tighter (smaller) limit so large bankrolls don't overspend.
            limit = min(limit, pct_limit) if limit > 0 else pct_limit
        return max(limit, 0.0)

    def daily_loss_ok(self) -> bool:
        if config.MODE != "live":
            return True
        limit = self._daily_loss_limit()
        if limit > 0 and self.realized_pnl_today <= -limit:
            self.halted = True
            self.halt_reason = f"daily loss limit hit (${limit:.2f})"
            return False
        return True

    def max_trades_ok(self) -> bool:
        cap = getattr(config, "MAX_TRADES_PER_DAY", 0) or 0
        if cap > 0 and self.trades_today >= cap:
            self.halted = True
            self.halt_reason = "max trades per day hit"
            return False
        return True

    def consecutive_loss_ok(self) -> bool:
        if getattr(config, "MODE", "paper") != "live":
            return True
        cap = getattr(config, "MAX_CONSECUTIVE_LOSSES", 0) or 0
        if cap > 0 and self.consecutive_losses >= cap:
            self.halted = True
            self.halt_reason = f"{cap} consecutive losses"
            return False
        return True

    def size_trade(
        self,
        model_prob: float,
        market_price: float,
        confidence: float = 1.0,
    ) -> float:
        """Return USD to stake. 0 means skip (see last_skip_reason).

        Binary Kelly for buying a contract at price p with win-prob q:
          f* = (q - p) / (1 - p)
        We subtract FEE_BUFFER from edge, haircut by confidence, then either
        use FIXED_STAKE_USD or a capped Kelly stake. We never *force* a stake
        up to BASE when Kelly says smaller — that was a hidden risk leak.

        FIXED_STAKE path caps only by bankroll remaining and open exposure —
        not by the Kelly 5%-of-bankroll rule (that was zeroing $1 bets on
        small UI bankrolls like $5).
        """
        self.last_skip_reason = ""

        if self.halted:
            self.last_skip_reason = self.halt_reason or "halted"
            return 0.0
        if not self.daily_loss_ok():
            self.last_skip_reason = self.halt_reason or "daily loss limit"
            return 0.0
        if not self.max_trades_ok():
            self.last_skip_reason = self.halt_reason or "max trades/day"
            return 0.0
        if not self.consecutive_loss_ok():
            self.last_skip_reason = self.halt_reason or "consecutive losses"
            return 0.0
        if market_price <= 0.0 or market_price >= 1.0:
            self.last_skip_reason = f"bad price {market_price:.3f}"
            return 0.0

        # Entry band: avoid buying expensive favorites (asymmetric payoff kills EV).
        min_px = getattr(config, "MIN_ENTRY_PRICE", 0.0) or 0.0
        max_px = getattr(config, "MAX_ENTRY_PRICE", 1.0) or 1.0
        if market_price < min_px or market_price > max_px:
            self.last_skip_reason = (
                f"ask {market_price:.3f} outside band [{min_px:.2f},{max_px:.2f}]"
            )
            return 0.0

        conf = max(0.0, min(1.0, confidence))
        min_conf = getattr(config, "MIN_CONFIDENCE", 0.0) or 0.0
        if conf < min_conf:
            self.last_skip_reason = f"conf {conf:.2f} < MIN_CONFIDENCE {min_conf:.2f}"
            return 0.0

        # Haircut model prob toward 0.5 by (1 - conf).
        adj_prob = 0.5 + (model_prob - 0.5) * conf
        fee = getattr(config, "FEE_BUFFER", 0.0) or 0.0
        # Effective edge after fees / adverse selection buffer.
        edge = adj_prob - market_price - fee
        min_edge = config.EDGE_THRESHOLD
        if edge < min_edge:
            self.last_skip_reason = f"edge {edge:+.3f} < EDGE_THRESHOLD {min_edge:.3f}"
            return 0.0

        # EV on $1 of contracts bought at market_price, after fee buffer on prob.
        # Treat fee as lowering win-prob for EV: (adj_prob - fee) / price - 1
        adj_for_ev = max(0.0, adj_prob - fee)
        min_ev = getattr(config, "MIN_EV", 0.0) or 0.0
        ev = adj_for_ev / market_price - 1.0
        if min_ev > 0 and ev < min_ev:
            self.last_skip_reason = f"ev {ev:+.3f} < MIN_EV {min_ev:.3f}"
            return 0.0

        # Late-scalp: book-driven favorite; skip model edge/EV (already gated above
        # only if EDGE/MIN_EV > 0 — profile sets them to 0).
        fixed = getattr(config, "FIXED_STAKE_USD", 0.0) or 0.0
        if fixed > 0:
            stake = float(fixed)
        else:
            kelly = edge / max(1.0 - market_price, 1e-6)
            kelly = max(0.0, kelly)
            stake = self.bankroll * config.KELLY_FRACTION * kelly * conf
            # Cap only — do NOT force stake up to BASE (that overbets weak edges).
            stake = min(stake, config.MAX_TRADE_USD)
            if stake < config.BASE_TRADE_USD:
                self.last_skip_reason = (
                    f"kelly stake ${stake:.2f} < BASE ${config.BASE_TRADE_USD:.2f}"
                )
                return 0.0

        if self.open_exposure + stake > config.MAX_EXPOSURE_USD:
            stake = max(0.0, config.MAX_EXPOSURE_USD - self.open_exposure)
        if stake < 0.5:
            self.last_skip_reason = (
                f"exposure full (open ${self.open_exposure:.2f} / "
                f"max ${config.MAX_EXPOSURE_USD:.2f})"
            )
            return 0.0

        if fixed > 0:
            # Fixed stake: allow the configured bet as long as bankroll covers it.
            # Do NOT apply the Kelly 5%-of-bankroll haircut (zeros $1 bets on $5 bankroll).
            if self.bankroll < 0.5:
                self.last_skip_reason = f"bankroll ${self.bankroll:.2f} too low"
                return 0.0
            stake = min(stake, self.bankroll)
            if stake < 0.5:
                self.last_skip_reason = f"bankroll ${self.bankroll:.2f} below min stake"
                return 0.0
            stake = self._apply_min_shares(round(stake, 2), market_price)
            return self._cap_mid_stake(stake, market_price)

        # Kelly path: never stake more than a small fraction of remaining bankroll.
        stake = min(stake, max(0.0, self.bankroll * 0.05))
        if stake < 0.5:
            self.last_skip_reason = (
                f"kelly after 5% bankroll cap ${stake:.2f} < $0.50 "
                f"(bankroll ${self.bankroll:.2f})"
            )
            return 0.0
        stake = self._apply_min_shares(round(stake, 2), market_price)
        return self._cap_mid_stake(stake, market_price)

    def _cap_mid_stake(self, stake: float, market_price: float) -> float:
        """Optional mid max-stake after min-share bump (limits loss size)."""
        if stake <= 0:
            return 0.0
        # Only apply when mid-style band (not late favorites ~0.9+).
        if market_price >= 0.85:
            return stake
        max_mid = float(getattr(config, "MID_MAX_STAKE_USD", 0) or 0)
        if max_mid > 0 and stake > max_mid + 1e-9:
            self.last_skip_reason = (
                f"mid stake ${stake:.2f} > MID_MAX_STAKE_USD ${max_mid:.2f}"
            )
            return 0.0
        return stake

    def _apply_min_shares(self, stake: float, market_price: float) -> float:
        """Bump stake so shares >= MIN_ORDER_SHARES (Polymarket CLOB ~5 share min)."""
        if not getattr(config, "ENFORCE_MIN_SHARES", True):
            return stake
        min_sh = float(getattr(config, "MIN_ORDER_SHARES", 0) or 0)
        if min_sh <= 0 or market_price <= 0:
            return stake
        need_usd = round(min_sh * float(market_price), 2)
        if stake + 1e-9 >= need_usd:
            return stake
        # Allow exceeding MAX_TRADE_USD so base $1 still works on high favorites
        # when venue needs 5 shares (~$4.75 at 0.95). Cap by bankroll.
        room = max(0.0, self.bankroll - self.open_exposure)
        if room + 1e-9 < need_usd:
            self.last_skip_reason = (
                f"need ${need_usd:.2f} for {min_sh:.0f} shares @ {market_price:.2f} "
                f"but only ${room:.2f} available"
            )
            return 0.0
        print(f"  [size] min {min_sh:.0f} shares @ {market_price:.3f} -> "
              f"stake ${stake:.2f} -> ${need_usd:.2f}")
        return round(need_usd, 2)

    def size_late_scalp(self, market_price: float, edge: float = 0.0) -> float:
        """Smart stake for late-favorite mode with optional edge-based scaling.

        When edge > 0, scales stake proportionally (higher edge = larger bet).
        Still bumps to CLOB min shares when required and refuses trades that
        would exceed LATE_SCALP_MAX_STAKE_USD after bump (caps loss size).
        """
        self.last_skip_reason = ""
        if self.halted:
            self.last_skip_reason = self.halt_reason or "halted"
            return 0.0
        if not self.daily_loss_ok():
            self.last_skip_reason = self.halt_reason or "daily loss limit"
            return 0.0
        if not self.max_trades_ok():
            self.last_skip_reason = self.halt_reason or "max trades/day"
            return 0.0
        if not self.consecutive_loss_ok():
            self.last_skip_reason = self.halt_reason or "consecutive losses"
            return 0.0
        if market_price <= 0.0 or market_price >= 1.0:
            self.last_skip_reason = f"bad price {market_price:.3f}"
            return 0.0
        min_px = getattr(config, "MIN_ENTRY_PRICE", 0.0) or 0.0
        max_px = getattr(config, "MAX_ENTRY_PRICE", 1.0) or 1.0
        if market_price < min_px or market_price > max_px:
            self.last_skip_reason = (
                f"ask {market_price:.3f} outside band [{min_px:.2f},{max_px:.2f}]"
            )
            return 0.0
        fixed = getattr(config, "FIXED_STAKE_USD", 0.0) or 0.0
        use_edge_sizing = getattr(config, "LATE_SCALP_USE_EDGE_SIZING", True)
        if fixed > 0 and not use_edge_sizing:
            stake = float(fixed)
        elif edge > 0:
            # Edge-scaled sizing: higher edge → larger stake (capped).
            # At edge=0.05 with bankroll=$50, kelly_frac=0.10 → ~$0.25 base
            # but min-shares bump will bring it to venue floor anyway.
            kelly_frac = getattr(config, "KELLY_FRACTION", 0.10) or 0.10
            win_per_share = 1.0 - market_price
            if win_per_share > 0:
                kelly_f = kelly_frac * (edge / market_price)
                stake = max(float(config.BASE_TRADE_USD),
                            round(self.bankroll * kelly_f, 2))
            else:
                stake = float(config.BASE_TRADE_USD)
        else:
            stake = float(config.BASE_TRADE_USD)

        # Cap base stake to max trade size / late max stake first before min shares bump.
        # This prevents skipping a trade just because Kelly sizing calculates a size > max_late.
        max_late = float(getattr(config, "LATE_SCALP_MAX_STAKE_USD", 0) or 0)
        max_allowed = max_late if max_late > 0 else float(config.MAX_TRADE_USD)
        if stake > max_allowed:
            stake = max_allowed

        if self.bankroll < 0.5:
            self.last_skip_reason = f"bankroll ${self.bankroll:.2f} too low"
            return 0.0
        stake = min(stake, self.bankroll)
        if stake < 0.5:
            self.last_skip_reason = "stake below dust"
            return 0.0
        stake = self._apply_min_shares(round(stake, 2), market_price)
        if stake <= 0:
            return 0.0
        # Hard cap on late loss size (session R13-style -$4.45 blowups).
        if max_late > 0 and stake > max_late + 1e-9:
            self.last_skip_reason = (
                f"late stake ${stake:.2f} > LATE_SCALP_MAX_STAKE_USD "
                f"${max_late:.2f} (skip to limit loss size)"
            )
            return 0.0
        # Extra safety: never risk more than MAX_EXPOSURE_USD * 1.7 on late
        # (min-shares can exceed MAX_EXPOSURE slightly; keep a hard ceiling).
        # Counts OPEN exposure too — trades now settle in the background, so
        # without this a new late bet could stack on an unresolved one.
        exp_cap = float(getattr(config, "MAX_EXPOSURE_USD", 0) or 0)
        if exp_cap > 0:
            hard = max(exp_cap * 1.7, max_late if max_late > 0 else exp_cap)
            if self.open_exposure + stake > hard + 1e-9:
                self.last_skip_reason = (
                    f"open ${self.open_exposure:.2f} + stake ${stake:.2f} "
                    f"> exposure hard cap ${hard:.2f}"
                )
                return 0.0
        return stake

    def on_open(self, stake: float):
        self.open_exposure += stake
        self.trades_today += 1

    def on_close(self, stake: float, pnl: float):
        self.open_exposure = max(0.0, self.open_exposure - stake)
        self.bankroll += pnl
        self.realized_pnl_today += pnl
        if pnl < 0:
            self.consecutive_losses += 1
            self.consecutive_loss_ok()
        elif pnl > 0:
            self.consecutive_losses = 0
