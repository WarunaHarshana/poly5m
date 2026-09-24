"""Conservative momentum move reference.

Polymarket 5m Up/Down markets settle on Chainlink's 60s-TWAP stream (finalPrice vs priceToBeat),
while the bot's historical strike is the raw Chainlink spot tick at slot open. When the two
references disagree, the momentum entry gate measures the move against both and uses the
smaller magnitude (or 0 when they disagree on direction).

Pure helpers only; no trading side effects.
"""
from __future__ import annotations

import math
from typing import Any

MODE_SPOT = "spot"
MODE_CONSERVATIVE = "conservative"

SOURCE_GAMMA = "gamma_price_to_beat"
SOURCE_CHAINLINK_TWAP = "chainlink_twap"
SOURCE_UNAVAILABLE = "unavailable"

REF_SPOT_MODE = "spot_mode"          # MOMENTUM_MOVE_REF=spot (legacy behaviour)
REF_SPOT_FALLBACK = "spot_fallback"  # conservative requested but TWAP strike unavailable
REF_SPOT = "spot"                    # conservative: spot-strike move was the smaller one
REF_TWAP = "twap"                    # conservative: TWAP-strike move was the smaller one
REF_DIRECTION_CONFLICT = "direction_conflict"  # strikes disagree on direction -> move 0


def normalize_mode(value: Any) -> str:
    v = str(value or "").strip().lower()
    return v if v in (MODE_SPOT, MODE_CONSERVATIVE) else MODE_CONSERVATIVE


def _positive(x: Any) -> float | None:
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f) or f <= 0:
        return None
    return f


def estimate_twap_strike(
    cl_stream: Any,
    slot_start_ts: float | None,
    window_s: float,
    asset: str | None = None,
    price_to_beat: Any = None,
    min_ticks: int = 3,
    edge_tolerance_s: float = 5.0,
) -> tuple[float | None, str]:
    """Return (twap_strike, source).

    Priority: (1) an already-known official priceToBeat; (2) average of Chainlink ticks over
    [slot_start - window_s, slot_start], only if the local tick history actually covers that
    window (earliest tick <= window start + tolerance and >= min_ticks ticks inside it).
    """
    ptb = _positive(price_to_beat)
    if ptb is not None:
        return ptb, SOURCE_GAMMA
    if cl_stream is None or not slot_start_ts or float(window_s) <= 0:
        return None, SOURCE_UNAVAILABLE
    end = float(slot_start_ts)
    start = end - float(window_s)
    snap_fn = getattr(cl_stream, "history_snapshot", None)
    if snap_fn is None:
        return None, SOURCE_UNAVAILABLE
    try:
        ticks = snap_fn(asset)
    except Exception:
        return None, SOURCE_UNAVAILABLE
    if not ticks:
        return None, SOURCE_UNAVAILABLE
    earliest = min(ts for ts, _ in ticks)
    in_window = [p for ts, p in ticks if start <= ts <= end]
    if earliest > start + float(edge_tolerance_s) or len(in_window) < int(min_ticks):
        return None, SOURCE_UNAVAILABLE
    try:
        twap, _ = cl_stream.compute_realized_twap(start_ts=start, end_ts=end, asset=asset)
    except Exception:
        return None, SOURCE_UNAVAILABLE
    twap = _positive(twap)
    if twap is None:
        return None, SOURCE_UNAVAILABLE
    return twap, SOURCE_CHAINLINK_TWAP


def conservative_move(spot: float, strike_spot: float, strike_twap: float | None) -> tuple[float, str]:
    """Signed move using the smaller-magnitude reference; 0 when the two disagree on direction."""
    m_spot = float(spot) - float(strike_spot)
    if strike_twap is None:
        return m_spot, REF_SPOT_FALLBACK
    m_twap = float(spot) - float(strike_twap)
    if m_spot == 0.0 or m_twap == 0.0 or (m_spot > 0) != (m_twap > 0):
        return 0.0, REF_DIRECTION_CONFLICT
    if abs(m_twap) < abs(m_spot):
        return m_twap, REF_TWAP
    return m_spot, REF_SPOT


def entry_move(mode: Any, spot: float, strike_spot: float, strike_twap: float | None) -> tuple[float, str]:
    """Move used by the momentum entry gate for the given MOMENTUM_MOVE_REF mode."""
    if normalize_mode(mode) == MODE_SPOT:
        return float(spot) - float(strike_spot), REF_SPOT_MODE
    return conservative_move(spot, strike_spot, strike_twap)


def gate_move_ok(move: float, min_move: float, side: str | None = None) -> bool:
    """abs(move) >= min_move, and (if side given) the move points in the signal's direction."""
    if abs(float(move)) < float(min_move):
        return False
    if side is None:
        return True
    if float(move) == 0.0:
        return False
    return (float(move) > 0) == (str(side).upper() == "UP")
