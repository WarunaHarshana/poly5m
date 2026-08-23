"""Comprehensive Quantitative Research across all datasets in data/.

Focus: Non-latency-arbitrage, structurally sound, falsifiable hypotheses
suitable for realistic infrastructure (single-sided taker, directional mispricing,
book lag, session regimes, favorite-underdog bias).
"""
import glob
import json
import math
import os
from collections import defaultdict
import numpy as np

def load_all_data():
    obs_files = glob.glob("data/*/archive/observations.jsonl")
    print(f"Found {len(obs_files)} observations.jsonl files across data/")

    all_obs = {}  # slug/end_ts -> latest record
    for fpath in obs_files:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    slug = row.get("slug") or str(row.get("end_ts"))
                    res = row.get("outcome") or row.get("resolution") or row.get("resolved")
                    # Keep the record with resolution if possible
                    if slug not in all_obs or (res and not (all_obs[slug].get("outcome") or all_obs[slug].get("resolution"))):
                        all_obs[slug] = row
        except Exception as e:
            print(f"Error reading {fpath}: {e}")

    print(f"Total unique slots loaded: {len(all_obs)}")
    return list(all_obs.values())

def run_analysis():
    records = load_all_data()
    
    labeled = []
    for r in records:
        res = r.get("outcome") or r.get("resolution") or r.get("resolved")
        if res in ("UP", "DOWN"):
            # If move_usd is None, compute from spot - strike
            spot = r.get("spot")
            strike = r.get("strike")
            if spot is not None and strike is not None:
                r["calc_move"] = spot - strike
                r["calc_abs_move"] = abs(spot - strike)
                r["calc_side"] = "UP" if (spot - strike) >= 0 else "DOWN"
            labeled.append(r)

    print(f"Total labeled slots: {len(labeled)}")
    if not labeled:
        return

    # 1. Order Book Asking Price vs Win Rate (Favorite-Underdog Bias)
    print("\n" + "="*80)
    print("ANALYSIS 1: ORDER BOOK ASKS VS REALIZED WIN RATE")
    print("="*80)
    
    ask_buckets = [(0.10, 0.30), (0.30, 0.50), (0.50, 0.70), (0.70, 0.80), (0.80, 0.85), (0.85, 0.90), (0.90, 0.95), (0.95, 1.00)]
    for low, high in ask_buckets:
        n_tot = 0
        n_win = 0
        for r in labeled:
            res = r.get("outcome")
            ask = r.get("ask")
            side = r.get("side") or r.get("calc_side")
            if ask is not None and low <= ask < high:
                n_tot += 1
                if side == res:
                    n_win += 1
        if n_tot > 0:
            wr = (n_win / n_tot) * 100
            mid = ((low + high) / 2) * 100
            print(f"Ask [{low:.2f} - {high:.2f}): N = {n_tot:4d} | Realized WinRate = {wr:6.2f}% | Market Implied = {mid:5.1f}% | Diff = {wr-mid:+5.2f}%")

    # 2. Macro Realized Volatility Regimes
    print("\n" + "="*80)
    print("ANALYSIS 2: DAILY REALIZED VOLATILITY REGIMES & PREDICTABILITY")
    print("="*80)
    vol_buckets = [(0.0, 0.010), (0.010, 0.015), (0.015, 0.020), (0.020, 0.050)]
    for v_low, v_high in vol_buckets:
        n_tot = 0
        n_win = 0
        moves = []
        chops = []
        for r in labeled:
            vol = r.get("realized_vol_daily")
            move = r.get("calc_abs_move")
            chop = r.get("chop_crosses")
            side = r.get("calc_side")
            res = r.get("outcome")
            if vol is not None and v_low <= vol < v_high:
                if move is not None:
                    moves.append(move)
                if chop is not None:
                    chops.append(chop)
                if side and res:
                    n_tot += 1
                    if side == res:
                        n_win += 1
        if n_tot > 0:
            wr = (n_win / n_tot) * 100
            avg_m = np.mean(moves) if moves else 0
            avg_c = np.mean(chops) if chops else 0
            print(f"Daily Vol [{v_low*100:4.1f}% - {v_high*100:4.1f}%): N = {n_tot:4d} | AvgMove = ${avg_m:5.1f} | AvgChop = {avg_c:4.1f} | Directional Accuracy = {wr:6.2f}%")

    # 3. Time-of-Day / Trading Session Effects (UTC Hours)
    print("\n" + "="*80)
    print("ANALYSIS 3: TRADING SESSION REGIMES (UTC)")
    print("="*80)
    session_buckets = {
        "Asian Session (00:00 - 08:00 UTC)": (0, 8),
        "London Opening (08:00 - 12:00 UTC)": (8, 12),
        "US-Europe Overlap (12:00 - 16:00 UTC)": (12, 16),
        "US Afternoon (16:00 - 21:00 UTC)": (16, 21),
        "Pacific / Low Liquidity (21:00 - 24:00 UTC)": (21, 24),
    }
    for s_name, (h_start, h_end) in session_buckets.items():
        moves = []
        chops = []
        n_tot = 0
        n_win_lead = 0
        n_large_move = 0
        for r in labeled:
            end_ts = r.get("end_ts")
            if not end_ts:
                continue
            utc_hour = (end_ts % 86400) // 3600
            if h_start <= utc_hour < h_end:
                move = r.get("calc_abs_move")
                chop = r.get("chop_crosses")
                side = r.get("calc_side")
                res = r.get("outcome")
                if move is not None:
                    moves.append(move)
                    if move >= 10.0:
                        n_large_move += 1
                if chop is not None:
                    chops.append(chop)
                if side and res:
                    n_tot += 1
                    if side == res:
                        n_win_lead += 1
        if moves:
            avg_m = np.mean(moves)
            med_m = np.median(moves)
            avg_c = np.mean(chops) if chops else 0
            wr = (n_win_lead / n_tot * 100) if n_tot else 0
            pct_large = (n_large_move / len(moves)) * 100
            print(f"{s_name:44s} | N={n_tot:4d} | AvgMove=${avg_m:5.1f} | MedMove=${med_m:5.1f} | % Move>=$10: {pct_large:5.1f}% | AvgChop={avg_c:4.1f} | WinRate={wr:5.1f}%")

    # 4. Strike Source Reliability: Chainlink Data Stream vs Exchange Proxy
    print("\n" + "="*80)
    print("ANALYSIS 4: STRIKE SOURCE RELIABILITY & SETTLEMENT SKEW")
    print("="*80)
    for src in ["chainlink-data-stream", "exchange-proxy", None]:
        n_tot = 0
        n_win = 0
        moves = []
        for r in labeled:
            s_src = r.get("strike_src")
            if s_src == src:
                move = r.get("calc_abs_move")
                side = r.get("calc_side")
                res = r.get("outcome")
                if move is not None and move >= 10.0:
                    moves.append(move)
                    n_tot += 1
                    if side == res:
                        n_win += 1
        if n_tot > 0:
            wr = (n_win / n_tot) * 100
            print(f"Strike Source: {str(src):24s} | N(Move>=$10) = {n_tot:4d} | WinRate = {wr:6.2f}%")

    # 5. Non-Latency Mid-Slot Drift (Observation at 30s-60s remaining)
    print("\n" + "="*80)
    print("ANALYSIS 5: MID-SLOT HORIZON (REMAINING TIME 30s-60s vs 10s-30s)")
    print("="*80)
    for t_low, t_high in [(8, 15), (15, 30), (30, 60), (60, 150)]:
        for m_low, m_high in [(0, 10), (10, 25), (25, 50), (50, 500)]:
            n_tot = 0
            n_win = 0
            for r in labeled:
                rem = r.get("remaining_s")
                move = r.get("calc_abs_move")
                side = r.get("calc_side")
                res = r.get("outcome")
                if rem is not None and t_low <= rem < t_high and move is not None and m_low <= move < m_high:
                    n_tot += 1
                    if side == res:
                        n_win += 1
            if n_tot >= 20:
                wr = (n_win / n_tot) * 100
                print(f"Horizon [{t_low:3d}s - {t_high:3d}s) & Move [${m_low:2d} - ${m_high:3d}): N = {n_tot:4d} | WinRate = {wr:6.2f}%")

if __name__ == "__main__":
    run_analysis()
