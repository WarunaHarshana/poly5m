"""Deep Quantitative Hypothesis Discovery for Practical Infrastructure.

Explores:
1. Longshot / Underdog Bias (Retail overpaying for lottery tickets).
2. Passive Maker / Limit Order Edge (Earning the spread vs paying taker fees).
3. Trailing Volatility Clustering (Predicting wide move slots from trailing 15m).
4. Order Book Depth & Spread Toxicity.
"""
import glob
import json
import math
import numpy as np
from scipy import stats

def load_data():
    obs_files = glob.glob("data/*/archive/observations.jsonl")
    all_obs = {}
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
                    if slug not in all_obs or (res and not (all_obs[slug].get("outcome") or all_obs[slug].get("resolution"))):
                        all_obs[slug] = row
        except Exception:
            pass
    
    labeled = []
    for r in all_obs.values():
        res = r.get("outcome") or r.get("resolution")
        if res in ("UP", "DOWN"):
            spot = r.get("spot")
            strike = r.get("strike")
            if spot is not None and strike is not None:
                r["calc_move"] = spot - strike
                r["calc_abs_move"] = abs(spot - strike)
                r["calc_side"] = "UP" if (spot - strike) >= 0 else "DOWN"
            labeled.append(r)
    return labeled

def analyze_deep():
    data = load_data()
    print(f"Total labeled slots: {len(data)}")

    # 1. Longshot / Favorite Pricing & True Win Rates
    # If the market quotes Underdog Ask = 0.05 - 0.15, does it win ~10% or < 2%?
    print("\n" + "="*80)
    print("HYPOTHESIS TEST: FAVORITE UNDERPRICING & UNDERDOG OVERPRICING (LONGSHOT BIAS)")
    print("="*80)
    
    # Check slots where spot move was >= $15 at remaining <= 15s
    conviction_slots = [r for r in data if r.get("calc_abs_move", 0) >= 15.0 and (r.get("remaining_s") or 0) <= 15.0]
    print(f"High-conviction slots (|move| >= $15, tau <= 15s): N = {len(conviction_slots)}")
    
    n_won = sum(1 for r in conviction_slots if r.get("calc_side") == r.get("outcome"))
    win_rate = n_won / len(conviction_slots) if conviction_slots else 0
    print(f"Favorite Realized Win Rate: {win_rate*100:.2f}% ({n_won}/{len(conviction_slots)})")
    
    # Compare with Polymarket ask prices on these slots
    asks = [r.get("ask") for r in conviction_slots if r.get("ask") is not None]
    if asks:
        avg_ask = np.mean(asks)
        med_ask = np.median(asks)
        print(f"Market Ask Distribution on Favorite: Mean Ask = {avg_ask:.3f}, Median Ask = {med_ask:.3f}")
        print(f"Implied Win Rate = {avg_ask*100:.1f}% vs Realized Win Rate = {win_rate*100:.2f}%")
        print(f"Unconditional Favorite Underpricing Gap = {(win_rate - avg_ask)*100:+.2f}%")

    # 2. Trailing Volatility Clustering (Autocorrelation of Move Sizes across Consecutive Slots)
    print("\n" + "="*80)
    print("HYPOTHESIS TEST: INTER-SLOT VOLATILITY CLUSTERING (SLOT T-1 -> SLOT T)")
    print("="*80)
    
    # Sort slots by end_ts
    sorted_slots = sorted([r for r in data if r.get("end_ts") and r.get("calc_abs_move") is not None], key=lambda x: x["end_ts"])
    
    # Find consecutive slots (end_ts difference == 300s)
    consec_pairs = []
    for i in range(len(sorted_slots) - 1):
        s1 = sorted_slots[i]
        s2 = sorted_slots[i+1]
        if s2["end_ts"] - s1["end_ts"] == 300:
            consec_pairs.append((s1["calc_abs_move"], s2["calc_abs_move"], s1["chop_crosses"] or 0, s2["chop_crosses"] or 0))
            
    print(f"Total consecutive 5m slot pairs analyzed: {len(consec_pairs)}")
    if consec_pairs:
        m1 = [p[0] for p in consec_pairs]
        m2 = [p[1] for p in consec_pairs]
        corr_moves, p_val_moves = stats.pearsonr(m1, m2)
        spearman_moves, sp_pval = stats.spearmanr(m1, m2)
        print(f"Move Size Autocorrelation (Lag 1): Pearson r = {corr_moves:.4f} (p = {p_val_moves:.2e}), Spearman rho = {spearman_moves:.4f} (p = {sp_pval:.2e})")
        
        # If previous slot had large move (>= $30), what is probability next slot also has large move (>= $20)?
        high_prev = [p for p in consec_pairs if p[0] >= 30.0]
        low_prev = [p for p in consec_pairs if p[0] < 10.0]
        
        p_high_after_high = np.mean([1 if p[1] >= 20.0 else 0 for p in high_prev]) if high_prev else 0
        p_high_after_low = np.mean([1 if p[1] >= 20.0 else 0 for p in low_prev]) if low_prev else 0
        
        print(f"P(Next Move >= $20 | Prev Move >= $30): {p_high_after_high*100:.2f}% (N = {len(high_prev)})")
        print(f"P(Next Move >= $20 | Prev Move < $10):  {p_high_after_low*100:.2f}% (N = {len(low_prev)})")
        
        # Chop persistence
        c1 = [p[2] for p in consec_pairs]
        c2 = [p[3] for p in consec_pairs]
        corr_chop, p_val_chop = stats.pearsonr(c1, c2)
        print(f"Chop Crossings Autocorrelation (Lag 1): Pearson r = {corr_chop:.4f} (p = {p_val_chop:.2e})")

    # 3. Directional Momentum vs Reversal (Do consecutive slots trend in the same direction?)
    print("\n" + "="*80)
    print("HYPOTHESIS TEST: CONSECUTIVE SLOT DIRECTIONAL PERSISTENCE (UP->UP vs UP->DOWN)")
    print("="*80)
    
    dir_pairs = []
    for i in range(len(sorted_slots) - 1):
        s1 = sorted_slots[i]
        s2 = sorted_slots[i+1]
        if s2["end_ts"] - s1["end_ts"] == 300:
            res1 = s1.get("outcome")
            res2 = s2.get("outcome")
            if res1 in ("UP", "DOWN") and res2 in ("UP", "DOWN"):
                dir_pairs.append((res1, res2, s1["calc_abs_move"]))
                
    if dir_pairs:
        same_dir = sum(1 for p in dir_pairs if p[0] == p[1])
        pct_same = same_dir / len(dir_pairs)
        print(f"Unconditional Directional Persistence: {pct_same*100:.2f}% same direction ({same_dir}/{len(dir_pairs)})")
        
        # After strong trend move (>= $40)
        strong_pairs = [p for p in dir_pairs if p[2] >= 40.0]
        same_strong = sum(1 for p in strong_pairs if p[0] == p[1])
        pct_strong = same_strong / len(strong_pairs) if strong_pairs else 0
        print(f"Conditional on Prev Move >= $40: {pct_strong*100:.2f}% same direction ({same_strong}/{len(strong_pairs)})")

    # 4. Asymmetric Taker Fee & Breakeven Horizon
    print("\n" + "="*80)
    print("HYPOTHESIS TEST: MAKER LIMIT ORDER SPREAD CAPTURE VS TAKER SPREAD CROSSING")
    print("="*80)
    
    spreads = [r.get("spread") for r in data if r.get("spread") is not None]
    if spreads:
        print(f"Observed Spreads: Mean = ${np.mean(spreads):.3f}, Median = ${np.median(spreads):.3f}, 75th pct = ${np.percentile(spreads, 75):.3f}")

if __name__ == "__main__":
    analyze_deep()
