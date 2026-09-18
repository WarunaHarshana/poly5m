# Polymarket 5-Minute BTC TWAP Bot (`poly5m`)

Paper-first bot for Polymarket rolling **BTC 5-minute Up/Down** markets.

## Settlement (read this first)

These markets resolve on **Chainlink BTC/USD TWAP** (60s window via Polymarket RTDS), **not** Binance last price.
This bot:

1. Streams Polymarket RTDS Chainlink ticks
2. Builds a realized path TWAP inside the 300s slot
3. Gates entries on Z-score + **net edge after crypto taker fees** (`fee ≈ rate · p · (1−p)`)
4. Refuses trades when the oracle stream is stale

Default mode is **`paper_live`**: real websockets, simulated fills. Live trading requires `MODE=live` **and** `LIVE_CONFIRM=yes`.

## Recommended strategy

| Strategy | Env value | Notes |
|----------|-----------|-------|
| **H-001 Sniper (default)** | `STRATEGY=H-001_SNIPER` | Fee-aware TWAP late-window; best structural fit |
| H-004 Maker | `STRATEGY=H-004_MAKER` | Resting bids; adverse-selection risk |
| H-005 Momentum | `STRATEGY=H-005_MOMENTUM` | Speculative late follow-through — use only intentionally |

## Quick start (paper)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Keep MODE=paper_live and STRATEGY=H-001_SNIPER
python test_twap_bot.py
python bot.py
```

## Live arming (real money)

Only after paper journals look sane:

```env
MODE=live
LIVE_CONFIRM=yes
PRIVATE_KEY=0x...
FUNDER_ADDRESS=0x...
```

Daily loss / consecutive-loss kill switches apply in **live** only so paper can accumulate sample size.

## Layout

- `twap_engine.py` — TWAP math, fee curve, sniper/maker/momentum gates
- `feeds.py` — Chainlink RTDS + CLOB book websockets (stale-tick refusal)
- `polymarket_client.py` — Gamma discovery + CLOB trading client
- `risk_manager.py` — bankroll / daily loss / cooldown
- `bot.py` — async loop, paper + live execution
- `test_twap_bot.py` — unit tests (math, fees, stale feed, risk)

## Verify

```bash
python test_twap_bot.py
```

Then run paper for several slots and read `state/journal.jsonl` before considering live.
