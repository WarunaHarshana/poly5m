# Go-Live Runbook

Everything you need to switch this bot to real money, in order. The live
order path is covered by `_selftest_realtime.py` Phase C (order → cancel →
fill-check → PnL), and wallet auth was verified working on 2026-07-18.

## 0. Decide WHEN (the only non-mechanical step)

Open the dashboard's **Performance Analysis** panel (or `python analyze.py`).
The verdict banner is the go/no-go:

- **GREEN — "ABOVE breakeven" with n ≥ 100 momentum trades** → the data
  supports a small live experiment.
- **AMBER — above breakeven but small sample** → keep paper running; a
  win streak at n < 30 proves nothing (this bot once did 92.5% WR and
  still lost money).
- **RED — below breakeven** → live mode just converts paper losses into
  real ones. Don't.

Note: the ARB strategy stays **paper-only** even in live mode (leg risk is
not handled) — live trading is momentum-only. Judge the momentum line.

## 1. Fund the wallet

- The wallet needs **USDC on Polymarket** (deposit address = FUNDER_ADDRESS).
- Minimum for one trade: ~$2.75–$4.60 (venue minimum 5 shares × entry price).
- Sensible first-live balance: **$15–25** — enough to absorb the daily-loss
  stop ($5) a couple of times while staying "money you can lose entirely."
- Signature type 3 (site deposit wallet) trades gaslessly — no POL needed.
- Balance check without starting the bot: dashboard → Live Wallet Setup →
  Refresh balance.

## 2. Flip the switch (deliberately, yourself)

In `.env`:

```
MODE=live
LIVE_CONFIRM=yes
LIVE_AGGRESSIVE=0     # keep 0 — this is what arms the safety profile
```

Then start from the dashboard (pick `live`, tick the risk acknowledgement,
type `LIVE`) or CLI: `python bot.py --mode live --rounds 10`.

## 3. What the safety profile enforces automatically (LIVE_AGGRESSIVE=0)

| Guard | Value |
|---|---|
| Stake basis | $1 fixed base (min-share bump to ~$3–4.60 max) |
| Concurrent exposure | ~1 open trade (hard cap $5) |
| Daily loss stop | $5, then halt until restart |
| Loss streak stop | 3 consecutive losses |
| Trades per day | max 10 |
| Orders | GTC **limit** at ask only, cancelled at slot end |
| Prices | Chainlink Data Streams first (the resolution oracle) |

Worst realistic day ≈ −$5 (daily stop) before the bot refuses to continue.

## 4. Watch the first session live

- Dashboard header: **MODE: LIVE** badge; PM Wallet tile = source of truth
  (local bankroll is an estimate).
- Every trade prints a Polymarket link — verify the first fill on the
  website matches what the bot logged (price, size, outcome).
- `[live fill] matched shares:` in the console shows partial/zero fills —
  zero-fill trades are recorded at $0 stake (no phantom PnL).
- Kill switches: **■ Stop** button; or set `LIVE_CONFIRM=no` in `.env` to
  block future live starts.

## 5. After each live session

```
python analyze.py --hours 12
```

Compare live expectancy vs the paper expectancy for the same period. If
live runs materially below paper for 30+ trades, that gap is adverse
selection — stop and reassess (the next lever is maker orders, which earn
fee rebates instead of paying the 7% taker fee).

## Known live-mode facts (verified)

- Taker fee: `rate × min(p, 1−p)` per share, rate 0.07 on 5m crypto
  markets (from Gamma `feeSchedule`). Already modeled in every gate.
- Wallet auth: CLOB accepts creds, signature_type=3, creds cached in
  `state/poly_api_creds.json`.
- Live balance found 2026-07-18: **$0.57 — must deposit before trading.**

## Running multiple timeframes in parallel

Verified 2026-07-19: only **5m, 15m and 4h** series exist (1h / 30m / 10m
do NOT). Each instance needs its own state directory or they overwrite
each other's trades — that is automatic for non-5m runs.

```powershell
# terminal 1 — 5m (default, uses state/)
python bot.py --mode paper_live --rounds 200

# terminal 2 — 15m (uses state/15m/ automatically)
$env:TIMEFRAME='15m'; python bot.py --mode paper_live --rounds 100
```

TIMEFRAME must be set in the environment, not via `--timeframe`: the slug
prefix and state paths are resolved at import time.

Analyse each series separately (they are different strategies statistically):

```powershell
python analyze.py                                    # 5m
# for 15m, point HISTORY at state/15m/archive/trades_history.jsonl
```

### Honest trade-offs before you do this

- **Liquidity is ~4x worse on 15m** ($23k/24h vs $81k on 5m) — expect
  worse fills and fewer maker fills, which is where the edge now is.
- **Correlated exposure.** 5m and 15m windows overlap; being long UP on
  both is one bigger directional bet, not diversification. The risk caps
  are PER INSTANCE, so two instances = double the configured exposure.
  Halve the stake settings if you run both.
- **The upside is sample size**, not edge: +4 slots/hour on top of 12,
  i.e. ~33% faster learning. The late-window logic itself transfers
  cleanly (fair value keys off REMAINING seconds, not window length).
- **4h is not worth it** at 0.25 slots/hour — one trade every four hours
  will never accumulate a meaningful sample.
