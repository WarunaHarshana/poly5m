# Polymarket 5-Minute BTC TWAP Mispricing Bot (H-001)

A clean, high-performance quantitative trading bot purpose-built to exploit **High-Conviction TWAP Path-Accumulation Mispricings (H-001)** on Polymarket's recurring 5-Minute Bitcoin Up/Down markets (`btc-updown-5m-{window_ts}`).

---

## 1. Strategy & Mathematical Foundation

Polymarket resolves 5-minute BTC markets on the **Chainlink 60-second TWAP stream**:
$$\text{Outcome} = \begin{cases} \text{Up} & \text{if } \text{TWAP}_{[0, 300]} \ge S_0 \\ \text{Down} & \text{if } \text{TWAP}_{[0, 300]} < S_0 \end{cases}$$

At elapsed time $t \in [0, 300]$ (remaining time $\tau = 300 - t$):
1. **Realized Integral:**
   $$I_{\text{realized}}(t) = \int_0^t S(u) du \approx \sum_{k=0}^{n-1} \frac{S(u_k) + S(u_{k+1})}{2} \Delta u_k$$
2. **Required Remaining Average Spot Price ($\overline{S}_{\text{req}}$):**
   $$\overline{S}_{\text{req}}(\tau) = \frac{300 \cdot S_0 - I_{\text{realized}}(t)}{\tau}$$
3. **Cubic Variance Decay of Remaining TWAP:**
   $$\sigma_{\text{TWAP}}(\tau) = S(t) \cdot \sqrt{\frac{\sigma^2 \tau}{3}}$$
4. **Standardized TWAP Z-Score ($Z_{\text{TWAP}}$):**
   $$Z_{\text{TWAP}} = \frac{S(t) - \overline{S}_{\text{req}}(\tau)}{\sigma_{\text{TWAP}}(\tau)}$$
5. **Calibrated Win Probability:**
   $$P(Z) = \frac{1}{1 + e^{-\alpha |Z|}} \quad (\alpha = 1.75)$$
6. **Polymarket Dynamic Crypto Taker Fee:**
   $$\text{Fee}(p) = \text{rate} \cdot p \cdot (1 - p)$$
7. **Net Edge Gating:**
   $$\text{Edge}_{\text{net}} = P(Z) - \text{Ask} - \text{Fee}(\text{Ask}) \ge 3\%$$

---

## 2. Codebase Architecture

* `twap_engine.py`: Pure mathematical pricing model, $S_{\text{req}}$ calculation, cubic variance decay, calibrated probability, dynamic taker fees, and decision gating.
* `feeds.py`: Real-time WebSocket clients for **Polymarket RTDS Chainlink BTC/USD** (`wss://ws-live-data.polymarket.com`) and **Polymarket CLOB L2 Orderbook** (`wss://ws-subscriptions-clob.polymarket.com/ws/market`).
* `polymarket_client.py`: Gamma market discovery and CLOB authenticated trading client.
* `risk_manager.py`: Bankroll protection, daily loss limit, position sizing, and cooldown logic.
* `bot.py`: Main asynchronous event loop executing trade cycles and settlement.
* `test_twap_bot.py`: Unit test suite verifying mathematical derivations and integrations.

---

## 3. Running the Bot

### Local Test Mode (Paper Trading with Live Data)
```bash
python bot.py
```

### Local Test Suite
```bash
python test_twap_bot.py
```

### Live Real-Money Mode
1. Configure credentials in `.env`:
   ```env
   MODE=live
   LIVE_CONFIRM=yes
   PRIVATE_KEY=0x...
   FUNDER_ADDRESS=0x...
   ```
2. Run:
   ```bash
   python bot.py
   ```
