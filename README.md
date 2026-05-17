# Crypto Orchestra

A multi-agent AI trading system for BTC and ETH on Coinbase. Five specialist Claude sub-agents run in parallel every hour, feed signals to an orchestrator, and autonomously place limit orders at support levels with trailing stop management.

## How It Works

```
Every 60 minutes:
  1. Check open positions → close any that hit stop / target / max-hold
  2. Check pending limit orders → fill simulation (paper) or Coinbase poll (live)
  3. Run 5 sub-agents concurrently:
       technical  — RSI, MACD, Bollinger Bands, EMA trend
       macro      — 4h EMA regime (BULL / BEAR / RANGING), acts as veto
       sentiment  — Fear & Greed index + news headlines
       whale      — OKX perpetual funding rate + BTC dominance
       risk       — ATR-based stop/target, portfolio exposure check
  4. Orchestrator (claude-sonnet-4-6) weighs all signals
  5. BUY (≥3 agents agree) → limit order placed at nearest support level
  6. Telegram alert sent for every order, fill, open, and close
```

## Entry Rules

| Rule | Value |
|------|-------|
| Min agents for BUY | 3 of 5 must explicitly signal BUY |
| Support gate | Price must be within 1.5x ATR of a known support level |
| Momentum filter | Candle body > +0.3% required at live entry |
| Macro veto | BEAR regime blocks all BUY signals |
| Stop loss | Entry − 2.5x ATR |
| Take profit | Entry + 4.0x ATR |
| Max hold | ETH: 8h · BTC: 12h (acts as implicit take-profit) |
| Entry fee | 0.2% maker (limit order) |
| Exit fee | 0.4% taker (market order) |

## Trailing Stop

Tuned via grid search across 17 configurations (profit factor 1.12 → 3.15):

| Parameter | Value |
|-----------|-------|
| Break-even trigger | +0.5% above entry → stop moves to entry |
| Trail activation | +0.5% above entry |
| Trail distance | 0.8% below high-water mark |

## Quick Start

```bash
# 1. Create and activate virtual environment
python -m venv venv
venv\Scripts\activate          # Windows
source venv/bin/activate       # macOS / Linux

# 2. Install dependencies
pip install -r requirements.txt

# 3. Copy and fill in environment variables
copy .env.example .env         # add ANTHROPIC_API_KEY and TELEGRAM_BOT_TOKEN

# 4. Run the pipeline once (paper trading, no Coinbase keys needed)
python pipeline/runner.py ETH-USD

# 5. View P&L dashboard
python pipeline/dashboard.py

# 6. Start hourly scheduler (Windows — double-click)
run_scheduler.bat
```

## Going Live on Coinbase

The Coinbase Advanced Trade API client is built and ready. To switch from paper trading:

1. Create an API key at [coinbase.com/settings/api](https://www.coinbase.com/settings/api) with **Trade** permission for ETH-USD and BTC-USD
2. Add to `.env`:
   ```
   COINBASE_API_KEY=organizations/xxx/apiKeys/yyy
   COINBASE_API_SECRET=-----BEGIN EC PRIVATE KEY-----\n...
   DRY_RUN=false
   ```

All orders (limit buys and market sells) will route to Coinbase automatically.

## Repository Layout

```
agents/
  orchestrator.py      — final decision engine (claude-sonnet-4-6)
  technical_agent.py   — RSI, MACD, Bollinger Bands, EMA
  macro_agent.py       — 4h regime classification with veto power
  sentiment_agent.py   — Fear & Greed + news headlines
  whale_agent.py       — OKX funding rate (primary), BTC dominance
  risk_agent.py        — ATR stops, position sizing, exposure check

exchange/
  coinbase_client.py   — Coinbase Advanced Trade API wrapper (dry-run safe)

pipeline/
  runner.py            — main hourly pipeline (positions → fills → agents → order)
  limit_orders.py      — limit order lifecycle: place, fill, expire, cancel
  position_tracker.py  — trailing stop, P&L computation, trade history
  scheduler.py         — production loop with error recovery and log mirroring
  dashboard.py         — ASCII P&L dashboard with equity curve
  daily_summary.py     — Telegram P&L snapshot (runs at 9 AM via Task Scheduler)

tools/
  price_data.py        — yfinance wrapper with 55-min thread-safe TTL cache
  price_levels.py      — swing high/low support/resistance detection
  funding_data.py      — OKX public API funding rates (no auth)

backtesting/
  backtest.py          — core backtesting engine
  period_validation.py — 4-period out-of-sample validation
  fee_comparison.py    — legacy / market / limit fee scenario comparison
  stop_target_tune.py  — ATR stop/target grid search (13 combinations)
  trailing_stop_tune.py— trailing stop parameter grid search (17 configs)
  entry_diagnostics.py — per-trade win/loss pattern analysis

notifications/
  telegram.py          — Telegram alerts for all trade events

logs/                  — runtime logs (git-ignored)
  agent_decisions.jsonl
  trade_history.jsonl
  open_positions.json
  pending_orders.json
  scheduler.log
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ANTHROPIC_API_KEY` | Yes | Claude API key |
| `TELEGRAM_BOT_TOKEN` | Yes | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Yes | Chat ID for alerts |
| `DRY_RUN` | No | `true` (default) = paper trade, `false` = real orders |
| `COINBASE_API_KEY` | Live only | Coinbase Advanced Trade API key |
| `COINBASE_API_SECRET` | Live only | Coinbase API private key (PEM) |
| `TRADE_SIZE_PCT` | No | Position size as fraction of balance (default: 0.02) |
| `MAX_POSITIONS` | No | Max concurrent open positions (default: 5) |

## Key Backtest Findings

- **Win rate is fixed by entry quality, not stop/target ratio** — all 13 ATR combinations tested showed the same ~20% signal win rate. Actual win rate when trades fire: ~41%.
- **88% of exits are MAX_HOLD** — support bounces are short; the time limit acts as an implicit take-profit. ETH 8h, BTC 12h is optimal.
- **Limit orders are required for profitability** — limit fees (0.6% RT) push ETH to +0.02% avg. Market fees (0.8% RT) keep it negative.
- **Jan–Mar 2025: zero trades** — system correctly avoided the −46% ETH crash by staying in BEAR regime HOLD.

## Security

- Never commit `.env` files or exchange credentials — both are git-ignored.
- `DRY_RUN=true` by default — no real orders are placed without explicit opt-in.
- All Coinbase calls are wrapped in a single module (`exchange/coinbase_client.py`) for easy auditing.
