# Crypto Orchestra

[![Python](https://img.shields.io/badge/python-3.10%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-Windows-lightgrey?logo=windows)](update_obsidian.bat)
[![AI](https://img.shields.io/badge/AI-Claude%20Sonnet%204.6-purple)](https://www.anthropic.com/)
[![Status](https://img.shields.io/badge/status-live%20ready-brightgreen)](.env.example)

A multi-agent AI trading system for BTC, ETH, SOL and ZEC on Coinbase Advanced Trade. Seven specialist Claude sub-agents run in parallel every hour, feed signals to an orchestrator, and autonomously place limit orders at support levels with trailing stop management and a full risk engine.

## How It Works

```
Every 60 minutes:
  1. Check open positions → close any that hit stop / target / max-hold
  2. Check pending limit orders → fill simulation (paper) or Coinbase poll (live)
  3. Run 7 sub-agents concurrently:
       technical   — RSI, MACD, Bollinger Bands, EMA trend
       macro       — 4h EMA regime (BULL / BEAR / RANGING), acts as veto
       sentiment   — Fear & Greed index + news headlines
       whale       — OKX perpetual funding rate + BTC dominance
       risk        — ATR-based stop/target, portfolio exposure check
       news        — asset-specific news headlines via web search
       breakout    — price structure breakout / breakdown detection
  4. Orchestrator (claude-sonnet-4-6) weighs all signals
  5. BUY → limit order placed at nearest support level (maker fee 0.2%)
  6. Telegram alert sent for every order, fill, open, and close
```

## Entry Rules & Risk Engine

| Rule | Value |
|------|-------|
| Min agents for BUY | majority of 7 must agree |
| Limit order gate | price within 5x ATR of a support level |
| BTC BEAR veto | corr ≥ 0.65 = full block · corr 0.35–0.65 = 50% size cut |
| Funding rate veto | OKX annualized funding > 20% → block (crowded longs) |
| Velocity veto | asset down > 5% in 24h → no long entry |
| Whipsaw guard | 2+ stops in 96h → block re-entry |
| Bounce confirmation | must recover +1.5x ATR above last stop-exit |
| Entry fee | 0.2% maker (limit order) |
| Exit fee | 0.4% taker (market order) |

## Per-Asset ATR Parameters

Tuned from full-year signal scanner across 371 trades:

| Asset | Stop | Target | R:R | Rationale |
|-------|------|--------|-----|-----------|
| BTC-USD | 2.0x ATR | 3.5x ATR | 1.75 | Tighter stop — BTC has cleaner structure |
| ETH-USD | 2.5x ATR | 4.5x ATR | 1.80 | Wider stop — absorbs intraday wicks |
| SOL-USD | 2.5x ATR | 4.5x ATR | 1.80 | Wider stop — high volatility |
| ZEC-USD | 2.0x ATR | 3.5x ATR | 1.75 | Best walk-forward edge (+0.30% avg OOS) |

## Drawdown Circuit Breakers

| Drawdown from Peak | Action |
|--------------------|--------|
| −5% | Position size reduced to 50% |
| −8% | Position size reduced to 25% |
| −12% | **All trading HALTED** — manual review required |
| Daily loss −2% | Position size reduced to 50% |

## Trailing Stop

| Parameter | Value |
|-----------|-------|
| Break-even trigger | +1.5% above entry → stop moves to entry price |
| Trail activation | +2.0% above entry |
| Trail distance | 1.5% below high-water mark |
| Hold extension | Up to 3×8h extensions if 3/5 conditions met + ADX ≥ 20 |

## Walk-Forward Validation

Out-of-sample results across 3 market regimes (Aug 2024 – Jun 2025):

| Asset | W1 Bull | W2 Bear | W3 Bear | Avg OOS | Verdict |
|-------|---------|---------|---------|---------|---------|
| ZEC-USD | +2.57% | −0.07% | −1.61% | **+0.30%** | ✅ EDGE |
| ETH-USD | −0.05% | −1.72% | −0.50% | −0.89% | ⚠ MARGINAL |
| BTC-USD | −0.01% | −2.53% | −1.06% | −1.20% | ❌ WEAK |
| SOL-USD | −0.72% | −2.87% | −0.97% | −1.52% | ❌ WEAK |

ZEC is the only asset with genuine out-of-sample edge. Manual per-asset parameter tuning matched or outperformed walk-forward winners on OOS data.

## Quick Start

```bash
# 1. Create and activate virtual environment
python -m venv venv
venv\Scripts\activate          # Windows

# 2. Install dependencies
pip install -r requirements.txt

# 3. Copy and fill in environment variables
copy .env.example .env

# 4. Run the pipeline once (paper trading, no Coinbase keys needed)
python pipeline/runner.py ZEC-USD

# 5. Run continuous scheduler (every 60 minutes)
python pipeline/scheduler.py
```

## Going Live on Coinbase

1. Go to [coinbase.com/settings/api](https://www.coinbase.com/settings/api)
2. Create a key with **Trade + View** permissions, **ECDSA algorithm**
3. Download the JSON file → place at project root as `cdp_api_key.json`
4. Set in `.env`:
   ```
   DRY_RUN=false
   LIVE_BALANCE_USD=100    # bot will only use this amount, rest of account untouched
   ```

The system places orders up to `LIVE_BALANCE_USD × position_size_pct` — your full account balance beyond this amount is never touched.

## Obsidian Second Brain

The system auto-generates an Obsidian knowledge vault from all trading data:

```bash
python backtesting/generate_journal.py   # generate vault manually
update_obsidian.bat                      # one-click Windows shortcut
```

A Windows Task Scheduler job runs `update_obsidian.bat` every night at 23:00 automatically. The vault includes trade notes, agent decision logs, backtest summaries, and strategy changelogs — designed to grow into a RAG knowledge base for the orchestrator.

## Repository Layout

```
agents/
  orchestrator.py       — final decision engine (claude-sonnet-4-6)
  technical_agent.py    — RSI, MACD, Bollinger Bands, EMA
  macro_agent.py        — 4h regime classification with veto power
  sentiment_agent.py    — Fear & Greed + news headlines
  whale_agent.py        — OKX funding rate + BTC dominance
  risk_agent.py         — ATR stops, position sizing, exposure check
  asset_news_agent.py   — asset-specific news via web search
  breakout_agent.py     — price structure breakout detection

exchange/
  coinbase_client.py    — Coinbase Advanced Trade API (ECDSA key file, dry-run safe)

pipeline/
  runner.py             — main hourly pipeline with full risk engine
  limit_orders.py       — limit order lifecycle: place, fill, expire, cancel
  position_tracker.py   — trailing stop, hold extension, P&L, trade history
  scheduler.py          — continuous loop (ET timestamps)
  dashboard.py          — ASCII P&L dashboard with equity curve
  daily_summary.py      — Telegram P&L snapshot (9 AM)
  weekly_review.py      — Telegram weekly performance report

backtesting/
  signal_scanner.py     — full-year signal scanner with per-asset ATR params
  monte_carlo.py        — 10,000-sim Monte Carlo per asset
  walk_forward.py       — 3-window walk-forward optimization (OOS validation)
  generate_journal.py   — Obsidian vault generator from all system data
  backtest.py           — core backtesting engine

tools/
  price_data.py         — yfinance wrapper with 55-min TTL cache
  price_levels.py       — swing high/low support/resistance detection
  market_positioning.py — OKX funding rates + BTC dominance

notifications/
  telegram.py           — alerts for all trade lifecycle events

update_obsidian.bat     — regenerate Obsidian vault (runs nightly via Task Scheduler)

logs/                   — runtime logs (git-ignored)
  agent_decisions.jsonl
  trade_history.jsonl
  open_positions.json
  pending_orders.json
  scheduler.log

obsidian_vault/         — Obsidian knowledge base (git-ignored, local only)
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ANTHROPIC_API_KEY` | Yes | Claude API key |
| `TELEGRAM_BOT_TOKEN` | Yes | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Yes | Chat ID for alerts |
| `DRY_RUN` | No | `true` (default) = paper trade · `false` = real orders |
| `LIVE_BALANCE_USD` | No | Capital allocated to the bot (default: 10000) |
| `SUBAGENT_MODEL` | No | Model for sub-agents (default: claude-haiku-4-5) |
| `ORCHESTRATOR_MODEL` | No | Model for orchestrator (default: claude-sonnet-4-6) |

## Security

- Never commit `.env` files or exchange credentials — both are git-ignored.
- `cdp_api_key.json` (Coinbase ECDSA key) is git-ignored — local only.
- `DRY_RUN=true` by default — no real orders without explicit opt-in.
- `LIVE_BALANCE_USD` caps the bot's spending — rest of account is untouched.
- All Coinbase calls isolated in `exchange/coinbase_client.py` for easy auditing.
