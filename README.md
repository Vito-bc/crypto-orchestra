# Crypto Orchestra

[![Python](https://img.shields.io/badge/python-3.10%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-Windows-lightgrey?logo=windows)](scripts/update_obsidian.bat)
[![AI](https://img.shields.io/badge/AI-Claude%20Sonnet%204.6-purple)](https://www.anthropic.com/)
[![Status](https://img.shields.io/badge/status-paper%2Fshadow%20only-orange)](docs/trial_registry.md)
[![Live](https://img.shields.io/badge/live%20trading-NOT%20AUTHORIZED-red)](docs/trial_registry.md)

A multi-agent AI trading system built for Coinbase Advanced Trade. Seven specialist Claude sub-agents run in parallel every hour, feed signals to an orchestrator, and produce limit-order decisions with ATR stop/target management and a full risk engine.

> ### Status: paper / shadow only — live trading is NOT authorized
>
> The system runs with `DRY_RUN=true`. Orders are **simulated, not placed.**
>
> **No positive expectancy has been demonstrated.** The frozen V2 momentum
> mechanism is unprofitable on the continuous evaluation window:
>
> | Result | Value |
> |---|---|
> | ZEC continuous (2021-06-26 → 2026-07-12), no filter | **PF 0.761**, −0.62%/trade, n=114 |
> | ZEC continuous, integrated V3 ER-30 filter | **PF 0.706**, −0.83%/trade, n=71 |
> | Frozen mechanism transferred to BTC / ETH / SOL | PF 0.359 / 0.476 / 0.718 — all < 1.0 |
>
> - **V3 ER-30 is RETIRED**, not pending: enforcement makes results *worse*, and
>   there is no trade count that activates it.
> - **Earlier walk-forward results in this repository are void.** The tool never
>   attached the daily frame, so it always validated a weaker mechanism than it
>   reported. It has been repaired, but its windows remain historical
>   diagnostics — not out-of-sample evidence.
> - ZEC is the only enabled asset, and only in shadow mode.
>
> Every figure above is asserted against `docs/research/artifacts/results.json`
> by `tests/test_research_provenance.py`. Read `docs/trial_registry.md` before
> believing any number in this file.

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

Tuned in-sample from the full-year signal scanner across 371 trades. These are **selected** parameters, not validated ones — the registry counts ~25-30 trials before the freeze, so any single positive cell must be read against that multiple-testing budget.

| Asset | Stop | Target | R:R | Rationale |
|-------|------|--------|-----|-----------|
| BTC-USD | 2.0x ATR | 3.5x ATR | 1.75 | Tighter stop — BTC has cleaner structure |
| ETH-USD | 2.5x ATR | 4.5x ATR | 1.80 | Wider stop — absorbs intraday wicks |
| SOL-USD | 2.5x ATR | 4.5x ATR | 1.80 | Wider stop — high volatility |
| ZEC-USD | 2.0x ATR | 3.5x ATR | 1.75 | Frozen at `v2-adx25-frozen`; selected in-sample, not validated |

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

## Walk-Forward Validation — WITHDRAWN

This section previously reported out-of-sample results across three market
regimes and concluded that "ZEC is the only asset with genuine out-of-sample
edge" (+0.30% avg OOS).

**Those numbers are void and have been removed.** `backtesting/walk_forward.py`
never attached the daily frame, so `close_1d` / `ema*_1d` were absent from every
row and the declared daily-EMA trend gate was skipped for every signal on every
asset. Its scan loop also enumerated only three blocked reasons, so
`daily_trend` and `btc_regime` blocks fell through and were traded, it charged
the wrong fees, and it reported an unfinished holding period as a completed
trade.

The tool was **repaired** in trial `2026-08-walkforward-repair.v1` and now
shares the scanner's frame assembly and trade simulator. Enforcing the gates it
used to skip removed **475 `daily_trend`** and **118 `btc_regime`** signals it
previously traded. Its output is a deterministic artifact under
`docs/research/artifacts/walk_forward/`.

Its windows are nonetheless **historical diagnostics, not out-of-sample
evidence**: they were inspected repeatedly during development and are counted in
the trial registry's multiple-testing budget. The repair establishes that the
tool now measures the mechanism it declares — nothing more.

The current, reproducible evaluation is the continuous-window result in the
status block above: **not profitable**.

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

## Live Trading — not authorized

Live trading is **not** enabled and this README deliberately does not document
how to switch it on. `DRY_RUN=true` is the safe default and the research does not
currently support changing it: the strategy's measured expectancy is negative
(see the status block above).

Activation would require, in order: a repaired evaluation harness, a
pre-registered strategy with an acceptance rule fixed in advance, a forward
shadow trial that passes it, and an explicit human decision recorded in
`docs/trial_registry.md`. None of those conditions is met.

`LIVE_BALANCE_USD=100` is the *cap* that would apply if live trading were ever
authorized — it is not evidence that money is at risk today.

## Obsidian Second Brain

The system auto-generates an Obsidian knowledge vault from all trading data:

```bash
python backtesting/generate_journal.py   # generate vault manually
scripts/update_obsidian.bat              # one-click Windows shortcut
```

A Windows Task Scheduler job runs `scripts/update_obsidian.bat` every night at 23:00 automatically. The vault includes trade notes, agent decision logs, backtest summaries, and strategy changelogs — designed to grow into a RAG knowledge base for the orchestrator.

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
  walk_forward.py       — walk-forward diagnostic (historical windows only)
  research_runner.py    — deterministic research runner, byte-identical artifacts
  generate_journal.py   — Obsidian vault generator from all system data
  backtest.py           — core backtesting engine

tools/
  price_data.py         — yfinance wrapper with 55-min TTL cache
  price_levels.py       — swing high/low support/resistance detection
  market_positioning.py — OKX funding rates + BTC dominance

notifications/
  telegram.py           — alerts for all trade lifecycle events

scripts/
  update_obsidian.bat   — regenerate Obsidian vault (runs nightly via Task Scheduler)
  run_scheduler.bat     — start the continuous scheduler
  view_pnl.bat          — P&L dashboard shortcut

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
