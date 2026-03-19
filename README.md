# Crypto Orchestra

Crypto Orchestra is a local-first crypto trading research project focused on building deterministic, testable trading logic before introducing any higher-level AI-assisted analysis.

The current repository contains an early backtesting engine for BTC and ETH on hourly data. The near-term goal is to improve signal quality, validate performance across multiple market regimes, and keep backtest behavior aligned with eventual paper-trading behavior.

## Principles

- Deterministic code makes trading decisions.
- Backtests and live logic should stay aligned.
- Complexity is added only after the simple version is measurable.
- AI may explain decisions later, but it should not decide trades directly.

## Current Status

- Backtesting currently lives in [`backtesting/backtest.py`](backtesting/backtest.py).
- The project structure already reserves space for trading, agents, analysis, notifications, logs, data, and tests.
- This repository is intentionally private while the strategy is still being developed.

## Quick Start

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run the backtester:

```bash
python backtesting/backtest.py
```

## Repository Layout

- `backtesting/` backtest scripts and historical strategy evaluation
- `trading/` trading engine code for paper/live execution
- `analysis/` notebooks, experiments, and review notes
- `agents/` future AI-assisted analysis components
- `notifications/` messaging and alert integrations
- `tests/` automated tests
- `logs/` local runtime logs, ignored from git
- `data/` local datasets and exports, ignored from git

## Security

- Never commit `.env` files or exchange credentials.
- Keep this repository private until the project is mature.
- Review changes before every push, especially anything touching config or data export paths.

## Roadmap

Immediate priorities:

- improve backtest signal quality
- add richer performance reporting
- implement true short logic if short exposure is required
- align backtest structure with the future live trading engine
- add tests around signal generation and PnL logic
