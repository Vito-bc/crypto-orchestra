# Phase B Status

## Goal

Phase B is about observation and execution discipline, not more blind tuning.

The immediate focus is:

- run the current ETH baseline as a paper-trading candidate
- log every evaluated signal snapshot
- persist paper position state across runs
- observe fills, inactivity periods, trade cadence, and operational issues

## Current Candidate

- Symbol: `ETH-USD`
- Signal timeframe: `1h`
- Higher timeframe context: `4h`
- Style: selective medium-term system

## Initial Workflow

1. Run the paper signal script:

```bash
python trading/paper_trade.py
```

2. Review logs written to:

- `logs/paper_signals.jsonl`
- `logs/paper_signals.csv`
- `logs/paper_position_eth.json`
- `logs/paper_position_events.jsonl`
- `logs/paper_position_events.csv`
- optional Telegram alerts via local `.env`

3. Run the log review helper:

```bash
python trading/review_paper_logs.py
```

4. Track:

- how often ETH produces `BUY`, `SELL`, or `HOLD`
- how often trades are blocked by MACD, BB, or volume
- whether the runner is currently `FLAT` or `LONG`
- whether real-time cadence matches the backtest expectation

## Success Criteria

- stable repeated signal snapshots
- no strategy drift between paper logic and backtest logic
- enough operational confidence to monitor ETH paper trades consistently

## Notes

- BTC remains a secondary benchmark, not the primary Phase B candidate
- if ETH stays too inactive in paper mode, the next decision is strategic:
  accept the system as selective, or redesign the framework rather than keep loosening filters
- Telegram config lives in a local root `.env` file and is never committed
