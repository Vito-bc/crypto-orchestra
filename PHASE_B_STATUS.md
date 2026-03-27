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

Or on Windows, use the scheduler-friendly wrapper:

```powershell
powershell -ExecutionPolicy Bypass -File trading\run_paper_trade.ps1
```

To register an hourly Windows Task Scheduler job:

```powershell
powershell -ExecutionPolicy Bypass -File trading\register_paper_trade_task.ps1
```

To remove it later:

```powershell
powershell -ExecutionPolicy Bypass -File trading\remove_paper_trade_task.ps1
```

2. Review logs written to:

- `logs/paper_signals.jsonl`
- `logs/paper_signals.csv`
- `logs/paper_position_eth.json`
- `logs/paper_position_events.jsonl`
- `logs/paper_position_events.csv`
- `logs/paper_runner_health.jsonl`
- optional Telegram alerts via local `.env`

3. Run the log review helper:

```bash
python trading/review_paper_logs.py
```

4. Run the daily paper summary:

```bash
python trading/daily_paper_summary.py
```

5. Send the daily summary to Telegram:

```bash
python trading/send_daily_telegram_summary.py
```

To register a daily Telegram summary task:

```powershell
powershell -ExecutionPolicy Bypass -File trading\register_daily_summary_task.ps1
```

To remove it later:

```powershell
powershell -ExecutionPolicy Bypass -File trading\remove_daily_summary_task.ps1
```

6. Track:

- how often ETH produces `BUY`, `SELL`, or `HOLD`
- how often trades are blocked by MACD, BB, or volume
- whether the runner is currently `FLAT` or `LONG`
- whether real-time cadence matches the backtest expectation
- whether the hourly runner is healthy or failing to build snapshots

## Success Criteria

- stable repeated signal snapshots
- no strategy drift between paper logic and backtest logic
- enough operational confidence to monitor ETH paper trades consistently
- a repeatable operating workflow you can schedule and review daily

## Notes

- BTC remains a secondary benchmark, not the primary Phase B candidate
- if ETH stays too inactive in paper mode, the next decision is strategic:
  accept the system as selective, or redesign the framework rather than keep loosening filters
- Telegram config lives in a local root `.env` file and is never committed
