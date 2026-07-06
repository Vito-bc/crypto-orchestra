# Crypto Orchestra — CLAUDE.md

Project context for AI assistants. Read this before touching any file.

## What This Is

A multi-agent AI trading system running live on Coinbase Advanced Trade.
Seven Claude sub-agents analyze BTC, ETH, SOL, ZEC every 60 minutes and place
limit orders at support levels. Real money is live ($100 allocated).

**Owner:** NYC-based, Coinbase Advanced Trade account.

## Security — Never Violate These

- Never commit `.env` — contains Anthropic + Telegram keys
- Never commit `cdp_api_key.json` — Coinbase ECDSA private key
- Never commit `obsidian_vault/` — personal knowledge base
- `DRY_RUN=true` is the safe default — only change to `false` explicitly
- `LIVE_BALANCE_USD=100` caps the bot's spending — never increase without asking

## How to Run

```powershell
# Single pipeline run (one asset, dry-run):
venv\Scripts\python.exe pipeline/runner.py ZEC-USD

# Continuous scheduler (every 60 min, ET timestamps):
venv\Scripts\python.exe pipeline/scheduler.py

# Backtesting:
venv\Scripts\python.exe backtesting/signal_scanner.py --period full_year
venv\Scripts\python.exe backtesting/walk_forward.py
venv\Scripts\python.exe backtesting/monte_carlo.py --scanner

# Regenerate Obsidian vault:
venv\Scripts\python.exe backtesting/generate_journal.py
```

## Architecture — Key Decisions

### Agent Flow
```
7 sub-agents (parallel) → OrchestratorAgent → TradeDecision → limit order on Coinbase
```
Sub-agents use `claude-haiku-4-5-20251001` (fast + cheap).
Orchestrator uses `claude-sonnet-4-6` (smarter final decision).

### Why Limit Orders (not market)
Maker fee 0.2% vs taker 0.4% — saves 0.4% per round trip.
This is the margin that was blocking profitability in backtests.

### Active Assets
**ETH-USD and ZEC-USD only.** BTC and SOL excluded — bounce strategy has
consistently negative edge on these (backtested over 371 signals, full year).

Re-enable BTC/SOL only after developing asset-specific entry logic for them
(e.g. trend-following for BTC instead of mean-reversion).

### Per-Asset Strategy Config (signal_scanner.py `ASSET_CONFIG`)
| Asset | Stop | Target | R:R | Min Conds | Daily EMA | Enabled |
|-------|------|--------|-----|-----------|-----------|---------|
| BTC-USD | 2.0x | 3.5x | 1.75 | 4 | 50d | **No** |
| ETH-USD | 2.5x | 4.5x | 1.80 | 4 | **50d** | Yes |
| SOL-USD | 2.5x | 4.5x | 1.80 | 4 | 200d | **No** |
| ZEC-USD | 2.0x | 3.5x | 1.75 | 4 | 200d | Yes |

ETH uses daily 50EMA (faster trend gate). ZEC uses 200EMA (slower, more stable).
Changing these requires re-running `backtesting/signal_scanner.py full_year`.

### Position Sizing
`LIVE_BALANCE_USD × position_size_pct` = order size.
Default: 5% of $100 = $5 per trade. The rest of the Coinbase account is untouched.

### Entry Filters (runner.py `_check_entry_filters`)
1. BTC 4h BEAR + correlation veto (corr ≥ 0.65 → full block; ≥ 0.35 → 50% size)
2. OKX funding rate veto (>20% annualized = crowded longs)
3. Bounce confirmation: price must recover +1.5x ATR above stop-exit
4. Velocity veto: asset down >5% in 24h → no long entry
5. **Per-asset daily EMA veto**: ETH uses 50EMA, ZEC uses 200EMA (see `_DAILY_EMA_PERIOD`)
6. Whipsaw guard: 2+ stops in 96h → no new entry

### Circuit Breakers (runner.py)
-5% drawdown → 50% size | -8% → 25% size | -12% → FULL HALT
These read `LIVE_BALANCE_USD` as the baseline. Do not hardcode dollar amounts.

## Key Files

| File | Purpose |
|------|---------|
| `schemas/signals.py` | Pydantic schemas for all inter-agent data — source of truth |
| `agents/base_agent.py` | Shared Claude client, JSON parsing, error fallback — touch carefully |
| `agents/breakout_agent.py` | Fully deterministic (no LLM) — safe to unit test |
| `pipeline/runner.py` | Main pipeline + all entry filters + circuit breakers |
| `pipeline/limit_orders.py` | Order lifecycle — uses `LIVE_BALANCE_USD` for sizing |
| `exchange/coinbase_client.py` | All Coinbase calls isolated here — ECDSA key file |
| `backtesting/walk_forward.py` | OOS validation — run this before changing ATR params |

## What NOT to Touch Without Reason

- `schemas/signals.py` — changing field names breaks all agents simultaneously
- `agents/base_agent.py` — all 7 agents depend on it; test carefully
- ATR multipliers in `pipeline/limit_orders.py` — validated, don't tune casually
- `_WHIPSAW_MAX_STOPS` and `_BOUNCE_CONFIRMATION_ATR` in `runner.py` — calibrated

## Coinbase API

Uses `coinbase-advanced-py` v1.8.2 with ECDSA key file (NOT ed25519, NOT env vars).
Client: `RESTClient(key_file="cdp_api_key.json")`
Response objects use attribute access, not `.get()` — see `_parse_balance()`.

## Obsidian Vault

Auto-generated nightly from logs via `backtesting/generate_journal.py`.
Windows Task Scheduler runs `update_obsidian.bat` every night at 23:00.
The vault is a growing knowledge base — future goal is RAG for the orchestrator.

## Pending Work (as of July 2026)

1. Watch first 2-3 live DRY_RUN signals → confirm $5 order sizes appear in logs
2. Switch `DRY_RUN=false` for ZEC-USD live test
3. Build test fixtures from first real signals (raw market snapshots for regression tests)
4. n8n pipeline for visual automation (good for portfolio/resume)
