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

### Fees (Coinbase Advanced base tier)
Maker 0.4%, taker 0.6% (`_ENTRY_FEE`, `_TP_FEE`, `_SL_FEE` in signal_scanner.py).
An earlier 0.2%/0.4% model understated fees and inflated backtest P&L.

### Active Assets
**ZEC-USD only, paper/shadow mode.** ETH/BTC/SOL are `enabled: False` in
`ASSET_CONFIG`. The frozen V2 mechanism transfers negatively to all of them
(see `docs/trial_registry.md`, 2026-08-09 pass: BTC PF 0.38, ETH 0.50, SOL 0.72).

### Per-Asset Strategy Config (signal_scanner.py `ASSET_CONFIG`)
| Asset | Stop | Target | R:R | Min Conds | Daily EMA | Enabled |
|-------|------|--------|-----|-----------|-----------|---------|
| BTC-USD | 2.0x | 3.5x | 1.75 | 4 | 50d | **No** |
| ETH-USD | 2.5x | 4.5x | 1.80 | 4 | **50d** | **No** |
| SOL-USD | 2.5x | 4.5x | 1.80 | 4 | 200d | **No** |
| ZEC-USD | 2.0x | 3.5x | 1.75 | 4 | 200d | Yes (shadow) |

ZEC params are frozen (git tag `v2-adx25-frozen`). Any change creates a new
trial — log it in `docs/trial_registry.md` first.

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

## Validation Status — read `docs/trial_registry.md` before believing any number

Authoritative record: `docs/trial_registry.md` (+ `docs/research/2026-08-strategy-review.md`).
Summary as of 2026-08-09:

- V2 momentum (ZEC): combined ~PF 1.00 on the four registry windows; **PF 0.86
  (-0.37%/trade, n=133) on the continuous 2021→2026 window**; the never-scanned
  2023→mid-2024 gap loses -2.35%/trade. Not profitable. Paper/shadow only.
- V3 ER-30 filter: pre-registered OOS trial continues (enforcement OFF), but
  integrated enforcement on the continuous window makes results worse
  (PF 0.69). Do not activate on IS grounds.
- Earlier "profitable, ready to go live" conclusions came from period-selected
  windows and an obsolete fee model. They are superseded.

**Do NOT switch `DRY_RUN=false` on current evidence.**

## Pending Work (as of Aug 2026)

1. Keep scheduler running so the V3 shadow journal accumulates forward-OOS
   signals (it was down 2026-07-23 → 2026-08-09; use
   `backtesting/oos_replay.py` to reconstruct missed windows deterministically)
2. Evaluate the 5 pre-registered V3 criteria every 5 closed accepted trades
3. If pursuing a new edge: pre-register a slow trend-following trial (the only
   family probe that wasn't structurally negative — see registry 2026-08-09)
4. n8n pipeline for visual automation (good for portfolio/resume)
