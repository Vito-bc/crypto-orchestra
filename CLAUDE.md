# Crypto Orchestra — CLAUDE.md

Project context for AI assistants. Read this before touching any file.

## What This Is

A multi-agent AI trading system **designed for** Coinbase Advanced Trade live
trading, but **currently restricted to `DRY_RUN` / paper-shadow mode. No
real-money trading is authorized.** Seven Claude sub-agents analyze BTC, ETH,
SOL, ZEC every 60 minutes and produce limit-order decisions; in the current mode
those orders are simulated, not placed.

`LIVE_BALANCE_USD=100` is the *cap* that would apply if live trading were ever
authorized — it is not evidence that money is at risk today. Going live requires
an explicit decision that the research does not currently support: see
"Validation Status" below, where the verdict is **LIVE NO-GO**.

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
venv\Scripts\python.exe backtesting/monte_carlo.py --scanner
venv\Scripts\python.exe backtesting/walk_forward.py           # historical diagnostic only
venv\Scripts\python.exe backtesting/walk_forward.py --verify  # reproduce its artifact

# Regenerate + verify the research artifacts (local candle cache, no network):
venv\Scripts\python.exe backtesting/research_runner.py
venv\Scripts\python.exe backtesting/research_runner.py --verify
venv\Scripts\python.exe backtesting/research_runner.py --verify-code  # cheap: no candles/git

# Rebuild the candle cache from PUBLIC Coinbase data (no credentials):
venv\Scripts\python.exe backtesting/hydrate_research_data.py

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
(`docs/research/artifacts/results.json`, trial `2026-08-warmup-semantics.v1`:
BTC PF 0.359 n=174, ETH 0.476 n=150, SOL 0.718 n=97 — and ZEC itself 0.761
n=114). Do not quote these from memory; they are asserted against the artifact
by `tests/test_research_provenance.py`.

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
| `backtesting/walk_forward.py` | REPAIRED (trial `2026-08-walkforward-repair.v1`). Uses the scanner's own frame assembly and simulator. **Historical diagnostic only** — its windows are in the registry's multiple-testing budget, so it is not clean OOS and cannot support activation. Its pre-repair numbers remain VOID. |
| `backtesting/research_runner.py` | Deterministic research runner — frozen config, registered boundaries, byte-identical artifacts |

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
Windows Task Scheduler runs `scripts/update_obsidian.bat` every night at 23:00.
The vault is a growing knowledge base — future goal is RAG for the orchestrator.

## Validation Status — read `docs/trial_registry.md` before believing any number

Authoritative record: `docs/trial_registry.md` (+ `docs/research/2026-08-strategy-review.md`
and `docs/research/2026-08-professional-review-addendum.md`).
Summary as of 2026-08-09:

- V2 momentum (ZEC): **PF 0.761 (-0.62%/trade, n=114) on the continuous
  2021-06-26→2026-07-12 window**; the never-scanned 2023→mid-2024 gap loses
  -2.35%/trade. Not profitable. Paper/shadow only.
- **Warm-up correction (2026-08-13, trial `2026-08-warmup-semantics.v1`):** the
  scanner used to fail OPEN when an indicator was still warming up, so a
  declared gate that could not be computed was silently skipped. 19 ZEC trades
  worth +22.28% ran without the 200-day daily EMA veto. Correcting it moved the
  continuous window from PF 0.855/-0.37% to PF 0.761/-0.62%, and collapsed
  `bull_2021` from n=25/PF 1.42 to **n=6/PF 0.96** — the entire apparent 2021
  bull-window edge came from the ungated span. Superseded artifacts are kept
  under `docs/research/artifacts/superseded/`; their numbers are NOT comparable.
- V3 ER-30 filter: **RETIRED / REJECTED FOR ACTIVATION (2026-08-09)**.
  Integrated enforcement on the continuous window makes results *worse*
  (**PF 0.706 with V3 vs PF 0.761 without**, trial `2026-08-warmup-semantics.v1`;
  the superseded figures PF 0.69 vs 0.86 measured a different mechanism — see
  `docs/research/artifacts/superseded/`). The earlier positive case came from
  period-selected windows. Enforcement stays OFF permanently for this trial ID;
  the former "n >= 20 closed trades" activation criteria are withdrawn. Further
  `v3_would_block` logging is diagnostic only and cannot reactivate it —
  that would require a new pre-registered trial ID. See `docs/trial_registry.md`.
- Earlier "profitable, ready to go live" conclusions came from period-selected
  windows and an obsolete fee model. They are superseded.

**Do NOT switch `DRY_RUN=false` on current evidence.**

## Pending Work (as of Aug 2026)

Immediate implementation brief:
`docs/tasks/2026-08-research-evidence-hardening.md`.

1. V3 is retired as an activation candidate (recorded in `docs/trial_registry.md`);
   enforcement stays off. Integrated-path replay and journal cohort/outcome
   semantics were fixed in PR #4; equity calendar-duration accounting and the
   reproducible research runner plus data/result manifests shipped in the same
   PR. Warm-up semantics were corrected on 2026-08-13 — see the trial registry.
2. ~~Phase 6.8~~ **DONE (PR #7).** `_check_entry_filters` fails closed on every
   unreadable input, funding has a typed applicable/not-applicable/unavailable
   contract, and the suite is hermetic: safe config pinned before the first
   project import, outbound network denied at the socket layer.
3. ~~Phase 6.9~~ **DONE.** Dependencies pinned exactly (canonical Python
   **3.13.5**, exact — `write_artifacts` and both verify paths refuse any other
   interpreter; `numpy`/`pandas`/`ta`/`pyarrow` recorded in the manifest). Code
   identity is content-addressed with `code_commit` demoted to an informational
   label. Input identity is the window-scoped logical OHLCV hash
   (`ohlcv-logical-v1`, scope `2020-01-01` → `2026-07-12`, both inclusive), so
   the tail the exchange keeps revising no longer breaks verification. Both
   `--verify-code` and the full `--verify` run in CI, the latter fed by
   credential-free public hydration. README corrected.
4. ~~Repair `backtesting/walk_forward.py`~~ **DONE (Phase 6.10).** It now builds
   its frame with `build_merged_frame` and simulates with `_simulate_trade`, so
   it cannot drift from the mechanism it validates. Enforcing the previously
   skipped gates removed 475 `daily_trend` and 118 `btc_regime` signals the old
   tool traded. Output is a deterministic artifact under
   `docs/research/artifacts/walk_forward/`, verifiable with `--verify`.
5. If pursuing a new edge: a slow trend-following trial would have to be
   pre-registered from scratch. The earlier "positive on all four assets"
   result is **LEGACY / UNVERIFIED** — its implementation is not in this
   repository, so it cannot be regenerated and is recorded under
   `non_reproducible` in `docs/research/artifacts/results.json`. It is not
   evidence that this family is promising; it is an unverified note.
6. Run LLM agents on scanner events rather than hourly until an ablation shows
   measurable incremental value.
7. n8n pipeline for visual automation (good for portfolio/resume)
