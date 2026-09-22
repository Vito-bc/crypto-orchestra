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

### Fees — two separate numbers, do not merge them

**Historical research (frozen): maker 0.4%, taker 0.6%.** `_ENTRY_FEE`,
`_TP_FEE`, `_SL_FEE` in `signal_scanner.py`, inherited by every research
consumer of `_simulate_trade`. These are a modeled assumption about the
2020-2026 backtest periods. An earlier 0.2%/0.4% model understated fees and
inflated backtest P&L. **Do not change them to match today's account tier** —
that would back-project a September 2026 measurement into historical windows.
Whether they were ever accurate is an open, unstarted research question.

**Prospective paper/shadow accounting (measured): maker 0.5%, taker 0.9%.**
`pipeline/fees.py` `CURRENT_SCHEDULE` (`coinbase-intro-2026-09-22`), tier
`Intro`, evidence: `docs/operations/fee_tier_2026-09-22.json`. Dated
2026-09-22 — a selected accounting-boundary timestamp on the schedule
(`accounting_effective_from` in the evidence file), not a live cutover;
`active_schedule()` does not consult it and always returns the current
schedule the moment it runs. Actual operational adoption is the date this
change merges to main, not this timestamp — see commit/PR history for that
date. These are current measured values for one account, not Coinbase
constants — tiers move with trailing volume.

**Superseded: maker 0.6%, taker 1.2%** (`coinbase-intro-1-2026-09`, tier
`Intro 1`, adopted 2026-09-15, evidence: `docs/operations/fee_tier_2026-09-15.json`).
No longer what `active_schedule()` returns, but still registered in
`pipeline/fees.py`'s schedule table so trades stamped under it keep settling
under it — the boundary below is per order, not per position, so a position
opened under the old tier and closed under the new one is expected, not a bug.

The boundary is **per order**, not per position: Coinbase prices each order at
the tier in force when that order is placed. The entry schedule is stamped on
the `PendingOrder` at placement and carried into the `Position`; the exit is a
separate order priced when it is sent. A trade may settle its two legs under
two schedules.

The evidence for the current schedule is 4 measured fee-tier readings
(2026-09-17, 09-18, 09-19, 09-22, no tier change across them; 09-20 and 09-21
were lost to the daily probe's scheduled-task WakeToRun failure). The adoption
bar has always been four readings with no tier change between them, not four
consecutive calendar days — the prior (superseded) cohort tolerated the same
kind of one-day gap on 2026-09-13. Unlike that cohort, this one was reproduced
from the local probe log (`backtesting/stf_cost_probe.py --report`, no
network) rather than independently audited; see
`docs/operations/fee_tier_2026-09-22.json` for the full evidence and that
distinction. Adopting this tier here is **operational fee accounting only**
and touches neither of the following two separate, already-standing
determinations:

  - **7R3b is not run.** That decision stands on its own — its synthetic
    construction preserves net expectancy by design, so no run of it can
    demonstrate a real edge one way or the other, independent of what any
    coverage contract says. This document does not claim that decision's
    prior review is verified by a repository artifact; it is recorded here
    without that claim.
  - **Phase 7B remains unauthorized**, a separate and still-open gate, pending
    its own declared coverage contract (see "Project State" item 5 below).

A future 30-day checkpoint of the cost probe would be confirmatory of the fee
measurement only, not a new authorization for either of the above.

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
Default: 2% of $100 = $2 per trade. The rest of the Coinbase account is untouched.

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

## Research program status (2026-09-20)

**The research program is COMPLETE. The project is in MONITOR mode.** Four
lines are recorded in `docs/trial_registry.md`, each closed on different
grounds — do not conflate them:

- **Closure 1 — V2/ZEC momentum: RETIRED, on EDGE.** The one-sided 95% upper
  bound on the true per-trade mean is below zero at operational cost.
- **Closure 2 — broad-universe Coinbase spot: NOT STARTED, on
  FEASIBILITY (correlation).** 20-61 pairs carry N_eff 1.67-1.73; the
  decidable-edge floor exceeds break-even before any SESOI is applied.
- **Closure 3 — 4h BTC/ETH perpetuals: NOT STARTED, on FEASIBILITY (funding
  cost + minimums, and independently the trade-count identity).** Perp funding
  is a documented cost for a long directional position, not an edge; CFM's
  per-contract minimums exceed the $100 cap for BTC/SOL/ZEC; and both declared
  mechanisms fail the floor gate (M1 22-29%/yr, M2 38-49%/yr against a
  10%/yr SESOI) by an identity that makes trading more often unable to help —
  `floor_annual = 1.645 · SD_bar · sqrt(bars in position per year / Y)`, where
  the trade count cancels out exactly.
- **Carry (long spot / short perp): PRICED, NOT STARTED.** Hedged, so the
  floor identity does not bind it — carry is the one class the closures above
  do not cover. Break-even funding is ~14.4-14.8%/yr; the measured premium has
  been below that for five years running (2025 and 2026-to-date both net
  negative). Not closed on edge or feasibility — simply not yet worth its cost.

**Capital scale does not reopen any of this.** The floor-gate identity is
scale-free — it depends on per-bar dispersion and time in market, not on
position size or account balance — so a larger `LIVE_BALANCE_USD` changes
nothing about whether a directional program can decide its own question.

**Two recorded conditions would reopen the line, and only these two:**

1. CFM's own funding, once observed, sustains above the carry break-even
   (~14.4-14.8%/yr) for longer than a typical cycle — see the funding monitor
   spec in `docs/research/2026-09-19-carry-scoping.md` §5 and operations guide
   in `docs/operations/cfm_funding_monitor.md`.
2. A venue with maker rebates or near-zero effective fees, reachable by a
   New York resident, becomes accessible — changing the cost side of the
   floor-gate identity rather than the statistics.

**What runs today:** the daily STF execution-cost probe (`stf_cost_probe.py`)
and hourly credential-free CFM funding monitor (`cfm_funding_monitor.py`).
**What does not run:** the seven-agent pipeline, any LLM call, and any order
path — none of these is required by, or currently used for, anything the
program above is deciding. The funding monitor records the condition for
re-evaluation and decides nothing itself.

## Validation Status — read `docs/trial_registry.md` before believing any number

Authoritative record: `docs/trial_registry.md`. Evidence base:
`docs/research/2026-09-cost-sensitivity.md`,
`docs/research/data/universe_inventory_2026-09-17.md`, and the filed literature
review at `docs/research/literature/2026-09-17-review/`.
Summary as of 2026-09-18:

- **V2 momentum (ZEC): RETIRED AS AN ACTIVATION CANDIDATE (2026-09-18, Closure
  1 in `docs/trial_registry.md`), on EDGE.** PF 0.761 (-0.62%/trade, n=114) on
  the continuous 2021-06-26→2026-07-12 window. The point estimate is negative,
  but the sample does **not** statistically establish a loss (t = -1.41,
  p = 0.159; 95% PF interval roughly [0.49, 1.13]). What it does establish is
  the bound: the one-sided 95% upper bound on the true per-trade mean is
  **-0.7220% at the adopted operational schedule and -0.3187% at the candidate
  schedule — below zero**, so a profitable version of this mechanism is ruled
  out at the costs we face. At the frozen 1.0% research model that same bound is
  **+0.1645%, above zero**: the rejection comes from the cost gap, not from the
  signal being disproved. The edge is not demonstrably absent; it is
  demonstrably smaller than the cost of trading it on this venue. (The
  never-scanned 2023→mid-2024 gap inside that window loses -2.35%/trade — no
  sub-window of it is a revival case.)
- **Execution optimisation cannot rescue it.** Perfect maker-both-legs execution
  at current rates reproduces a ~1.0% round trip — the modelled baseline under
  which this mechanism already measures PF 0.761. That baseline is the ceiling,
  not break-even.
- **The cross-asset ordering among BTC/ETH/SOL/ZEC is NOT established.** BTC
  (t = -6.79) and ETH (t = -4.47) lose for real, but no pairwise gap survives
  Bonferroni across six comparisons and SOL vs ZEC is a coin flip (t = -0.19).
  ZEC's selection as the shadow asset rests on noise.
- **Broad-universe Coinbase spot: NOT STARTED (2026-09-18, Closure 2), on
  FEASIBILITY — not on edge.** 20-61 USD pairs carry the information of fewer
  than two independent assets (rho_bar 0.57-0.58, N_eff 1.67-1.73), so the
  decidable-edge floor (2.02-3.76%) exceeds the 1.41% break-even gross move. No
  return hypothesis was evaluated and no P&L was computed: this is **not**
  evidence that such a program would be unprofitable.
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
- **Standing policy for every trial registered after 2026-09-18:** a
  pre-registered SESOI and equivalence kill rule, a declared decidable-edge
  floor that must not exceed that SESOI, retained per-trial return series, and a
  monotone trial counter. Pre-registered for future trials; the two closures
  above are its first, explicitly post-hoc, applications. See
  `docs/trial_registry.md`.

**Do NOT switch `DRY_RUN=false` on current evidence.**

## Project State and Remaining Work (as of Aug 2026)

The evidence-hardening brief under `docs/tasks/` is completed history, not an
instruction to repeat the work. Phases 6.7-6.10 are on `main`.

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
   interpreter; the declared `numpy`/`pandas`/`ta`/`pyarrow` lock pins and the
   installed versions are both recorded). Exact per-file source hashes,
   dependency-pin identity and the environment form `provenance_sha256`;
   `code_commit` is informational only. Input identity is the window-scoped
   logical OHLCV hash
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
5. **RESOLVED (2026-09-19, Closures 2-3).** Phase 7R's feasibility finding is
   now folded into Closure 2 (broad-universe spot, feasibility on correlation)
   and Closure 3 (4h BTC/ETH perpetuals, feasibility on funding cost +
   minimums, and independently the floor-gate identity). **Phase 7B is now
   moot**: the coverage contract it was gated behind no longer needs meeting,
   because the venue and asset class it targeted are closed on feasibility.
   The STF execution-cost probe continues to run daily as research
   infrastructure, independent of any trial.
6. **RESOLVED, in effect (2026-09-19, Closure 3).** The floor-gate identity
   generalizes across directional rules at this cost and volatility: no
   re-parameterization changes the annual floor, because the trade count
   cancels out of it exactly. This does not disprove the legacy result — no
   P&L was computed against it — but it explains why chasing it further is not
   decidable without a new venue, asset, or volatility regime. It remains
   **LEGACY / UNVERIFIED** and is still not to be used to select a family.
7. **RESOLVED, as moot (2026-09-19).** With the directional class closed on
   feasibility (Closures 2-3) and no active directional trial, there is no
   live decision for an agent-call-frequency ablation to optimize. Revisit
   only if a future closure reopens a directional line.
8. n8n pipeline for visual automation (good for portfolio/resume) — optional
   portfolio work, unaffected by the closures above.
9. **BUILT (2026-09-20). Funding monitor for Coinbase CFM perpetuals.**
   Specified in `docs/research/2026-09-19-carry-scoping.md` §5 and operated per
   `docs/operations/cfm_funding_monitor.md`: a public
   product-record poll (`get_public_products(product_type="FUTURE")`, no
   credential required), hourly, alerting on the carry break-even thresholds
   (14.8%/yr BTC, 14.4%/yr ETH) with fail-closed coverage reporting.
