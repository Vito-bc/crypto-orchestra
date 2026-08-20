# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

Version numbers describe the *software*. They say nothing about strategy
performance — see [`docs/trial_registry.md`](docs/trial_registry.md) for that,
and note that several previously published numbers have been retracted.

## Unreleased

### Fixed

- `LIVE_BALANCE_USD` had two values: `.env.example` and the test bootstrap said
  `100`, while the code fallback said `10000` at three call sites and was
  hardcoded as `10_000.0` at three more. Running without a `.env` therefore
  sized orders — and measured the drawdown circuit breakers — against a balance
  100x the documented default. Now defined once in `pipeline/sizing.py`, default
  `100`.
- README advertised a 0.2% maker / 0.4% taker fee model. The scanner charges
  0.4% / 0.4% / 0.6%, and this project's own research records that the
  0.2%/0.4% model understated fees and inflated backtest P&L — so the README
  was publishing the exact model the research had already retracted.
- `scripts/register_pipeline_task.ps1` registered a 30-minute Task Scheduler
  trigger while the README, the scheduler loop and `PIPELINE_INTERVAL_MINUTES`
  all specify 60. The job ran at twice the documented rate.
- ruff `target-version` said `py310` while the pinned interpreter, CI and
  `research_runner._CANONICAL_PYTHON` all require exactly 3.13.5.
- Scanner-elevated trades fell back to a 5% size while `TRADE_SIZE_PCT` and the
  risk agent defaulted to 2%; the fallback now matches the configured default.
- `MAX_POSITIONS` and `DAILY_LOSS_LIMIT` looked like deterministic controls in
  `.env.example`, but were only passed to an LLM prompt that lacked the
  portfolio-wide position count and daily P&L needed to enforce them. The inert
  knobs and claims were removed; the deterministic circuit breakers remain.

### Added

- Research index at [`docs/research/`](docs/research/) — detailed results, the
  retraction history, ATR parameters, the fee model and reproducibility
  instructions, moved out of the README.
- Contribution, security and conduct policies; issue forms and a PR template
  that make trial pre-registration part of the review checklist.
- Visual identity: agent-network banner, upload-ready 1280×640 social preview,
  and a terminal card generated from real `--verify` output.

### Changed

- README rebuilt: the headline verdict stays above the fold, the detail moves to
  the research index, and the badge row now reports the version actually
  verified in CI.

## Baseline through 2026-08-19

First consolidated state of the research and paper-trading system. **Not a
release for live use**; `DRY_RUN=true` and live trading is not authorized.

### Added

- Seven specialist Claude sub-agents feeding an orchestrator, with a
  deterministic risk gate between the orchestrator's decision and any order.
- Deterministic research runner producing byte-identical artifacts, with data
  and result manifests (`2026-08-12`).
- Pinned reproducible environment: Python 3.13.5 exactly, result-determining
  libraries pinned and recorded, content-addressed code identity, window-scoped
  logical OHLCV input hash, and both verify paths running in CI fed by
  credential-free public hydration (`2026-08-16`, Phase 6.9).
- Hermetic test suite: safe configuration pinned before the first project
  import, outbound network denied at the socket layer (`2026-08-14`, Phase 6.8).
- Risk epoch system, SQLite order/signal ledger, and Coinbase preflight that
  treats withdrawal rights as a blocking error.

### Fixed

- Entry filters now fail **closed** on every unreadable input; funding has a
  typed applicable / not-applicable / unavailable contract (`2026-08-14`).
- Warm-up semantics: the scanner used to fail *open* when an indicator was still
  warming up, so a declared gate that could not be computed was silently
  skipped. 19 ZEC trades worth +22.28% had run without the 200-day daily EMA
  veto (`2026-08-13`).
- `backtesting/walk_forward.py` repaired: it now uses the scanner's own frame
  assembly and trade simulator, closing a train/test boundary leak and enforcing
  gates it previously skipped — which removed 475 `daily_trend` and 118
  `btc_regime` signals it had been trading (`2026-08-18`, Phase 6.10).
- SQLite connection race that could fail a live ENTRY or EXIT (`2026-08-09`).
- CI quality gate restored with a clean ruff baseline and a real tests job
  (`2026-08-02`).

### Retracted

- **V3 ER-30 filter — retired, not pending.** Integrated enforcement on the
  continuous window makes results worse (PF 0.706 with, PF 0.761 without). The
  earlier positive case came from period-selected windows; the "n ≥ 20 closed
  trades" activation criteria are withdrawn (`2026-08-12`).
- **Walk-forward out-of-sample results — void.** The tool never attached the
  daily frame, so it always validated a weaker mechanism than it reported. Its
  repaired windows remain historical diagnostics, not out-of-sample evidence.
- **Earlier "profitable, ready to go live" conclusions — superseded.** They came
  from period-selected windows and an obsolete fee model. Superseded artifacts
  are preserved under `docs/research/artifacts/superseded/`.
