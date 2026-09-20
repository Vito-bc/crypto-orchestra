<p align="center">
  <img src="assets/banner.svg" alt="Crypto Orchestra — multi-agent crypto research and paper-trading system" width="100%">
</p>

<p align="center">
  <a href="https://github.com/Vito-bc/crypto-orchestra/actions/workflows/ci.yml"><img src="https://github.com/Vito-bc/crypto-orchestra/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI"></a>
  <a href="https://www.python.org/downloads/release/python-3135/"><img src="https://img.shields.io/badge/python-3.13.5-2F81F7?logo=python&logoColor=white" alt="Python 3.13.5"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-00E887" alt="MIT"></a>
  <a href="docs/trial_registry.md"><img src="https://img.shields.io/badge/research-complete%20%7C%20monitor%20mode-blue" alt="Research complete, monitor mode"></a>
</p>

This is a completed negative-result research program with a reproducible
evidence harness, not a live trading bot. It asked one question — does any
directional crypto mechanism this account can afford to run have a
demonstrable edge on Coinbase — and it answers **no**, on four separate and
differently-grounded lines of evidence, plus a fifth line (funding carry) that
is priced but not yet worth starting. Every headline number below is a
byte-identical regeneration from a pinned environment, including the numbers
that say the strategy does not work.

> [!WARNING]
> **Paper / shadow only — live trading is not authorized.** `DRY_RUN=true`;
> orders are simulated, not placed. The research program is **COMPLETE** and
> the project is in **MONITOR mode**: no directional edge has been
> demonstrated on any evaluated mechanism, asset, or venue. Full record:
> **[docs/research/](docs/research/)** · **[docs/trial_registry.md](docs/trial_registry.md)**.

## The research program

Seven Claude sub-agents and a deterministic risk engine exist to run a
directional momentum mechanism (ZEC-USD, paper/shadow) if one were ever
authorized. Over 2026, four separate lines of evidence were opened against
that possibility, each closed on its own grounds, plus a fifth priced and
left open. All four closures and the carry pricing are recorded in
[`docs/trial_registry.md`](docs/trial_registry.md), which is the authoritative
source for every number below.

| Line | Verdict | Ground |
|---|---|---|
| V2/ZEC momentum (the frozen mechanism above) | **RETIRED** as an activation candidate | **Edge** — one-sided 95% bound on the true per-trade mean is below zero at operational cost |
| Broad-universe Coinbase spot trend | **NOT STARTED** | **Feasibility** — 20-61 pairs carry the information of fewer than two independent assets (N_eff 1.67-1.73); the decidable-edge floor exceeds break-even before any minimum-effect-size gate is even applied |
| 4h BTC/ETH perpetuals (breakout and momentum) | **NOT STARTED** | **Feasibility** — perp funding is a documented *cost* for a long directional position, not an edge; Coinbase Financial Markets' per-contract minimums exceed the account's cap for most products; and both declared mechanisms fail the same decidable-edge floor by 2-5x |
| Long-spot / short-perp funding carry | **PRICED, NOT STARTED** | Hedged, so the floor identity does not bind it — the measured funding premium has simply been below this venue's break-even for five years running |

**The reusable result.** The perpetuals closure derives a floor identity that
applies to any directional rule on a given asset, not just the two it tested:

```
floor_annual = 1.645 · SD_bar · sqrt(bars in position per year / Y)
```

Trades-per-year times mean-hold-in-bars *is* bars-in-position-per-year, so the
trade count cancels out of the annual floor exactly. **Trading a directional
rule more often cannot lower the smallest edge that history could tell apart
from zero — it only multiplies the fee bill.** This is scale-free: it depends
on volatility and time in market, not on account size, so a larger balance
does not reopen any of the lines above.

**What would reopen a line, and what would not.** Two recorded conditions:
Coinbase Financial Markets' own funding (never yet observed — the exchange has
no funding history endpoint) sustaining above the carry break-even for longer
than a cycle, or a venue with materially lower fees becoming reachable by a
New York resident. A bigger account balance is explicitly not one of them.

**What runs today:** a daily execution-cost probe, credential-scoped
view-only. **What does not run:** the seven-agent pipeline, any LLM call on a
schedule, and any order path. The mechanism below is what would run if a line
were ever reopened and authorized — it is documented, tested, and currently
idle.

## Reproducibility

The claim this project actually stands on: the committed research artifacts
regenerate byte-for-byte, on a pinned interpreter and pinned result-determining
libraries, from a local candle cache.

<p align="center">
  <img src="assets/verify.svg" alt="research_runner.py --verify exits 0: committed artifacts reproduce byte-for-byte" width="100%">
</p>

Both this and the cheaper `--verify-code` run in CI, fed by credential-free
public hydration. Exact source-file hashes, the declared pins for the whole
numerical dependency closure, and the installed environment form one
content-addressed provenance identity; input identity is a window-scoped
logical OHLCV hash. The interpreter
is pinned to **3.13.5 exactly** — regenerating under anything else is refused
rather than silently producing different numbers. See
[docs/research/](docs/research/#reproducibility).

## The mechanism, as designed

```mermaid
flowchart LR
  subgraph A["7 sub-agents · claude-haiku-4-5 · 5-worker pool"]
    direction TB
    A1["technical — RSI, MACD, BB, EMA"]
    A2["macro — 4h regime, veto power"]
    A3["sentiment — Fear and Greed"]
    A4["whale — funding, BTC dominance"]
    A5["risk — ATR stop and target"]
    A6["news — asset headlines"]
    A7["breakout — price structure"]
  end

  S["signal scanner — momentum breakout"] --> A
  A --> O{{"orchestrator — claude-sonnet-4-6"}}
  O --> G["deterministic risk gate — 6 entry filters, circuit breakers"]
  G -->|allowed| L["limit order — simulated, DRY_RUN"]
  G -->|blocked| H["HOLD"]
```

If ever run continuously: every 60 minutes the pipeline closes positions that
hit stop / target / max-hold, reconciles pending limit orders, runs the
scanner, and — only if the scanner produces a signal — spends tokens on the
agents. The orchestrator's BUY still has to clear the risk gate before an
order is written. None of this is scheduled today; see "Research program"
above for what actually runs.

## Safety model

The agents propose; deterministic code disposes. Every gate below is plain
Python, testable without an LLM, and **fails closed** — an unreadable input
blocks entry rather than waving it through.

| Entry filter | Rule |
|---|---|
| BTC regime + correlation | BTC 4h BEAR: corr ≥ 0.65 → full block · 0.35–0.65 → 50% size |
| Funding rate | OKX annualized funding > 20% → block (crowded longs) |
| Bounce confirmation | Price must recover +1.5x ATR above the last stop-exit |
| Velocity | Asset down > 5% in 24h → no long entry |
| Daily EMA | Per-asset trend veto (BTC/ETH 50d, SOL/ZEC 200d) |
| Whipsaw guard | 2+ stops in 96h → block re-entry |

| Drawdown from peak | Action |
|---|---|
| −5% | Position size → 50% |
| −8% | Position size → 25% |
| −12% | **All trading halted** — manual review required |
| Daily loss −2% | Position size → 50% |

Sizing is `LIVE_BALANCE_USD x position_size_pct`, defined once in
[`pipeline/sizing.py`](pipeline/sizing.py) and defaulting to **$100**. The
circuit breakers measure against the same baseline. It is not an aggregate
spending cap: concurrent positions can add up to more than that baseline.
`TRADE_SIZE_PCT` defaults to **2%** and cannot exceed the deterministic **12%**
per-trade ceiling.

### Live trading

The code contains a live path for auditability, but this project does not
authorize its use and the Quick Start does not enable it. Activation would
require, in order: a repaired evaluation harness, a
pre-registered strategy with an acceptance rule fixed in advance, a forward
shadow trial that passes it, and an explicit human decision recorded in
[docs/trial_registry.md](docs/trial_registry.md). **None of those conditions is
met, and the research program above found no candidate that would meet them.**
`LIVE_BALANCE_USD=100` would remain only the per-trade sizing baseline — it is
not a portfolio cap and not evidence that money is at risk today.

## Quick start

Requires **Python 3.13.5** (exact — the research tooling refuses other
versions). No exchange credentials are needed for anything below.

```bash
python -m venv venv
venv\Scripts\activate                    # Windows
pip install -r requirements.txt
copy .env.example .env                   # fill in ANTHROPIC_API_KEY

python pipeline/runner.py ZEC-USD        # one paper run, one asset
python pipeline/scheduler.py             # continuous loop, every 60 min
streamlit run app.py -- --demo           # dashboard on the synthetic UI fixture
```

Research, offline and credential-free:

```bash
python backtesting/hydrate_research_data.py      # public Coinbase candles
python backtesting/research_runner.py --verify   # byte-identical regeneration
python -m pytest -q                              # hermetic: no network, no .env
```

> The `--demo` dashboard renders `demo/` — a hand-authored synthetic fixture for
> exercising the UI. It is **not** a backtest and **not** a track record. Real
> results are in [docs/research/](docs/research/).

## Repository layout

```
agents/          7 specialist sub-agents + orchestrator
pipeline/        runner (risk engine), limit orders, position tracker,
                 scheduler, sizing, dashboards, Telegram summaries
exchange/        all Coinbase calls, isolated for auditing
backtesting/     signal scanner, research runner, walk-forward, Monte Carlo,
                 the perpetuals feasibility gate, the carry scoping tool
tools/           price data, support/resistance levels, market positioning
schemas/         Pydantic contracts shared by every agent
docs/research/   research index, artifacts, manifests, closure documents
docs/            trial registry (the authoritative record), ADRs, task briefs
tests/           1,300+ tests — hermetic, network denied at the socket layer
```

## Configuration

| Variable | Required | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | Yes | Claude API key |
| `TELEGRAM_BOT_TOKEN` | No | Telegram alerts |
| `TELEGRAM_CHAT_ID` | No | Chat ID for alerts |
| `DRY_RUN` | No | `true` (default) = paper · `false` = real orders |
| `LIVE_BALANCE_USD` | No | Sizing baseline (default: 100) |
| `TRADE_SIZE_PCT` | No | Per-trade fraction (default: 0.02; maximum: 0.12) |
| `PIPELINE_INTERVAL_MINUTES` | No | Scheduler interval (default: 60) |
| `SUBAGENT_MODEL` | No | Default `claude-haiku-4-5-20251001` |
| `ORCHESTRATOR_MODEL` | No | Default `claude-sonnet-4-6` |

`LIVE_BALANCE_USD` is unrelated to `START_BALANCE` in `backtesting/` — that is a
simulation convention so historical replays report readable dollars, not a cap.

## Security

- `.env` and `cdp_api_key.json` (Coinbase ECDSA key) are git-ignored — never commit either.
- `DRY_RUN=true` is the default; no real order is placed without an explicit opt-in.
- Coinbase API keys must have `can_transfer=false`; preflight treats withdrawal rights as a blocking error.
- Authenticated order calls are isolated in `exchange/coinbase_client.py`;
  public candle hydration lives in `exchange/coinbase_candles.py`.
- Vulnerability reports: [SECURITY.md](SECURITY.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). The short version: research claims need a
pre-registered trial ID in [docs/trial_registry.md](docs/trial_registry.md)
before the scan, not after, and every PR must keep `lint`, `tests` and
`research-verify` green.

## License

[MIT](LICENSE).
