# Contributing

Thanks for looking. This is a research repository before it is a trading
repository, so most of the rules below exist to stop a number from becoming
believable before it has earned it.

## Ground rules

**1. Never weaken the safety default.**
`DRY_RUN=true` stays the default. `LIVE_BALANCE_USD` is a per-trade sizing
baseline, not an aggregate cap; raising it still increases exposure and must be
a deliberate decision, not a side effect of another change. PRs that flip the
safety default or raise the baseline without a separately stated reason will be
closed.

**2. Research claims are pre-registered, not post-hoc.**
If a change is meant to produce a *result* — a new filter, a new parameter, a
new window — open [`docs/trial_registry.md`](docs/trial_registry.md) and
register the trial ID, the hypothesis and the acceptance rule **before** you run
the scan. A positive number found first and justified second is a selected
number, and this repository has already been burned by several.

**3. Gates fail closed.**
Every entry filter must block on an unreadable or unavailable input rather than
skip itself. A gate that silently no-ops when its input is missing is the exact
bug that invalidated 19 trades and an entire bull-market window — see the
warm-up correction in [`docs/research/`](docs/research/).

**4. Do not touch manifest-listed files casually.**
These are content-addressed in
[`docs/research/artifacts/manifest.json`](docs/research/artifacts/manifest.json):

```
backtesting/backtest.py        backtesting/research_runner.py
backtesting/equity_report.py   backtesting/signal_scanner.py
backtesting/oos_replay.py      exchange/coinbase_candles.py
```

Editing one — *including adding a comment* — changes its hash and turns
`research-verify` red. If a change to one is genuinely required, regenerate the
artifacts in the same PR and say so in the description.

## Setup

Python **3.13.5 exactly**. The research runner refuses any other interpreter,
because the manifest records the version the numbers were computed on.

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements-dev.txt
```

## Before you open a PR

```bash
ruff check .
python -m pytest -q
python backtesting/research_runner.py --verify-code
```

All three run in CI. Branch protection should require the `lint`, `tests` and
`research-verify` jobs. The full `research-verify` job regenerates the artifacts
byte-for-byte from publicly hydrated candles; run it locally with
`python backtesting/research_runner.py --verify` if you touched anything under
`backtesting/`.

The test suite is hermetic by construction: the root `conftest.py` pins safe
configuration before the first project import and denies outbound network at the
socket layer. A test that needs credentials or the internet is a test that needs
redesigning.

## Commit and PR style

- One concern per PR. Safety-relevant behaviour changes do not ride along inside
  a documentation PR.
- Explain *why* in the commit body, not just what. The interesting half of a
  change here is usually the reasoning.
- If your change alters a published number, say which number, by how much, and
  which artifact now supersedes which.

## What is unlikely to be merged

- Parameter tuning presented as an improvement without a registered trial.
- New indicators added because they are popular rather than because a
  pre-registered hypothesis predicted they would help.
- Anything that makes the README more confident than the evidence.
