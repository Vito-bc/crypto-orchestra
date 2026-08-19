# Security Policy

## Reporting a vulnerability

Please report privately via
[GitHub Security Advisories](https://github.com/Vito-bc/crypto-orchestra/security/advisories/new).
Do not open a public issue for anything that could expose credentials or place
an unintended order.

Expect an acknowledgement within about a week. This is a personal research
project, not a funded product — there is no bounty, and response times are
best-effort.

## What is in scope

This project touches an exchange account, so the interesting failures are not
memory-safety bugs:

- **Credential exposure** — any path that could read, log, transmit or commit
  `.env` or `cdp_api_key.json`.
- **Unintended order placement** — anything that could place, size or cancel a
  real order while `DRY_RUN=true`, or bypass the preflight that must pass before
  `DRY_RUN=false`.
- **Risk-gate bypass** — an entry filter or circuit breaker that can be made to
  fail *open*: to skip itself when its input is missing, unreadable or
  attacker-influenced rather than blocking.
- **Sizing manipulation** — anything that makes order notional exceed
  `LIVE_BALANCE_USD x position_size_pct`.
- **Research integrity** — a way to make `research_runner.py --verify` or
  `--verify-code` report success against artifacts that do not actually match
  the code, environment or inputs that produced them.

## What is out of scope

- The absence of profit. The strategy is documented as unprofitable; that is a
  finding, not a vulnerability.
- Dependency CVEs with no exploit path in this codebase — open a normal issue.
- Anything requiring an attacker who already has local filesystem or shell
  access on the operator's machine.

## Operator-side expectations

If you run this yourself:

- Coinbase API keys must be created with `can_view=true`, `can_trade=true` and
  **`can_transfer=false`**. Preflight treats withdrawal rights as a blocking
  error — the bot must never be able to move funds off the exchange.
- Pin `COINBASE_PORTFOLIO_UUID` before considering `DRY_RUN=false`. Preflight
  fails when a key exposes multiple portfolios without one pinned.
- `LIVE_BALANCE_USD` is a per-trade sizing baseline, **not** an enforced
  aggregate spending cap: with `MAX_POSITIONS` concurrent trades, total exposure
  can exceed it. A true hard cap needs a dedicated portfolio plus an aggregate
  exposure limit, which is not implemented.
- `.env`, `cdp_api_key.json` and `obsidian_vault/` are git-ignored. If you ever
  commit one by accident, rotate the credential — removing the commit is not
  enough.
