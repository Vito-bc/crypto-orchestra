# Crypto Orchestra Domain Context

This document defines the canonical language for Crypto Orchestra. Use these
terms in code, tests, ADRs, logs, and agent conversations. It describes stable
domain concepts, not current strategy performance or live readiness.

Current experiment status and frozen parameters live in
`docs/trial_registry.md`. State-machine behavior lives in `pipeline/ledger.py`.
Do not infer profitability or live readiness from README text or old chat
summaries.

## Truth Ownership

### Coinbase-authoritative facts

Coinbase is authoritative for facts that occurred at the exchange:

- Whether an order was accepted and its exchange order ID.
- Exchange order status and executed quantity.
- Individual fills, fill time, price, quantity, fees, and liquidity role.
- Available and hold balances in the strategy's Coinbase portfolio.
- Product constraints such as increments, minimum size, and trading status.

### Ledger-authoritative facts

The SQLite ledger is authoritative for local strategy state:

- Submission intent and the exact client order ID.
- Risk epoch membership.
- Strategy stop, target, sizing decision, and reasoning.
- Idempotently applied fills and positions derived from those fills.
- Local P&L, risk classification, and reconciliation audit history.

Coinbase facts must not overwrite local strategy intent. Local state must not
invent or discard Coinbase orders or fills. A disagreement is a reconciliation
discrepancy, not permission to choose the more convenient value.

JSON files are transitional shadow records until ledger cutover. They are not
an independent authority once the ledger becomes load-bearing.

## Signal Pipeline

**Market snapshot**: look-ahead-safe market data available at evaluation time.
Only closed candles may influence a decision.

**Candidate signal**: the scanner emitted an entry opportunity because its
technical signal conditions passed. This does not imply strategy approval,
execution eligibility, order submission, or a fill.

**Strategy-approved signal**: a candidate signal that passed all enforced
strategy rules, including regime filters and veto logic. Shadow-only rules do
not affect this state.

**Execution-eligible signal**: a strategy-approved signal that also passed
live risk and operational gates: circuit breakers, entry filters, reconciled
account state, exposure checks, and the one-active-entry-per-asset rule.

**Blocked signal**: a candidate signal stopped by any downstream strategy,
risk, exposure, or operational gate. Blocked signals remain visible in the
OOS/shadow journal even though they cannot create a new ENTRY order.

**Shadow verdict**: a recorded counterfactual decision that has no effect on
live execution.

Avoid the unqualified term **accepted signal**. State which gate accepted it.
The legacy `v3_journal.accepted` field means "passed V3 enforcement"; it does
not prove that an order was submitted or filled.

## Execution Pipeline

**Submission intent persisted**: outbox TX-A committed an ENTRY order as
`SUBMITTING`, including its client order ID and trade intent. The external
outcome may still be unknown.

**Exchange acknowledged**: Coinbase returned a non-empty exchange order ID or
the order was otherwise found at Coinbase. This is an exchange fact.

**Acknowledgement recorded**: outbox TX-B attached the exchange order ID and
transitioned the local order to `OPEN`.

**Fill observed**: Coinbase reported an execution event.

**Fill applied**: `apply_fill()` idempotently recorded the fill and updated the
derived order and position state.

**Order attempt**: one client order ID from durable submission intent through a
terminal order state or unresolved human escalation. No-fill and rejected
orders are still order attempts.

Avoid the unqualified term **placement**. Use `submission intent persisted`,
`exchange acknowledged`, or `acknowledgement recorded` so the crash boundary is
explicit.

## Order States

These are local ledger states. Coinbase remains authoritative for the external
order facts that reconciliation imports into them.

**SUBMITTING**: local intent is durable, but exchange acceptance is unknown.

**OPEN**: exchange acceptance is known and recorded; the local ledger has not
yet applied fills for the full requested quantity.

**PARTIAL**: one or more fills were applied and unfilled quantity remains.

**FILLED**: the requested quantity was fully executed.

**CANCELLED**: Coinbase accepted the order and later cancelled its remainder.
It may have partial fills that are discovered after the local terminal
transition; it cannot execute after cancellation became effective at Coinbase.

**EXPIRED**: Coinbase accepted the order and its remaining quantity expired. It
may have partial fills that are discovered after the local terminal transition.

**REJECTED**: Coinbase definitively refused the submission. A rejected order
never became an exchange order and cannot have fills.

**UNRESOLVED**: a reconciliation finding, not an exchange order status. It
means local and exchange facts cannot yet be reconciled safely.

`Not found` at Coinbase is evidence of uncertainty, not evidence of rejection.
It leaves a local `SUBMITTING` order unresolved until retry or human review.

## Positions And Episodes

**Position**: exposure derived from applied ENTRY and EXIT fills. It begins with
the first ENTRY fill and becomes `CLOSED` only after the remaining base quantity
is fully exited.

**Position lifecycle**: first ENTRY fill through final EXIT fill. Use this term
for execution and P&L state; do not call it a market episode.

**Market episode**: the V3 concentration-analysis grouping implemented in
`v3_journal.py`: adjacent signals no more than 30 days apart belong to the same
episode. It measures event concentration and is independent of order lifecycle.

## Reconciliation

**Reconciliation**: a fail-closed comparison of local intent and derived state
against Coinbase orders, fills, balances, and holds. It resolves recoverable
differences and records every remaining discrepancy.

**Successful reconciliation**: the latest run completed, is within its required
freshness window, and has no unresolved discrepancies. Historical failed runs
remain audit history and do not permanently block the system.

An unresolved order or exposure discrepancy blocks new ENTRY placements. Signal
evaluation and shadow journaling continue. Cancellation and risk-reducing EXIT
actions remain available. Until an ADR defines a proven narrower scope, an
unknown exchange exposure blocks ENTRY placement globally.

Reconciliation must record real fills before resolving the conflicts they
create. A late fill is never ignored to preserve a preferred local state.

## Risk And Capital

**Risk epoch**: an evaluation period with a fixed starting capital and strategy
version boundary. It assigns each order and position to the configuration that
opened the risk.

**Epoch paper capital**: the starting reference capital for an epoch. It is not
current account NAV, available balance, or permission to spend that amount.

**Account NAV**: the sum of each strategy-portfolio balance (`available + hold`)
valued at current marks, less liabilities. Fees already debited from Coinbase
balances must not be subtracted a second time.

**Reconciled NAV**: account NAV computed from a successful, fresh reconciliation
snapshot. Position sizing must be bounded by reconciled NAV, available balance,
the approved capital cap, and the strategy's risk rule.

**Epoch drawdown breaker**: protection based on performance inside one risk
epoch.

**Account-level breaker**: protection based on total reconciled account NAV.
Reserve the word `global` for account-level protection; do not use it for an
epoch-scoped threshold.

**Risk-reducing action**: cancellation or EXIT that cannot increase absolute
exposure. It remains permitted when new ENTRY placement is halted.

## Research And Validation

**In-sample (IS)**: any data inspected or used to select an asset, parameter,
filter, entry rule, exit rule, or cost assumption.

**Forward OOS**: data strictly after the pre-registered freeze timestamp and
commit, untouched by parameter selection. Once inspected for selection, it is
no longer OOS for that decision.

**Trial**: one pre-registered hypothesis, baseline, parameter set, dataset,
cost model, and acceptance rule. Changing any selection dimension creates a new
trial.

**Shadow mode**: compute and journal a rule's decision without enforcing it.

**Enforcement mode**: a rule is allowed to block or modify live execution.

**Counterfactual outcome**: the simulated outcome of a blocked or unfilled
signal. It is research evidence, not realized P&L.

**Execution parity**: a replay that matches the live system's data timing, full
gate stack, state transitions, sizing, order policy, fills, fees, and slippage.
A replay that models only support-limit behavior is an execution or fill replay,
not execution parity.

## Safety Invariants

- Unknown exchange state fails closed for new ENTRY placement.
- Signal collection continues while execution is blocked.
- Every bot-created external order begins with durable local submission intent.
- Every real fill is immutable, idempotent, and applied exactly once.
- Rejection requires positive evidence; absence is not rejection.
- CANCELLED and EXPIRED orders may have fills; REJECTED orders cannot.
- One asset cannot receive a new ENTRY while it has active entry exposure.
- Position sizing never treats epoch paper capital as current NAV.
- Strategy parameter changes create a new trial and cannot rewrite prior OOS.
- Timestamps used for identity, ordering, and research boundaries are UTC.
