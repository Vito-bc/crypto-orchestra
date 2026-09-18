# Literature review — 2026-09-17

Six files: one synthesis and the five reports it synthesises. Filed 2026-09-18,
unedited. Each file carries a provenance header; the body under each header is
verbatim as delivered, including its own "What the evidence does NOT establish"
section.

**This directory authorizes nothing.** It is evidence, not a decision and not a
trial. `DRY_RUN`, `LIVE_BALANCE_USD`, `ASSET_CONFIG`, V3 retirement, Phase 7B
authorization and the 7R3b decision are untouched by it and remain exactly as
[`../../../trial_registry.md`](../../../trial_registry.md) and
[`../../../../CLAUDE.md`](../../../../CLAUDE.md) record them.

## Method

Five parallel web-research agents, one per standing problem, then a synthesis
pass over their output. Each agent rated its own sources **A** (peer-reviewed,
primary text obtained) down to **E** (SEO/content marketing, cited only to show
that nothing better exists); the scales are defined per report and are not
identical across them. Where a primary text was unreachable, the agent says so
and downgrades the rating.

Figures derived from **this project's own** PF/n values use a two-point
+1.75R/−1R payoff approximation unless the passage says otherwise. That model
understates dispersion. The measured per-trade SD is in
[`../../2026-09-cost-sensitivity.md`](../../2026-09-cost-sensitivity.md) §6 and
supersedes it — the measured SD (5.1095% at the frozen fee model) is larger than
the approximation's 4.70%, so the approximation's intervals are too narrow.

## Index

| File | Question it covers | Load-bearing finding |
|---|---|---|
| [`00-synthesis.md`](00-synthesis.md) | Synthesis across the five | V2/ZEC is bounded, not merely unproven; the single-asset 4h program cannot reach a verdict in human time; execution optimisation ceiling is the modelled 1.0% baseline |
| [`01-execution-cost-structure.md`](01-execution-cost-structure.md) | Execution cost, fee tiers, maker/taker, minimum viable size | No published crypto strategy result was evaluated at anything near our round trip; perfect maker-both-legs execution only restores the cost assumption the backtests already used |
| [`02-strategy-families-cost-tolerance.md`](02-strategy-families-cost-tolerance.md) | Which families tolerate our cost level | No documented family meets spot-only + 2–4 assets + ≥1.4% round trip + multi-year OOS including 2022 and 2025–26 |
| [`03-entry-filter-evidence.md`](03-entry-filter-evidence.md) | Evidence behind the six entry filters | No filter has documented support in the form used here; ~13 free parameters against 114 trades; removing filters is not an edge either |
| [`04-momentum-breakout-evidence.md`](04-momentum-breakout-evidence.md) | Short-horizon momentum/breakout, ATR brackets, cross-asset ordering | No literature on 4h single-asset ATR-bracket breakouts at all; the BTC/ETH/SOL/ZEC ordering does not survive Bonferroni (SOL vs ZEC t = −0.19) |
| [`05-validation-methodology.md`](05-validation-methodology.md) | DSR/PBO, multiple testing, walk-forward, pre-registration, stopping rules | Equivalence testing is the frame that decides an underpowered sample; DSR/PBO are widely cited and weakly validated, and PBO is undefined at N = 1 |

## What this review changed in the repository

Two closures and one standing policy, all recorded in
[`../../../trial_registry.md`](../../../trial_registry.md):

1. **Standing evidence policy** — pre-registered SESOI, an equivalence kill
   rule, a decidable-edge floor gate, retained per-trial return series, and a
   monotone trial counter. Pre-registered for every trial registered after that
   commit; post-hoc for the two closures below, which are its first
   applications.
2. **Closure 1 — V2 / ZEC, on EDGE.** Retired as an activation candidate. The
   sample does not establish that the mechanism loses money; it does bound any
   true edge below zero at operational cost.
3. **Closure 2 — broad-universe Coinbase spot, on FEASIBILITY.** Not started,
   because it cannot decide its own question in a usable horizon. No return
   hypothesis was evaluated and no P&L was computed, so this is not evidence
   that such a program would be unprofitable.

The two closures rest on different grounds and are deliberately worded
differently. Do not merge them.

## Known weaknesses in these reports

Recorded here so a later reader does not have to re-derive them. They do not
change either closure.

- **`00-synthesis.md` §2 overstates report 01 on Hudson & Urquhart.** It says
  break-even costs across 25 rule classes are 8–148 bps and "ours exceeds every
  one." Report 01 (Q3) flags that the paper does not state unambiguously whether
  that figure is per transaction or per round trip, and that on the per-one-way
  reading 2 of the 25 cells (both Ethereum, shortest sample, ending at the 2017
  peak) would clear a 180 bps round trip. The synthesis drops that caveat.
- **`00-synthesis.md` §1 and `04-momentum-breakout-evidence.md` §5a disagree on
  the PF interval**, quoting roughly [0.49, 1.11] and [0.50, 1.13] for the same
  quantity. Both are approximations from the two-point payoff model; neither is
  measured. Treat the interval as "roughly [0.49, 1.13], and wider than that in
  reality."
- **`01-execution-cost-structure.md`'s closing arithmetic table is linear in the
  fee rate** (net = gross − round trip), which understates the effect because
  the exit fee is charged on gross proceeds rather than on entry notional. Its
  −1.42%/trade at the 1.8% round trip is the measured −1.4918%; its
  −1.02%/trade at the 1.4% round trip is the measured −1.0909%. Direction and
  conclusion are unaffected; use
  [`../../2026-09-cost-sensitivity.md`](../../2026-09-cost-sensitivity.md) for
  the numbers.
- **The 2026-09-16 Coinbase tier change is reported from three secondary
  outlets**, not from Coinbase's own page (all coinbase.com properties returned
  403 to the agent). The project's own probe corroborates it with one reading.
  Adoption still waits on the 4-reading cohort; nothing in this directory
  changes `pipeline/fees.py`.
