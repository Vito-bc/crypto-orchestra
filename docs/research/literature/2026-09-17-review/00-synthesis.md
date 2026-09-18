<!--
PROVENANCE HEADER — added 2026-09-18 when this review was filed into the
repository. Everything below the header rule is the delivered report, verbatim.
-->

> **Filed:** 2026-09-18 · **Produced:** 2026-09-17 · **Trial ID:** none — a
> literature review is not a trial and registers nothing.
>
> **Method.** Five parallel web-research agents, one per standing problem
> (execution-cost structure; strategy-family cost tolerance; entry-filter
> evidence; momentum/breakout evidence; validation methodology), plus one
> synthesis pass over their five reports. Each agent rated its own sources on a
> quality scale running from **A** (peer-reviewed, primary text obtained) down
> to **E** (SEO/content marketing, cited only to show that nothing better
> exists). The scales are defined inside each report and are **not identical
> across them**. Sources reached only as an abstract, snippet or secondary
> summary are flagged in place; several primary texts were unreachable (403 or
> paywall) and are marked as such by the agent that tried.
>
> **Arithmetic on this project's own numbers.** Unless a passage states
> otherwise, every figure derived from this repository's own PF/n values uses a
> two-point **+1.75R / −1R** payoff approximation — the geometry implied by the
> frozen 2.0×/3.5× ATR bracket, not a measured return series. That model
> understates per-trade dispersion, so intervals computed from it are narrower
> and |t| larger than the measured series supports. The measured per-trade SD
> is in [`../../2026-09-cost-sensitivity.md`](../../2026-09-cost-sensitivity.md)
> §6 and supersedes the approximation wherever the two disagree.
>
> **Status.** Recorded as evidence, not as authorization. Nothing in this
> directory starts, revives or approves a trial, and nothing in it changes
> `DRY_RUN`, `LIVE_BALANCE_USD`, `ASSET_CONFIG`, V3 status, Phase 7B status or
> the 7R3b decision. Section 5 of `00-synthesis.md` is recorded as constraints
> on any **future** pre-registration — it is not a plan and not a
> recommendation.
>
> The body below is reproduced verbatim, including its "What the evidence does
> NOT establish" section.

---

# Literature review synthesis — 2026-09-17

**What this is.** A synthesis of five parallel literature reviews commissioned on 2026-09-17, each targeting one of the project's standing problems: (1) execution-cost structure, (2) cost tolerance of strategy families, (3) evidence behind the six entry filters, (4) short-horizon momentum/breakout evidence and backtest artifacts, (5) validation methodology. The five reports are filed alongside this document (`01-` … `05-`). Every number below is traceable to one of them, and each report carries its own source-quality ratings and a "what the evidence does NOT establish" section that this synthesis does not override.

**What this is not.** It is not a strategy recommendation and it does not select a replacement family. Project rule (CLAUDE.md, Project State item 6): any new family requires pre-registration before it touches data. Reading is not data contact, but a family chosen from reading inherits the multiple-testing exposure of the literature it came from (report 2, §9; report 3, Q6). This document records constraints and evidence; it authorizes nothing.

**Standing determinations unchanged:** DRY_RUN=true; LIVE NO-GO; V3 retired; Phase 7B unauthorized; 7R3b not run; the 30-day cost checkpoint is confirmatory only.

---

## 1. Bottom line

1. **The V2 momentum mechanism on ZEC is not "unproven" — it is bounded.** A conventional significance test on n=114 is uninformative (t = −1.41, p = 0.159; 95% PF interval ≈ [0.49, 1.11]). But an equivalence test — the correct tool for "is the effect at least X" — gives a **one-sided 95% upper bound on the true per-trade edge of +0.105%/trade (PF ≈ 1.045)** *net of the modeled 1.0% round trip* (report 5, §7b). At the measured operational cost (1.4% round trip since 2026-09-16; 1.8% before) even that most favourable bound is negative. "Underpowered to prove it loses" and "sufficient to rule out any profitable version of it" are both true; only the second is decision-relevant.
   *Caveat that must be closed before this is cited as a project number:* the bound rests on a per-trade SD derived from a binary ±R payoff model (4.70% of position). Real exits gap and fill partially, so true dispersion is higher and the bound wider. The executor's running cost-sensitivity study re-runs the frozen scan and can measure the SD directly; that measurement is requested as an addendum.

2. **The single-asset 4h program cannot reach a verdict in human time, regardless of whether an edge exists.** At ~22.6 trades/year, 80% power to detect a true PF of 1.2 needs ~1,000 trades ≈ 44 years; PF 1.1 needs ~3,700 ≈ 160 years (reports 4 §5a, 5 §3b). Any edge small enough to be plausible after retail fees is an edge this program design structurally cannot detect. This is a constraint on the *research program*, not on any strategy.

3. **Execution optimisation cannot rescue the mechanism.** Perfect maker-both-legs execution at the new tier is a 1.0% round trip — exactly the modeled cost under which V2 already shows PF 0.761 (report 1, arithmetic table). Converting take-profit exits to maker limits is cost hygiene, not a path to edge; it is also operationally non-trivial (`post_only` only on `limit_limit_gtc/gtd`, not on brackets; Hummingbot reports high rejection rates on Coinbase; the non-fill rate of a directional resting TP is unstudied).

4. **No documented strategy family meets this project's constraints.** Spot-only, 2–4 assets, small notional, ≥1.4–1.8% round trip, multi-year OOS including 2022 and 2025–26: every candidate in report 2 fails at least one criterion. The literature's cost assumptions cluster at 4–60 bps; the highest explicitly tested is ~1.4% one-way, for a long-short weekly-rebalanced factor across thousands of coins. Our cost level is beyond the right edge of everything published.

5. **The filter stack is degrees of freedom, not edge — and removing filters does not create edge either.** Six filters carry ~13 free parameters against 114 trades (report 3, Q6). No filter has documented support in the form used here; most have documented neutral-to-negative evidence in isolation (200-EMA weakest of lookbacks on alts; BTC-correlation thresholds inside ordinary regime variation; funding R² = 0.003 at 8h; bounce confirmation gives up ~0.75R by construction; whipsaw guards neutral-to-negative under negative trade autocorrelation). But the warm-up incident shows the opposite direction is beta, not edge: the ungated span happened to be a bull run.

**Decision recommended to the owner:** formally retire the V2/ZEC 4h momentum program as an activation candidate on the equivalence criterion (once the SD measurement lands), record that criterion as standing trial-registry policy, and treat the repository's rigorous negative result plus its research harness as the deliverable. Whether to open a new pre-registered program is a separate decision; §5 records the constraints it would have to satisfy.

---

## 2. What the reviews establish (firm)

**Costs**
- Results invert at cost levels one to two orders of magnitude below ours: Bysik & Ślepaczuk (2026) — hourly BTC, Sharpe 2.59 → −1.82 at **10 bps**; recovery came from cutting turnover 97%, not from a better model. Hudson & Urquhart (2021) — break-even costs across 25 rule classes 8–148 bps; ours exceeds every one. Novy-Marx & Velikov (RFS 2016) — few strategies above ~50% monthly one-way turnover survive costs. Gârleanu & Pedersen (JF 2013) — cost re-ranks signals by decay speed; a fast-decaying 4h signal is worth least under cost because it cannot amortise the round trip.
- Fee tier is not a lever at this notional: the first volume rung required ~250 round trips/month at $2; unreachable by orders of magnitude. The account moved from Intro 1 (0.6/1.2) to Intro (0.5/0.9) on 2026-09-16 by Coinbase's own schedule change, confirmed by the project's probe on 2026-09-17 (one reading; adoption pending a 4-reading cohort).

**Sample and power**
- BTC (t = −6.79) and ETH (t = −4.47) losses are real. SOL (t = −1.58) and ZEC (t = −1.41) are indistinguishable from zero and from each other (t = −0.19); no pairwise gap survives Bonferroni across six comparisons. "ZEC is the least bad" is the top of an unresolved ranking of degrees of losing, not a fact about ZEC (report 4, Q4).
- Minimum backtest length (Bailey et al. 2014, Theorem 3.1, reproduced exactly): at a target true Sharpe of 0.5, the 5.04-year ZEC window supports ~4–6 genuinely independent trials in total across project history (report 5, §3a). The window is, in practical terms, spent for this program.
- Profit factor is not an academic metric; every "good PF" threshold online is unsourced. The defensible statement is conditional: at n=114, PF 1.47 for t=2, PF 1.78 for t=3 (Harvey–Liu–Zhu).

**Momentum/breakout**
- No peer-reviewed literature exists on single-asset 4h ATR-bracket breakouts; all evidence is adjacent and negative. Institutional crypto trend (Man Group) reports per-market Sharpe "positive but small" with the edge coming from 10–15-coin diversification — unavailable on a 4-asset spot book.
- ATR brackets and fixed R:R: under a martingale, bracket geometry is expectancy-neutral by the optional stopping theorem; under positive drift it is a cost (Acar & Toffel; Kaminski & Lo); it helps only under genuine short-horizon positive autocorrelation. Take-profit is the more clearly documented cost of the two legs. No evidence for any specific R:R value or for ATR scaling over fixed percentages.
- Whether crypto cross-sectional momentum exists in tradeable form is actively contested (36%/week → 0.13%/week for the same anomaly depending on survivorship construction; Grobys & Shahzad argue the variance may be undefined). ZEC, as a privacy coin with delisting history, sits in the population the survivorship literature is about.
- The literature does **not** support "crypto trend decayed after 2021"; two sources across that boundary find it held. Our 2021-window collapse was period selection, consistent with the registry, not a regime shift.

**Methodology**
- The project's existing positions are the literature's positions: repaired walk-forward is a diagnostic, not OOS (Arnott/Harvey/Markowitz: "iterated out of sample is not out of sample"; Hansen–Timmermann: split-point mining triples rejection rates; Inoue–Kilian: OOS tests are lower-powered, not safer). Reviving a retired trial under a new ID is exactly Bailey et al.'s "the counter of trials cannot be turned back."
- DSR/PBO are widely cited and weakly validated; PBO is undefined at N=1 and DSR's corrections at T=114 are noisier than what they correct. They are usable only on a future pre-registered sweep whose per-trial return series are retained.
- Fail-closed typed gates (PASS/BLOCK/UNAVAILABLE) — the project's Phase 6.8 rule — are ahead of published guidance; no literature names the warm-up/silent-default class, and the documented magnitudes of the general class (declared ≠ executed mechanism) are consistently larger than the effects under study.

---

## 3. What the reviews explicitly do not establish

- That 4h single-asset breakouts work or don't — no literature on the configuration.
- That the strategy loses money — n=114 cannot say; only that any true edge is capped near PF 1.05 (pending SD measurement).
- That any filter hurts *in our data* — only that none has support in the form used and most have neutral-to-negative analogues.
- That a maker TP exit is practicable on Coinbase or what its non-fill rate would be.
- Any minimum viable account size (proportional fees make the question mis-specified; the real small-account penalty is being locked at the worst tier).
- That DSR/PBO/CPCV control Type I error on real data; that pre-registration improves realized strategy performance (logical necessity, no efficacy evidence).
- Anything about ZEC specifically.

---

## 4. Corrections to the project's own framing

1. "Consistent negative result" → **point estimate negative; loss not statistically established; edge bounded above at ~PF 1.05.** The equivalence framing replaces the significance framing.
2. "ZEC is the least-bad asset" → **unsupported ordering**; the choice of ZEC as the shadow asset rests on noise.
3. "Maker TP exits could fix the cost problem" → **ceiling is the modeled 1.0% baseline**; a return to PF 0.761, not a fix.

---

## 5. Constraints any future edge test would have to satisfy (derived, not selected)

These follow from the evidence; they are not a family recommendation.

- **Cost floor:** expected gross move per round trip must clear 1.4% (current) with margin; at 4h horizons on these assets that requires a hit rate the mechanism class has never shown. The only signal speed where documented edge direction and cost tolerance coincide is slow multi-week time-series trend (Kang & Ryu 2026; Detzel et al.; Novy-Marx & Velikov turnover threshold) — and even there per-market Sharpe is documented as small.
- **Power floor:** the program must generate enough independent trade events to reach a verdict in a bounded time. On one asset at ~23 trades/year it cannot. More assets or pooled inference trade against the documented negative transfer of the frozen mechanism; slower signals reduce trade count further. There is a real tension here that no design resolves for free.
- **Unseen data:** the 2020–2026 window is spent for this program (MinBTL). New evidence means forward/shadow data or a genuinely different universe, pre-registered before contact.
- **Pre-registration with SESOI and kill rule:** the trial entry must state, before the run, the smallest economically meaningful edge (derived from the fee schedule) and the equivalence-bound rule that retires the trial.
- **Retained per-trial return series**, so DSR/PBO become computable if a sweep is ever run.
- **No filter without a pre-declared rationale and a typed availability contract.**

Honest prior: within Coinbase spot, four assets and $2–100 notional, the literature offers nothing that clears the cost floor with documented evidence. A new program on this venue at this size is a low-prior bet. That is a legitimate finding, and the repository as a rigorous negative-result research harness is a legitimate deliverable independent of it.

---

## 6. Method changes to adopt (from report 5, §"Three things")

1. **Equivalence framing in the trial registry.** Every trial carries a pre-registered SESOI and a kill rule stated as a one-sided upper-bound test. Applied to V2/ZEC as the first case, explicitly labelled post-hoc for that trial and pre-registered for all subsequent ones.
2. **Per-trial return series in research artifacts**, not only summary statistics. Costs nothing now; the only thing that makes multiple-testing corrections computable later.
3. **Typed-availability contract as a repo-wide invariant test**: enumerate every declared gate and fail if any lacks a PASS/BLOCK/UNAVAILABLE return. The warm-up bug and the funding-rate contract are the same defect class; encode it rather than remember it.

---

## 7. Sequencing

1. Running: cost-sensitivity study (executor, branch `research/cost-sensitivity-2026-09`) — with addendum 2 (measured per-trade SD, one-sided upper bound at each cost level).
2. Next: record this review in the repository (this file + the five reports under `docs/research/literature/`), add the SESOI/equivalence policy to `docs/trial_registry.md`, apply it to V2/ZEC with the measured bound, and update CLAUDE.md's Validation Status.
3. Then, as small separate tasks: the typed-availability invariant test; per-trial return series in `research_runner.py` artifacts; the `cancel_order()` return-value defect in `check_and_fill` (live-only, pre-existing).
4. 2026-09-20: fourth reading of the new fee tier → follow-up PR adding the new `FeeSchedule`.
5. Whether to open a new pre-registered program: owner's decision, informed by §5.
