# Coinbase spot universe inventory — 2026-09-17

READ-ONLY inventory. No strategy return, PF, Sharpe, expectancy, equity curve, or asset performance ranking is computed anywhere in this document or its source script (`backtesting/universe_inventory.py`). This is not a pre-registration and selects no universe, trial ID, or parameter.

## Deliverable 1 — universe inventory

- As-of date: 2026-09-18
- Source: Coinbase public REST API (`get_public_products`, `get_public_candles`), no credentials
- CSV: `docs/research/data/universe_inventory_2026-09-17.csv`
- Total USD spot pairs returned by the public endpoint: 407
  - status=online, trading enabled: 403
  - status=delisted or trading_disabled (still visible on this endpoint, excluded from candidate rules below): 4 (DAI-USD, EOS-USD, MKR-USD, RNDR-USD)

`min_market_funds_usd` = Advanced Trade's `quote_min_size`, the direct successor of the old Coinbase Pro `min_market_funds` field (minimum order size in the quote currency; quote is USD for every row here).

### Candidate universe rules (proposed, not selected)

No price behaviour was consulted to BUILD these rules — only listing history length and liquidity. (Cross-asset return correlation is measured further down, on the rules' members, and is a structural property of the universe rather than a selection criterion.)

| Rule | Pairs (N) | rho_bar | N_eff | Exposure clusters | Cluster floor @95% | Correlation-adjusted pooled floor @95% |
|---|---:|---:|---:|---:|---:|---:|
| A: >=3y history AND median 30d vol >= $1,000,000 | 31 | 0.5801 | 1.685 | 8 | 2.91–2.97% | 2.02–2.06% |
| B: >=2y history AND median 30d vol >= $250,000 | 61 | 0.5695 | 1.734 | 8 | 2.91–2.97% | 2.48–2.54% |
| C: >=1y history AND median 30d vol >= $5,000,000 | 20 | 0.5793 | 1.666 | 7 | 3.11–3.18% | 3.68–3.76% |

**What the cluster column measures, and what it does not.** A cluster here is a maximal span of portfolio exposure, and two spans are counted separately only when the WHOLE portfolio is flat for at least `CLUSTER_GAP_DAYS` = 60 days between them — every asset in the rule out of position simultaneously. With 20-61 assets that condition is nearly unreachable: something in the book is almost always in a position, so the count saturates BY CONSTRUCTION at a handful of spans and stops responding to universe size. It is a count of portfolio-wide quiet periods, and it cannot distinguish "8 independent episodes" from "8 flat gaps that happened to occur." **This table therefore establishes neither that breadth adds independent events nor that it fails to.** Both the saturation and the 31-pair and 61-pair rules landing on the same count are properties of the gap definition on a wide universe, not findings about independence. The correlation-adjusted column is the other end of the bracket; see below.

### Effective sample size behind the two floors

| Rule | N | rho_bar | N_eff | Common overlap window | Overlap (days / years) | Mean 55/20 entries per asset per year | Correlation-adjusted pooled n |
|---|---:|---:|---:|---|---:|---:|---:|
| A | 31 | 0.5801 | 1.685 | 2023-07-13 → 2026-09-18 | 1164 / 3.19 | 3.094 | 16.63 |
| B | 61 | 0.5695 | 1.734 | 2024-09-04 → 2026-09-18 | 745 / 2.04 | 3.102 | 10.97 |
| C | 20 | 0.5793 | 1.666 | 2025-08-20 → 2026-09-18 | 395 / 1.08 | 2.778 | 5.0 |

Estimator, fixed in advance and not searched over alternatives:

```
rho_bar = mean pairwise Pearson correlation of DAILY LOG RETURNS
          across the rule's assets over their common overlap window
N_eff   = N / (1 + (N - 1) * rho_bar)
pooled n = N_eff * entries_per_asset_per_year * years_of_overlap
```

rho_bar on daily returns is itself regime-dependent — report 03 of the literature review records BTC-alt R² swinging from 0.89 to roughly 0 within 24 months — so N_eff here is a full-window average and is not a guarantee for any sub-period.

**What N_eff says about breadth on this venue.** All three rules land in a narrow band — rho_bar 0.57–0.58, N_eff 1.67–1.73. On daily log returns these 20–61 USD pairs carry roughly the information of fewer than two independent assets, so pair count is a poor proxy for sample size here. The pooled n is driven more by the LENGTH of the common overlap window than by how many pairs a rule admits: rule B admits about twice rule A's pairs, but admitting 2-year-old listings shortens the window every member must share, and it ends with the smaller pooled n. This is a structural property of the universe measured on returns; it says nothing about what any rule would earn.

**The two floors bracket the decidable edge, and neither is a result.** They rest on different, non-nested assumptions:

- The **cluster floor** counts each portfolio-wide exposure span as ONE observation. It discards every trade inside a span, and it saturates on a wide universe as described above.
- The **correlation-adjusted pooled floor** counts N_eff independent assets, each firing at its measured event rate for the length of the common overlap window. It ignores that one asset's events cluster in time, and that a single trend regime moves many names together beyond what a daily-return rho_bar captures.

Read each rule's pair as a range, not as two rival estimates. Neither end dominates. For rule C the pooled count falls BELOW the cluster count — a short common overlap window costs more than N_eff above 1 recovers — so there the pooled floor is the wider of the two. Across both bases and all three borrowed SD scenarios: A 2.02–2.97%; B 2.48–2.97%; C 3.11–3.76%.

### Exact floors per borrowed SD scenario

| Rule | Basis | frozen 1.0% model | adopted 0.6/1.2% measured | candidate 0.5/0.9% |
|---|---|---:|---:|---:|
| A | cluster floor | 2.9717% | 2.9062% | 2.9150% |
| A | correlation-adjusted pooled | 2.0611% | 2.0156% | 2.0218% |
| B | cluster floor | 2.9717% | 2.9062% | 2.9150% |
| B | correlation-adjusted pooled | 2.5374% | 2.4815% | 2.4890% |
| C | cluster floor | 3.1768% | 3.1068% | 3.1163% |
| C | correlation-adjusted pooled | 3.7595% | 3.6766% | 3.6878% |

## Deliverable 2 — survivorship note

The public `get_public_products` endpoint is a snapshot of pairs Coinbase lists TODAY. It is not purely survivors-only in the strictest sense — 4 pairs above carry `status=delisted` and are still returned — but Coinbase does not guarantee that delisted symbols stay discoverable here indefinitely, and pairs delisted long enough ago are simply absent with no trace. Any inventory or universe built from this endpoint alone is biased UPWARD for a trend/momentum result: assets that delisted because they collapsed, were hacked, or were abandoned are underrepresented or missing, while assets that survived to today (by definition including the eventual winners) dominate. A future universe test on this venue would need a delisted-pair source to bound this bias.

A candidate delisted-pair source: **Binance** (`data.binance.vision`) publishes historical daily/monthly klines for symbols it has since delisted, without requiring credentials. It is a DIFFERENT VENUE — different listing calendar, different liquidity profile, different fee schedule, and Binance-delisted is not the same event as Coinbase-would-have-delisted. Using it to estimate a Coinbase survivorship correction would be a venue-mismatch approximation, not a direct measurement, and would need to be flagged as such. No data was pulled from Binance in this task — this is a documentation-only note, per the task's explicit constraint.

## Deliverable 3 — method notes

Event counting reuses `entry_exit_events` from `backtesting/stf_feasibility.py` unmodified (the same function `tests/test_stf_feasibility.py::test_events_carry_no_price_information` guards). Rule constants (`ENTRY_LOOKBACK=55`, `EXIT_LOOKBACK` from `stf_protocol.py`, `CLUSTER_GAP_DAYS=60`) are imported from `backtesting/stf_protocol.py`, also unmodified.

Unlike the frozen 7B audit's `portfolio_structure` (which requires one common start date across its fixed 4-asset universe), each asset here is evaluated from ITS OWN first-eligible date (first candle + ENTRY_LOOKBACK days) for clustering. The union of all qualifying assets' in-position flags is then clustered with the same `CLUSTER_GAP_DAYS` gap rule. This is a universe-inventory modeling choice appropriate to staggered listing dates, not a change to the frozen trial design, and it registers nothing.

**Correlation is the one price-movement quantity computed here.** It enters through a single function, `mean_pairwise_log_return_correlation`, which returns a mean scalar and the window it was measured on — no per-asset correlation and no return series of any kind leaves it, so nothing downstream can rank assets or build a P&L from it. `tests/test_universe_inventory.py` asserts this structurally.

**Decidable edge floor** = `1.645 * SD / sqrt(n)`, where SD is NOT measured on this universe — it is borrowed from `docs/research/2026-09-cost-sensitivity.md` section 6 (per-trade net return SD, in percent of position, n=114, on the FROZEN SINGLE-ASSET V2 ZEC mechanism — a mechanism with a hard stop, unlike STF-CLOSE-55-20). It answers one arithmetic question: *if a future sample here scattered like that sample did, how small a true per-trade edge could n observations distinguish from zero at one-sided 95%?* It is not, and must not be read as, a prediction of this universe's dispersion or edge. Two values of n are reported — the saturating cluster count and the correlation-adjusted pooled count — because neither is defensible alone.

Analysis window is frozen at `ANALYSIS_END` = 2026-09-18, the as-of date of the committed CSV, so re-running `--analysis-only` does not restate these numbers against a longer window than the inventory describes.

