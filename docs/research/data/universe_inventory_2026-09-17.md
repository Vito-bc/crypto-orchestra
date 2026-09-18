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

No price behaviour (return, momentum, volatility) was consulted to build these — only listing history length and liquidity.

| Rule | Pairs | Exposure clusters (STF-CLOSE-55-20, 7R method) | Decidable edge floor @95% (frozen-model SD / adopted SD / candidate SD) |
|---|---:|---:|---|
| A: >=3y history AND median 30d vol >= $1,000,000 | 31 | 8 | 2.9717% / 2.9062% / 2.915% |
| B: >=2y history AND median 30d vol >= $250,000 | 61 | 8 | 2.9717% / 2.9062% / 2.915% |
| C: >=1y history AND median 30d vol >= $5,000,000 | 20 | 7 | 3.1768% / 3.1068% / 3.1163% |

## Deliverable 2 — survivorship note

The public `get_public_products` endpoint is a snapshot of pairs Coinbase lists TODAY. It is not purely survivors-only in the strictest sense — 4 pairs above carry `status=delisted` and are still returned — but Coinbase clearly does not guarantee delisted symbols stay discoverable here indefinitely, and pairs that were delisted long enough ago are simply absent with no trace. Any inventory or universe built from this endpoint alone is biased UPWARD for a trend/momentum result: assets that delisted because they collapsed, were hacked, or were abandoned are underrepresented or missing, while assets that survived to today (by definition including the eventual winners) dominate. A future universe test on this venue would need a delisted-pair source to bound this bias.

A candidate delisted-pair source: **Binance** (`data.binance.vision`) publishes historical daily/monthly klines for symbols it has since delisted, without requiring credentials. It is a DIFFERENT VENUE — different listing calendar, different liquidity profile, different fee schedule, and Binance-delisted is not the same event as Coinbase-would-have-delisted. Using it to estimate a Coinbase survivorship correction would be a venue-mismatch approximation, not a direct measurement, and would need to be flagged as such. No data was pulled from Binance in this task — this is a documentation-only note, per the task's explicit constraint.

## Deliverable 3 — method notes

Event counting reuses `entry_exit_events` from `backtesting/stf_feasibility.py` unmodified (the same function `tests/test_stf_feasibility.py::test_events_carry_no_price_information` guards). Rule constants (`ENTRY_LOOKBACK=55`, `EXIT_LOOKBACK` from `stf_protocol.py`, `CLUSTER_GAP_DAYS=60`) are imported from `backtesting/stf_protocol.py`, also unmodified.

Unlike the frozen 7B audit's `portfolio_structure` (which requires one common start date across its fixed 4-asset universe), each asset here is evaluated from ITS OWN first-eligible date (first candle + ENTRY_LOOKBACK days). The union of all qualifying assets' in-position flags is then clustered with the same `CLUSTER_GAP_DAYS` gap rule. This is a universe-inventory modeling choice appropriate to staggered listing dates, not a change to the frozen trial design, and it registers nothing.

**Decidable edge floor** = `1.645 * SD / sqrt(n_clusters)`, where SD is NOT measured on this universe — it is borrowed from `docs/research/2026-09-cost-sensitivity.md` section 6 (per-trade net return SD, in percent of position, n=114, on the FROZEN SINGLE-ASSET V2 ZEC mechanism — a mechanism with a hard stop, unlike STF-CLOSE-55-20). It is reported as a resolving-power arithmetic exercise only: 'if a future sample here scattered like that sample did, how small a true per-trade edge could n_clusters distinguish from zero at one-sided 95%?' It is not, and must not be read as, a prediction of this universe's actual dispersion or edge. n_clusters, not pooled trade count, is used as the sample size — the more conservative choice, consistent with 7R's finding that trades within a cluster are not independent.

