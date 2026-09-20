# Carry scoping — long spot + short perpetual, as a non-directional income source

**2026-09-19. READ-ONLY. No trial ID, no pre-registration, no recommendation.**

This prices funding carry on **this account's venue**. It is arithmetic on data
already hydrated: no strategy is backtested, no parameter is searched, no trial
is registered, and nothing is recommended. The conclusion section is titled
**"numbers"**, not "verdict", because the decision is the owner's.

No profit factor, expectancy, Sharpe, equity curve, drawdown or directional
return is computed anywhere in this document or in
[`../../backtesting/carry_scoping.py`](../../backtesting/carry_scoping.py).
Nothing here changes `DRY_RUN=true`, **LIVE NO-GO**, `LIVE_BALANCE_USD`,
`ASSET_CONFIG`, the spot fee schedule, or any of the standing determinations.
No `pipeline/` or `exchange/` code was modified. No authenticated call was made.

```powershell
venv\Scripts\python.exe backtesting/hydrate_carry_basis.py   # credential-free
venv\Scripts\python.exe backtesting/carry_scoping.py
venv\Scripts\python.exe backtesting/carry_scoping.py --verify
```

## Why the closures do not cover this, and why that is not re-argued

Closures 1-3 and the floor-gate identity bound **directional** programs, whose
per-trade variance is the underlying's. A hedged carry position cancels price
exposure; its income is the funding series this project has so far measured
only as a long's **cost**. Low variance means a high risk-adjusted return is
decidable in a short window, so the floor-gate identity works *for* such a
program rather than against it. **This document takes that as given and does
not re-derive it.** What it adds is the price tag on this venue.

## Two things to fix in the brief before reading the numbers

**The spot fee.** The brief says 0.5%/0.9%. That is the **candidate** tier read
from the account on 2026-09-18, which has **not** cleared its reading cohort.
`pipeline/fees.py` `CURRENT_SCHEDULE` is **Intro 1 at 0.6%/1.2%**, and the
standing policy names it as the schedule to price against, explicitly excluding
"a candidate tier that has not cleared its reading cohort". Both are therefore
reported, adopted first, exactly as
[`2026-09-cost-sensitivity.md`](2026-09-cost-sensitivity.md) does. **The choice
does not change any sign in this document**, only magnitudes.

**The funding window ends 2026-08-31.** The public archive publishes daily
kline files but no daily funding files, so every "2026" row covers **January to
August**, 243.7 days, and is annualised from that. This is declared, not
filled from a live endpoint.

## Venue mismatch

Funding, prices and the premium index are **Binance USDT-margined perps**, used
as proxy history for Coinbase CFM. Binance funds every 8h; **CFM funds hourly**.
What transfers is the annualised *level*, not the payment cadence. Basis,
leverage population, cap/floor rules and the liquidation engine all differ. CFM
has ~14 months of price history and **no funding history at all**
([`2026-09-18-perps-scoping.md`](2026-09-18-perps-scoping.md) §4-5). Fee rates,
contract sizes and margin fractions are **CFM's own**.

## The declared harvest rule

Long 1 unit spot + short 1 contract-equivalent perp. Opened when the trailing
**7-day** mean annualised funding is > 0, held while it stays > 0, closed when
it is <= 0, re-opened on the next crossing. The signal formed at one funding
event governs holding from the next, so no rate is harvested on the strength of
knowing it.

This is an **accounting convention** that makes cycle counting deterministic,
not a strategy to optimise. No alternative window was tried, and
[`../../tests/test_carry_scoping.py`](../../tests/test_carry_scoping.py)
asserts that exactly one window constant exists.

**The rule is a function of the funding series alone.** It cannot see a price.
That is what makes the position non-directional by construction rather than by
assertion, and it is enforced three ways: the rule's signature takes only a
funding frame, an AST walk asserts no price-shaped identifier appears in its
body, and a module-wide walk asserts no undeclared function receives one. Both
guards were mutation-tested.

---

## Deliverable 1 — gross and net carry per unit

Per unit of **spot notional**, at the **ADOPTED** schedule (0.6%/1.2% spot,
0.095%/0.100% perp). Cost per cycle is **1.9950%** of notional: 1.80% spot
round trip plus 0.195% perp round trip. Machine-readable copy:
[`data/carry_net_by_year_2026-09-19.csv`](data/carry_net_by_year_2026-09-19.csv).

### BTCUSDT — capital multiple x1.3830

| Year | Gross always-on | **Gross, harvest rule** | Cycles | Median cycle (d) | **Fee %/yr** | **Net on notional** | **Net on committed capital** |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2020 | 17.173 | 17.973 | 7 | 35.7 | 13.911 | **+4.061** | **+2.937** |
| 2021 | 30.574 | 30.740 | 6 | 4.2 | 11.956 | **+18.783** | **+13.582** |
| 2022 | 4.160 | 4.306 | 10 | 28.7 | 19.927 | **−15.621** | **−11.295** |
| 2023 | 7.857 | 7.819 | 3 | 27.0 | 5.978 | **+1.841** | **+1.331** |
| 2024 | 11.911 | 11.935 | 3 | 12.0 | 5.962 | **+5.973** | **+4.319** |
| 2025 | 5.121 | 5.125 | 3 | 105.7 | 5.978 | **−0.853** | **−0.617** |
| 2026 (Jan–Aug) | 2.612 | 3.117 | 8 | 5.8 | 23.924 | **−20.806** | **−15.044** |
| **Full window** | **11.803** | **12.019** | **40** | **17.0** | **11.967** | **+0.052** | **+0.038** |

### ETHUSDT — capital multiple x1.4185

| Year | Gross always-on | **Gross, harvest rule** | Cycles | Median cycle (d) | **Fee %/yr** | **Net on notional** | **Net on committed capital** |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2020 | 27.384 | 27.264 | 2 | 253.0 | 3.975 | **+23.290** | **+16.419** |
| 2021 | 37.494 | 37.375 | 5 | 21.3 | 9.964 | **+27.411** | **+19.324** |
| 2022 | 0.786 | 2.834 | 10 | 16.8 | 19.927 | **−17.093** | **−12.050** |
| 2023 | 8.250 | 8.247 | 1 | 345.3 | 1.993 | **+6.254** | **+4.409** |
| 2024 | 12.947 | 12.927 | 3 | 6.0 | 5.962 | **+6.965** | **+4.910** |
| 2025 | 4.923 | 4.834 | 5 | 60.0 | 9.964 | **−5.130** | **−3.616** |
| 2026 (Jan–Aug) | 1.565 | 2.263 | 7 | 10.0 | 20.933 | **−18.670** | **−13.162** |
| **Full window** | **13.953** | **14.277** | **33** | **21.3** | **9.873** | **+4.405** | **+3.105** |

### At the CANDIDATE schedule (0.5%/0.9%), cost per cycle 1.5950%

| Year | BTC net on notional | BTC net on capital | ETH net on notional | ETH net on capital |
|---|---:|---:|---:|---:|
| 2020 | +6.851 | +4.953 | +24.087 | +16.980 |
| 2021 | +21.181 | +15.315 | +29.409 | +20.732 |
| 2022 | −11.626 | −8.406 | −13.098 | −9.233 |
| 2023 | +3.040 | +2.198 | +6.654 | +4.691 |
| 2024 | +7.168 | +5.183 | +8.160 | +5.753 |
| 2025 | **+0.346** | **+0.250** | −3.132 | −2.208 |
| 2026 (Jan–Aug) | −16.010 | −11.576 | −14.473 | −10.203 |
| **Full window** | **+2.452** | **+1.773** | **+6.384** | **+4.501** |

### Reconciliation with the perps gate

The short leg's **always-on** receipt is the exact mirror of the long's
always-on cost. This document reads **11.8025%/yr** (BTC) and **13.9527%/yr**
(ETH); the perps-gate document reads 11.8057 and 13.9565 for the same series.
The 0.003-0.004pp difference is the annualisation denominator — that document
counts whole days between first and last funding event, this one counts elapsed
time — and it is asserted by an integration test that fails if the two
documents ever disagree about the same series by more than 0.05pp.

### Is it net positive?

**2025: net NEGATIVE** on both assets at the adopted schedule (BTC −0.85%/yr,
ETH −5.13%/yr on notional). At the candidate schedule BTC turns marginally
positive (+0.35%) and ETH stays negative (−3.13%). This matches the literature:
Borri et al. report crypto carry **negative in 2025**.

**2026 to August: net NEGATIVE**, decisively, on both assets and both
schedules — **−14% to −21%/yr** on notional. Gross funding collapsed to
2.3-3.1%/yr while the rule cycled 7-8 times in eight months, so fees alone ran
21-24%/yr.

**The full-window BTC figure is the one to sit with: +0.052%/yr on notional
over 6.67 years.** Not positive, not negative — zero, to two decimal places,
after this venue's fees. ETH keeps +4.41%/yr, and essentially all of it was
earned in 2020-2021.

---

## Deliverable 2 — capital and expressibility

Per unit, at the 2026-09-18 CFM notionals. Reserve is the declared 25% of
posted short margin. Machine-readable copy:
[`data/carry_capital_2026-09-19.csv`](data/carry_capital_2026-09-19.csv).

| Product | Unit | Spot notional | Short margin | Reserve | **Total per unit** | Units at $10k | Units at $20k | **Units at $100 cap** |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `BIP-20DEC30-CDE` | 0.01 BTC | $808.30 | $247.66 | $61.92 | **$1,117.88** | 8 | 17 | **0** |
| `ETP-20DEC30-CDE` | 0.1 ETH | $258.40 | $86.51 | $21.63 | **$366.54** | 27 | 54 | **0** |

**At `LIVE_BALANCE_USD = 100`, not one unit of either is affordable.** The
cheapest hedged unit on this venue costs 3.7x the entire cap. This is the same
quantisation wall the perps gate recorded, and it binds here before any
question of whether carry pays.

**Quantity matching.** The hedge is exact at the contract granularity: one BIP
contract is 0.01 BTC and one ETP contract is 0.1 ETH, and Coinbase spot accepts
fractional size, so a spot lot can match a contract exactly. The constraint runs
the other way — **the short leg moves in whole contracts only**, so spot must be
held in integer multiples of 0.01 BTC or 0.1 ETH.

**Residual directional exposure from a rounding mismatch is not small.** Holding
0.015 BTC spot against one contract leaves 0.005 BTC — **33% of the position**
— outright long. At ETH's $258 unit the granularity is finer relative to a
small account, but the same arithmetic applies. A "hedged" position carrying a
third of its notional unhedged is a directional position, and every closure in
this repository applies to it.

---

## Deliverable 3 — margin-event frequency

**This is cash-flow risk, not P&L.** A short perp loses when price rises; the
spot leg gains the same amount at the same instant, so the *position* is
economically flat. What is not flat is **where the money sits**. The spot gain
is unrealised and sits in the spot portfolio; the futures loss is realised
against posted margin in the CFM wallet. Coinbase exposes `list_futures_sweeps`
and `schedule_futures_sweep`, so a transfer path between the two exists, but it
is a **scheduled operation, not an automatic transfer of unrealised spot
gains** — and realising the spot gain to fund the margin means selling the
hedge. Counted below is how often that problem would arise.

Every 4h bar is treated as an entry at its close; the adverse excursion is the
highest high over the following window. **Two counts, because one alone
misleads**: `episodes` skips a full window after each breach so one rally counts
once, while the per-entry probability treats every bar as a start. Machine-
readable copy:
[`data/carry_margin_events_2026-09-19.csv`](data/carry_margin_events_2026-09-19.csv).

### BTCUSDT — short margin 30.64%

| Window | Threshold | Price rise required | Episodes | Per year | Per-entry probability |
|---|---|---:|---:|---:|---:|
| 1 day | 50% of margin | 15.32% | 23 | 3.43 | 0.40% |
| 1 day | 75% | 22.98% | 4 | 0.60 | 0.06% |
| 1 day | **100% (liquidation)** | 30.64% | **1** | **0.15** | 0.04% |
| 3 days | 50% | 15.32% | 51 | 7.60 | 2.57% |
| 3 days | 75% | 22.98% | 15 | 2.24 | 0.67% |
| 3 days | **100%** | 30.64% | **4** | **0.60** | 0.18% |
| 7 days | 50% | 15.32% | 68 | 10.13 | 9.29% |
| 7 days | 75% | 22.98% | 29 | 4.32 | 3.39% |
| 7 days | **100%** | 30.64% | **10** | **1.49** | 1.00% |

### ETHUSDT — short margin 33.48%

| Window | Threshold | Price rise required | Episodes | Per year | Per-entry probability |
|---|---|---:|---:|---:|---:|
| 1 day | 50% of margin | 16.74% | 33 | 4.92 | 0.58% |
| 1 day | 75% | 25.11% | 5 | 0.75 | 0.12% |
| 1 day | **100% (liquidation)** | 33.48% | **3** | **0.45** | 0.06% |
| 3 days | 50% | 16.74% | 83 | 12.37 | 4.57% |
| 3 days | 75% | 25.11% | 20 | 2.98 | 0.94% |
| 3 days | **100%** | 33.48% | **6** | **0.89** | 0.39% |
| 7 days | 50% | 16.74% | 100 | 14.90 | 14.28% |
| 7 days | 75% | 25.11% | 47 | 7.00 | 5.62% |
| 7 days | **100%** | 33.48% | **21** | **3.13** | 1.88% |

### Liquidation-threshold episodes by year, 7-day window

| Year | BTC | ETH |
|---|---:|---:|
| 2020 | 4 | 8 |
| 2021 | 4 | 7 |
| 2022 | 0 | 2 |
| 2023 | 1 | 0 |
| 2024 | 1 | 2 |
| 2025 | 0 | 1 |
| 2026 (to 09-17) | 0 | 1 |

**A full-margin event is not rare.** Without a top-up, the short leg would have
been liquidated 10 times (BTC) and 21 times (ETH) over 6.7 years on a 7-day
view. Liquidation destroys the hedge at the worst moment and leaves the spot leg
outright long — turning a carry position into exactly the directional exposure
the closures bound. Margin events cluster in 2020-2021, the same years that
produced essentially all the carry income, which is the BIS finding reproduced:
**high carry coincides with the conditions that threaten it**.

**Maintenance margin was not observed.** The scoping document recorded posted
overnight initial margin; the level at which CFM actually issues a call is not
in the record, so the 50%/75%/100%-of-initial thresholds are a declared ladder,
not the venue's own trigger.

---

## Deliverable 4 — basis, as a caveat with a magnitude

Binance publishes the **premium index** credential-free, so this is measured
rather than left qualitative. 14,664 4h observations per symbol, 2020-01-01 to
2026-09-17. Machine-readable copy:
[`data/carry_basis_2026-09-19.csv`](data/carry_basis_2026-09-19.csv).

| Symbol | Mean | **SD** | **Worst 1-day change** | 99th pct abs 1-day change |
|---|---:|---:|---:|---:|
| BTCUSDT | −1.36 bps | **5.71 bps** | **107.76 bps** | 12.82 bps |
| ETHUSDT | −0.81 bps | **6.45 bps** | **62.47 bps** | 17.75 bps |

**No basis P&L is modelled here**, and the function that computes these returns
two scalars so that none can be.

What the magnitude says: day to day the basis is small against the fee cost —
a 5.7 bps SD against a 199.5 bps round trip. But the worst single-day move is
**108 bps on BTC**, which is more than half a round trip appearing or vanishing
in a day. Entry and exit timing therefore carries a basis cost of the same order
as the fees, and a position opened at a wide basis and closed at a narrow one
pays it. CFM's basis is **not** Binance's, so these are an order-of-magnitude
guide to a quantity this venue has not been measured on.

---

## Deliverable 5 — regime map and monitor design

### Break-even funding, two ways

Cost per cycle is 1.9950% of notional (adopted). Amortising it needs a cycle
rate, and **the cycle-length distribution is heavily skewed** — BTC's median
cycle is 17 days against a mean of 54.8; ETH's is 21.3 against 66.5. The two
amortisations answer different questions and both are reported:

| | BTC | ETH |
|---|---:|---:|
| Cost per cycle | 1.9950% | 1.9950% |
| Median cycle | 17.0 d | 21.3 d |
| Mean cycle | 54.8 d | 66.5 d |
| Top-up friction allowance | 2.81%/yr | 4.52%/yr |
| **Break-even, median cycle** | **45.68%/yr** | **38.68%/yr** |
| **Break-even, realised cycle rate** | **14.78%/yr** | **14.39%/yr** |

The **median-cycle** figure is what a *typical* cycle must earn to pay for its
own round trip — the conservative reading, and the one the brief specifies. The
**realised-rate** figure uses the cycles actually observed per year and is what
the program *as run* must average. Quoting only the first overstates the
requirement for a program that spends most of its time inside long cycles;
quoting only the second hides how often a short cycle fails to cover its costs.

The **top-up friction** is declared arithmetic: each non-overlapping 7-day
episode breaching 50% of margin is charged one spot round trip on the amount
topped up, which is 50% of the margin fraction of notional. It does **not**
price the directional risk of running partially unhedged while cash is raised,
which is unpriced here.

### Regime map — fraction of time trailing 7-day funding exceeded each level

Adopted schedule. Machine-readable copy:
[`data/carry_regime_map_2026-09-19.csv`](data/carry_regime_map_2026-09-19.csv).

| Year | BTC > BE | > 2×BE | > 3×BE | BTC > realised BE | BTC > 0 | BTC median | ETH > BE | > 2×BE | > 3×BE | ETH > realised BE | ETH > 0 | ETH median |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2020 | 9.56% | 1.37% | 0 | 39.89% | 85.97% | 10.95 | 22.86% | 5.46% | 1.82% | 48.82% | 99.00% | 13.67 |
| 2021 | 23.20% | 7.76% | 0.37% | 54.43% | 91.96% | 16.87 | 33.15% | 16.53% | 8.31% | 58.26% | 97.44% | 19.10 |
| 2022 | **0** | 0 | 0 | **0** | 88.86% | 4.82 | **0** | 0 | 0 | **0** | 67.85% | 3.21 |
| 2023 | **0** | 0 | 0 | 6.12% | 98.26% | 6.83 | 0.18% | 0 | 0 | 9.22% | 97.90% | 6.83 |
| 2024 | 3.37% | 0 | 0 | 23.86% | 94.35% | 10.41 | 5.56% | 0 | 0 | 24.95% | 98.82% | 10.32 |
| 2025 | **0** | 0 | 0 | **0** | 94.79% | 5.18 | **0** | 0 | 0 | **0** | 97.99% | 4.75 |
| **2026 (to 08-31)** | **0** | **0** | **0** | **0** | **69.41%** | **3.01** | **0** | **0** | **0** | **0** | **62.96%** | **1.86** |
| Full window | 5.42% | 1.37% | 0.05% | 18.66% | 90.08% | 6.83 | 9.27% | 3.30% | 1.52% | 21.20% | 90.16% | 7.44 |

**The 2026 reading to date is the clearest line in this document. Trailing
7-day funding has not once exceeded either break-even, on either asset, in
2026.** Median trailing funding is 3.01%/yr (BTC) and 1.86%/yr (ETH) against a
realised-rate break-even near 14.4%/yr. Funding is still positive most of the
time — 69% and 63% of readings — but positive is not the bar; **the bar is
roughly 14-15%/yr, and 2026 is running at a fifth of it.**

Since 2022 the median-cycle break-even has been cleared in only one year (2024,
3-6% of the time) and never twice over.

### Monitor spec — implemented 2026-09-20

**CFM has no funding-history endpoint, so a monitor must record forward.** Every
hour it misses is a permanently unrecoverable gap; there is no way to backfill.

[`../../backtesting/cfm_funding_monitor.py`](../../backtesting/cfm_funding_monitor.py)
implements the following through the cost probe's public client; operating and
verification instructions are in
[`../operations/cfm_funding_monitor.md`](../operations/cfm_funding_monitor.md):

| | |
|---|---|
| **Call** | `get_public_products(product_type="FUTURE")` via the probe's existing `_public_client()` |
| **Credential** | **none — the product record is public.** This sidesteps the view-only-key gap entirely: the scoping document found the repo's only key is a *trading* key that `_REQUIRED_PERMISSIONS` correctly refuses, and a funding monitor never has to meet that gate because it never authenticates |
| **Products** | `BIP-20DEC30-CDE`, `ETP-20DEC30-CDE` |
| **Fields** | `future_product_details.funding_rate`, `.funding_time`, `.funding_interval` |
| **Interval** | `funding_interval` is **3600s**. The record carries a *snapshot* only, so polling must be at least hourly; a missed poll is a lost observation, not a delayed one |
| **Storage** | one JSON line per product per poll, appended to the probe's existing `logs/*.jsonl` pattern, with `funding_time` as the dedup key so a double poll inside one interval does not double-count |
| **Alert** | trailing 7-day mean annualised funding crossing **14.8%/yr (BTC) or 14.4%/yr (ETH)** (realised-rate break-even, adopted schedule). A second threshold at **45.7%/yr** marks the median-cycle break-even |
| **Health check** | alert on *coverage*, not only on level: a monitor with gaps produces a trailing mean over an unknown denominator, which is worse than no reading |

**Cost of building it was low.** It is one public call on an hourly schedule,
with no new credential and no new permission surface. **Cost of not running it:
the clock restarts whenever it stops** — there is no history to buy later, so a
future decision to evaluate carry can use only the observations actually kept.

---

## Numbers

Gathered, with no conclusion attached:

- **BTC carry netted +0.05%/yr on notional over 6.67 years** at the adopted
  schedule. ETH netted +4.41%/yr, essentially all of it in 2020-2021.
- **2025 is net negative** on both assets at the adopted schedule (BTC −0.85%,
  ETH −5.13%); BTC turns marginally positive (+0.35%) at the candidate one.
- **2026 to August is net negative by 14-21%/yr** on both assets and both
  schedules.
- **Break-even funding is 14.4-14.8%/yr** on the realised cycle rate, or 38-46%/yr
  on a typical cycle. Trailing 7-day funding has **not reached either in 2026**,
  and has cleared the median-cycle figure in only one year since 2022.
- **Net on committed capital is 0.72x the figure on notional** (BTC), because
  margin and the declared reserve add 38% to the capital a unit ties up.
- **One hedged unit costs $1,118 (BTC) or $367 (ETH)**, against a
  `LIVE_BALANCE_USD` of $100. Zero units are affordable.
- **A full-margin loss on the short leg occurred 10 times (BTC) and 21 times
  (ETH)** over 6.7 years on a 7-day view, concentrated in the same years that
  produced the income.
- **Basis SD is 5.7-6.5 bps** with a worst 1-day move of 108 bps (BTC), against
  a 199.5 bps round trip.

Three things that would change these numbers, none of them measured here: a
funding regime above ~15%/yr sustained for longer than a cycle; a lower spot
fee tier, which is the larger of the two legs by a factor of nine; and a
maintenance-margin level lower than the posted initial margin, which would move
the top-up thresholds.

Three things this document does **not** establish: that carry is unprofitable
on CFM (CFM's own funding has never been observed), that the Binance proxy
transfers (it is declared as a proxy and its cadence differs), and that the
declared 7-day harvest rule is a good one (it is an accounting convention, and
a different rule would produce different cycle counts and therefore different
fees).

## Provenance

Inputs: 4h klines and funding from `backtesting/hydrate_perps_proxy.py`
(perps-gate branch, PR #24 — **if that has not merged, run it from there**);
194 premium-index files from
[`../../backtesting/hydrate_carry_basis.py`](../../backtesting/hydrate_carry_basis.py)
in this branch, each verified against Binance's published sha256 sidecar, with
per-file digests in
[`data/carry_inputs_2026-09-19.csv`](data/carry_inputs_2026-09-19.csv).

`carry_scoping.py --verify` recomputes all five tables and compares
byte-for-byte; it passes on this tree. As with the perps gate, `--verify` is
deliberately not a CI check, because the inputs come from a venue this
repository does not trade and sit outside the frozen research closure. The
guard tests need no data and do run in CI.
