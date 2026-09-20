# Blind feasibility gate — 4h BTC/ETH perpetuals

**2026-09-19. READ-ONLY. BLIND. No trial ID, no pre-registration, no
recommendation, no authorization.**

This document applies the standing policy's **decidable-edge floor gate**
([`../trial_registry.md`](../trial_registry.md), "Standing policy" §2) *ex
ante* to two mechanisms declared before any data was touched. It answers one
question about each: **could a return trial of this mechanism decide its own
question?**

It computes **no P&L, no profit factor, no expectancy, no Sharpe, no drawdown
and no equity curve**, it simulates no return of any kind, and it ranks neither
mechanism against the other nor either asset against the other. Every number
below is a property of a price series, a funding series or a position-flag
series. Where two numbers sit side by side in a table, their ordering is
structural — a dispersion, an event rate, a cost — and **carries no claim about
which would earn more**. That question is not asked here and the data to answer
it was never computed.

Nothing here changes `DRY_RUN=true`, **LIVE NO-GO**, `LIVE_BALANCE_USD`,
`ASSET_CONFIG`, the spot fee schedule, V3's retirement, Phase 7B's unauthorized
status, or the standing decision that 7R3b is not run. No `pipeline/` or
`exchange/` code was modified. No authenticated call of any kind was made.

Reproduce every number with:

```powershell
venv\Scripts\python.exe backtesting/hydrate_perps_proxy.py   # credential-free
venv\Scripts\python.exe backtesting/perps_gate.py
venv\Scripts\python.exe backtesting/perps_gate.py --verify   # recompute, compare
```

---

## 0. The answer, first

**Both declared mechanisms FAIL the gate, and neither is close.**

| Mechanism | Basis | Annual floor over the 6.7 y proxy window | SESOI | Gate | Years to reach SESOI at this event rate |
|---|---|---:|---:|---|---:|
| M1 breakout | BTCUSDT | **22.09%/yr** | 10% | **FAIL** | 32.7 |
| M1 breakout | ETHUSDT | **28.81%/yr** | 10% | **FAIL** | 55.6 |
| M1 breakout | pooled, N_eff-adjusted | **26.48%/yr** | 10% | **FAIL** | 47.0 |
| M2 momentum | BTCUSDT | **37.79%/yr** | 10% | **FAIL** | 95.7 |
| M2 momentum | ETHUSDT | **49.44%/yr** | 10% | **FAIL** | 163.8 |
| M2 momentum | pooled, N_eff-adjusted | **45.58%/yr** | 10% | **FAIL** | 139.2 |

Neither mechanism may be pre-registered for a return trial on this evidence.

The floors are **2.2x to 4.9x the SESOI**, and these are the *optimistic* end
(§7). The "years to reach SESOI" column is arithmetic on the floor at an
unchanged event rate, not a forecast, and a horizon of 33 to 164 years is not a
trial — it is the same "commitment to wait" Closure 2 recorded for spot, on a
venue whose product launched fourteen months ago.

**The cost-side bar, separately.** Before any statistical question arises, each
mechanism must clear its own running cost plus the SESOI just to reach the
smallest edge worth having:

| Mechanism | Symbol | Total cost drag | + SESOI | = **gross annual return required** |
|---|---|---:|---:|---:|
| M1 | BTCUSDT | 10.34%/yr | 10% | **20.34%/yr** |
| M1 | ETHUSDT | 12.84%/yr | 10% | **22.84%/yr** |
| M2 | BTCUSDT | 42.07%/yr | 10% | **52.07%/yr** |
| M2 | ETHUSDT | 43.50%/yr | 10% | **53.50%/yr** |

---

## 1. The two declared mechanisms

Fixed before this task touched data. **No parameter was searched, tuned,
varied or compared on any outcome, and no third variant was tried.** Both are
canonical forms with literature-conventional parameters, chosen for structural
reasons only.

**M1 — "breakout, rare".** Donchian on 4h bars: enter long when the close
exceeds the prior **55**-bar high, exit when the close falls below the prior
**20**-bar low. Long only, one unit, at most one position per asset.
*Rationale:* it is the frozen STF-CLOSE-55-20 rule on a faster clock, so
nothing about it was chosen here. Both lookbacks and the event counter itself
are **imported unmodified** from `backtesting/stf_protocol.py` and
`backtesting/stf_feasibility.py`; `tests/test_perps_gate.py` asserts the
identity, so this module structurally cannot vary M1's parameters.

**M2 — "time-series momentum, every bar".** At each 4h close, position =
`sign(close_t − close_{t−30})` in {−1, +1}, held for the **next** bar, both
assets, always in a position. *Rationale:* the only class whose power is
structurally reachable at this venue's cost is one that trades every bar; 30
bars (5 days) is a declared convention, not a fit.

The two exist to bracket the event-rate axis — the rarest plausible rule and
the densest one. They are not candidates being compared. **Both fail, which
means the interval between them fails too**, and that is the useful content of
having declared two.

### Venue mismatch, declared

The price and funding history is **Binance USDT-margined perpetuals**, used as
**proxy history for Coinbase CFM**. These are different venues: Binance funds
every 8h and CFM every hour; basis, leverage population, cap/floor rules, fee
schedule and liquidation engine all differ. A proxy is used because CFM has
~14 months of price history and **no funding history at all**
([`2026-09-18-perps-scoping.md`](2026-09-18-perps-scoping.md) §4–5), not
because the two are interchangeable. Every number below inherits this
limitation. The **fee rates** are CFM's own, from an authenticated read of this
account on 2026-09-18; only the price and funding *series* are borrowed.

### Data

Public archive `data.binance.vision`, no credentials, USDT-margined perps,
BTCUSDT and ETHUSDT only. 4h klines and fundingRate history. Each downloaded
archive is verified against Binance's own published sha256 sidecar before
extraction; a mismatch is an error, not a warning.

| | |
|---|---|
| Kline window | 2020-01-01 00:00 → **2026-09-17 20:00** UTC (last bar the archive had published) |
| Bars per symbol | **14,712** |
| Funding window | 2020-01-01 → **2026-08-31** UTC |
| Funding events per symbol | **7,305**, all at an 8h interval |
| Input files | **354** (194 kline, 160 funding), 44,340 lines |
| Per-file sha256 | [`data/perps_gate_inputs_2026-09-19.csv`](data/perps_gate_inputs_2026-09-19.csv) |

**The funding series ends 17 days before the kline series**, because the
archive publishes daily kline files but no daily funding files. The boundary is
declared rather than filled from a live endpoint: funding drag is summed over
the funding window and annualised by that window's own length (6.667 y), while
event statistics use the full 6.71 y. The gap is 0.7% of the window.

---

## 2. Deliverable 1 — structural dispersion (mechanism-agnostic)

4h log-return standard deviation, sample (ddof=1), in percent. Machine-readable
copy: [`data/perps_gate_dispersion_2026-09-19.csv`](data/perps_gate_dispersion_2026-09-19.csv).

| Scope | Bars (BTC) | **BTCUSDT SD** | Bars (ETH) | **ETHUSDT SD** |
|---|---:|---:|---:|---:|
| **Full window** | 14,711 | **1.2716%** | 14,711 | **1.6641%** |
| 2020 | 2,195 | 1.4658% | 2,195 | 1.8935% |
| 2021 | 2,190 | 1.8652% | 2,190 | 2.3439% |
| 2022 | 2,190 | 1.3121% | 2,190 | 1.7985% |
| 2023 | 2,190 | 0.9014% | 2,190 | 0.9795% |
| 2024 | 2,196 | 1.1083% | 2,196 | 1.3587% |
| 2025 | 2,190 | 0.9442% | 2,190 | 1.5612% |
| 2026 (to 09-17) | 1,560 | 0.8930% | 1,560 | 1.2009% |

| | Value |
|---|---|
| **rho_bar**, mean pairwise Pearson correlation of **4h log returns** | **0.8385** |
| Observations behind it | 14,711 |
| **N_eff** = N / (1 + (N−1)·rho_bar), from N = 2 | **1.088** |

Estimator reused unmodified from
[`../../backtesting/universe_inventory.py`](../../backtesting/universe_inventory.py)
(`mean_pairwise_log_return_correlation`, `effective_assets`) — the same pair of
functions Closure 2 used, so this gate cannot drift from the estimator that
closed spot breadth.

**Two structural facts.** Dispersion has fallen by roughly half since its 2021 peak on
both symbols — BTC 1.8652% to 0.8930%, ETH 2.3439% to 1.2009% — though not
monotonically: both rise again in 2024, and ETH again in 2025. And **two perps carry the
information of 1.09 independent assets**, against 1.67–1.73 for Closure 2's
20–61 spot pairs — measured at 4h rather than daily, and essentially identical
to the 1.09 the scoping document found on daily bars. Breadth is *worse* here.
Adding the second symbol buys 8.8% more independent information and pays a full
second set of fees and funding for it.

---

## 3. Deliverable 2 — blind event statistics

No return was consulted. M1's events come from the frozen `entry_exit_events`,
which emits timestamps and labels only. M2's come from `np.sign`, which
discards magnitude. Both are turned into a per-bar position-flag series, and
every statistic below is computed from flags and the calendar alone.
Machine-readable copy:
[`data/perps_gate_events_2026-09-19.csv`](data/perps_gate_events_2026-09-19.csv).

### M1 — entries, holds and occupancy

| Scope | Bars | **BTC entries** | Mean hold (bars) | Median hold | Fraction in position | **ETH entries** | Mean hold (bars) | Median hold | Fraction in position |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **Full window** | 14,712 | **128** | **39.20** | 30.0 | **0.3410** | **144** | **34.62** | 25.0 | **0.3389** |
| 2020 | 2,196 | 18 | 56.22 | 44.5 | 0.4344 | 22 | 41.82 | 32.0 | 0.3916 |
| 2021 | 2,190 | 18 | 39.39 | 27.5 | 0.3502 | 24 | 37.75 | 26.0 | 0.4411 |
| 2022 | 2,190 | 19 | 24.21 | 19.0 | 0.2100 | 20 | 28.00 | 21.0 | 0.2557 |
| 2023 | 2,190 | 20 | 41.55 | 34.0 | 0.3795 | 23 | 32.78 | 25.0 | 0.3443 |
| 2024 | 2,196 | 19 | 47.37 | 39.0 | 0.4098 | 18 | 39.94 | 27.5 | 0.3274 |
| 2025 | 2,190 | 19 | 35.11 | 27.0 | 0.2854 | 21 | 31.05 | 21.0 | 0.2977 |
| 2026 (to 09-17) | 1,560 | 15 | 29.20 | 28.0 | 0.3077 | 16 | 29.69 | 25.0 | 0.3045 |

Entries per asset per year: **19.06** (BTC), **21.44** (ETH). Mean hold 39.2 and
34.6 bars is **6.5 and 5.8 days**. The event rate is remarkably stable across
years — 15 to 24 entries on every symbol in every year — which is the one thing
here that makes a forward projection of the rate defensible.

### M2 — by construction one position per bar

| Scope | Bars | **BTC sign runs** | Mean run (bars) | Frac. long | Frac. short | **ETH sign runs** | Mean run (bars) | Frac. long | Frac. short |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **Full window** | 14,712 | **1,288** | **11.40** | 0.5299 | 0.4680 | **1,223** | **12.00** | 0.5353 | 0.4625 |
| 2020 | 2,196 | 171 | 13.02 | 0.6116 | 0.3743 | 183 | 11.88 | 0.6011 | 0.3848 |
| 2021 | 2,190 | 179 | 11.99 | 0.5347 | 0.4653 | 166 | 13.20 | 0.6155 | 0.3840 |
| 2022 | 2,190 | 200 | 10.89 | 0.4457 | 0.5543 | 172 | 12.70 | 0.4726 | 0.5274 |
| 2023 | 2,190 | 201 | 10.86 | 0.5443 | 0.4557 | 177 | 12.36 | 0.5292 | 0.4708 |
| 2024 | 2,196 | 163 | 13.51 | 0.5638 | 0.4362 | 168 | 13.05 | 0.5464 | 0.4536 |
| 2025 | 2,190 | 235 | 9.35 | 0.5178 | 0.4822 | 210 | 10.65 | 0.4895 | 0.5105 |
| 2026 (to 09-17) | 1,560 | 139 | 11.12 | 0.4756 | 0.5244 | 147 | 10.29 | 0.4750 | 0.5250 |

The effective trade count is the sign-run count: **191.80** and **182.12** runs
per year. Occupancy is 0.998 rather than 1.000 only because the first 31 bars
warm up flat, as a forward trial would.

**The run-length distribution is severely skewed**: mean 11.4 bars against a
**median of 4**. **29% (BTC) and 26% (ETH) of all positions last a single bar**, and
over half last four bars or fewer. This
matters for §4 — the fee bill is driven by the count, which the short runs
dominate, while the floor is driven by total time in position, which the long
ones dominate.

### The blind, and how it is enforced

Exactly three functions in `backtesting/perps_gate.py` may see a price level or
difference: `log_return_dispersion` (returns scalars), `m2_position_flags`
(returns signs), and `load_closes` (reads the file, does no arithmetic).
Everything else operates on flags, timestamps, integer bar counts and published
funding rates. `tests/test_perps_gate.py` enforces this three ways:

1. **Structurally** — an AST walk asserts that `log`, `diff`, `shift`,
   `pct_change`, `std` and `sign` appear only inside the declared boundary
   functions, following the pattern of `tests/test_universe_inventory.py`.
2. **Behaviourally** — multiplying every price by 0.001, 3.7 or 10,000 must
   leave both mechanisms' flag series bit-identical. A quantity that survives
   rescaling cannot be a return.
3. **By type** — M2's flags are `int8` with values in {−1, 0, +1}; the
   dispersion estimator returns floats and never a series; the counting
   functions accept no parameter that could be a price.

Both guards were **mutation-tested**: inserting a `.diff()` into a counting
function fails guard 1, and returning a magnitude instead of a sign from M2
fails guards 2 and 3. A further test asserts that exactly two `*_position_flags`
functions exist, so a third mechanism cannot be added without the test failing.

---

## 4. Deliverable 3 — cost and funding exposure (cost side only)

Fee rates are Coinbase CFM's, read from this account on 2026-09-18: **maker
0.095%, taker 0.100%**. The round trip is priced **maker-in / taker-out** at
`(1+maker)/(1−taker) − 1` = **+0.1952%**.

> **This is the optimistic assignment, stated as such.** It assumes every entry
> rests on the book and is filled as a maker. Neither mechanism is built to
> guarantee that: M1 enters on a close that has just broken a 55-bar high, and
> M2 reverses on a signal computed at a close — both are the conditions under
> which a resting order is least likely to fill at the price that triggered it.
> The all-taker round trip is **+0.2002%**, and the all-taker columns are in
> the CSV. The maker/taker spread is only 0.005pp here, so the assignment
> moves the totals by under 3% and **changes no conclusion**.

Funding is arithmetic on the position flag and the published rate: for each
funding event, `position sign × published rate`, summed. A positive published
rate is paid by longs and received by shorts. This is a cost, not a P&L —
nothing in it knows whether a position gained or lost. Machine-readable copy:
[`data/perps_gate_cost_2026-09-19.csv`](data/perps_gate_cost_2026-09-19.csv).

| Mechanism | Symbol | **Round trips/yr** | **Fee drag** | **Funding drag** | long leg | short leg | **Total drag** |
|---|---|---:|---:|---:|---:|---:|---:|
| M1 | BTCUSDT | 19.06 | 3.7205%/yr | **6.6243%/yr** | +6.6243 | 0.0000 | **10.3448%/yr** |
| M1 | ETHUSDT | 21.44 | 4.1856%/yr | **8.6554%/yr** | +8.6554 | 0.0000 | **12.8410%/yr** |
| M2 | BTCUSDT | 191.80 | 37.4375%/yr | **4.6305%/yr** | +8.2148 | **−3.5844** | **42.0680%/yr** |
| M2 | ETHUSDT | 182.12 | 35.5482%/yr | **7.9497%/yr** | +10.9436 | **−2.9940** | **43.4979%/yr** |

All figures are percent of **position notional** per year. A negative leg is a
credit received.

### Funding by year, per mechanism

Each cell is the funding **paid over that calendar year**, not annualised; 2026
covers January to August only, because the funding archive ends 2026-08-31.

| Year | M1 BTC | M1 ETH | M2 BTC (net) | M2 BTC long / short | M2 ETH (net) | M2 ETH long / short |
|---|---:|---:|---:|---|---:|---|
| 2020 | 11.9521 | 17.6766 | 9.8757 | +13.5359 / −3.6602 | 15.9818 | +21.6728 / −5.6909 |
| 2021 | 19.0728 | 25.9416 | 14.0951 | +22.3518 / −8.2567 | 25.3481 | +31.4427 / −6.0946 |
| 2022 | 1.3407 | 1.0897 | 1.0646 | +2.6148 / −1.5502 | 2.8751 | +1.8312 / **+1.0439** |
| 2023 | 3.3686 | 4.0847 | 1.4709 | +4.6683 / −3.1974 | 2.8866 | +5.5730 / −2.6865 |
| 2024 | 6.2775 | 6.1250 | 4.0137 | +7.9853 / −3.9716 | 4.1335 | +8.5652 / −4.4317 |
| 2025 | 1.5797 | 2.0452 | 0.5243 | +2.8253 / −2.3011 | 1.1646 | +3.0467 / −1.8821 |
| 2026 (to 08-31) | 0.5709 | 0.7398 | **−0.1745** | +0.7842 / −0.9586 | 0.6081 | +0.8259 / −0.2179 |

The level is strongly regime-dependent and has fallen hard since 2021 — the
same shape the scoping document recorded. The single positive short leg (ETH
2022) is a bear year in which funding was negative while the mechanism was
short, so the short leg paid rather than received.

### Two findings on the cost side

**M1's long-only funding is selected, not average.** A position that was always
long would have paid 11.8057%/yr (BTC) and 13.9565%/yr (ETH) — these reproduce
the scoping document's figures. M1 is in position ~34% of the time, so a naive
occupancy-weighted estimate is 4.03% and 4.73%/yr. It actually pays **6.62%**
and **8.66%**:

| Mechanism | Symbol | Occupancy-weighted estimate | **Measured** | **Ratio** |
|---|---|---:|---:|---:|
| M1 | BTCUSDT | 4.0257%/yr | **6.6243%/yr** | **1.65x** |
| M1 | ETHUSDT | 4.7299%/yr | **8.6554%/yr** | **1.83x** |
| M2 | BTCUSDT | 11.7809%/yr | 4.6305%/yr | 0.39x |
| M2 | ETHUSDT | 13.9258%/yr | 7.9497%/yr | 0.57x |

A breakout long is in position precisely when the market is rising and longs are
crowded, which is when funding is most expensive. **Estimating a long-only
mechanism's funding cost by scaling the unconditional mean by its
time-in-market understates it badly: the measured cost is 65% (BTC) to 83%
(ETH) HIGHER than that estimate.** Any pre-registration that budgets
funding that way will under-budget it. M2's ratio is below 1 for the opposite
reason: its short leg collects during exactly the periods the long leg pays.

**M2's netting is real but small against its fee bill.** Being short 46–47% of
the time recovers 3.0–3.6pp/yr of funding — but the mechanism pays 35–37%/yr in
fees to be in a position every bar. **Fees are 4.5x (ETH) to 8.1x (BTC) its funding drag.** M1 has
the reverse profile: funding is 1.8x (BTC) to 2.1x (ETH) its fees. The two mechanisms fail the
cost side for opposite reasons, which is exactly what bracketing the event-rate
axis was meant to expose.

---

## 5. Deliverable 4 — the decidable floor and the gate

Construction, fixed in advance:

```
sigma_trade  = SD(4h log return) x sqrt(mean hold in bars)     # a LOWER BOUND
n            = trades per year   (M1: entries; M2: sign runs)
floor_annual = 1.645 x sigma_trade x sqrt(n / Y)
```

`Y = 6.7` years is the proxy window. The pooled basis applies the same
correlation adjustment Closure 2 used: `N_eff` assets each firing at the mean
per-asset rate. Machine-readable copy:
[`data/perps_gate_floors_2026-09-19.csv`](data/perps_gate_floors_2026-09-19.csv).

| Mech | Basis | sigma_trade | n/yr | Bars in position/yr | **floor @ Y=6.7** | @ Y=3 | @ Y=2 | @ Y=1 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| M1 | BTCUSDT | 7.9615% | 19.06 | 747.1 | **22.0896%** | 33.0115% | 40.4306% | 57.1775% |
| M1 | ETHUSDT | 9.7914% | 21.44 | 742.5 | **28.8147%** | 43.0616% | 52.7395% | 74.5849% |
| M1 | pooled (N_eff 1.088) | 8.8765% | 22.03 | 810.3 | **26.4796%** | 39.5720% | 48.4656% | 68.5407% |
| M2 | BTCUSDT | 4.2934% | 191.80 | 2,186.1 | **37.7877%** | 56.4712% | 69.1628% | 97.8110% |
| M2 | ETHUSDT | 5.7646% | 182.12 | 2,186.0 | **49.4394%** | 73.8839% | 90.4889% | 127.9706% |
| M2 | pooled (N_eff 1.088) | 5.0290% | 203.41 | 2,378.4 | **45.5820%** | 68.1193% | 83.4288% | 117.9862% |

**A forward shadow on CFM starts at Y = 0**, where the floor is unbounded. The
Y = 1 column is what it looks like after a full year of forward data: 57% to
128% per year.

### The gate

**SESOI = 10% net annual.** A mechanism whose floor over the proxy window
exceeds it fails and is not to be pre-registered for a return trial.

| Mechanism | Floor (pooled) | SESOI | Ratio | **Gate** |
|---|---:|---:|---:|---|
| **M1 breakout, rare** | 26.48%/yr | 10% | 2.65x | **FAIL** |
| **M2 momentum, every bar** | 45.58%/yr | 10% | 4.56x | **FAIL** |

Every per-asset basis fails too, by 2.21x to 4.94x. There is no basis, asset or
horizon inside the proxy window on which either mechanism passes.

### Why trading more often cannot fix this

Substituting the declared sigma construction into the floor gives an identity
worth stating plainly:

```
floor_annual = 1.645 x SD_bar x sqrt(n x h / Y)
             = 1.645 x SD_bar x sqrt(bars in position per year / Y)
```

because `n × h` — trades per year times mean hold in bars — **is** the number
of bars in position per year. The trade count cancels out exactly.

**The annual floor depends only on per-bar dispersion and on time in market.
How that time is cut into trades makes no difference to it at all.** Cutting
the same exposure into twice as many trades of half the length leaves the floor
identical (`tests/test_perps_gate.py` asserts this), while **doubling the fee
bill**, which scales with `n` and not with its square root.

This is the structural reason M2 is worse off than M1 despite having ten times
the trade count. M2's higher floor comes entirely from being in the market
2.9x as many bars — not from anything about the trades. Its trade count buys it
nothing statistically and costs it 34%/yr in fees.

It also closes the obvious escape route. Lowering the floor requires spending
**less time in the market**, and the annual edge available scales with time in
the market too. There is no trading frequency that improves this ratio.

**This also corrects a reading the scoping document invited.** That document
reported 4h floors of 0.0286%/trade at three years and noted this was "the
first time in this repository a floor has come in below the cost of trading."
That is true *per trade*, and it is not the operative quantity. A trial that
takes 2,387 trades a year must clear that per-trade floor **2,387 times over**
to show an annual edge; expressed annually the same figure is ~68%/yr. The
per-trade framing made a dense mechanism look resolvable because it divides the
requirement by the trade count while the trade count is what generates the
requirement. **The annual framing is the one that answers the question the
owner is actually asking**, and it reverses the scoping document's more
hopeful-looking result. Nothing in that document was wrong; the quantity it
reported was not the one that decides this.

---

## 6. Deliverable 5 — positive-control design (design only, NOT run)

Declared now, before any return is computed, so the magnitudes and seeds are on
record rather than chosen once a result is visible. **Nothing below has been
run.** On record as an artifact:
[`data/perps_gate_control_spec_2026-09-19.csv`](data/perps_gate_control_spec_2026-09-19.csv).

**What the controls are for.** A return trial that reports "no edge found" has
said nothing until it has shown it could have found one. The controls
calibrate the harness against a known answer before it is pointed at the
unknown one.

**Method.** A constant per-bar drift `d` is added to the proxy log-return
series on bars the mechanism is in position, the mechanism's own per-trade net
return series is formed under it, and the harness's one-sided 95% **lower**
bound `mean − 1.645·SE` is checked against zero. Because the injected annual
edge is `d × bars in position per year`, each target below inverts directly to
a per-bar drift. **1,000 replications**, seed **20260919** for injection, seed
**20260920** for the shuffle.

**Expectations follow from the floor's own definition and are not tuned.** At a
true edge equal to 1.0x the floor, the lower bound sits at zero in expectation,
so it clears about half the time. At 2.0x the floor — the `1.645 + 1.645` case
— it clears about 95% of the time.

| Control | Target annual edge | Expectation |
|---|---|---|
| **A — floor 1x** | each mechanism's own floor | ~50% of replications clear zero |
| **B — floor 2x** | twice its floor | ~95% clear zero |
| **C — SESOI** | 10%/yr, the same for all | well under 50%, because the floor is above the SESOI |
| **D — null, shuffled** | 0 | ~5% clear zero, the nominal false-positive rate |

Per-bar drift magnitudes, on record:

| Control | M1 BTC | M1 ETH | M2 BTC | M2 ETH |
|---|---:|---:|---:|---:|
| A — floor 1x | 0.029567% | 0.038808% | 0.017285% | 0.022616% |
| B — floor 2x | 0.059134% | 0.077615% | 0.034571% | 0.045233% |
| C — SESOI 10%/yr | 0.013385% | 0.013468% | 0.004574% | 0.004575% |
| D — null, shuffled | 0 | 0 | 0 | 0 |

**Control D** shuffles the per-bar return series with a fixed permutation under
seed 20260920, destroying time structure while preserving the marginal
distribution, and re-runs the mechanism. Its bound must sit at zero. If D
clears zero materially more than 5% of the time, the harness has a bug or a
look-ahead, and **no result from it may be believed** regardless of what A, B
and C show.

**Control C is the one that matters for the decision.** It injects an edge
exactly at the SESOI. If a harness cannot reliably detect an edge the owner has
declared to be the smallest worth having, the trial cannot answer its own
question — which is the same statement §5 makes, arrived at by simulation
instead of by arithmetic. **The gate and the control must agree**, and if a
future run of C shows detection rates materially above what §5's floors imply,
the discrepancy must be resolved before either is trusted.

---

## 7. These floors are the optimistic end

Four things all cut the same way, and none is quantified:

1. **sigma_trade is a lower bound, stated as one.** `SD_bar × sqrt(h)` assumes
   returns compound independently inside a position. A trend-following position
   is *selected* for autocorrelation, so its true per-trade dispersion is
   larger. A real per-trade series also carries entry and exit timing
   dispersion this construction ignores entirely.
2. **The proxy is not the venue.** CFM funds hourly, not 8-hourly; its basis,
   leverage population and cap/floor rules differ. The direction of that error
   is unknown, which is worse than knowing it is adverse.
3. **Dispersion is regime-dependent.** Full-window SD averages a quantity that
   ranged from 0.89% to 1.87% per bar across these seven years, and `rho_bar`
   is likewise a full-window average. Neither is a guarantee for any
   sub-period.
4. **No execution cost beyond the fee is modelled.** No spread, no slippage, no
   partial fill, no funding-rate cap, and no liquidation. §4's maker assignment
   is itself optimistic for both mechanisms.

**The true floors are therefore higher than the table, by an unquantified
amount, and the horizon arithmetic is correspondingly optimistic.** This is the
same direction of error Closure 2 recorded for its own floors. A mechanism that
fails by 2.2x to 4.9x on the optimistic end does not become marginal on the
pessimistic one.

---

## 8. What this does and does not establish

**It establishes**, for these two declared mechanisms on this proxy history:
the event rates, the dispersion, the cross-asset independence, the funding and
fee exposure, and the resulting decidable-edge floors — and that both floors
exceed the declared SESOI by a wide margin, which is the condition under which
the standing policy says a trial is not started.

**It does not establish** that either mechanism is unprofitable. **No return
was computed, no P&L simulated, no asset or mechanism ranked.** This is not
evidence about profitability in either direction and must never be cited as if
it were. The mechanisms fail because their question is undecidable at this
event rate and this dispersion within a usable horizon — not because it was
answered.

**It does not close the venue.** Three things, not tested here, could each
change the arithmetic, and a future gate would have to measure them rather than
assume them:

- a mechanism with **materially lower time in market** — the only lever §5's
  identity leaves open, and it shrinks the available annual edge in proportion;
- a **larger and genuinely less correlated** instrument set on this venue —
  though `N_eff` 1.09 across the two largest names is not encouraging, and
  Closure 2 established that adding correlated names multiplies cost without
  adding information;
- a **higher SESOI**, which is the owner's call and not a research finding. The
  gate is a comparison between two declared numbers, and this document moves
  neither of them.

**It does not touch** the separate, still-open questions the scoping document
raised: whether CFM charges fees per notional or per contract (§3 of that
document, unresolved and material at small notionals), the absence of a
view-only credential, and the 26–34 engineer-day build estimate. None of those
was reached, because the gate fires before they matter.

### One thing that would close this venue on its own

Not tested here, and recorded as the thing to check first if this line is ever
revisited: **the minimum order is one contract, and one BTC PERP contract is
$808 of notional against a $100 cap.** Of the 29 CFM products, BTC PERP is not
holdable overnight within the cap at all, and ETH PERP consumes 63% of it for a
single indivisible contract at 2.58x leverage
([`2026-09-18-perps-scoping.md`](2026-09-18-perps-scoping.md) §2).

Both mechanisms here are specified on **BTC and ETH**, and M2 requires a
position in **both, in every bar, in both directions**. At the current cap that
is not merely expensive — **it is not expressible**. Position sizing, the 2%
rule, and the 50%/25% circuit-breaker reductions all have no expression at one
indivisible contract. A funding drag quoted as a percentage of notional becomes
18%/yr of the *cap* at ETH PERP's 2.58x, and the floors above are percentages
of notional too, so the same multiplier applies to them.

This is a quantisation constraint, not a statistical one, and it is not what
fires the gate. But it means that **even a mechanism that passed the floor gate
could not be run at this account's cap**, and it would have to be resolved
before any of the statistical work above could be acted on.

---

## 9. Provenance

| Artifact | sha256 (first 16) | Rows |
|---|---|---:|
| `data/perps_gate_dispersion_2026-09-19.csv` | `8663560a6fdc266c` | 18 |
| `data/perps_gate_events_2026-09-19.csv` | `c8e149153d9a7044` | 32 |
| `data/perps_gate_cost_2026-09-19.csv` | `e996a3ac661d67fa` | 32 |
| `data/perps_gate_floors_2026-09-19.csv` | `c0d8fb74accde9d9` | 6 |
| `data/perps_gate_control_spec_2026-09-19.csv` | `028e6e086b528776` | 16 |
| `data/perps_gate_inputs_2026-09-19.csv` | `2666da136fa118b9` | 354 |

Digests were taken after the final run; `perps_gate.py --verify` recomputes
every table from the cached archives and compares byte-for-byte, and passes on
this tree.

**`--verify` is deliberately NOT wired into CI**, which is a decision and not an
oversight. The CI research-verify job regenerates the frozen research closure
from credential-free *Coinbase* hydration; this gate's inputs are 354 archives
from a *third-party* venue, gitignored and outside that closure, and making a
required check depend on Binance's archive staying up would couple this
repository's CI to a venue it does not trade. The same choice was made for the
spot universe inventory and the CFM scoping document. Verification here is
local and reproducible: rehydrate, re-run, compare the digests above. The guard
tests in `tests/test_perps_gate.py` need no data at all and **do** run in CI,
which is the part that matters — they protect the blind, not the numbers. Source: [`../../backtesting/perps_gate.py`](../../backtesting/perps_gate.py),
[`../../backtesting/hydrate_perps_proxy.py`](../../backtesting/hydrate_perps_proxy.py),
guarded by [`../../tests/test_perps_gate.py`](../../tests/test_perps_gate.py)
(30 tests).
