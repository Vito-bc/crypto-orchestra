# Option C scoping — Coinbase Financial Markets perpetual-style futures

**2026-09-18. READ-ONLY. No trial ID, no pre-registration, no recommendation.**

This is a fact sheet and a price tag. It exists so the owner can decide whether
pre-registering anything on this venue is worth the build cost. It produces no
strategy result: no P&L, profit factor, expectancy, Sharpe, equity curve or
asset ranking appears anywhere in it or in
[`../../backtesting/perps_scoping.py`](../../backtesting/perps_scoping.py), and
no rule is simulated. Nothing here changes `DRY_RUN`, `LIVE_BALANCE_USD`,
`ASSET_CONFIG`, the spot fee schedule, or the V3 / Phase 7B / 7R3b
determinations.

**Why the question was asked.** Coinbase spot at this account's tier is closed
on two recorded grounds — Closure 1 (edge) and Closure 2 (feasibility) in
[`../trial_registry.md`](../trial_registry.md). The literature review
([`literature/2026-09-17-review/`](literature/2026-09-17-review/)) places every
documented crypto result at 2–60 bps round trip against our 140–180 bps, and
Closure 2 established that breadth cannot buy statistical power here because
`N_eff` is 1.7 across 20–61 spot pairs. Cost and event rate are the two
remaining levers. This document measures what this venue does to each.

Reproduce every number below with:

```powershell
venv\Scripts\python.exe backtesting/perps_scoping.py --products   # credential-free
venv\Scripts\python.exe backtesting/perps_scoping.py --funding    # credential-free
venv\Scripts\python.exe backtesting/perps_scoping.py --power      # credential-free
```

The account-specific facts in §1 and §3 came from authenticated READ calls made
once, by hand, on 2026-09-18; they are quoted here rather than re-read by a
script, because a committed tool that reads an account is a tool that can be
pointed at one.

---

## 0. Credential finding — recorded first because the task premise was wrong

The brief stated that `cdp_api_key.json` is view-only. **It is not.**
`get_api_key_permissions()` on 2026-09-18 returns:

```
can_view      true
can_trade     true        <-- the premise said false
can_transfer  false
portfolio     DEFAULT
```

Consequences, in order of importance:

1. **`backtesting/stf_cost_probe.py` would refuse this key.** Its
   `_REQUIRED_PERMISSIONS` gate demands `can_trade: False` and fails closed
   otherwise. That gate is working exactly as designed; what it tells us is
   that the repository's main credential is a trading credential and has been
   all along.
2. Every call made for this document was a **read**. No order, cancel, sweep,
   transfer, margin-setting change or portfolio modification was issued, and
   `schedule_futures_sweep` / `cancel_pending_futures_sweep` were never called.
   The forbidden set is defined by operation, not by what the key happens to
   permit.
3. Any future work on this venue that involves an authenticated read needs a
   separate view-only key, on the same principle Phase 7R-2 already
   established. This is not a new rule; it is the existing rule meeting a key
   that does not satisfy it.

Two other access facts, recorded as findings rather than worked around:

- `get_perps_portfolio_summary` and `get_perps_portfolio_balances` return
  **403 PERMISSION_DENIED**. Those endpoints serve **Coinbase International
  Exchange (INTX)** perpetuals, which are not the product this account can
  reach. No new key was sought.
- Every Coinbase-hosted fee or product page tried (`coinbase.com/advanced-fees`,
  the help-centre futures fee article, the US perps launch blog post) returned
  **HTTP 403** to this environment, and `coinbasederivatives.com` did not
  resolve. This is the same documentation wall report 01 of the literature
  review hit. The authenticated API is therefore the primary source for
  everything in §1–§3, and it is a better one for this account than any
  published page would be.

---

## 1. Facts — access and eligibility

**The account is CFM-enabled.** Every futures endpoint that serves the US
FCM-managed product answers with data rather than a permission error:

| Call | Result |
|---|---|
| `get_futures_balance_summary` | returns a full balance summary (below) |
| `list_futures_positions` | `{"positions": []}` — no open positions |
| `get_intraday_margin_setting` | `INTRADAY_MARGIN_SETTING_STANDARD` |
| `get_current_margin_window` | returns; killswitches both `false` |
| `list_futures_sweeps` | `{"sweeps": []}` |
| `get_perps_portfolio_summary` | **403 PERMISSION_DENIED** (INTX, not CFM) |
| `get_perps_portfolio_balances` | **403 PERMISSION_DENIED** (INTX, not CFM) |

`get_futures_balance_summary` on 2026-09-18:

| Field | Value |
|---|---|
| `cfm_usd_balance` | **0** |
| `cbi_usd_balance` | 0.03 |
| `futures_buying_power` | **2.29** |
| `initial_margin` | 0 |
| `unrealized_pnl` / `daily_realized_pnl` / `funding_pnl` | 0 |
| `liquidation_threshold` | 0 |

So: eligible, enrolled, and **funded with nothing**. The futures wallet holds
$0 and buying power is $2.29. Using this venue at all requires an explicit
transfer into the CFM wallet — an action this task is forbidden from taking and
which is the owner's decision, not a scoping conclusion.

**What the key may lack.** Nothing observed. Every CFM read succeeded on the
existing key. The 403s are product-scope (INTX), not permission-scope, and no
key this account can create would change them. The permission that is *missing*
in the other direction is the absence of a view-only key (§0).

**Two traps for a future futures client, both observed:**

- `get_fills(product_type="FUTURE")` also returns rows from other venues and
  instrument types routed through the same account — `product_type` is not a
  venue filter. Any fill reader must filter on `product_venue == "FCM"` or the
  `-CDE` product-id suffix, or it will ingest unrelated instruments.
- `get_futures_position(product_id=...)` raises `TypeError` from inside the SDK
  when there is no position, instead of returning an empty result. Calling code
  must treat that exception as "flat", which is exactly the kind of silent
  coupling to a library bug the fail-closed rule exists to prevent.

---

## 2. Facts — the products

29 perpetual-style products are listed, all on venue `cde` (Coinbase
Derivatives Exchange), `product_venue` `FCM`, `risk_managed_by`
`MANAGED_BY_FCM`. The full inventory with every field is in
[`data/perps_products_2026-09-18.csv`](data/perps_products_2026-09-18.csv);
it is built from the **public**, credential-free products endpoint, which
carries contract specs, margin rates and the current funding rate in full.

**"Perpetual-style" confirmed from primary.** `contract_expiry_type` is
`EXPIRING`, not perpetual: crypto perps carry `contract_expiry`
**2089-12-30** and index perps **2030-12-19/20**. They are long-dated expiring
futures with a funding mechanism bolted on, which is how a CFTC-regulated venue
reproduces a perpetual. `twenty_four_by_seven` is `true`, and each product also
carries an `fcm_trading_session_details` block with daily open/close stamps —
so the session metadata exists even though trading is continuous, and a client
must not assume the two agree.

**Funding interval is 3600s — hourly**, on every product. Not 8-hourly. This
matters for the proxy comparison in §5 and for accounting design in §7.

The project's four spot assets, plus the extremes of the list:

| Product | Name | Underlying | Contract size | Notional/contract | Tick | Min order | Funding | Intraday margin (long) | Overnight margin (long / short) | Max lev overnight |
|---|---|---|---|---:|---:|---:|---|---:|---:|---:|
| `BIP-20DEC30-CDE` | BTC PERP | BTC | 0.01 | **$808.30** | $5 | 1 | 3600s | 10.00% | **24.56%** / 30.64% | 4.07x |
| `ETP-20DEC30-CDE` | ETH PERP | ETH | 0.1 | **$258.40** | $0.50 | 1 | 3600s | 10.01% | **24.53%** / 33.48% | 4.08x |
| `SLP-20DEC30-CDE` | SOL PERP | SOL | 5 | $554.30 | $0.01 | 1 | 3600s | 25.0% | 36.6% / — | 2.73x |
| `ZEC-20DEC30-CDE` | ZCASH PERP | ZEC | 1 | **$1,487.80** | $0.05 | 1 | 3600s | **25.00%** | **53.96% / 99.01%** | 1.85x |
| `AVP-20DEC30-CDE` | AVAX PERP | AVAX | 10 | $81.10 | $0.01 | 1 | 3600s | — | 32.4% | 3.09x |
| `SHP-20DEC30-CDE` | 1000SHIB PERP | SHIB | 10000 | $54.60 | $0.00001 | 1 | 3600s | — | 52.9% | 1.89x |

Margin is **asymmetric by side** — the short leg costs more overnight on every
crypto product (BTC 30.6% vs 24.6%; ETH 33.5% vs 24.5%; ZEC 99.0% vs 54.0%). A
short position is not the mirror of a long one on this venue.

**Quantisation is the binding constraint at the current cap.** The minimum
order is 1 contract everywhere, so position size is not continuous. Against
`LIVE_BALANCE_USD = 100`, asking only whether one contract's overnight margin
fits:

> **5 of 29 products are holdable overnight within the $100 cap:**
> ETH PERP ($63.37 margin), ADA PERP ($61.65), DOT PERP ($49.76),
> AVAX PERP ($26.28), 1000SHIB PERP ($28.88).
>
> **BTC PERP ($198.54), SOL PERP ($202.87) and ZCASH PERP ($802.85) are not.**

Of the project's four spot assets, only ETH is reachable overnight at the
current cap, at one contract, consuming 63% of the cap for a $258 notional —
2.58x leverage on the cap before any sizing decision is made. The spot
pipeline's 2%-of-balance sizing and its 50% / 25% circuit-breaker size
reductions have no expression at one indivisible contract.

---

## 3. Facts — fees, and whether the tiers are linked

Both rows below are **authenticated reads of this account** on 2026-09-18, from
`get_transaction_summary`. They are the primary source; Coinbase's own fee
pages were unreachable (§0).

| `product_type` | Tier | Maker | Taker | Round-trip break-even gross move |
|---|---|---:|---:|---:|
| (default, spot) | Intro | 0.500% | 0.900% | **+1.4127%** |
| `FUTURE` | Intro | **0.095%** | **0.100%** | **+0.1952%** |

Break-even is `(1+maker)/(1−taker) − 1`, the same arithmetic as
[`2026-09-cost-sensitivity.md`](2026-09-cost-sensitivity.md) §2, priced
maker-in / taker-out. All-taker on the perp is +0.2002%; all-maker +0.1902% —
the maker/taker spread is 0.005pp, so unlike spot, **execution style barely
matters here.**

**The cost ratio is 7.24x**, spot to perp, per round trip. That is the whole of
Option C's cost case, and it is smaller than an order of magnitude.

**Are spot and futures tiers linked? Partly — and not in the way the press
coverage implies.** The tier *ladder* is shared: both product types report
tier `Intro` with `next_tier` `Advanced 1`, and the same record carries
`perps_vol_from/to` and `futures_vol_from/to` alongside the spot range. The
`volume_types_and_range` block shows three separate qualifying dimensions:

| Volume types | Intro range | Advanced 1 range |
|---|---|---|
| `VOLUME_TYPE_SPOT` | $0 – $10,000 | $10,000 – $50,000 |
| `VOLUME_TYPE_INTX_PERPS`, `VOLUME_TYPE_DATED_FUTURES`, `VOLUME_TYPE_US_DERIVATIVES` | $0 – $100,000 | $100,000 – $500,000 |
| `VOLUME_TYPE_OPTIONS` | $0 – $100,000 | $100,000 – $500,000 |

`fee_tier_without_promotion.qualification_type` is currently
`FEE_TIER_QUALIFICATION_TYPE_SPOT_VOLUME`, i.e. the account is placed on the
ladder by whichever dimension is most favourable, and today that is spot.

So the accurate statement is: **one ladder, per-dimension thresholds, and
different rates per product type at the same rung.** Derivatives volume does
count toward tier progression, but it needs $100,000 to reach Advanced 1
against spot's $10,000 — and at $258 per ETH contract that is ~388 round trips
of derivatives volume for the first rung. As on spot, **the tier is not a lever
this account can move by trading.**

**One caveat I could not close.** The API reports futures fees as *rates*.
Nano-futures fees are elsewhere sometimes described as a flat charge per
contract, and I could not reach a Coinbase page to confirm which applies. If
CFM charges per contract rather than per notional, the small-notional economics
change materially and every cost number in §6 changes with them. A single
observed fill on this venue would settle it; there are none.

**A separate trap worth naming.** The widely quoted Coinbase perps fee of
**0.00% maker / 0.03% taker** belongs to **INTX** perpetuals — the product that
returns 403 for this account (§0, §1) — and the CDP documentation page that
states it also says margin is posted in USDC to a "perpetuals portfolio", which
this account does not have. That number is not available here. The number
available here is 0.095% / 0.100%, which is 3.3x the headline.

---

## 4. Facts — SDK coverage in the pinned venv

`coinbase-advanced-py 1.8.2`, the pinned version.

| Need | Covered? | How |
|---|---|---|
| (a) read futures products | **yes** | `get_products(product_type="FUTURE")`; `get_public_products(...)` credential-free, and the public record carries contract size, expiry, margin rates, funding interval and the current funding rate in full |
| (a) read futures candles | **yes** | `get_public_candles(product_id=...)`, credential-free, max 350 bars/request |
| (b) read funding — **current** | **yes** | `future_product_details.funding_rate` / `funding_time` / `funding_interval` on the product record — a **snapshot only** |
| (b) read funding — **history** | **no** | there is no funding-history method in the SDK at all; the only funding aggregate anywhere is `funding_pnl` in `get_futures_balance_summary`, which is an account total, not a series |
| (c) read fills | **yes** | `get_fills(product_type="FUTURE")` — but see the venue-filter trap in §1 |
| account/margin state | **yes** | `get_futures_balance_summary`, `list_futures_positions`, `get_intraday_margin_setting`, `get_current_margin_window`, `list_futures_sweeps` |
| position read when flat | **broken** | `get_futures_position` raises `TypeError` instead of returning empty |

**The gap that matters is funding history.** Building a per-hour funding series
for a CFM product means either polling `funding_rate` hourly and storing it
ourselves — i.e. starting the clock today, at zero depth — or finding a raw
REST endpoint the SDK does not wrap, which I could not confirm exists. There is
no way to reconstruct past CFM funding after the fact from anything observed
here.

---

## 5. Numbers — data availability

### Coinbase CFM, credential-free

| Series | Available? | From | Depth at 2026-09-18 |
|---|---|---|---|
| Daily / hourly candles, BTC PERP (`BIP`) | yes | **2025-07-18** | **14.0 months** |
| Daily / hourly candles, ETH PERP (`ETP`) | yes | **2025-07-18** | **14.0 months** |
| Daily / hourly candles, ZCASH PERP (`ZEC`) | yes | 2026-02-28 | 6.7 months |
| Funding **history** | **no** | — | current snapshot only |

2025-07-18 is the products' own `new_at` and matches the US launch. Fourteen
months of price history and **zero** months of funding history is the state of
the primary venue.

### Proxy venue — Binance USDT-margined perps, credential-free

`https://data.binance.vision/data/futures/um/monthly/...`, no credentials, and
it retains delisted symbols.

| Series | Format | From | Depth |
|---|---|---|---|
| `fundingRate/BTCUSDT`, `ETHUSDT` | zipped CSV, `calc_time,funding_interval_hours,last_funding_rate` | **2020-01-01** | 7,305 intervals each, 8h cadence |
| `klines/{sym}/1d`, `/4h` | zipped CSV, OHLCV | 2020-01-01 | 6.66 y common overlap |

### The venue-mismatch problem, stated plainly

Binance is not Coinbase. The funding levels in §6 and the volatilities in §7
are **Binance's**, and they differ from CFM's in mechanism as well as in level:
Binance funds every 8h, CFM every hour; the basis, the leverage population, the
cap/floor rules, the fee schedule and the liquidation engine are all different.
A proxy is used because CFM has 14 months of price history and no funding
history at all, not because the two are interchangeable.

**Any pre-registration that leans on these numbers must declare them as
proxy-venue history and carry that as a stated limitation** — the same way
Closure 2 declared its borrowed SD as borrowed.

One corroboration, worth exactly what one reading is worth: CFM's current
hourly funding rate for BTC PERP annualises to **+12.26%/yr**, against Binance's
2020–2026 mean of **+11.80%/yr**. That is a single snapshot against a six-year
mean, not a validation — but it is not evidence of a level mismatch either.
Current CFM annualised rates vary widely across the book (ETH +5.26%,
ZCASH **+42.05%**, 1000SHIB −6.13%).

---

## 6. Numbers — (a) funding drag

From Binance funding history, 2020-01-01 → 2026-08-31, 7,305 intervals per
symbol. Annualised as `rate × (24/interval_hours) × 365`, expressed as a
percentage of **position notional**, for a **long** position. A positive
published rate is paid by longs; a negative one is received, so the means below
are already net of the intervals the long was paid. Machine-readable copy:
[`data/perps_funding_2026-09-18.csv`](data/perps_funding_2026-09-18.csv).

| Symbol | Intervals | Long pays in | Mean | Median | p10 | p25 | p75 | p90 | p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| BTCUSDT | 7,305 | 85.89% | **+11.798%/yr** | +10.161%/yr | −1.726 | +2.949 | +10.950 | +25.531 | +116.287 |
| ETHUSDT | 7,305 | 86.17% | **+13.947%/yr** | +10.950%/yr | −1.589 | +3.340 | +10.950 | +34.422 | +148.262 |

Percentiles are of the annualised rate of *individual intervals* — the cost of
being long through one typical interval, extrapolated. They are not a
distribution of annual outcomes and the p99 must not be read as "a bad year".

### By year

| Year | BTC mean | BTC median | BTC long pays | ETH mean | ETH median | ETH long pays |
|---|---:|---:|---:|---:|---:|---:|
| 2020 | +17.193% | +10.950% | 85.7% | +27.415% | +10.950% | 97.4% |
| 2021 | +30.608% | +10.950% | 92.7% | +37.537% | +10.950% | 95.9% |
| 2022 | +4.165% | +5.623% | 77.9% | +0.787% | +4.244% | 65.8% |
| 2023 | +7.866% | +9.050% | 89.9% | +8.259% | +9.036% | 90.9% |
| 2024 | +11.924% | +10.950% | 91.6% | +12.961% | +10.950% | 95.8% |
| 2025 | +5.126% | +5.285% | 87.1% | +4.929% | +5.256% | 83.8% |
| 2026 (to 08-31) | +2.618% | +3.118% | 71.5% | +1.568% | +2.462% | 67.2% |

The long side pays in ~86% of all intervals and in every single year. The level
is strongly regime-dependent — 2021 at ~31–38%/yr, 2026 to date at ~1.6–2.6%/yr
— and it has been falling since 2021. **A long-only program on this venue is
structurally on the paying side of funding.** A short-side program would be on
the receiving side, at higher margin (§2).

### What a program in a position X% of the time pays

Arithmetic only — mean annual rate × X. No rule is simulated, and X is an
assumption supplied to the arithmetic, not a measurement of anything.

| X (in position) | BTC drag | ETH drag | = perp round trips | = spot round trips |
|---|---:|---:|---:|---:|
| 25% | **2.950%/yr** | **3.487%/yr** | 15.1 – 17.9 | 2.09 – 2.47 |
| 50% | **5.899%/yr** | **6.973%/yr** | 30.2 – 35.7 | 4.18 – 4.94 |
| 75% | **8.848%/yr** | **10.460%/yr** | 45.3 – 53.6 | 6.26 – 7.40 |

### The one sentence the brief asked for

**Long-side funding drag at 50% in-position (5.90–6.97%/yr of notional) is
larger than the 1.41% spot round trip we just escaped — by a factor of 4.2 to
4.9 — so on any program trading fewer than about five round trips a year,
Option C moves the cost rather than removing it.**

The full crossover, at 50% in-position: perps cost less than spot above
**4.85 round trips/yr** (BTC funding) or **5.73 round trips/yr** (ETH). At the
frozen spot mechanism's own rate of ~22.6 round trips/yr, perps cost
**10.31%/yr** (BTC) or **11.38%/yr** (ETH) of notional against spot's
**31.93%/yr** — 2.8x to 3.1x cheaper, not 7.2x, because funding consumes more
than half the fee saving.

### Leverage amplifies this, and the cap forces leverage

Funding is charged on **notional**, not on equity. One ETH PERP contract is
$258.40 of notional against a $100 cap — 2.58x. At 50% in-position the ETH drag
of 6.97%/yr of notional is therefore **18.0%/yr of the cap**. At BTC PERP's
4.07x overnight maximum it would be 24%/yr of the margin posted. Nothing in the
spot pipeline's cost model has a term for this.

---

## 7. Numbers — (b) power

Estimator reused unmodified from
[`../../backtesting/universe_inventory.py`](../../backtesting/universe_inventory.py):
`rho_bar` is the mean pairwise Pearson correlation of daily log returns over the
common overlap window, and `N_eff = N / (1 + (N−1)·rho_bar)`.

| | Value |
|---|---|
| Symbols | BTCUSDT, ETHUSDT perps (proxy venue) |
| Common overlap | 2020-01-01 → 2026-08-31 (6.66 y) |
| **rho_bar** | **0.8357** |
| **N_eff** | **1.09** (from N = 2) |

BTC and ETH perps are more correlated with each other (0.84) than the 20–61
spot pairs of Closure 2 were with each other on average (0.57), so two perps
carry **1.09** independent assets against spot's 1.67–1.73. **Breadth is worse
here, not better.** Option C's power case rests entirely on event *rate*.

### Per-trade dispersion — the derivation

`SD` below is the sample standard deviation of **one-bar log returns** on the
proxy series. The model behind it: a trade at signal frequency *f* is taken to
enter at one bar's open and leave at the next — the shortest position a signal
at that frequency can express — so its return dispersion is exactly the
dispersion of one-bar returns. That is a property of the price series and needs
no rule.

| Frequency | SD per bar |
|---|---:|
| daily | **3.7780%** |
| 4h | **1.4715%** |

Sanity check: 3.7780 / 1.4715 = 2.57 against √6 = 2.45, so daily dispersion is
~5% above the square-root-of-time scaling of the 4h figure — mild, and in the
direction of positive autocorrelation or fatter daily tails.

**This SD is a lower bound and every floor below is therefore optimistic.** A
real rule holds for more than one bar, and dispersion grows roughly as √t, so
true per-trade SD is larger and true floors are higher. This is the same
direction of error Closure 2 recorded for its own floors.

### Event counts and floors

`n = N_eff × bars_per_year × years`, where bars_per_year is the **bar count** —
the maximum number of non-overlapping observations a signal at that frequency
can produce. A real rule is in a position on a minority of bars and holds more
than one, so its realised `n` is far lower and its floor higher. Floor is the
standing policy's `1.645 × SD / √n`, unchanged.

| Frequency | Years | n | **Decidable-edge floor** | vs perp round trip (0.1952%) |
|---|---:|---:|---:|---|
| daily | 1 | 398 | **0.3116%/trade** | 1.60x the round trip |
| daily | 2 | 796 | **0.2203%/trade** | 1.13x |
| daily | 3 | 1,194 | **0.1799%/trade** | **0.92x — just below** |
| 4h | 1 | 2,387 | **0.0495%/trade** | 0.25x |
| 4h | 2 | 4,774 | **0.0350%/trade** | 0.18x |
| 4h | 3 | 7,161 | **0.0286%/trade** | 0.15x |

This is the first time in this repository a floor has come in **below** the cost
of trading. On spot, Closure 2's best floor was 2.02% against a 1.41% round
trip; here the 4h floors sit at 15–25% of the round trip after a single year.

### Can daily frequency decide an edge of 0.1% / 0.3% / 0.5% within 3 years?

| Target edge | n required | Years at 398/yr | Within 3 years? |
|---|---:|---:|---|
| 0.5%/trade | 154 | **0.39 y** | **yes** |
| 0.3%/trade | 429 | **1.08 y** | **yes** |
| 0.1%/trade | 3,862 | **9.71 y** | **no** |

### Against the standing policy's floor gate

The standing policy requires a declared SESOI derived from the operational fee
schedule, and refuses to start a trial whose floor exceeds it. Using the same
construction as Closure 1 — one tenth of the schedule's break-even gross move —
this venue's SESOI would be **0.01952%/trade**, because the SESOI scales with
the fee and the fee is small.

| Frequency | 3-year floor | SESOI | Gate | Years to reach SESOI |
|---|---:|---:|---|---:|
| daily | 0.1799% | 0.01952% | **fails, 9.2x over** | ~255 y |
| 4h | 0.0286% | 0.01952% | **fails, 1.5x over** | **~6.4 y** |

Both fail as constructed. The 4h case fails by 1.5x and clears at 6.4 years,
against spot's 13–52 years for far coarser targets — a different order of
problem, but a failure nonetheless.

**This exposes something about the policy, not about the venue.** SESOI scales
with cost; the floor does not. A cheaper venue therefore makes smaller edges
economically meaningful and *harder* to demonstrate, so the gate tightens
exactly where trading gets cheap. That is statistically correct and worth
stating out loud, because it means the one-tenth-of-break-even construction —
which was never load-bearing for Closure 1, where the bound was below zero
outright — **is** load-bearing here. Whether one tenth is the right coefficient
on a low-cost venue is an open question this document does not settle, and
anyone pre-registering here would have to settle it first, in advance.

---

## 8. Cost to build

Engineer-days are for someone who knows this codebase, and include tests at the
standard the repository already holds. They do not include research time,
waiting for forward data, or the decision itself.

### What carries over unchanged

| Asset | Why it transfers |
|---|---|
| `pipeline/fees.py` schedule pattern | dated schedules, per-order stamping, `active_schedule()` — the futures rate is another schedule, and the per-order boundary rule already matches how CFM prices |
| `docs/trial_registry.md` + standing policy | venue-independent; SESOI, kill rule, floor gate, retained series and the monotone counter all apply as written (with §7's coefficient question settled first) |
| `research_runner.py` provenance | content-addressed code/env/input identity is venue-independent |
| Fail-closed typed gates (Phase 6.8) | `PASS` / `BLOCK` / `UNAVAILABLE` applies unchanged, and §4's `TypeError`-when-flat is a new instance of exactly the class it guards |
| `stf_cost_probe.py` pattern | view-only key + exchange-verified permissions + declared-in-advance sample size; the instrument is right, the endpoints change |
| Equivalence tooling (`cost_sensitivity.py` §6) | bound, bootstrap and SD machinery operate on any per-trade series |
| Hydration pattern (`hydrate_research_data.py`) | credential-free public candles, same shape |

### What must be built

| Item | Days | Main risk |
|---|---:|---|
| Futures client + auth surface (`exchange/`), view-only key handling, venue filtering on fills, SDK `TypeError`-when-flat workaround | **3–4** | the SDK is thin here; venue leakage in `get_fills` is a silent-wrong-data defect, not a crash |
| Contract/position model: integer contracts, notional ≠ order size, short side, initial vs maintenance margin, intraday vs overnight margin windows, liquidation distance | **6–8** | the whole spot sizing model assumes a divisible dollar notional; this is a rewrite of the position concept, not an adapter |
| Funding accounting on open positions — hourly, calendar-time, accrued against an open position rather than per trade | **4–5** | every P&L, equity and cost path in the repo is per-trade; calendar-time cost is a new axis and `funding_pnl` is only an account aggregate |
| Perp cost probe (quoted impact + fee tier + realised funding), 14–30 day cohort | **2–3** | needs a view-only key that does not yet exist (§0) |
| Hydration for perp candles **and** a funding recorder | **3–4** | candles are easy; **funding history does not exist to hydrate** (§4), so the recorder starts the clock at zero depth and its data is only as good as its uptime |
| Leveraged-venue risk controls: margin-call and liquidation distance monitoring, overnight-window margin step-up, forced-close handling, per-contract exposure caps, kill switch | **6–8** | this is the class of failure spot never had — a liquidation is not a stop-out, and the existing circuit breakers read a balance, not a margin ratio |
| Registry/provenance wiring, docs, CI | **2** | low |
| **Total** | **26–34 engineer-days** | |

Not included, and each could be material: settling the rate-vs-per-contract fee
question (§3); deciding the SESOI coefficient for a low-cost venue (§7); and
whatever tax and regulatory treatment a CFTC-regulated futures account carries,
which is outside this document entirely.

### The smallest configuration that could run a pre-registered shadow trial

A shadow trial places no orders, so most of the price tag above is deferrable.
The irreducible core is: a credential-free candle reader for one or two CFM
perps, a **funding recorder** polling `funding_rate` hourly and appending to a
journal, a contract-aware position model that can express "1 contract, entered
here, exited there" with integer sizing, funding accrued hourly against the
open position, and the registry entry with its SESOI, kill rule and declared
floor. That is roughly **8–11 engineer-days**, most of it the position model and
the funding accrual, and it needs a view-only key before any authenticated read.

How long its forward data would take to reach the floor gate is the part that
does not shrink. A daily-frequency shadow program on BTC+ETH perps generates
about 398 independent bar-observations a year at `N_eff` 1.09 — and far fewer
actual trades, since a rule is not in a position on every bar. Against the
policy's floor gate as currently constructed it never arrives, at ~255 years.
Against the more useful question — the smallest edge it could *decide* — it
reaches 0.3%/trade in about **13 months** and 0.1%/trade only after about
**9.7 years**. At 4h the same program reaches the gate in about **6.4 years**
and 0.1%/trade in about **3 months**. Those are the honest brackets: a 4h
program on this venue buys real statistical power for the first time in this
project's history, and a daily one does not. Both numbers are optimistic,
because the SD they rest on is a one-bar lower bound (§7) and because a real
rule trades on a minority of bars.

---

## 9. Facts that bear on Option C without any further measurement

Recorded as facts, not as a conclusion. Each is independently sufficient to
change the shape of the question, and each was observed rather than inferred.

1. **The futures wallet holds $0 and buying power is $2.29.** Nothing can run
   here without a funding transfer, which is the owner's decision.
2. **At the $100 cap, 24 of 29 products cannot be held overnight at all**, and
   BTC, SOL and ZEC are among them. The only project asset reachable is ETH, at
   one indivisible contract consuming 63% of the cap and carrying 2.58x
   leverage on it. Percentage sizing and fractional circuit-breaker reductions
   cannot be expressed.
3. **Long-side funding drag exceeds the spot round trip several times over per
   year** — at 50% in-position it is 4.2–4.9 spot round trips annually — so
   below ~5 round trips/yr Option C is more expensive than the venue it would
   replace, and the cost advantage only exists for programs that trade often.
4. **CFM has no funding history, in the SDK or anywhere observed.** A trial
   whose cost model depends on funding must either use proxy-venue history and
   declare it, or start recording today at zero depth.
5. **The cost improvement is 7.24x per round trip, not an order of magnitude**,
   and it falls to 2.8–3.1x in total annual cost once funding is included at a
   realistic in-position fraction.
6. **The repository's main credential can trade** (§0), so the existing
   view-only discipline is not currently satisfied by anything on disk.
