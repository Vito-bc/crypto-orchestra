# Venue scoping — Bullish US, CME futures via a broker, Bitstamp

**2026-09-20. READ-ONLY. No account opened, no credentials created, no
authenticated call made, no order placed, no P&L computed, no trial ID, no
recommendation.** Nothing here changes `DRY_RUN`, `LIVE_BALANCE_USD`,
`ASSET_CONFIG`, or the V3 / 7B / 7R3b determinations in
[`../trial_registry.md`](../trial_registry.md). No `pipeline/` or `exchange/`
file was touched.

This is a fact sheet and a price tag for a New York resident, so that a future
build decision — if one is ever made — starts from measured numbers instead of
marketing copy. Section headers are **facts**, **costs**, **traps** and
**price tag**, never "verdict".

**Why now.** The research program is CLOSED (`docs/trial_registry.md`,
Closures 1–3 and the standing policy). This task does not reopen it. The
program closed on two independent walls: **cost** (1.4–1.8% spot round trip /
0.195% CFM perp round trip) and **power** (the floor identity in
[`2026-09-19-perps-gate.md`](2026-09-19-perps-gate.md), which is
venue-independent). Venue choice can only move the cost wall. Three venues
plausibly move it a lot for a New York resident; this prices them before
anyone opens an account.

Reproduce the data-driven numbers below with:

```powershell
venv\Scripts\python.exe backtesting\hydrate_perps_proxy.py     # credential-free, Binance proxy
venv\Scripts\python.exe backtesting\venue_scoping.py
venv\Scripts\python.exe backtesting\venue_scoping.py --verify
```

The arithmetic tables (CME cost, expressibility, spot-venue comparison, carry
re-derivation) are computed by
[`../../backtesting/venue_scoping.py`](../../backtesting/venue_scoping.py),
which reuses `carry_scoping.analyse_symbol` unmodified for the carry
re-derivation so it cannot drift from the already-reviewed carry document's
cycle and margin-event arithmetic. CSVs are under
[`data/`](data/) with the `venue_` prefix and today's date.

**A note on sourcing.** `cmegroup.com` and `interactivebrokers.com` returned
HTTP 403 to every fetch this task made (confirmed independently: a browser
user-agent via `curl` still gets 403 from `cmegroup.com`; the same request
succeeds against `interactivebrokers.com`'s static pages, which are client-
rendered and carry no fee tables in their HTML). Every CME/IBKR number below
that is not computed from this project's own hydrated data is therefore
**second-hand** — sourced from broker-comparison sites, reseller pages and
search-engine summaries — and is marked as such. Bullish's public REST API and
help-centre article were reachable directly and are treated as primary.

---

## Part A — Bullish US (reference spot venue)

### facts

**Confirmed directly against the public, credential-free markets endpoint**
(`GET https://api.exchange.bullish.com/trading-api/v1/markets`, read
2026-09-20, 1,737 objects):

| marketType | count |
|---|---:|
| OPTION | 1,472 |
| SPOT | 202 |
| PERPETUAL | 46 |
| DATED_FUTURE | 17 |

This matches the brief's own figures exactly.

**Our four assets, spot markets only:**

| Base | USD/USDC/USDT markets | feeGroupId | minQuantityLimit | Enabled |
|---|---|---:|---:|---|
| BTC | BTCUSD, BTCUSDC, BTCUSDT | **3** | 0.0001 | all three |
| ETH | ETHUSD, ETHUSDC, ETHUSDT | **3** | 0.00498753 | all three |
| SOL | SOLUSD, SOLUSDC, SOLUSDT | **5** | 0.1 | all three |
| ZEC | *(none)* | — | — | **does not exist on Bullish** |

**feeGroupId → fee-schedule mapping.** The help-centre article
(`support.exchange.bullish.com`, article 9373547, fetched directly — it is a
Confluence page and rendered fully, unlike the marketing-portal URL the brief
warned is blocked) publishes fee schedules **by named category**, not by
`feeGroupId` number, and states explicitly: *"Each market supported on the
exchange is assigned a 'feeGroupId'... available using the `getMarkets` and
`getMarket by symbol` endpoints."* No public page maps a `feeGroupId` integer
to a category name. This task **cross-referenced** the category's own
published asset lists against the API's `feeGroupId` field per symbol:

- **"Token Markets"** (`-1.5 / 2 bps`) is published with an explicit market
  list that includes `LINK/BTC, LINK/USD, LINK/USDC, LINK/USDT`,
  `DOT/USDC`, `AVAX/AUSD, AVAX/USDC`. Querying the API, **every one of those
  markets carries `feeGroupId 4`.** LINK (5 markets), DOT (`DOTUSDC`), and
  AVAX's AUSD/USDC pairs all resolve to `feeGroupId 4` and no other market
  with `feeGroupId 4` was found off that list. This is an exact match, not a
  guess.
- **"Standard Markets"** is defined by exclusion: *"Any market not listed
  above falls into this category. Standard market covers all spot markets
  that are not classified as promotional markets or stablecoin markets."*
  BTC, ETH and SOL's USD/USDC/USDT markets do not appear in the published
  Token, Promotional, Promotional II, Stable Type I/II, FX or Delta-1 lists —
  so by the article's own exhaustive-exclusion definition, **all three are
  Standard Markets**, individual rate **0 bps maker / 0.5 bps taker**,
  regardless of whether their `feeGroupId` reads 3 (BTC, ETH) or 5 (SOL).
  `feeGroupId` 3 vs. 5 therefore appears to be an internal sub-bucketing
  (plausibly by listing tier or market-making arrangement) that does **not**
  change the published individual rate — evidenced, not officially
  documented as a 1:1 mapping.
- **ZEC is absent from the exchange entirely** — no `ZEC*` or `ZCASH*` symbol
  exists in any of the 1,737 markets. This closes Bullish for our current
  shadow asset outright; only BTC/ETH/SOL are reachable here, and the frozen
  research already found the V2 mechanism unprofitable on all three
  (`docs/research/artifacts/results.json`: BTC PF 0.359, ETH PF 0.476, SOL PF
  0.718 — Closure 1, `2026-08-warmup-semantics.v1`).

**Institutional threshold, in dollars.** Note 1 of the fee schedule: *"Clients
whose ADTV (calculated on active trading days) exceeds 0.01% of the
Exchange's 12-month Average Daily Trading Volume are subject to the
Institutional fee schedule."* Bullish's monthly-metrics press releases (a
20-month table, Jan 2025–Aug 2026, fetched from the Aug 2026 GlobeNewswire
release) give trailing-12-month **total spot volume**, Sep 2025–Aug 2026:

| Month | Spot volume ($B) | Month | Spot volume ($B) |
|---|---:|---|---:|
| 2025-09 | 37.3 | 2026-03 | 52.9 |
| 2025-10 | 77.5 | 2026-04 | 38.0 |
| 2025-11 | 75.3 | 2026-05 | 30.0 |
| 2025-12 | 52.2 | 2026-06 | 45.5 |
| 2026-01 | 45.4 | 2026-07 | 29.1 |
| 2026-02 | 77.4 | 2026-08 | 35.8 |

Sum = **$596.4B** over 365 days ⇒ exchange ADV ≈ **$1.634B/day**. 0.01% of
that is **≈ $163,400/day of taker volume** — the threshold at which an
Individual account would be reclassified Institutional.

**A $20,000 account could never reach it** on any plausible trading pattern:
$163,400/day is **8.2× the entire account's capital, traded every single day**
on the 45-day lookback the SDS formula uses. Note also that crossing the
threshold is not necessarily worse: the Institutional schedule at SDS < 50%
is **the same 0/0.5 bps** as Individual; it only worsens above a 50%
same-direction score, up to 2.5 bps taker at SDS ≥ 80%. There is no scenario
in which this account's size makes the threshold relevant in either
direction.

**US-eligible subset.** The Sept 2025 BitLicense press release and the Oct
2025 US-launch release both describe the license as covering "spot trading and
custody" for "institutions and advanced traders," and the launch release's own
text says explicitly the Oct 2025 launch **is** spot trading, with derivatives
described as a future item ("advanced traders will have access... in the near
future" — language now nearly a year old with no confirmed derivatives-for-US
follow-up found in this search). **Expected: spot only, confirmed** from the
primary launch language; whether US individuals see all 202 spot markets or a
curated subset was **not confirmed** from public sources — this needs
verification at onboarding, not assumed either way.

**Onboarding facts.** Legal entity: **Bullish US Operations LLC**, NYDFS
BitLicense (Virtual Currency Business Activity License #0000046), FinCEN MSB,
NMLS #2383000. KYC is mandatory for all customers (standard industry
requirement; no Bullish-specific detail beyond that was found). Deposits: no
fee ("Bullish does not charge fees for opening an account," deposit fee
"None" for every method in the published schedule). Withdrawal fees (from the
same fee-schedule page): USD Fedwire/International Wire/CHATS $30, EUR SEPA
€2, USD CUBIX Instant and EUR BLINC free; crypto withdrawal fees are
per-asset flat amounts (BTC 0.00006, ETH 0.0025, SOL 0.04, published on the
same page). **ACH was not found described anywhere** in the reachable
documentation — Bullish's funding rails as documented are wire (USD/EUR) and
crypto deposit; absence of ACH is a gap in what was found, not a confirmed
absence. No account minimum was stated.

**API.** REST, WebSocket and FIX all documented at `docs.exchange.bullish.com`
/ `api.exchange.bullish.com`. WebSocket limits found directly: **10 open
connections per API key (authenticated)**, **100 per IP (unauthenticated)**.
A "blanket rate limit across all requests" is stated for the REST API with no
published numeric value found. A sandbox environment is referenced explicitly
("api.exchange.bullish.com (unless one of the sandbox environments is being
used)") but its URL was not located. Order types: limit and market are
confirmed; post-only is implied by fee-schedule language about "maker orders
filled" and the AMM fee mechanics, but no order-placement API page was reached
that lists `post_only`/`IOC`/`FOK` flags explicitly — **not independently
confirmed**. A market/candle REST endpoint is referenced in the docs
navigation but a direct probe (`/trading-api/v1/markets/{symbol}/candle`)
returned an opaque `errorCode 15999` with the parameters this task guessed;
the correct query shape and historical depth were **not confirmed**.

**Liquidity at size.** The Aug 2026 metrics release states an average trading
spread of **2.13 bps across all products** in one summary and **2.53 bps
(spot only)** in a more detailed breakdown found from the same release — both
numbers trace to the same GlobeNewswire text and the discrepancy could not be
resolved from the sources reached; report both. No public order-book-depth
endpoint was confirmed reachable without deeper API-doc access.

**Venue risk, facts only.** US entity **11.5 months old** at this writing
(launched 2025-10-01). Custody: Bullish states a "full reserve" model with
1:1 segregation and integrates with Fireblocks Network for custody/transfer
connectivity; **no independent third-party proof-of-reserves attestation or
audit report was found** in this search — the full-reserve claim is
self-stated. No incident history was found (absence of evidence found, not
evidence of absence — this was a search of public sources, not a systematic
incident-database check). One transparency-relevant fact not requested but
material: Bullish's parent (ticker **BLSH**) is publicly listed and publishes
monthly volume/spread/volatility metrics — explicitly marked *"unaudited,
preliminary... may differ from final SEC-filed results."*

### costs

Standard Markets, Individual: **0% maker / 0.005% taker**. Worst case (both
legs taker) round trip = **0.01%**; maker-in/taker-out ≈ 0.005%.
`backtesting/venue_scoping.py` prices the worst case:

| | round trip | M1 fee drag (19.06 rt/yr) | M2 fee drag (191.80 rt/yr) |
|---|---:|---:|---:|
| **Bullish Standard, Individual** | **0.0050%** | **0.095%/yr** | **0.959%/yr** |
| Coinbase (ADOPTED, `pipeline/fees.py`) | 1.8000% | 34.31%/yr | 345.24%/yr |

Bullish's round trip is **360× cheaper** than Coinbase's adopted spot
schedule for BTC/ETH/SOL — but ZEC is not on this venue at all, and the three
assets it does offer are the three the frozen mechanism already lost money on
(Closure 1).

Deposit is free; a USD wire withdrawal is $30 flat (immaterial at scale, large
relative to a $100 test balance); crypto withdrawal is a flat per-asset
amount, not ad-valorem.

### traps

- **ZEC is not listed.** This alone closes Bullish for the project's current
  shadow asset without needing any further finding.
- **The `feeGroupId` → schedule mapping is inferred, not published.** The 3-
  vs-5 split for BTC/ETH vs. SOL was cross-referenced from two independent
  public documents (the fee article's category lists and the markets API's
  `feeGroupId` field) and is well-evidenced, but no page states "feeGroupId 3
  and 5 are both Standard Markets" outright.
- **"0% fees for individual accounts"** (the launch press release's own
  marketing language) is not literally true: taker is 0.5 bps, only maker is
  zero.
- **Institutional reclassification is not automatically a worse outcome** —
  the marketing framing that positions "staying Individual" as protective is
  not itself a cost signal at SDS < 50%.
- **Spread and volume metrics are exchange-wide, self-reported, and
  explicitly marked unaudited** — they are not this account's execution
  experience and should not be treated as an execution-quality guarantee.
- The candle/historical-data endpoint's correct request shape was not
  established from public sources in the time available; anyone building here
  needs to read the actual OpenAPI spec rather than the guessed shape used in
  this task's probe.

---

## Part B — CME micro / spot-quoted Bitcoin futures via a US futures broker

Reference broker: **Interactive Brokers (IBKR)**. Alternative named for
comparison: **Tradovate** (a futures-only broker with per-contract commission
typically in the same $0.25–$0.85 range IBKR quotes before exchange fees, per
the same broker-comparison sources used below — not independently verified
beyond that range).

### facts

**Contracts** (CME contract-spec pages 403'd this tooling; the figures below
are cross-corroborated across ≥2 secondary sources each — StoneX,
brokerchooser, optimusfutures, TradeFundrr — and are marked second-hand):

| Contract | Symbol | Unit | Tick | Settlement | Listed months |
|---|---|---|---|---|---|
| Micro Bitcoin futures | MBT | 0.10 BTC | $5.00/BTC = **$0.50/contract** | Cash, to CME CF Bitcoin Reference Rate (BRR) | Monthly ×6 + 2 Dec |
| Spot-Quoted Bitcoin futures | (SQF) | **0.01 BTC** | reported as $1.00/index point | Cash, tracks spot via **daily financing adjustment** | Long-dated, no monthly roll |
| Micro Ether futures | MET | 0.10 ETH | $0.50/ETH = **$0.05/contract** | Cash, to CME CF Ether-Dollar Reference Rate | All 12 months |
| Spot-quoted ETH contract | — | — | — | — | **existence plausible, not independently confirmed** — one source states Spot-Quoted futures exist for "bitcoin, ether, SOL and XRP," but its own contract-spec page could not be reached |

**Trading hours** (standard CME Globex crypto-contract language, quoted
across multiple secondary sources for MBT/BTC): **Sunday 6:00pm – Friday
5:00pm ET**, with a **60-minute daily halt beginning 5:00pm ET**. This means
the market is **closed** every weekday from 5:00–6:00pm ET, and from **Friday
5:00pm ET through Sunday 6:00pm ET** (the weekend). One secondary source
describes MET's daily pause differently — "Saturday 2:00–4:00am CT" plus
"Monday–Friday 4:00–4:02pm CT" (a **2-minute**, not 60-minute, daily halt).
**This inconsistency across sources was not resolved**; the structural count
below applies the 60-minute assumption uniformly, and if MET's shorter halt is
the accurate one, the closed-hours fraction for that specific contract is a
slight overstatement.

**The Spot-Quoted contract's daily financing adjustment**: described only as
"a daily financing adjustment" that lets the contract track spot without a
monthly roll. **What index or rate it references could not be determined**
from any source this task reached — this is an open question, not a fact,
and it matters directly for comparing it to CFM's hourly perpetual funding.

**CME margin.** No current, dated figure could be obtained — every
`cmegroup.com` margin page 403'd. Secondary sources give **stale or
approximate** figures only: one dated snapshot of **$2,395.80 initial margin,
as of 2021-05-03**; a commonly repeated approximation of **"~$2,000 per lot"**
with no date attached. CME margin is SPAN-based and moves with price and
realized volatility, so neither figure should be treated as current — **any
build here must re-read margin live**, not from this document. IBKR's own
margin is typically the CME minimum plus a broker markup; the specific
multiplier was not obtainable (the commissions/margin pages are
client-rendered and carry no data in static HTML, even where the page itself
returned HTTP 200).

**Data.** CME's own futures history is not credential-free. Databento
(`GLBX.MDP3` dataset) lists both `BTC` and `MBT` as available products with
"usage-based pricing or included with any CME subscription" — an exact dollar
figure was **not obtained** without creating an account, so treat as
"paid, price unconfirmed" rather than a specific number. IBKR's TWS API
(`reqHistoricalData`) serves historical futures bars to a funded or paper
account holder, subject to IBKR's standard market-data entitlement rules
(delayed/sample data is generally available without a paid subscription;
full real-time and deep historical bars typically require one — the exact
entitlement tier for MBT/MET specifically was not confirmed).

**API.** IBKR's TWS API and Client Portal Web API both support futures order
placement, position and fill queries. A **Paper Trading Account** — requested
free via Account Management — mirrors this with simulated fills against real
market data and the same API surface, which is the natural shadow venue for
this project if this line is ever pursued.

**Eligibility.** Standard US retail futures-account application plus a
margin agreement; crypto-futures trading is a separate permission toggled in
account management alongside standard futures approval. **No state
restriction was found** in the sources reached (the brief expected none) —
this is "no contradicting evidence found," not an exhaustively confirmed
absence.

**Tax, as a fact, not advice.** CME-listed Bitcoin and Ether futures
(including the micro contracts, as CME-listed regulated futures) qualify as
**IRC Section 1256 contracts**: gains/losses are marked-to-market at year-end
and split 60% long-term / 40% short-term capital gain or loss regardless of
actual holding period, reported on IRS Form 6781.

### costs

Computed by `backtesting/venue_scoping.py`
([`data/venue_cme_cost_2026-09-20.csv`](data/venue_cme_cost_2026-09-20.csv)),
using the brief's own reference point — **$2.25/contract/side commission**
(brokerchooser's quoted IBKR benchmark) **+ ~$1.20/contract/side exchange,
clearing and NFA fees** (same source family) — which this task independently
corroborated: brokerchooser separately states a "$11.25 benchmark fee...
based on a typical trade of 5 contracts" (= $2.25/contract) for commission
alone, and "~$1.2 exchange and clearing" bringing the total to **$3.45/side,
$6.90/round trip**, matching the brief exactly. This is a **flat dollar fee**,
not ad-valorem, so its cost as a percentage of notional *falls* as BTC's price
rises — the opposite of Coinbase's percentage-based model:

| BTC price | Contract notional (0.1 BTC) | Round trip, $ | Round trip, % of notional | M1 fee drag (19.06 rt/yr) | M2 fee drag (191.80 rt/yr) |
|---:|---:|---:|---:|---:|---:|
| $60,000 | $6,000 | $6.90 | 0.1150% | 2.19%/yr | 22.06%/yr |
| $70,000 | $7,000 | $6.90 | 0.0986% | 1.88%/yr | 18.91%/yr |
| $80,000 | $8,000 | $6.90 | 0.0863% | 1.64%/yr | 16.54%/yr |

This excludes any margin financing cost, bid-ask spread or slippage — pure
commission and exchange fee only.

**Expressibility at $10,000 and $20,000**
([`data/venue_expressibility_2026-09-20.csv`](data/venue_expressibility_2026-09-20.csv)),
at 2%/5%/10% sizing:

| Account | BTC price | Product | Unit notional | 2% target | 5% target | 10% target | Contracts expressible (2% / 5% / 10%) |
|---:|---:|---|---:|---:|---:|---:|---|
| $10,000 | $60,000 | MBT (0.1 BTC) | $6,000 | $200 | $500 | $1,000 | **0 / 0 / 0** |
| $10,000 | $60,000 | Spot-Quoted (0.01 BTC) | $600 | $200 | $500 | $1,000 | 0 / **1** / **2** |
| $20,000 | $80,000 | MBT (0.1 BTC) | $8,000 | $400 | $1,000 | $2,000 | **0 / 0 / 0** |
| $20,000 | $80,000 | Spot-Quoted (0.01 BTC) | $800 | $400 | $1,000 | $2,000 | 0 / 1 / 2 |

**MBT is not expressible at all** at the project's 2%/5%/10% sizing
convention on either account size at any of the three BTC prices tested — one
contract alone is **30% to 133% of the entire account**, let alone a sizing
fraction of it. The 50%/25% circuit-breaker reductions are equally
inexpressible (the CSV's `cb_50pct_expressible`/`cb_25pct_expressible` columns
are `False` everywhere for MBT). The Spot-Quoted contract becomes marginally
expressible from the 5% fraction upward, but with large residual directional
exposure from integer rounding — e.g. at $10k/$60k-BTC/10% sizing, the exact
target is 1.667 contracts; rounding to 2 leaves a **20% residual** against the
sizing target (see the CSV's `residual_pct_of_target` column, which runs
20–100% across the grid).

**Weekend gap, measured**
([`data/venue_weekend_gaps_2026-09-20.csv`](data/venue_weekend_gaps_2026-09-20.csv),
350 weekly observations, BTC, Binance 4h proxy, 2020–2026 — the same
credential-free proxy history `perps_gate.py` uses, re-hydrated for this task
via `hydrate_perps_proxy.py`; **this is BTC's price path, not CME's own price
history, and it is a structural measurement, not P&L**):

| | value |
|---|---:|
| Weeks with both a pre-Fri-5pm-ET and post-Sun-6pm-ET bar | 350 |
| Mean | +0.045% |
| SD | 3.541% |
| Min / Max | −14.85% / +13.45% |
| p05 / p95 | −5.42% / +5.45% |
| Fraction with \|move\| > 1% | **70.0%** |
| Fraction with \|move\| > 3% | 27.4% |
| Fraction with \|move\| > 5% | 12.0% |

**Fraction of M1's entries/exits inside CME-closed hours**
([`data/venue_cme_closed_hours_2026-09-20.csv`](data/venue_cme_closed_hours_2026-09-20.csv),
frozen Donchian 55/20 on 4h, imported unmodified from `stf_feasibility.py`,
same mechanism `perps_gate.py` uses as M1 — a **structural count of
timestamps**, no return computed):

| Symbol | Total events | Entries closed | Exits closed | All events closed |
|---|---:|---:|---:|---:|
| BTC | 256 (128E/128X) | 18.75% | 19.53% | 19.14% |
| ETH | 288 (144E/144X) | 17.36% | 18.75% | 18.06% |

Roughly **1 in 5** of this mechanism's signals would fall inside CME's closed
window and have to wait for the next session open rather than executing
contemporaneously with the signal bar that produced them.

### traps

- **Every CME/IBKR fact above not computed from this project's own data is
  second-hand.** `cmegroup.com` returned 403 to every attempt, including with
  a browser user-agent via `curl`; `interactivebrokers.com`'s pricing/margin
  pages return HTTP 200 but are client-rendered React apps carrying no data
  in static HTML. Re-verify against a live account or quote screen before any
  build decision.
- **Published margin figures are stale** — one is explicitly dated
  2021-05-03. Do not use any dollar margin number in this document for
  sizing; margin must be read live.
- **The daily-halt duration disagrees across sources** (60 minutes vs. 2
  minutes for MET specifically) and was not resolved.
- **MBT's flat-dollar fee structure inverts the usual cost intuition**: it
  gets relatively cheaper as BTC's price rises, the opposite of an ad-valorem
  venue like Coinbase or Bullish.
- **MBT is not expressible at this project's position-sizing convention** at
  $10k–20k account scale. A build here needs either a different sizing rule
  (e.g., "1 contract minimum," which produces large discrete leverage jumps
  as the account grows), a materially larger account, or reliance on the
  smaller Spot-Quoted contract — whose fee schedule and financing-rate
  reference are both unconfirmed.
- **No IBKR-specific fee schedule was found for the Spot-Quoted contract**;
  the cost table above prices MBT only.
- **The Spot-Quoted contract's financing index is unknown.** Comparing it to
  CFM's hourly perpetual funding (Option C, already scoped) is not yet
  possible without knowing what the daily adjustment references.

---

## Part C — Bitstamp by Robinhood (second spot reference)

### facts

Official schedule from `bitstamp.net/fee-schedule/` (given directly in the
brief; this task's own fetch of that URL returned an empty client-rendered
page and could not independently re-confirm the numbers, so they are taken as
given and cross-checked only against general market knowledge of Bitstamp's
publicly known tier structure, which is consistent):

| 30-day volume | Maker | Taker |
|---|---:|---:|
| $0 – $1,000 | 0.00% | 0.00% |
| $1,000 – $10,000 | 0.30% | 0.40% |
| $10,000 – $100,000 | 0.20% | 0.30% |

**NYDFS BitLicense confirmed** — Bitstamp is named alongside Coinbase, Circle
and Robinhood Crypto as a BitLicense holder in multiple sources, and it is
now Robinhood-owned.

**Markets.** BTC/USD, ETH/USD and SOL/USD are all listed and among Bitstamp's
higher-volume pairs; **ZEC is not confirmed listed** (not found in the pairs
searched — Bitstamp historically delisted several privacy coins in various
jurisdictions; this was not independently resolved and should be checked
directly before relying on it).

**The 20% fiat/stablecoin volume weighting does NOT affect a BTC-USD
trader.** Bitstamp's own blog post on the tier mechanics states the 20%
weighting applies specifically to **fiat/stablecoin-pair volume** (e.g.
USDC/USD) counting at only 20% of notional toward the tier calculation — it
exists to let stablecoin-conversion traders reach lower tiers with less
"real" volume. BTC/USD, ETH/USD and SOL/USD are crypto/fiat pairs, not
fiat/stablecoin pairs, so their volume counts at **full (100%) notional**
toward tier progression. This is a clean, direct answer, not an inference.

**API.** REST (`bitstamp.net/api/`) and a WebSocket v2 feed are documented.
Order types confirmed from search of the API/FAQ pages: **limit, market,
instant, `daily_order`** (day-only, cancels at 00:00 UTC), **IOC**, and
**FOK** (Bitstamp has a dedicated FAQ page on fill-or-kill orders).
**Post-only was not confirmed** as a distinct API flag from the sources
reached — general crypto-exchange terminology pages describe post-only
generically, but no Bitstamp-specific confirmation of the flag's existence
was found. Rate limits: **400 requests/second**, with a secondary cap of
**10,000 requests per 10 minutes**, both raisable under a bespoke agreement. A
**sandbox environment exists** ("mimics the live API structure with separate
keys"). Historical OHLC: `GET /api/v2/ohlc/{pair}`, `step` in seconds, max
1,000 bars/request, **credential-free, with data reported reaching back to
2011** — this would be, by a wide margin, the **longest available history of
any venue examined in this scoping exercise**, exceeding even Coinbase's own
2020-12-08 ZEC start.

**Minimum order sizes** (sourced from Bitstamp's own historical blog posts on
minimum-size reductions, dated 2021–2022 — plausibly still current but not
re-confirmed against a live schedule): **0.0002 BTC** for BTC-denominated
pairs, **0.005 ETH** for ETH-denominated pairs, and a **$20 minimum notional**
for fiat/stablecoin-denominated pairs (USD, EUR, GBP, USDC and others). SOL's
specific minimum was not found and should be confirmed directly.

### costs

Round trip at the two size-relevant tiers, from
`backtesting/venue_scoping.py`
([`data/venue_spot_compare_2026-09-20.csv`](data/venue_spot_compare_2026-09-20.csv)):

| Tier | Round trip | M1 fee drag (19.06 rt/yr) | M2 fee drag (191.80 rt/yr) |
|---|---:|---:|---:|
| $0–1k/30d | 0.00% | 0.00%/yr | 0.00%/yr |
| $1k–10k/30d | 0.70% | 13.34%/yr | 134.26%/yr |
| $10k–100k/30d | 0.50% | 9.53%/yr | 95.90%/yr |

Both tiers are materially cheaper than Coinbase's adopted 1.80% round trip
(2.6× and 3.6× cheaper respectively), though nowhere near Bullish's.

### traps

- **This task could not independently re-fetch Bitstamp's live fee page** —
  the numbers above are taken as given in the brief and cross-checked only
  against general knowledge, not against a fresh primary read. Re-verify
  before relying on them for a build decision.
- **ZEC's listing status on Bitstamp is unconfirmed** — do not assume it is
  tradable here without checking directly.
- **Post-only is unconfirmed as an API order-type flag**, unlike IOC/FOK
  which have direct documentation.
- The $20/30-day tier boundary and per-asset minimums are sourced from posts
  dated 2021–2022; Bitstamp has changed its schedule and minimums before and
  may have again.

---

## Part D — costs and carry, comparative tables

### costs

**Spot round-trip comparison, all three venues plus Coinbase**, from
`backtesting/venue_scoping.py`:

| Venue | Round trip | M1 fee drag (19.06 rt/yr) | M2 fee drag (191.80 rt/yr) |
|---|---:|---:|---:|
| Coinbase, ADOPTED (`pipeline/fees.py`) | 1.8000% | 34.31%/yr | 345.24%/yr |
| **Bullish Standard, Individual** | **0.0050%** | **0.095%/yr** | **0.959%/yr** |
| Bitstamp, $10k–100k/30d | 0.5000% | 9.53%/yr | 95.90%/yr |
| Bitstamp, $1k–10k/30d | 0.7000% | 13.34%/yr | 134.26%/yr |

**Carry-cycle re-derivation**, pairing each spot venue with the existing
Coinbase CFM perp leg (round trip 0.195%), holding the funding series, the
harvest rule, and the margin-event frequencies from
[`2026-09-19-carry-scoping.md`](2026-09-19-carry-scoping.md) fixed — this
task's script calls `carry_scoping.analyse_symbol` unmodified with a new spot
schedule, so the cycle counts (BTC 40, ETH 33, over 6.67y) and margin-event
frequencies are **identical** to the already-reviewed document, only the
spot fee changes
([`data/venue_carry_rederived_2026-09-20.csv`](data/venue_carry_rederived_2026-09-20.csv)):

| Symbol | Spot venue | Cost/cycle | Top-up friction | **Break-even, median cycle** | **Break-even, realised rate** |
|---|---|---:|---:|---:|---:|
| BTC | Coinbase ADOPTED (existing doc) | 1.995% | 2.81%/yr | 45.68%/yr | **14.78%/yr** |
| BTC | **Bullish Standard, Individual** | **0.200%** | 0.01%/yr | **4.30%/yr** | **1.21%/yr** |
| BTC | Bitstamp $10k–100k | 0.695% | 0.78%/yr | 15.71%/yr | 4.95%/yr |
| BTC | Bitstamp $1k–10k | 0.895% | 1.09%/yr | 20.32%/yr | 6.46%/yr |
| ETH | Coinbase ADOPTED (existing doc) | 1.995% | 4.52%/yr | 38.68%/yr | **14.39%/yr** |
| ETH | **Bullish Standard, Individual** | **0.200%** | 0.01%/yr | **3.44%/yr** | **1.00%/yr** |
| ETH | Bitstamp $10k–100k | 0.695% | 1.26%/yr | 13.16%/yr | 4.69%/yr |
| ETH | Bitstamp $1k–10k | 0.895% | 1.76%/yr | 17.08%/yr | 6.19%/yr |

At Bullish's individual spot rate, the break-even collapses from **14.4–14.8%/yr
to ~1.0–1.2%/yr (realised-rate)** — an order of magnitude, because the spot
leg was 9× the perp leg's cost in the original pairing and is now negligible
next to it. This does **not** mean carry becomes profitable: the carry
document's own regime map shows trailing 7-day funding has not cleared even
the **old, higher** 14.4–14.8% break-even at any point in 2026, and 2026's
median trailing funding (3.0% BTC, 1.9% ETH) is still below the **new,
lower** ~1.0–1.2% break-even on some but not most days — the venue question
and the "is funding high enough right now" question are separate, and this
document answers only the first.

**The two-venue margin problem, restated per pairing.** In every pairing
above, the spot leg and the perp leg sit on **different venues** (Bullish or
Bitstamp for spot, Coinbase CFM for the perp), so a P&L swing on one cannot
top up margin on the other without an explicit transfer — the same structural
problem the original carry document raised for Coinbase-spot-vs-CFM, now
doubled by adding a third company into the loop. Rails: crypto on-chain
transfer (BTC ~10–60 min confirmation depending on fee/congestion, no
intermediary) versus USD ACH (1–3 business days) or wire (same-day,
$25–30 fee at each of Bullish/Bitstamp). Using the carry document's own
margin-event frequencies (7-day, 50%-of-margin threshold): **10.13 events/yr
(BTC)** and **14.90 events/yr (ETH)** — a two-venue pairing would need a
cross-company transfer roughly **once a month (BTC) to once every 3.5 weeks
(ETH)** merely to keep the short leg funded, each transfer carrying wire fees
and multi-day settlement risk that a same-venue pairing would not.

**A single-venue carry row is not available.** Bullish does not offer
perpetuals or futures to US individuals (Part A), so no same-venue carry
pairing exists on any of the three venues scoped here; the two-venue transfer
problem above is not avoidable on this venue set.

---

## Part E — build price tag (engineer-days)

### price tag

**What carries over unchanged**, per venue, from the existing harness (the
same list already established for the Coinbase CFM scoping in
[`2026-09-18-perps-scoping.md`](2026-09-18-perps-scoping.md) §8 — repeated
here because it applies identically to a spot-only venue too):

| Asset | Why it transfers |
|---|---|
| `pipeline/fees.py` schedule pattern | dated schedules, per-order stamping, `active_schedule()` — a new venue is another schedule entry, same shape |
| `docs/trial_registry.md` + standing policy | venue-independent; SESOI, kill rule, floor gate, retained series, monotone counter all apply unchanged |
| `research_runner.py` provenance scheme | content-addressed code/env/input identity is venue-independent |
| Fail-closed typed gates (Phase 6.8) | PASS/BLOCK/UNAVAILABLE applies to any new venue's entry filters unchanged |
| Hydration pattern (`hydrate_research_data.py`, `hydrate_perps_proxy.py`) | credential-free public candles, same shape, for any venue with a public candle endpoint (Bullish and Bitstamp both have one; CME does not) |
| Equivalence tooling (`cost_sensitivity.py` bound/bootstrap/SD machinery) | operates on any per-trade return series regardless of venue |

**What must be built, per venue:**

| Item | Bullish (spot) | Bitstamp (spot) | CME via IBKR (futures) |
|---|---:|---:|---:|
| Exchange client + auth | 2–3d (REST+WS, bearer token) | 2–3d (REST+WS, HMAC) | 4–5d (TWS/CP Web API is a different paradigm from a REST exchange client; paper-account wiring) |
| Fee-schedule entry (`pipeline/fees.py` pattern) | 0.5d | 0.5d | 1d (flat per-contract, not ad-valorem — the schedule dataclass needs a new shape) |
| Cost probe | 1d | 1d | 2d (needs a live margin/commission read, since both are unconfirmed from public sources) |
| Hydration | 1d (public candle endpoint exists, exact shape TBD) | 0.5d (documented `ohlc` endpoint, 2011+ depth) | 3–4d (no credential-free CME source found; either pay for Databento or hydrate from the broker API only for a funded/paper account) |
| Position model | 0.5d (same divisible-dollar-notional model as Coinbase spot — no change) | 0.5d (same) | **6–8d** (integer contracts, notional ≠ order size, margin financing, no percentage sizing without a rewrite — same class of work the CFM scoping document costed at 6–8d) |
| Risk controls specific to the venue | low (spot, no leverage) | low (spot, no leverage) | **6–8d** (liquidation distance is not applicable to CME retail futures the way it is to CFM, but margin-call handling, the weekend gap and the daily halt all need explicit handling — this document measured that 1-in-5 M1 signals fall in CME-closed hours, so an order-routing layer must decide what "signal during closed hours" means before this can run) |
| Registry/docs/CI wiring | 1d | 1d | 2d |
| **Total** | **~6–7 engineer-days** | **~5–6 engineer-days** | **~24–30 engineer-days** |

Bullish and Bitstamp are both **an order of magnitude cheaper to build** than
the CME/IBKR line, because they are spot venues that slot into the existing
divisible-dollar-notional position model with no new leverage or contract
concept — the same reason Coinbase spot itself was cheap to build originally.
CME/IBKR's price tag is comparable to the CFM perp build (26–34 days,
per the perps-scoping document) for the same underlying reason: **both
require replacing the position-sizing concept itself**, not just adding a
client.

**The smallest configuration that could run a pre-registered SHADOW.** For
Bullish or Bitstamp, the smallest shadow is nearly free: point the existing
`pipeline/fees.py`-shaped schedule and the existing hydration pattern at the
new venue's public candle endpoint, log signals with the new fee schedule
applied, and rely entirely on the existing position model — no orders, no new
credential, no new risk-control class. That is roughly **2–3 engineer-days**
for either venue, almost all of it hydration-endpoint discovery and a fee-
schedule entry, because nothing about the mechanism, the sizing, or the
registry changes.

For CME/IBKR, a shadow trial is not nearly as cheap, because the position
model itself has to change before a shadow can even *simulate* a fill: an
IBKR paper account removes the "real money" risk but not the "integer
contracts, margin, weekend gap" complexity, all of which a shadow still has to
represent correctly to be worth running. The irreducible core — a
credential-free or paper-account candle reader, a contract-aware position
model expressing "N contracts, entered here, exited there," and the registry
entry with its own SESOI and floor — is closer to **10–12 engineer-days**,
comparable to the CFM perp shadow estimate (8–11 days) for the same reason:
the expensive part is the position model, not the API client.

---

## What closes a venue on its own

Three things found in this task meet that bar without needing any further
measurement:

1. **ZEC does not exist on Bullish.** For the project's current shadow asset,
   this ends the question outright — Bullish can only ever be a venue for
   BTC/ETH/SOL, which the frozen mechanism already lost on.
2. **MBT is not expressible at this project's 2%/5%/10% sizing convention**
   at $10k–20k account scale (0 contracts expressible in every cell of the
   grid tested). A CME/IBKR build cannot use the existing sizing rule
   unmodified regardless of any other finding in this document.
3. **CME's daily halt and weekend closure remove roughly 1 in 5 of M1's
   signal timestamps from tradability** — any CME-routed version of a 4h
   mechanism needs an explicit policy for what happens to a signal generated
   while the market is closed, which is new design surface this venue
   specifically introduces.

None of these is a cost or power finding of the kind that closed the earlier
research lines — they are structural/expressibility findings about *this*
account's scale and *this* asset's availability, and they would not
necessarily block a materially larger account or a different asset set.

---

## Sources

- Bullish public markets API: `https://api.exchange.bullish.com/trading-api/v1/markets` (read 2026-09-20)
- Bullish fee schedule: `https://support.exchange.bullish.com/rest/servicedesk/knowledgebase/latest/articles/view/9373547` (read 2026-09-20)
- Bullish US launch: bullish.com press releases, PRNewswire (2025-09-17, 2025-10-01), SEC EDGAR filing (Bullish, 2025-09-17)
- Bullish monthly metrics: GlobeNewswire, 2026-09-08 (August 2026 release, 20-month table)
- CME contract specs: StoneX blog, brokerchooser.com, optimusfutures.com, TradeFundrr.com (all secondary; cmegroup.com returned HTTP 403)
- IBKR commission/margin: brokerchooser.com (secondary; interactivebrokers.com pages are client-rendered, no data in static HTML)
- Section 1256 treatment: terms.law, Lukka, McDermott Will & Emery tax alerts (general tax-law sources, not project-specific)
- Bitstamp fee schedule: as given in the task brief from `bitstamp.net/fee-schedule/`; this task's own fetch returned an empty client-rendered page
- Bitstamp API/OHLC: bitstamp.net/api/, CryptoDataDownload.com, Bitstamp blog posts (2021–2022, minimum order sizes)
- This project's own hydrated data: `data/perps_proxy/` (Binance USDT-margined perps, credential-free, via `backtesting/hydrate_perps_proxy.py`), used only for the weekend-gap and CME-closed-hours structural counts, and `docs/research/2026-09-19-carry-scoping.md`'s own cycle/margin-event data, reused unmodified via `carry_scoping.analyse_symbol`

---

Stopping here, as instructed. No account opened, no PR merged.
