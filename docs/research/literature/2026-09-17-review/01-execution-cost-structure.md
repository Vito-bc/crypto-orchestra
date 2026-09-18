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

# Transaction costs and crypto strategy viability — evidence review

**Scope note / headline:** the single most time-sensitive finding is unrelated to the academic questions — **Coinbase changed its Advanced fee schedule on 2026-09-16, one day after your `fee_tier_2026-09-15.json` evidence snapshot.** Multiple outlets reporting the same Coinbase press release put the new **US entry-level spot rate at 0.50% maker / 0.90% taker** (down from 0.60/1.20), with the first volume tier threshold cut from $25,000 to $10,000. I could not reach Coinbase's own page to confirm (see Q5). If true, your measured 0.6/1.2 and the 1.8% round trip derived from it are stale as of yesterday. Re-read `GET /api/v3/brokerage/transaction_summary` before treating that file as current.

*(Project Lead note, 2026-09-17: confirmed by the project's own daily probe — `logs/stf_cost_probe.jsonl` reading at 2026-09-17 00:05 UTC shows pricing_tier "Intro", maker 0.005, taker 0.009. Single reading; not adopted; adoption waits for a 4-reading cohort.)*

---

## Q1 — Transaction cost vs strategy viability: documented before/after numbers

**The strongest single before/after in crypto: Bysik & Ślepaczuk (2026), "Machine Learning-Based Bitcoin Trading Under Transaction Costs: Evidence From Walk-Forward Forecasting."**
https://arxiv.org/html/2606.00060v1

Hourly BTC, ~70,000 hourly observations, walk-forward. One cost level: **10 bps proportional per unit of turnover**, which the authors describe as a conservative all-in figure covering exchange fees, spread crossing and slippage. Same strategy configuration, zero-cost vs 10 bps:

| Model | Ann. return (0 bps) | Sharpe | Ann. return (10 bps) | Sharpe |
|---|---|---|---|---|
| XGBoost | +73.50% | 1.27 | **−64.00%** | −1.25 |
| LSTM | +72.43% | 1.45 | **−50.65%** | −1.16 |
| iTransformer | +129.07% | 2.59 | **−83.93%** | −1.82 |

Every headline result inverts at **one-tenth of a percent**. Baseline turnover was 8,400–17,900 trades over the sample; the measured cost drag was **1.43–6.06 bps per hour**. Their fix was not a better model but a cost-aware gate (trade only when forecast magnitude exceeds λ·c·turnover), which cut trades from 10,619 to 251 (−97% turnover) and restored XGBoost to +65.40% / Sharpe 1.09. The authors' own framing: the obstacle "is not only weak predictability, but also the way forecasts are converted into trades."
*Quality: working paper (arXiv preprint), not peer-reviewed. Authors are from the University of Warsaw quantitative finance group, which has a consistent publication record in this area. Methodology (walk-forward, explicit cost sensitivity, reported negative results) is better than typical for the genre. Rate: good working paper.*

**Cross-sectional crypto factors: Bianchi & Babiak, "A Factor Model for Cryptocurrency Returns" (May 2022 draft).**
http://wp.lancs.ac.uk/fofi2022/files/2022/08/FoFI-2022-056-Daniele-Bianchi.pdf

809 digital assets, 2016-12-02 to 2021-07-09, daily rebalancing. Cost assumption: **100 bps average bid-ask spread** (cross-sectional average of Corwin-Schultz and Abdi-Ranaldo synthetic spread estimates over the sample), plus 50 bps extra to open a short. Net-of-cost factor statistics (Table 1, daily mean %, annualised Sharpe):

| | Market | Size | Volatility | Amihud | Bid-ask | Reversal | mom r7 | r14 | r21 | r30 |
|---|---|---|---|---|---|---|---|---|---|---|
| Mean | 0.22** | 0.06 | 0.38 | −0.12 | 0.66** | 1.55*** | −0.06 | 0.03 | 0.04 | 0.07 |
| t | 2.14 | 1.76 | 1.16 | −0.79 | 2.18 | 9.34 | −0.29 | 0.18 | 0.21 | 0.89 |
| SR | 0.99 | 0.12 | 0.50 | −0.22 | 0.93 | 4.58 | −0.12 | 0.07 | 0.08 | 0.11 |

Directly relevant: **all four cross-sectional momentum specifications (7/14/21/30-day look-back) are statistically indistinguishable from zero net of 100 bps, with Sharpes between −0.12 and 0.11.** The authors' summary is that once reasonable costs are applied only market, liquidity and short-term reversal survive. Note the survivors are the two things a retail account cannot harvest: reversal at daily frequency implies enormous turnover, and the bid-ask factor is literally paid for by bearing illiquidity.
*Quality: conference/working paper draft by established academics (Bianchi has published widely on crypto asset pricing). The 100 bps figure is a spread estimate, not a fee — it is roughly comparable in magnitude to your one-way fee but measures a different thing. Rate: credible working paper; the specific draft I read is not the final journal version.*

**Peer-reviewed, crypto-specific, momentum: "Cryptocurrency anomalies and economic constraints," International Review of Financial Analysis 94 (2024).**
https://www.sciencedirect.com/science/article/abs/pii/S1057521924001509

Reported findings: **most momentum portfolios stay profitable after transaction costs but alphas fall by 26% to 53%**; size and volume anomalies come from micro-caps of negligible economic importance; alpha is extracted largely from the short leg; abnormal returns concentrate in bull markets and fade over time, with gains statistically insignificant in bear markets.
*Quality: peer-reviewed journal. **Caveat: I verified these numbers only from the abstract/indexing, not the full text** — ScienceDirect full text was not reachable. The 26–53% haircut is at institutional-scale cost assumptions on a long-short cross-sectional portfolio, not retail single-asset costs, so it is not a read-across to your situation.*

**Cost-sensitivity ladder, crypto perps: "Systematic Trend-Following with Adaptive Portfolio Construction" (arXiv 2602.11708, Feb 2026).** 150+ perp contracts, 6h bars, 134% monthly turnover, OOS Jan 2022–Dec 2024. Sensitivity: 0 bps → Sharpe 2.87 / +48.2%; 4 bps → 2.41 / +40.5%; 8 bps → 2.01 / +33.1%; 12 bps → 1.62 / +26.3%.
*Quality: **low.** Unrefereed preprint from an unidentified private "research" outfit, headline Sharpe 2.41 on crypto trend is implausibly high, and I could not verify the construction. I report it only because the sensitivity ladder is the shape of thing you asked for. My arithmetic on their ladder — not theirs — is that ~1.8 percentage points of annual return are lost per basis point of per-trade cost at their turnover, which would put a 180 bps round trip several hundred percentage points underwater against a 48% gross. Treat as illustrative of shape, not as a number.*

---

## Q2 — Does published crypto research account for realistic retail costs, and do results collapse?

**There is no meta-study I could find that counts what fraction of crypto strategy papers include realistic retail costs.** I searched for one specifically and came up empty. Anyone quoting a percentage to you is making it up. What exists is a pattern visible across individual papers rather than a measured base rate.

The pattern that is documented:

**The clearest single collapse-on-replication case: Hudson & Urquhart (2021), "Technical trading and cryptocurrencies," Annals of Operations Research 297(1).**
https://link.springer.com/article/10.1007/s10479-019-03357-1 (open-access PDF mirrored at https://centaur.reading.ac.uk/85715/)

~15,000 technical trading rules in five classes (moving average, filter, support-resistance, oscillator, channel breakout), on CoinDesk BTC (from 2010-07-18), Bitstamp (from 2012-12-01), Litecoin, Ripple (both from 2013), Ethereum (from 2015-08-07) — **all ending 2017-12-31.** Data-snooping controlled with multiple hypothesis testing. In-sample the paper is a positive result: all five rule classes show significant predictability and profitability, and the authors conclude performance "is not wiped out by appropriate transaction costs."

Then the out-of-sample test (best in-sample rule, first half of 2018, Table 9):

| | Best rule | Ann. return | Ann. Sharpe | Ann. Sortino |
|---|---|---|---|---|
| CoinDesk BTC | CB2: 25/0.05/0.025/5 | **−0.0010** | **−0.0502** | **−0.3470** |
| Bitstamp BTC | CB2: 25/0.05/0.025/3 | **−0.0091** | **−0.0641** | **−0.0553** |
| Litecoin | CB2: 25/0.05/0.025/5 | +0.0775 | 1.3553 | 2.1900 |
| Ripple | MA1: 2/0.001/1 | +0.0546 | 0.7380 | 1.2162 |
| Ethereum | MA4: 2/25/0/1/5 | +0.0631 | 1.1900 | 1.8500 |

Bitcoin — the most liquid, most studied asset — goes negative out of sample on both price series. Note this OOS test is **gross**; costs were not deducted. Note also that the entire in-sample window ends at the top of the 2017 bubble, and the OOS window is six months.
*Quality: peer-reviewed, well-constructed, honest about its own OOS failure. Rate: high, with the caveat that the in-sample period is an extreme bull and the OOS window is short.*

**The generic structural finding (equities, but it is the canonical reference): Novy-Marx & Velikov, "A Taxonomy of Anomalies and Their Trading Costs," Review of Financial Studies 29(1), 2016.** https://academic.oup.com/rfs/article-abstract/29/1/104/1844518 · NBER WP: https://www.nber.org/system/files/working_papers/w20721/w20721.pdf
Result: **in all cases transaction costs reduce profitability and statistical significance.** Anomalies with one-sided monthly turnover below 50% mostly keep significant net spreads when designed to mitigate costs; **few of the higher-turnover strategies do.** The most effective single mitigation is a buy/hold spread (keep holding what you would not actively trade into) — i.e. a no-trade band.
*Quality: peer-reviewed, top-3 finance journal, highly cited. Equities, not crypto. The transferable content is the turnover-threshold structure, not the numbers.*

**What I did not find:** a published, credible crypto study that applies retail-tier costs of the magnitude you face (60–120 bps per side). The upper end of cost assumptions in the crypto literature is around 100 bps of *round-trip spread* (Bianchi & Babiak) or ~50 bps *total* per trade (Lintilhac & Tourin 2017, as cited by Hudson & Urquhart). **Your 180 bps round trip is off the right-hand end of the cost range the published literature has ever tested.** That is itself a finding: no published crypto strategy result you could cite was evaluated at your cost level.

---

## Q3 — Break-even gross-move threshold: is there a grounded rule of thumb?

**The rule of thumb exists, is trivial, and is grounded — but only as arithmetic, not as an empirical finding.** The condition is: expected gross alpha over the holding period must exceed the round-trip cost. There is no crypto-specific empirical calibration of "how much cushion you need"; that part is practitioner assertion.

**The grounded academic version is Gârleanu & Pedersen, "Dynamic Trading with Predictable Returns and Transaction Costs," Journal of Finance 68(6), 2013, pp. 2309–2340.** https://onlinelibrary.wiley.com/doi/abs/10.1111/jofi.12080 · preprint: https://nbgarleanu.github.io/DynTrad.pdf
Closed-form optimal policy under quadratic costs: *aim in front of the target* and *trade partially toward the aim*. The load-bearing implication for you is that **the optimal weight on a signal falls with the signal's decay speed relative to cost** — a fast-decaying signal is worth less under cost not because it predicts less but because you cannot amortise the cost over its life. Cost does not just subtract from a strategy; it re-ranks which signals are worth acting on at all.
*Quality: peer-reviewed, Journal of Finance, foundational. Theory with a commodity-futures application; no crypto, no retail-fee calibration.*

**The practical form, with actual crypto data: Robot Wealth, "A simple, effective way to manage turnover and not get killed by costs."** https://robotwealth.com/a-simple-effective-way-to-manage-turnover-and-not-get-killed-by-costs/
Binance perps, top 30 by trailing 30-day volume, from Sept 2019, three alphas (carry, momentum, breakout), **0.15% commission** assumption. Implements the no-trade-buffer heuristic (rebalance only when position deviates from target by more than `trade_buffer`) and shows the zero-buffer-with-costs case degrading badly while the tuned buffer restores risk-adjusted returns. This is the empirical sibling of the Gârleanu-Pedersen result and of Novy-Marx & Velikov's buy/hold spread.
*Quality: practitioner with real data and open-source tooling (`rsims`), but a blog post — no full parameter disclosure, no significance testing, and the buffer is tuned in-sample on the same data. Rate: useful practitioner evidence, not a result.*

**The arithmetic applied to your own reported numbers** (this is my calculation from your CLAUDE.md figures, not from any source): the frozen V2 ZEC result is **−0.62% per trade over n=114** net of a **modelled 1.0% round trip**. That implies gross ≈ **+0.38% per trade**. At a realised 1.8% round trip the same trade sequence would be ≈ **−1.42% per trade**. To break even at 1.8% the mechanism would need to find **+1.8% gross per trade**, i.e. **4.7× the gross edge it actually produced**. If the Sept 2026 rates hold and the round trip becomes 1.4% (0.5% maker in, 0.9% taker out), the requirement is 3.7×; converting the exit to maker as well at 0.5% would make it 1.0% round trip and 2.6×. None of those is a small gap.

**Calibration against the only published crypto break-even numbers I found** — Hudson & Urquhart's Table 6 average break-even transaction costs, in basis points (with the % of rules in that class exceeding 50 bps):

| | MA | Filter | Support-res. | Oscillator | Channel breakout |
|---|---|---|---|---|---|
| CoinDesk BTC | 54.86 (32.0%) | 66.41 (32.4%) | 11.42 (0.0%) | 44.60 (36.1%) | 61.00 (30.2%) |
| Bitstamp BTC | 50.40 (22.8%) | 57.51 (28.2%) | 11.89 (1.0%) | 38.74 (25.2%) | 54.12 (27.4%) |
| Litecoin | 35.44 (25.8%) | 30.66 (19.7%) | 7.88 (0.2%) | 38.42 (24.4%) | 30.42 (18.0%) |
| Ripple | 36.01 (26.3%) | 33.59 (19.2%) | 16.16 (3.5%) | 52.14 (34.0%) | 33.06 (18.4%) |
| Ethereum | 78.60 (38.7%) | 147.56 (49.5%) | 9.89 (0.1%) | 50.05 (36.7%) | 144.03 (47.4%) |

The full range is **7.88 bps to 147.56 bps**. Trade counts behind these are 68–294 per rule over the sample. **Your 180 bps round trip exceeds every one of these 25 class averages**, including the maximum. The paper does not state unambiguously whether its break-even figure is per transaction or per round trip; on the generous reading (per one-way, so double it for a round trip) only 2 of 25 cells — Ethereum filter (295 bps) and Ethereum channel breakout (288 bps) — would clear 180 bps, and both are from the asset with the shortest sample ending at the 2017 peak. Their own cost benchmark, from Lintilhac & Tourin (2017), is **~50 bps for Bitcoin**; you are at 3.6× that.

**Holding-period interaction:** the mechanism is arithmetic, not empirical. Cost is per round trip and fixed; expected gross move scales with holding period roughly as σ√t. So the required *number* of trades falls and the required *per-trade* move is constant — meaning the only lever that changes the break-even is holding longer (larger expected move per round trip) or trading less. Bysik & Ślepaczuk is the cleanest demonstration: they did not improve the model at all, they cut turnover by 97%, and that alone moved XGBoost from −64% to +65%. I found **no crypto-specific empirical study of the optimal holding period as a function of retail fee level.**

---

## Q4 — Maker vs taker execution, and converting take-profit exits to post-only

**This is the weakest-evidenced of your six questions.** Almost everything published on maker-vs-taker execution is about *market making* — two-sided quoting where the whole P&L is the spread minus adverse selection. Your case is different in kind: a **directional exit** where the position already exists, the alternative to a resting sell is a guaranteed market sell, and the failure mode is not adverse selection on the fill but the unfilled tail (price approaches your TP, misses, reverses, and you exit at the stop instead). **I found no study, academic or practitioner, that measures this specific conversion in crypto with data.** Everything below is adjacent evidence.

**What is measured — Albers, Cucuringu, Howison & Shestopaloff, "The Market Maker's Dilemma: Navigating the Fill Probability vs. Post-Fill Returns Trade-Off," arXiv:2502.18625v2 (Nov 2025).** https://arxiv.org/html/2502.18625v2
This is a genuine **live** experiment (real orders, not simulation) on **Binance BTC USDT-margined perpetuals**, Feb 12–19 2024 (continuous quoting) and Aug 21–31 2024 (periodic quoting). Fee environment: **−0.5 bp maker rebate, 1.5 bp taker.** Measured:
- **Fill probability is highly state-dependent: ~30% when the near-side queue is large and the opposite side small, exceeding 90% in the reverse state.** Regression of fill probability on queue-imbalance state fits with R² = 0.946.
- **Fill probability and post-fill return are negatively correlated** — the orders that fill easily are the ones you least wanted filled.
- 1-second markouts: front-of-queue fills **−0.058 bp**; back-of-queue fills in the same state **−0.775 bp**; overall average across all orders **≈ −0.8 bp, or −0.3 bp net of the rebate.**
- Conclusion: naive market making loses because **adverse selection dominates the fee saving**.
*Quality: **high for what it is.** Live-traded experiment, Oxford/QMUL-affiliated authors, arXiv preprint (not yet peer-reviewed). **Weak read-across to you:** BTC perps on Binance at 2 bp fee granularity, quoting at top of book on second-scale horizons. Your TP sits multiple ATR away for hours-to-days at 60–120 bp fee granularity. The direction of the effect may transfer; none of the magnitudes do.*

**The mechanism in general form — DeLise, "The Negative Drift of a Limit Order Fill," arXiv:2407.16527 (Jan 2024).** https://arxiv.org/pdf/2407.16527
Proves in a discrete model, and confirms empirically on **10-Year US Treasury Bond futures (TY)**, that limit order fills coincide with adverse price movement: the measured mid-price drift conditional on a fill is **−0.45 ticks empirically vs −0.48 theoretically**, against a tick size of 1/64 — i.e. **roughly half a tick, closely matching the theoretical half-tick capture the maker was trying to earn.** The author's point is that standard market-making models assume low-cost random fills and reality delivers high-cost non-random fills.
*Quality: single-author arXiv preprint, not crypto, not peer-reviewed. Clean and internally consistent. Rate: moderate; useful as mechanism, not as a number.*

**Coinbase-specific operational facts (primary sources):**
- `post_only` **exists** on Advanced Trade but **only on `limit_limit_gtc` and `limit_limit_gtd`.** It is *not* available on `market_market_ioc`/`fok`, `limit_limit_fok`, `sor_limit_ioc`, `stop_limit_stop_limit_gtc`/`gtd`, `trigger_bracket_gtc`/`gtd`, `twap_limit_gtd`, or `scaled_limit_gtc`. Coinbase's description: "When enabled, only Maker Orders will be posted to the Order Book. Orders that will be posted as a Taker Order will be rejected." — https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/orders/create-order **This matters directly: you cannot get a post-only take-profit out of a bracket or stop-limit order type. A maker TP has to be a separately managed GTC/GTD limit.** *Quality: primary (Coinbase API reference).*
- Practitioner observation: Hummingbot's Coinbase connector notes **"LIMIT_MAKER: Coinbase rejection rate is high, currently same as LIMIT"** — they treat post-only as unusable on Coinbase and fall back to plain limit. https://hummingbot.org/exchanges/coinbase/ *Quality: practitioner assertion from a widely used open-source project; no supporting data, no date, may be stale. Worth verifying yourself rather than believing.*

**Spread context (does a maker exit cost you the spread?)** — Kaiko measured **BTC-USD spreads on Coinbase peaking at 1.7 bps** during the January stress window and **below 1 bps** normally (Bitstamp peaked at 6.7 bps over the same window). https://research.kaiko.com/insights/a-cheatsheet-for-bid-ask-spreads
*Quality: commercial data vendor with proprietary tick data; methodology partially disclosed. Good data, but **this is BTC. I found no comparable published spread series for ZEC-USD**, which will be materially wider, and your own 7R-2 quoted-book-impact percentiles are better evidence for your assets than anything public.* The relevant point: on a liquid pair the spread you cross is ~1–2 bps against a 60 bps fee difference between maker and taker — the fee, not the spread, is the entire decision.

**On non-fill rates for resting exits specifically:** the fill-probability literature (Lokin & Yu arXiv:2403.02572, which turned out to use FX spot not crypto; the Columbia deep-LOB work; survival-analysis approaches) measures **time-to-fill for orders at or near best quote on second-to-minute horizons**. None of it addresses a limit order resting several ATR away for hours. The generic backtesting critique — "limit orders should not be assumed to fill simply because the candle touched the level" — is repeated across practitioner writing and is correct, but it is **assertion, not measurement**, and I found no crypto study quantifying the bias.

---

## Q5 — Coinbase Advanced Trade fee tiers

**Primary-source problem, stated plainly: every `coinbase.com` and `help.coinbase.com` page I tried returned HTTP 403 to my fetcher** (`/advanced-fees`, `/blog/were-lowering-fees-for-many-active-traders-on-coinbase-advanced`, `/legal/trading_rules`, the help-centre fee-tier articles). I could not read Coinbase's published fee table directly. What follows is graded accordingly.

**Pre-2026-09-16 schedule** (secondary sources, but consistent with each other and with your own measured Intro 1 = 0.60/1.20):

| 30-day volume | Maker | Taker |
|---|---|---|
| < $1,000 (Intro 1) | 0.60% | 1.20% |
| ≥ $1,000 (Intro 2) | 0.35% | 0.75% |
| ≥ $10,000 (Advanced 1) | 0.25% | 0.40% |
| ≥ $50,000 | 0.15% | 0.25% |
| ≥ $500,000 | 0.10% | 0.20% |
| ≥ $1,000,000 | 0.07% | 0.16% |
| ≥ $15,000,000 | 0.05% | 0.14% |
| ≥ $100,000,000 | 0.00% | 0.08% |

Source: https://tokenecho.io/guides/coinbase-advanced-trade-fees/ (pub. 2026-04-04, updated 2026-04-07, cites the Coinbase fee schedule). *Quality: **SEO content site. Low.*** I include it only because its Intro 1 row matches your independently measured value, which is weak corroboration that the rest is transcribed rather than invented.

**Contradicting table, same period:** https://www.datawallet.com/crypto/coinbase-fees (updated 2026-08-20) gives **$0–$10K at 0.40%/0.60%**, which is irreconcilable with your measured 0.60/1.20. *Quality: **low; appears to be a stale Coinbase Pro-era schedule.*** **The lesson is the useful part: third-party Coinbase fee tables are unreliable and mutually contradictory. Your API reading is the only trustworthy source for your own account.**

**The 2026-09-16 change.** Coinbase announced a revised Advanced fee structure effective **September 16, 2026**:
- Entry-level spot rates now **regionalised**: **US 0.50% maker / 0.90% taker**; EU/UK 0.25% / 0.50%; Brazil, India and other international markets 0.09% / 0.10%.
- First volume tier threshold cut from **$25,000 to $10,000**.
- Spot and eligible derivatives volume now **consolidated into one tier ladder**.
- **USDC balance** can qualify you for a tier — whichever of trailing 30-day spot volume, eligible derivatives volume, or USDC balance is most favourable. Stated USDC thresholds: Advanced 1 $500K, Advanced 2 $1M, Advanced 3 $5M, VIP 1 $10M, VIP 2 $15M.
- Top tier VIP8: 0 bps maker / 2 bps taker.

Sources: https://www.investing.com/news/cryptocurrency-news/coinbase-cuts-trading-fees-for-active-users-on-advanced-platform-432SI-4904040 · https://www.securities.io/coinbase-lowers-advanced-trading-fees-with-tiers-starting-at-10-000/ · https://cryptodaily.co.uk/2026/09/coinbase-advanced-fees-usdc-vip-tiers
*Quality: **three independent outlets reporting the same Coinbase press release.** Consistent on every number I cross-checked. Still secondary. **None of them states explicitly what happens to Intro 1 and Intro 2** — whether 0.50/0.90 replaces the Intro tiers outright for US accounts, or whether Intro tiers survive below some threshold and 0.50/0.90 is the new Advanced 1. This ambiguity is material to you and I could not resolve it.*

**The reliable path, primary and specific to your account:** `GET /api/v3/brokerage/transaction_summary` returns `fee_tier` with `pricing_tier`, `maker_fee_rate`, `taker_fee_rate`, `aop_from`/`aop_to` (the asset-on-platform bounds that qualify a tier) and `volume_types_and_range`, plus **`fee_tier_without_promotion`, which reports your current tier, the next tier's threshold, and which dimension you qualify on.** https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/fees/get-transaction-summary *Quality: primary.* **This endpoint answers "what volume moves me off Intro" for your account directly, and it is authoritative in a way no web page is.** Note also that the docs confirm **Assets on Platform is a qualifying dimension alongside volume** — so a balance, not just turnover, can move the tier.

Also confirmed: **tier in force is determined at order placement time**, and rates recalculate hourly. That is consistent with the per-order (not per-position) fee boundary already recorded in your CLAUDE.md.

**What volume moves you off Intro at $2/trade:** at the pre-change schedule, Intro 2 required $1,000 of 30-day volume — **500 round trips per month** at $2 notional ($2 in + $2 out counts as $4, so ~250 round trips). Advanced 1 at $10,000 would be ~2,500 round trips/month. At one signal per asset per 60 minutes on one enabled asset, this is unreachable by orders of magnitude. **Under your current sizing the fee tier is not a variable you can move through trading.** The USDC/AOP route is the only other lever and its lowest rung is $500,000.

---

## Q6 — Minimum viable account size / notional

**There is no credible published evidence on this. I looked and found only SEO content.** Every result for "minimum account size for crypto algo trading" was affiliate-driven broker-comparison or bot-review content with no data behind it. No academic paper, no exchange study, no practitioner analysis with numbers. I am not going to manufacture a threshold.

What *is* establishable, from primary sources:

**Minimum-order constraints are not your binding constraint.** Coinbase's public products API for ZEC-USD returns:
`min_market_funds: "1"`, `quote_increment: "0.01"`, `base_increment: "0.00000001"`, `status: "online"`
https://api.exchange.coinbase.com/products/ZEC-USD *Quality: primary (Coinbase public API, no credentials).*
So a $2 order clears the $1 notional minimum with room. Price granularity of $0.01 on a ZEC price in the tens of dollars is ~1–3 bps — negligible against 60–120 bps fees. Base increment is 1e-8, so no rounding loss. Coinbase also **deprecated `base_min_size` in June 2022** and now enforces the notional minimum only, which removes the historical small-account trap where per-asset unit minimums forced oversized orders.

**The structural constraint is that Coinbase fees are purely proportional.** There is no per-order flat fee and no minimum commission on Advanced Trade (unlike equity brokers, where a flat $1 ticket charge makes small notional structurally dead). **A $2 trade and a $2,000,000 trade face the same percentage cost** — so the cost-per-trade argument against small accounts, as usually stated, does not actually apply here. What *does* bite is second-order and real:

1. **Tier progression is volume-gated**, so small notional locks you at the worst rate permanently — this is the genuine small-account penalty, and it is documented in the tier structure above rather than in any study.
2. **Fixed non-fee overheads** (infrastructure, API cost, your own time) are not proportional, and no source quantifies them for a retail crypto setup.
3. **Slippage is minimal at $2** — your notional is far below any depth threshold, so the market-impact term in every institutional cost model is ~0 for you. This cuts the other way: at your size the *only* cost is fees, which is the simplest possible case to reason about.

**No source I found establishes a dollar threshold below which retail crypto trading is structurally unviable.** The honest framing is that the question is mis-specified for a proportional-fee venue: viability depends on cost-per-round-trip relative to per-trade gross edge, which is size-invariant on Coinbase, not on account size as such.

---

## What the evidence does NOT establish

1. **No measured non-fill rate for a directional take-profit limit resting multiple ATR away over hours-to-days in crypto.** The entire fill-probability literature is top-of-book, second-to-minute, market-making. The maker/taker trade-off for a *directional exit* — as opposed to two-sided quoting — is essentially unstudied. If you convert your TP to post-only, you will be doing so on arithmetic and your own measurement, not on published evidence.
2. **No meta-study counting how much crypto strategy research applies realistic retail costs.** The pattern is visible paper-by-paper; the base rate is not documented.
3. **No published crypto strategy evaluation at anything close to 180 bps round trip.** The literature's cost assumptions top out around 100 bps round-trip spread or ~50 bps per trade. Every net-of-cost crypto result you could cite was computed in a cost regime cheaper than yours, which means none of them can be read across to your situation even directionally.
4. **No empirical calibration of holding period against retail fee level in crypto.** The σ√t argument is arithmetic. Nobody has measured what holding period a 1.8% round trip actually requires on ZEC or anything like it.
5. **No dollar threshold for minimum viable account size** from any source I would rate above "SEO content."
6. **Coinbase's own fee schedule is unverified by me.** All coinbase.com properties 403'd. The 2026-09-16 rates (US 0.50/0.90) rest on three outlets reporting one press release, and **what happens to Intro 1 and Intro 2 under the new structure is not stated in any of them.**
7. **Whether Coinbase post-only is practically usable is unresolved.** The API supports it on GTC/GTD limits only; Hummingbot's undated note says the rejection rate is high enough that they treat it as equivalent to a plain limit. No data either way.
8. **No published spread/depth series for ZEC-USD.** Kaiko's public numbers are BTC. Your own 7R-2 probe is better evidence than anything I could find.
9. **Adverse-selection magnitudes do not transfer.** The Albers et al. −0.8 bp average markout and DeLise's −0.45 tick are real measurements in venues and horizons unlike yours. The sign of the effect probably transfers; the size does not, and nothing lets you scale it.

## Arithmetic worth recording (my calculation, from your own reported figures — not from any source)

Frozen V2 ZEC: −0.62%/trade net of a modelled 1.0% round trip, n=114 ⇒ gross ≈ **+0.38%/trade**.

| Round trip | Net per trade | Gross needed to break even | Multiple of achieved gross |
|---|---|---|---|
| 1.0% (modelled, backtests) | −0.62% | 1.0% | 2.6× |
| 1.4% (0.5 maker + 0.9 taker, if Sept-2026 rates hold) | −1.02% | 1.4% | 3.7× |
| 1.8% (0.6 maker + 1.2 taker, as measured 2026-09-15) | **−1.42%** | 1.8% | **4.7×** |
| 1.2% (0.6 maker both legs, current tier) | −0.82% | 1.2% | 3.2× |
| 1.0% (0.5 maker both legs, if Sept-2026 rates hold) | −0.62% | 1.0% | 2.6× |

The last row is the ceiling on what execution changes alone can buy: **even a perfect maker-both-legs execution at the new US rates only restores the cost assumption your backtests already used — the one that produced PF 0.761.** Execution optimisation cannot close this gap; it can at best return you to the losing baseline you already measured.

## Sources

- [Bysik & Ślepaczuk (2026), ML-Based Bitcoin Trading Under Transaction Costs — arXiv](https://arxiv.org/html/2606.00060v1)
- [Bianchi & Babiak, A Factor Model for Cryptocurrency Returns (2022 draft)](http://wp.lancs.ac.uk/fofi2022/files/2022/08/FoFI-2022-056-Daniele-Bianchi.pdf)
- [Cryptocurrency anomalies and economic constraints, IRFA 94 (2024)](https://www.sciencedirect.com/science/article/abs/pii/S1057521924001509)
- [Hudson & Urquhart, Technical trading and cryptocurrencies, Ann. Oper. Res. 297 (2021)](https://centaur.reading.ac.uk/85715/8/Hudson-Urquhart2019_Article_TechnicalTradingAndCryptocurre.pdf)
- [Novy-Marx & Velikov, A Taxonomy of Anomalies and Their Trading Costs, RFS 29(1) 2016](https://www.nber.org/system/files/working_papers/w20721/w20721.pdf)
- [Gârleanu & Pedersen, Dynamic Trading with Predictable Returns and Transaction Costs, JF 68(6) 2013](https://nbgarleanu.github.io/DynTrad.pdf)
- [Albers, Cucuringu, Howison & Shestopaloff, The Market Maker's Dilemma — arXiv:2502.18625v2](https://arxiv.org/html/2502.18625v2)
- [DeLise, The Negative Drift of a Limit Order Fill — arXiv:2407.16527](https://arxiv.org/pdf/2407.16527)
- [Systematic Trend-Following with Adaptive Portfolio Construction — arXiv:2602.11708](https://arxiv.org/html/2602.11708v1)
- [Robot Wealth, managing turnover and costs](https://robotwealth.com/a-simple-effective-way-to-manage-turnover-and-not-get-killed-by-costs/)
- [Kaiko Research, A Cheatsheet for Bid Ask Spreads](https://research.kaiko.com/insights/a-cheatsheet-for-bid-ask-spreads)
- [Coinbase CDP: Create Order (post_only)](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/orders/create-order)
- [Coinbase CDP: Get Transaction Summary (fee tier)](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/fees/get-transaction-summary)
- [Coinbase public products API, ZEC-USD](https://api.exchange.coinbase.com/products/ZEC-USD)
- [Investing.com on the 2026-09-16 Coinbase fee change](https://www.investing.com/news/cryptocurrency-news/coinbase-cuts-trading-fees-for-active-users-on-advanced-platform-432SI-4904040)
- [Securities.io on the same](https://www.securities.io/coinbase-lowers-advanced-trading-fees-with-tiers-starting-at-10-000/)
- [CryptoDaily on the same](https://cryptodaily.co.uk/2026/09/coinbase-advanced-fees-usdc-vip-tiers)
- [Hummingbot Coinbase connector notes](https://hummingbot.org/exchanges/coinbase/)
- [TokenEcho Coinbase fee tier table (low quality, for contrast)](https://tokenecho.io/guides/coinbase-advanced-trade-fees/)
- [Datawallet Coinbase fee table (contradictory, low quality)](https://www.datawallet.com/crypto/coinbase-fees)
