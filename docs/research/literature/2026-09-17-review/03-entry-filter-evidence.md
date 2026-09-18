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

# Literature review: entry-filter evidence for a 4h crypto momentum strategy

Scope: the seven questions posed. Method: WebSearch plus full-text extraction (pdftotext) of 15 primary PDFs; where only an abstract or a search snippet was reachable (SSRN, Springer, ScienceDirect, Medium all returned 403), the number is flagged "snippet-only". Quality scale: **A** = peer-reviewed major journal or heavily cited methodological paper; **B** = peer-reviewed lesser journal or well-documented working paper; **C** = practitioner analysis with disclosed method and data; **D** = abstract/snippet-only, blog, or unverifiable.

Nothing below is a recommendation to add or remove a filter. It reports what is documented, with emphasis on where a filter's apparent value is a selection/overfitting artifact.

---

## Q1. Long-term trend filters (200-day MA/EMA) as a regime gate

**What the evidence says**

1. **Detzel, Liu, Strauss, Zhou, Zhu — "Bitcoin: Learning, Predictability, and Profitability via Technical Analysis" (WP Apr 2018; published *Financial Management* 2021).** BTC 2010-10-27 → 2018-01-31. Price/MA ratios for L = 5, 10, 20, 50, 100 days predict 7-day-ahead returns in- and out-of-sample; MEAN-combination OOS R² = 1.17–3.66% (weekly), three-pass filter 0.91–2.46%. MA timing strategies (long BTC above MA, else T-bills) raise Sharpe from 1.9 (buy-and-hold) to 2.1–2.5; max drawdown 89.5% → 64.3–70.3%; second-half sample Sharpe 1.5 → up to 2.1, DD 73.4% → 33.5%. Authors state the outperformance "largely stems from the MA strategy having less severe and much shorter drawdowns" — i.e. it is exposure reduction during crashes, not higher returns when invested. Also holds for ETH and XRP. **No 200-day lookback was tested; the longest was 100.** Sample is bull-dominated and ends before the 2018 crash. Quality **A**. https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3115846 (full text read from mirror: https://community.portfolio123.com/uploads/short-url/jS0qs8gndLATbD3WlYQ79thDnzD.pdf)

2. **Grobys, Ahmed, Sapkota — "Technical trading rules in the cryptocurrency market", *Finance Research Letters* 32 (2020).** 11 most-traded coins, daily, 2016–2018, variable-MA (1, L) rules. Joint SUR tests: only (1,20), (1,50), (1,100) jointly significant. Ex-BTC excess return over buy-and-hold: (1,20) = 8.76% p.a.; (1,50) = 3.65% p.a.; (1,100) profitable only for ETH. Verbatim: "Applying longer time horizons did not generate profits in any of the cryptocurrencies" — the **(1,150) and (1,200) rules had no significant payoff for any altcoin** (t-stats 0.04–1.64; NMC negative). BTC individually significant for all L. Authors conclude cryptos are "rather short-memory processes". Three-year sample, one cycle. Quality **B**. https://www.sciencedirect.com/science/article/pii/S1544612319308852 (full text: https://osuva.uwasa.fi/server/api/core/bitstreams/6d20e4ef-7741-419e-b2ea-0cb103bfa1dd/content)

3. **Hudson & Urquhart — "Technical trading and cryptocurrencies", *Annals of Operations Research* 297 (2021).** ~15,000 rules across five families on two BTC markets + three other coins; multiple-hypothesis-corrected in-sample profitability; break-even costs above typical crypto costs. But: "**there is no predictability for Bitcoin in the out-of-sample period**, although predictability remains in other cryptocurrency markets." Quality **A**. https://ideas.repec.org/a/spr/annopr/v297y2021i1d10.1007_s10479-019-03357-1.html

4. **Zarattini, Pagani, Barbon — "Catching Crypto Trends" (SSRN 5209907, Apr 2025).** BTC 2015-01-01 → 2025-03-19, long-only Donchian breakouts, 25% vol target, no costs in Table 1. Sharpe by lookback: 5d 1.66, 10d 1.55, 20d 1.60, 30d 1.61, 60d 1.30, 90d 1.20, **150d 0.99, 250d 1.13, 360d 1.28**; Combo 1.58, MDD 19% vs >80% passive. Trade counts (Table 2): 150d = 15 trades, 250d = 9, 360d = 5 in ten years — the long-lookback statistics rest on 5–15 trades. Per-coin Combo (10 bps costs): BTC 1.56, ETH 1.51, SOL 1.68, **ZEC 0.57 (CAGR 8%, MDD 22%)**, BCH 0.48, BSV 0.21 — a 0.2–1.7 Sharpe dispersion across coins under one identical rule. Diversified top-20 rotational program: Sharpe 1.57, CAGR 18%, MDD 11%, alpha 10.8% vs BTC, **no BTC regime filter used**. Quality **B** (working paper; practitioner authors; survivorship-free dataset). https://concretumgroup.com/wp-content/uploads/2026/02/Catching-Crypto-Trends.pdf

5. **Padysak & Vojtko (SSRN 4081000, 2022) and Beluská & Vojtko (SSRN 4955617, 2024), Quantpedia.** BTC Nov 2015 → Feb 2022, then extended to Aug 2024 as a genuine OOS check. Lookbacks 10/20/30/40/50-day max (trend) and min (mean reversion); **shortest (10-day) best**; combined strategy 98.4% ann., vol 47.8%, MDD −37.7% in-sample. Extension Feb 2022–Aug 2024: "performance of both strategies is slightly less effective", MIN strategy "low or even negative returns". Quality **C** (practitioner research, SSRN, not peer-reviewed; the 2024 extension is an honest OOS test). https://quantpedia.com/revisiting-trend-following-and-mean-reversion-strategies-in-bitcoin/

6. **Grayscale, "The Trend is Your Friend"** (snippet-only): 20d/100d crossover from 2012: 116% ann. / Sharpe 1.7 vs buy-and-hold 110% / 1.3. Quality **D** (vendor, unverified, no cost or sensitivity data retrievable). https://research.grayscale.com/reports/the-trend-is-your-friend-managing-bitcoins-volatility-with-momentum-signals

**Lookback sensitivity / arbitrariness**

- **Levine & Pedersen, "Which Trend Is Your Friend?", *FAJ* 72(3) 2016** — time-series momentum and MA crossovers are "equivalent representations"; any linear filter (HP, Kalman) is a weighting function on past returns. Choosing 50 vs 100 vs 200 is choosing a weighting shape, not a separate hypothesis. Quality **A**. https://www.aqr.com/Insights/Research/Journal-Article/Which-Trend-Is-Your-Friend
- **Zakamulin, "Market Timing with Moving Averages: Anatomy and Performance" (SSRN 2585056)** — same equivalence; tests 300 weighting shapes OOS on four indices; no single lookback is optimal. **Zakamulin, *J. Asset Management* 15 (2014)**: OOS with costs, 1930–2012 equities — reported MA/momentum timing performance "highly overstated, to say the least"; secondary summaries (Swedroe, ETF.com) cite ~80% failure to beat buy-and-hold OOS while cutting vol ~30% (snippet-level, not verified). Quality **A/B**. https://econpapers.repec.org/RePEc:pal:assmgt:v:15:y:2014:i:4:d:10.1057_jam.2014.25
- **Newfound (Hoffstein), "Timing Trend Model Specification with Momentum" (Dec 2019)** — 1,023 specifications, lookbacks 20–360 days across three model types: "little evidence of meaningful persistence in the returns of different model specifications"; 4 of 5 specification-timing models underperform naive equal-weighting; a lone 200-day SMA shows "massive dispersion" across slightly perturbed histories. Quality **C**. https://blog.thinknewfound.com/2019/12/timing-trend-model-specification-with-momentum/
- **Sullivan, Timmermann, White, *J. Finance* 54(5) 1999** — 7,846 rules on DJIA 1897–1996. Best rule 1897–1986 (50-day VMA, 0.01 band) is Reality-Check significant in-sample (p < 0.002) but OOS 1987–1996 the best-rule p-value is **0.421 (BLL universe) / 0.908 (full universe)** while the *nominal* p-values were 0.204 / 0.042 — ignoring the search makes an OOS failure look like a 5% success. Best OOS rule was structurally different (200-day VMA in the BLL universe; a filter rule in the full universe). Quality **A**. https://www.kevinsheppard.com/files/teaching/mfe/advanced-econometrics/Sullivan_Timmermann_White.pdf
- setup4alpha "20 regime filters" (SPY/QQQ/BTC, 2000–2026, MAs 100–300): "the 'best' setting from a backtest does not repeat in the future"; several filters "charge 2–4 annual percentage points" for protection obtainable cheaper. Quality **D** (paywalled blog; top-10 results not visible).

**Altcoin-specific:** only Grobys (2016–2018: 150/200-day rules unprofitable on all 10 alts) and Zarattini (per-coin Combo Sharpe 0.2–1.7, ZEC 0.57) address altcoins directly. Detzel's ETH/XRP result uses L ≤ 100. No paper tests a daily 200-EMA gate layered on a 4h entry.

**Net:** Peer-reviewed support exists for *short* (5–100-day) BTC MA rules on 2010–2018 data, with the benefit coming from drawdown avoidance. The 200-day specifically has the weakest documented support of the lookbacks tested (Grobys: nil on alts; Zarattini: lowest Sharpe band, 5–15 trades/decade). Two A-grade papers (STW; Hudson–Urquhart) show the best in-sample trend rule failing OOS, and two A/B-grade papers show lookback choice is a weighting-shape choice with no persistent winner.

---

## Q2. BTC as regime indicator for altcoins; BTC–alt correlation stability

1. **Li, Zhou, Huang, Xie, Huang — "Bitcoin ETFs and structural decoupling…", *Cogent Economics & Finance* 14(1) 2026.** BTC vs 18 major alts, daily, 2021-01-01 → 2025-09-03, LSTM rolling R² (a fit metric, not Pearson ρ). Long-term (12-mo) R²: **0.7161 (2021–22) → peak 0.8851 (2022–23) → 0.5131 (2023–24) → 0.1876 after 2024-01-10 ETF approval → −0.047 (Jan–Sep 2025)**. Short-term (6-mo): 0.7757 (Jan–Jul 2023) → 0.3633 (Jul 2023–Jan 2024) → 0.1614 (Jan–Jul 2024) → **−0.037 (Jan–Jun 2025)**. Authors: "correlation-based assumptions calibrated on pre-ETF data" no longer hold. Caveats: single nonlinear method, 91 views, the 2025 near-zero values are more extreme than typical rolling-Pearson practitioner charts. Quality **B**. https://researchonline.lse.ac.uk/id/eprint/137306/1/Bitcoin_ETFs_and_structural_decoupling_in_the_cryptocurrency_market_evidence_from_altcoin_correlation_dynamics.pdf

2. **Wu — "Institutional Adoption and Correlation Dynamics" (arXiv 2501.09911, 2025)** — BTC vs equities (not alts), but shows how unstable annual-window correlations are: BTC–VOO 0.13 (2018), 0.55 (2019), 0.77 (2020), **0.28 (2021), 0.87 (2022)**, 0.73 (2023), 0.76 (2024). Regime-dependent: high in risk-on, drops in deleveraging. Quality **C/D** (single-author arXiv). https://arxiv.org/pdf/2501.09911

3. **Ozaydin — "Bitcoin's lagged effect on altcoins", *PressAcademia Procedia* 14 (2021)** — daily, Feb 2018 → Oct 2021, VAR on ETH/BNB/ADA/XRP: BTC lags 1,3,4 significant for ETH; 1,2 for BNB; 1,2,4 for ADA; **none for XRP**; Granger causality bidirectional. No R², no OOS, no costs, 4 pages. Quality **D**. https://dergipark.org.tr/en/download/article-file/2206815

4. **"Price Transmission from Bitcoin to Altcoins: High-Frequency Evidence…", *Asia-Pacific Financial Markets* (2026)** — abstract only: small-cap/illiquid alts respond to BTC with a lag; BTC leads price discovery; a lag-trading strategy "consistently outperforms buy-and-hold". No magnitudes or costs retrievable. Quality **D** for our purposes. https://link.springer.com/article/10.1007/s10690-026-09589-z

5. **"Cross-cryptocurrency return predictability", *J. Economic Dynamics & Control* 163 (2024)** — Binance data; lagged returns of other coins predict a focal coin (adaptive LASSO / PCA); long-short portfolio profitable OOS after costs; mechanism = slow diffusion under limited attention. This is cross-sectional lead-lag, not "block entries when BTC is bearish". Quality **A**. https://ideas.repec.org/a/eee/dyncon/v163y2024ics0165188924000551.html

6. **Liu, Tsyvinski, Wu — "Common Risk Factors in Cryptocurrency", *J. Finance* 77(2) 2022** — a crypto market factor plus size and momentum span the cross-section. Establishes that alt returns load on a BTC-dominated market factor; it does not establish that gating on BTC regime improves an alt strategy. Quality **A**. https://www.nber.org/papers/w25882

7. Zarattini et al. (Q1 item 4): the top-20 altcoin trend program delivers Sharpe 1.57 and 10.8% alpha vs BTC **without any BTC filter** (beta to BTC 0.08). Not a test of adding one, but evidence that a BTC gate is not necessary for that mechanism in that sample.

**Net:** BTC→alt lead-lag at daily/hourly horizons is documented (strongest for small caps; OOS-profitable only in cross-sectional long-short form, JEDC 2024). **No study tests an alt-entry veto based on BTC 4h regime and a rolling-correlation threshold, in- or out-of-sample.** Correlation is documented to swing from R² ≈ 0.89 to ≈ 0 within 24 months, and annual BTC–equity ρ from 0.28 to 0.87 in adjacent years. Thresholds of 0.35/0.65 therefore sit inside the range of ordinary regime variation: the fraction of time such a veto binds is itself non-stationary, so its historical contribution cannot be attributed to a stable effect.

---

## Q3. Perpetual funding rate as a contrarian/crowding signal for spot entries

1. **Presto Research, "Can Funding Rate Predict Price Change?"** — Binance BTC perp, early 2021 → early 2024, weekly. Contemporaneous Δfunding vs Δprice: **R² = 12.5%, p = 1.9e-115**. Forward (funding at T vs price T+1): **R² ≈ 0, insignificant**. Conclusion: funding tracks price concurrently, "cannot reliably predict subsequent price changes" for a single asset. Quality **C**. https://www.prestolabs.io/research/can-funding-rate-predict-price-change

2. **Fulgur Ventures, "Bitcoin funding rates and price predictability"** (snippet-only, Medium 403): OLS of BTC returns in the 8h after BitMEX funding publication: **β = −0.087, p = 0.008, R² = 0.003**. Statistically nonzero, economically negligible. Quality **D**. https://medium.com/@fulgur.ventures/bitcoin-funding-rates-and-price-predictability-27ce95535af1

3. **"Failure of Cross-Sectional Alpha Screening on Cryptocurrency Perpetual Futures…" (SSRN 6701738, 2026)** (snippet-only): daily/intraday OHLCV + funding, Jul 2022 → Apr 2026: funding-rate signals contain **no exploitable cross-sectional alpha for large-cap perps at an 8-hour horizon**. Quality **D** (preprint, unread). https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6701738

4. **Schmeling, Schrimpf, Todorov — "Crypto Carry", BIS WP 1087 (rev. Oct 2025)** — futures carry up to 60% p.a., strongly time-varying, not explained by fundamentals; carry-strategy profits driven by funding; crash risk. If elevated funding reliably predicted spot declines at hour-to-day horizons, the long-spot/short-perp carry trade would embed that; the paper documents carry as a compensated risk premium with crashes, not as a return-predictor. Abstract-level only. Quality **A**. https://www.bis.org/publ/work1087.pdf

5. **arXiv 1912.03270 (BitMEX funding correlation)** — funding is heteroskedastic; Granger tests; no magnitudes in abstract. Quality **D**. **Inan, SSRN 5576424** — next-period *funding* is predictable (DAR beats no-change); says nothing about returns. Quality **D**.

**Net:** The only quantified spot/perp return-prediction results are R² = 0.003 at 8 hours and R² ≈ 0 at one week forward. At R² = 0.003 the conditional expected 8h return given extreme funding is on the order of a few basis points against an 8h return SD of roughly 1–2% — below the 0.6%/1.2% maker/taker fees in `pipeline/fees.py`. No study tests a "~20% annualized" threshold or entry blocking on spot. Arithmetic note (inference, not a source): 20% annualized ≈ 0.018% per 8h, a level routinely exceeded during trending advances, so such a veto is "on" precisely in the phases a breakout-long strategy targets.

---

## Q4. Velocity veto (no buy after −5%/24h) and bounce confirmation (+1.5 ATR)

**Direction of short-horizon returns for large caps**

- **Zaremba et al., "Up or down? Short-term reversal, momentum, and liquidity effects…", *IRFA* 2021** — >3,600 coins: daily reversal exists but is "cross-sectionally dependent on liquidity"; the **largest, most tradeable coins exhibit daily momentum, not reversal**. Quality **A**. https://www.sciencedirect.com/science/article/pii/S1057521921002349
- **Fičura, "Impact of size and volume on cryptocurrency momentum and reversal", FFA WP 3/2023** — weekly, 2017-06 → 2022-12, split at $50M cap / $5M weekly volume: 1-week reversal only in small/illiquid (t = −7.31); **large/liquid coins show weekly momentum (t = 2.33)**; distance from 1-week high predicts *positively* for large/liquid (t = 4.93). Quality **B**. https://wp.ffu.vse.cz/pdfs/wps/2023/01/03.pdf
- **Dobrynskaya, "Cryptocurrency Momentum and Reversal" (HSE, 2014–2020, ~2,000 coins)** — momentum up to 2 weeks, insignificant 2–4 weeks, reversal beyond ~1 month. Quality **B**. https://conference.hse.ru/files/download_file_ex?hash=FAE0AB2DC7A67656E89A0B1CB27D8C7D&id=3B5EE9A5-0B18-458A-9458-B4ED0F6C6664
- **Caporale & Plastun, "Momentum effects… after one-day abnormal returns", *FMPM* 34 (2020); WP Brunel 1917** — BTC/ETH/LTC hourly, 2017-01-01 → 2019-09-01. After negative overreaction days: continuation next morning for BTC (till ~11:00) and LTC (till ~10:00); **contrarian for ETH**. Companion paper "Price overreactions in the cryptocurrency market", *J. Economic Studies* 46(5) 2019 (BTC/LTC/XRP/DASH): "a strategy based on counter-movements after overreactions is **not profitable**", the inertia strategy's results are "not statistically different from the random ones". Quality **B**. https://www.brunel.ac.uk/economics-finance-and-accounting/research/pdf/1917-Oct-GMC-Momentum-effect-in-the-Cryptocurrency-Market-after-One-day-Abnormal-Returns.pdf ; https://ideas.repec.org/p/diw/diwwpp/dp1718.html
- **"Short-horizon mean reversion in cryptocurrency markets" (arXiv 2608.21888, 2026)** — 15-min reversal in 90% of 183 Binance pairs (AUC 0.531 vs 0.499 equities), decays to no-skill within hours, gross edge ≈ 1.3 bp vs 5 bp round-trip cost: "too small to capture at benchmark spot costs". Frozen 6-month holdout Feb–Aug 2026 gap +0.020. Quality **B**. https://arxiv.org/html/2608.21888v1

**Cost of confirmation / delayed entry**

- **STW 1999** included "time delay filters" (signal must persist d days) and "band filters" (b%) in all MA and support/resistance rules; among 7,846 rules the best rules in every subperiod were a plain 50-day VMA with a 0.01 band, a 5-day MA, a 2-day OBV, or a 0.12 filter rule — **no time-delay variant was ever the best rule**, and the banded best rule failed OOS (Q1). Quality **A**.
- **Zarattini, Pagani, Wilcox — "Does Trend Following Still Work on Stocks?" (2025)**, 66,000+ trades 1950–2024: **56% of trades lose, ~37% break even, <7% of trades generate all cumulative profit**; avg win 1.90R vs avg loss −0.70R; win rate 43.9%; holds OOS 2005–2024 with "a modest decline in average trade profitability". Entry is an unfiltered all-time-high breakout. Implication for any confirmation rule: its value depends entirely on whether it retains the <7% outliers; a rule that raises the entry by 1.5 ATR when the stop is 2 ATR gives up ~0.75R of every trade's potential by construction (arithmetic, not a source finding). Quality **B**. https://concretumgroup.com/wp-content/uploads/2026/02/Does-Trend-Following-Still-Work-on-Stocks.pdf
- **Newfound "Decomposing Trend Equity" / "Tightening the Uncertain Payout…"** — faster signals = more whipsaw; slower/confirmed signals = "less average exposure" and reduced participation; the trade-off is documented, not resolved in favor of confirmation. Quality **C**.

**Net:** For large-cap coins the documented daily/weekly effect is *continuation*, so "do not buy the day after a −5% move" is directionally consistent with the literature, and the bounce (counter-movement) trade after overreaction is documented as unprofitable. But **no study measures either veto's effect on a breakout strategy's expectancy**, and the two A/B-grade sources on entry mechanics (STW; Concretum) point the other way for confirmation delays: delay variants never won, and profit is concentrated in a handful of outliers that a later entry dilutes.

---

## Q5. Whipsaw guards / consecutive-loss lockouts

- **Carver, "Random data: Evaluating 'Trading the equity curve'" (Nov 2015)** — AR(1) synthetic P&L across Sharpe −1…+2, skew −2…+1, autocorrelation −0.3…+0.3; MA overlays 10–512 days that switch the system off below the MA. For profitable systems the overlay **reduced returns at every Sharpe level**, with only modest DD reduction at short lookbacks; benefit exists only when P&L is positively autocorrelated, and "trend following systems seem to have negative autocorrelation", making the overlay counterproductive. Costs make short lookbacks worse. Quality **B/C** (ex-AHL, method published, simulation). https://qoppac.blogspot.com/2015/11/random-data-evaluating-trading-equity.html
- **KJ Trading, "Equity Curve Trading Myths"** — three live/OOS systems (GC daily, ES 1-min, JY 360-min), 450 trades each, six rule variants each: improvements **0/6, 3/6, 0/6**; "relatively easy to take a good strategy, and significantly degrade its performance." Quality **C**. https://kjtradingsystems.com/equity-curve-trading.html
- **Alvarez, "Trading the Equity Curve"** — ConnorsRSI on S&P 500 stocks 2001–2017: equity-below-MA200 filter **−29% CAR with worse DD**; ROC252>0 −15% CAR, similar DD; "all the stats got worse or stayed about the same." Quality **C**. https://alvarezquanttrading.com/blog/trading-the-equity-curve/
- **No academic study** of "pause after N stop-outs within T hours" was found. Practitioner sources (edgeflo, LuxAlgo, etc.) state the rule; the only non-behavioural argument offered is that it "helps when losing streaks come from regime mismatch [and] hurts strategies whose losses arrive randomly." Quality **D**.

**Net:** Every documented test of stopping after adverse P&L runs shows neutral-to-negative expectancy effects for trend/momentum systems; the sign of the effect is governed by the autocorrelation of trade outcomes, which for trend systems is documented as negative (Carver). The behavioural-comfort rationale is the only one with consistent support. A 2-stops-in-96h rule also acts as a churn/cost control, but that is an expected-cost argument whose size depends on the (unmeasured) clustering of stop-outs in the specific data.

---

## Q6. Filter stacking: sample size, power, overfitting

- **Novy-Marx, "Backtesting Strategies Based on Multiple Signals", NBER WP 21329 (2015)** — combining purely random signals, each signed to look good in-sample: strategies "usually backtest, in real data, with t-statistics in excess of five, and statistical significance at the 5% level requires t-statistics in excess of seven." 5% critical t: **best 3-of-10 ≈ 4; best 3-of-20 ≈ 5; best 7-of-100 ≈ 7**; selecting the best k of n candidate signals "yields a bias almost as large as… selecting the single best of n^k candidate signals." Two signals with t = 1.5 or five with t = 1 combine to a nominally "significant" strategy. Quality **A**. https://www.nber.org/system/files/working_papers/w21329/w21329.pdf
- **Bailey & López de Prado, "The Deflated Sharpe Ratio", *JPM* 2014** — E[max SR] under N trials with zero true SR: (1−γ)Φ⁻¹(1−1/N) + γΦ⁻¹(1−1/(Ne)). Worked example: a Sharpe 2.5 found over a 5-year daily sample among many configurations is **not significant at 95% unless N ≤ 46 trials** (≤ 88 if returns were Normal). Hold-out ignores N. Quality **A**. https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf
- **Bailey, Borwein, López de Prado, Zhu, "The Probability of Backtest Overfitting" (2015)** — hold-out "unreliable and inaccurate" because it disregards the number of trials; CSCV estimates PBO; introduces minimum backtest length as a function of N. Quality **A**. https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf
- **Harvey, Liu, Zhu, "…and the Cross-Section of Expected Returns", *RFS* 29(1) 2016** — hundreds of published factors; a new one needs **t > 3.0**, not 2.0. **Harvey & Liu, "Backtesting", *JPM* 42(1) 2015** — haircut Sharpe is nonlinear in N; a flat 50% haircut is "a serious mistake." Quality **A**. https://people.duke.edu/~charvey/Research/Published_Papers/P118_and_the_cross.PDF ; https://people.duke.edu/~charvey/backtesting/
- **Suhonen, Lennkh, Perez, "Quantifying Backtest Overfitting in Alternative Beta Strategies", *JPM* 43(2) 2017** — 215 commercial strategies, five asset classes: **median 73% Sharpe deterioration live vs backtest**; most complex strategies deteriorate >30 percentage points more than the simplest. Quality **A**. https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2757113
- **STW 1999** (Q1): the nominal OOS p-value of the best rule was 0.042; the search-adjusted p-value was 0.908.
- **"30 trades per parameter"** and similar heuristics appear only in blogs (edgeflo, Trading Dude, Backtrex) with no primary source; treat as folklore. Quality **D**.

**Applied to a ~100–114-trade sample (inference from the sources above, not a source result):** the six filters carry roughly 9–10 tunable thresholds (0.65 / 0.35 / 20% / 1.5 ATR / 5% / 24h / 200 vs 50 / 2 stops / 96h) on top of stop, target and min-conditions — order of 13 free parameters, i.e. under 9 trades per parameter. Under Novy-Marx, keeping 6 filters out of even 12 tried is bias-equivalent to picking the best single rule from ~3 million candidates; under DSR, N in the dozens already pushes E[max SR] past 1 on a 5-year daily sample. No documented rule gives "how many filters n trades can support"; the documented answer is that the question must be posed as a trial count N and a deflated statistic, and that hold-out alone cannot answer it.

---

## Q7. Documented cases where removing filters improved OOS performance

No source was found in the form "published crypto strategy → filters removed → OOS improved." The closest documented cases:

1. **STW 1999 (A):** the banded 50-day VMA, best of 7,846 over 90 years, fails OOS (p 0.421/0.908); the OOS-best rule is a different, un-banded structure.
2. **Hudson & Urquhart 2021 (A):** BTC technical-rule predictability vanishes OOS despite multiple-hypothesis-adjusted in-sample significance.
3. **Carver 2015 (B/C):** removing an equity-curve overlay raises returns for every profitable synthetic system tested.
4. **Alvarez (C):** removing the equity-below-MA200 gate is worth +29% CAR relative; ROC252 gate +15%.
5. **KJ Trading (C):** overlay removal improves 15 of 18 configurations.
6. **Suhonen et al. 2017 (A):** simpler strategies decay >30 pp less than complex ones live.
7. **Zakamulin 2014 (A/B):** MA/momentum timing rules' reported edge largely disappears OOS with costs.
8. **Concretum 2025 (B):** an entirely unfiltered all-time-high breakout with an ATR trailing stop survives 2005–2024 OOS with only "a modest decline in average trade profitability."
9. **setup4alpha "4 rules vs 12 rules" (D):** S&P 500 mean reversion, IS 2000–2018 / OOS 2019–Apr 2026: Sharpe 0.69 → 1.18 (simple) vs 0.93 → 0.50 (complex); CAGR 7.18% → 11.78% vs 8.62% → 4.51%. Blog, unaudited, example may be selected.
10. **Your own trial `2026-08-warmup-semantics.v1`** is a documented case in the opposite framing: enforcing the daily-EMA gate removed 19 trades worth +22.28%, PF 0.855 → 0.761, `bull_2021` n=25/PF 1.42 → n=6/PF 0.96.
11. Baseline decay for any published predictor: **McLean & Pontiff, *J. Finance* 71(1) 2016** — WP version (2012, 82 characteristics) 10% OOS / 35% post-publication decay; published version reports 26% / 58% on 97 predictors (published figures from secondary summaries; WP figures verified in full text). Quality **A**. https://www.hec.ca/finance/Fichier/McLean.pdf

---

## Summary table

| Filter type | Direction of documented evidence | Strength | Applicable to altcoins? |
|---|---|---|---|
| 200-day MA/EMA regime gate | Short MAs (5–100d) improve BTC Sharpe by cutting drawdowns (Detzel, A); 150/200-day rules unprofitable on 10 alts 2016–18 (Grobys, B); long lookbacks lowest Sharpe with 5–15 trades/decade (Zarattini, B); best in-sample rule fails OOS (STW, A; Hudson–Urquhart, A) | Mixed, leaning negative for the 200-day specifically; benefit is exposure reduction | Weakly; only two direct alt studies, both showing weaker/nil effect for long lookbacks |
| Lookback choice (50/100/200) | Equivalent weighting shapes (Levine–Pedersen, A; Zakamulin, A/B); no persistent best specification (Newfound, C); best setting does not repeat (STW, A) | Strong that choice is arbitrary | Yes (structural result) |
| BTC regime + rolling-correlation veto | BTC lead-lag exists for small caps/slow diffusion (JEDC 2024, A; APFM 2026, D); correlation swings 0.89 → ~0 in 24 months (Cogent 2026, B); no test of the veto itself | None for the veto; strong that thresholds are non-stationary | Only as cross-sectional long-short, not as an entry gate |
| Funding-rate crowding veto (spot) | β −0.087, R² 0.003 at 8h (D); forward weekly R² ≈ 0 (Presto, C); no cross-sectional alpha at 8h (SSRN 2026, D); funding is a carry premium with crash risk (BIS, A) | Weak-to-nil; effect size below fees | Untested on alts as an entry gate |
| Velocity veto (−5%/24h) | Large caps show daily/weekly continuation, not reversal (Zaremba, A; Fičura, B); counter-movement after overreaction unprofitable (Caporale–Plastun, B) | Directionally consistent; no test of the veto on breakout expectancy | Yes for large/liquid coins; reverses for small/illiquid |
| Bounce confirmation (+1.5 ATR) | Time-delay/band variants never the best rule and banded rule fails OOS (STW, A); <7% of trades carry all profit so later entries dilute outliers (Concretum, B) | Moderate, negative | Structural, applies to any breakout system |
| Whipsaw guard (2 stops / 96h) | Every documented equity-curve/lockout test neutral-to-negative for trend systems (Carver, B/C; KJ, C; Alvarez, C); no academic test of N-stop lockouts | Moderate, negative; behavioural rationale only | Untested on crypto |
| Filter stacking in general | Critical t of 4–7 for best k-of-n (Novy-Marx, A); N ≤ 46 trials for SR 2.5/5 yrs (DSR, A); median 73% live decay, complexity penalty >30 pp (Suhonen, A) | Strong that stacking inflates in-sample statistics | Yes (structural) |

---

## What the evidence does NOT establish

1. **No study tests any of the six filters in the stacked configuration used here** (4h breakout + daily EMA + BTC regime/correlation + funding + velocity + bounce + whipsaw). Every result above is about a filter in isolation, usually as a stand-alone timing rule on daily data, not as a gate on a 4h entry.
2. **It does not establish that the 200-day EMA is worse than a 50-day EMA for ZEC or ETH.** It establishes that lookback choice is a weighting-shape choice with no persistent winner, and that the longest lookbacks had the weakest support in the two direct altcoin tests.
3. **It does not establish that BTC regime has zero information for altcoin entries** — lead-lag is documented — only that no out-of-sample test of a regime/correlation *gate* exists and that the correlation thresholds are non-stationary.
4. **It does not establish that funding has zero predictive content** — the 8h β is nonzero — only that the documented effect size is orders of magnitude below your fee schedule and vanishes by one week.
5. **It does not establish that the velocity veto hurts**; the direction of the documented short-horizon effect for large caps (continuation) is consistent with it. It does establish that no one has measured its effect on a breakout strategy.
6. **It does not establish that bounce confirmation or whipsaw guards hurt in your data**; it establishes that in every documented test of analogous rules the effect was neutral-to-negative and that the only supported rationale is behavioural.
7. **It does not provide a "maximum number of filters for n = 100 trades."** The documented answer is that this is a trial-count question (N, not n) requiring a deflated/haircut statistic, and that hold-out validation cannot substitute for it.
8. **Access caveats:** Fulgur (β −0.087), SSRN 6701738, Grayscale, and the Springer APFM 2026 paper were read only as abstracts/snippets; the published McLean–Pontiff decay figures (26%/58%) are from secondary summaries while the WP figures (10%/35%) were verified; the Cogent 2026 R² values are LSTM fit metrics, not Pearson correlations.
9. **The PF 0.761 / n = 114 result in your registry makes most of the above secondary:** the filter-stacking literature explains how a mechanism with no edge can look like one in-sample, not how to make one that has none acquire it.
