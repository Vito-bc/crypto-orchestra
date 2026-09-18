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

# Literature review: short-horizon crypto momentum/breakout, ATR exits, and backtest artifacts

**Scope note up front:** I searched for academic and credible practitioner work on all six questions. The single most important structural finding is that **the specific mechanism class you run — single-asset, 4-hour-timeframe breakout with multi-condition confirmation and ATR-scaled stop/target — has essentially no peer-reviewed literature at all.** The academic crypto momentum corpus is almost entirely *cross-sectional* (long-short portfolios across dozens-to-thousands of coins, weekly rebalance) or *time-series momentum on daily/intraday BTC*. Nobody publishes on 4h ATR-bracket breakouts on a single altcoin. Everything below is therefore adjacent evidence, and I flag where the mapping is loose.

Source-quality scale used throughout:
- **A** — peer-reviewed in a recognised finance journal, primary text obtained
- **B** — working paper / preprint from identifiable academic authors, primary text obtained
- **C** — credible practitioner research with disclosed method and data
- **D** — secondhand (abstract or search summary only; primary blocked)
- **E** — SEO/content marketing; cited only to demonstrate that this is all that exists

---

## Q1. Published evidence on intraday and short-horizon (4h–daily) crypto momentum/breakout

### 1a. The cross-sectional crypto momentum literature is wildly inconsistent

Reported weekly long-short momentum payoffs, same broad asset class, overlapping periods:

| Study | Sample | Universe | Reported payoff | Costs? |
|---|---|---|---|---|
| Liu, Tsyvinski & Wu (2020) | 2015–2018 | 78 coins | **36% / week** | No |
| Liu, Tsyvinski & Wu (2022) | 2014–2020 | 1,827 coins | **3% / week** | No |
| Zhang & Li (2020) | 2014–2019 | 500 coins | **12% / week** | No |
| Liu et al. (alt. spec., via Grobys) | — | — | 2.50% / week | No |
| Liebi (2022) | — | — | 0.98% / week (t=0.72) | No |
| Grobys & Sapkota (2019) | 2014–2018 | 143 coins, monthly | **no significant payoff** | — |
| Ammann, Burdorf, Liebi & Stöckl (2022), survivorship-free | 2014–2021, 417 wks | 3,904 coins | **0.13% / week (t = 0.13)** | No |
| Grobys, Sandretto & Äijö (2026), survivor coins | 2017–2024, 398 wks | 9 coins | **0.36% / week (t = 0.75)** | No |

A spread from 36%/week to 0.13%/week *for the same nominal anomaly* is the headline fact. The dispersion is driven almost entirely by universe construction, not by the signal.

Sources: Ammann et al., *Survivorship and Delisting Bias in Cryptocurrency Markets*, Nov 2022 working paper, Univ. St. Gallen / Univ. Liechtenstein — https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4287573 (full text: https://www.alexandria.unisg.ch/bitstreams/2bc8397d-47dd-4f66-8467-9004b2c9d212/download) — **B, primary text extracted**. Grobys, Sandretto & Äijö, *On survivor cryptocurrency momentum*, Finance Research Letters vol. 92 (2026) — https://www.sciencedirect.com/science/article/pii/S1544612326001339 (open copy: https://iris.unito.it/retrieve/f41aebe6-ee86-4e5e-b065-e85c3e38cb3b/) — **A, primary text extracted**. The earlier figures are as tabulated inside those two papers' literature reviews.

### 1b. The one survey that reports *no* decay

Borri, Liu, Tsyvinski & Wu, *Cryptocurrency as an Investable Asset Class: Coming of Age* (Oct 2025), arXiv:2510.14435 — **B**. https://arxiv.org/html/2510.14435v2
- Crypto-momentum (CMOM) long-short: **2.6%/week, t = 3.89**, 2014–2025 full sample.
- Post-2020 subsample: **2.1%/week, t = 3.70** — explicitly robust.
- Size (CSIZE): −2.3%/week full sample → **−0.9%/week post-2020**, significance weakened.
- Crypto arbitrage (CARB): 68.5%/week apparent across all exchanges → **3.8%/week when restricted to liquid, actually-implementable pairs** (post-2020: 95% → 4.1%). Their own words: "where the strategy is actually implementable, the apparent 'free lunch' largely vanishes."
- **No transaction-cost model is applied to CMOM.** These are gross numbers.

This paper is the strongest pro-momentum evidence in crypto and it directly contradicts Ammann et al. and Grobys et al. on the same question. Note the authors overlap with the studies producing the largest reported payoffs.

### 1c. Short-horizon / hourly evidence, with costs

**Bysik & Ślepaczuk (Univ. of Warsaw), *Machine Learning-Based Bitcoin Trading Under Transaction Costs: Evidence From Walk-Forward Forecasting*, May 2026, arXiv:2606.00060 — B.** https://arxiv.org/html/2606.00060v1
Dec 2017 – Jan 2026, ~70,000 hourly BTC/USDT bars, walk-forward:

| Model (long-only) | Gross ann. return | Gross Sharpe | At **10 bps** cost | Sharpe at 10 bps |
|---|---|---|---|---|
| XGBoost | +73.50% | 1.27 | **−64.00%** | **−1.25** |
| LSTM | +72.43% | 1.45 | **−50.65%** | **−1.16** |
| iTransformer | +129.07% | 2.59 | **−83.93%** | **−1.82** |

A **10 basis point** round-trip assumption flips Sharpe 2.59 to −1.82. With a cost-aware execution filter (trade only when forecast magnitude exceeds cost) XGBoost recovers to +65.4% / Sharpe 1.09 on 251 trades — but the authors state bootstrap tests show **no statistically significant outperformance versus buy-and-hold**. This is the cleanest published demonstration that hourly crypto "edges" are cost artifacts.

For calibration: 10 bps is 0.10%. Your frozen research model is 0.4%/0.6% and your prospective measured tier is 0.6%/1.2% — i.e. **10× to 24× the cost level that already destroyed every strategy in that paper.**

**Mesfin, *Structural Limits of OHLCV-Based Intraday Momentum Signals in MNQ Futures: A Systematic Falsification Study*, arXiv:2605.04004 (May 2026, v3 Sept 2026) — B.** https://arxiv.org/abs/2605.04004
Not crypto (Micro E-mini Nasdaq futures), but the closest published analogue to what you are doing methodologically: 14 OHLCV-derived intraday signal families, 947 trading days of 5-min bars (2021–2025), expanding-window walk-forward, five pre-registered acceptance criteria (OOS net t ≥ 2.0; ≥30 trades/fold; positive after realistic execution cost; consistent sign across 2023/2024/2025; permutation p < 0.001).
- **0 of 14 families passed.**
- 11 families: gross return before friction 0.07–1.50 points against a **2.0-point friction floor** — i.e. the raw signal was smaller than the cost.
- The one large gross result (+16.53 points, gap-continuation short) failed year-stability.
- Positive controls did pass (RTH confluence t=3.11, N=196; London session B t=4.30, N=247, p=0.000025), so the test had power.

**Reality check under data-snooping controls.** *A reality check on trading rule performance in the cryptocurrency market: Machine learning vs. technical analysis*, Finance Research Letters (2020), doi 10.1016/j.frl.2020.101655 — **D (abstract only; ScienceDirect paywalled).** https://www.sciencedirect.com/science/article/abs/pii/S1544612320304414
Applies White's Reality Check and Hansen's SPA. Finding: after controlling for data snooping and market frictions, **statistically significant positive excess returns are rarely achieved, independent of sampling frequency, position type, or significance level.** Cross-sectional trading-rule performance correlates with beta and idiosyncratic volatility, i.e. the rules mostly harvest market risk premium rather than alpha.

### 1d. The positive practitioner results — and what they actually are

**Zarattini, Pagani & Barbon, *Catching Crypto Trends: A Tactical Approach for Bitcoin and Altcoins*, SSRN 5209907 (Apr 2025) — C.** https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5209907
Ensemble of Donchian-channel breakout models across multiple lookbacks, volatility-based sizing, rotational portfolio of the **top 20 most liquid coins**, survivorship-bias-free dataset since 2015. Reported: **CAGR ~30%, Sharpe 1.58, Sortino 2.03, alpha +10.8% to +14% vs BTC, net of 0.10–0.50% fees.** (Numbers via the Concretum summary page and secondary replication write-ups — I could not pull the SSRN PDF; treat the exact figures as **D**.)
Critically: this is a **20-asset diversified portfolio with vol targeting**, not a single-asset bracket trade. It says nothing about what one coin does standalone.

**Rzym & Abou Zeid (Man Group), *In Crypto We Trend* — C.** https://www.man.com/insights/in-crypto-we-trend
Moving-average crossover and breakout models on BTC + altcoins, data through Dec 2024. Findings:
- "**The Sharpe ratio of an individual market is positive but small**" — the returns come from cross-sectional diversification, not from any one coin.
- Portfolio Sharpe peaks at **10–15 coins** for crossover models, **~10 coins** for breakout models; beyond that transaction costs and slippage outweigh added diversification.
- The authors volunteer that their measured Sharpes are "higher than expected... probably a function of the short history."

This is the most directly relevant credible finding to a single-asset system: the people who run crypto trend at institutional scale say **the per-market Sharpe is small and the edge is a diversification effect.**

**Begušić & Kostanjčar, *Momentum and liquidity in cryptocurrencies*, arXiv:1904.00890 (Apr 2019) — B, primary text extracted.** https://arxiv.org/pdf/1904.00890
Jan 2015 – Jan 2019 only (4 years, entirely pre-regime-change). 14-day momentum, bivariate momentum×liquidity sorts. Finds momentum **concentrated in highly liquid coins**, plus an illiquidity premium, and states the long-only strategies improve risk-adjusted performance over cap-weighted "even when transaction costs are included." Cost level is not stated numerically in the text I extracted. Short sample; superseded by the survivorship work.

**Bitcoin intraday time-series momentum** — Shen et al., *Financial Review* (2022), https://onlinelibrary.wiley.com/doi/abs/10.1111/fire.12290 — **D (both the Reading repository PDF and the publisher are access-blocked; figures below are secondhand from search summaries and should not be relied on without verification).** Reported: first-half-hour return predicts last-half-hour return; certainty-equivalent return **5.95% p.a.** over the random-walk benchmark; buy-and-hold baseline Sharpe 0.53. Claims profitability survives fees "especially for leveraged investors" — a qualifier that should be read as a warning.

### Q1 verdict
Where costs are modelled at realistic levels, short-horizon crypto momentum/breakout results collapse. The surviving positive results are (a) gross-of-cost cross-sectional portfolios over large coin universes, or (b) diversified 10–20 coin trend portfolios with volatility sizing. **There is no published, cost-inclusive, out-of-sample evidence that a single-asset short-horizon breakout system has positive expectancy.** Evidence on 4h specifically: none academic. The only 4h-timeframe material I could find is content marketing (**E**): financefeeds.com, keenbase-trading.com, coinquant.ai, mudrex.com. None discloses a reproducible method.

---

## Q2. Documented decay over time

**This is the weakest-evidenced of your six questions, and the honest answer is that the literature does not cleanly establish crypto trend/breakout decay from 2017–2021 to 2022–2026.** What exists:

**Evidence *against* decay:**
- Borri/Liu/Tsyvinski/Wu (above): CMOM 2.6%/wk (t=3.89) full sample vs **2.1%/wk (t=3.70) post-2020**. Explicitly robust. **B**
- Quantpedia, *Revisiting Trend-following and Mean-reversion Strategies in Bitcoin*, published **12 Sept 2024** — **C**. https://quantpedia.com/revisiting-trend-following-and-mean-reversion-strategies-in-bitcoin/ Original study Nov 2015–Feb 2022; genuine out-of-sample window **4 Feb 2022 – 20 Aug 2024**, which covers a full bear leg. Result: the trend-following (MAX) rule "remains alive and well" out-of-sample; the mean-reversion (MIN) rule delivered "low or even negative returns." **Weakness: the article reports no CAGR, Sharpe or drawdown figures for either window** — it is charts and prose only. So the direction is documented, the magnitude is not.

**Evidence *for* decay:**
- **Size factor** in crypto: −2.3%/wk full sample → **−0.9%/wk post-2020**, significance weakened (Borri et al.). **B**
- **Crypto factor instability**: *Crypto factor zoo (.Zip)*, Journal of Intl. Financial Markets (2026) — **D (paywalled)**. https://www.sciencedirect.com/science/article/abs/pii/S1057521926000645 Reports that **only one factor (bid-ask spread) survives in both halves of the sample** for value-weighted portfolios; factor rankings shift substantially between periods. Reads as: crypto factor premia are period-specific, not stable risk compensation.
- **Calendar anomalies have vanished.** Day-of-week effects replicate for BTC but **not for ETH or ADA**; weekend-vs-weekday return gaps are absent in the full sample and in every subsample (2016–2019, 2020–2023, early 2024) despite lower weekend volatility and volume. Sources: https://jfin-swufe.springeropen.com/articles/10.1186/s40854-023-00499-x (**A**); https://mlquants.substack.com/p/are-day-of-the-week-effects-in-cryptocurrencies (**C**).
- **Parameter-optimised MA strategies**: Chen & co., *A Data Science Pipeline for Algorithmic Trading* (arXiv:2206.14932) — **B**. Walk-forward strategies beat buy-and-hold across all tested cryptocurrencies in 2020 but **underperformed buy-and-hold in 2021**; authors state the reason "remains unclear," speculating that parameters optimal in 2018–2019 stopped providing an edge.
- **Actual fund returns.** Crypto Fund Research index, data through April 2026 — **C**. https://cryptofundresearch.com/crypto-hedge-fund-performance/

| Year | CFR index | BTC |
|---|---|---|
| 2017 | +1,708.7% | +1,318.0% |
| 2018 | −71.8% | −72.6% |
| 2019 | +37.1% | +92.2% |
| 2020 | +168.4% | +303.2% |
| 2021 | +119.7% | +57.6% |
| 2022 | −53.5% | −64.2% |
| 2025 | −10.3% | −6.3% |

2025: average fund **−7.2%**, median **−5.2%**, only **37% of 84 reporting funds positive** — first negative year since 2022. The provider itself flags survivorship in its own index: **409 closed funds** have dropped out, and "the actual experience of a randomly selected crypto fund investor would likely be worse than the index suggests, particularly during 2018 and 2022."

**General (non-crypto) decay baseline:** McLean & Pontiff — anomaly returns average **−26% out-of-sample** and **−58% post-publication**. Widely replicated; the standard prior for any published edge.

### Q2 verdict
Documented decay in crypto is clearest for **mean-reversion, size, calendar anomalies, and parameter-optimised MA rules**. For **trend/breakout specifically, the direct dated evidence is thin and points weakly the other way** (Quantpedia's 2022–2024 OOS test, Borri et al.'s post-2020 subsample). A 2021-bull-window edge collapsing is therefore *not* corroborated by a literature-wide finding that crypto trend stopped working after 2021; it is more consistent with the period-selection and bias literature in Q6.

---

## Q3. ATR-based stops/targets and fixed reward:risk

### 3a. The analytical result is unambiguous and negative

**Acar & Toffel, *Stop-loss and Investment Returns*, Investment Conference, Faculty & Institute of Actuaries, June 2000 — B, primary text extracted.** https://www.actuaries.org.uk/system/files/documents/pdf/stop-loss-and-investment-returns.pdf

Their conclusion, verbatim: *"Under the random walk assumption, stop-loss and take profits strategies are a cost for doing business. Such rules cannot beat buy and hold and can only be justified by specific risk preferences."* And: *"under the random walk hypothesis, the optimal strategy is buy and hold. As a consequence, path-dependent strategies such as the stop-loss rule, can only diminish earnings. In this situation, stop-losses are like premiums on an insurance policy."*

Quantified (asset mean 10%, stop at 5%):

| Asset volatility | P(hit stop) | Expected return |
|---|---|---|
| 5% | 1.68% | 8.98% |
| 20% | 69.29% | 6.83% |

**The cost of a stop scales with volatility/drift.** Crypto has an extreme volatility-to-drift ratio, so under a random walk this is where brackets cost the most.

Empirical, S&P 500 / T-Bonds / Yen futures intraday, 26 Dec 1984 – 12 May 2000, stops and targets placed at 1 daily σ:
- S&P 500 take-profit at 1.10%: realised **6.30% annualised vs 8.95% expected**.
- T-Bonds stop-loss at 0.60%: realised **7.07% vs 4.41% expected** — better than expected, which they read as evidence of genuine trend.
- T-Bonds take-profit at 0.60%: realised **−1.70% vs +0.62% expected**.
- Their reading of the target: *"take-profits are counter-productive since they are not triggered often enough and do not prevent very large losses which actually occur ten times more often than anticipated."*

They also show a conditional stop-loss **is** a trend-following rule. Under AR(1) with no drift, E(Sc) = ασ√(2/π) > 0. With α=0.05, µ=10%/yr, σ=10%/yr, E(Sc) = 6.82% annualised — still below buy-and-hold. **Stops add value only in the presence of genuine short-horizon positive autocorrelation, and even then not automatically enough to beat the drift.**

### 3b. The peer-reviewed confirmation

**Kaminski & Lo, *When Do Stop-Loss Rules Stop Losses?*, Journal of Financial Markets (Mar 2014) — A (via abstract/DSpace; primary blocked, so figures are D).** https://dspace.mit.edu/entities/publication/bb69ca4b-0cdc-487f-831d-63b2e84fafee
- **Under the Random Walk Hypothesis, simple 0/1 stop-loss rules *always decrease* a strategy's expected return.**
- Under momentum or regime-switching dynamics, they can add value.
- Empirically, US futures, monthly data Jan 1950 – Dec 2004: certain stop-loss policies added **50–100 bps/month** during stop-out periods.

### 3c. Fixed R:R specifically

The mathematically exact statement is the **optional stopping theorem**: for a martingale with bounded increments and finite expected stopping time, the expected value at any stopping time equals the initial value. A stop/target pair is a stopping rule. **Under a martingale, moving the bracket cannot change expectancy — it only moves probability mass between hit rate and payoff size.** Under a *positive-drift* process it is strictly worse than holding (Acar & Toffel above). Under positive autocorrelation it can help. Reference: https://en.wikipedia.org/wiki/Optional_stopping_theorem (**A** for the theorem; it is textbook mathematics, not an empirical claim).

I could find **no academic study supporting any particular fixed R:R value**, and nothing supporting 1.75 or any neighbouring value. Everything returned by search on "risk:reward ratio" was content marketing (**E**: luxalgo.com, tradingview idea posts, traderssecondbrain.com, journalplus.co, chartmini.com). Several of those blogs state the correct thing — that R:R redistributes win-rate against payoff and preserves expected value — but none is a source you can cite.

### 3d. Evidence on ATR specifically

**There is no peer-reviewed evidence that ATR-scaled stop/target levels outperform fixed-percentage levels.** What exists:
- CTA practice uses ATR to **normalise position size and express P&L in comparable units across markets** (Man Group; general CTA description). That is a *risk-normalisation* claim, not an expectancy claim, and it is the claim the practitioner literature actually supports.
- *Optimal Stop-Loss and Take-Profit Parameterization for Autonomous Trading Agent Swarm*, Li, Laryea & Ihlamur, arXiv:2604.27150 (29 Apr 2026) — **B but very weak**. https://arxiv.org/abs/2604.27150 900+ historical trades replayed under alternative exit policies; concludes "exit design matters meaningfully," favouring tighter loss limits, earlier profit capture, closer trailing stops. **Red flags: 4 pages; chronological data splitting "produced distorted results due to unusual market conditions," so the authors reran the main analysis on randomized splits.** Choosing a validation scheme because the chronological one gave the wrong answer is exactly the failure mode in Q6. I would not rely on this.
- *Stop-loss rules and momentum payoffs in cryptocurrencies*, Journal of Behavioral and Experimental Finance (2023) — **D (paywalled)**. https://www.sciencedirect.com/science/article/abs/pii/S2214635023000473 Claims stop-loss momentum gives "exceedingly higher returns, Sharpe ratio, and alphas" than benchmark crypto momentum. Unverified; note it is built on the same cross-sectional crypto momentum base that Ammann et al. and Grobys et al. show to be a survivorship artifact.
- The one quantitative "ATR stops beat fixed stops by ~5%" figure that search surfaces is from a dev.to blog post (**E**). Unusable.

### Q3 verdict
The evidence supports the redistribution reading, not the edge reading. Under a martingale, bracket geometry is expectancy-neutral by theorem. Under positive drift it is a cost. It can add value only under genuine short-horizon momentum, and Kaminski & Lo's positive empirical result is for **monthly** futures data over 55 years, not 4h crypto. **A fixed take-profit is the more clearly documented cost of the two legs** — Acar & Toffel find targets underperform their own theoretical expectation on two of three markets, because they truncate the right tail without truncating the left.

---

## Q4. Altcoins vs BTC, and whether your cross-asset ordering means anything

### 4a. The literature points the *opposite* way, and contradicts itself

- Begušić & Kostanjčar (2019): momentum is **concentrated in highly liquid cryptocurrencies**; "the handful of largest and most tradeable coins exhibit daily momentum rather than a reversal." **B**
- Ammann et al. (2022): "We document **momentum (reversal) within large (small) cryptocurrencies**." Their Figure 7 shows the momentum premium is **largest in the top-100 survivors (2.51%/wk)**, falls to 0.78% across all survivors, and to **0.13% (t=0.13) across the full survivorship-free universe.** **B**
- Grobys, Sandretto & Äijö (2026): but the **nine largest, most stable survivor coins show no momentum at all** — 0.36%/week, t = 0.75. So "large coins have momentum" fails too. **A**
- Wen/Bouri et al. and the reversal literature: small coins show **short-term reversal**, not momentum, over 3,600+ coins 2015–2021. https://www.sciencedirect.com/science/article/pii/S1057521921002349 — **D**

There is no stable, replicated cross-asset ordering in the literature. "BTC worst, small alt least bad" is **not** a documented pattern; if anything the documented pattern (momentum in liquid/large, reversal in small) runs the other way.

### 4b. Your four-asset ordering, quantified

Taking your PFs and n's at face value and modelling each trade as a two-point outcome (+1.75R at target, −1R at stop — the geometry implied by a 2.0×/3.5× ATR bracket):

| Asset | PF | n | Implied win rate | Expectancy | Per-trade SD | t | p |
|---|---|---|---|---|---|---|---|
| BTC | 0.359 | 174 | 17.0% | **−0.532R** | 1.034 | **−6.79** | <0.001 |
| ETH | 0.476 | 150 | 21.4% | **−0.412R** | 1.128 | **−4.47** | <0.001 |
| SOL | 0.718 | 97 | 29.1% | −0.200R | 1.249 | −1.58 | 0.115 |
| ZEC | 0.761 | 114 | 30.3% | −0.167R | 1.264 | −1.41 | 0.159 |

Pairwise differences in per-trade expectancy (independence assumed, which is generous — these assets are highly correlated and share a BTC-regime filter, so true SEs are larger):

| Pair | Δ expectancy | SE | t | p |
|---|---|---|---|---|
| BTC − ZEC | −0.365R | 0.142 | −2.57 | 0.010 |
| BTC − SOL | −0.332R | 0.149 | −2.23 | 0.026 |
| ETH − ZEC | −0.245R | 0.150 | −1.64 | 0.102 |
| ETH − SOL | −0.212R | 0.157 | −1.35 | 0.176 |
| BTC − ETH | −0.120R | 0.121 | −0.99 | 0.321 |
| SOL − ZEC | −0.033R | 0.173 | −0.19 | 0.847 |

Six pairwise tests → Bonferroni threshold p < 0.0083 (|t| > 2.64). **No pair clears it.** The BTC-vs-ZEC gap is the closest and still fails, and it fails on the optimistic independence assumption.

**Caveat on my model:** I assumed every trade closes at exactly +1.75R or −1R. Real exits include time stops, gaps through the stop, and partial fills, all of which increase per-trade variance. Higher variance means **smaller |t| and wider intervals than shown** — my numbers are the best case for resolving the ordering.

### Q4 verdict
Two things are simultaneously true and should not be merged. **The BTC and ETH losses are real** — t = −6.79 and −4.47 are not noise; that mechanism loses money on those assets with high confidence. **The ordering among the four is noise** — no pairwise comparison survives multiple-comparison correction, and SOL vs ZEC is a coin flip (t = −0.19). "ZEC is the least bad" is not an established fact about ZEC; it is the top of a four-item ranking where the ranking itself is unresolved, and it is a ranking of degrees of losing.

---

## Q5. What profit factor would actually constitute evidence

### 5a. The academic literature does not use profit factor

Not once in anything I read. Finance uses t-statistics, Sharpe ratios, and their multiple-testing-adjusted versions. Every source returned for "what profit factor is good" was SEO content (**E**: tradezella.com, quantvps.com, edgeflo.com, blodsalgo.com, journalplus.co, traderssecondbrain.com). Their claimed benchmarks — "1.3–2.0 for professional systematic traders," "1.5–2.5 for swing traders," "400 trades for reliability" — have **no sourcing whatsoever** and should be treated as invented. I will not repeat them as evidence.

The defensible route is to translate PF into a t-statistic. Using the same two-point model:

**Where your ZEC number sits:**
- PF 0.761, n=114 → **t = −1.41, p = 0.159**. The *loss* is not statistically established either. This is a sample too small to establish anything in either direction.
- 95% CI on the win rate (≈35/114) is [0.222, 0.392], which maps to a **95% CI on PF of roughly [0.50, 1.13]** — the interval still contains break-even.

**What it would take to clear a bar:**

| Bar | n = 114 | n = 200 | n = 400 | n = 1,000 | n = 2,000 |
|---|---|---|---|---|---|
| t = +2.0 (single pre-registered test) | **PF 1.47** (45.7% wins) | PF 1.34 | PF 1.23 | PF 1.14 | PF 1.10 |
| t = +3.0 (multiple-testing adjusted) | **PF 1.78** (50.4% wins) | PF 1.55 | PF 1.36 | PF 1.22 | PF 1.15 |

The t = 3.0 row is calibrated to **Harvey, Liu & Zhu, "… and the Cross-Section of Expected Returns," Review of Financial Studies 29(1), 2016 — A.** https://academic.oup.com/rfs/article/29/1/5/1843824 Their finding: given the volume of factor mining in finance, a newly proposed factor must clear **t > 3.0**, and "most claimed research findings in financial economics are likely false" under conventional thresholds. A repository with a declared multiple-testing budget across trials is in exactly the regime that motivates this.

**Statistical power — trades required to detect a true edge at 80% power, α=0.05:**

| True PF | Implied win rate | Expectancy | Trades needed |
|---|---|---|---|
| 1.1 | 38.6% | +0.061R | **≈ 3,727** |
| 1.2 | 40.7% | +0.119R | **≈ 1,016** |
| 1.3 | 42.6% | +0.172R | **≈ 489** |
| 1.5 | 46.2% | +0.269R | **≈ 203** |
| 2.0 | 53.3% | +0.467R | **≈ 68** |

At roughly 114 trades per ~5 years on one asset (~23/year), **detecting a true PF of 1.2 would take on the order of 44 years of trading at that rate.** That is the binding constraint, independent of whether any edge exists.

Complementary methods, both **A**: Bailey & López de Prado, *The Deflated Sharpe Ratio* (SSRN 2460551, https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551), which haircuts a reported Sharpe by the number of trials attempted plus skew and kurtosis; and Bailey, Borwein, López de Prado & Zhu, *The Probability of Backtest Overfitting* (SSRN 2326253, https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253), which formalises **minimum backtest length** — the sample needed before a reported Sharpe is not expected to be pure selection.

### 5b. Base rates for retail-accessible systems

- **Man Group (C):** per-market crypto trend Sharpe is "**positive but small**"; portfolio Sharpe peaks at 10–15 coins and falls beyond that as costs bite. Institutional crypto trend is a diversification product, not a per-asset edge.
- **Crypto hedge funds (C):** 2025 average **−7.2%**, median **−5.2%**, **37% positive**, against BTC −6.3%; 409 funds have closed out of the database.
- **Retail day trading (A/D):** Chague, De-Losso & Giovannetti's Brazilian futures study — 19,000+ retail day traders, **fewer than 3% consistently profitable over a 300-day horizon**; among those who persisted ≥300 days, **97% lost money**. Barber, Lee, Liu & Odean's Taiwan work and the *Financial Analysts Journal* (2003) day-trader study find roughly twice as many losers as winners, with ~20% more than marginally profitable. (Primary texts paywalled; figures via secondary summaries — **D**.)

### Q5 verdict
A PF of 1.47 at n=114 would be the minimum for a conventional single-test t=2, and 1.78 for the multiple-testing-adjusted t=3. A PF in the 1.1–1.2 range — the realistic level for a cost-laden retail trend system — is **statistically indistinguishable from noise at any sample size you can reach on one asset in a human timeframe.** The question "what PF is evidence of an edge" has no answer independent of n and of how many variants were tried.

---

## Q6. Documented cases of crypto strategies that turned out to be artifacts

This is the best-evidenced of the six questions. The error class you hit is common enough that 2026 saw multiple papers proposing formal machinery to catch it.

### 6a. Survivorship / delisting — the largest documented crypto artifact

**Ammann, Burdorf, Liebi & Stöckl (2022) — B, primary text extracted.**
3,904 cryptocurrencies, 7 Jan 2014 – 28 Dec 2021, 417 weeks.
- Annualized survivorship/delisting bias: **0.93% (value-weighted), 62.19% (equal-weighted)**. Weekly: 0.018% / 0.934%.
- **Size premium overstated by 50.3%** by survivor conditioning (2.84%/wk vs the true 1.89%/wk).
- **Momentum HML, full survivorship-free universe: 0.13%/week, t = 0.13.** Survivors-only: 0.78%. Top-100 survivors: **2.51%**. Their Figure 7 shows the premium sliding continuously from 2.51% to 0.13% as you widen the universe from 100 coins to 3,904.
- Equal-weighted HML across all coins: **−11.46%/week, t = −32.91**.
- Their conclusion: "no momentum effect in cryptocurrency markets," in direct contradiction of Liu et al. (2020, 2022) and Zhang & Li (2020).
- Context: **over 14,000 of ~24,000 tokens ever listed on CoinMarketCap are classified dead (>58% attrition).** In traditional equities, ignoring delistings inflates returns by only 1–3%/year.

**Grobys, Sandretto & Äijö (2026) — A, primary text extracted.** Their bottom line: *"The cryptocurrency momentum effect appears to be driven by digital coins that temporarily gain popularity and become actively [traded]"* — i.e. **the anomaly is an artifact of coins that are only temporarily accessible for trading.** Robustness: they shift the survivor-identification start date ±1 year (7 survivors from Dec 2015, 18 from Dec 2017) and drop Dogecoin; results unchanged.

### 6b. Ex-post trimming presented as a result

Same paper. The top-30 plain momentum portfolio yields 0.56%/week, not significant. **After trimming 5% of the return distribution ex post, it becomes 0.93%/week, t = 2.62, significant at 1%.** The survivor-coin portfolio stays at −0.10%/week (t = −0.33) under the same trimming. The authors are honest about it: *"Since the trimming procedure is applied ex post, the resulting payoffs may not be feasible from an investment perspective and are intended as only a diagnostic."* This is a published anomaly that exists only conditional on removing the trades that killed it. Supporting statistics: max drawdown **−211.74%** for the top-30 portfolio, **−56.28%** for survivors; kurtosis **80.67** and **27.46**.

### 6c. Reported Sharpe ratios that do not mathematically exist

**Grobys & Shahzad, *Cryptocurrency Momentum: Is It an Illusion?*, International Journal of Finance & Economics 31(2), 2026, pp. 2180–2193 — A (abstract/RePEc; publisher and SSRN both 403'd, so figures are D).** https://ideas.repec.org/a/wly/ijfiec/v31y2026i2p2180-2193.html · https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4633099
Headline: crypto momentum long-short generates ~**3% excess weekly return**. But the realized variance is governed by a power law and **they cannot reject the infinite-theoretical-variance hypothesis**, which implies **"t-statistics or Sharpe ratios do not exist for this strategy and the premium is not observable in reality."** Also documents cross-sectional dependence among tail risks of momentum strategies at different formation periods. If the second moment is undefined, every Sharpe and t-stat in the crypto momentum literature is reporting a statistic that has no population value.

### 6d. Cost omission — the one that flips signs

Bysik & Ślepaczuk (Q1c): **Sharpe 2.59 → −1.82 from a 10 bps cost assumption.** This is the class of error that produces the largest and most reliable reversal, and it is the easiest to introduce silently because a cost parameter defaulting to zero looks like nothing in a diff.

### 6e. Data snooping / multiple testing

- **Finance Research Letters (2020) reality check** (Q1c): after White's Reality Check and Hansen's SPA plus frictions, significant excess returns in crypto are "rarely achieved." The paper's own framing is that the crypto market is **more efficient than previously recognised**. **D**
- **Sullivan, Timmermann & White, "Data-Snooping, Technical Trading Rule Performance, and the Bootstrap," Journal of Finance 54(5), 1999 — A.** https://onlinelibrary.wiley.com/doi/10.1111/0022-1082.00163 The founding result for this whole class.
- **US futures reality check**: best technical trading rules generate significant economic profits for only **2 of 17 contracts** after data-snooping correction. **D**
- **Harvey, Liu & Zhu (2016) — A** (Q5a): t > 3.0.
- **Mesfin (2026) — B** (Q1c): 0/14 signal families passed pre-registered criteria, with working positive controls.

### 6f. Period selection and calendar artifacts

Day-of-week effects in crypto: present for BTC, absent for ETH and ADA (**A**, https://jfin-swufe.springeropen.com/articles/10.1186/s40854-023-00499-x). No weekend/weekday return gap in the full sample or in any of 2016–2019, 2020–2023, early 2024 (**C**). A practitioner analysis notes the "Monday effect" claims are plausibly **an artifact of time-zone-dependent daily bar boundaries in a 24/7 market** — a pure bar-construction artifact, structurally similar to a warm-up handling bug.

### 6g. Look-ahead specifically — an active 2026 research front

That three separate 2026 papers are building formal machinery for this is itself the evidence that it is endemic and **not reliably caught by peer review**:
- *When Alpha Disappears: A One-Switch Benchmark for Decision-Time Leakage in Financial Backtests*, Zhang, Li, Peng & Chen, arXiv:2605.23959 (26 May 2026) — **B (metadata; PDF text layer unextractable, so no numbers)**. Isolates a single leakage source and toggles it to measure its effect in isolation.
- *Look-Ahead-Freedom as Temporal Non-Interference: A Verifiable Correctness Property for Backtesting and Agentic Trading Pipelines*, arXiv:2607.04958 — **B**. Proposes look-ahead freedom as a **formally verifiable program property**, which is the right framing for a repo that binds provenance to source hashes.
- *Look-Ahead-Bench*, arXiv:2601.13770 and *Summoning the Oracle to Slay It*, Li, Wang & Ma, arXiv:2605.24564 — **B**. Benchmarks for point-in-time correctness.

**How this class is actually caught, per the practitioner and academic literature:**
1. **The delay test.** Add an artificial lag of D bars to every feature and re-run. If performance changes substantially, you had leakage. This is the single most-cited detection method (Michael Harris, https://mikeharrisny.medium.com/look-ahead-bias-in-backtests-and-how-to-detect-it-ad5e42d97879 — **C**).
2. **Negative and positive controls.** "A pipeline that finds alpha in noise has leakage; one that can't find a planted signal has bugs." Mesfin's falsification study is the published instance — its positive controls (t=3.11, t=4.30) demonstrate the test had power, which is what makes the 0-for-14 result interpretable rather than just a broken harness.
3. **Suspicious-baseline triggers.** Documented heuristics: a very smooth log equity curve, Sharpe > 1.5, or an equal-weight baseline Sharpe near 1.00 as a trigger to go looking for survivorship.
4. **Event-driven rather than vectorized backtesting.** Vectorized array code is the classic source of off-by-one index shifts (QuantStart, https://www.quantstart.com/articles/Successful-Backtesting-of-Algorithmic-Trading-Strategies-Part-I/ — **C**).
5. **Shared code paths between research and execution**, so the backtest cannot silently diverge from the mechanism it claims to validate.
6. **Deflated Sharpe, PBO, and combinatorial purged cross-validation.** Comparative work in *Expert Systems with Applications* (2024) finds **CPCV superior to walk-forward and k-fold for mitigating overfitting**, on both lower PBO and higher DSR. https://www.sciencedirect.com/science/article/abs/pii/S0950705124011110 — **D**

**On "warm-up" specifically:** I found **no named treatment of indicator warm-up handling as a distinct error category** anywhere in the literature. It is folded into look-ahead / indicator-initialization bugs and mentioned only in passing in practitioner guides. That is worth knowing: **the failure mode where a declared gate fails OPEN because its indicator is not yet warm has no literature, no benchmark, and no standard test.** The structurally identical documented cases are the ones above — a filter that silently does not filter is the same thing as a universe that silently excludes dead coins, or a cost parameter that silently defaults to zero. What they share is that the *declared* mechanism and the *executed* mechanism differ, and nothing in the output announces it.

A useful magnitude comparison for calibration: your warm-up correction moved PF from 0.855 to 0.761 on the continuous window, and collapsed `bull_2021` from n=25/PF 1.42 to n=6/PF 0.96. Ammann et al.'s universe-construction effect moves the momentum premium from 2.51% to 0.13%/week — a factor of 19. Bysik & Ślepaczuk's 10 bps cost assumption moves Sharpe from +2.59 to −1.82. **The documented magnitudes of this error class are consistently larger than the effects being measured.**

---

## What the evidence does NOT establish

1. **It does not establish that 4h single-asset crypto breakout systems work, or that they don't.** There is no academic literature on this configuration. The adjacent evidence is negative, but it is adjacent — cross-sectional portfolios, daily/hourly BTC, and non-crypto index futures. Anyone claiming the literature settles your specific mechanism is overreaching.

2. **It does not establish that crypto trend/breakout decayed from 2017–2021 to 2022–2026.** The documented decay is in mean-reversion, size, calendar anomalies, and parameter-optimised MA rules. The two sources that test trend across that boundary (Quantpedia's Feb 2022–Aug 2024 OOS window; Borri et al.'s post-2020 subsample, 2.1%/wk t=3.70) both find it **held**. A 2021-window edge disappearing is not corroborated by a decay finding in the literature.

3. **It does not establish that ATR-scaled exits beat fixed-percentage exits.** Zero peer-reviewed evidence either way. The CTA case for ATR is about risk normalisation across markets, which is a different claim.

4. **It does not establish that any R:R value, including 1.75, has or lacks an edge.** Under a martingale it is expectancy-neutral by theorem; under positive drift it is a documented cost; under positive autocorrelation it can help. Which regime 4h ZEC is in is an empirical question no published work has asked.

5. **It does not establish that your cross-asset ordering is real.** BTC (t=−6.79) and ETH (t=−4.47) losing is real. SOL and ZEC are not distinguishable from zero (t=−1.58, −1.41) or from each other (t=−0.19), and no pairwise gap survives Bonferroni across six comparisons. There is also no documented cross-asset pattern in the literature for it to be consistent *with* — and what pattern exists (momentum in liquid/large coins, reversal in small) runs the other direction.

6. **It does not establish what profit factor constitutes an edge.** PF is not an academic metric and the thresholds circulating online are unsourced. The defensible statement is conditional: PF 1.47 for t=2 at n=114, PF 1.78 for t=3 at n=114, and roughly 1,000 trades to detect a true PF of 1.2 at 80% power.

7. **It does not establish that your current number is a loss.** PF 0.761 at n=114 gives t=−1.41, p=0.159, with a 95% PF interval of roughly [0.50, 1.13] that still contains break-even. The sample establishes neither an edge nor its absence. My per-trade variance model is optimistic (pure two-point outcomes), so the true interval is wider.

8. **It does not tell you how common warm-up bugs specifically are.** Nobody measures them; they aren't a named category. What is documented is that *the general class* — declared mechanism ≠ executed mechanism — is endemic, survives peer review, and typically produces effects **larger than the signal under study**. Three separate 2026 preprints building formal verification machinery for temporal leakage is the field conceding that human review does not catch it.

9. **It does not establish that crypto momentum exists at all in a tradeable form.** Ammann et al. (0.13%/wk, t=0.13, survivorship-free) and Grobys et al. (0.36%/wk, t=0.75, survivor coins) say no. Borri/Liu/Tsyvinski/Wu (2.1–2.6%/wk, t≈3.7–3.9, gross) say yes. Grobys & Shahzad say the question may be unanswerable because the variance is plausibly infinite and the Sharpe ratio therefore undefined. **This is an unresolved and actively contested question, not a settled premise anyone is building on.**

10. **Nothing was found on ZEC specifically.** No study covers it. Note that ZEC is a privacy coin with a history of exchange delistings — which places it squarely in the population the survivorship literature is about, and means the usual "large liquid coin" reasoning does not transfer to it either.

---

### Caveats on this review
- Several key primary texts were behind 403s or paywalls: Kaminski & Lo's full paper, Grobys & Shahzad, the Bitcoin intraday TSM paper (Reading repository and Wiley both blocked), the FRL reality-check paper, the Monash trend-following paper, and the Zarattini SSRN PDF. Figures from those are marked **D** and should be verified before load-bearing use.
- All statistical computations in Q4 and Q5 are mine, derived from the PF/n figures in your brief under an explicit two-point payoff model (+1.75R / −1R). They are arithmetic on your stated numbers, not findings from any paper, and the model understates true per-trade variance.
- The 4h-timeframe and R:R-ratio searches returned essentially nothing but content marketing. That absence is itself a finding and I have reported it as one rather than dressing up blog claims as evidence.
