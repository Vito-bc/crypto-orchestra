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

# Literature map: cost tolerance and evidence quality of crypto strategy families (as of 2026-09-17)

Scope note: this is a map of what is documented, not a recommendation. Per project rules no family is being selected here. Quality scale used: **A** = peer-reviewed top-tier journal or central-bank working paper, costs modeled, some OOS; **B** = peer-reviewed or serious working paper with a material limitation (gross of costs, short sample, or inaccessible detail); **C** = practitioner write-up with real data but no cost model or not verifiable; **D** = commentary/secondary only. "Our cost" below means 1.8% round trip (0.6% maker in, 1.2% taker out), i.e. ~0.9% average one-way.

Access failures to note up front: SSRN, ScienceDirect, Springer and ResearchGate blocked direct fetches (403/CAPTCHA) and Semantic Scholar rate-limited me. I extracted full text locally from the open PDFs (Borri/Liu/Tsyvinski/Wu 2025; Detzel et al.; Fieberg et al. JFQA 2025; Hudson & Urquhart 2021; Liu/Tsyvinski/Wu NBER; Dobrynskaya; Gbadebo 2026; Rozario et al. 2020). For Han/Kang/Ryu (SSRN 4675565), Zarattini et al. (SSRN 5209907) and the funding-arbitrage paper, only abstract-level content was reachable; those are marked.

---

## 1. Long-horizon time-series trend following (daily bars, weekly/monthly signal speed, holding weeks)

**(a) Returns after realistic costs, with sample periods**

- **Detzel, Liu, Strauss, Zhou, Zhu (Financial Management 2021; SSRN 3115846).** BTC daily, 10/27/2010–1/31/2018. MA(L) strategies for L=5..100 days, in and out of sample (OOS R² 0.91–2.46%). The paper reports the **one-way fee that would eliminate the alpha** (FEE) and daily turnover (TO): MA5 TO 21.75%/day, FEE 1.12%; MA10 12.93%, 1.76%; MA20 7.95%, 2.36%; **MA50 5.01%/day, FEE 2.74%; MA100 2.68%/day, FEE 3.96%.** Sharpe of MA50/MA100 2.13/2.09 vs BTC buy-and-hold 1.9. Second-half subsample still positive (Sharpe up to 2.1, max DD cut from 73.4% to as low as 33.5%). Caveat: the break-evens are high because gross alpha in 2010–2018 was enormous (BTC excess return 213.6%/yr); they do not transfer to a later regime. Quality **A-** (sample ends Jan 2018). https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3115846
- **Hudson & Urquhart (Annals of OR 2021).** ~15,000 rules, BTC (CoinDesk, Bitstamp), ETH, XRP, LTC, to end-2017 in sample. **Break-even one-way costs for Bitcoin top out at 66.41 bp (CoinDesk) and 57.51 bp (Bitstamp)**; range across all coins/rules 7.88–147.56 bp. **Pure OOS test on H1 2018 (bear): best in-sample rules gave negative annualized return and Sharpe for both BTC series** (CoinDesk −0.10%/Sharpe −0.05; Bitstamp −0.91%/−0.06), positive for LTC/XRP/ETH. Quality **A-**. Both BTC break-evens are below our 0.9% average one-way cost. https://d-nb.info/1202710646/34
- **Zarattini, Pagani, Barbon (SSRN 5209907, Apr 2025; Swiss Finance Inst. WP 25-80).** Ensemble of Donchian-channel trend models, vol-based sizing, survivorship-bias-free universe since 2015, rotational top-20 liquid coins, Jan 2015–Mar 2025. Secondary reporting: **CAGR 30%, Sharpe 1.58, alpha ~10.8–14% vs BTC, net of fees tested at 0.10%–0.50%.** I could not open the paper; the fee-sensitivity table, turnover and 2022 subperiod are unverified by me. Note the highest fee tested (0.50%) is under a third of our round-trip. Quality **B** (working paper, inaccessible detail). https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5209907 ; https://ideas.repec.org/p/chf/rpseri/rp2580.html
- **Frömmel & Deprez (IREF 2024, vol 93 pp 858–874).** 75,360 rules, daily and intraday BTC, multiple-hypothesis selection **after transaction costs**; "certain combinations" beat buy-and-hold mainly on risk-adjusted terms. Cost level and net numbers inaccessible to me. Quality **B**. https://www.sciencedirect.com/science/article/abs/pii/S1059056024003010
- **Svogun & Bazán-Palomino (JIFMIM 2022).** 69 MA/breakout rules, daily and 1-minute, BTC/BCH/ETH/XRP/LTC, 2016–2021. Adding transaction costs "decrease[s] the likelihood [of beating B&H] for Ripple and Litecoin, but increase[s] it for Bitcoin and Ethereum"; bubble periods help ETH/XRP/LTC but not BTC/BCH. Cost level not visible to me. Quality **B+**. https://faculty.up.edu.pe/en/publications/technical-analysis-in-cryptocurrency-markets-do-transaction-costs/
- **Anghel (FRL 2021) — negative result.** After controlling for data snooping and market frictions, "statistically significant positive excess returns are rarely achieved, independent of the data sampling frequency"; rule performance is correlated with beta and idiosyncratic vol, i.e. rules mostly harvest market risk premia. Quality **A-**. https://www.sciencedirect.com/science/article/abs/pii/S1544612320304414
- **Kang & Ryu (Risk Management, 2026).** BTC time-series momentum across signal speeds: **slow 12-week-baseline signals beat intermediate and fast ones** (inverse of the equity pattern); speed acts as risk management. No cost figures visible. Quality **B**. https://link.springer.com/article/10.1057/s41283-026-00234-7
- **Grayscale Research, "The Trend is Your Friend" (2023).** BTC Jan 2012–Jul 2023. 50d MA Sharpe 1.9 vs B&H 1.3; 20/100 crossover 116%/yr (Sharpe 1.7) vs 110%; 150d EMA 126% (1.9). **Explicitly no trading fees.** 2020–2023 subperiod: crossover "better risk-adjusted returns but lower total returns" than B&H. Quality **C**. https://research.grayscale.com/reports/the-trend-is-your-friend-managing-bitcoins-volatility-with-momentum-signals
- **Rozario, Holt, West, Ng (arXiv 2009.12155, 2020).** BTC hourly, Sep 2011–Dec 2019, SMA/EMA/DEMA walk-forward. "Negligible transaction fees" assumed. ~10/40-day SMA "consistently performs well, with a slight dropoff in more recent times"; walk-forward Sharpe 0.5–1.5; "notable absence of profitable intra-day trend following strategies." Quality **C+** (exchange-sponsored, no costs). https://arxiv.org/pdf/2009.12155
- **Gbadebo (Vilnius Univ. BATP 2026).** 8 majors incl. SOL, 1 Jan 2020–31 Oct 2025, multi-horizon EMA TS vs CS momentum, daily rebalancing, **gross of costs**. TS 31.96%/yr vs CS 14.59%; **"During downturn or choppy years (2022–2023), TS produces negative annual returns as trends collapse"**; 2024–2025 "modest, similar positive returns." Quality **C+** (minor journal, no costs, daily rebalancing so not truly long-horizon). https://www.journals.vu.lt/BATP/en/article/download/44540/42590/138419
- **Quantpedia multi-timeframe BTC study (Dec 2018–Nov 2025, Gemini).** Hourly MACD with daily filter: 4.6–6.6%/yr, Sharpe 0.33–1.07, max DD −12 to −24%, vs B&H ~60%/yr with −80% DD; 1,000–2,262 trades. Illustrates how thin the net edge is once signals are fast. Quality **C**. https://quantpedia.com/how-to-design-a-simple-multi-timeframe-trend-strategy-on-bitcoin/

**(b) Frequency / structural cost tolerance.** Daily-bar MA rules at 50–100-day lookback turn over ~2.7–5% of the position per day (Detzel), i.e. a full round trip every ~20–40 days. At our 0.9% average one-way cost that is roughly 9–16%/yr of drag on MA100/MA50, before slippage. Slow (12-week) signals are the only speed with documented superiority in BTC (Kang & Ryu), and they are exactly the ones whose cost drag is lowest. No study models a 1.5–2% round trip explicitly; the closest are the Detzel break-evens (which exceed it, but only for 2010–2018) and Hudson & Urquhart's (which do not, at 57–66 bp for BTC).

**(c) Failure modes / breaking periods.** H1 2018 bear: BTC rules negative OOS (Hudson & Urquhart). 2022–2023: TS momentum negative (Gbadebo). 2020–2023: crossover lagged B&H on total return (Grayscale). 2024–2026 context: BTC ATH $126k (6 Oct 2025), −54% to $57.7k (1 Jul 2026), H1-2026 BTC −32.9%, ETH −47%, then +25% in Aug 2026 (NYDIG Q2-2026 review; CoinDesk 26 Jun 2026; Yahoo/Fortune Sep 2026). NYDIG characterizes 2026 as "momentum traders lack confirmation signals" — a whipsaw environment for trend. Traditional-market CTAs were the worst-performing hedge-fund strategy in 2025 (With Intelligence). https://www.nydig.com/research/q2-2026-review-leverage-not-spot-demand-is-driving-bitcoin-while-value-and-momentum-buyers-wait ; https://www.coindesk.com/daybook-us/2026/06/26/with-crypto-ending-the-first-half-in-the-red-bitcoin-s-solace-is-it-beat-strategy

**(d) Data/infrastructure.** Daily OHLCV only; long-only spot suffices for single-asset TS trend. Rotational multi-coin variants (Zarattini) need a survivorship-free universe and liquidity screens we do not have on Coinbase for 20 coins.

**(e) Evidence quality.** Mixed A-/B for pre-2022 samples; C for anything covering 2024–2026. No peer-reviewed result at ≥1.5% round-trip cost.

---

## 2. Cross-sectional momentum (rank, hold winners, rebalance)

**(a) What the literature reports and what survives**

- **Liu & Tsyvinski (RFS 2021)** and **Liu, Tsyvinski, Wu (JF 2022; NBER w25882).** Coins >$1M cap, 2014–2018 (JF) / to mid-2020 (later versions), **weekly value-weighted, weekly rebalanced**; 1-, 2-, 3-, 4-week lookback long-short spreads 2.7%, 3.3%, 4.1%, 2.5% per week; three-factor model (market, size, momentum). The NBER text states plainly: **"this strategy does not take into account trading costs."** Momentum concentrated in above-median-size coins (4.2%/wk) vs 0.6% insignificant in small. Quality **A** (gross). https://www.nber.org/system/files/working_papers/w25882/w25882.pdf ; https://onlinelibrary.wiley.com/doi/abs/10.1111/jofi.13119
- **Post-publication update — Borri, Liu, Tsyvinski, Wu (arXiv 2510.14435, v4 2025).** Sample to 6 Sep 2025. Two-week CMOM long-short still significant post-2020: **2.1%/week, t=3.70** (vs 2.6% full sample), value-weighted weekly, **gross**. But **12- and 24-week lookback momentum are insignificant post-2020 (0.6%, t=1.30; −0.4%, t=−0.66)** and 24-week is negative even in the full sample. Quality **B+** (working paper; gross). https://arxiv.org/pdf/2510.14435
- **Dobrynskaya (SSRN 3913263; 2,000 coins 2014–2020).** Positive momentum only up to 2–4 weeks; **significant reversal beyond ~1 month**, strongest for 4–6-week sorts with 6–8-week rebalancing (annualized reversal >100%), driven by past losers. **Direct implication: "rank assets, hold winners, rebalance monthly" as posed is not the documented anomaly — at monthly horizon the documented sign is reversal.** Zero-cost long-short, no transaction costs. Quality **B+**. https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3913263
- **Grobys & Sapkota (Econ Letters 2019).** 143 coins, monthly returns, 2014–2018: **no significant momentum payoff at 2–12-month windows.** Quality **A-**. https://www.sciencedirect.com/science/article/pii/S0165176519301077
- **Han, Kang, Ryu (SSRN 4675565, Dec 2023, "…under Realistic Assumptions").** Abstract only: "evidence of time-series momentum is strong, whereas evidence of cross-sectional momentum is weak"; accounting for transaction costs and **daily price fluctuations, "many momentum portfolios are liquidated and many with statistically significant returns earn insignificant profits."** Cost levels not visible to me. Quality **B** (working paper; details unverified). https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4675565
- **Fieberg, Liedtke, Poddig, Walker, Zaremba, "A Trend Factor for the Cross Section of Cryptocurrency Returns" (JFQA Nov 2025, open access).** >3,000 coins, Apr 2015–May 2022, weekly value-weighted. CTREND long-short gross 3.87%/wk; **turnover 68%/week**; net 2.90%/2.62%/2.35% at one-way costs of 30/40, 40/50, 50/60 bp (long/short); **break-even one-way cost 1.41% (0.88% for 5% significance); largest-100 coins: gross 3.40%, BETC 1.25%/0.70%.** Extending rebalance to 2 weeks cuts weekly return by 1.5 pp to 2.34%. Long-short (needs shorting). Quality **A** (gross-to-net modeled; sample ends May 2022 so excludes the 2022 crash aftermath and 2024–26). https://www.cambridge.org/core/journals/journal-of-financial-and-quantitative-analysis/article/trend-factor-for-the-cross-section-of-cryptocurrency-returns/4C1509ACBA33D5DCAF0AC24379148178
- **Li & Zhu, "Taming crypto anomalies: a Lasso-type factor model" (Research in Intl Business & Finance 2026; SSRN 2023).** **Of 49 anomalies, only 13 significant 2014–2023**; two-week momentum retained in the DS3 model; size effect disappears OOS. Quality **B+**. https://www.sciencedirect.com/science/article/abs/pii/S0275531926000255
- **Grobys, Kolari, Sandretto, Shahzad, Äijö, "Cryptocurrency momentum has (not) its moments" (FMPM Dec 2025).** Large-cap equal-weight momentum "is subject to severe crashes. Even a single cryptocurrency can cause insignificant momentum portfolio returns"; vol management mitigates. Quality **A-**. https://link.springer.com/article/10.1007/s11408-025-00474-9
- **Grobys & Shahzad, "Cryptocurrency Momentum: Is It an Illusion?" (Intl J. Finance & Econ 2026).** Realized variances of momentum factors follow power laws such that **"population mean and variance … are statistically not defined"**; Sharpe-type metrics "are not informative." Quality **B+**. https://ideas.repec.org/a/wly/ijfiec/v31y2026i2p2180-2193.html
- **Yang, "Cryptocurrency market risk-managed momentum strategies" (FRL 2025).** Risk management lifts weekly returns 3.18%→3.47%, Sharpe 1.12→1.42; cost treatment not visible. Quality **B**. https://www.sciencedirect.com/science/article/abs/pii/S1544612325011377
- Post-2022 practitioner context: altcoin universe ex-BTC/ETH/stables −44% from late-2024 peak through end-2025; altcoin rallies shortened to ~20 days in 2025 (Wintermute via Bitcoin.com); "no altcoin season" (21Shares). Quality **D**. https://www.21shares.com/en-us/insights/thousands-of-altcoins-but-no-altcoin-season-what-comes-next

**(b) Frequency / cost tolerance.** The documented crypto momentum is 1–4-week lookback with **weekly** rebalancing; CTREND turns over 68%/week. At 0.9% one-way that is ~0.6%/week ≈ 30%+/yr of cost — survivable only because gross spreads were 2–4%/week in 2015–2022 samples of thousands of coins. Monthly-rebalanced versions have no documented positive expectancy (Grobys & Sapkota; Dobrynskaya reversal; Borri MOM12/24 insignificant).

**(c) Failure modes.** Severe crashes, single-coin dependence, undefined variance (Grobys et al.); "liquidation" under daily price paths (Han et al.); 2022–2023 momentum failures reported in secondary sources; long-short construction needs shorting; small-coin spreads dominate the gross return.

**(d) Data/infrastructure.** Needs a broad (hundreds+) survivorship-free universe with market caps, weekly rebalancing, and a short leg. Four Coinbase spot assets cannot form the documented portfolios; Gbadebo's 8-coin CS version had −55% max DD and half the return of TS, gross.

**(e) Evidence quality.** A for the existence of the short-horizon anomaly, gross; B for post-2020 persistence; no A-quality net-of-retail-cost, spot-only, post-2022 evidence.

---

## 3. Funding-rate / basis carry

**Plain statement: the documented carry strategy is long spot + short perpetual (or dated) futures. It cannot be run spot-only. Without a short futures leg there is no funding receipt to harvest.**

- **Schmeling, Schrimpf, Todorov, "Crypto Carry" (BIS WP 1087; CEPR DP20719, 2025).** Carry "can become very large (up to 60% p.a.)", averages above 10%/yr; **high carry predicts crashes** and coincides with rising crash-insurance prices; arbitrage is risky "due to spikes in margins and liquidations amid drawdowns." Quality **A**. https://www.bis.org/publ/work1087.pdf ; https://cepr.org/voxeu/columns/crypto-carry-market-segmentation-and-price-distortions-digital-asset-markets
- **Borri, Liu, Tsyvinski, Wu (2025)**, Binance BTC 8-hour funding, 1 Aug 2020–31 May 2025: **carry Sharpe 6.45 full sample; 4.06 from 2024; negative in 2025**; funding mean ≈8% with 0.8% vol. "Funding-rate premia are neither guaranteed nor permanent." Quality **B+**. https://arxiv.org/pdf/2510.14435
- **"Exploring risk and return profiles of funding rate arbitrage on CEX and DEX" (Blockchain: Research and Applications, 2025).** Abstract: 60 scenarios, BTC/ETH/XRP/BNB/SOL on Binance, BitMEX, ApolloX, Drift; uncorrelated with HODL; "up to 115.9% over six months" (leveraged) with losses "minimal 1.92%"; secondary reporting gives unleveraged PNL 7.61% (Drift), 5.59% (ApolloX), 2.17% (Binance), 1.98% (BitMEX) and max DD 0.10–0.14%. Sample window not visible to me. Quality **B-**. https://www.sciencedirect.com/science/article/pii/S2096720925000818
- 2026 state: NYDIG Q2-2026 reports perpetual funding back to positive with weak spot — i.e. carry exists but is tied to liquidation-cascade risk. Quality **C**.
- Access note (not a recommendation): Coinbase Financial Markets launched CFTC-regulated perpetual-style futures for US retail on 21 Jul 2025 (nano BTC 0.01, nano ETH 0.10, funding mechanism, 5-year expiry, up to 10x, taker fees from 0.02%). This changes the "no derivatives access" premise only if the project explicitly decides to add that venue. https://www.coinbase.com/blog/perpetual-futures-have-arrived-in-the-us

**(b)** Positions are held continuously; cost is in spread/basis at entry plus margin. **(c)** Compression since 2024, negative in 2025; margin spikes and liquidations in drawdowns; exchange/counterparty risk. **(d)** Perps venue, margin account, funding data. **(e)** A/B for the pre-2024 return history; the 2025 negative result is documented by the same authors.

---

## 4. Mean reversion / market-making-style at retail (taker-fee payer)

- **"Short-horizon mean reversion in cryptocurrency markets: a matched cross-market measurement" (arXiv 2608.21888, 2026).** 183 Binance USDT spot pairs vs 187 US equities, 1 Jan 2025–11 Feb 2026 plus six-month holdout to 8 Aug 2026. A persistent 15-minute reversal exists in crypto (90% FDR-significant), but **"the gross edge peaks near 1.3 bp per trade"** against 5 bp cheapest round-trip and 10–20 bp taker costs; **"not exploitable under benchmark spot-cost assumptions: a public-schedule taker or maker paying explicit spot costs cannot capture it anywhere in the cross-section."** Quality **B+** (preprint; recent, includes 2025–26). https://arxiv.org/html/2608.21888v1
- **Dynamic grid trading (arXiv 2506.11921).** Under simple assumptions classic grid trading's **expected return is essentially zero**; the proposed dynamic variant's backtest (BTC/ETH minute data, Jan 2021–Jul 2024) does not state cost assumptions in the abstract. Quality **C**. https://arxiv.org/abs/2506.11921
- **Pairs/cointegration trading.** Thesis-level work reports costs consuming up to 47.7% of gross profits at low thresholds and negative net returns in other specifications; results are highly parameter-sensitive. Quality **C**. https://thesis.eur.nl/pub/67552/Thesis-Pairs-trading-.pdf ; https://link.springer.com/article/10.1186/s40854-024-00702-7
- **Anghel (2021)**, **Hudson & Urquhart (2021)** apply here too: after snooping and frictions, short-horizon rules rarely retain significance; BTC break-even 57–66 bp.
- **Chakraborty & Kearns (market making profitable under OU mean reversion)** is a fee-free theoretical result; practitioner sources uniformly state that passive quoting is profitable only "while adverse selection stays below what the spread and rebate pay" — i.e. it is structured around maker rebates/near-zero maker fees, which a 0.6% maker tier does not have. Quality **D** for the practitioner claims.

**Answer to the question as posed:** I found **no credible evidence** that mean reversion or market-making-style strategies work for a retail participant paying taker (or 0.6% maker) fees on spot. The best recent measurement says the short-horizon edge is ~1 bp against costs of 5–20 bp — two orders of magnitude below our 180 bp. The evidence points to this family being structurally reserved for near-zero-fee/rebate participants.

---

## 5. Volatility-targeted / risk-parity long-only allocation, infrequent rebalancing

- **Man Group, "Crypto. Too Hot to Handle?" (Sep 2012–Dec 2024).** Scaling BTC to a 30% ex-ante vol target adds "around 40 Sharpe points" and improves vol-of-vol and expected shortfall; keeps the risk share of a 1% sleeve stable (unscaled oscillates 0–6%). **Turnover and transaction costs are not quantified.** Quality **C+**. https://www.man.com/insights/crypto-too-hot-to-handle
- **Moreira & Muir (JF 2017)** — the parent result survives realistic equity costs, but it is not a crypto result and equity costs are ~4 bp. Quality A for equities, not transferable.
- **Grobys et al. (2025)** and **Yang (2025)** above: vol management mitigates momentum crashes (gross).
- **Quantpedia, "How much Bitcoin should we allocate"**: risk parity assigns ~2% to BTC in both 2013–2017 and 2018–2023; 2018–2023 risk parity 6.54%/yr at 9.84% vol vs equal-weight 9.05%/13.93%; authors cap BTC at 2–3%. No cost modeling stated. Quality **C**. https://quantpedia.com/how-much-bitcoin-should-we-allocate-to-the-portfolio/
- **Hung, Liu, Yang (NAJEF 2024).** Volatility-timing with rebalancing: **Bitcoin adds value in dovish regimes but "destroys value" during rapid rate hikes** (2022); "the rebalancing strategy exerts substantial effects … in the presence of transaction costs." Quality **B+**. https://ideas.repec.org/a/eee/ecofin/v74y2024ics1062940824001852.html
- **Target-volatility rebalancing boundaries (FMPM 2025)** reduce cost drag without losing risk control — generic, not crypto-specific. Quality **B**. https://link.springer.com/article/10.1007/s11408-025-00486-5
- **Systematic Trend-Following with Adaptive Portfolio Construction (arXiv 2602.11708, 2026)** is sometimes cited as vol-targeted crypto: 2022–2024 OOS Sharpe 2.41 vs BTC 0.17 — but it is a **long-short Binance Futures strategy at 4 bp taker fees with ~134% monthly turnover and 142 trades/month**, and its own bear-regime Sharpe is −0.31. Not evidence for this family at our costs. Quality **C**.

**(b)** Vol targeting on a single asset is a risk transform, not a return source; it trades on vol changes, and no crypto source I found reports its turnover or net-of-cost result. **(c)** 2022 rate-hike regime (Hung et al.); rising crypto-equity correlation limits the diversification benefit (Man). **(d)** Daily data suffices. **(e)** C+ for crypto-specific net-of-cost claims; nothing at ≥1.5% round trip.

---

## 6. Is there documented positive OOS expectancy after ≥1.5% round-trip, spot-only, small-notional, multi-year including 2022 and 2025–2026?

**I could not find one.** Every candidate fails at least one criterion:

| Candidate | Fails on |
|---|---|
| Detzel et al. daily MA (break-even one-way 1.12–3.96%) | sample ends Jan 2018; break-even inflated by 2010–2018 gross alpha; Hudson & Urquhart's 2018 OOS shows negative BTC results |
| Zarattini et al. Donchian ensemble (net of 0.10–0.50%) | fees tested ≤0.50%; rotational 20-coin universe; full text unverified; not pre-registered |
| CTREND (BETC 1.41%/0.88%) | long-short; weekly 68% turnover; thousands of coins; sample ends May 2022 |
| LTW two-week momentum post-2020 (2.1%/wk) | gross of costs; long-short; weekly; hundreds of coins |
| Carry (Sharpe 6.45 → negative 2025) | requires short perpetual |
| 15-min mean reversion (1.3 bp edge) | uncapturable even at 5 bp |
| Vol-targeting (+0.4 Sharpe) | no turnover/cost figures; not an expectancy source |
| Crypto quant funds +0.4% in 2025 vs BTC −6.3% (Crypto Insights Group, per search snippet; I could not verify on their site) | institutional fee tiers, derivatives, not spot-only; unverified |

This absence is itself the result: the literature's cost assumptions cluster at 4–60 bp; the highest explicitly tested is ~1.4% one-way (CTREND break-even, long-short). Nothing peer-reviewed tests a 1.8% round trip on a 2–4-asset spot book, and nothing covers 2025–2026 except preprints and practitioner commentary.

---

## Summary table

| Family | Documented cost tolerance | Evidence quality | Known breaking periods | Needs derivatives? |
|---|---|---|---|---|
| Long-horizon TS trend (daily bars, slow signals) | Break-even one-way 1.1–4.0% for BTC MA5–MA100 in 2010–18 (Detzel); 57–66 bp (Hudson & Urquhart, to 2017); tested net of ≤0.5% (Zarattini, 2015–Mar 2025). Slow (12-wk) signals documented best in BTC. | A-/B pre-2022; C after | H1 2018 (BTC OOS negative); 2022–23 (TS negative, gross); 2026 whipsaw (BTC −32.9% H1 then +25% Aug) | No |
| Cross-sectional momentum | 1–4-wk lookback, weekly rebalance, 68%/wk turnover; BETC 1.41% (long-short, 2015–22); monthly-horizon momentum not documented (reversal) | A gross; B net | Momentum crashes; 2022–23; single-coin dependence; undefined variance | Needs shorting; needs broad universe |
| Funding/basis carry | Not a cost question; Sharpe 6.45 (2020–25) → 4.06 (2024) → negative (2025); margin/liquidation risk | A/B+ | 2024–25 compression; crash episodes when carry is high | **Yes** (short perp) |
| Mean reversion / MM at retail | Gross edge ~1 bp vs 5–20 bp costs; grid EV ≈ 0; pairs net negative at realistic costs | B+ (negative result) | Trending regimes (grid); always at taker fees | No, but structurally requires rebate/near-zero maker fees |
| Vol-target / risk parity long-only | Not quantified in crypto; +0.4 Sharpe gross (Man); BTC ~2% weight under risk parity | C+ | 2022 rate-hike regime (value destroyed, Hung et al.) | No |

---

## What the evidence does NOT establish

1. **No study tests a ≥1.5% round-trip cost on spot.** The highest modeled costs are 50–60 bp per leg (CTREND) and the highest reported break-even at our horizon class is Detzel's 2010–2018 figure, which is regime-dependent and not replicated post-2018.
2. **No net-of-realistic-cost result includes 2025–2026.** Coverage of that period is limited to gross-of-cost academic samples ending 2025 (Gbadebo; Borri et al.) and practitioner commentary.
3. **Nothing addresses small notional.** Minimum order sizes, price/size rounding, and spread on a $2 order are outside every source found.
4. **Nothing addresses ZEC-specific liquidity on Coinbase.** All altcoin evidence is Binance/CoinMarketCap-universe based.
5. **Cross-sectional momentum evidence does not transfer to a 4-asset long-only book.** The anomaly is defined on hundreds-to-thousands of coins, value-weighted, long-short, weekly. The monthly-rebalance version asked about has documented *negative* sign beyond ~1 month.
6. **Vol targeting has no crypto net-of-cost turnover accounting** in any source I found; it is a risk transform, and its "+0.4 Sharpe" is gross.
7. **Carry's 2025 negative year is documented by the same authors who documented the 6.45 Sharpe**, so the pre-2024 history should not be read as forward expectancy; and it is unavailable without a short futures leg regardless.
8. **Three key sources were only partially readable** (Han/Kang/Ryu; Zarattini et al.; funding-arb BCRA paper). Their headline claims above are abstract-level or secondary and should be verified before being used in any pre-registration document.
9. **Publication effects are real in this literature**: 49 anomalies → 13 significant (Li & Zhu); size effect vanished OOS; 12/24-week momentum insignificant post-2020; momentum variance possibly undefined (Grobys & Shahzad). Any family chosen from this reading inherits multiple-testing exposure that only a pre-registered, unseen-data test can remove.
