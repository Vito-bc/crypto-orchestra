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

# Backtest-Validity Methodology Review

**Scope:** validation method only. No strategy is recommended, retained, or proposed anywhere below.
**Date:** 2026-09-17. All formulas verified against the primary PDFs (downloaded and text-extracted, not summarized from memory). All numerical results below were recomputed locally and one of them reproduces a published figure exactly (see Q3), which is the check that the implementations are right.

**Source-quality scale used throughout:**
- **A** — peer-reviewed in a top econometrics/finance journal, result independently replicated or mathematically self-evident.
- **B** — peer-reviewed, well-cited, but little or no independent empirical validation of the *method itself*.
- **C** — practitioner journal / working paper, influential, method essentially unvalidated out of sample.
- **D** — secondary, blog, vendor documentation. Used only for formula restatement, never as evidence.

---

## Q1. Deflated Sharpe Ratio and PBO/CSCV

### 1a. What the DSR actually measures

**Primary:** Bailey & López de Prado, "The Deflated Sharpe Ratio: Correcting for Selection Bias, Backtest Overfitting and Non-Normality," *Journal of Portfolio Management* 40(5):94–107, 2014. https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551 — full text: https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf

The DSR is **not** a corrected Sharpe ratio. It is a **probability**: the probability that the true Sharpe of the selected strategy exceeds the benchmark implied by the best-of-N selection process. It is the Probabilistic Sharpe Ratio (PSR) evaluated at a raised threshold.

Exact chain (transcribed from the paper, Eq. 1 and Eq. 2):

```
Expected max Sharpe under the null (true SR = 0), Eq. (1):

  SR*  =  E[max{SR_n}]  ≈  sqrt(V[{SR_n}]) · ( (1-γ)·Z⁻¹[1 - 1/N]  +  γ·Z⁻¹[1 - 1/(N·e)] )

     γ = 0.5772156649 (Euler–Mascheroni), Z⁻¹ = inverse standard normal CDF, e = Euler's number

Deflated Sharpe Ratio, Eq. (2):

  DSR  =  Z[  (SR̂ - SR*)·sqrt(T-1)  /  sqrt( 1 - γ̂₃·SR̂ + ((γ̂₄ - 1)/4)·SR̂² )  ]

     SR̂ = observed (non-annualized) Sharpe; T = number of return observations;
     γ̂₃ = skewness; γ̂₄ = Pearson (non-excess) kurtosis of the selected strategy's returns
```

Set N = 1 and SR* = 0 and DSR collapses to the PSR of Bailey & López de Prado (2012), *The Sharpe Ratio Efficient Frontier*, **Journal of Risk** 15(2) — https://www.davidhbailey.com/dhbpapers/sharpe-frontier.pdf

Related, same family: **Minimum Track Record Length**, MinTRL = 1 + [1 - γ₃·SR̂ + ((γ₄-1)/4)·SR̂²]·(Z_α/(SR̂ - SR*))² observations.

**Required inputs — all five are needed, and four are hard:**
1. `SR̂` — easy.
2. `T` — easy.
3. `γ̂₃, γ̂₄` — estimable but noisy at small T (quantified below).
4. `V[{SR_n}]` — the **variance of the Sharpe ratios across all trials you ran**. Requires you to have retained every trial's P&L, including failures.
5. `N` — the number of **independent** trials. This is the weak link.

**Documented limitations — stated by the authors themselves** (Appendix A.3 of the DSR paper, verbatim points):
- "correlation is a limited notion of linear dependence."
- "in practice M almost always exceeds the sample length, T. Then the estimate of average correlation may itself be overfit."
- "In general for short samples … the correlation matrix will be numerically ill-conditioned … Estimating an average correlation is then pointless, because there are more correlations than independent pairs of observations."

That last point is decisive for your situation and is the authors' own admission, not a critic's.

The paper's Appendix 2 validates only the **accuracy of the extreme-value approximation** in Eq. (1) against simulation. It does **not** validate that the DSR achieves nominal Type I error control on real financial data under realistic trial-dependence. I found **no independent study** that does. Rating: **B, with the "widely cited, weakly validated" flag raised.**

**Later refinement for estimating N:** López de Prado, "A Data Science Solution to the Multiple-Testing Crisis in Financial Research," *JFDS* 1(1):99–110, 2019 — https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3177057 (open PDF: https://www.aqr.com/-/media/AQR/Documents/Journal-Articles/JFDS_Winter2019_A-Data-Science-Solution-to-Multiple-Testing-Crisis---Lopez_de_Prado.pdf). Reduces selection bias to two sub-problems: **(i) number of essentially independent trials, (ii) variance across those trials**, and estimates (i) by clustering the trials' return series (ONC algorithm; López de Prado & Lewis, *Quantitative Finance*, 2019). In his worked example the optimal clustering was **K = 4** clusters out of a much larger raw trial count. Rating: **C** (practitioner journal, no independent validation; the clustering-quality criterion is itself a tuned choice).

### 1b. What PBO/CSCV actually measures

**Primary:** Bailey, Borwein, López de Prado & Zhu, "The Probability of Backtest Overfitting," *Journal of Computational Finance* 20(4):39–69, 2016. https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253 — full text https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf

PBO = the probability that the configuration ranked best **in sample** will rank **below the median out of sample**. It is a property of a **selection procedure over a set of N configurations** — it is not a property of one strategy.

CSCV algorithm, exactly as specified (Algorithm 2.3):
1. Build matrix `M` of order `(T × N)`: N columns = configurations tried, T rows = synchronous P&L observations. Conditions: same row count per column, synchronous observations, and the performance metric must be computable on subsamples.
2. Partition rows into an **even** number `S` of disjoint submatrices of order `(T/S × N)`.
3. Form all `C(S, S/2)` combinations. **S = 16 → 12,780 combinations.**
4. Per combination: train set J = S/2 submatrices in original order (T/2 × N); test set J̄ = complement. Find n* = best rank IS; get its OOS relative rank ω̄ = r̄_{n*}/(N+1); logit λ = ln(ω̄/(1-ω̄)).
5. PBO = fraction of combinations with λ ≤ 0.

CSCV also yields performance degradation (OOS vs IS regression), probability of loss, and stochastic dominance diagnostics.

**Documented limitations — §5 of the paper, the authors' own list:**
- *Design:* symmetry may be unsuitable; "if the performance measure as a time series has a strong autocorrelation, then such a division may obscure the characterization especially when S is large."
- *Application 1 (file-drawer):* "the researcher must provide full information regarding the actual trials conducted … Hiding trials will lead to an **underestimation** of the overfit." And: "adding trials that are doomed to fail in order to make one particular model configuration succeed biases the result."
- *Application 2:* "this procedure does nothing to evaluate the correctness of a backtest. If the backtest is flawed due to bad assumptions, such as incorrect transaction costs or **using data not available at the moment of making a decision**, our approach will be making an assessment based on flawed information." **This is precisely your warm-up-bug class. PBO/DSR would not have caught it.**
- *Application 3:* structural breaks outside the sample window are invisible.
- *Application 4:* high PBO does not imply no skillful strategy exists in the set (all N could be genuinely good and similar).
- *Application 5:* "we must warn the reader against applying CSCV to guide the search for an optimal strategy. That would constitute a gross misuse of our method." Goodhart's law, cited explicitly.

Rating: **B.** Peer-reviewed, mathematically clean, authors unusually candid about limits; but again no independent Type-I-error validation on real data.

### 1c. Are these applicable at n = 114 trades? — **Largely no.**

| Requirement | Your situation | Verdict |
|---|---|---|
| DSR: T observations | T = 114 trade returns | Above the crude "T ≥ 50" floor, below the "≥ 252" norm. **Marginal.** |
| DSR: skew/kurtosis estimates | SE(skew) = √(6/114) = **0.229**; SE(excess kurtosis) = √(24/114) = **0.459** | The non-normality correction — the DSR's second selling point — is estimated with error comparable to the correction itself. **Adds noise, not precision.** |
| DSR: V[{SR_n}] across trials | Requires retained P&L for every trial ever run | Feasible only if your trial registry stores return series, not just summary numbers. |
| DSR: N independent trials | Authors: if M > T the correlation estimate "may itself be overfit … estimating an average correlation is then pointless" | With T = 114 and any nontrivial trial count, **the authors' own caveat fires.** |
| PBO/CSCV: matrix (T × N) | With one frozen configuration N = 1 | **PBO is undefined at N = 1.** A rank among one strategy carries no information. |
| PBO/CSCV: S partitions | S = 16 → 114/16 ≈ 7 observations per strip; Sharpe computed on 7-trade strips | **Not usable.** Even S = 4 gives 28-trade strips. The metric noise would dominate the ranking. |

**Bottom line on Q1:** DSR is computable on your sample but its two adjustments (non-normality, effective N) are both estimated so imprecisely at n = 114 that the output is closer to a restatement of your priors about N than a measurement. PBO/CSCV is **not applicable at all** to a single frozen configuration, and is only applicable retrospectively if you can reconstruct synchronous P&L series for the full set of configurations you tried. Where they *are* usable is on a **future, pre-registered sweep** where you retain all trial series by construction — which your registry infrastructure is already shaped to support.

---

## Q2. Multiple testing: haircuts, thresholds, and counting trials

### 2a. Harvey, Liu & Zhu — the t > 3.0 hurdle

**Primary:** Harvey, Liu & Zhu, "…and the Cross-Section of Expected Returns," *Review of Financial Studies* 29(1):5–68, 2016. https://academic.oup.com/rfs/article/29/1/5/1843824 — NBER WP 20592 full text: https://www.nber.org/system/files/working_papers/w20592/w20592.pdf

Verified numbers, quoted from the text:

| Adjustment | Controls | Implied benchmark t-ratio (2012) | 2032 projection | p-value |
|---|---|---|---|---|
| Bonferroni (α=5%) | FWER | **3.78** | 4.00 | 0.02% |
| Holm (α=5%) | FWER | **3.64** | 3.83 | — |
| BHY (α=1%) | FDR | **3.39** (stable after 2010) | — | 0.07% |
| BHY (α=5%) | FDR | **2.78** | 2.81 | 0.54% |

Their headline recommendation: **"a newly discovered factor needs to clear a much higher hurdle, with a t-ratio greater than 3.0"** — and they add, explicitly: *"While a ratio of 3.0 (which corresponds to a p-value of 0.27%) seems like a very high hurdle, we also argue that there are good reasons to expect that 3.0 is too low."*

Counting: they catalogue **316 published factors**, then note this "likely under-represents the factor population." Correlation-adjusted model with correlation among test statistics: **M ≈ 1,300 trials**, giving t ≈ 3.9 (FWER 5%) and 3.0 (FDR 1%). Rating: **A.**

### 2b. Harvey & Liu — the haircut

**Primary:** Harvey & Liu, "Backtesting," *Journal of Portfolio Management* 42(1):13–28, 2015. https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2345489. Code: https://people.duke.edu/~charvey/backtesting/ (`Haircut_SR.m`, `Profit_Hurdle.m`, `sample_random_multests.m`).
**Companion (open access, full text verified):** Harvey & Liu, "Evaluating Trading Strategies," *JPM* 40(5):108–118, 2014. https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2474755

Mechanics: adjust the p-value for multiplicity (Bonferroni / Holm / BHY, correlation-aware), back out the adjusted t-statistic, back out the Sharpe that would have produced it. Haircut = (SR − SR_adj)/SR.

**Worked example from the paper (verified):** candidate SR = 0.92, t = 2.91, p = 0.4%. With **200 trials**, Bonferroni cutoff = 0.05/200 = 0.00025, required t = **3.66**. Adjusted p = 0.004 × 200 = 0.80, adjusted t = 0.25, **adjusted Sharpe = 0.08 → haircut = 91%.**

**S&P Capital IQ real-data example**, 484 strategies, BHY (their *recommended* method): top performers SR 0.83 / 0.37 / 0.67 → adjusted 0.52 / 0.00 / 0.34 → **haircuts 37% / 100% / 49%.** Bonferroni on the same data: 47% / 100% / 90%.

**The 50% rule of thumb is rejected.** The haircut is non-linear: high Sharpes are lightly penalized, marginal Sharpes are annihilated (taken to zero). A flat 50% is always wrong in one direction or the other. Rating: **B** (practitioner journal, but the statistics are standard and the code is public).

### 2c. How is "number of trials" supposed to be counted when trials are informal and undocumented?

This is the weakest-supported part of the whole literature. Four distinct answers exist, and **none of them is validated**:

1. **Count every variation. Ex ante.** Arnott/Harvey/Markowitz protocol item 2a: *"Did the researcher keep track of all models and variables that were tried, both successful and unsuccessful?"* and 2c: *"Did the researchers investigate all variables set out in the research agenda, or did they cut the research as soon as they found a good model?"* The ml4trading practitioner doc states the rule bluntly: *"Every parameter variation, feature combination, and lookback period counts as a trial."* (Rating: D for that phrasing, A for the protocol.)

2. **Recover M statistically from the observed distribution of results.** HLZ Appendix A fits a **truncated exponential** to the observed t-ratios (truncation at 2.57) and estimates the unobserved mass. Their estimate: **M ≈ 817–820 total trials** behind 316 published factors; doubling the assumed sampling ratio roughly doubles M and raises Bonferroni/Holm t by ~0.2 but BHY by only ~0.03. **Takeaway: FDR-based hurdles are far less sensitive to your guess about N than FWER-based ones.** This is the single most practically useful finding for a project that cannot count its own trials.

3. **Cluster what you did run.** López de Prado (JFDS 2019): cluster trial return series; the **number of clusters** is the effective N. His explicit defence against undocumented trials: *"even if some trials are missing, the number of clusters will still be the same. The missing trials will be redundant, as they would have been folded onto clusters formed by the reported trials."* This is an **argument, not a demonstration** — no simulation or empirical test of that claim is provided. Rating: **C.**

4. **Reconstruct the counterfactual search.** Also LdP (2019): *"referees must require that authors run trials that other reasonable authors (who unselected themselves) would have attempted."* I.e. if you can't count what you did, count what a competent researcher **would** have done on this idea. Unvalidated but operationally honest.

**Harvey & Liu's own framing of the base rate:** with 10,000 managers picking randomly, *"you would expect at least 300 of them to have five consecutive years of outperformance."*

### 2d. The countervailing evidence — flag this, it matters

**Primary:** Andrew Y. Chen (Federal Reserve Board), "Most claimed statistical findings in cross-sectional return predictability are likely true," arXiv:2206.15365 (v10, Nov 2025; refereed, Haddad ed.). https://arxiv.org/abs/2206.15365

Chen derives FDR bounds: **"Easy Bound" FDR ≤ 25%** in eight of nine studies examined; **"Visual Bound" FDR ≤ 9%**, i.e. at least 91% of discoveries true. He shows HLZ's own methods imply FDR ≈ 9%, and that the "most findings are false" claim comes from **interpreting insignificant factors as false discoveries**, not from the data.

**Why this does not help you.** Chen's bound rests on a *base-rate* fact specific to cross-sectional equity accounting ratios: **randomly selected accounting ratios produce |t| > 2 about one time in five**, versus one in twenty under the null — so the prior probability that a randomly drawn candidate is real is high in that domain. **No analogous base rate has been established for single-asset crypto technical rules.** Chen's argument, correctly read, says the right hurdle depends on the prior fraction of true effects in the search space — which must be *argued from domain evidence*, and in your domain it has not been. **This is an argument for a higher hurdle in your setting, not a lower one.** Rating: **A.**

**Practical synthesis for Q2:** Use an **FDR-based** threshold rather than FWER, precisely because HLZ's own sensitivity analysis shows FDR hurdles barely move (±0.03 in t) when your trial count is off by 2×, whereas FWER hurdles move ±0.2. The defensible operating threshold, taken straight from HLZ: **t ≥ 2.78–3.39 depending on whether you set FDR at 5% or 1%.**

---

## Q3. Minimum backtest length, and the actual power of n = 114

### 3a. What Bailey et al.'s MinBTL result actually says

**Primary:** Bailey, Borwein, López de Prado & Zhu, "Pseudo-Mathematics and Financial Charlatanism: The Effects of Backtest Overfitting on Out-of-Sample Performance," *Notices of the AMS* 61(5):458–471, 2014. https://www.ams.org/notices/201405/rnoti-p458.pdf — full text used: https://www.davidhbailey.com/dhbpapers/backtest-pseudo.pdf

**Theorem 3.1, transcribed exactly:**

```
MinBTL (years)  ≤  [ (1-γ)·Z⁻¹(1 - 1/N)  +  γ·Z⁻¹(1 - 1/(N·e)) ]²  /  (E[max_N])²

                 <  2·ln(N) / (E[max_N])²
```

**What it assumes (all four, stated in the paper):**
1. Returns IID Gaussian (via re-scaling by y^(-1/2), y = years).
2. The N trials are **independent** — "which leads to a quite conservative estimate."
3. The **expected OOS Sharpe is exactly zero** (pure null). It is a statement about skill-less search.
4. `E[max_N]` is the IS Sharpe you would be willing to be fooled by.

**What it explicitly is not:** *"a backtest may be overfit even if it is computed on a sample greater than MinBTL. From that perspective, MinBTL should be considered a necessary, non-sufficient condition to avoid overfitting."*

**Recomputed locally (my implementation reproduces the paper's published figure exactly — the paper states 5 years → 45 trials; I get 45.0, and 2 years → 7 trials; I get 7.4, matching their "after trying only 7 independent strategy configurations"):**

| N independent trials | E[max_N] (z) | MinBTL yrs @ target SR=1 | MinBTL yrs @ target SR=0.5 |
|---|---|---|---|
| 5 | 1.193 | 1.42 | 5.7 |
| 10 | 1.575 | 2.48 | 9.9 |
| 20 | 1.901 | 3.61 | 14.5 |
| 45 | 2.236 | **5.00** | 20.0 |
| 100 | 2.531 | 6.40 | 25.6 |
| 200 | 2.766 | 7.65 | 30.6 |
| 1000 | 3.255 | 10.60 | 42.4 |

**Inverted for your windows:**

| Backtest length | Max independent trials, target SR = 1 | Max independent trials, target SR = 0.5 |
|---|---|---|
| 5.04 yr (ZEC continuous window) | **46** | **4.4** |
| 6.5 yr (research window 2020-01-01→2026-07-12) | **106** | **5.7** |
| 10 yr | 724 | 10.1 |

**The SR = 0.5 column is the operationally relevant one** — a true annualized Sharpe of 1.0 on a single crypto asset net of 0.6%/1.2% per-order fees is not a realistic target. At a target of SR = 0.5, your available history supports on the order of **five or six genuinely independent trials, total, across the entire project history.** That is the number your trial registry should be measured against.

### 3b. Power of n = 114 — computed

Derivation (stated so you can replace my assumptions with measured ones): from PF = 0.761 and R:R = 1.75, implied win rate = 30.31%, expectancy = **−0.1666 R**. With observed mean = −0.62%/trade, implied R = **3.722%** of position. Under a clean binary +1.75R/−1R payoff, SD per trade = **1.264 R = 4.704%**, SE = 4.704/√114 = **0.4406%**.

> **Caveat to verify in-repo:** the binary payoff assumption is a *lower bound* on dispersion — real exits gap, fill partially, and land between stop and target. Measure the actual per-trade return SD from `docs/research/artifacts/results.json`. If the true SD is larger, every power number below gets **worse**, and every confidence interval gets **wider**.

**Result 1 — the observed result is not statistically distinguishable from zero:**

```
t = −0.62 / 0.4406 = −1.407,  two-sided p = 0.159
95% CI on per-trade mean:  [−1.484% , +0.244%]
            in R units:    [−0.399R , +0.065R]
        implied PF range:  [ 0.490  ,  1.107 ]
```

**n = 114 at PF 0.761 does not establish that the strategy loses money.** The 95% interval still contains PF up to 1.11. The honest reading is "uninformative in both directions, with a negative point estimate," not "proven unprofitable." (Section Q7 shows how to convert this into a defensible kill decision anyway.)

**Result 2 — power at n = 114 to detect a positive edge, one-sided:**

| True edge | Implied PF | α = 0.05 (single) | α = 0.0025 (Bonf, 20 trials) | α = 0.00025 (Bonf, 200) |
|---|---|---|---|---|
| +0.25%/trade | 1.11 | **14.1%** | 1.3% | 0.2% |
| +0.50%/trade | 1.23 | **30.5%** | 4.7% | 0.9% |
| +0.75%/trade | 1.36 | 52.3% | 13.5% | 3.8% |
| +1.00%/trade | 1.50 | 73.4% | 29.5% | 11.3% |
| +1.50%/trade | 1.82 | 96.1% | 72.5% | 47.0% |

**Result 3 — sample size for 80% power:**

| True edge | α = 0.05 | α = 0.0025 (20 trials) | α = 0.00025 (200 trials) |
|---|---|---|---|
| +0.25%/trade | 2,189 trades (**97 yr** at 22.6 trades/yr) | 4,714 (208 yr) | 6,616 (293 yr) |
| +0.50%/trade | 547 trades (**24 yr**) | 1,178 (52 yr) | 1,654 (73 yr) |
| +1.00%/trade | 137 trades (6.0 yr) | 295 (13 yr) | 413 (18 yr) |
| +1.50%/trade | 61 trades (2.7 yr) | 131 (5.8 yr) | 184 (8.1 yr) |

**Result 4 — Sharpe framing.** 114 trades / 5.04 yr = 22.6 trades/yr; per-trade Sharpe −0.1318 → annualized ≈ **−0.627**. Years of data needed for 80% power at α = 0.05 one-sided: **SR 0.3 → 69 yr; SR 0.5 → 25 yr; SR 0.75 → 11 yr; SR 1.0 → 6.2 yr; SR 1.5 → 2.7 yr.**

**Bottom line on Q3:** at your trade frequency, a sample of n = 114 has meaningful power only against edges of roughly **+1%/trade or larger (PF ≳ 1.5)**, and essentially none once any multiple-testing correction is applied. Any edge small enough to be plausible after 0.6%/1.2% per-order fees is an edge this sample size **structurally cannot detect**. Increasing sample length is not a fix on any human timescale at 22.6 trades/year — the only lever that changes the arithmetic is **more independent trade events per unit time** (more assets, higher frequency, or pooled cross-sectional inference), and that trades directly against the fact that the frozen mechanism already transfers negatively across assets.

---

## Q4. Walk-forward analysis: does it prevent overfitting or relocate it?

### 4a. The strongest documented result — mining over the split point

**Primary:** Hansen & Timmermann, "Choice of Sample Split in Out-of-Sample Forecast Evaluation," EUI Working Paper ECO 2012/10 / CREATES RP 2012-43. https://rady.ucsd.edu/_files/faculty-research/timmermann/samplesplitmining2012_feb07.pdf

This is the single most directly applicable result to your question and it is quantitative:

- **"tests of predictive accuracy for a model with one additional parameter conducted at the nominal 5% level, but conducted at all split points between 10% and 90% of the sample, reject 15% of the time, i.e., three times as often as they should."**
- Distortion **grows** with the dimension of the prediction model.
- Distortion is **worst at the sample ends**: "we find p_min(0.8,0.9) ≤ 0.05 with a probability that exceeds 10%" — i.e. *even a modest* amount of split-point mining near the end of the sample over-rejects badly. Under the null the smallest p-value is most likely to fall between **80% and 90% of the data**.
- **Their split-mining-adjusted critical values (Table 3, k = dimension excess):**

| k | α=20% | α=10% | α=5% | α=1% |
|---|---|---|---|---|
| 1 | 0.073 | 0.029 | **0.013** | 0.001 |
| 2 | 0.059 | 0.024 | 0.011 | 0.001 |
| 3–5 | 0.050–0.044 | 0.021–0.020 | 0.001 | 0.001 |

  Read: **to get a true 5% test when the split point was chosen after seeing data, your minimum p-value across split points must be below ~0.013** (and below 0.001 for richer models); for a true 1% test, below **0.001**.

Rating: **A.**

### 4b. Out-of-sample testing is *lower powered*, not *safer*

**Primary:** Inoue & Kilian, "In-Sample or Out-of-Sample Tests of Predictability: Which One Should We Use?", *Econometric Reviews* 23(4):371–402, 2004. https://www.tandfonline.com/doi/abs/10.1081/ETC-200040785 — open WP: https://igier.unibocconi.eu/sites/default/files/media/attach/011002.pdf

Conclusion: *neither* data mining *nor* parameter instability explains why in-sample tests reject more often than out-of-sample tests. The explanation is simply the **higher power of in-sample tests**, and therefore "results of in-sample tests of predictability will typically be more credible than results of out-of-sample tests." **The common belief that splitting the sample buys protection against data mining is not supported.** It buys a loss of power. Rating: **A.**

Harvey & Liu (2014) reach the same conclusion from the practitioner side, verbatim: *"often the OOS period is not really out-of-sample because the researcher knows what has happened in that period"*; *"with fewer observations in the in-sample period, we might not have enough power to identify true strategies"*; *"with only 60 monthly observations in the OOS period, a true strategy will have a good chance to fail the OOS test."*

### 4c. The accepted view on reused windows — this is unambiguous

**Primary:** Arnott, Harvey & Markowitz, "A Backtesting Protocol in the Era of Machine Learning," *Journal of Financial Data Science* 1(1):64–74, 2019. https://people.duke.edu/~charvey/Research/Published_Papers/P138_A_backtesting_protocol.pdf

Two direct quotations, both decisive for your question:

> **"Researchers have lived through the hold-out sample and thus understand the history, are knowledgeable about when markets rose and fell, and associate leading variables with past experience. As such, no true out-of-sample data exist; the only true out of sample is the live trading experience."**

> **"Recognize That Iterated Out of Sample Is Not Out of Sample.** Suppose a model is successful in the in-sample period but fails out of sample. The researcher observes that the model fails for a particular reason. The researcher modifies the initial model so it then works both in sample and out of sample. **This is no longer an out-of-sample test. It is overfitting.**"

And on the fallback of "just use a different market": *"a data-mined (and potentially fake) anomaly that works in the US market over a certain sample may also work in Canada or the United Kingdom over the same time span, given the correlation between these markets."*

**Applied to your repaired walk-forward tool:** your CLAUDE.md already records the correct conclusion — "its windows are in the registry's multiple-testing budget, so it is not clean OOS and cannot support activation." **That position is exactly what the literature supports.** Arnott/Harvey/Markowitz on window reuse, plus Hansen–Timmermann on the quantified size distortion from split-point mining, plus Inoue–Kilian on the absent power benefit, together mean: a walk-forward over previously examined data is a **diagnostic of internal consistency**, not evidence about future performance. Do not revisit that decision.

### 4d. On CPCV as the proposed replacement — flag the circularity

**Primary:** Arian, Norouzi Mobarekeh & Seco, "Backtest overfitting in the machine learning era: A comparison of out-of-sample testing methods in a synthetic controlled environment," *Knowledge-Based Systems* 305:112477, 2024. https://dl.acm.org/doi/10.1016/j.knosys.2024.112477 / https://www.ssrn.com/abstract=4778909

Finds CPCV superior to K-Fold, Purged K-Fold and especially Walk-Forward, in a synthetic environment (Heston stochastic vol + Merton jump diffusion + Markov regime switching).

**Flag:** the paper ranks methods **using PBO and DSR as the scoring criteria**. PBO, DSR and CPCV all originate from the same research programme (López de Prado). Judging CPCV the winner by its own family's metrics, on synthetic data generated under fully known models, is **suggestive, not independent validation.** The walk-forward criticism (single historical path, high temporal variability) is sound on its own terms; the ranking is not evidence that CPCV controls Type I error on real data. Rating: **B**, with the circularity flag.

### 4e. The classic data-snooping-robust alternatives (for completeness)

- **White, "A Reality Check for Data Snooping," *Econometrica* 68(5):1097–1126, 2000.** https://onlinelibrary.wiley.com/doi/abs/10.1111/1468-0262.00152 — bootstrap test of "best model in a specification search has no superiority over the benchmark." Rating **A**. Documented weakness: **conservative** when the universe contains many poor/irrelevant models.
- **Hansen, "A Test for Superior Predictive Ability," *JBES* 23(4):365–380, 2005.** Re-centres the null distribution to restore power against White's conservatism. Rating **A**.
- **Politis & Romano, "The Stationary Bootstrap," *JASA* 89(428):1303–1313, 1994.** https://www.tandfonline.com/doi/abs/10.1080/01621459.1994.10476870 — geometric (random) block length, so resamples are stationary, unlike fixed-block. Rating **A**. Block length via **Politis & White, "Automatic Block-Length Selection for the Dependent Bootstrap," *Econometric Reviews* 23(1):53–70, 2004** — https://public.econ.duke.edu/~ap172/Politis_White_2004.pdf. Rating **A**.
- Applied example: Kuan et al., "Re-Examining the Profitability of Technical Analysis with White's Reality Check and Hansen's SPA Test" — https://homepage.ntu.edu.tw/~ckuan/pdf/snoop01.pdf.

These are the best-validated tools in the whole review (genuine econometric theory with asymptotic guarantees) but they require a **universe of rules** evaluated on a common sample — same structural requirement as CSCV, and same problem at N = 1.

---

## Q5. Pre-registration in quantitative finance

### 5a. Does an analogue of clinical-trial pre-registration exist? — **Only as an advocacy position, not as institutionalized practice.**

**Primary:** Harvey, "Presidential Address: The Scientific Outlook in Financial Economics," *Journal of Finance* 72(4):1399–1440, 2017. https://people.duke.edu/~charvey/Research/Published_Papers/P131_The_scientific_outlook.pdf — argues finance's incentive structure (competition for top-journal space, unreported tests, no multiple-testing adjustment, direct and indirect p-hacking) makes most published results unlikely to hold up; proposes Bayesianized p-values / Minimum Bayes Factors and a move toward scientific protocol. Rating: **A** (as a peer-reviewed AFA presidential address; note it is an *argument*, not an empirical test of pre-registration).

**Primary:** Arnott, Harvey & Markowitz (2019), the **Seven-Point Protocol**. This is the closest thing to a pre-registration checklist that exists in this field, and it is explicitly modelled on clinical protocols and Gawande's *Checklist Manifesto*. Decoded verbatim from the PDF's custom font encoding:

> **1. Research Motivation** — (a) Does the model have a solid economic foundation? (b) **Did the economic foundation or hypothesis exist before the research was conducted?**
> **2. Multiple Testing and Statistical Methods** — (a) Did the researcher keep track of all models and variables that were tried, both successful and unsuccessful, and are the researchers aware of the multiple testing issue? (b) Is there a full accounting of all possible interaction variables? (c) Did the researchers investigate all variables set out in the research agenda, or did they cut the research as soon as they found a good model?
> **3. Data and Sample Choice** — (a) Do the data chosen make sense, and is it reasonable to exclude other available data? (b) Steps to ensure data integrity? (c) **Were data transformations selected in advance?** Robust to minor changes? (d) Are outlier-exclusion rules reasonable? (e) **Was the winsorization rule chosen before the research was started?** Was only one tried?
> **4. Cross-Validation** — (a) Are the researchers aware that **true out-of-sample tests are only possible in live trading**? (b) Are steps in place to eliminate the risk of out-of-sample iterations? (c) Is the out-of-sample analysis representative of live trading — trading costs and data revisions taken into account?
> **5. Model Dynamics** — (a) Resilient to structural change; steps taken to minimize overfitting of the model *dynamics*? (b) Overcrowding risk in live trading? (c) Steps to minimize tweaking of a live model?
> **6. Complexity** — (a) Curse of dimensionality? (b) Simplest practicable specification? (c) Has an attempt been made to interpret the model's predictions rather than using it as a black box?
> **7. Research Culture** — (a) Does the culture reward the quality of the science rather than the finding of a winning strategy? (b) **Do the researchers and management understand that most tests will fail?** (c) Are expectations clear that researchers should seek the truth, not just something that works?

Rating: **A** for authority and adoption; **C** for *evidence that it works* — see 5c.

Also relevant, the "Define the Test Sample Ex Ante" rule: *"The training sample needs to be justified in advance. The sample should never change after the research begins. For example, suppose the model 'works' if the sample begins in 1970 but does not work if the sample begins in 1960 — in such a case, the model does not work."*

### 5b. The infrastructure proposal

López de Prado (JFDS 2019) proposes the direct clinical-trials analogue: **"journals share trials among themselves, in order to build a trials repository, like medical journals did with www.alltrials.net"** and that journals publish or at least archive negative results. He notes the asymmetry that favours you: *"Financial firms can legally enforce their right to record all trials used in selecting a strategy … There is no such thing as 'publication bias' when a firm records all trials ever conducted."*

**This is exactly what your `docs/trial_registry.md` + content-addressed artifacts already implement.** In the literature's own terms, a private research programme with a complete trial ledger is in a *better* position than academic finance, because the file-drawer problem — the acknowledged Achilles heel of PBO (§5.2 of the PBO paper) and of DSR (V[{SR_n}] estimation) — is eliminated by construction rather than by trust.

### 5c. Is there evidence pre-registration helps? — **In finance, no. Outside finance, partial and contested.**

I found **no study measuring whether pre-registration improves out-of-sample performance of trading strategies.** The case in finance rests entirely on analogy to medicine and on the theoretical argument that multiple-testing corrections are only computable if the denominator is known. That is a strong *logical* argument — the DSR literally cannot be evaluated without N — but it is **not empirical validation**, and it should be labelled as such. Rating: **C for efficacy evidence, A for logical necessity.**

The cross-domain evidence for pre-registration comes from the reproducibility literature outside finance (Open Science Collaboration 2015 and successors), where results are positive but confounded and disputed. Nothing in that literature measures profit.

---

## Q6. Look-ahead and warm-up leakage; bug taxonomies

### 6a. The best systematic taxonomy — and it is not from finance

**Primary:** Kapoor & Narayanan, "Leakage and the Reproducibility Crisis in Machine-Learning-Based Science," *Patterns* 4(9):100804, 2023. https://arxiv.org/abs/2207.07048

The scale finding, verbatim from the abstract: *"we find **17 fields** where errors have been found, collectively affecting **329 papers** and in some cases leading to wildly overoptimistic conclusions."*

**Full taxonomy of 8 leakage types, transcribed:**

- **[L1] Lack of clean separation of training and test dataset**
  - **L1.1 No test set** — same data for training and testing.
  - **L1.2 Pre-processing on training and test set** — e.g. imputation or over/under-sampling computed on the full dataset before splitting.
  - **L1.3 Feature selection on training and test set** — using test-set performance to decide which features to include.
  - **L1.4 Duplicates in datasets.**
- **[L2] Illegitimate features** — a feature that is a proxy for, or unavailable before, the outcome. No sub-categories: *"researchers decide which features are suitable for a modeling task and justify their choice using domain expertise."*
- **[L3] Test set is not drawn from the distribution of scientific interest**
  - **L3.1 Temporal leakage** — *"When an ML model is used to make predictions about a future outcome of interest, the test set should not contain any data from a date before the training set."*
  - **L3.2 Nonindependence between train and test samples.**
  - **L3.3 Sampling bias in test distribution** — spatial bias, selection bias.

**Case study result:** across all four civil-war-prediction papers claiming complex ML beats decades-old logistic regression, **every one failed to reproduce once leakage was corrected**, and *"none of these errors could have been caught by reading the papers."* Their proposed remedy is the **model info sheet** — a structured, per-claim disclosure form that would have detected leakage in every case.

Rating: **A.** This is the closest thing to a rigorous, quantified bug taxonomy in existence, and it is more careful than anything the finance literature has produced.

**Where your warm-up bug sits in this taxonomy:** your scanner *failed open* when an indicator was still warming up, so a declared gate silently did not apply. That is **not** L3.1 temporal leakage in the textbook sense (no future data was read). It is closest to **L2 (illegitimate features / feature-availability violation)** combined with a **silent-default control-flow defect** — the model's effective specification differed from its declared specification on a subset of observations. **The Kapoor–Narayanan taxonomy does not have a category for this**, and neither does the finance literature. That is a real gap, and it is worth noting that your fix — fail *closed* on any uncomputable gate — is the correct general principle and is not written down anywhere I could find. Your repo's Phase 6.8 rule ("`_check_entry_filters` fails closed on every unreadable input") is, as far as this review can determine, **ahead of the published guidance.**

### 6b. Standard practice for indicator warm-up specifically — **there is essentially none published**

I searched for a documented standard and did not find one. What exists:

- **The general principle (correct but not warm-up-specific):** Arnott/Harvey/Markowitz protocol 4c — the out-of-sample analysis must be *representative of live trading*, including data revisions. If a filter could not have been computed live, the backtest must not act as though it were satisfied.
- **The closest quantified analogue** is the Deutsche Bank survivorship/look-ahead work (below), which shows this class of defect can *reverse the sign* of a result, not merely inflate it.

**The operational rule that follows and that I would treat as the standard, stated explicitly because the literature does not state it:** every declared gate must return one of three typed states — `PASS`, `BLOCK`, `UNAVAILABLE` — and `UNAVAILABLE` must be treated as `BLOCK`, never silently as `PASS`. Your funding-rate filter already has exactly this typed applicable/not-applicable/unavailable contract (Phase 6.8); the warm-up bug was the same class of defect in a filter that lacked it. **The generalizable lesson from your own incident is: any filter without a typed unavailability contract is a warm-up bug waiting to happen** — and that is a repo-wide invariant worth asserting in tests, not a one-off fix.

### 6c. Quantified evidence that this class of bug inflates results

**Primary:** Luo et al., "Seven Sins of Quantitative Investing," Deutsche Bank Markets Research, 8 September 2014. https://hudsonthames.org/wp-content/uploads/2022/01/DB-201409-Seven_Sins_of_Quantitative_Investing.pdf

The seven sins: **(1) survivorship bias, (2) look-ahead bias, (3) storytelling bias, (4) data mining / data snooping, (5) turnover and transaction costs, (6) outliers, (7) asymmetric payoff pattern and shorting cost.**

The quantified results are about **sign reversal**, not inflation:
- **Merton distance-to-default factor:** on the correct point-in-time Russell 3000 universe, high-credit-risk Q1 underperforms. On the "survivor universe" (stocks in the index on 1986-12-31 that survived to the present), **the ranking reverses completely** — worst-quality companies appear to be the best performers.
- **Low-volatility factor:** on the correct point-in-time S&P 500 universe, low-vol outperforms (the documented anomaly). On current index constituents, **"we see exactly the opposite — high volatility stocks have outperformed low volatility stocks by 16x."**

Rating: **C** (sell-side research, not peer-reviewed) but the demonstrations are reproducible and the mechanism is transparent.

**Primary:** Löw, Maier-Paape & Platen, "Correctness of Backtest Engines," arXiv:1509.08248, RWTH Aachen. https://arxiv.org/abs/1509.08248 — the only formal work I found on *verifying a backtest engine itself*. Abstract: *"The construction of a correct working backtest engine is, however, a subtle task … **Several platforms are struggling on the correctness.**"* They construct model candles and intra-period price models such that passing a specified battery of tests constitutes a **proof of correctness** for a given engine. Rating: **B.** This is the right idea for your situation — property-based tests on synthetic candles with known correct answers, rather than only regression tests against previous outputs.

**Summary for Q6:** the finance literature has **no systematic bug taxonomy**. The best taxonomy is Kapoor & Narayanan's from ML-based science (8 types, 329 papers, 17 fields). Neither covers silent-default filter bugs. The frequency of this class of error in finance is **anecdotally universal and never systematically measured** — I could find no paper estimating what fraction of published backtests contain implementation defects.

---

## Q7. When to declare a strategy dead

### 7a. There is almost no literature on stopping rules for *research programmes*

This was the thinnest area of the search by a wide margin. Academic finance has essentially nothing on "how much negative evidence is enough to stop researching an idea." What exists falls into four buckets:

**(1) Optimal stopping for the *search* — the secretary problem.** The DSR paper has a section titled "WHEN SHOULD WE STOP TESTING?" and its answer is the 1/e-law of optimal choice (Bruss 1984):

> *"From the set of strategy configurations that are theoretically justifiable, sample a fraction 1/e of them (roughly 37%) at random and measure their performance. After that, keep drawing and measuring the performance of additional configurations from that set, one by one, until you find one that beats all of the previous. That is the optimal number of trials."*

The rationale: *"every additional trial irremediably increases the probability of a false positive."* Rating: **C** — this is an analogy, and the authors concede the secretary problem's no-recall assumption doesn't hold. It is useful mainly as a *pre-commitment device*: it forces you to enumerate the theoretically justifiable configuration set **before** searching it, which is the pre-registration discipline in another guise.

**(2) Stopping a *live* strategy — drawdown-based.** Bailey & López de Prado, "Stop-Outs under Serial Correlation and the Triple Penance Rule," *Journal of Risk* 18(2):61–93. https://www.davidhbailey.com/dhbpapers/stop-out.pdf — under standard portfolio theory assumptions, **recovery from maximum drawdown takes three times as long as the drawdown took to accumulate**, at the same confidence level. Also: ignoring serial correlation **underestimates downside potential by as much as 70%**. Rating: **B**. Note this answers "when to stop *trading*," not "when to stop *researching*."

**(3) Sequential/anytime-valid testing — the right statistical machinery, rarely applied in finance.** Grünwald, de Heide & Koolen, "Safe Testing," *JRSS-B* 86(5):1091–1128, 2024. https://academic.oup.com/jrsssb/article/86/5/1091/7623686. E-values preserve Type-I error **under optional continuation** — you can keep accumulating shadow trades and look at the evidence whenever you like without inflating false-positive rates, which p-values do not permit. Growth-Rate-Optimality (GRO) is the power analogue. Rating: **A** for the statistics; **C** for any evidence of application to strategy research (I found none).

**(4) Equivalence testing — and this is the tool that actually answers your question.** Lakens, "Equivalence Tests: A Practical Primer for t Tests, Correlations, and Meta-Analyses," *Social Psychological and Personality Science* 8(4):355–362, 2017. https://journals.sagepub.com/doi/full/10.1177/1948550617697177. Rating: **A**.

### 7b. Why equivalence testing is the right frame, applied to your numbers

The core error the field makes — Lakens's central point — is that *"researchers often incorrectly conclude an effect is absent based on a nonsignificant result."* Absence of evidence is not evidence of absence. The fix is **TOST**: pre-specify a **SESOI** (smallest effect size of interest) and test against *that* bound, not against zero.

**In your setting the SESOI is not a statistical choice — it is an economic one.** The strategy must clear 0.6% maker / 1.2% taker per order, two orders per round trip, before it is worth running at all. That sets a natural SESOI floor.

**Computed on your sample (mean = −0.62%, SE = 0.4406%, n = 114):**

| Pre-specified SESOI | Implied PF | one-sided p | Verdict at α = 0.05 |
|---|---|---|---|
| +0.00%/trade | 1.000 | 0.0797 | **cannot reject** |
| +0.10%/trade | 1.043 | 0.0511 | cannot reject |
| **+0.11%/trade** | **1.047** | **0.0488** | **REJECT** |
| +0.25%/trade | 1.110 | 0.0242 | **REJECT** |
| +0.50%/trade | 1.229 | 0.0055 | **REJECT** |
| +1.00%/trade | 1.500 | 0.0001 | **REJECT** |

One-sided 95% upper confidence bound on the true per-trade mean: **+0.105%/trade, i.e. PF 1.045.**
One-sided 99% upper bound: **+0.405%/trade.**

**This is the decisive methodological result of the whole review.** A conventional significance test on n = 114 says *nothing* (p = 0.159, "cannot reject zero"). **The equivalence test says a great deal: the data reject, at one-sided 5%, any true edge at or above PF ≈ 1.05.** Given that the round-trip fee burden alone consumes well over 1% of position value, an edge capped at PF 1.045 is **economically dead even under its own most favourable 95% bound**.

So: *"underpowered to prove the strategy is unprofitable"* and *"sufficient to rule out any profitable version of it"* are **both true simultaneously**, and only the second one is decision-relevant. The literature's failure to make this distinction is why practitioners keep strategies alive on "it wasn't statistically significant, so we don't know yet."

### 7c. Guarding against sunk-cost continuation

The literature offers three concrete mechanisms, all of which are pre-commitments made **before** the evidence arrives:

1. **Pre-specify the SESOI and the kill rule in the trial registry entry, before the trial runs.** Post-hoc SESOI selection is p-hacking with extra steps.
2. **Arnott/Harvey/Markowitz, protocol item 7b: "Do the researchers and management understand that most tests will fail?"** and 5c: *"Do researchers take steps to minimize the tweaking of a live model?"* — with the explicit warning: *"It may be tempting to tweak the model, especially as a means to improve its fit in recent, now in-sample, data. Although these modifications are a natural response to failure, we should be fully aware that they will generally lead to further overfitting of the model and may lead to even worse live-trading performance."*
3. **Bailey et al.'s irreversibility principle:** *"the counter of trials cannot be turned back."* Every resurrection attempt on a retired idea increments N permanently and raises the bar for every *future* claim in the same programme. Your existing rule — that reviving V3 would require a new pre-registered trial ID rather than reuse of the old one — is the correct implementation of this and matches the literature exactly.

There is also a substantive finding that bears on "how negative is negative enough." Bailey et al. (2014, AMS) show that **in the presence of memory in the performance series, overfitting produces *negative* expected OOS performance, not merely zero.** In a memoryless process, an overfit strategy's OOS expectation is zero; with memory, it is significantly negative. So a persistently negative OOS result is the *predicted signature of overfitting*, not an unlucky draw — which is a reason to weight negative evidence more heavily than a symmetric-null intuition would suggest.

---

## What the evidence does NOT establish

Stated as flatly as I can, because every one of these is a place where the literature is routinely over-read:

1. **No published study establishes that DSR or PBO achieves nominal Type I error control on real financial data.** Both have been validated only on synthetic data, and in DSR's case only the extreme-value approximation in Eq. (1) was checked, not the full statistic. They are principled adjustments, widely adopted, **weakly validated**.

2. **No method exists for reliably counting N when trials were informal.** Four approaches are proposed (exhaustive logging, truncated-distribution recovery, clustering, counterfactual reconstruction). Only the first is sound, and it only works prospectively. The clustering claim that omitted trials "fold onto existing clusters" is **asserted, never demonstrated.** Any DSR you compute retrospectively on this project is a function of an unverifiable N.

3. **The t > 3.0 hurdle is contested and domain-specific.** Harvey/Liu/Zhu derive it for US equity cross-sectional factors with ~316 observed and ~1,300 estimated trials. Chen (2025) shows their own methods imply FDR ≈ 9%, and that the pessimistic reading comes from an interpretive choice. **Neither result transfers to single-asset crypto technical rules**, because both depend on a domain base rate that has never been measured there. Do not import 3.0 as if it were a constant of nature — and do not import Chen's optimism either.

4. **MinBTL is a necessary, non-sufficient condition** — the authors say so explicitly. Satisfying it does not mean the backtest isn't overfit. It also assumes IID Gaussian returns, *independent* trials, and a true OOS Sharpe of exactly zero. Crypto returns violate the first assumption badly, which makes the bound optimistic in an unquantified direction.

5. **No evidence exists that walk-forward analysis prevents overfitting.** The documented findings run the other way: split-point mining triples the nominal rejection rate (Hansen–Timmermann), out-of-sample tests have *lower* power without compensating protection against data mining (Inoue–Kilian), and iterated out-of-sample is definitionally overfitting (Arnott/Harvey/Markowitz). The claim that CPCV is superior comes from a study that scores methods using metrics from the same research programme that produced CPCV.

6. **No study measures whether pre-registration improves realized strategy performance.** The case is by analogy to medicine plus the logical point that multiple-testing corrections are uncomputable without a known denominator. That logical point is airtight; the efficacy evidence is absent.

7. **No systematic study of backtest bug taxonomies exists in finance.** The best taxonomy (Kapoor & Narayanan, 8 leakage types, 329 papers, 17 fields) is from ML-based science and covers no finance venues. **No one has measured how often warm-up/silent-default bugs occur or how much they inflate results** — your +22.28% over 19 ungated trades is, as far as I can tell, better quantified than anything in the published record.

8. **There is no accepted stopping rule for abandoning a research programme.** What exists is drawdown-based stop-outs for *live* strategies, the secretary-problem heuristic for stopping a *search*, and general sequential-testing theory (e-values) that nobody has applied to this problem. The equivalence-testing framing in §7b is standard statistics correctly applied — but it is **my application**, not a citation; no finance paper frames strategy retirement as a TOST.

9. **n = 114 does not establish that your strategy loses money.** 95% CI on per-trade mean: [−1.484%, +0.244%], implied PF in [0.49, 1.11]. Anyone citing PF 0.761 as proof of a negative edge is over-reading the sample. What the sample *does* establish — and this is the usable result — is that **any true edge is capped at roughly PF 1.05 at one-sided 95% confidence**, which is below the level at which the fee structure makes trading worthwhile.

10. **My power and equivalence numbers rest on one derived quantity I could not measure: the per-trade return standard deviation (4.704%), inferred from PF, R:R and mean under a clean binary ±R payoff.** Real exits gap and fill partially, so true dispersion is almost certainly **higher**, which would widen every interval and lower every power figure. **Recommend measuring per-trade return SD directly from `docs/research/artifacts/results.json` and re-running the four calculations before any of these figures is cited as a project number.** The direction of the error is known and unfavourable; the magnitude is not.

---

## Source index by rating

**A — peer-reviewed, foundational, well-established:**
- Harvey, Liu & Zhu (2016), *RFS* 29(1):5–68 — https://academic.oup.com/rfs/article/29/1/5/1843824 · https://www.nber.org/system/files/working_papers/w20592/w20592.pdf
- Chen (2025), arXiv:2206.15365 — https://arxiv.org/abs/2206.15365
- Harvey (2017), *JF* 72(4):1399–1440 — https://people.duke.edu/~charvey/Research/Published_Papers/P131_The_scientific_outlook.pdf
- Arnott, Harvey & Markowitz (2019), *JFDS* 1(1):64–74 — https://people.duke.edu/~charvey/Research/Published_Papers/P138_A_backtesting_protocol.pdf
- Hansen & Timmermann (2012) — https://rady.ucsd.edu/_files/faculty-research/timmermann/samplesplitmining2012_feb07.pdf
- Inoue & Kilian (2004), *Econometric Reviews* 23(4) — https://igier.unibocconi.eu/sites/default/files/media/attach/011002.pdf
- White (2000), *Econometrica* 68(5) — https://onlinelibrary.wiley.com/doi/abs/10.1111/1468-0262.00152
- Hansen (2005), *JBES* 23(4)
- Politis & Romano (1994), *JASA* 89(428) — https://www.tandfonline.com/doi/abs/10.1080/01621459.1994.10476870
- Politis & White (2004), *Econometric Reviews* 23(1) — https://public.econ.duke.edu/~ap172/Politis_White_2004.pdf
- Kapoor & Narayanan (2023), *Patterns* 4(9) — https://arxiv.org/abs/2207.07048
- Grünwald, de Heide & Koolen (2024), *JRSS-B* 86(5) — https://academic.oup.com/jrsssb/article/86/5/1091/7623686
- Lakens (2017), *SPPS* 8(4) — https://journals.sagepub.com/doi/full/10.1177/1948550617697177

**B — peer-reviewed / influential, method itself not independently validated:**
- Bailey & López de Prado (2014), DSR, *JPM* 40(5) — https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551 · https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf
- Bailey, Borwein, López de Prado & Zhu (2016), PBO, *J. Computational Finance* 20(4) — https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253 · https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf
- Bailey et al. (2014), MinBTL, *Notices of the AMS* 61(5) — https://www.ams.org/notices/201405/rnoti-p458.pdf · https://www.davidhbailey.com/dhbpapers/backtest-pseudo.pdf
- Bailey & López de Prado (2012), PSR/MinTRL, *Journal of Risk* 15(2) — https://www.davidhbailey.com/dhbpapers/sharpe-frontier.pdf
- Harvey & Liu (2015), "Backtesting," *JPM* 42(1) — https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2345489 · code https://people.duke.edu/~charvey/backtesting/
- Harvey & Liu (2014), "Evaluating Trading Strategies," *JPM* 40(5) — https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2474755
- Bailey & López de Prado, Triple Penance, *Journal of Risk* 18(2) — https://www.davidhbailey.com/dhbpapers/stop-out.pdf
- Löw, Maier-Paape & Platen (2015), arXiv:1509.08248 — https://arxiv.org/abs/1509.08248
- Arian, Norouzi & Seco (2024), *Knowledge-Based Systems* 305 — https://dl.acm.org/doi/10.1016/j.knosys.2024.112477 *(circularity flag, §4d)*

**C — practitioner / unvalidated:**
- López de Prado (2019), *JFDS* 1(1):99–110 — https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3177057
- Luo et al. (2014), Deutsche Bank Seven Sins — https://hudsonthames.org/wp-content/uploads/2022/01/DB-201409-Seven_Sins_of_Quantitative_Investing.pdf

**D — used only for formula restatement, not as evidence:** ml4trading.io DSR page, marti.ai DSR post, portfoliooptimizationbook.com ch. 8.2/8.3.

---

## Three things this review would change about the project's current practice

Offered as method only, with no view on any strategy:

1. **Replace the significance framing with an equivalence framing in the trial registry.** Every trial entry should carry a pre-registered SESOI (economically derived from the fee schedule) and a kill rule stated as "we will reject the hypothesis of an edge ≥ SESOI if the one-sided upper bound falls below it." This converts underpowered samples from "inconclusive forever" into decidable. §7b shows it already decides your current case.

2. **Record per-trial return *series*, not just summary statistics, in the research artifacts.** DSR needs `V[{SR_n}]` and CSCV needs a synchronous `(T × N)` P&L matrix. Neither is reconstructible from PF and n alone. This costs nothing now and is the only thing that makes any multiple-testing correction computable later — and it is the one advantage private research has over academia (López de Prado 2019).

3. **Assert the typed-availability contract as a repo-wide invariant, not a per-filter fix.** The warm-up bug and the funding-rate contract are the same defect class. A test that enumerates every declared gate and fails if any lacks a three-state `PASS`/`BLOCK`/`UNAVAILABLE` return would have caught the 200-day EMA bug before it cost 19 trades. This is the one place where your practice is ahead of the literature, and it should be encoded rather than remembered.
