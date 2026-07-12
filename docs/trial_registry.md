# Trial Registry — ZEC Momentum Strategy

All hyperparameter searches, asset tests, and model variants explored before the V2 freeze.
Any positive result must be interpreted in light of this registry (multiple testing).

OOS start: **2026-07-12** (git tag `v2-adx25-frozen`, commit `f06881d`)
Frozen config: ZEC-USD only, ADX=25, atr_stop=2.0, atr_target=3.5, btc_regime_filter=False

---

## Assets tested

| Asset    | Period        | Result        | Decision   |
|----------|---------------|---------------|------------|
| ZEC-USD  | recent_year   | PF=1.32 ✅   | Live (V2)  |
| ETH-USD  | recent_year   | not run (disabled after ATR bug) | Disabled |
| LINK-USD | recent_year   | PF=0.25 ❌   | Rejected   |
| ATOM-USD | recent_year   | PF=0.81 ❌   | Rejected   |
| AVAX-USD | recent_year   | PF=0.40 ❌   | Rejected   |
| DOT-USD  | recent_year   | PF=0.61 ❌   | Rejected   |

## ADX threshold

| ADX   | Period       | WR   | Avg P&L | Notes             |
|-------|-------------|------|---------|-------------------|
| 20    | full_year   | ~40% | -0.56%  | V1 baseline       |
| 25    | full_year   | ~40% | -0.55%  | Marginal improvement |
| 25    | live_period | 75%  | +4.42%  | 4 trades — not OOS |

ADX=25 was selected after observing the live_period. That period is now IS, not OOS.

## Stop / target multipliers

| atr_stop | atr_target | R:R   | Tested on | Result                    |
|----------|-----------|-------|-----------|---------------------------|
| 2.0      | 3.5       | 1.75  | ZEC full_year | -0.77% avg           |
| 2.5      | 4.5       | 1.80  | ETH (disabled) | n/a                 |

## BTC regime filter

| Config                  | Period      | Impact                        |
|------------------------|-------------|-------------------------------|
| btc_regime_filter=True  | live_period | Blocked all Jun 2026 ZEC signals (ZEC +30% while BTC below EMA) |
| btc_regime_filter=False | live_period | Passed all 30 scanner signals |

Decision: ZEC is decorrelated from BTC during its breakout regimes. Filter disabled for ZEC.

## Fee model iterations

| Maker  | Taker  | Notes                                      |
|--------|--------|--------------------------------------------|
| 0.2%   | 0.4%   | V1 — wrong, inflated backtest P&L          |
| 0.4%   | 0.6%   | V2 — correct Coinbase Advanced base tier   |

## Data source

| Source  | Status | Notes                                              |
|---------|--------|----------------------------------------------------|
| yfinance | Limited | 730-day window for 1h data; cannot reproduce Jul 2024 or earlier |
| Coinbase API | Planned | Exact exchange data, paginated, immutable dataset |

## Periods used

| Period name       | Warmup       | Test window         | Used for              |
|-------------------|-------------|---------------------|-----------------------|
| full_year         | 2024-07-12  | ~Jul-Dec 2024       | Initial development (now outside yfinance window) |
| recent_year       | 2025-04-01  | Jul 2025 – Jul 2026 | ADX comparison, asset selection |
| live_period       | 2026-04-01  | Jun – Jul 2026      | ADX=25 selection ← makes this IS |
| mid_year_holdout  | 2024-07-14  | Aug 2024 – May 2025 | Retrospective holdout (not used in any tuning) |

## Approximate trial count

Counting distinct (asset, ADX, stop/target, fee_model, period) combinations tested: ~15-20.
For proper Deflated Sharpe Ratio correction, log new trials as they occur.
