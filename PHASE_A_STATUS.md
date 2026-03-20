# Phase A Status

## Summary

Phase A is complete in a narrow, honest sense:

- The project now has a deterministic backtesting baseline.
- BTC and ETH are treated as separate strategy tracks.
- ETH is the primary Phase A candidate.
- The strategy behaves like a selective medium-term system, not a frequent short-window system.

## Current Baseline

### BTC

- Strict local `1h close >= ema50`
- Strict MACD crossover
- Conservative volume filter
- Result on `365d`: about `-0.06%`, `57.1%` win rate, `1.17` profit factor

### ETH

- Strict local `1h close >= ema50`
- Refined MACD momentum confirmation
- `min_volume_ratio = 1.00`
- ATR trailing exit with:
  - `trail_multiplier = 2.0`
  - `activation_multiplier = 1.2`
- Result on `365d`: about `-0.06%`, `57.1%` win rate, `1.34` profit factor

## What Worked

- Per-symbol configuration
- Treating ETH as the stronger candidate
- ETH refined MACD logic
- ETH volume filter loosened from `1.05` to `1.00`
- Better diagnostics and multi-window validation

## What Did Not Work

- Removing or buffering the local `1h ema50` protection too much
- Broadly loosening MACD for BTC
- Reintroducing complexity before proving simpler filters
- ETH recent `4h` trend relaxation did not unlock `90d/180d`

## Honest Constraint

Across repeated tests, `30d`, `60d`, `90d`, and `180d` windows remained inactive.
That means the current strategy is **not** validated as a robust multi-window active system for recent market regimes.

The best-supported interpretation is:

- ETH: selective medium-term candidate
- BTC: stricter benchmark / secondary research track

## Recommended Next Step

Move into a cautious Phase B style workflow for ETH:

- paper trade the current ETH baseline
- monitor live fills, fees, slippage, and latency
- keep BTC frozen unless a clearly new hypothesis appears
