"""
Block Bootstrap Analysis — regime-aware confidence intervals for ZEC strategy.

Standard Monte Carlo resamples individual trades independently.
For a momentum/regime strategy, consecutive trades are correlated:
winning trades cluster in trends, losing trades cluster in chop.
Block bootstrap preserves this dependence by resampling contiguous blocks.

Method: Politis-Romano stationary block bootstrap
  - Block size b=4 (approx mean max-hold duration in signals)
  - N=10,000 resamplings
  - Reports: 95% CI on profit factor and avg P&L

Usage:
    python backtesting/bootstrap_analysis.py
    python backtesting/bootstrap_analysis.py --period recent_year
    python backtesting/bootstrap_analysis.py --period mid_year_holdout
    python backtesting/bootstrap_analysis.py --multi-block   # sweep b=2,4,8,12
    python backtesting/bootstrap_analysis.py --leave-out-event 2025-09 2025-11
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting.signal_scanner import scan_asset, PERIODS


def _block_bootstrap_pf(
    returns: list[float],
    block_size: int = 4,
    n_iter: int = 10_000,
    rng: np.random.Generator | None = None,
) -> np.ndarray:
    """
    Stationary block bootstrap on a return series.
    Returns array of profit_factor values from each resampled sequence.
    """
    if rng is None:
        rng = np.random.default_rng(42)

    arr = np.array(returns, dtype=float)
    n   = len(arr)
    pfs = np.empty(n_iter)

    for i in range(n_iter):
        # Build a resampled sequence of length n using random blocks
        sample: list[float] = []
        while len(sample) < n:
            start = rng.integers(0, n)
            block = arr[start : start + block_size]
            if len(block) < block_size and start + block_size > n:
                # wrap around (stationary bootstrap)
                block = np.concatenate([block, arr[: block_size - len(block)]])
            sample.extend(block.tolist())
        sample = sample[:n]

        wins   = [r for r in sample if r > 0]
        losses = [r for r in sample if r <= 0]
        gw = sum(wins)
        gl = abs(sum(losses))
        pfs[i] = gw / gl if gl > 0 else np.inf

    return pfs


def _pct_above_1(returns: list[float], block_size: int, n_iter: int) -> float:
    pfs = _block_bootstrap_pf(returns, block_size=block_size, n_iter=n_iter)
    return float((pfs[np.isfinite(pfs)] > 1.0).mean() * 100)


def run_multi_block(period: str, asset: str = "ZEC-USD", n_iter: int = 10_000) -> None:
    """Sweep block sizes 2, 4, 8, 12 to test stability of P(PF>1)."""
    if period not in PERIODS:
        print(f"Unknown period '{period}'. Available: {list(PERIODS)}")
        sys.exit(1)

    period_cfg = PERIODS[period]
    result     = scan_asset(asset, period_cfg)
    signals    = result.get("signals", [])
    if not signals:
        print("No signals found.")
        return

    returns = [s["trade"]["pnl_pct"] for s in signals]
    print(f"\nBlock-size sensitivity — {asset} / {period}  ({len(returns)} signals)")
    print(f"{'Block':>6}  {'P(PF>1)':>9}")
    print("-" * 20)
    for b in [2, 4, 8, 12]:
        pct = _pct_above_1(returns, block_size=b, n_iter=n_iter)
        print(f"  b={b:>2}     {pct:>6.1f}%")
    print()


def run_leave_one_event_out(
    period: str,
    asset: str = "ZEC-USD",
    exclude_start: str = "2025-09-01",
    exclude_end:   str = "2025-11-30",
    block_size: int = 4,
    n_iter: int = 10_000,
) -> None:
    """Remove an entire date-range cluster and recompute stats + bootstrap."""
    if period not in PERIODS:
        print(f"Unknown period '{period}'. Available: {list(PERIODS)}")
        sys.exit(1)

    period_cfg = PERIODS[period]
    result     = scan_asset(asset, period_cfg)
    signals    = result.get("signals", [])
    if not signals:
        print("No signals found.")
        return

    all_returns = [s["trade"]["pnl_pct"] for s in signals]

    excluded = [
        s for s in signals
        if exclude_start <= s["timestamp"][:10] <= exclude_end
    ]
    kept = [
        s for s in signals
        if not (exclude_start <= s["timestamp"][:10] <= exclude_end)
    ]
    kept_returns = [s["trade"]["pnl_pct"] for s in kept]

    excl_sum = sum(s["trade"]["pnl_pct"] for s in excluded)
    print(f"\nLeave-one-event-out — removing {exclude_start} to {exclude_end}")
    print(f"  Excluded {len(excluded)} signals  (cluster P&L sum: {excl_sum:+.2f}%)")
    print(f"  Kept {len(kept)} signals")

    if not kept_returns:
        print("  No signals remain after exclusion.")
        return

    wins_k   = [r for r in kept_returns if r > 0]
    losses_k = [r for r in kept_returns if r <= 0]
    gw = sum(wins_k)
    gl = abs(sum(losses_k))
    pf_kept  = gw / gl if gl else float("inf")
    avg_kept = sum(kept_returns) / len(kept_returns)

    print(f"\nWithout Oct cluster:")
    print(f"  Avg P&L      : {avg_kept:+.2f}%  (full: {sum(all_returns)/len(all_returns):+.2f}%)")
    print(f"  Profit factor: {pf_kept:.3f}  (full: {sum([r for r in all_returns if r>0]) / abs(sum([r for r in all_returns if r<=0])):.3f})")
    print(f"  Win rate     : {len(wins_k)/len(kept_returns)*100:.1f}%")

    if len(kept_returns) >= 4:
        pct = _pct_above_1(kept_returns, block_size=block_size, n_iter=n_iter)
        print(f"  P(PF > 1)    : {pct:.1f}%  (block_size={block_size})")

    print()


def run_bootstrap(period: str, asset: str = "ZEC-USD", block_size: int = 4, n_iter: int = 10_000) -> None:
    if period not in PERIODS:
        print(f"Unknown period '{period}'. Available: {list(PERIODS)}")
        sys.exit(1)

    print(f"\nRunning block bootstrap — {asset} / {period}")
    print(f"Block size: {block_size}  |  Iterations: {n_iter:,}")
    print("Downloading data and scanning signals...")

    period_cfg = PERIODS[period]
    result  = scan_asset(asset, period_cfg)
    signals = result.get("signals", [])
    if not signals:
        print("No signals found.")
        return

    returns = [s["trade"]["pnl_pct"] for s in signals]
    n       = len(returns)
    wins    = [r for r in returns if r > 0]
    losses  = [r for r in returns if r <= 0]

    gross_win  = sum(wins)
    gross_loss = abs(sum(losses))
    actual_pf  = gross_win / gross_loss if gross_loss else float("inf")
    actual_avg = sum(returns) / n

    print(f"\nActual sample  ({n} signals)")
    print(f"  Win rate     : {len(wins)/n*100:.1f}%")
    print(f"  Avg P&L      : {actual_avg:+.2f}%")
    print(f"  Profit factor: {actual_pf:.3f}")

    # Identify clusters (consecutive wins / losses)
    clusters: list[tuple[str, list[float]]] = []
    current_sign = None
    current: list[float] = []
    for r in returns:
        sign = "W" if r > 0 else "L"
        if sign != current_sign:
            if current:
                clusters.append((current_sign, current))  # type: ignore[arg-type]
            current_sign = sign
            current = [r]
        else:
            current.append(r)
    if current:
        clusters.append((current_sign, current))  # type: ignore[arg-type]

    longest_win_run  = max((len(c) for s, c in clusters if s == "W"), default=0)
    longest_loss_run = max((len(c) for s, c in clusters if s == "L"), default=0)
    print(f"  Longest win streak : {longest_win_run}")
    print(f"  Longest loss streak: {longest_loss_run}")

    # Big-trade contribution
    top3 = sorted(returns, reverse=True)[:3]
    gross_without_top3 = gross_win - sum(r for r in top3 if r > 0)
    pf_without_top3 = gross_without_top3 / gross_loss if gross_loss else float("inf")
    print(f"\nTop 3 trades: {[f'{r:+.2f}%' for r in top3]}")
    print(f"PF without top 3 wins: {pf_without_top3:.3f}")

    print(f"\nRunning {n_iter:,} block bootstrap iterations (block_size={block_size})...")
    pfs = _block_bootstrap_pf(returns, block_size=block_size, n_iter=n_iter)

    finite_pfs = pfs[np.isfinite(pfs)]
    p5, p25, p50, p75, p95 = np.percentile(finite_pfs, [5, 25, 50, 75, 95])
    pct_above_1 = (finite_pfs > 1.0).mean() * 100

    print(f"\nBlock Bootstrap 95% CI on Profit Factor")
    print(f"  5th  pct : {p5:.3f}")
    print(f"  25th pct : {p25:.3f}")
    print(f"  Median   : {p50:.3f}  (actual: {actual_pf:.3f})")
    print(f"  75th pct : {p75:.3f}")
    print(f"  95th pct : {p95:.3f}")
    print(f"  P(PF > 1): {pct_above_1:.1f}%")

    print(f"\nBlock Bootstrap 95% CI on Avg P&L")
    avg_pls = []
    rng = np.random.default_rng(42)
    arr = np.array(returns, dtype=float)
    n_  = len(arr)
    for _ in range(n_iter):
        sample: list[float] = []
        while len(sample) < n_:
            start = rng.integers(0, n_)
            block = arr[start : start + block_size]
            if len(block) < block_size:
                block = np.concatenate([block, arr[: block_size - len(block)]])
            sample.extend(block.tolist())
        avg_pls.append(np.mean(sample[:n_]))
    a5, a95 = np.percentile(avg_pls, [5, 95])
    print(f"  95% CI: [{a5:+.2f}%, {a95:+.2f}%]  (actual mean: {actual_avg:+.2f}%)")

    pct_pos = (np.array(avg_pls) > 0).mean() * 100
    print(f"  P(avg > 0): {pct_pos:.1f}%")

    print("\n" + "=" * 55)
    if pct_above_1 >= 75 and a5 > 0:
        print("  VERDICT: Robust edge — bootstrap supports positive expectancy")
    elif pct_above_1 >= 50:
        print("  VERDICT: Marginal — edge is real but sensitive to regime clustering")
    else:
        print("  VERDICT: Fragile — positive result driven by a small number of clustered wins")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", default="recent_year",
                        choices=list(PERIODS.keys()))
    parser.add_argument("--asset",  default="ZEC-USD")
    parser.add_argument("--block",  type=int, default=4,
                        help="Block size for bootstrap (default 4)")
    parser.add_argument("--iters",  type=int, default=10_000)
    parser.add_argument("--multi-block",  action="store_true",
                        help="Sweep block sizes 2, 4, 8, 12")
    parser.add_argument("--leave-out-event", nargs=2,
                        metavar=("START_YYYY-MM", "END_YYYY-MM"),
                        help="Exclude a date-range cluster, e.g. 2025-09 2025-11")
    args = parser.parse_args()

    if args.multi_block:
        run_multi_block(args.period, args.asset, args.iters)
    elif args.leave_out_event:
        start = args.leave_out_event[0] + "-01"
        # end: last day of given month
        import calendar, datetime
        y, m = int(args.leave_out_event[1].split("-")[0]), int(args.leave_out_event[1].split("-")[1])
        last_day = calendar.monthrange(y, m)[1]
        end = f"{y:04d}-{m:02d}-{last_day:02d}"
        run_leave_one_event_out(args.period, args.asset, start, end, args.block, args.iters)
    else:
        run_bootstrap(args.period, args.asset, args.block, args.iters)
