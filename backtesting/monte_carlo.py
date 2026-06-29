"""
Monte Carlo Analysis for crypto-orchestra.

Runs 10,000 simulations by resampling backtest trade returns to answer:
  1. What is the realistic worst-case drawdown distribution?
  2. Is 2% position sizing safe? What about 5%?
  3. Is our win rate statistically meaningful or just luck?
  4. How many consecutive losses should we expect?

Data sources (in order of priority):
  1. logs/trade_history.jsonl  — real live paper trades
  2. Backtest engine           — historical simulation trades (90+ days per asset)

Usage:
    python backtesting/monte_carlo.py
    python backtesting/monte_carlo.py --live-only    # only real paper trades
    python backtesting/monte_carlo.py --size 0.05    # test 5% position size
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

N_SIMS        = 10_000
START_BALANCE = 10_000.0


# ── Data loading ──────────────────────────────────────────────────────────────

def _load_live_trades() -> list[dict]:
    path = ROOT / "logs" / "trade_history.jsonl"
    if not path.exists():
        return []
    trades = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                trades.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return trades


def _load_backtest_trades(symbols: list[str] | None = None) -> list[dict]:
    """Run the backtest engine and collect all trade records."""
    from backtesting.backtest import run_backtest, SYMBOLS
    targets = symbols or SYMBOLS
    trades = []
    print("Running backtests to build trade sample (this takes ~60s)...")
    for sym in targets:
        try:
            result = run_backtest(sym, days=365)
            records = result.get("trade_records", []) if result else []
            for t in records:
                # pnl_pct in backtest is a fraction (0.05 = 5%), convert to percent
                raw_pct = t.get("pnl_pct", 0.0)
                pct = raw_pct * 100 if abs(raw_pct) < 1.0 else raw_pct
                trades.append({
                    "asset":   sym,
                    "pnl_pct": pct,
                    "reason":  t.get("reason", ""),
                    "source":  "backtest",
                })
            print(f"  {sym}: {len(records)} trades")
        except Exception as exc:
            print(f"  {sym}: backtest failed — {exc}")
    return trades


# ── Monte Carlo engine ────────────────────────────────────────────────────────

def run_monte_carlo(
    returns_pct: np.ndarray,
    position_size_pct: float,
    n_sims: int = N_SIMS,
    start_balance: float = START_BALANCE,
) -> dict:
    """
    Bootstrap resample `returns_pct` (array of % P&L per trade) n_sims times.
    Returns distribution statistics.

    position_size_pct: e.g. 0.02 for 2% of balance per trade.
    The raw pnl_pct from backtest is % of the position, so we scale:
      balance_return = pnl_pct * position_size_pct
    """
    n_trades = len(returns_pct)
    if n_trades < 5:
        raise ValueError(f"Need at least 5 trades for meaningful Monte Carlo, got {n_trades}")

    # Scale returns from "% of position" to "% of total balance"
    balance_returns = returns_pct / 100.0 * position_size_pct

    final_balances  = np.zeros(n_sims)
    max_drawdowns   = np.zeros(n_sims)
    max_loss_runs   = np.zeros(n_sims, dtype=int)

    rng = np.random.default_rng(42)  # reproducible

    for i in range(n_sims):
        # Bootstrap: sample with replacement (realistic — some trades can recur)
        sample = rng.choice(balance_returns, size=n_trades, replace=True)
        equity = start_balance * np.cumprod(1 + sample)

        # Max drawdown
        peak   = np.maximum.accumulate(equity)
        dd     = (equity - peak) / peak
        max_drawdowns[i] = dd.min()

        final_balances[i] = equity[-1]

        # Longest consecutive loss streak
        signs    = (sample < 0).astype(int)
        max_run  = 0
        cur_run  = 0
        for s in signs:
            if s:
                cur_run += 1
                max_run = max(max_run, cur_run)
            else:
                cur_run = 0
        max_loss_runs[i] = max_run

    return {
        "n_trades":         n_trades,
        "n_sims":           n_sims,
        "position_size":    position_size_pct,
        "win_rate":         float((returns_pct > 0).mean()),
        "avg_win_pct":      float(returns_pct[returns_pct > 0].mean()) if (returns_pct > 0).any() else 0.0,
        "avg_loss_pct":     float(returns_pct[returns_pct < 0].mean()) if (returns_pct < 0).any() else 0.0,
        "expectancy_pct":   float(returns_pct.mean()),
        # Final balance distribution
        "final_median":     float(np.median(final_balances)),
        "final_p5":         float(np.percentile(final_balances, 5)),
        "final_p95":        float(np.percentile(final_balances, 95)),
        "ruin_pct":         float((final_balances < start_balance * 0.5).mean() * 100),
        # Drawdown distribution
        "dd_median_pct":    float(np.median(max_drawdowns) * 100),
        "dd_p5_pct":        float(np.percentile(max_drawdowns, 5) * 100),
        "dd_p95_pct":       float(np.percentile(max_drawdowns, 95) * 100),
        "dd_worst_pct":     float(max_drawdowns.min() * 100),
        # Loss streak distribution
        "max_loss_run_median": float(np.median(max_loss_runs)),
        "max_loss_run_p95":    float(np.percentile(max_loss_runs, 95)),
        "max_loss_run_worst":  int(max_loss_runs.max()),
    }


# ── Reporting ─────────────────────────────────────────────────────────────────

def _bar(val: float, max_val: float, width: int = 30, fill: str = "#") -> str:
    n = int(abs(val) / abs(max_val) * width) if max_val else 0
    return fill * min(n, width)


def print_report(results: list[dict], trade_source: str) -> None:
    print("\n" + "=" * 68)
    print("CRYPTO ORCHESTRA — MONTE CARLO ANALYSIS")
    print(f"Trade source:  {trade_source}")
    print(f"Simulations:   {N_SIMS:,}")
    print("=" * 68)

    # Trade sample stats (same across all sizing scenarios)
    r0 = results[0]
    print(f"\nTrade Sample ({r0['n_trades']} trades):")
    print(f"  Win rate:        {r0['win_rate']:.1%}")
    print(f"  Avg win:         +{r0['avg_win_pct']:.2f}% of position")
    print(f"  Avg loss:        {r0['avg_loss_pct']:.2f}% of position")
    print(f"  Expectancy:      {r0['expectancy_pct']:+.3f}% per trade")

    verdict = "POSITIVE EDGE" if r0['expectancy_pct'] > 0 else "NEGATIVE EDGE — system losing money on average"
    print(f"  Edge verdict:    {verdict}")

    print(f"\n{'Size':<8} {'Final $':>10} {'5th pct':>10} {'95th pct':>10} "
          f"{'Median DD':>10} {'Worst DD':>10} {'Ruin%':>8} {'Max Streak':>11}")
    print("-" * 80)

    for r in results:
        size_str = f"{r['position_size']:.0%}"
        print(
            f"{size_str:<8} "
            f"${r['final_median']:>9,.0f} "
            f"${r['final_p5']:>9,.0f} "
            f"${r['final_p95']:>9,.0f} "
            f"{r['dd_median_pct']:>9.1f}% "
            f"{r['dd_worst_pct']:>9.1f}% "
            f"{r['ruin_pct']:>7.1f}% "
            f"{r['max_loss_run_p95']:>10.0f}"
        )

    print("\nColumn guide:")
    print("  Final $   = median ending balance after all trades")
    print("  5th pct   = balance at unlucky 5th percentile (bad luck scenario)")
    print("  95th pct  = balance at lucky 95th percentile")
    print("  Median DD = typical max drawdown from peak")
    print("  Worst DD  = worst drawdown across all 10k simulations")
    print("  Ruin%     = % of simulations where balance fell below 50% of start")
    print("  Max Streak= 95th percentile longest consecutive losing streak")

    print("\n--- Interpretation ---")
    for r in results:
        size = r['position_size']
        dd95 = abs(r['dd_p5_pct'])
        ruin = r['ruin_pct']
        streak = r['max_loss_run_p95']
        safe = ruin < 5.0 and dd95 < 25.0

        symbol = "OK" if safe else "RISKY"
        print(f"  {size:.0%} position size: [{symbol}]  "
              f"worst-case DD {dd95:.1f}%  |  ruin risk {ruin:.1f}%  |  "
              f"expect up to {streak:.0f} losses in a row")

    print("=" * 68)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    live_only  = "--live-only" in sys.argv
    size_arg   = next((a for a in sys.argv if a.startswith("--size")), None)
    extra_size = float(size_arg.split("=")[1]) if size_arg and "=" in size_arg else None

    # Load trades
    live_trades = _load_live_trades()
    print(f"Live paper trades found: {len(live_trades)}")

    if live_only or not live_trades:
        trades = live_trades
        source = f"live paper trades only ({len(live_trades)})"
    else:
        bt_trades = _load_backtest_trades()
        trades    = live_trades + bt_trades
        source    = f"live ({len(live_trades)}) + backtest ({len(bt_trades)})"

    if not trades:
        print("No trades available. Run the system first to accumulate paper trades,")
        print("or remove --live-only to also use backtest data.")
        sys.exit(1)

    returns = np.array([t.get("pnl_pct", 0.0) for t in trades], dtype=float)
    print(f"Total trade sample: {len(returns)} trades")

    # Test multiple position sizes
    sizes = [0.02, 0.05]
    if extra_size and extra_size not in sizes:
        sizes.append(extra_size)
    sizes.sort()

    results = []
    for size in sizes:
        print(f"Running Monte Carlo for {size:.0%} position size...")
        r = run_monte_carlo(returns, position_size_pct=size)
        results.append(r)

    print_report(results, source)

    # Save JSON results for further analysis
    out_path = ROOT / "backtesting" / "monte_carlo_results.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"source": source, "results": results}, f, indent=2)
    print(f"\nFull results saved to: {out_path}")


if __name__ == "__main__":
    main()
