"""Run backtest for April-June 2024 correction period."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import backtesting.trump_period_backtest as bt

bt.WARMUP_START = "2024-07-01"
bt.TRADE_START  = "2024-08-01"
bt.TRADE_END    = "2024-09-30"

print("=" * 65)
print("  CRYPTO ORCHESTRA -- HISTORICAL BACKTEST")
print("  Period: 2024-08-01 to 2024-09-30")
print("  Event:  August 2024 crash + recovery")
print("  BTC:    $66k -> $50k (-24%), recovery to $63k")
print("=" * 65)

results = []
for sym in bt.SYMBOLS:
    print(f"\n[{sym}]")
    r = bt.run_period_backtest(sym)
    if r:
        results.append(r)

print("\n" + "=" * 65)
print("  SUMMARY")
print("=" * 65)
header = f"  {'Symbol':<10} {'Trades':>6} {'Win%':>6} {'PF':>5} {'Return':>8} {'Balance':>10} {'Breakouts':>10}"
print(header)
print("  " + "-" * 63)
for r in results:
    print(
        f"  {r['symbol']:<10} {r['trades']:>6} {r['win_rate']:>5.1f}% "
        f"{r['profit_factor']:>5.2f} {r['return_pct']:>+7.2f}% "
        f"${r['final_balance']:>9,.2f} {r['breakout_entries']:>10}"
    )
print("=" * 65)
total_ret = sum(r["return_pct"] for r in results) / len(results)
total_tr  = sum(r["trades"] for r in results)
total_br  = sum(r["breakout_entries"] for r in results)
print(f"\n  Avg return:     {total_ret:+.2f}%")
print(f"  Total trades:   {total_tr}")
print(f"  Breakout entries: {total_br}")
print(f"  BTC buy-hold for same period: approx -12% (correction, no recovery)")
print("=" * 65)
