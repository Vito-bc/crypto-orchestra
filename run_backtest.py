"""Full-range agent backtest runner — Sep 2024 to May 2026."""
import sys
sys.path.insert(0, ".")
import backtesting.agent_backtest as ab

ab.BACKTEST_START = "2024-09-01"
ab.BACKTEST_END   = "2026-05-23"

print("Crypto Orchestra — Full Agent Backtest (Sep 2024 — May 2026)")
print("Covers: Bull run, ATH correction, ETH crash -46%, recovery, current BEAR")
print("=" * 65)

results = []
for symbol in ab.SYMBOLS:
    r = ab.run_agent_backtest(symbol)
    if r:
        results.append(r)

if len(results) > 1:
    print("\nИТОГОВОЕ СРАВНЕНИЕ")
    print("=" * 65)
    for r in results:
        verdict = "PASS" if r["passed"] else "FAIL"
        print(f"  {r['symbol']:<10} return={r['return']:+.2f}%  "
              f"WR={r['win_rate']:.1f}%  PF={r['profit_factor']:.2f}  "
              f"DD={r['max_drawdown']:.1f}%  trades={r['trades']}  {verdict}")
    print("=" * 65)
