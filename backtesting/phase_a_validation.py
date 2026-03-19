from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting.backtest import SYMBOLS, run_backtest


WINDOWS = [30, 60, 90, 180, 365]


def main():
    print("\nPHASE A VALIDATION")
    print("=" * 72)

    all_results = []
    for days in WINDOWS:
        print(f"\nTesting lookback window: {days} days")
        print("-" * 72)
        for symbol in SYMBOLS:
            result = run_backtest(symbol, timeframe="1h", days=days)
            if result:
                result["days"] = days
                all_results.append(result)

    if not all_results:
        print("\nNo validation results were produced.")
        return

    print("\nVALIDATION SUMMARY")
    print("=" * 72)
    print(f"{'Symbol':12} {'Days':>5} {'Return%':>9} {'Trades':>7} {'WinRate%':>10} {'PF':>6} {'DD%':>7}")
    print("-" * 72)
    for result in all_results:
        print(
            f"{result['symbol']:12} "
            f"{result['days']:5d} "
            f"{result['return']:9.2f} "
            f"{result['trades']:7d} "
            f"{result['win_rate']:10.1f} "
            f"{result['profit_factor']:6.2f} "
            f"{result['max_drawdown']:7.2f}"
        )

    print("\nSYMBOL ROLLUP")
    print("=" * 72)
    for symbol in SYMBOLS:
        symbol_results = [r for r in all_results if r["symbol"] == symbol]
        avg_return = sum(r["return"] for r in symbol_results) / len(symbol_results)
        avg_win_rate = sum(r["win_rate"] for r in symbol_results) / len(symbol_results)
        pf_values = [r["profit_factor"] for r in symbol_results if r["trades"] > 0 and r["profit_factor"] != float("inf")]
        avg_pf = sum(pf_values) / len(pf_values) if pf_values else 0.0
        positive_windows = sum(1 for r in symbol_results if r["return"] > 0)
        print(
            f"{symbol:12} "
            f"AvgReturn: {avg_return:+.2f}%  "
            f"AvgWinRate: {avg_win_rate:.1f}%  "
            f"AvgPF: {avg_pf:.2f}  "
            f"PositiveWindows: {positive_windows}/{len(symbol_results)}"
        )


if __name__ == "__main__":
    main()
