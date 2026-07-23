"""
Cost stress test: How much per-trade friction before each ER-30 threshold turns negative?

Models realistic execution costs: spread, slippage, missed fills, wider-than-limit fills.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backtesting.signal_scanner import scan_asset, PERIODS

all_signals = []
for pname in ['bull_2021', 'bear_2022', 'mid_year_holdout', 'recent_year']:
    p = PERIODS[pname]
    result = scan_asset('ZEC-USD', p)
    for s in result.get('signals', []):
        er = s.get('regime', {}).get('er_30')
        if er is not None:
            all_signals.append({'period': pname, 'er': er, 'pnl': s['trade']['pnl_pct']})

print(f'\nCost stress test — {len(all_signals)} signals across 4 independent periods')
print('(Backtested P&L already includes Coinbase maker/taker fees of 0.4%/0.6%)')
print('Additional friction = spread + slippage + missed fills + market impact')
print()

thresholds = [0.00, 0.20, 0.25, 0.35, 0.40]
friction_pcts = [0.0, 0.10, 0.25, 0.50, 1.00]

header = f"{'ER min':>8}" + ''.join(f"  {'fric+'+str(int(f*100))+'bps':>10}" for f in friction_pcts)
print(header)
print('-' * len(header))

for thr in thresholds:
    passed = [x for x in all_signals if x['er'] >= thr]
    if not passed:
        continue
    row = f"  >= {thr:.2f}"
    for friction in friction_pcts:
        adj_pnls = [x['pnl'] - friction for x in passed]
        w  = [p for p in adj_pnls if p > 0]
        l  = [p for p in adj_pnls if p <= 0]
        gw = sum(w)
        gl = abs(sum(l))
        pf = gw / gl if gl else float('inf')
        avg = sum(adj_pnls) / len(adj_pnls)
        row += f"  {pf:6.3f}/{avg:+.2f}%"
    row += f"  (n={len(passed)})"
    print(row)

print()
print('Format: PF/avg_pnl at each additional friction level')
print('Breakeven friction = point where avg P&L crosses 0%')
