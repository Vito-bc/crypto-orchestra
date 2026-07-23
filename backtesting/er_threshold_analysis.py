"""One-shot ER-30 threshold analysis across all 4 cross-period validation sets."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backtesting.signal_scanner import scan_asset, PERIODS

all_signals = []
for pname in ['bull_2021', 'bear_2022', 'mid_year_holdout', 'recent_year']:
    p = PERIODS[pname]
    result = scan_asset('ZEC-USD', p)
    sigs = result.get('signals', [])
    for s in sigs:
        er = s.get('regime', {}).get('er_30')
        if er is not None:
            all_signals.append({
                'period': pname,
                'er': er,
                'win': s['trade']['pnl_pct'] > 0,
                'pnl': s['trade']['pnl_pct'],
            })

print(f'\nTotal signals with ER-30: {len(all_signals)}')
wins   = [x for x in all_signals if x['win']]
losses = [x for x in all_signals if not x['win']]
print(f'All:   n={len(all_signals):3}  WR={len(wins)/len(all_signals)*100:.1f}%  ER wins={sum(x["er"] for x in wins)/len(wins):.3f}  ER loss={sum(x["er"] for x in losses)/len(losses):.3f}')

print()
print(f"{'Threshold':>12}  {'N passed':>8}  {'WR%':>6}  {'PF':>6}  {'Avg PnL':>8}  {'Skipped':>8}")
print('-' * 60)
for thr in [0.0, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40]:
    passed  = [x for x in all_signals if x['er'] >= thr]
    skipped = len(all_signals) - len(passed)
    if not passed:
        continue
    w  = [x for x in passed if x['win']]
    l  = [x for x in passed if not x['win']]
    gw = sum(x['pnl'] for x in w)
    gl = abs(sum(x['pnl'] for x in l))
    pf = gw / gl if gl else float('inf')
    wr = len(w) / len(passed) * 100
    avg = sum(x['pnl'] for x in passed) / len(passed)
    print(f"  er >= {thr:.2f}  {len(passed):>8}  {wr:>6.1f}  {pf:>6.3f}  {avg:>+8.2f}%  {skipped:>8}")

print()
print('ER-30 percentiles (all signals):')
import numpy as np
ers = [x['er'] for x in all_signals]
for pct in [10, 25, 50, 75, 90]:
    print(f'  {pct:3}th pct: {np.percentile(ers, pct):.3f}')
