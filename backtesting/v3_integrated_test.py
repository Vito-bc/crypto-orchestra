"""
Integrated V3 filter test: scan with v3_candidate_threshold active (not post-hoc).
This is what live trading would actually experience.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backtesting.signal_scanner import scan_asset, PERIODS, ASSET_CONFIG

def run_period(pname, er_min):
    # Set candidate threshold; enable enforcement so scan_asset gates on it.
    ASSET_CONFIG['ZEC-USD']['v3_candidate_threshold'] = er_min
    ASSET_CONFIG['ZEC-USD']['v3_enforcement_enabled'] = (er_min is not None)
    p = PERIODS[pname]
    r = scan_asset('ZEC-USD', p)
    sigs = r.get('signals', [])
    bv3  = r.get('blocked_v3', 0)
    if not sigs:
        return None, bv3
    wins = [s for s in sigs if s['trade']['pnl_pct'] > 0]
    gw = sum(s['trade']['pnl_pct'] for s in wins)
    gl = abs(sum(s['trade']['pnl_pct'] for s in sigs if s['trade']['pnl_pct'] <= 0))
    pf = gw / gl if gl else float('inf')
    avg = sum(s['trade']['pnl_pct'] for s in sigs) / len(sigs)
    wr = len(wins) / len(sigs)
    return {'n': len(sigs), 'wr': wr, 'pf': pf, 'avg': avg, 'bv3': bv3}, bv3

thresholds = [None, 0.20, 0.25, 0.30, 0.35]
periods    = ['bull_2021', 'bear_2022', 'mid_year_holdout', 'recent_year']

print('\nIntegrated V3 filter — PF across 4 independent periods\n')
print(f'{"Period":>20}  {"No filter":>14}  {"er>=0.20":>14}  {"er>=0.25":>14}  {"er>=0.30":>14}  {"er>=0.35":>14}')
print('-' * 100)

# Collect per-period results for all thresholds
table = {}
for pname in periods:
    table[pname] = {}
    for thr in thresholds:
        stats, bv3 = run_period(pname, thr)
        table[pname][str(thr)] = stats

for pname in periods:
    row = f'{pname:>20}'
    for thr in thresholds:
        s = table[pname][str(thr)]
        if s is None:
            row += f'  {"--":>14}'
        else:
            row += f'  PF={s["pf"]:.2f} n={s["n"]:>2}'
    print(row)

# Also print combined across all periods
print('\nCombined across all 4 periods:')
for thr in thresholds:
    all_sigs = []
    for pname in periods:
        s = table[pname][str(thr)]
        if s:
            all_sigs.append(s)
    n_total   = sum(s['n'] for s in all_sigs)
    if n_total == 0:
        continue
    # Weighted sum (not quite right since PF isn't additive, but indicative)
    pf_list   = [s['pf'] for s in all_sigs]
    avg_list  = [s['avg'] for s in all_sigs]
    bv3_total = sum(s['bv3'] for s in all_sigs)
    thr_str   = f'er>={thr:.2f}' if thr else 'No filter'
    print(f'  {thr_str:>12}: n_total={n_total:>3}  PFs={[f"{p:.2f}" for p in pf_list]}  avgs={[f"{a:+.2f}%" for a in avg_list]}  blocked_v3={bv3_total}')
