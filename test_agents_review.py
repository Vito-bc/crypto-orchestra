"""Quick review of all agent data sources."""
import sys
sys.path.insert(0, ".")

from tools.onchain_data import get_onchain_metrics
from tools.funding_data import get_funding_rate
from tools.sentiment_data import get_fear_and_greed, get_recent_headlines
from tools.price_data import get_snapshot
from tools.market_positioning import get_open_interest, get_long_short_ratio

ASSETS = ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]

print("=" * 60)
print("1. CoinGecko metrics (onchain_data)")
print("=" * 60)
for asset in ASSETS:
    m = get_onchain_metrics(asset)
    ok = "OK  " if "price_change_24h" in m else "FAIL"
    chg = m.get("price_change_24h", "ERR")
    note = m.get("exchange_note", m.get("error", "?"))
    print(f"  {asset}: [{ok}] 24h={chg}%  {note[:50]}")

print()
print("=" * 60)
print("2. OKX Funding rates (funding_data)")
print("=" * 60)
for asset in ASSETS:
    f = get_funding_rate(asset)
    ok = "OK  " if not f.get("error") else "FAIL"
    rate = f["current_rate_pct"]
    sig = f["signal"]
    err = f.get("error", "-")
    print(f"  {asset}: [{ok}] rate={rate:+.5f}%  signal={sig}  err={err}")

print()
print("=" * 60)
print("3. OKX Open Interest (market_positioning)")
print("=" * 60)
for asset in ASSETS:
    oi = get_open_interest(asset)
    ok = "OK  " if oi.get("source") else "FAIL"
    chg = oi.get("oi_change_pct", 0)
    trend = oi.get("oi_trend", "?")
    sig = oi.get("signal", "?")
    print(f"  {asset}: [{ok}] OI chg={chg:+.1f}%  trend={trend}  signal={sig}")

print()
print("=" * 60)
print("4. OKX Long/Short ratio (market_positioning)")
print("=" * 60)
for asset in ASSETS:
    ls = get_long_short_ratio(asset)
    ok = "OK  " if ls.get("source") else "FAIL"
    lp = ls.get("long_pct", 50)
    sp = ls.get("short_pct", 50)
    sig = ls.get("signal", "?")
    print(f"  {asset}: [{ok}] long={lp}%  short={sp}%  signal={sig}")

print()
print("=" * 60)
print("5. Price snapshots (price_data / yfinance)")
print("=" * 60)
for asset in ASSETS:
    s = get_snapshot(asset)
    if s:
        print(f"  {asset}: [OK  ] close={s['close']:.2f}  rsi={s['rsi_1h']:.1f}  trend={s.get('trend_4h','?')}  signal={s['signal']}")
    else:
        print(f"  {asset}: [FAIL] snapshot returned None")

print()
print("=" * 60)
print("6. Fear & Greed Index")
print("=" * 60)
fg = get_fear_and_greed()
print(f"  Value={fg['value']}  Label={fg['label']}  err={fg.get('error', '-')}")

print()
print("=" * 60)
print("7. CryptoPanic headlines")
print("=" * 60)
for asset in ["BTC-USD", "ETH-USD"]:
    h = get_recent_headlines(asset, limit=3)
    base = asset.split("-")[0]
    print(f"  {base}: {len(h)} headlines")
    for line in h:
        print(f"    - {line[:75]}")
