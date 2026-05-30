from tools.market_positioning import get_open_interest, get_long_short_ratio, get_binance_funding_rate

print("=== BTC-USD ===")
oi = get_open_interest("BTC-USD")
print(f"OI: ${oi['oi_usd']:,.0f}  change: {oi['oi_change_pct']:+.2f}%  trend: {oi['oi_trend']}")
print(f"Signal: {oi['signal']}  conf: {oi['confidence']:.0%}")
print(f"-> {oi['interpretation']}")

ls = get_long_short_ratio("BTC-USD")
print(f"L/S: {ls['long_pct']}% long / {ls['short_pct']}% short -> {ls['signal']}")
print(f"-> {ls['interpretation']}")

bf = get_binance_funding_rate("BTC-USD")
print(f"Binance funding: {bf['rate_pct']:+.5f}% -> {bf['signal']}")

print("\n=== ZEC-USD (no Binance perp) ===")
oi_z = get_open_interest("ZEC-USD")
print(f"OI: {oi_z['interpretation']}")
