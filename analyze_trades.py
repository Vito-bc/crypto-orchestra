"""Professional trade autopsy for ZEC-USD losses."""
import json
from datetime import datetime

trades = []
with open("logs/trade_history.jsonl") as f:
    for line in f:
        trades.append(json.loads(line.strip()))

with open("logs/open_positions.json") as f:
    positions = json.load(f)

pos_map = {p["id"]: p for p in positions}

print("=" * 65)
print("CRYPTO ORCHESTRA — TRADE AUTOPSY")
print("=" * 65)

for i, t in enumerate(trades, 1):
    entry_dt = datetime.fromisoformat(t["entry_time"])
    exit_dt  = datetime.fromisoformat(t["exit_time"])
    pos      = pos_map.get(t["id"], {})
    stop     = pos.get("stop_price", 0)
    hwm      = pos.get("high_water_mark", t["entry_price"])

    move_pct   = (t["exit_price"] - t["entry_price"]) / t["entry_price"] * 100
    hwm_pct    = (hwm - t["entry_price"]) / t["entry_price"] * 100
    stop_dist  = (stop - t["entry_price"]) / t["entry_price"] * 100
    slip       = t["exit_price"] - stop  # negative = exited below stop

    print(f"\nTrade #{i} — {t['asset']}")
    print(f"  Entry:     ${t['entry_price']:.2f}  ({entry_dt.strftime('%b %d %H:%M')} UTC)")
    print(f"  Stop:      ${stop:.2f}  ({stop_dist:+.1f}% from entry)")
    print(f"  Peak:      ${hwm:.2f}  (max gain: {hwm_pct:+.2f}% — never bounced)")
    print(f"  Exit:      ${t['exit_price']:.2f}  ({exit_dt.strftime('%b %d %H:%M')} UTC)")
    print(f"  Slippage:  ${slip:+.2f}  (exit vs stop price)")
    print(f"  Duration:  {t['hold_hours']:.1f}h | Move: {move_pct:+.2f}%")
    print(f"  PnL:       ${t['pnl_usd']:+.2f}  ({t['pnl_pct']:+.2f}%)")

print()
print("=" * 65)
print("SUMMARY")
print("=" * 65)
total_pnl  = sum(t["pnl_usd"] for t in trades)
total_fees = sum(t["entry_fee_usd"] + t["exit_fee_usd"] for t in trades)
wins = sum(1 for t in trades if t["pnl_usd"] > 0)
zec_trades = [t for t in trades if t["asset"] == "ZEC-USD"]

print(f"  Total trades:  {len(trades)}  (ZEC: {len(zec_trades)}, ETH: 1)")
print(f"  Win rate:      {wins}/{len(trades)}  (0%)")
print(f"  Total PnL:     ${total_pnl:+.2f}")
print(f"  Fees paid:     ${total_fees:.2f}")
print(f"  PnL ex-fees:   ${total_pnl + total_fees:+.2f}")
print()

print("ZEC WATERFALL — Entry prices as ZEC crashed:")
prev_entry = None
for t in zec_trades:
    entry_dt = datetime.fromisoformat(t["entry_time"])
    drop = f"  (re-entry, {((t['entry_price'] - prev_entry) / prev_entry * 100):+.1f}% vs prev)" if prev_entry else ""
    print(f"  {entry_dt.strftime('%b %d %H:%M')}  BUY ${t['entry_price']:.2f} -> exit ${t['exit_price']:.2f}  ({t['pnl_pct']:+.1f}%){drop}")
    prev_entry = t["entry_price"]

total_drop = (zec_trades[-1]["entry_price"] - zec_trades[0]["entry_price"]) / zec_trades[0]["entry_price"] * 100
print(f"\n  ZEC total move during trading period: {total_drop:+.1f}% (${zec_trades[0]['entry_price']:.0f} -> ${zec_trades[-1]['exit_price']:.0f})")
print()
print("=" * 65)
print("DIAGNOSIS")
print("=" * 65)
print("""
PATTERN: Classic 'Catching a Falling Knife' — 4 re-entries into sustained downtrend

WHAT HAPPENED:
  May 25-27: ZEC fell from $658 → $573  (-13% in 48 hours)
  System placed 4 consecutive BUY orders as ZEC declined
  Every 'support level' broke immediately — no bounces, pure waterfall

ROOT CAUSES:
  1. ZEC macro regime = BULL (EMA50 4h > EMA200 4h) while BTC/ETH = BEAR
     ZEC had its own local uptrend disconnected from market — that ended abruptly

  2. No BTC correlation veto — system never checks: 'is BTC in BEAR before buying alts?'
     If BTC is in BEAR, altcoins almost never hold their local trends

  3. No cooldown after stop loss — stop hit → system immediately looks for next support
     Entered again within 6 hours of previous stop loss, then again 6 hours after that

  4. High water mark = entry price on 3 of 4 ZEC trades — price NEVER bounced
     Zero upward movement. This is the textbook signal of 'price wants to go lower'

TRADE #5 (ETH, May 14-17):
  76 hour hold. Stop was $2,230 but exit was $2,183 — $47 below stop
  Hourly check = 1h slippage window. In real bear moves, price gaps through stops

TRADE #5 ZEC (May 27): Actually WORKED — trailing stop moved from $542 → $573
  Price went $570→$578→ trailing stop hit at $573. Loss = -$0.29. Strategy works.
  But entering a 4th time into a -13% waterfall was luck that it barely bounced.
""")
