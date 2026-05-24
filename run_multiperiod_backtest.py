"""Multi-period backtest: Bull, Bear, Recovery, Current Bear."""
import sys
sys.path.insert(0, ".")

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from backtesting.backtest import (
    calculate_indicators, attach_higher_timeframe_context,
    ATR_STOP_MULTIPLIER, ATR_TARGET_MULTIPLIER,
    FEE_RATE, TRADE_SIZE_PCT, START_BALANCE,
    get_signal, get_symbol_config,
)

PERIODS = [
    ("BULL RUN",     "2024-09-01", "2024-12-31"),
    ("BEAR / CRASH", "2025-01-01", "2025-04-30"),
    ("RECOVERY",     "2025-05-01", "2025-09-30"),
    ("CURRENT BEAR", "2025-10-01", "2026-05-22"),
]
SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]
WARMUP  = 60
MAX_HOLD = {"ETH-USD": 8, "BTC-USD": 12, "SOL-USD": 8, "ZEC-USD": 8}


def fetch(symbol, start, end, interval):
    ws = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=WARMUP)).strftime("%Y-%m-%d")
    df = yf.download(symbol, start=ws, end=end, interval=interval,
                     progress=False, auto_adjust=True)
    if df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.columns = [c.lower() for c in df.columns]
    df = df.reset_index()
    for col in ["Datetime", "datetime", "date", "index"]:
        if col in df.columns:
            df = df.rename(columns={col: "time"})
            break
    df = df[["time", "open", "high", "low", "close", "volume"]].dropna()
    df = calculate_indicators(df)
    cutoff = pd.Timestamp(start)
    if df["time"].dt.tz is not None:
        cutoff = cutoff.tz_localize(df["time"].dt.tz)
    return df[df["time"] >= cutoff].reset_index(drop=True)


def run_period(symbol, label, start, end):
    cfg      = get_symbol_config(symbol)
    max_hold = MAX_HOLD.get(symbol, 12)
    sig_df   = fetch(symbol, start, end, "1h")
    trend_df = fetch(symbol, start, end, "4h")
    if sig_df is None or trend_df is None or len(sig_df) < 10:
        return None

    df = attach_higher_timeframe_context(sig_df, trend_df)
    rows     = df.to_dict("records")
    balance  = float(START_BALANCE)
    position = None
    trades   = []

    for i, row in enumerate(rows):
        price = row["close"]
        if position:
            held_h     = (row["time"] - position["entry_time"]).total_seconds() / 3600
            hit_stop   = price <= position["stop"]
            hit_target = price >= position["target"]
            hit_hold   = held_h >= max_hold
            if hit_stop or hit_target or hit_hold:
                reason = ("STOP_LOSS"  if hit_stop   else
                          "TAKE_PROFIT" if hit_target else "MAX_HOLD")
                cost  = position["size"] * FEE_RATE
                pnl   = (price - position["entry"]) / position["entry"] * position["size"] - cost
                balance += position["size"] + pnl
                trades.append({"reason": reason, "pnl": pnl,
                               "hold_h": held_h, "win": pnl > 0})
                position = None
            continue

        prev = rows[i - 1] if i > 0 else row
        sig  = get_signal(row, prev, cfg)
        if sig == "BUY":
            size   = START_BALANCE * TRADE_SIZE_PCT
            stop   = price - ATR_STOP_MULTIPLIER  * row["atr"]
            target = price + ATR_TARGET_MULTIPLIER * row["atr"]
            cost   = size * FEE_RATE
            balance -= size + cost
            position = {"entry": price, "stop": stop, "target": target,
                        "size": size, "entry_time": row["time"]}

    if position:
        price = rows[-1]["close"]
        cost  = position["size"] * FEE_RATE
        pnl   = (price - position["entry"]) / position["entry"] * position["size"] - cost
        balance += position["size"] + pnl
        trades.append({"reason": "END", "pnl": pnl, "hold_h": 0, "win": pnl > 0})

    if not trades:
        return {"label": label, "symbol": symbol, "trades": 0}

    wins    = [t for t in trades if t["win"]]
    losses  = [t for t in trades if not t["win"]]
    avg_win  = sum(t["pnl"] for t in wins)    / len(wins)    if wins   else 0
    avg_loss = sum(t["pnl"] for t in losses)  / len(losses)  if losses else 0
    pf       = abs(avg_win / avg_loss) if avg_loss else float("inf")
    wr       = len(wins) / len(trades) * 100
    ret      = (balance - START_BALANCE) / START_BALANCE * 100
    by_reason = {}
    for t in trades:
        by_reason.setdefault(t["reason"], []).append(t["win"])
    return {"label": label, "symbol": symbol, "trades": len(trades),
            "win_rate": wr, "pf": pf, "ret": ret,
            "avg_hold": sum(t["hold_h"] for t in trades) / len(trades),
            "by_reason": by_reason, "wins": len(wins)}


print("=" * 65)
print("CRYPTO ORCHESTRA  Multi-Period Backtest")
print(f"Stop={ATR_STOP_MULTIPLIER}x ATR  |  Target={ATR_TARGET_MULTIPLIER}x ATR")
print("=" * 65)

all_results = []
for symbol in SYMBOLS:
    print(f"\n  {symbol}")
    print(f"  {'-'*60}")
    print(f"  {'Период':<20} {'Сделок':>6} {'WR':>6} {'PF':>6} {'Доход':>8} {'Hold':>6}")
    print(f"  {'-'*60}")
    sym_trades = 0
    sym_wins   = 0
    for label, start, end in PERIODS:
        r = run_period(symbol, label, start, end)
        if r is None:
            print(f"  {label:<20}  нет данных")
            continue
        if r["trades"] == 0:
            print(f"  {label:<20}  {'0':>6}  (BEAR вето — правильно)")
            continue
        flag = "OK" if r["pf"] >= 1.0 and r["win_rate"] >= 35 else "WEAK"
        print(f"  {label:<20}  {r['trades']:>6}  "
              f"{r['win_rate']:>5.0f}%  {r['pf']:>5.2f}  "
              f"{r['ret']:>+7.2f}%  {r['avg_hold']:>5.1f}h  [{flag}]")
        for reason, results in r["by_reason"].items():
            w = sum(results)
            print(f"    {reason:<16} {len(results):>3}  WR={w/len(results)*100:.0f}%")
        all_results.append(r)
        sym_trades += r["trades"]
        sym_wins   += r["wins"]
    if sym_trades:
        print(f"  {'-'*60}")
        print(f"  {'ИТОГО '+symbol:<20}  {sym_trades:>6}  WR={sym_wins/sym_trades*100:.0f}%")

print(f"\n{'='*65}")
print("ИТОГ ПО ВСЕМ ПЕРИОДАМ И АКТИВАМ")
print(f"{'='*65}")
total_t = sum(r["trades"] for r in all_results)
total_w = sum(r["wins"]   for r in all_results)
if total_t:
    print(f"  Всего сделок:       {total_t}")
    print(f"  Общий Win Rate:     {total_w/total_t*100:.1f}%")
    pos = sum(1 for r in all_results if r["ret"] > 0)
    print(f"  Прибыльных периодов: {pos}/{len(all_results)}")
    avg_pf = sum(r["pf"] for r in all_results) / len(all_results)
    print(f"  Средний PF:         {avg_pf:.2f}")
print("=" * 65)
