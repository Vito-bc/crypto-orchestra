"""
Historical Backtest — Trump Election Period (November 2024)

BTC ran from ~$67k to $93k+ after Trump won on Nov 5, 2024.
Tests whether the current strategy (RSI+MACD+BB+ADX+VWAP+CVD+EMA filters)
would have caught this move and compares each asset's performance.

Loads data from Aug 2024 (EMA200 warmup) but only simulates trades
in the Nov 1 – Dec 15, 2024 window.

Run:
    python backtesting/trump_period_backtest.py
"""

import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting.backtest import (
    attach_higher_timeframe_context,
    calculate_indicators,
    close_position,
    get_signal,
    get_symbol_config,
)

# ── Period ────────────────────────────────────────────────────────────────────
WARMUP_START  = "2024-08-01"   # load from here for EMA200 warmup
TRADE_START   = "2024-11-01"   # start placing trades here
TRADE_END     = "2024-12-15"   # stop placing trades here

SYMBOLS       = ["BTC-USD", "ETH-USD", "SOL-USD"]
START_BALANCE = 10_000.0
TRADE_SIZE_PCT = 0.05          # 5% per trade (vs 2% in rolling backtest — bigger to see signal)
FEE_RATE       = 0.006         # 0.6% round-trip
ATR_STOP       = 2.5
ATR_TARGET     = 4.0


def fetch_range(symbol: str, timeframe: str, start: str, end: str) -> pd.DataFrame | None:
    import yfinance as yf
    print(f"  Fetching {symbol} {timeframe} from {start} to {end}...")
    ticker = yf.download(symbol, start=start, end=end, interval=timeframe,
                         progress=False, auto_adjust=True)
    if ticker.empty:
        print(f"  No data returned for {symbol}")
        return None
    if isinstance(ticker.columns, pd.MultiIndex):
        ticker.columns = ticker.columns.get_level_values(0)
    ticker.columns = [c.lower() for c in ticker.columns]
    df = ticker.reset_index()
    for col in ["Datetime", "datetime", "date", "index"]:
        if col in df.columns:
            df = df.rename(columns={col: "time"})
            break
    df = df[["time", "open", "high", "low", "close", "volume"]].dropna()
    print(f"    Got {len(df)} candles ({df['time'].iloc[0].date()} to {df['time'].iloc[-1].date()})")
    return df


def detect_breakout_signal(df: pd.DataFrame, i: int) -> tuple[bool, int]:
    """Returns (is_breakout, candles_above_ema50) looking back up to 4 candles."""
    look = min(12, i)
    close_arr = df["close"].values
    ema50_arr = df["ema50"].values
    candles_above = 0
    crossed = False
    for j in range(i, max(i - look, -1), -1):
        if close_arr[j] > ema50_arr[j]:
            candles_above += 1
        else:
            if candles_above > 0:
                crossed = True
            break
    return (crossed and 0 < candles_above <= 4), candles_above


def run_period_backtest(symbol: str) -> dict:
    config  = get_symbol_config(symbol)

    signal_df_raw = fetch_range(symbol, "1h",  WARMUP_START, TRADE_END)
    trend_df_raw  = fetch_range(symbol, "4h",  WARMUP_START, TRADE_END)
    if signal_df_raw is None or trend_df_raw is None:
        return {}

    signal_df = calculate_indicators(signal_df_raw.copy())
    trend_df  = calculate_indicators(trend_df_raw.copy())
    df        = attach_higher_timeframe_context(signal_df, trend_df)

    # Convert to datetime for filtering — strip timezone so comparisons work
    df["time"] = pd.to_datetime(df["time"]).dt.tz_localize(None)
    trade_start = pd.Timestamp(TRADE_START)
    trade_end   = pd.Timestamp(TRADE_END)

    balance  = START_BALANCE
    position = None
    trades   = []
    breakout_entries = 0

    for i in range(1, len(df)):
        row      = df.iloc[i]
        prev_row = df.iloc[i - 1]
        ts       = row["time"]
        price    = row["close"]

        # Only trade within target window
        in_window = trade_start <= ts <= trade_end

        # Position management (always active while we have a position)
        if position:
            held_h = (ts - pd.Timestamp(position["entry_time"])).total_seconds() / 3600
            atr    = row["atr"] if not pd.isna(row["atr"]) else position["atr_at_entry"]

            # Trailing stop for ETH/SOL/ZEC
            if config.get("trailing_stop_multiplier"):
                ts_mult = config["trailing_stop_multiplier"]
                act_mult = config.get("trail_activation_multiplier")
                if act_mult is None or price >= position["entry"] + atr * act_mult:
                    trailing = price - atr * ts_mult
                    position["stop_price"] = max(position["stop_price"], trailing)

            hit_stop   = price <= position["stop_price"]
            hit_target = price >= position["target_price"]
            hit_hold   = config.get("max_hold_hours") and held_h >= config["max_hold_hours"]

            if hit_stop or hit_target or hit_hold or ts > trade_end:
                reason = ("STOP_LOSS" if hit_stop else
                          "TAKE_PROFIT" if hit_target else
                          "MAX_HOLD" if hit_hold else "END_WINDOW")
                ct = close_position(position, price, ts, reason, symbol)
                balance += ct["net_returned"]
                ct["breakout_entry"] = position.get("breakout_entry", False)
                trades.append(ct)
                position = None
            continue

        if not in_window:
            continue

        signal                     = get_signal(row, prev_row, config)
        is_breakout, candles_above = detect_breakout_signal(df, i)

        # BUY: normal signal OR breakout signal with at least RSI not overbought
        rsi = row["rsi"] if not pd.isna(row["rsi"]) else 99
        entry_reason = None
        if signal == "BUY":
            entry_reason = "SIGNAL"
        elif is_breakout and rsi < 65 and row.get("adx", 0) >= 20:
            entry_reason = "BREAKOUT"

        if entry_reason:
            usd     = balance * TRADE_SIZE_PCT
            fee     = usd * FEE_RATE
            cost    = usd + fee
            if cost > balance:
                continue
            atr_val = row["atr"] if not pd.isna(row["atr"]) else price * 0.01
            balance -= cost
            if entry_reason == "BREAKOUT":
                breakout_entries += 1
            position = {
                "side":         "LONG",
                "qty":          usd / price,
                "entry":        price,
                "cost":         usd,
                "entry_time":   ts,
                "stop_price":   price - atr_val * ATR_STOP,
                "target_price": price + atr_val * ATR_TARGET,
                "atr_at_entry": atr_val,
                "entry_reason": entry_reason,
                "candles_above_ema50": candles_above,
                "breakout_entry": entry_reason == "BREAKOUT",
            }
            trades.append({
                "type":         "OPEN",
                "symbol":       symbol,
                "price":        price,
                "time":         ts,
                "entry_reason": entry_reason,
                "rsi":          round(rsi, 1),
                "candles_above": candles_above if is_breakout else 0,
            })

    # Close any open position at end
    if position:
        last = df.iloc[-1]
        ct = close_position(position, last["close"], last["time"], "END_WINDOW", symbol)
        balance += ct["net_returned"]
        ct["breakout_entry"] = position.get("breakout_entry", False)
        trades.append(ct)

    closed  = [t for t in trades if t.get("type") == "CLOSE"]
    opens   = [t for t in trades if t.get("type") == "OPEN"]
    wins    = [t for t in closed if t["pnl_usd"] > 0]
    losses  = [t for t in closed if t["pnl_usd"] <= 0]
    ret_pct = (balance - START_BALANCE) / START_BALANCE * 100
    win_rt  = len(wins) / len(closed) * 100 if closed else 0
    avg_win = sum(t["pnl_usd"] for t in wins) / len(wins) if wins else 0
    avg_los = sum(t["pnl_usd"] for t in losses) / len(losses) if losses else 0
    pf      = abs(avg_win / avg_los) if avg_los else float("inf")

    # Print per-trade log
    print(f"\n  --- Trade log for {symbol} ---")
    for o in opens:
        reason_tag = f"[{o['entry_reason']}]" if o.get("entry_reason") == "BREAKOUT" else ""
        print(f"  OPEN  {str(o['time'])[:16]}  ${o['price']:>10,.2f}  RSI={o['rsi']}  {reason_tag}")
    for c in closed:
        pnl_str = f"${c['pnl_usd']:+.2f}"
        tag = " [BREAKOUT]" if c.get("breakout_entry") else ""
        print(f"  CLOSE {str(c['exit_time'])[:16]}  ${c['exit']:>10,.2f}  {c['reason']:<12}  {pnl_str}{tag}")

    return {
        "symbol":           symbol,
        "trades":           len(closed),
        "wins":             len(wins),
        "losses":           len(losses),
        "win_rate":         win_rt,
        "profit_factor":    pf,
        "return_pct":       ret_pct,
        "final_balance":    balance,
        "avg_win":          avg_win,
        "avg_loss":         avg_los,
        "breakout_entries": breakout_entries,
    }


def main():
    print("=" * 65)
    print("  CRYPTO ORCHESTRA — HISTORICAL BACKTEST")
    print(f"  Period: {TRADE_START} to {TRADE_END}")
    print("  Event:  Trump wins US Election (Nov 5, 2024)")
    print("  BTC:    $67,000 -> $93,000+  (+38%)")
    print("=" * 65)

    results = []
    for sym in SYMBOLS:
        print(f"\n[{sym}]")
        r = run_period_backtest(sym)
        if r:
            results.append(r)

    if not results:
        print("No results.")
        return

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
    total_trades = sum(r["trades"] for r in results)
    total_br = sum(r["breakout_entries"] for r in results)
    print(f"\n  Avg return across assets: {total_ret:+.2f}%")
    print(f"  Total trades:             {total_trades}")
    print(f"  Breakout agent entries:   {total_br}")
    print("\n  BTC buy-hold for same period: +38%")
    print("=" * 65)


if __name__ == "__main__":
    main()
