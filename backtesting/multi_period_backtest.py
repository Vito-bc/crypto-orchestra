"""
Multi-Period Backtest — 2 bull + 2 bear periods.

Tests the current strategy (including breakout volume hard gate) across
four distinct market regimes to see where the system earns and where it bleeds.

Periods:
  BULL-1  Trump Rally      Nov 01 – Dec 15 2024   BTC $67k -> $93k  (+38%)
  BULL-2  BTC ATH Run      Dec 15 2024 – Jan 20 2025  BTC $93k -> $108k (+16%)
  BEAR-1  Aug 2024 Crash   Aug 01 – Sep 30 2024   BTC $66k -> $50k  (-24%)
  BEAR-2  Q1 2025 Corr.    Feb 01 – Mar 31 2025   BTC $108k -> $77k (-29%)

Run:
    python backtesting/multi_period_backtest.py
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np

from backtesting.backtest import (
    attach_higher_timeframe_context,
    calculate_indicators,
    close_position,
    get_signal,
    get_symbol_config,
)

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOLS        = ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]
START_BALANCE  = 10_000.0
TRADE_SIZE_PCT = 0.05
FEE_RATE       = 0.006
ATR_STOP       = 2.5
ATR_TARGET     = 2.0   # reduced from 4.0 — matching live system change

# Breakout volume hard gate — mirrors BreakoutAgent._MIN_VOL_RATIO
_MIN_VOL_RATIO        = 0.8
_MAX_CANDLES_SINCE    = 4
_MAX_RSI_AT_CROSS     = 65.0
_MIN_ADX              = 20.0

PERIODS = [
    {
        "label":       "BULL-1: Trump Rally",
        "event":       "Trump wins election Nov 5, BTC $67k -> $93k (+38%)",
        "warmup":      "2024-08-01",
        "start":       "2024-11-01",
        "end":         "2024-12-15",
        "btc_context": "+38%",
        "regime":      "BULL",
    },
    {
        "label":       "BULL-2: BTC ATH Run",
        "event":       "BTC breaks ATH, alt season begins, $93k -> $108k (+16%)",
        "warmup":      "2024-10-15",
        "start":       "2024-12-15",
        "end":         "2025-01-20",
        "btc_context": "+16%",
        "regime":      "BULL",
    },
    {
        "label":       "BEAR-1: Aug 2024 Crash",
        "event":       "Global equity panic, BTC $66k -> $50k (-24%)",
        "warmup":      "2024-07-01",
        "start":       "2024-08-01",
        "end":         "2024-09-30",
        "btc_context": "-24%",
        "regime":      "BEAR",
    },
    {
        "label":       "BEAR-2: Q1 2025 Correction",
        "event":       "Post-ATH unwind, BTC $108k -> $77k (-29%)",
        "warmup":      "2024-12-01",
        "start":       "2025-02-01",
        "end":         "2025-03-31",
        "btc_context": "-29%",
        "regime":      "BEAR",
    },
]


# ── Data fetch ─────────────────────────────────────────────────────────────────

def fetch_range(symbol: str, timeframe: str, start: str, end: str) -> pd.DataFrame | None:
    import yfinance as yf
    ticker = yf.download(symbol, start=start, end=end, interval=timeframe,
                         progress=False, auto_adjust=True)
    if ticker.empty:
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
    return df


# ── Breakout detection (with volume hard gate) ─────────────────────────────────

def detect_breakout(df: pd.DataFrame, i: int) -> tuple[bool, int]:
    """
    Replicates BreakoutAgent logic including the volume hard gate (0.8x avg).
    Returns (is_valid_breakout, candles_above_ema50).
    """
    look        = min(12, i)
    close_arr   = df["close"].values
    ema50_arr   = df["ema50"].values
    candles_above = 0
    crossed = False

    for j in range(i, max(i - look, -1), -1):
        if close_arr[j] > ema50_arr[j]:
            candles_above += 1
        else:
            if candles_above > 0:
                crossed = True
            break

    if not crossed or candles_above == 0 or candles_above > _MAX_CANDLES_SINCE:
        return False, candles_above

    # Volume hard gate
    vol_ratio = float(df["volume_ratio"].iloc[i]) if "volume_ratio" in df.columns else 1.0
    if np.isnan(vol_ratio):
        vol_ratio = 1.0
    if vol_ratio < _MIN_VOL_RATIO:
        return False, candles_above  # blocked by hard gate

    return True, candles_above


# ── Single period × single symbol ─────────────────────────────────────────────

def run_one(symbol: str, period: dict, use_4h_filter: bool = False) -> dict:
    config = get_symbol_config(symbol)

    df1h_raw = fetch_range(symbol, "1h", period["warmup"], period["end"])
    df4h_raw = fetch_range(symbol, "4h", period["warmup"], period["end"])
    if df1h_raw is None or df4h_raw is None:
        return {}

    df1h = calculate_indicators(df1h_raw.copy())
    df4h = calculate_indicators(df4h_raw.copy())
    df   = attach_higher_timeframe_context(df1h, df4h)

    df["time"] = pd.to_datetime(df["time"]).dt.tz_localize(None)
    t_start = pd.Timestamp(period["start"])
    t_end   = pd.Timestamp(period["end"])

    balance  = START_BALANCE
    position = None
    trades   = []
    breakout_entries  = 0
    vol_blocked       = 0
    trend_4h_blocked  = 0

    for i in range(1, len(df)):
        row      = df.iloc[i]
        prev_row = df.iloc[i - 1]
        ts       = row["time"]
        price    = row["close"]
        in_window = t_start <= ts <= t_end

        if position:
            held_h = (ts - pd.Timestamp(position["entry_time"])).total_seconds() / 3600
            atr    = row["atr"] if not pd.isna(row["atr"]) else position["atr_at_entry"]

            if config.get("trailing_stop_multiplier"):
                ts_mult  = config["trailing_stop_multiplier"]
                act_mult = config.get("trail_activation_multiplier")
                if act_mult is None or price >= position["entry"] + atr * act_mult:
                    trailing = price - atr * ts_mult
                    position["stop_price"] = max(position["stop_price"], trailing)

            hit_stop   = price <= position["stop_price"]
            hit_target = price >= position["target_price"]
            hit_hold   = config.get("max_hold_hours") and held_h >= config["max_hold_hours"]

            if hit_stop or hit_target or hit_hold or ts > t_end:
                reason = ("STOP_LOSS"  if hit_stop   else
                          "TAKE_PROFIT" if hit_target else
                          "MAX_HOLD"   if hit_hold   else "END_WINDOW")
                ct = close_position(position, price, ts, reason, symbol)
                balance += ct["net_returned"]
                ct["breakout_entry"] = position.get("breakout_entry", False)
                trades.append(ct)
                position = None
            continue

        if not in_window:
            continue

        signal               = get_signal(row, prev_row, config)
        is_breakout, n_above = detect_breakout(df, i)

        # Count volume-blocked crossovers
        vol_ratio = float(row.get("volume_ratio", 1.0) or 1.0)
        if not is_breakout and n_above > 0 and n_above <= _MAX_CANDLES_SINCE and vol_ratio < _MIN_VOL_RATIO:
            vol_blocked += 1

        # 4h trend filter: only enter breakout if 4h price is above 4h EMA50
        if use_4h_filter and is_breakout:
            close_4h = float(row.get("close_4h", np.nan))
            ema50_4h = float(row.get("ema50_4h",  np.nan))
            if not np.isnan(close_4h) and not np.isnan(ema50_4h):
                if close_4h < ema50_4h:
                    trend_4h_blocked += 1
                    is_breakout = False  # 4h bearish — block this entry

        rsi = float(row["rsi"]) if not pd.isna(row["rsi"]) else 99
        entry_reason = None
        if signal == "BUY":
            entry_reason = "SIGNAL"
        elif is_breakout and rsi < _MAX_RSI_AT_CROSS:
            entry_reason = "BREAKOUT"

        if entry_reason:
            usd  = balance * TRADE_SIZE_PCT
            cost = usd * (1 + FEE_RATE)
            if cost > balance:
                continue
            atr_val = row["atr"] if not pd.isna(row["atr"]) else price * 0.01
            balance -= cost
            if entry_reason == "BREAKOUT":
                breakout_entries += 1
            position = {
                "side":           "LONG",
                "qty":            usd / price,
                "entry":          price,
                "cost":           usd,
                "entry_time":     ts,
                "stop_price":     price - atr_val * ATR_STOP,
                "target_price":   price + atr_val * ATR_TARGET,
                "atr_at_entry":   atr_val,
                "entry_reason":   entry_reason,
                "breakout_entry": entry_reason == "BREAKOUT",
                "vol_ratio":      round(vol_ratio, 2),
            }
            trades.append({
                "type": "OPEN", "symbol": symbol, "price": price,
                "time": ts, "entry_reason": entry_reason,
                "rsi": round(rsi, 1), "vol_ratio": round(vol_ratio, 2),
            })

    if position:
        last = df.iloc[-1]
        ct = close_position(position, last["close"], last["time"], "END_WINDOW", symbol)
        balance += ct["net_returned"]
        ct["breakout_entry"] = position.get("breakout_entry", False)
        trades.append(ct)

    closed  = [t for t in trades if t.get("type") == "CLOSE"]
    wins    = [t for t in closed if t["pnl_usd"] > 0]
    losses  = [t for t in closed if t["pnl_usd"] <= 0]
    ret_pct = (balance - START_BALANCE) / START_BALANCE * 100
    win_rt  = len(wins) / len(closed) * 100 if closed else 0
    avg_win = sum(t["pnl_usd"] for t in wins)   / len(wins)   if wins   else 0
    avg_los = sum(t["pnl_usd"] for t in losses) / len(losses) if losses else 0
    pf      = abs(avg_win / avg_los) if avg_los else float("inf")

    return {
        "symbol":          symbol,
        "trades":          len(closed),
        "wins":            len(wins),
        "losses":          len(losses),
        "win_rate":        win_rt,
        "profit_factor":   pf,
        "return_pct":      ret_pct,
        "final_balance":   balance,
        "avg_win":         avg_win,
        "avg_loss":        avg_los,
        "breakout_entries":  breakout_entries,
        "vol_blocked":       vol_blocked,
        "trend_4h_blocked":  trend_4h_blocked,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def _run_period(period: dict, use_4h_filter: bool) -> list[dict]:
    results = []
    for sym in SYMBOLS:
        r = run_one(sym, period, use_4h_filter=use_4h_filter)
        if r:
            results.append(r)
    return results


def _avg(results: list[dict], key: str) -> float:
    vals = [r[key] for r in results if key in r]
    return sum(vals) / len(vals) if vals else 0.0


def main():
    print()
    print("=" * 72)
    print("  CRYPTO ORCHESTRA — MULTI-PERIOD BACKTEST")
    print("  Comparing: NO 4h filter  vs  WITH 4h trend filter (close > EMA50_4h)")
    print("=" * 72)

    summary_rows = []

    for period in PERIODS:
        print()
        print(f"  [{period['regime']}] {period['label']}  |  BTC buy-hold: {period['btc_context']}")
        print(f"  {period['event']}")
        print(f"  {period['start']} -> {period['end']}")
        print()
        print(f"  {'Symbol':<10} {'No filter':>10} {'4h filter':>10} {'Delta':>8} "
              f"{'Trades(no)':>11} {'Trades(4h)':>11} {'4h_blocked':>11}")
        print("  " + "-" * 74)

        base_results = _run_period(period, use_4h_filter=False)
        filt_results = _run_period(period, use_4h_filter=True)

        for b, f in zip(base_results, filt_results):
            delta = f["return_pct"] - b["return_pct"]
            sign_b = "+" if b["return_pct"] >= 0 else ""
            sign_f = "+" if f["return_pct"] >= 0 else ""
            sign_d = "+" if delta >= 0 else ""
            print(f"  {b['symbol']:<10} {sign_b}{b['return_pct']:>+7.2f}%  "
                  f"{sign_f}{f['return_pct']:>+7.2f}%  "
                  f"{sign_d}{delta:>+5.2f}%  "
                  f"{b['trades']:>10}  {f['trades']:>10}  {f['trend_4h_blocked']:>10}")

        avg_base = _avg(base_results, "return_pct")
        avg_filt = _avg(filt_results, "return_pct")
        avg_delta = avg_filt - avg_base
        sign_d = "+" if avg_delta >= 0 else ""
        print(f"  {'  AVG':<10} {avg_base:>+8.2f}%  {avg_filt:>+8.2f}%  "
              f"{sign_d}{avg_delta:>+5.2f}%")

        summary_rows.append({
            "label":      period["label"],
            "regime":     period["regime"],
            "btc_bh":     period["btc_context"],
            "avg_base":   avg_base,
            "avg_filt":   avg_filt,
            "avg_delta":  avg_delta,
        })

    # ── Final comparison table ────────────────────────────────────────────────
    print()
    print("=" * 72)
    print("  SUMMARY: No 4h filter  vs  4h trend filter")
    print("=" * 72)
    print(f"  {'Period':<28} {'BTC':>6} {'No filter':>10} {'4h filter':>10} {'Delta':>8}")
    print("  " + "-" * 66)
    for row in summary_rows:
        sign_d = "+" if row["avg_delta"] >= 0 else ""
        regime_tag = "[BULL]" if row["regime"] == "BULL" else "[BEAR]"
        print(f"  {regime_tag} {row['label'][:24]:<24} {row['btc_bh']:>6}  "
              f"{row['avg_base']:>+8.2f}%  {row['avg_filt']:>+8.2f}%  "
              f"{sign_d}{row['avg_delta']:>+5.2f}%")
    print("=" * 72)
    print()
    grand_base = sum(r["avg_base"]  for r in summary_rows) / len(summary_rows)
    grand_filt = sum(r["avg_filt"]  for r in summary_rows) / len(summary_rows)
    grand_delt = grand_filt - grand_base
    bull_base = _avg([r for r in summary_rows if r["regime"]=="BULL"], "avg_base")
    bull_filt = _avg([r for r in summary_rows if r["regime"]=="BULL"], "avg_filt")
    bear_base = _avg([r for r in summary_rows if r["regime"]=="BEAR"], "avg_base")
    bear_filt = _avg([r for r in summary_rows if r["regime"]=="BEAR"], "avg_filt")
    print(f"  Grand avg (all periods):  {grand_base:>+.2f}%  ->  {grand_filt:>+.2f}%  ({grand_delt:>+.2f}%)")
    print(f"  Bull periods only:        {bull_base:>+.2f}%  ->  {bull_filt:>+.2f}%")
    print(f"  Bear periods only:        {bear_base:>+.2f}%  ->  {bear_filt:>+.2f}%")
    print()


if __name__ == "__main__":
    main()
