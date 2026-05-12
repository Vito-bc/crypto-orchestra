"""
Trailing Stop Grid Search — find break-even and trail percentages that
maximise return across all market periods.

Grid:
  break_even_pct       : 0.5%, 1.0%, 1.5%, 2.0%
  trail_activation_pct : same as break_even_pct or +0.5% above it
  trail_pct            : 0.5%, 0.8%, 1.0%, 1.5%
  none                 : no trailing stop (baseline)

Uses limit-order fees (0.2% entry + 0.4% exit) and stop=2.5x/target=4.0x ATR.
Best params are printed at the end for copy-paste into position_tracker.py.

Usage:
    python backtesting/trailing_stop_tune.py
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting.backtest import (
    START_BALANCE,
    TRADE_SIZE_PCT,
    attach_higher_timeframe_context,
    calculate_indicators,
    get_signal,
    get_symbol_config,
    macd_buy_ok,
    macd_sell_ok,
)
from tools.price_levels import get_levels

ENTRY_FEE   = 0.002
EXIT_FEE    = 0.004
WARMUP_DAYS = 60
ATR_STOP    = 2.5
ATR_TARGET  = 4.0
SYMBOLS     = ["BTC-USD", "ETH-USD"]

WEIGHTS           = {"macro": 0.30, "technical": 0.25, "whale": 0.20, "sentiment": 0.15, "risk": 0.10}
VETO_TS_THRESHOLD = -0.002
MIN_AGENTS        = 3
MIN_BUY_SCORE     = 0.28

PERIODS = [
    ("Oct-Dec 2024", "2024-10-01", "2024-12-31"),
    ("Jan-Mar 2025", "2025-01-01", "2025-03-31"),
    ("Jun-Aug 2025", "2025-06-01", "2025-08-31"),
]


@dataclass
class TrailConfig:
    label:               str
    break_even_pct:      float         # % above entry to trigger break-even
    trail_activation_pct: float        # % above entry to start trailing
    trail_pct:           float         # % below HWM for trailing stop
    active:              bool = True   # False = no trailing stop (baseline)


CONFIGS: list[TrailConfig] = [
    TrailConfig("No trail (baseline)", 0, 0, 0, active=False),
]
for be in [0.005, 0.010, 0.015, 0.020]:
    for trail in [0.005, 0.008, 0.010, 0.015]:
        CONFIGS.append(TrailConfig(
            label=f"BE={be:.1%} trail={trail:.1%}",
            break_even_pct=be,
            trail_activation_pct=be,   # trail activates at same level as BE
            trail_pct=trail,
        ))


# ── Data fetch ────────────────────────────────────────────────────────────────

def fetch_range(symbol, timeframe, start, end):
    warmup = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=WARMUP_DAYS)).strftime("%Y-%m-%d")
    ticker = yf.download(symbol, start=warmup, end=end,
                         interval=timeframe, progress=False, auto_adjust=True)
    if ticker.empty:
        return None
    if isinstance(ticker.columns, pd.MultiIndex):
        ticker.columns = ticker.columns.get_level_values(0)
    ticker.columns = [c.lower() for c in ticker.columns]
    df = ticker.reset_index()
    for col in ["Datetime", "datetime", "date", "index"]:
        if col in df.columns:
            df = df.rename(columns={col: "time"}); break
    df = df[["time", "open", "high", "low", "close", "volume"]].dropna()
    df = calculate_indicators(df)
    df["momentum_20"] = df["close"].pct_change(20) * 100
    return df


# ── Agent simulators ──────────────────────────────────────────────────────────

def sim_agents(row, prev_row, config):
    sig = get_signal(row, prev_row, config)
    if sig == "BUY":
        aligned = sum([row["rsi"] < config["buy_rsi_max"], macd_buy_ok(row, config), row["bb_pct"] < config["buy_bb_pct_max"]])
        tech = ("BUY", 0.40 + aligned * 0.15)
    else:
        tech = ("NEUTRAL", 0.45)

    c4, e50, e200 = row.get("close_4h", np.nan), row.get("ema50_4h", np.nan), row.get("ema200_4h", np.nan)
    ts = row.get("trend_strength_4h", 0) or 0
    if any(pd.isna(v) for v in [c4, e50, e200]):
        macro = ("NEUTRAL", 0.40, False)
    elif c4 < e200 and e50 < e200 and ts < VETO_TS_THRESHOLD:
        macro = ("SELL", 0.80, True)
    elif c4 > e200 and e50 > e200:
        macro = ("BUY", min(0.85, 0.60 + abs(ts) * 5), False)
    else:
        macro = ("NEUTRAL", 0.50, False)

    mom = row.get("momentum_20", 0) or 0
    sent = ("BUY", 0.55) if mom > 8 else ("BUY", 0.45) if mom > 3 else \
           ("SELL", 0.55) if mom < -8 else ("SELL", 0.45) if mom < -3 else ("NEUTRAL", 0.40)
    vr = row.get("volume_ratio", 1.0) or 1.0
    whale = ("BUY", 0.68) if vr > 1.8 and mom > 3 else \
            ("SELL", 0.70) if vr > 1.8 and mom < -3 else \
            ("NEUTRAL", 0.30) if vr < 0.5 else ("NEUTRAL", 0.50)

    signals = {"macro": macro, "technical": tech, "sentiment": sent,
               "whale": whale, "risk": ("NEUTRAL", 0.85)}

    _, _, bear_veto = macro
    if bear_veto:
        return "HOLD"
    buy_count = buy_score = 0
    for agent, val in signals.items():
        s, c = val[0], val[1]
        w = WEIGHTS.get(agent, 0.1)
        if s == "BUY":
            buy_count += 1; buy_score += w * c
    return "BUY" if buy_count >= MIN_AGENTS and buy_score > MIN_BUY_SCORE else "HOLD"


# ── Single run ────────────────────────────────────────────────────────────────

def run_period(df: pd.DataFrame, symbol: str, cfg: TrailConfig) -> dict:
    sym_cfg  = get_symbol_config(symbol)
    balance  = START_BALANCE
    position = None
    trades   = []
    equity   = []

    for i in range(1, len(df)):
        row      = df.iloc[i]
        prev_row = df.iloc[i - 1]
        price    = row["close"]
        eq = balance + (position["qty"] * price if position else 0)
        equity.append(eq)

        if position:
            held_h = (pd.Timestamp(row["time"]) - pd.Timestamp(position["entry_time"])).total_seconds() / 3600

            # Update high-water mark
            if price > position["hwm"]:
                position["hwm"] = price

            # Trailing stop logic
            if cfg.active:
                new_stop = position["stop_price"]
                hwm = position["hwm"]
                if price >= position["entry"] * (1 + cfg.break_even_pct):
                    new_stop = max(new_stop, position["entry"])
                if hwm >= position["entry"] * (1 + cfg.trail_activation_pct):
                    trail = hwm * (1 - cfg.trail_pct)
                    new_stop = max(new_stop, trail)
                position["stop_price"] = new_stop

            hit_stop   = price <= position["stop_price"]
            hit_target = price >= position["target_price"]
            hit_max    = sym_cfg.get("max_hold_hours") and held_h >= sym_cfg["max_hold_hours"]
            if hit_stop or hit_target or hit_max:
                reason = "STOP_LOSS" if hit_stop else ("TAKE_PROFIT" if hit_target else "MAX_HOLD")
                gross  = position["qty"] * price
                net    = gross - gross * EXIT_FEE
                pnl    = net - position["cost"]
                trades.append({"type": "CLOSE", "pnl_usd": pnl,
                                "reason": reason, "net_returned": net})
                balance += net; position = None
                continue

        action = sim_agents(row, prev_row, sym_cfg)
        if action == "BUY":
            levels = get_levels(df, i, lookback=150, n_swing=5)
            if not levels["at_support"]:
                action = "HOLD"

        if position or action != "BUY":
            continue

        usd = balance * TRADE_SIZE_PCT
        fee = usd * ENTRY_FEE
        if usd + fee > balance:
            continue
        balance -= usd + fee
        position = {
            "qty": usd / price, "entry": price, "cost": usd,
            "entry_time": row["time"], "hwm": price,
            "stop_price":   price - row["atr"] * ATR_STOP,
            "target_price": price + row["atr"] * ATR_TARGET,
        }
        trades.append({"type": "OPEN"})

    if position:
        price = df["close"].iloc[-1]
        gross = position["qty"] * price
        net   = gross - gross * EXIT_FEE
        pnl   = net - position["cost"]
        trades.append({"type": "CLOSE", "pnl_usd": pnl,
                        "reason": "END_OF_TEST", "net_returned": net})
        balance += net

    closed = [t for t in trades if t["type"] == "CLOSE"]
    wins   = [t for t in closed if t["pnl_usd"] > 0]
    losses = [t for t in closed if t["pnl_usd"] <= 0]
    total_return  = (balance - START_BALANCE) / START_BALANCE * 100
    win_rate      = len(wins) / len(closed) * 100 if closed else 0
    avg_win       = sum(t["pnl_usd"] for t in wins)   / len(wins)   if wins   else 0
    avg_loss      = sum(t["pnl_usd"] for t in losses) / len(losses) if losses else 0
    profit_factor = abs(avg_win / avg_loss) if avg_loss and avg_win else (float("inf") if wins else 0)
    peak = START_BALANCE; max_dd = 0
    for v in equity:
        if v > peak: peak = v
        dd = (peak - v) / peak
        if dd > max_dd: max_dd = dd
    return {"return": total_return, "trades": len(closed), "win_rate": win_rate,
            "profit_factor": profit_factor, "max_dd": max_dd * 100}


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\nCrypto Orchestra -- Trailing Stop Grid Search")
    print(f"Fees: {ENTRY_FEE:.1%} entry + {EXIT_FEE:.1%} exit  |  stop {ATR_STOP}x ATR  target {ATR_TARGET}x ATR")
    print(f"Testing {len(CONFIGS)} configs across {len(PERIODS)} periods x {len(SYMBOLS)} symbols\n")

    print("Fetching data...")
    cache: dict = {}
    for symbol in SYMBOLS:
        for name, start, end in PERIODS:
            sf = fetch_range(symbol, "1h", start, end)
            tf = fetch_range(symbol, "4h", start, end)
            if sf is not None and tf is not None:
                df = attach_higher_timeframe_context(sf, tf)
                start_ts = pd.Timestamp(start, tz="UTC")
                df["time"] = pd.to_datetime(df["time"], utc=True)
                df = df[df["time"] >= start_ts].reset_index(drop=True)
                cache[(symbol, start)] = df
    print(f"Data ready. Running {len(CONFIGS)} configs...\n")

    summary = []
    for cfg in CONFIGS:
        all_ret, all_wr, all_pf, all_dd, total_trades = [], [], [], [], 0
        for symbol in SYMBOLS:
            for name, start, end in PERIODS:
                df = cache.get((symbol, start))
                if df is None:
                    continue
                r = run_period(df, symbol, cfg)
                all_ret.append(r["return"]); all_wr.append(r["win_rate"])
                all_pf.append(r["profit_factor"]); all_dd.append(r["max_dd"])
                total_trades += r["trades"]
        if not all_ret:
            continue
        summary.append({
            "cfg":     cfg,
            "avg_ret": sum(all_ret) / len(all_ret),
            "avg_wr":  sum(all_wr)  / len(all_wr),
            "avg_pf":  sum(all_pf)  / len(all_pf),
            "avg_dd":  sum(all_dd)  / len(all_dd),
            "trades":  total_trades,
        })

    summary.sort(key=lambda x: x["avg_ret"], reverse=True)

    print(f"{'Rank':<5} {'Config':<30} {'AvgRet':>8} {'WinRate':>8} {'PF':>6} {'MaxDD':>6} {'Trades':>7}")
    print("-" * 75)
    for rank, row in enumerate(summary, 1):
        note = " <-- BEST"    if rank == 1 and row["cfg"].active else ""
        note = " <-- BASELINE" if not row["cfg"].active else note
        print(f"{rank:<5} {row['cfg'].label:<30} {row['avg_ret']:>+7.3f}% "
              f"{row['avg_wr']:>7.1f}% {row['avg_pf']:>6.2f} "
              f"{row['avg_dd']:>5.1f}% {row['trades']:>7}{note}")

    baseline = next(r for r in summary if not r["cfg"].active)
    best     = next(r for r in summary if r["cfg"].active)

    print(f"\n{'='*75}")
    print(f"Baseline (no trail): avg return {baseline['avg_ret']:+.3f}%  WR {baseline['avg_wr']:.1f}%")
    print(f"Best trailing stop : avg return {best['avg_ret']:+.3f}%  WR {best['avg_wr']:.1f}%  "
          f"({best['cfg'].label})")
    delta = best["avg_ret"] - baseline["avg_ret"]
    print(f"Improvement        : {delta:+.3f}% per period on average")
    print(f"\nCopy these into pipeline/position_tracker.py:")
    print(f"  BREAK_EVEN_PCT       = {best['cfg'].break_even_pct}   # {best['cfg'].break_even_pct:.1%}")
    print(f"  TRAIL_ACTIVATION_PCT = {best['cfg'].trail_activation_pct}   # {best['cfg'].trail_activation_pct:.1%}")
    print(f"  TRAIL_PCT            = {best['cfg'].trail_pct}   # {best['cfg'].trail_pct:.1%}")
    print(f"{'='*75}")
