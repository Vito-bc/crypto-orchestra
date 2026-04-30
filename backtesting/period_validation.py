"""
Multi-Period Validation — does the best config hold across market regimes?

Tests the "3-agent filter" config on 4 distinct market periods:
  Oct-Dec 2024  BULL RUN   — training period (should match tuning results)
  Jan-Mar 2024  BULL 2     — another bull, tests generalization
  Apr-Jun 2024  CORRECTION — post-ATH selloff, tests bear resilience
  Jul-Sep 2024  RANGING    — sideways chop, tests false-signal filtering

Also shows buy-and-hold benchmark for each period.

Usage:
    python backtesting/period_validation.py
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
    FEE_RATE,
    START_BALANCE,
    TRADE_SIZE_PCT,
    attach_higher_timeframe_context,
    calculate_indicators,
    close_position,
    get_signal,
    get_symbol_config,
    macd_buy_ok,
    macd_sell_ok,
)
from tools.price_levels import get_levels

WARMUP_DAYS = 60
SYMBOLS     = ["BTC-USD", "ETH-USD"]

WEIGHTS = {
    "macro":     0.30,
    "technical": 0.25,
    "whale":     0.20,
    "sentiment": 0.15,
    "risk":      0.10,
}

# Best config from tuning — 3-agent filter + all fixes
BEST_ATR_STOP      = 2.0
BEST_ATR_TARGET    = 4.0
VETO_TS_THRESHOLD  = -0.002   # relaxed veto
TREND_SENTIMENT    = True
MIN_AGENTS         = 3
MIN_BUY_SCORE      = 0.28


@dataclass
class Period:
    name:  str
    label: str    # regime description
    start: str
    end:   str


PERIODS = [
    Period("Oct-Dec 2024", "BULL RUN  (training)",   "2024-10-01", "2024-12-31"),
    Period("Jul-Sep 2024", "RANGING   (sideways)",   "2024-07-01", "2024-09-30"),
    Period("Jan-Mar 2025", "POST-ATH CORRECTION",    "2025-01-01", "2025-03-31"),
    Period("Jun-Aug 2025", "SUMMER 2025",            "2025-06-01", "2025-08-31"),
]


# ── Data fetch ────────────────────────────────────────────────────────────────

def fetch_range(symbol: str, timeframe: str, start: str, end: str) -> pd.DataFrame | None:
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


# ── Agent simulators (same as agent_backtest_tune.py) ────────────────────────

def sim_technical(row, prev_row, config) -> tuple[str, float]:
    signal = get_signal(row, prev_row, config)
    if signal == "BUY":
        aligned = sum([row["rsi"] < config["buy_rsi_max"], macd_buy_ok(row, config), row["bb_pct"] < config["buy_bb_pct_max"]])
        return "BUY", 0.40 + aligned * 0.15
    if signal == "SELL":
        aligned = sum([row["rsi"] > config["sell_rsi_min"], macd_sell_ok(row, config), row["bb_pct"] > config["sell_bb_pct_min"]])
        return "SELL", 0.40 + aligned * 0.15
    return "NEUTRAL", 0.45


def sim_macro(row) -> tuple[str, float, bool]:
    close_4h  = row.get("close_4h",  np.nan)
    ema50_4h  = row.get("ema50_4h",  np.nan)
    ema200_4h = row.get("ema200_4h", np.nan)
    ts        = row.get("trend_strength_4h", 0) or 0
    if any(pd.isna(v) for v in [close_4h, ema50_4h, ema200_4h]):
        return "NEUTRAL", 0.40, False
    bear_ema_cross = close_4h < ema200_4h and ema50_4h < ema200_4h
    if bear_ema_cross and ts < VETO_TS_THRESHOLD:
        return "SELL", 0.80, True
    if close_4h > ema200_4h and ema50_4h > ema200_4h:
        return "BUY", min(0.85, 0.60 + abs(ts) * 5), False
    return "NEUTRAL", 0.50, False


def sim_sentiment(row) -> tuple[str, float]:
    mom = row.get("momentum_20", 0) or 0
    if mom > 8:    return "BUY",  0.55
    if mom > 3:    return "BUY",  0.45
    if mom < -8:   return "SELL", 0.55
    if mom < -3:   return "SELL", 0.45
    return "NEUTRAL", 0.40


def sim_whale(row) -> tuple[str, float]:
    vr  = row.get("volume_ratio", 1.0) or 1.0
    mom = row.get("momentum_20", 0) or 0
    if vr > 1.8 and mom < -3: return "SELL", 0.70
    if vr > 1.8 and mom >  3: return "BUY",  0.68
    if vr < 0.5:               return "NEUTRAL", 0.30
    return "NEUTRAL", 0.50


def sim_orchestrator(signals: dict, bull_mode: bool = False) -> tuple[str, float, bool]:
    _, _, bear_veto = signals["macro"]
    if bear_veto:
        return "HOLD", 0.0, True
    buy_count = sell_count = 0
    buy_score = sell_score = 0.0
    for agent, val in signals.items():
        sig, conf = val[0], val[1]
        w = WEIGHTS.get(agent, 0.1)
        if sig == "BUY":   buy_count += 1; buy_score  += w * conf
        elif sig == "SELL": sell_count += 1; sell_score += w * conf
    if buy_count >= MIN_AGENTS and buy_score > MIN_BUY_SCORE and buy_score > sell_score:
        return "BUY",  round(buy_score, 3), False
    if sell_count >= MIN_AGENTS and sell_score > MIN_BUY_SCORE and sell_score > buy_score:
        return "SELL", round(sell_score, 3), False
    return "HOLD", round(max(buy_score, sell_score), 3), False


# ── Single period run ─────────────────────────────────────────────────────────

def run_period(df: pd.DataFrame, symbol: str, period: Period) -> dict:
    sym_config = get_symbol_config(symbol)
    balance    = START_BALANCE
    position   = None
    trades     = []
    equity     = []

    for i in range(1, len(df)):
        row      = df.iloc[i]
        prev_row = df.iloc[i - 1]
        price    = row["close"]
        eq = balance + (position["qty"] * price if position else 0)
        equity.append(eq)

        if position:
            held_h = (pd.Timestamp(row["time"]) - pd.Timestamp(position["entry_time"])).total_seconds() / 3600
            tm = sym_config.get("trailing_stop_multiplier")
            ta = sym_config.get("trail_activation_multiplier")
            be = sym_config.get("break_even_trigger_atr")
            if be and price >= position["entry"] + row["atr"] * be:
                position["stop_price"] = max(position["stop_price"], position["entry"])
            if tm and (ta is None or price >= position["entry"] + row["atr"] * ta):
                position["stop_price"] = max(position["stop_price"], price - row["atr"] * tm)
            hit_stop   = price <= position["stop_price"]
            hit_target = price >= position["target_price"]
            hit_max    = sym_config.get("max_hold_hours") and held_h >= sym_config["max_hold_hours"]
            if hit_stop or hit_target or hit_max:
                reason = "STOP_LOSS" if hit_stop else ("TAKE_PROFIT" if hit_target else "MAX_HOLD")
                ct = close_position(position, price, row["time"], reason, symbol)
                balance += ct["net_returned"]; trades.append(ct); position = None
                continue

        tech_sig,  tech_conf          = sim_technical(row, prev_row, sym_config)
        macro_sig, macro_conf, b_veto = sim_macro(row)
        sent_sig,  sent_conf          = sim_sentiment(row)
        whale_sig, whale_conf         = sim_whale(row)

        signals = {
            "macro":     (macro_sig, macro_conf, b_veto),
            "technical": (tech_sig,  tech_conf),
            "sentiment": (sent_sig,  sent_conf),
            "whale":     (whale_sig, whale_conf),
            "risk":      ("NEUTRAL", 0.85),
        }

        action, conf, veto = sim_orchestrator(signals)

        # S/R filter: BUY only when price is near a key support level
        # This is the entry timing improvement — avoids mid-range entries
        if action == "BUY" and not veto:
            levels = get_levels(df, i, lookback=150, n_swing=5)
            if not levels["at_support"]:
                action = "HOLD"   # good signal, wrong timing

        if action == "SELL" and position:
            ct = close_position(position, price, row["time"], "SIGNAL", symbol)
            balance += ct["net_returned"]; trades.append(ct); position = None
            continue
        if position:
            continue
        if action == "BUY":
            usd = balance * TRADE_SIZE_PCT
            fee = usd * FEE_RATE
            if usd + fee > balance:
                continue
            balance -= usd + fee
            position = {
                "side": "LONG", "qty": usd / price, "entry": price,
                "cost": usd, "entry_time": row["time"],
                "stop_price":   price - row["atr"] * BEST_ATR_STOP,
                "target_price": price + row["atr"] * BEST_ATR_TARGET,
                "entry_context": {},
            }
            trades.append({"type": "OPEN", "side": "LONG", "symbol": symbol,
                           "price": price, "time": row["time"]})

    if position:
        ct = close_position(position, df["close"].iloc[-1], df["time"].iloc[-1], "END_OF_TEST", symbol)
        balance += ct["net_returned"]; trades.append(ct)

    closed = [t for t in trades if t["type"] == "CLOSE"]
    wins   = [t for t in closed if t["pnl_usd"] > 0]
    losses = [t for t in closed if t["pnl_usd"] <= 0]
    total_return  = (balance - START_BALANCE) / START_BALANCE * 100
    win_rate      = len(wins) / len(closed) * 100 if closed else 0
    avg_win       = sum(t["pnl_usd"] for t in wins)   / len(wins)   if wins   else 0
    avg_loss      = sum(t["pnl_usd"] for t in losses) / len(losses) if losses else 0
    profit_factor = abs(avg_win / avg_loss) if avg_loss and avg_win else (float("inf") if wins else 0)

    peak = START_BALANCE; max_dd = 0
    for val in equity:
        if val > peak: peak = val
        dd = (peak - val) / peak
        if dd > max_dd: max_dd = dd

    sl_count = sum(1 for t in closed if t["reason"] == "STOP_LOSS")
    tp_count = sum(1 for t in closed if t["reason"] == "TAKE_PROFIT")

    # Buy-and-hold benchmark
    bah_return = (df["close"].iloc[-1] - df["close"].iloc[0]) / df["close"].iloc[0] * 100

    return {
        "period":        period.name,
        "regime":        period.label,
        "return":        total_return,
        "bah_return":    bah_return,
        "vs_bah":        total_return - bah_return,
        "trades":        len(closed),
        "win_rate":      win_rate,
        "profit_factor": profit_factor,
        "max_drawdown":  max_dd * 100,
        "stop_losses":   sl_count,
        "take_profits":  tp_count,
        "avg_win_usd":   avg_win,
        "avg_loss_usd":  avg_loss,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\nCrypto Orchestra -- Multi-Period Validation")
    print("Config: 3-agent filter + trend sentiment + wider stops (2.0x/4.0x)")
    print("=" * 75)

    for symbol in SYMBOLS:
        print(f"\n{'='*75}")
        print(f"  {symbol}")
        print(f"{'='*75}")
        print(f"  {'Period':<16} {'Regime':<26} {'Return':>7} {'B&H':>7} {'vs B&H':>7} {'WR':>6} {'PF':>5} {'Trades':>7} {'DD':>5}")
        print(f"  {'-'*73}")

        all_results = []
        for period in PERIODS:
            signal_df = fetch_range(symbol, "1h", period.start, period.end)
            trend_df  = fetch_range(symbol, "4h", period.start, period.end)
            if signal_df is None or trend_df is None:
                print(f"  {period.name:<16}  No data available.")
                continue

            df = attach_higher_timeframe_context(signal_df, trend_df)
            start_ts = pd.Timestamp(period.start, tz="UTC")
            df["time"] = pd.to_datetime(df["time"], utc=True)
            df = df[df["time"] >= start_ts].reset_index(drop=True)

            r = run_period(df, symbol, period)
            all_results.append(r)

            bah_str = f"{r['bah_return']:+.1f}%"
            vs_str  = f"{r['vs_bah']:+.1f}%"
            verdict = "PASS" if r["return"] > 0 else ("CLOSE" if r["return"] > -0.5 else "FAIL")

            print(
                f"  {r['period']:<16} {r['regime']:<26} "
                f"{r['return']:>+6.2f}% {bah_str:>7} {vs_str:>7} "
                f"{r['win_rate']:>5.1f}% "
                f"{r['profit_factor']:>5.2f} "
                f"{r['trades']:>7} "
                f"{r['max_drawdown']:>4.1f}%  {verdict}"
            )

        if all_results:
            print(f"  {'-'*73}")
            avg_ret = sum(r["return"] for r in all_results) / len(all_results)
            avg_wr  = sum(r["win_rate"] for r in all_results) / len(all_results)
            avg_pf  = sum(r["profit_factor"] for r in all_results) / len(all_results)
            print(f"  {'AVERAGE':<16} {'across all periods':<26} {avg_ret:>+6.2f}%  {'':>7} {'':>7} {avg_wr:>5.1f}% {avg_pf:>5.2f}")

    print(f"\n{'='*75}")
    print("  B&H = Buy and Hold benchmark for the period")
    print("  vs B&H = system return minus buy-and-hold (positive = outperformed)")
    print("  PASS = profitable  |  CLOSE = <0.5% loss  |  FAIL = >0.5% loss")
