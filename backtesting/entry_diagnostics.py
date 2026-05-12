"""
Entry Diagnostics — why do 80% of support entries fail?

Logs every individual trade with context:
  - Which support level was touched
  - Macro regime at entry
  - Technical alignment score
  - How many candles until stop / target hit
  - Whether a bounce was already forming (1h candle direction)

Shows the patterns that distinguish wins from losses.

Usage:
    python backtesting/entry_diagnostics.py
"""

from __future__ import annotations

import sys
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

WEIGHTS = {"macro": 0.30, "technical": 0.25, "whale": 0.20, "sentiment": 0.15, "risk": 0.10}
VETO_TS_THRESHOLD = -0.002
MIN_AGENTS        = 3
MIN_BUY_SCORE     = 0.28

PERIODS = [
    ("Oct-Dec 2024", "2024-10-01", "2024-12-31"),
    ("Jan-Mar 2025", "2025-01-01", "2025-03-31"),
    ("Jun-Aug 2025", "2025-06-01", "2025-08-31"),
]
SYMBOLS = ["BTC-USD", "ETH-USD"]


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


def sim_agents(row, prev_row, config):
    """Return (signals_dict, tech_score) for a candle."""
    sig = get_signal(row, prev_row, config)
    if sig == "BUY":
        aligned = sum([row["rsi"] < config["buy_rsi_max"], macd_buy_ok(row, config), row["bb_pct"] < config["buy_bb_pct_max"]])
        tech = ("BUY", 0.40 + aligned * 0.15, aligned)
    else:
        tech = ("NEUTRAL", 0.45, 0)

    close_4h  = row.get("close_4h", np.nan)
    ema50_4h  = row.get("ema50_4h", np.nan)
    ema200_4h = row.get("ema200_4h", np.nan)
    ts        = row.get("trend_strength_4h", 0) or 0
    if any(pd.isna(v) for v in [close_4h, ema50_4h, ema200_4h]):
        macro = ("NEUTRAL", 0.40, False)
    elif close_4h < ema200_4h and ema50_4h < ema200_4h and ts < VETO_TS_THRESHOLD:
        macro = ("SELL", 0.80, True)
    elif close_4h > ema200_4h and ema50_4h > ema200_4h:
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

    signals = {
        "macro": macro, "technical": tech,
        "sentiment": sent, "whale": whale, "risk": ("NEUTRAL", 0.85),
    }

    _, _, bear_veto = macro
    if bear_veto:
        return signals, "HOLD", 0.0

    buy_count = buy_score = 0
    for agent, val in signals.items():
        s, c = val[0], val[1]
        w = WEIGHTS.get(agent, 0.1)
        if s == "BUY":
            buy_count += 1; buy_score += w * c

    if buy_count >= MIN_AGENTS and buy_score > MIN_BUY_SCORE:
        return signals, "BUY", buy_score
    return signals, "HOLD", buy_score


def run_diagnostics(df, symbol, period_name):
    config   = get_symbol_config(symbol)
    balance  = START_BALANCE
    position = None
    all_trades = []

    for i in range(1, len(df)):
        row      = df.iloc[i]
        prev_row = df.iloc[i - 1]
        price    = row["close"]

        if position:
            held_h = (pd.Timestamp(row["time"]) - pd.Timestamp(position["entry_time"])).total_seconds() / 3600
            hit_stop   = price <= position["stop_price"]
            hit_target = price >= position["target_price"]
            hit_max    = config.get("max_hold_hours") and held_h >= config["max_hold_hours"]
            if hit_stop or hit_target or hit_max:
                reason = "STOP_LOSS" if hit_stop else ("TAKE_PROFIT" if hit_target else "MAX_HOLD")
                gross  = position["qty"] * price
                net    = gross - gross * EXIT_FEE
                pnl    = net - position["cost"]
                trade  = position.copy()
                trade.update({
                    "exit_price": price, "exit_time": row["time"],
                    "reason": reason, "pnl_usd": pnl,
                    "pnl_pct": (price - position["entry"]) / position["entry"] * 100,
                    "hold_hours": held_h,
                    "outcome": "WIN" if pnl > 0 else "LOSS",
                })
                all_trades.append(trade)
                balance += net
                position = None
                continue

        signals, action, buy_score = sim_agents(row, prev_row, config)

        if action == "BUY":
            levels = get_levels(df, i, lookback=150, n_swing=5)
            if levels["at_support"]:
                usd = balance * TRADE_SIZE_PCT
                fee = usd * ENTRY_FEE
                if usd + fee > balance:
                    continue
                balance -= usd + fee
                # Candle direction: is current candle already rising?
                candle_body = (row["close"] - row["open"]) / row["open"] * 100
                macro_sig   = signals["macro"][0]
                tech_aligned = signals["technical"][2] if len(signals["technical"]) > 2 else 0

                position = {
                    "qty": usd / price, "entry": price, "cost": usd,
                    "entry_time": row["time"],
                    "stop_price":   price - row["atr"] * ATR_STOP,
                    "target_price": price + row["atr"] * ATR_TARGET,
                    "stop_abs":     row["atr"] * ATR_STOP,
                    "target_abs":   row["atr"] * ATR_TARGET,
                    # Diagnostic context
                    "period":        period_name,
                    "symbol":        symbol,
                    "support":       levels["nearest_support"],
                    "dist_atr":      levels["dist_to_support"],
                    "buy_score":     round(buy_score, 3),
                    "macro_regime":  macro_sig,
                    "tech_aligned":  tech_aligned,
                    "candle_body_pct": round(candle_body, 3),
                    "rsi":           round(float(row["rsi"]), 1),
                    "atr":           round(float(row["atr"]), 2),
                    "volume_ratio":  round(float(row.get("volume_ratio", 1.0)), 2),
                }

    if position:
        price = df["close"].iloc[-1]
        gross = position["qty"] * price
        net   = gross - gross * EXIT_FEE
        pnl   = net - position["cost"]
        trade = position.copy()
        trade.update({
            "exit_price": price, "exit_time": df["time"].iloc[-1],
            "reason": "END_OF_TEST", "pnl_usd": pnl,
            "pnl_pct": (price - position["entry"]) / position["entry"] * 100,
            "hold_hours": (pd.Timestamp(df["time"].iloc[-1]) - pd.Timestamp(position["entry_time"])).total_seconds() / 3600,
            "outcome": "WIN" if pnl > 0 else "LOSS",
        })
        all_trades.append(trade)

    return all_trades


if __name__ == "__main__":
    print("\nCrypto Orchestra -- Entry Diagnostics")
    print("Investigating why 80% of support entries result in stop losses")
    print("=" * 75)

    all_trades = []
    for symbol in SYMBOLS:
        for period_name, start, end in PERIODS:
            sf = fetch_range(symbol, "1h", start, end)
            tf = fetch_range(symbol, "4h", start, end)
            if sf is None or tf is None:
                continue
            df = attach_higher_timeframe_context(sf, tf)
            start_ts = pd.Timestamp(start, tz="UTC")
            df["time"] = pd.to_datetime(df["time"], utc=True)
            df = df[df["time"] >= start_ts].reset_index(drop=True)
            trades = run_diagnostics(df, symbol, period_name)
            all_trades.extend(trades)

    closed = [t for t in all_trades if t.get("reason") != "OPEN"]
    wins   = [t for t in closed if t["outcome"] == "WIN"]
    losses = [t for t in closed if t["outcome"] == "LOSS"]

    print(f"\nTotal trades: {len(closed)}  Wins: {len(wins)}  Losses: {len(losses)}")
    if closed:
        print(f"Win rate: {len(wins)/len(closed)*100:.1f}%")

    if closed:
        print(f"\n{'-'*75}")
        print(f"{'Symbol':<10} {'Period':<16} {'Entry':>8} {'Exit':>8} {'P&L%':>7} {'Outcome':<8} {'Reason':<14} {'Hold':>5}h  Context")
        print(f"{'-'*75}")
        for t in closed:
            ctx = (f"RSI={t.get('rsi','?')}  macro={t.get('macro_regime','?')}  "
                   f"candle={t.get('candle_body_pct',0):+.2f}%  "
                   f"dist={t.get('dist_atr','?')}xATR  score={t.get('buy_score','?')}")
            print(f"{t['symbol']:<10} {t['period']:<16} "
                  f"${t['entry']:>8,.0f} ${t['exit_price']:>8,.0f} "
                  f"{t['pnl_pct']:>+6.1f}%  {t['outcome']:<8} {t['reason']:<14} "
                  f"{t['hold_hours']:>5.0f}h  {ctx}")

    print(f"\n{'-'*75}")
    print("Pattern analysis:")
    if wins:
        avg_candle_win = sum(t.get("candle_body_pct", 0) for t in wins) / len(wins)
        avg_rsi_win    = sum(t.get("rsi", 50) for t in wins) / len(wins)
        avg_dist_win   = sum(t.get("dist_atr", 0) or 0 for t in wins) / len(wins)
        print(f"  Winners  — avg candle body: {avg_candle_win:+.2f}%  avg RSI: {avg_rsi_win:.1f}  avg dist-to-support: {avg_dist_win:.2f}x ATR")
    if losses:
        avg_candle_loss = sum(t.get("candle_body_pct", 0) for t in losses) / len(losses)
        avg_rsi_loss    = sum(t.get("rsi", 50) for t in losses) / len(losses)
        avg_dist_loss   = sum(t.get("dist_atr", 0) or 0 for t in losses) / len(losses)
        print(f"  Losers   — avg candle body: {avg_candle_loss:+.2f}%  avg RSI: {avg_rsi_loss:.1f}  avg dist-to-support: {avg_dist_loss:.2f}x ATR")

    if wins and losses:
        print(f"\n  Key differences:")
        candle_diff = sum(t.get("candle_body_pct", 0) for t in wins) / len(wins) - \
                      sum(t.get("candle_body_pct", 0) for t in losses) / len(losses)
        rsi_diff    = sum(t.get("rsi", 50) for t in wins) / len(wins) - \
                      sum(t.get("rsi", 50) for t in losses) / len(losses)
        print(f"  Candle body: winners are {candle_diff:+.2f}% stronger at entry")
        print(f"  RSI at entry: winners have RSI {rsi_diff:+.1f} pts lower (more oversold = better)")
        macro_wins = [t.get("macro_regime") for t in wins]
        macro_losses = [t.get("macro_regime") for t in losses]
        print(f"  Macro regime in wins:   {', '.join(set(macro_wins))}")
        print(f"  Macro regime in losses: {', '.join(set(macro_losses))}")
