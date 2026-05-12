"""
Fee Comparison Backtest — quantifies the impact of limit-order maker fees.

Runs the same 4-period validation under three fee scenarios:
  A. Backtest legacy  : 0.6% entry + 0.6% exit = 1.2% round-trip
  B. Market orders    : 0.4% entry + 0.4% exit = 0.8% round-trip (taker)
  C. Limit orders     : 0.2% entry + 0.4% exit = 0.6% round-trip (maker in, taker out)

All other parameters are identical to period_validation.py so the fee impact
is the only variable being changed.

Usage:
    python backtesting/fee_comparison.py
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

WARMUP_DAYS = 60
SYMBOLS     = ["BTC-USD", "ETH-USD"]

# ── Fee scenarios ─────────────────────────────────────────────────────────────
@dataclass
class FeeScenario:
    name:       str
    entry_fee:  float   # fraction per side on entry
    exit_fee:   float   # fraction per side on exit
    label:      str

SCENARIOS = [
    FeeScenario("legacy",  0.006, 0.006, "Legacy 1.2%RT"),
    FeeScenario("market",  0.004, 0.004, "Market 0.8%RT"),
    FeeScenario("limit",   0.002, 0.004, "Limit  0.6%RT"),
]

# ── Strategy constants (same as period_validation.py) ────────────────────────
WEIGHTS = {
    "macro":     0.30,
    "technical": 0.25,
    "whale":     0.20,
    "sentiment": 0.15,
    "risk":      0.10,
}
VETO_TS_THRESHOLD  = -0.002
MIN_AGENTS         = 3
MIN_BUY_SCORE      = 0.28
BEST_ATR_STOP      = 2.0
BEST_ATR_TARGET    = 4.0

@dataclass
class Period:
    name:  str
    label: str
    start: str
    end:   str

PERIODS = [
    Period("Oct-Dec 2024", "BULL RUN  (training)",  "2024-10-01", "2024-12-31"),
    Period("Jul-Sep 2024", "RANGING   (sideways)",  "2024-07-01", "2024-09-30"),
    Period("Jan-Mar 2025", "POST-ATH CORRECTION",   "2025-01-01", "2025-03-31"),
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


# ── Agent simulators ──────────────────────────────────────────────────────────

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
    close_4h  = row.get("close_4h", np.nan)
    ema50_4h  = row.get("ema50_4h", np.nan)
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
    if mom > 8:   return "BUY",  0.55
    if mom > 3:   return "BUY",  0.45
    if mom < -8:  return "SELL", 0.55
    if mom < -3:  return "SELL", 0.45
    return "NEUTRAL", 0.40


def sim_whale(row) -> tuple[str, float]:
    vr  = row.get("volume_ratio", 1.0) or 1.0
    mom = row.get("momentum_20", 0) or 0
    if vr > 1.8 and mom < -3: return "SELL", 0.70
    if vr > 1.8 and mom >  3: return "BUY",  0.68
    if vr < 0.5:               return "NEUTRAL", 0.30
    return "NEUTRAL", 0.50


def sim_orchestrator(signals: dict) -> tuple[str, float, bool]:
    _, _, bear_veto = signals["macro"]
    if bear_veto:
        return "HOLD", 0.0, True
    buy_count = sell_count = 0
    buy_score = sell_score = 0.0
    for agent, val in signals.items():
        sig, conf = val[0], val[1]
        w = WEIGHTS.get(agent, 0.1)
        if sig == "BUY":    buy_count += 1; buy_score  += w * conf
        elif sig == "SELL": sell_count += 1; sell_score += w * conf
    if buy_count >= MIN_AGENTS and buy_score > MIN_BUY_SCORE and buy_score > sell_score:
        return "BUY",  round(buy_score, 3), False
    if sell_count >= MIN_AGENTS and sell_score > MIN_BUY_SCORE and sell_score > buy_score:
        return "SELL", round(sell_score, 3), False
    return "HOLD", round(max(buy_score, sell_score), 3), False


# ── Local close with configurable fees ───────────────────────────────────────

def _close(position: dict, price: float, time, reason: str,
           exit_fee: float) -> dict:
    """Like backtest.close_position() but with an explicit exit_fee rate."""
    gross_value  = position["qty"] * price
    fee_paid     = gross_value * exit_fee
    net_returned = gross_value - fee_paid
    pnl_usd      = net_returned - position["cost"]
    pnl_pct      = (price - position["entry"]) / position["entry"]
    hold_hours   = (pd.Timestamp(time) - pd.Timestamp(position["entry_time"])).total_seconds() / 3600
    return {
        "type": "CLOSE", "side": "LONG",
        "entry": position["entry"], "exit": price,
        "pnl_usd": pnl_usd, "pnl_pct": pnl_pct,
        "exit_time": time, "entry_time": position["entry_time"],
        "hold_hours": hold_hours, "reason": reason,
        "net_returned": net_returned,
    }


# ── Single period under one fee scenario ─────────────────────────────────────

def run_period(df: pd.DataFrame, symbol: str, period: Period,
               scenario: FeeScenario) -> dict:
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
                ct = _close(position, price, row["time"], reason, scenario.exit_fee)
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

        if action == "BUY" and not veto:
            levels = get_levels(df, i, lookback=150, n_swing=5)
            if not levels["at_support"]:
                action = "HOLD"

        if action == "SELL" and position:
            ct = _close(position, price, row["time"], "SIGNAL", scenario.exit_fee)
            balance += ct["net_returned"]; trades.append(ct); position = None
            continue
        if position:
            continue
        if action == "BUY":
            usd = balance * TRADE_SIZE_PCT
            fee = usd * scenario.entry_fee
            if usd + fee > balance:
                continue
            balance -= usd + fee
            position = {
                "side": "LONG", "qty": usd / price, "entry": price,
                "cost": usd, "entry_time": row["time"],
                "stop_price":   price - row["atr"] * BEST_ATR_STOP,
                "target_price": price + row["atr"] * BEST_ATR_TARGET,
            }
            trades.append({"type": "OPEN", "side": "LONG", "symbol": symbol,
                           "price": price, "time": row["time"]})

    if position:
        ct = _close(position, df["close"].iloc[-1], df["time"].iloc[-1],
                    "END_OF_TEST", scenario.exit_fee)
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

    bah_return = (df["close"].iloc[-1] - df["close"].iloc[0]) / df["close"].iloc[0] * 100
    return {
        "period":        period.name,
        "regime":        period.label,
        "return":        total_return,
        "bah_return":    bah_return,
        "trades":        len(closed),
        "win_rate":      win_rate,
        "profit_factor": profit_factor,
        "max_drawdown":  max_dd * 100,
        "avg_win_usd":   avg_win,
        "avg_loss_usd":  avg_loss,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\nCrypto Orchestra -- Fee Comparison Backtest")
    print("Comparing: Legacy 1.2%RT  |  Market orders 0.8%RT  |  Limit orders 0.6%RT")
    print("All other parameters identical (3-agent filter, 2x/4x ATR stops, S/R gate)")

    # Pre-fetch all data (avoid re-downloading per scenario)
    cache: dict[tuple, tuple] = {}
    for symbol in SYMBOLS:
        for period in PERIODS:
            key = (symbol, period.start, period.end)
            sf = fetch_range(symbol, "1h", period.start, period.end)
            tf = fetch_range(symbol, "4h", period.start, period.end)
            cache[key] = (sf, tf)

    for symbol in SYMBOLS:
        print(f"\n{'='*85}")
        print(f"  {symbol}")
        print(f"{'='*85}")
        print(f"  {'Period':<16} {'Regime':<26} {'Legacy':>8} {'Market':>8} {'Limit':>8}  {'Trades':>6}  {'WR':>5}  Verdict")
        print(f"  {'-'*83}")

        all_by_scenario: dict[str, list] = {s.name: [] for s in SCENARIOS}

        for period in PERIODS:
            key = (symbol, period.start, period.end)
            signal_df, trend_df = cache[key]

            if signal_df is None or trend_df is None:
                print(f"  {period.name:<16}  No data.")
                continue

            from backtesting.backtest import attach_higher_timeframe_context
            df = attach_higher_timeframe_context(signal_df, trend_df)
            start_ts = pd.Timestamp(period.start, tz="UTC")
            df["time"] = pd.to_datetime(df["time"], utc=True)
            df = df[df["time"] >= start_ts].reset_index(drop=True)

            results = {}
            for sc in SCENARIOS:
                r = run_period(df, symbol, period, sc)
                results[sc.name] = r
                all_by_scenario[sc.name].append(r)

            r_leg = results["legacy"]
            r_mkt = results["market"]
            r_lim = results["limit"]
            trades = r_lim["trades"]
            wr     = r_lim["win_rate"]

            # Verdict based on limit scenario
            verdict = "PASS" if r_lim["return"] > 0 else ("CLOSE" if r_lim["return"] > -0.5 else "FAIL")
            # Delta labels: show improvement over legacy
            delta_mkt = r_mkt["return"] - r_leg["return"]
            delta_lim = r_lim["return"] - r_leg["return"]

            print(
                f"  {period.name:<16} {period.label:<26} "
                f"{r_leg['return']:>+7.2f}% "
                f"{r_mkt['return']:>+7.2f}% "
                f"{r_lim['return']:>+7.2f}%  "
                f"{trades:>6}  "
                f"{wr:>4.0f}%  {verdict}"
            )

        # Averages
        print(f"  {'-'*83}")
        for sc in SCENARIOS:
            res = all_by_scenario[sc.name]
            if res:
                avg = sum(r["return"] for r in res) / len(res)
                avg_wr = sum(r["win_rate"] for r in res) / len(res)
                avg_pf = sum(r["profit_factor"] for r in res) / len(res)
                avg_dd = sum(r["max_drawdown"] for r in res) / len(res)
                label = f"AVG ({sc.label})"
                print(f"  {label:<42} {avg:>+7.2f}%  WR {avg_wr:.0f}%  PF {avg_pf:.2f}  DD {avg_dd:.1f}%")

    print(f"\n{'='*85}")
    print("  Legend:")
    print("  Legacy  = 0.6% entry + 0.6% exit (1.2% round-trip) — original backtest assumption")
    print("  Market  = 0.4% entry + 0.4% exit (0.8% round-trip) — Coinbase taker fee")
    print("  Limit   = 0.2% entry + 0.4% exit (0.6% round-trip) — maker entry via limit order")
    print("  PASS = profitable | CLOSE = <0.5% loss | FAIL = >0.5% loss")
    print("  Verdict based on Limit scenario (the one we actually trade)")
