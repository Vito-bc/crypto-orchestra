"""
Stop/Target ATR Ratio Grid Search — find the ratio that maximises return + win rate.

Tests combinations of (atr_stop, atr_target) across all 4 market periods
for both BTC-USD and ETH-USD, using realistic limit-order fees (0.2% entry,
0.4% exit = 0.6% round-trip).

Current baseline: stop=2.0  target=4.0

Grid tested:
  ATR stops:   1.5, 2.0, 2.5, 3.0
  ATR targets: 2.0, 3.0, 4.0, 5.0, 6.0
  (only R:R >= 1.5 kept — smaller is not worth trading)

Output: ranked table sorted by avg return across all periods and symbols.

Usage:
    python backtesting/stop_target_tune.py
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
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

# ── Fees: limit-order scenario ────────────────────────────────────────────────
ENTRY_FEE = 0.002   # 0.2% maker
EXIT_FEE  = 0.004   # 0.4% taker

WARMUP_DAYS = 60
SYMBOLS     = ["BTC-USD", "ETH-USD"]

# ── Grid ──────────────────────────────────────────────────────────────────────
ATR_STOPS   = [1.5, 2.0, 2.5, 3.0]
ATR_TARGETS = [2.0, 3.0, 4.0, 5.0, 6.0]

COMBOS = [
    (s, t) for s in ATR_STOPS for t in ATR_TARGETS
    if t / s >= 1.5   # minimum 1.5:1 reward-to-risk
]

# ── Strategy constants ────────────────────────────────────────────────────────
WEIGHTS = {
    "macro":     0.30,
    "technical": 0.25,
    "whale":     0.20,
    "sentiment": 0.15,
    "risk":      0.10,
}
VETO_TS_THRESHOLD = -0.002
MIN_AGENTS        = 3
MIN_BUY_SCORE     = 0.28


@dataclass
class Period:
    name:  str
    label: str
    start: str
    end:   str


PERIODS = [
    Period("Oct-Dec 2024", "BULL RUN",   "2024-10-01", "2024-12-31"),
    Period("Jul-Sep 2024", "RANGING",    "2024-07-01", "2024-09-30"),
    Period("Jan-Mar 2025", "CORRECTION", "2025-01-01", "2025-03-31"),
    Period("Jun-Aug 2025", "SUMMER 25",  "2025-06-01", "2025-08-31"),
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
        aligned = sum([row["rsi"] < config["buy_rsi_max"],
                       macd_buy_ok(row, config),
                       row["bb_pct"] < config["buy_bb_pct_max"]])
        return "BUY", 0.40 + aligned * 0.15
    if signal == "SELL":
        aligned = sum([row["rsi"] > config["sell_rsi_min"],
                       macd_sell_ok(row, config),
                       row["bb_pct"] > config["sell_bb_pct_min"]])
        return "SELL", 0.40 + aligned * 0.15
    return "NEUTRAL", 0.45


def sim_macro(row) -> tuple[str, float, bool]:
    close_4h  = row.get("close_4h", np.nan)
    ema50_4h  = row.get("ema50_4h", np.nan)
    ema200_4h = row.get("ema200_4h", np.nan)
    ts        = row.get("trend_strength_4h", 0) or 0
    if any(pd.isna(v) for v in [close_4h, ema50_4h, ema200_4h]):
        return "NEUTRAL", 0.40, False
    bear_cross = close_4h < ema200_4h and ema50_4h < ema200_4h
    if bear_cross and ts < VETO_TS_THRESHOLD:
        return "SELL", 0.80, True
    if close_4h > ema200_4h and ema50_4h > ema200_4h:
        return "BUY", min(0.85, 0.60 + abs(ts) * 5), False
    return "NEUTRAL", 0.50, False


def sim_sentiment(row) -> tuple[str, float]:
    mom = row.get("momentum_20", 0) or 0
    if mom > 8:  return "BUY",  0.55
    if mom > 3:  return "BUY",  0.45
    if mom < -8: return "SELL", 0.55
    if mom < -3: return "SELL", 0.45
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


# ── Close with explicit fees ──────────────────────────────────────────────────

def _close(position: dict, price: float, time, reason: str) -> dict:
    gross       = position["qty"] * price
    net         = gross - gross * EXIT_FEE
    pnl_usd     = net - position["cost"]
    pnl_pct     = (price - position["entry"]) / position["entry"]
    hold_hours  = (pd.Timestamp(time) - pd.Timestamp(position["entry_time"])).total_seconds() / 3600
    return {
        "type": "CLOSE", "entry": position["entry"], "exit": price,
        "pnl_usd": pnl_usd, "pnl_pct": pnl_pct,
        "hold_hours": hold_hours, "reason": reason, "net_returned": net,
    }


# ── Single run for one period + one (stop, target) combo ─────────────────────

def run_period(df: pd.DataFrame, symbol: str,
               atr_stop: float, atr_target: float) -> dict:
    config   = get_symbol_config(symbol)
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
            # Trailing stop / break-even (use symbol config if present)
            tm = config.get("trailing_stop_multiplier")
            ta = config.get("trail_activation_multiplier")
            be = config.get("break_even_trigger_atr")
            if be and price >= position["entry"] + row["atr"] * be:
                position["stop_price"] = max(position["stop_price"], position["entry"])
            if tm and (ta is None or price >= position["entry"] + row["atr"] * ta):
                position["stop_price"] = max(position["stop_price"], price - row["atr"] * tm)
            hit_stop   = price <= position["stop_price"]
            hit_target = price >= position["target_price"]
            hit_max    = config.get("max_hold_hours") and held_h >= config["max_hold_hours"]
            if hit_stop or hit_target or hit_max:
                reason = "STOP_LOSS" if hit_stop else ("TAKE_PROFIT" if hit_target else "MAX_HOLD")
                ct = _close(position, price, row["time"], reason)
                balance += ct["net_returned"]; trades.append(ct); position = None
                continue

        tech_sig,  tech_conf          = sim_technical(row, prev_row, config)
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
        action, _, veto = sim_orchestrator(signals)

        # S/R gate
        if action == "BUY" and not veto:
            levels = get_levels(df, i, lookback=150, n_swing=5)
            if not levels["at_support"]:
                action = "HOLD"

        if action == "SELL" and position:
            ct = _close(position, price, row["time"], "SIGNAL")
            balance += ct["net_returned"]; trades.append(ct); position = None
            continue
        if position:
            continue
        if action == "BUY":
            usd = balance * TRADE_SIZE_PCT
            fee = usd * ENTRY_FEE
            if usd + fee > balance:
                continue
            balance -= usd + fee
            position = {
                "qty": usd / price, "entry": price, "cost": usd,
                "entry_time": row["time"],
                "stop_price":   price - row["atr"] * atr_stop,
                "target_price": price + row["atr"] * atr_target,
            }
            trades.append({"type": "OPEN"})

    if position:
        ct = _close(position, df["close"].iloc[-1], df["time"].iloc[-1], "END_OF_TEST")
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

    return {
        "return":        total_return,
        "trades":        len(closed),
        "win_rate":      win_rate,
        "profit_factor": profit_factor,
        "max_drawdown":  max_dd * 100,
        "avg_win_usd":   avg_win,
        "avg_loss_usd":  avg_loss,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\nCrypto Orchestra -- Stop/Target ATR Grid Search")
    print(f"Fee model: {ENTRY_FEE*100:.1f}% entry (maker) + {EXIT_FEE*100:.1f}% exit (taker)")
    print(f"Testing {len(COMBOS)} combinations × {len(PERIODS)} periods × {len(SYMBOLS)} symbols")
    print("Fetching data...")

    # Pre-fetch all period data once
    cache: dict = {}
    for symbol in SYMBOLS:
        for period in PERIODS:
            key = (symbol, period.start, period.end)
            sf = fetch_range(symbol, "1h", period.start, period.end)
            tf = fetch_range(symbol, "4h", period.start, period.end)
            if sf is not None and tf is not None:
                df = attach_higher_timeframe_context(sf, tf)
                start_ts = pd.Timestamp(period.start, tz="UTC")
                df["time"] = pd.to_datetime(df["time"], utc=True)
                df = df[df["time"] >= start_ts].reset_index(drop=True)
                cache[key] = df
            else:
                cache[key] = None

    print(f"Data ready. Running {len(COMBOS)} combos...\n")

    # Collect results per (stop, target)
    summary: list[dict] = []
    baseline_key = (2.0, 4.0)

    for (atr_stop, atr_target) in COMBOS:
        rr = atr_target / atr_stop
        all_returns     = []
        all_win_rates   = []
        all_pf          = []
        all_dd          = []
        total_trades    = 0

        for symbol in SYMBOLS:
            for period in PERIODS:
                key = (symbol, period.start, period.end)
                df  = cache.get(key)
                if df is None:
                    continue
                r = run_period(df, symbol, atr_stop, atr_target)
                all_returns.append(r["return"])
                all_win_rates.append(r["win_rate"])
                all_pf.append(r["profit_factor"])
                all_dd.append(r["max_drawdown"])
                total_trades += r["trades"]

        if not all_returns:
            continue

        avg_ret = sum(all_returns)   / len(all_returns)
        avg_wr  = sum(all_win_rates) / len(all_win_rates)
        avg_pf  = sum(all_pf)        / len(all_pf)
        avg_dd  = sum(all_dd)        / len(all_dd)

        summary.append({
            "stop":    atr_stop,
            "target":  atr_target,
            "rr":      round(rr, 2),
            "avg_ret": round(avg_ret, 3),
            "avg_wr":  round(avg_wr, 1),
            "avg_pf":  round(avg_pf, 2),
            "avg_dd":  round(avg_dd, 2),
            "trades":  total_trades,
            "current": atr_stop == baseline_key[0] and atr_target == baseline_key[1],
        })

    # Sort by avg return descending
    summary.sort(key=lambda x: x["avg_ret"], reverse=True)

    print(f"{'Rank':<5} {'Stop':>5} {'Target':>7} {'R:R':>5} {'AvgRet':>8} {'WinRate':>8} {'PF':>6} {'MaxDD':>6} {'Trades':>7}  Note")
    print("-" * 75)
    for rank, row in enumerate(summary, 1):
        note = "<-- CURRENT" if row["current"] else ""
        note = "<-- BEST"    if rank == 1 and not row["current"] else note
        print(
            f"{rank:<5} {row['stop']:>5.1f} {row['target']:>7.1f} "
            f"{row['rr']:>5.1f} {row['avg_ret']:>+7.3f}% "
            f"{row['avg_wr']:>7.1f}% "
            f"{row['avg_pf']:>6.2f} "
            f"{row['avg_dd']:>5.1f}% "
            f"{row['trades']:>7}  {note}"
        )

    best = summary[0]
    current = next((r for r in summary if r["current"]), None)
    print(f"\n{'='*75}")
    print(f"Best combo:    stop={best['stop']}x ATR  target={best['target']}x ATR  (R:R={best['rr']})")
    print(f"Avg return:    {best['avg_ret']:+.3f}%   win rate: {best['avg_wr']:.1f}%   PF: {best['avg_pf']:.2f}")
    if current and best != current:
        delta = best["avg_ret"] - current["avg_ret"]
        print(f"Current combo: stop={current['stop']}x ATR  target={current['target']}x ATR  avg={current['avg_ret']:+.3f}%")
        print(f"Improvement:   {delta:+.3f}% per period on average")
    print(f"{'='*75}")
    print(f"\nTo apply: update BEST_ATR_STOP={best['stop']} and BEST_ATR_TARGET={best['target']}")
    print(f"in backtesting/period_validation.py and pipeline/runner.py (risk agent prompt)")
