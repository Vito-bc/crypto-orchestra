"""
Agent Backtest Tuning — Oct-Dec 2024 Bull Run

Tests configurations side by side to find which fixes improve performance.

Config 0 — Baseline          (original live settings)
Config 1 — Wider stops       (ATR x2.5 stop, x5.0 target)
Config 2 — Relaxed veto      (macro veto requires stronger bear confirmation)
Config 3 — Trend sentiment   (sentiment follows trend instead of contrarian)
Config 4 — All fixes         (1 + 2 + 3 combined)
Config 5 — 3-agent filter    (All fixes + need 3 agents aligned to act)
Config 6 — High conviction   (All fixes + 3-agent + higher score threshold)
Config 7 — Veto off          (All fixes + macro veto completely disabled)

Usage:
    python backtesting/agent_backtest_tune.py
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

BACKTEST_START = "2024-10-01"
BACKTEST_END   = "2024-12-31"
WARMUP_DAYS    = 60
SYMBOLS        = ["BTC-USD", "ETH-USD"]

WEIGHTS = {
    "macro":     0.30,
    "technical": 0.25,
    "whale":     0.20,
    "sentiment": 0.15,
    "risk":      0.10,
}


# ── Config dataclass ──────────────────────────────────────────────────────────

@dataclass
class BacktestConfig:
    name:              str
    atr_stop:          float = 1.5   # stop loss multiplier
    atr_target:        float = 3.0   # take profit multiplier
    strict_veto:       bool  = True  # True = veto on any bear EMA cross
    veto_ts_threshold: float = -0.005  # trend_strength required for relaxed veto
    trend_sentiment:   bool  = False # True = sentiment follows trend, not contrarian
    min_agents:        int   = 2     # minimum agents aligned to act
    min_buy_score:     float = 0.28  # minimum weighted score to act


CONFIGS = [
    BacktestConfig("Baseline",          atr_stop=1.5, atr_target=3.0, strict_veto=True,  trend_sentiment=False, min_agents=2, min_buy_score=0.28),
    BacktestConfig("Wider stops",       atr_stop=2.5, atr_target=5.0, strict_veto=True,  trend_sentiment=False, min_agents=2, min_buy_score=0.28),
    BacktestConfig("Relaxed veto",      atr_stop=1.5, atr_target=3.0, strict_veto=False, veto_ts_threshold=-0.002, trend_sentiment=False, min_agents=2, min_buy_score=0.28),
    BacktestConfig("Trend sentiment",   atr_stop=1.5, atr_target=3.0, strict_veto=True,  trend_sentiment=True,  min_agents=2, min_buy_score=0.28),
    BacktestConfig("All fixes",         atr_stop=2.5, atr_target=5.0, strict_veto=False, veto_ts_threshold=-0.002, trend_sentiment=True,  min_agents=2, min_buy_score=0.28),
    BacktestConfig("3-agent filter",    atr_stop=2.5, atr_target=5.0, strict_veto=False, veto_ts_threshold=-0.002, trend_sentiment=True,  min_agents=3, min_buy_score=0.28),
    BacktestConfig("High conviction",   atr_stop=2.5, atr_target=5.0, strict_veto=False, veto_ts_threshold=-0.002, trend_sentiment=True,  min_agents=3, min_buy_score=0.35),
    BacktestConfig("Veto off",          atr_stop=2.5, atr_target=5.0, strict_veto=False, veto_ts_threshold= 99.0, trend_sentiment=True,  min_agents=2, min_buy_score=0.28),
]


# ── Data fetch (done once, shared across configs) ─────────────────────────────

def fetch_range(symbol: str, timeframe: str) -> pd.DataFrame | None:
    warmup = (datetime.strptime(BACKTEST_START, "%Y-%m-%d") - timedelta(days=WARMUP_DAYS)).strftime("%Y-%m-%d")
    ticker = yf.download(symbol, start=warmup, end=BACKTEST_END,
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
        aligned = sum([
            row["rsi"] < config["buy_rsi_max"],
            macd_buy_ok(row, config),
            row["bb_pct"] < config["buy_bb_pct_max"],
        ])
        return "BUY", 0.40 + aligned * 0.15
    if signal == "SELL":
        aligned = sum([
            row["rsi"] > config["sell_rsi_min"],
            macd_sell_ok(row, config),
            row["bb_pct"] > config["sell_bb_pct_min"],
        ])
        return "SELL", 0.40 + aligned * 0.15
    return "NEUTRAL", 0.45


def sim_macro(row, cfg: BacktestConfig) -> tuple[str, float, bool]:
    close_4h  = row.get("close_4h",  np.nan)
    ema50_4h  = row.get("ema50_4h",  np.nan)
    ema200_4h = row.get("ema200_4h", np.nan)
    ts        = row.get("trend_strength_4h", 0) or 0

    if any(pd.isna(v) for v in [close_4h, ema50_4h, ema200_4h]):
        return "NEUTRAL", 0.40, False

    bear_ema_cross = close_4h < ema200_4h and ema50_4h < ema200_4h

    if cfg.strict_veto:
        if bear_ema_cross:
            return "SELL", 0.80, True
    else:
        # Relaxed: require EMA cross + trend_strength below configurable threshold
        # veto_ts_threshold=99.0 effectively disables the veto entirely
        if bear_ema_cross and ts < cfg.veto_ts_threshold:
            return "SELL", 0.80, True

    if close_4h > ema200_4h and ema50_4h > ema200_4h:
        conf = min(0.85, 0.60 + abs(ts) * 5)
        return "BUY", conf, False

    return "NEUTRAL", 0.50, False


def sim_sentiment(row, trend_mode: bool) -> tuple[str, float]:
    mom = row.get("momentum_20", 0) or 0
    if trend_mode:
        # Trend-following: sentiment agrees with momentum direction
        if mom > 8:    return "BUY",  0.55   # strong up = buy
        if mom > 3:    return "BUY",  0.45
        if mom < -8:   return "SELL", 0.55   # strong down = sell
        if mom < -3:   return "SELL", 0.45
        return "NEUTRAL", 0.40
    else:
        # Contrarian (current live behavior)
        if mom < -8:   return "BUY",  0.62
        if mom < -4:   return "BUY",  0.52
        if mom > 15:   return "SELL", 0.60
        if mom > 8:    return "SELL", 0.50
        return "NEUTRAL", 0.40


def sim_whale(row) -> tuple[str, float]:
    vr  = row.get("volume_ratio", 1.0) or 1.0
    mom = row.get("momentum_20", 0) or 0
    if vr > 1.8 and mom < -3:  return "SELL", 0.70
    if vr > 1.8 and mom >  3:  return "BUY",  0.68
    if vr < 0.5:                return "NEUTRAL", 0.30
    return "NEUTRAL", 0.50


def sim_orchestrator(signals: dict, cfg: BacktestConfig) -> tuple[str, float, bool]:
    macro_sig, macro_conf, bear_veto = signals["macro"]
    if bear_veto:
        return "HOLD", 0.0, True

    buy_count = sell_count = 0
    buy_score = sell_score = 0.0

    for agent, val in signals.items():
        sig, conf = val[0], val[1]
        w = WEIGHTS.get(agent, 0.1)
        if sig == "BUY":
            buy_count += 1; buy_score  += w * conf
        elif sig == "SELL":
            sell_count += 1; sell_score += w * conf

    if buy_count >= cfg.min_agents and buy_score > cfg.min_buy_score and buy_score > sell_score:
        return "BUY",  round(buy_score, 3), False
    if sell_count >= cfg.min_agents and sell_score > cfg.min_buy_score and sell_score > buy_score:
        return "SELL", round(sell_score, 3), False
    return "HOLD", round(max(buy_score, sell_score), 3), False


# ── Single run ────────────────────────────────────────────────────────────────

def run_config(df: pd.DataFrame, symbol: str, cfg: BacktestConfig) -> dict:
    sym_config = get_symbol_config(symbol)
    balance    = START_BALANCE
    position   = None
    trades     = []
    equity     = []
    buys = sells = holds = vetoes = 0

    for i in range(1, len(df)):
        row      = df.iloc[i]
        prev_row = df.iloc[i - 1]
        price    = row["close"]

        eq = balance + (position["qty"] * price if position else 0)
        equity.append(eq)

        if position:
            held_h = (pd.Timestamp(row["time"]) - pd.Timestamp(position["entry_time"])).total_seconds() / 3600

            # trailing stop
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

        # Simulate agents
        tech_sig, tech_conf           = sim_technical(row, prev_row, sym_config)
        macro_sig, macro_conf, b_veto = sim_macro(row, cfg)
        sent_sig, sent_conf           = sim_sentiment(row, cfg.trend_sentiment)
        whale_sig, whale_conf         = sim_whale(row)

        signals = {
            "macro":     (macro_sig, macro_conf, b_veto),
            "technical": (tech_sig,  tech_conf),
            "sentiment": (sent_sig,  sent_conf),
            "whale":     (whale_sig, whale_conf),
            "risk":      ("NEUTRAL", 0.85),
        }

        action, conf, veto = sim_orchestrator(signals, cfg)

        if veto:   vetoes += 1
        if action == "BUY":   buys  += 1
        if action == "SELL":  sells += 1
        if action == "HOLD":  holds += 1

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
                "side":         "LONG",
                "qty":          usd / price,
                "entry":        price,
                "cost":         usd,
                "entry_time":   row["time"],
                "stop_price":   price - row["atr"] * cfg.atr_stop,
                "target_price": price + row["atr"] * cfg.atr_target,
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
    avg_hold      = sum(t["hold_hours"] for t in closed) / len(closed) if closed else 0

    peak = START_BALANCE; max_dd = 0
    for val in equity:
        if val > peak: peak = val
        dd = (peak - val) / peak
        if dd > max_dd: max_dd = dd

    sl_count = sum(1 for t in closed if t["reason"] == "STOP_LOSS")
    tp_count = sum(1 for t in closed if t["reason"] == "TAKE_PROFIT")

    passed = (win_rate > 55 and profit_factor > 1.3 and
              max_dd < 0.08 and total_return > 0 and len(closed) >= 5)

    return {
        "config":        cfg.name,
        "return":        total_return,
        "trades":        len(closed),
        "win_rate":      win_rate,
        "profit_factor": profit_factor,
        "max_drawdown":  max_dd * 100,
        "avg_hold":      avg_hold,
        "avg_win":       avg_win,
        "avg_loss":      avg_loss,
        "buy_signals":   buys,
        "vetoes":        vetoes,
        "stop_losses":   sl_count,
        "take_profits":  tp_count,
        "passed":        passed,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\nCrypto Orchestra — Agent Backtest Tuning")
    print(f"Period: {BACKTEST_START} to {BACKTEST_END}  (Oct-Dec 2024 Bull Run)")
    print("=" * 70)

    for symbol in SYMBOLS:
        print(f"\nFetching {symbol} data ...")
        signal_df = fetch_range(symbol, "1h")
        trend_df  = fetch_range(symbol, "4h")
        if signal_df is None or trend_df is None:
            print(f"  No data."); continue

        df = attach_higher_timeframe_context(signal_df, trend_df)
        start_ts = pd.Timestamp(BACKTEST_START, tz="UTC")
        df["time"] = pd.to_datetime(df["time"], utc=True)
        df = df[df["time"] >= start_ts].reset_index(drop=True)
        print(f"  {len(df)} candles  ({df['time'].iloc[0].date()} to {df['time'].iloc[-1].date()})")

        results = []
        for cfg in CONFIGS:
            r = run_config(df, symbol, cfg)
            results.append(r)

        # ── Print comparison table ─────────────────────────────────────────
        print(f"\n{'='*70}")
        print(f"  {symbol} — Config Comparison")
        print(f"{'='*70}")
        print(f"  {'Config':<18} {'Return':>7} {'WR':>6} {'PF':>6} {'MaxDD':>6} {'Trades':>7} {'SL':>5} {'TP':>5} {'Verdict'}")
        print(f"  {'-'*68}")
        for r in results:
            verdict = "PASS" if r["passed"] else "FAIL"
            marker  = " <--" if r["passed"] else ""
            print(
                f"  {r['config']:<18} {r['return']:>+6.2f}% "
                f"{r['win_rate']:>5.1f}% "
                f"{r['profit_factor']:>6.2f} "
                f"{r['max_drawdown']:>5.1f}% "
                f"{r['trades']:>7} "
                f"{r['stop_losses']:>5} "
                f"{r['take_profits']:>5}  "
                f"{verdict}{marker}"
            )
        print(f"{'='*70}")

        # ── Highlight winner ───────────────────────────────────────────────
        best = max(results, key=lambda r: r["return"])
        print(f"\n  Best config for {symbol}: [{best['config']}]")
        print(f"    Return: {best['return']:+.2f}%  |  Win Rate: {best['win_rate']:.1f}%  |  "
              f"Profit Factor: {best['profit_factor']:.2f}  |  Trades: {best['trades']}")
        print(f"    Stop losses: {best['stop_losses']}  |  Take profits: {best['take_profits']}  |  "
              f"Vetoes: {best['vetoes']}  |  Avg hold: {best['avg_hold']:.1f}h")
