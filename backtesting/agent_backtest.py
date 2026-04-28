"""
Agent Pipeline Historical Backtest

Simulates all 5 agents deterministically on historical candle data — no
API calls, no cost, runs in seconds. Covers any date range yfinance has.

Agent simulation rules (matching the live agent logic):

  Technical  (w=0.25) — existing get_signal() + indicator confidence
  Macro      (w=0.30) — 4h EMA trend + regime veto on BEAR
  Sentiment  (w=0.15) — simulated from 20-candle price momentum
  Whale      (w=0.20) — volume ratio + price direction proxy
  Risk       (w=0.10) — always NEUTRAL, gates ok_to_trade

Orchestrator: weighted vote, confidence >= 0.55 to act, macro BEAR = veto.

Usage:
    python backtesting/agent_backtest.py
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
    ATR_STOP_MULTIPLIER,
    ATR_TARGET_MULTIPLIER,
    FEE_RATE,
    START_BALANCE,
    TRADE_SIZE_PCT,
    attach_higher_timeframe_context,
    calculate_indicators,
    close_position,
    export_trade_log,
    get_signal,
    get_symbol_config,
    macd_buy_ok,
    macd_sell_ok,
    trend_ok,
)

# ── Date range ────────────────────────────────────────────────────────────────
BACKTEST_START = "2024-10-01"
BACKTEST_END   = "2024-12-31"
WARMUP_DAYS    = 60          # extra days before start for indicator warmup
SYMBOLS        = ["BTC-USD", "ETH-USD"]

# ── Agent weights (must sum to 1.0) ───────────────────────────────────────────
WEIGHTS = {
    "macro":     0.30,
    "technical": 0.25,
    "whale":     0.20,
    "sentiment": 0.15,
    "risk":      0.10,
}
CONFIDENCE_THRESHOLD = 0.55


# ── Data fetching ─────────────────────────────────────────────────────────────

def fetch_range(symbol: str, timeframe: str, start: str, end: str) -> pd.DataFrame | None:
    warmup_start = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=WARMUP_DAYS)).strftime("%Y-%m-%d")
    ticker = yf.download(symbol, start=warmup_start, end=end, interval=timeframe,
                         progress=False, auto_adjust=True)
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
    # add 20-candle momentum for sentiment simulation
    df["momentum_20"] = df["close"].pct_change(20) * 100
    return df


# ── Agent simulators ──────────────────────────────────────────────────────────

def sim_technical(row, prev_row, config) -> tuple[str, float]:
    signal = get_signal(row, prev_row, config)
    if signal == "BUY":
        # count how many of 3 indicators aligned
        aligned = sum([
            row["rsi"] < config["buy_rsi_max"],
            macd_buy_ok(row, config),
            row["bb_pct"] < config["buy_bb_pct_max"],
        ])
        return "BUY", 0.4 + aligned * 0.15        # 0.55 / 0.70 / 0.85
    if signal == "SELL":
        aligned = sum([
            row["rsi"] > config["sell_rsi_min"],
            macd_sell_ok(row, config),
            row["bb_pct"] > config["sell_bb_pct_min"],
        ])
        return "SELL", 0.4 + aligned * 0.15
    return "NEUTRAL", 0.45


def sim_macro(row) -> tuple[str, float, bool]:
    """Returns (signal, confidence, is_bear_veto)."""
    close_4h  = row.get("close_4h",  np.nan)
    ema50_4h  = row.get("ema50_4h",  np.nan)
    ema200_4h = row.get("ema200_4h", np.nan)
    ts        = row.get("trend_strength_4h", 0) or 0

    if any(pd.isna(v) for v in [close_4h, ema50_4h, ema200_4h]):
        return "NEUTRAL", 0.4, False

    # BEAR regime — veto
    if close_4h < ema200_4h and ema50_4h < ema200_4h:
        return "SELL", 0.80, True

    # BULL regime
    if close_4h > ema200_4h and ema50_4h > ema200_4h:
        conf = min(0.85, 0.60 + abs(ts) * 5)
        return "BUY", conf, False

    return "NEUTRAL", 0.50, False


def sim_sentiment(row) -> tuple[str, float]:
    mom = row.get("momentum_20", 0) or 0
    if mom < -8:                    # sharp drop to Extreme Fear to contrarian BUY
        return "BUY",  0.62
    if mom < -4:                    # moderate drop to Fear
        return "BUY",  0.52
    if mom > 15:                    # strong rally to Greed to contrarian SELL
        return "SELL", 0.60
    if mom > 8:                     # moderate rally
        return "SELL", 0.50
    return "NEUTRAL", 0.40


def sim_whale(row) -> tuple[str, float]:
    vr  = row.get("volume_ratio", 1.0) or 1.0
    mom = row.get("momentum_20", 0)  or 0
    if vr > 1.8 and mom < -3:      # high volume sell-off
        return "SELL", 0.70
    if vr > 1.8 and mom > 3:       # high volume rally
        return "BUY",  0.68
    if vr < 0.5:                    # very low volume = no conviction
        return "NEUTRAL", 0.30
    return "NEUTRAL", 0.50


def sim_orchestrator(signals: dict) -> tuple[str, float, bool]:
    """
    signals: {agent_name: (signal_str, confidence, ...)}
    Returns: (action, confidence, veto_triggered)

    Decision rule — matches the live orchestrator:
      - Macro BEAR = hard veto, force HOLD
      - Need >= 2 agents saying BUY/SELL *and* weighted score > 0.28
      - Weighted score = sum(weight * confidence) for aligned agents
    """
    macro_sig, macro_conf, bear_veto = signals["macro"]
    if bear_veto:
        return "HOLD", 0.0, True

    buy_count = sell_count = 0
    buy_score = sell_score = 0.0

    for agent, val in signals.items():
        sig, conf = val[0], val[1]
        w = WEIGHTS.get(agent, 0.1)
        if sig == "BUY":
            buy_count += 1
            buy_score += w * conf
        elif sig == "SELL":
            sell_count += 1
            sell_score += w * conf

    # >= 2 agents aligned + meaningful weighted score
    if buy_count >= 2 and buy_score > 0.28 and buy_score > sell_score:
        return "BUY",  round(buy_score, 3), False
    if sell_count >= 2 and sell_score > 0.28 and sell_score > buy_score:
        return "SELL", round(sell_score, 3), False
    return "HOLD", round(max(buy_score, sell_score), 3), False


# ── Main backtest loop ────────────────────────────────────────────────────────

def run_agent_backtest(symbol: str) -> dict | None:
    print(f"\nFetching {symbol} data ({BACKTEST_START} to {BACKTEST_END}) ...")
    signal_df = fetch_range(symbol, "1h",  BACKTEST_START, BACKTEST_END)
    trend_df  = fetch_range(symbol, "4h",  BACKTEST_START, BACKTEST_END)

    if signal_df is None or trend_df is None:
        print(f"  No data for {symbol}"); return None

    df = attach_higher_timeframe_context(signal_df, trend_df)

    # Filter to the actual backtest window (after warmup)
    start_ts = pd.Timestamp(BACKTEST_START, tz="UTC")
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df[df["time"] >= start_ts].reset_index(drop=True)
    print(f"  Backtest candles: {len(df)}  ({df['time'].iloc[0].date()} to {df['time'].iloc[-1].date()})")

    config  = get_symbol_config(symbol)
    balance = START_BALANCE
    position = None
    trades   = []
    equity   = []
    agent_decision_log = []

    for i in range(1, len(df)):
        row      = df.iloc[i]
        prev_row = df.iloc[i - 1]
        price    = row["close"]

        # ── Equity tracking ───────────────────────────────────────────────
        eq = balance + (row.get("qty_held", 0) or 0) * price
        if position:
            eq = balance + position["qty"] * price
        equity.append(eq)

        # ── Position management ───────────────────────────────────────────
        if position:
            held_hours = (pd.Timestamp(row["time"]) - pd.Timestamp(position["entry_time"])).total_seconds() / 3600

            # trailing stop
            trail_mult = config.get("trailing_stop_multiplier")
            trail_act  = config.get("trail_activation_multiplier")
            be_trigger = config.get("break_even_trigger_atr")
            if be_trigger and price >= position["entry"] + row["atr"] * be_trigger:
                position["stop_price"] = max(position["stop_price"], position["entry"])
            if trail_mult and (trail_act is None or price >= position["entry"] + row["atr"] * trail_act):
                position["stop_price"] = max(position["stop_price"], price - row["atr"] * trail_mult)

            hit_stop   = price <= position["stop_price"]
            hit_target = price >= position["target_price"]
            hit_max    = config.get("max_hold_hours") and held_hours >= config["max_hold_hours"]

            if hit_stop or hit_target or hit_max:
                reason = "STOP_LOSS" if hit_stop else ("TAKE_PROFIT" if hit_target else "MAX_HOLD")
                ct = close_position(position, price, row["time"], reason, symbol)
                balance += ct["net_returned"]; trades.append(ct); position = None
                continue

        # ── Simulate all agents ───────────────────────────────────────────
        tech_sig,  tech_conf           = sim_technical(row, prev_row, config)
        macro_sig, macro_conf, b_veto  = sim_macro(row)
        sent_sig,  sent_conf           = sim_sentiment(row)
        whale_sig, whale_conf          = sim_whale(row)

        signals = {
            "technical": (tech_sig,  tech_conf),
            "macro":     (macro_sig, macro_conf, b_veto),
            "sentiment": (sent_sig,  sent_conf),
            "whale":     (whale_sig, whale_conf),
            "risk":      ("NEUTRAL", 0.85),
        }

        action, conf, veto = sim_orchestrator(signals)

        agent_decision_log.append({
            "time":      str(row["time"]),
            "price":     round(price, 2),
            "action":    action,
            "conf":      conf,
            "veto":      veto,
            "technical": tech_sig,
            "macro":     macro_sig,
            "sentiment": sent_sig,
            "whale":     whale_sig,
        })

        # ── Execute decision ──────────────────────────────────────────────
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
                "stop_price":   price - row["atr"] * ATR_STOP_MULTIPLIER,
                "target_price": price + row["atr"] * ATR_TARGET_MULTIPLIER,
                "entry_context": {"conf": conf},
            }
            trades.append({"type": "OPEN", "side": "LONG", "symbol": symbol,
                           "price": price, "time": row["time"]})

    # Close any open position at end
    if position:
        ct = close_position(position, df["close"].iloc[-1], df["time"].iloc[-1], "END_OF_TEST", symbol)
        balance += ct["net_returned"]; trades.append(ct)

    # ── Results ───────────────────────────────────────────────────────────────
    closed = [t for t in trades if t["type"] == "CLOSE"]
    wins   = [t for t in closed if t["pnl_usd"] > 0]
    losses = [t for t in closed if t["pnl_usd"] <= 0]

    total_return  = (balance - START_BALANCE) / START_BALANCE * 100
    win_rate      = len(wins) / len(closed) * 100 if closed else 0
    avg_win       = sum(t["pnl_usd"] for t in wins)   / len(wins)   if wins   else 0
    avg_loss      = sum(t["pnl_usd"] for t in losses) / len(losses) if losses else 0
    avg_hold      = sum(t["hold_hours"] for t in closed) / len(closed) if closed else 0
    profit_factor = abs(avg_win / avg_loss) if avg_loss else float("inf")

    peak = START_BALANCE; max_dd = 0
    for val in equity:
        if val > peak: peak = val
        dd = (peak - val) / peak
        if dd > max_dd: max_dd = dd

    # Agent vote breakdown
    buys   = sum(1 for d in agent_decision_log if d["action"] == "BUY")
    sells  = sum(1 for d in agent_decision_log if d["action"] == "SELL")
    holds  = sum(1 for d in agent_decision_log if d["action"] == "HOLD")
    vetoes = sum(1 for d in agent_decision_log if d["veto"])

    passed = (win_rate > 55 and profit_factor > 1.3 and
              max_dd < 0.08 and total_return > 0 and len(closed) >= 5)

    print(f"\n{'='*60}")
    print(f"  AGENT BACKTEST: {symbol} | Oct-Dec 2024 (Bull Run)")
    print(f"{'='*60}")
    print(f"  Period:          {BACKTEST_START} to {BACKTEST_END}")
    print(f"  Starting:        ${START_BALANCE:,.2f}")
    print(f"  Final:           ${balance:,.2f}")
    print(f"  Total Return:    {total_return:+.2f}%")
    print(f"  {'-'*50}")
    print(f"  Trades:          {len(closed)}")
    print(f"  Win Rate:        {win_rate:.1f}%  (need >55%)")
    print(f"  Profit Factor:   {profit_factor:.2f}  (need >1.3)")
    print(f"  Max Drawdown:    {max_dd*100:.1f}%  (need <8%)")
    print(f"  Avg Win:         ${avg_win:+.2f}")
    print(f"  Avg Loss:        ${avg_loss:+.2f}")
    print(f"  Avg Hold Hours:  {avg_hold:.1f}h")
    print(f"  {'-'*50}")
    print(f"  Agent Decisions: {len(agent_decision_log)} candles evaluated")
    print(f"    BUY signals:   {buys}  ({buys/len(agent_decision_log)*100:.1f}%)")
    print(f"    SELL signals:  {sells}  ({sells/len(agent_decision_log)*100:.1f}%)")
    print(f"    HOLD:          {holds}  ({holds/len(agent_decision_log)*100:.1f}%)")
    print(f"    Macro vetoes:  {vetoes}")
    print(f"  {'-'*50}")

    # Exit reason breakdown
    for reason in ["STOP_LOSS", "TAKE_PROFIT", "MAX_HOLD", "SIGNAL", "END_OF_TEST"]:
        subset = [t for t in closed if t["reason"] == reason]
        if subset:
            wr = sum(1 for t in subset if t["pnl_usd"] > 0) / len(subset) * 100
            print(f"  {reason:<16} {len(subset):3} trades  WR: {wr:.0f}%")

    print(f"  {'-'*50}")
    print(f"  VERDICT: {'PASS' if passed else 'FAIL'}")
    print(f"{'='*60}\n")

    # Export trade log
    export_trade_log(symbol, closed, "1h_agent", 92)

    return {
        "symbol":        symbol,
        "return":        total_return,
        "trades":        len(closed),
        "win_rate":      win_rate,
        "profit_factor": profit_factor,
        "max_drawdown":  max_dd * 100,
        "final_balance": balance,
        "avg_hold_hours": avg_hold,
        "buy_signals":   buys,
        "sell_signals":  sells,
        "vetoes":        vetoes,
        "passed":        passed,
    }


if __name__ == "__main__":
    print("\nCrypto Orchestra — Agent Pipeline Backtest")
    print(f"Period: {BACKTEST_START} to {BACKTEST_END}  (Oct-Dec 2024 Bull Run)")
    print("=" * 60)

    results = []
    for symbol in SYMBOLS:
        r = run_agent_backtest(symbol)
        if r:
            results.append(r)

    if len(results) > 1:
        print("\nCOMPARISON SUMMARY")
        print("=" * 60)
        print(f"  {'Asset':<10} {'Return':>8} {'WinRate':>8} {'PF':>6} {'MaxDD':>7} {'Trades':>7} {'Verdict'}")
        print(f"  {'-'*58}")
        for r in results:
            verdict = "PASS" if r["passed"] else "FAIL"
            print(f"  {r['symbol']:<10} {r['return']:>+7.2f}% {r['win_rate']:>7.1f}% "
                  f"{r['profit_factor']:>6.2f} {r['max_drawdown']:>6.1f}% "
                  f"{r['trades']:>7}  {verdict}")
        print("=" * 60)
