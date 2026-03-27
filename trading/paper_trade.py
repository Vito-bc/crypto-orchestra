from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting.backtest import (
    attach_higher_timeframe_context,
    evaluate_entry_components,
    get_signal,
    get_symbol_config,
    prepare_timeframe_df,
)
from notifications.telegram import format_trade_event_message, send_telegram_message


DEFAULT_SYMBOL = "ETH-USD"
DEFAULT_SIGNAL_TIMEFRAME = "1h"
DEFAULT_LOOKBACK_DAYS = 90

LOG_DIR = Path("logs")
JSONL_LOG = LOG_DIR / "paper_signals.jsonl"
CSV_LOG = LOG_DIR / "paper_signals.csv"
POSITION_STATE = LOG_DIR / "paper_position_eth.json"
EVENTS_JSONL_LOG = LOG_DIR / "paper_position_events.jsonl"
EVENTS_CSV_LOG = LOG_DIR / "paper_position_events.csv"
HEALTH_JSONL_LOG = LOG_DIR / "paper_runner_health.jsonl"


def build_signal_snapshot(symbol: str = DEFAULT_SYMBOL, days: int = DEFAULT_LOOKBACK_DAYS) -> dict | None:
    signal_df = prepare_timeframe_df(symbol, DEFAULT_SIGNAL_TIMEFRAME, days)
    trend_df = prepare_timeframe_df(symbol, "4h", days)
    if signal_df is None or trend_df is None:
        return None

    df = attach_higher_timeframe_context(signal_df, trend_df)
    if len(df) < 2:
        return None

    row = df.iloc[-1]
    prev_row = df.iloc[-2]
    config = get_symbol_config(symbol)
    signal = get_signal(row, prev_row, config)
    entry_components = evaluate_entry_components(row, prev_row, config) or {}

    snapshot = {
        "logged_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "timeframe": DEFAULT_SIGNAL_TIMEFRAME,
        "lookback_days": days,
        "candle_time": str(row["time"]),
        "close": float(row["close"]),
        "signal": signal,
        "trend_ok": bool(entry_components.get("trend_ok", False)),
        "macd_ok": bool(entry_components.get("macd_ok", False)),
        "macd_cross_ok": bool(entry_components.get("macd_cross_ok", False)),
        "rsi_ok": bool(entry_components.get("rsi_ok", False)),
        "bb_ok": bool(entry_components.get("bb_ok", False)),
        "signal_count_ok": bool(entry_components.get("signal_count_ok", False)),
        "volume_ok": bool(entry_components.get("volume_ok", False)),
        "buy_ready": bool(entry_components.get("buy_ready", False)),
        "blocked_by_macd": bool(entry_components.get("blocked_by_macd", False)),
        "blocked_by_rsi": bool(entry_components.get("blocked_by_rsi", False)),
        "blocked_by_bb": bool(entry_components.get("blocked_by_bb", False)),
        "blocked_by_volume": bool(entry_components.get("blocked_by_volume", False)),
        "rsi_1h": float(row["rsi"]),
        "macd_diff_1h": float(row["macd_diff"]),
        "bb_pct_1h": float(row["bb_pct"]),
        "volume_ratio_1h": float(row["volume_ratio"]),
        "atr_1h": float(row["atr"]),
        "ema50_1h": float(row["ema50"]),
        "ema200_1h": float(row["ema200"]),
        "close_4h": float(row["close_4h"]) if "close_4h" in row else None,
        "ema50_4h": float(row["ema50_4h"]) if "ema50_4h" in row else None,
        "ema200_4h": float(row["ema200_4h"]) if "ema200_4h" in row else None,
        "trend_4h": row.get("trend_4h", ""),
        "trend_strength_4h": float(row["trend_strength_4h"]) if "trend_strength_4h" in row else None,
    }
    return snapshot


def append_logs(snapshot: dict) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    with JSONL_LOG.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(snapshot) + "\n")

    write_header = not CSV_LOG.exists()
    with CSV_LOG.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(snapshot.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(snapshot)


def load_position_state() -> dict[str, Any]:
    if not POSITION_STATE.exists():
        return {
            "status": "FLAT",
            "symbol": DEFAULT_SYMBOL,
            "entry_price": None,
            "entry_time": None,
            "stop_price": None,
            "target_price": None,
            "last_action": "NONE",
            "last_updated_utc": None,
        }

    with POSITION_STATE.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_position_state(state: dict[str, Any]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with POSITION_STATE.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2)


def append_event_log(event: dict[str, Any]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    with EVENTS_JSONL_LOG.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event) + "\n")

    write_header = not EVENTS_CSV_LOG.exists()
    with EVENTS_CSV_LOG.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(event.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(event)


def append_health_log(status: str, details: dict[str, Any] | None = None) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "logged_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
    }
    if details:
        payload.update(details)

    with HEALTH_JSONL_LOG.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def format_runner_issue_message(status: str, message: str) -> str:
    return "\n".join(
        [
            "Crypto Orchestra Runner Alert",
            f"Status: {status}",
            f"Message: {message}",
            f"Logged UTC: {datetime.now(timezone.utc).isoformat()}",
        ]
    )


def evaluate_position_action(snapshot: dict[str, Any], state: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    config = get_symbol_config(snapshot["symbol"])
    state = dict(state)
    state["last_updated_utc"] = snapshot["logged_at_utc"]

    if state["status"] != "LONG":
        if snapshot["signal"] == "BUY":
            entry_price = snapshot["close"]
            atr = snapshot["atr_1h"]
            state.update(
                {
                    "status": "LONG",
                    "symbol": snapshot["symbol"],
                    "entry_price": entry_price,
                    "entry_time": snapshot["candle_time"],
                    "stop_price": entry_price - atr * 1.5,
                    "target_price": entry_price + atr * 3.0,
                    "last_action": "OPEN_LONG",
                }
            )
            event = {
                "logged_at_utc": snapshot["logged_at_utc"],
                "symbol": snapshot["symbol"],
                "event": "OPEN_LONG",
                "reason": "BUY_SIGNAL",
                "price": entry_price,
                "candle_time": snapshot["candle_time"],
            }
            return state, event

        state["last_action"] = "HOLD_FLAT"
        return state, None

    current_price = snapshot["close"]
    atr = snapshot["atr_1h"]
    entry_price = float(state["entry_price"])
    stop_price = float(state["stop_price"])
    target_price = float(state["target_price"])

    trail_multiplier = config.get("trailing_stop_multiplier")
    activation_multiplier = config.get("trail_activation_multiplier")
    if (
        trail_multiplier is not None
        and (
            activation_multiplier is None
            or current_price >= entry_price + atr * activation_multiplier
        )
    ):
        trailing_stop = current_price - atr * trail_multiplier
        stop_price = max(stop_price, trailing_stop)
        state["stop_price"] = stop_price

    entry_ts = datetime.fromisoformat(str(state["entry_time"]).replace("Z", "+00:00"))
    candle_ts = datetime.fromisoformat(str(snapshot["candle_time"]).replace("Z", "+00:00"))
    hold_hours = (candle_ts - entry_ts).total_seconds() / 3600
    max_hold_hours = config.get("max_hold_hours")

    exit_reason = None
    if current_price <= stop_price:
        exit_reason = "STOP_LOSS"
    elif current_price >= target_price:
        exit_reason = "TAKE_PROFIT"
    elif snapshot["signal"] == "SELL":
        exit_reason = "SIGNAL"
    elif max_hold_hours is not None and hold_hours >= max_hold_hours:
        exit_reason = "MAX_HOLD"

    if exit_reason is None:
        state["last_action"] = "HOLD_LONG"
        return state, None

    pnl_pct = (current_price - entry_price) / entry_price * 100
    event = {
        "logged_at_utc": snapshot["logged_at_utc"],
        "symbol": snapshot["symbol"],
        "event": "CLOSE_LONG",
        "reason": exit_reason,
        "price": current_price,
        "entry_price": entry_price,
        "candle_time": snapshot["candle_time"],
        "entry_time": state["entry_time"],
        "hold_hours": round(hold_hours, 2),
        "pnl_pct": round(pnl_pct, 4),
    }
    state.update(
        {
            "status": "FLAT",
            "entry_price": None,
            "entry_time": None,
            "stop_price": None,
            "target_price": None,
            "last_action": f"CLOSE_LONG_{exit_reason}",
        }
    )
    return state, event


def print_snapshot(snapshot: dict, state: dict[str, Any], event: dict[str, Any] | None) -> None:
    print("\nPHASE B PAPER SIGNAL")
    print("=" * 60)
    print(f"Symbol:          {snapshot['symbol']}")
    print(f"Candle Time:     {snapshot['candle_time']}")
    print(f"Signal:          {snapshot['signal']}")
    print(f"Paper Status:    {state['status']}")
    print(f"Last Action:     {state['last_action']}")
    print(f"Close:           {snapshot['close']:.2f}")
    print(f"Trend OK:        {snapshot['trend_ok']}")
    print(f"MACD Cross OK:   {snapshot['macd_cross_ok']}")
    print(f"RSI OK:          {snapshot['rsi_ok']}")
    print(f"BB OK:           {snapshot['bb_ok']}")
    print(f"Volume OK:       {snapshot['volume_ok']}")
    print(f"Buy Ready:       {snapshot['buy_ready']}")
    print(f"Blocked by MACD: {snapshot['blocked_by_macd']}")
    print(f"Blocked by BB:   {snapshot['blocked_by_bb']}")
    print(f"Blocked by Volume: {snapshot['blocked_by_volume']}")
    print("-" * 60)
    print(f"RSI 1h:          {snapshot['rsi_1h']:.2f}")
    print(f"MACD Diff 1h:    {snapshot['macd_diff_1h']:.6f}")
    print(f"BB % 1h:         {snapshot['bb_pct_1h']:.3f}")
    print(f"Volume Ratio 1h: {snapshot['volume_ratio_1h']:.3f}")
    print(f"Trend 4h:        {snapshot['trend_4h']}")
    print(f"Trend Strength:  {snapshot['trend_strength_4h']:.6f}")
    print("-" * 60)
    if state["status"] == "LONG":
        print(f"Entry Price:     {state['entry_price']:.2f}")
        print(f"Stop Price:      {state['stop_price']:.2f}")
        print(f"Target Price:    {state['target_price']:.2f}")
        print("-" * 60)
    if event is not None:
        print("Paper Event:")
        print(f"  Event:         {event['event']}")
        print(f"  Reason:        {event['reason']}")
        if "pnl_pct" in event:
            print(f"  PnL %:         {event['pnl_pct']:.4f}")
        print("-" * 60)
    print(f"JSONL Log:       {JSONL_LOG}")
    print(f"CSV Log:         {CSV_LOG}")
    print(f"State File:      {POSITION_STATE}")
    print(f"Events JSONL:    {EVENTS_JSONL_LOG}")
    print(f"Events CSV:      {EVENTS_CSV_LOG}")
    print(f"Health JSONL:    {HEALTH_JSONL_LOG}")


def main() -> None:
    try:
        snapshot = build_signal_snapshot()
        if snapshot is None:
            append_health_log("NO_SNAPSHOT", {"symbol": DEFAULT_SYMBOL})
            send_telegram_message(
                format_runner_issue_message("NO_SNAPSHOT", "No signal snapshot could be built.")
            )
            print("No signal snapshot could be built.")
            return

        state = load_position_state()
        updated_state, event = evaluate_position_action(snapshot, state)
        append_logs(snapshot)
        save_position_state(updated_state)
        append_health_log(
            "SUCCESS",
            {
                "symbol": snapshot["symbol"],
                "signal": snapshot["signal"],
                "buy_ready": snapshot["buy_ready"],
                "paper_status": updated_state["status"],
                "candle_time": snapshot["candle_time"],
            },
        )
        telegram_sent = False
        if event is not None:
            append_event_log(event)
            telegram_sent = send_telegram_message(format_trade_event_message(event))
        print_snapshot(snapshot, updated_state, event)
        if event is not None:
            print(f"Telegram Alert:  {'sent' if telegram_sent else 'skipped'}")
    except Exception as exc:
        append_health_log(
            "ERROR",
            {
                "symbol": DEFAULT_SYMBOL,
                "error": str(exc),
                "error_type": exc.__class__.__name__,
            },
        )
        send_telegram_message(format_runner_issue_message("ERROR", str(exc)))
        raise


if __name__ == "__main__":
    main()
