# backtesting/backtest.py
# Uses free Yahoo Finance data to evaluate a simple crypto strategy.

import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator
from ta.trend import MACD, EMAIndicator
from ta.volatility import AverageTrueRange, BollingerBands
from pathlib import Path


FEE_RATE = 0.006
TRADE_SIZE_PCT = 0.02
ATR_STOP_MULTIPLIER = 1.5
ATR_TARGET_MULTIPLIER = 3.0
MIN_TREND_STRENGTH = 0.003
MIN_VOLUME_RATIO = 1.05
MAX_VOLUME_RATIO = 4.0
MAX_HOLD_HOURS = {
    "BTC-USD": 12,
    "ETH-USD": 8,
}
START_BALANCE = 10000

SYMBOLS = ["BTC-USD", "ETH-USD"]

EXIT_SETTINGS = {
    "BTC-USD": {
        "trailing_stop_multiplier": 1.0,
        "break_even_trigger_atr": 1.0,
    },
    "ETH-USD": {
        "trailing_stop_multiplier": None,
        "break_even_trigger_atr": None,
    },
}


def fetch_historical(symbol, timeframe="1h", days=365):
    cache_dir = Path("data") / "yfinance"
    cache_dir.mkdir(parents=True, exist_ok=True)

    import yfinance as yf
    from datetime import datetime, timedelta

    yf.set_tz_cache_location(str(cache_dir.resolve()))

    print(f"Fetching {days} days of {symbol} data from Yahoo Finance...")

    end = datetime.now()
    start = end - timedelta(days=days)

    ticker = yf.download(
        symbol,
        start=start,
        end=end,
        interval=timeframe,
        progress=False,
        auto_adjust=True,
    )

    if ticker.empty:
        print(f"  No data for {symbol}")
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
    print(f"  Got {len(df)} candles from {df['time'].iloc[0].date()} to {df['time'].iloc[-1].date()}")
    return df


def calculate_indicators(df):
    df["rsi"] = RSIIndicator(df["close"], window=14).rsi()
    df["volume_sma20"] = df["volume"].rolling(window=20).mean()
    df["volume_ratio"] = df["volume"] / df["volume_sma20"]

    macd = MACD(df["close"])
    df["macd_diff"] = macd.macd_diff()
    df["macd_prev"] = df["macd_diff"].shift(1)

    bb = BollingerBands(df["close"], window=20, window_dev=2)
    df["bb_upper"] = bb.bollinger_hband()
    df["bb_lower"] = bb.bollinger_lband()
    df["bb_mid"] = bb.bollinger_mavg()
    df["bb_pct"] = (df["close"] - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"])
    df["atr"] = AverageTrueRange(df["high"], df["low"], df["close"], window=14).average_true_range()

    df["ema50"] = EMAIndicator(df["close"], window=50).ema_indicator()
    df["ema200"] = EMAIndicator(df["close"], window=200).ema_indicator()
    df["trend"] = np.where(df["ema50"] > df["ema200"], "bull", "bear")

    swing_high = df["high"].rolling(window=50).max()
    swing_low = df["low"].rolling(window=50).min()
    swing_range = swing_high - swing_low

    df["fib_382"] = swing_high - swing_range * 0.382
    df["fib_500"] = swing_high - swing_range * 0.500
    df["fib_618"] = swing_high - swing_range * 0.618
    df["fib_support_zone"] = (
        (df["close"] >= df["fib_618"]) &
        (df["close"] <= df["fib_382"])
    )
    df["fib_deep_discount"] = df["close"] <= df["fib_500"]
    return df


def prepare_timeframe_df(symbol, timeframe, days):
    df = fetch_historical(symbol, timeframe=timeframe, days=days)
    if df is None:
        return None
    return calculate_indicators(df)


def attach_higher_timeframe_context(signal_df, trend_df):
    trend_cols = trend_df[["time", "ema50", "ema200", "trend"]].copy()
    trend_cols["trend_strength_4h"] = (trend_cols["ema50"] - trend_cols["ema200"]) / trend_cols["ema200"]
    trend_cols = trend_cols.rename(columns={
        "ema50": "ema50_4h",
        "ema200": "ema200_4h",
        "trend": "trend_4h",
    })

    merged = pd.merge_asof(
        signal_df.sort_values("time"),
        trend_cols.sort_values("time"),
        on="time",
        direction="backward",
    )
    return merged


def attach_entry_timeframe_context(signal_df, entry_df):
    entry_cols = entry_df[["time", "rsi", "macd_diff", "macd_prev", "bb_pct", "close", "ema50"]].copy()
    entry_cols = entry_cols.rename(columns={
        "rsi": "rsi_15m",
        "macd_diff": "macd_diff_15m",
        "macd_prev": "macd_prev_15m",
        "bb_pct": "bb_pct_15m",
        "close": "close_15m",
        "ema50": "ema50_15m",
    })

    merged = pd.merge_asof(
        signal_df.sort_values("time"),
        entry_cols.sort_values("time"),
        on="time",
        direction="backward",
    )
    return merged


def get_signal(row, prev_row):
    if (
        pd.isna(row["rsi"])
        or pd.isna(row["macd_diff"])
        or pd.isna(row["bb_pct"])
        or pd.isna(row["ema50"])
        or pd.isna(row["fib_382"])
        or pd.isna(row["atr"])
        or pd.isna(row["volume_ratio"])
    ):
        return "HOLD"

    use_entry_timing = bool(row.get("use_15m_timing", False))
    entry_confirmation = True
    if use_entry_timing:
        entry_confirmation = (
            not pd.isna(row.get("macd_diff_15m", np.nan))
            and (
                row["macd_diff_15m"] > 0
                or row["close_15m"] > row["ema50_15m"]
                or row["rsi_15m"] < 55
                or row["bb_pct_15m"] < 0.60
            )
        )

    uptrend = (
        row["ema50"] > row["ema200"]
        and row["close"] > row["ema50"]
        and row.get("trend_4h") == "bull"
        and row.get("ema50_4h", np.nan) > row.get("ema200_4h", np.nan)
        and row.get("trend_strength_4h", 0) > MIN_TREND_STRENGTH
    )
    volume_ok = MIN_VOLUME_RATIO < row["volume_ratio"] < MAX_VOLUME_RATIO

    buy_signals = 0
    if row["rsi"] < 55:
        buy_signals += 1
    if row["macd_diff"] > 0 and row["macd_prev"] <= 0:
        buy_signals += 1
    if row["bb_pct"] < 0.50:
        buy_signals += 1

    sell_signals = 0
    if row["rsi"] > 58:
        sell_signals += 1
    if row["macd_diff"] < 0 and row["macd_prev"] >= 0:
        sell_signals += 1
    if row["bb_pct"] > 0.70:
        sell_signals += 1

    if uptrend and row["macd_diff"] > 0 and buy_signals >= 2 and entry_confirmation and volume_ok:
        return "BUY"
    if sell_signals >= 2:
        return "SELL"
    return "HOLD"


def evaluate_entry_components(row, prev_row):
    if (
        pd.isna(row["rsi"])
        or pd.isna(row["macd_diff"])
        or pd.isna(row["bb_pct"])
        or pd.isna(row["ema50"])
        or pd.isna(row["fib_382"])
        or pd.isna(row["atr"])
        or pd.isna(row["volume_ratio"])
    ):
        return None

    use_entry_timing = bool(row.get("use_15m_timing", False))
    entry_confirmation = True
    if use_entry_timing:
        entry_confirmation = (
            not pd.isna(row.get("macd_diff_15m", np.nan))
            and (
                row["macd_diff_15m"] > 0
                or row["close_15m"] > row["ema50_15m"]
                or row["rsi_15m"] < 55
                or row["bb_pct_15m"] < 0.60
            )
        )

    uptrend = (
        row["ema50"] > row["ema200"]
        and row["close"] > row["ema50"]
        and row.get("trend_4h") == "bull"
        and row.get("ema50_4h", np.nan) > row.get("ema200_4h", np.nan)
        and row.get("trend_strength_4h", 0) > MIN_TREND_STRENGTH
    )
    volume_ok = MIN_VOLUME_RATIO < row["volume_ratio"] < MAX_VOLUME_RATIO

    buy_signals = 0
    if row["rsi"] < 55:
        buy_signals += 1
    if row["macd_diff"] > 0 and row["macd_prev"] <= 0:
        buy_signals += 1
    if row["bb_pct"] < 0.50:
        buy_signals += 1

    return {
        "trend_ok": uptrend,
        "macd_ok": row["macd_diff"] > 0,
        "signal_count_ok": buy_signals >= 2,
        "volume_ok": volume_ok,
        "timing_ok": bool(entry_confirmation),
        "buy_ready": bool(uptrend and row["macd_diff"] > 0 and buy_signals >= 2 and entry_confirmation and volume_ok),
    }


def close_position(position, price, time, reason, symbol):
    if position["side"] == "LONG":
        gross_value = position["qty"] * price
        exit_fee = gross_value * FEE_RATE
        net_returned = gross_value - exit_fee
        pnl_pct = (price - position["entry"]) / position["entry"]
    else:
        gross_pnl = (position["entry"] - price) * position["qty"]
        gross_value = position["cost"] + gross_pnl
        exit_fee = (position["qty"] * price) * FEE_RATE
        net_returned = gross_value - exit_fee
        pnl_pct = (position["entry"] - price) / position["entry"]

    pnl_usd = net_returned - position["cost"]
    hold_hours = (pd.Timestamp(time) - pd.Timestamp(position["entry_time"])).total_seconds() / 3600

    trade = {
        "type": "CLOSE",
        "side": position["side"],
        "symbol": symbol,
        "entry": position["entry"],
        "exit": price,
        "pnl_usd": pnl_usd,
        "pnl_pct": pnl_pct,
        "exit_time": time,
        "entry_time": position["entry_time"],
        "hold_hours": hold_hours,
        "reason": reason,
        "net_returned": net_returned,
    }
    trade.update(position.get("entry_context", {}))
    return trade


def print_breakdown(label, subset):
    if not subset:
        return
    wins = [t for t in subset if t["pnl_usd"] > 0]
    win_rate = len(wins) / len(subset) * 100
    avg_pnl = sum(t["pnl_usd"] for t in subset) / len(subset)
    print(f"  {label:12} {len(subset):3} trades  Win Rate: {win_rate:5.1f}%  Avg PnL: ${avg_pnl:+.2f}")


def export_trade_log(symbol, closed_trades, timeframe, days):
    if not closed_trades:
        return None

    output_dir = Path("analysis") / "trade_logs"
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{symbol.replace('-', '_')}_{timeframe}_{days}d_trades.csv"
    output_path = output_dir / filename

    trade_df = pd.DataFrame(closed_trades).copy()
    trade_df.to_csv(output_path, index=False)
    return output_path


def run_backtest(symbol, timeframe="1h", days=365):
    signal_df = prepare_timeframe_df(symbol, timeframe, days)
    trend_df = prepare_timeframe_df(symbol, "4h", days)
    use_15m_timing = days <= 60
    entry_df = prepare_timeframe_df(symbol, "15m", days) if use_15m_timing else None

    if signal_df is None or trend_df is None:
        return None

    df = attach_higher_timeframe_context(signal_df, trend_df)
    if use_15m_timing and entry_df is not None:
        df = attach_entry_timeframe_context(df, entry_df)

    df["use_15m_timing"] = use_15m_timing

    balance = START_BALANCE
    position = None
    trades = []
    equity = []
    exit_settings = EXIT_SETTINGS.get(symbol, {})
    max_hold_hours = MAX_HOLD_HOURS.get(symbol)
    diagnostics = {
        "eligible_rows": 0,
        "trend_ok": 0,
        "macd_ok": 0,
        "signal_count_ok": 0,
        "volume_ok": 0,
        "timing_ok": 0,
        "buy_ready": 0,
    }

    for i in range(1, len(df)):
        row = df.iloc[i]
        prev_row = df.iloc[i - 1]
        price = row["close"]

        entry_check = evaluate_entry_components(row, prev_row)
        if entry_check is not None:
            diagnostics["eligible_rows"] += 1
            for key in ["trend_ok", "macd_ok", "signal_count_ok", "volume_ok", "timing_ok", "buy_ready"]:
                if entry_check[key]:
                    diagnostics[key] += 1

        equity_value = balance
        if position:
            if position["side"] == "LONG":
                equity_value += position["qty"] * price
            else:
                unrealized = (position["entry"] - price) * position["qty"]
                equity_value += position["cost"] + unrealized
        equity.append({"time": row["time"], "balance": equity_value})

        if position:
            held_hours = (pd.Timestamp(row["time"]) - pd.Timestamp(position["entry_time"])).total_seconds() / 3600

            if position["side"] == "LONG":
                pnl_pct = (price - position["entry"]) / position["entry"]
            else:
                pnl_pct = (position["entry"] - price) / position["entry"]

            if position["side"] == "LONG":
                break_even_trigger_atr = exit_settings.get("break_even_trigger_atr")
                trailing_stop_multiplier = exit_settings.get("trailing_stop_multiplier")

                if (
                    break_even_trigger_atr is not None
                    and price >= position["entry"] + row["atr"] * break_even_trigger_atr
                ):
                    position["stop_price"] = max(position["stop_price"], position["entry"])

                if trailing_stop_multiplier is not None:
                    trailing_stop = price - row["atr"] * trailing_stop_multiplier
                    position["stop_price"] = max(position["stop_price"], trailing_stop)

            hit_stop = price <= position["stop_price"]
            hit_target = price >= position["target_price"]
            hit_max_hold = max_hold_hours is not None and held_hours >= max_hold_hours

            if hit_stop or hit_target or hit_max_hold:
                close_trade = close_position(
                    position,
                    price,
                    row["time"],
                    "STOP_LOSS" if hit_stop else ("TAKE_PROFIT" if hit_target else "MAX_HOLD"),
                    symbol,
                )
                balance += close_trade["net_returned"]
                trades.append(close_trade)
                position = None
                continue

        signal = get_signal(row, prev_row)

        if signal == "SELL" and position is not None and position["side"] == "LONG":
            close_trade = close_position(position, price, row["time"], "SIGNAL", symbol)
            balance += close_trade["net_returned"]
            trades.append(close_trade)
            position = None
            continue

        if position is not None:
            continue

        usd_amount = balance * TRADE_SIZE_PCT
        fee = usd_amount * FEE_RATE
        total_cost = usd_amount + fee
        if total_cost > balance:
            continue

        if signal == "BUY":
            balance -= total_cost
            position = {
                "side": "LONG",
                "qty": usd_amount / price,
                "entry": price,
                "cost": usd_amount,
                "entry_time": row["time"],
                "stop_price": price - row["atr"] * ATR_STOP_MULTIPLIER,
                "target_price": price + row["atr"] * ATR_TARGET_MULTIPLIER,
                "entry_context": {
                    "entry_rsi_1h": row["rsi"],
                    "entry_macd_diff_1h": row["macd_diff"],
                    "entry_bb_pct_1h": row["bb_pct"],
                    "entry_volume_ratio_1h": row["volume_ratio"],
                    "entry_atr_1h": row["atr"],
                    "entry_trend_strength_4h": row.get("trend_strength_4h", np.nan),
                    "entry_trend_4h": row.get("trend_4h", ""),
                    "entry_rsi_15m": row.get("rsi_15m", np.nan),
                    "entry_macd_diff_15m": row.get("macd_diff_15m", np.nan),
                    "entry_bb_pct_15m": row.get("bb_pct_15m", np.nan),
                    "entry_volume_1h": row.get("volume", np.nan),
                },
            }
            trades.append({
                "type": "OPEN",
                "side": "LONG",
                "symbol": symbol,
                "price": price,
                "time": row["time"],
            })

    if position:
        close_trade = close_position(
            position,
            df["close"].iloc[-1],
            df["time"].iloc[-1],
            "END_OF_TEST",
            symbol,
        )
        balance += close_trade["net_returned"]
        trades.append(close_trade)

    closed_trades = [t for t in trades if t["type"] == "CLOSE"]
    trade_log_path = export_trade_log(symbol, closed_trades, timeframe, days)
    wins = [t for t in closed_trades if t["pnl_usd"] > 0]
    losses = [t for t in closed_trades if t["pnl_usd"] <= 0]
    long_trades = [t for t in closed_trades if t["side"] == "LONG"]
    total_return = (balance - START_BALANCE) / START_BALANCE * 100
    win_rate = len(wins) / len(closed_trades) * 100 if closed_trades else 0
    avg_win = sum(t["pnl_usd"] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t["pnl_usd"] for t in losses) / len(losses) if losses else 0
    profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float("inf")

    eq_values = [e["balance"] for e in equity]
    peak = START_BALANCE
    max_dd = 0
    for val in eq_values:
        if val > peak:
            peak = val
        dd = (peak - val) / peak
        if dd > max_dd:
            max_dd = dd

    print(f"\n{'=' * 55}")
    print(f"  BACKTEST RESULTS: {symbol} | {timeframe} | {days} days")
    print(f"{'=' * 55}")
    print(f"  Starting Balance:  ${START_BALANCE:,.2f}")
    print(f"  Final Balance:     ${balance:,.2f}")
    print(f"  Total Return:      {total_return:+.2f}%")
    print(f"  {'-' * 45}")
    mode_label = "4h bullish trend + 15m timing" if use_15m_timing else "4h bullish trend only"
    print(f"  Regime Filter:     {mode_label}")
    print(f"  Total Trades:      {len(closed_trades)}")
    print(f"  Win Rate:          {win_rate:.1f}%  (need >55%)")
    print(f"  Profit Factor:     {profit_factor:.2f}  (need >1.3)")
    print(f"  Max Drawdown:      {max_dd * 100:.1f}%  (need <8%)")
    print(f"  {'-' * 45}")
    print(f"  Avg Win:           ${avg_win:+.2f}")
    print(f"  Avg Loss:          ${avg_loss:+.2f}")
    print(f"  {'-' * 45}")
    if trade_log_path:
        print(f"  Trade Log:         {trade_log_path}")
        print(f"  {'-' * 45}")
    print_breakdown("LONG", long_trades)
    print_breakdown("SIGNAL", [t for t in closed_trades if t["reason"] == "SIGNAL"])
    print_breakdown("STOP_LOSS", [t for t in closed_trades if t["reason"] == "STOP_LOSS"])
    print_breakdown("TAKE_PROFIT", [t for t in closed_trades if t["reason"] == "TAKE_PROFIT"])
    print_breakdown("MAX_HOLD", [t for t in closed_trades if t["reason"] == "MAX_HOLD"])
    print_breakdown("END_OF_TEST", [t for t in closed_trades if t["reason"] == "END_OF_TEST"])
    print(f"  {'-' * 45}")
    print("  Entry Diagnostics:")
    print(f"  Eligible Rows:     {diagnostics['eligible_rows']}")
    print(f"  Trend OK:          {diagnostics['trend_ok']}")
    print(f"  MACD OK:           {diagnostics['macd_ok']}")
    print(f"  Signal Count OK:   {diagnostics['signal_count_ok']}")
    print(f"  Volume OK:         {diagnostics['volume_ok']}")
    print(f"  Timing OK:         {diagnostics['timing_ok']}")
    print(f"  Buy Ready:         {diagnostics['buy_ready']}")

    passed = (
        win_rate > 55
        and profit_factor > 1.3
        and max_dd < 0.08
        and total_return > 0
        and len(closed_trades) >= 5
    )
    print(f"  {'-' * 45}")
    print(f"  VERDICT: {'PASS' if passed else 'FAIL'}")
    print(f"{'=' * 55}\n")

    return {
        "symbol": symbol,
        "return": total_return,
        "trades": len(closed_trades),
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "max_drawdown": max_dd * 100,
        "final_balance": balance,
        "passed": passed,
    }


if __name__ == "__main__":
    print("\nStarting Crypto Orchestra Backtester")
    print("Using free Yahoo Finance data\n")

    results = []
    for symbol in SYMBOLS:
        result = run_backtest(symbol, timeframe="1h", days=365)
        if result:
            results.append(result)

    print("\nOVERALL SUMMARY")
    print("=" * 55)
    all_passed = all(r["passed"] for r in results) if results else False
    for r in results:
        status = "PASS" if r["passed"] else "FAIL"
        print(f"  {r['symbol']:12} {status}  Return: {r['return']:+.1f}%  WinRate: {r['win_rate']:.0f}%")

    print(f"\n  Overall: {'Strategy ready for paper trading' if all_passed else 'Needs refinement before paper trading'}")
    print("=" * 55)
