"""
Signal Scanner — scans historical periods for when the system WOULD have fired.

No Claude API calls. Uses the backtest engine + breakout logic to show exactly:
  - Which candles had valid BUY conditions (trend, MACD, RSI, volume, EMA crossover)
  - How the 4h trend filter and volume hard gate would have blocked false entries
  - What the P&L would have been if we entered at each signal

Answers: "Would the system have fired during the Trump rally?"

Usage:
    python backtesting/signal_scanner.py trump_rally
    python backtesting/signal_scanner.py trump_rally --asset BTC-USD
    python backtesting/signal_scanner.py aug_crash
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import yfinance as yf
from backtesting.backtest import (
    attach_higher_timeframe_context,
    STRATEGY_CONFIG,
)

# Coinbase Advanced Trade base tier (<$10K/month volume):
#   Maker (limit orders): 0.40%
#   Taker (market orders): 0.60%
_ENTRY_FEE = 0.004   # maker: limit order at support
_TP_FEE    = 0.004   # maker: limit order at target price
_SL_FEE    = 0.006   # taker: stop-market and max-hold exits

# Keep FEE_RATE alias so any code that imported it still works
FEE_RATE = _SL_FEE
from ta.momentum import RSIIndicator
from ta.trend import MACD, EMAIndicator, ADXIndicator
from ta.volatility import AverageTrueRange, BollingerBands

# ── Periods ───────────────────────────────────────────────────────────────────

PERIODS = {
    "trump_rally": {
        "label":  "Trump Rally  (Nov 5 2024 – Jan 20 2025)",
        "start":  "2024-11-05",
        "end":    "2025-01-20",
        "warmup": "2024-08-01",   # download from here for indicator warmup
        "btc_move": "+60%  ($68k -> $109k)",
    },
    "aug_crash": {
        "label":  "August 2024 Flash Crash  (Jul 20 – Sep 1 2024)",
        "start":  "2024-07-20",
        "end":    "2024-09-01",
        "warmup": "2024-07-12",   # yfinance 730-day rolling limit; update if too old
        "btc_move": "-32%  ($68k -> $49k crash and recovery)",
    },
    "q1_2025_bear": {
        "label":  "Q1 2025 Bear  (Jan 20 – Apr 1 2025)",
        "start":  "2025-01-20",
        "end":    "2025-04-01",
        "warmup": "2024-10-01",
        "btc_move": "-28%  ($109k -> $78k)",
    },
    "current": {
        "label":  "Current Bear  (Mar 1 – Jun 28 2025)",
        "start":  "2025-03-01",
        "end":    "2025-06-28",
        "warmup": "2024-12-01",
        "btc_move": "~-23%  ($90k -> $60k)",
    },
    "full_year": {
        "label":  "Full Year  (Aug 2024 – Jun 2025)",
        "start":  "2024-08-01",
        "end":    "2025-06-28",
        "warmup": "2024-07-12",   # yfinance 730-day rolling limit; update if too old
        "btc_move": "Multi-regime: crash -> +60% rally -> -28% bear",
    },
    "live_period": {
        "label":  "Live Period  (Jun 1 – present 2026)",
        "start":  "2026-06-01",
        "end":    "2026-07-09",
        "warmup": "2025-12-01",
        "btc_move": "Recovery rally ~2026",
    },
}

ASSETS = ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]

# ── Per-asset strategy configs ────────────────────────────────────────────────
# Each asset gets its own entry conditions. Tweak and re-run signal_scanner
# to test hypotheses without touching live code.
#
# Fields:
#   atr_stop / atr_target  — ATR multipliers for stop/target
#   min_conditions         — how many of 5 scored conditions must be met (3–5)
#   vol_spike_ratio        — volume spike threshold (above this = "volume confirmation")
#   daily_ema_period       — which daily EMA to use as trend gate: 50 or 200
#   enabled                — set False to exclude asset from the run

ASSET_CONFIG = {
    "BTC-USD": {
        "atr_stop":        2.0, "atr_target": 3.5,  # R:R = 1.75
        "min_conditions":  4,
        "vol_spike_ratio": 1.3,
        "daily_ema_period": 50,
        "enabled": False,  # hypothesis D — BTC consistently weakest, test without it
    },
    "ETH-USD": {
        "atr_stop":        2.5, "atr_target": 4.5,  # R:R = 1.80 — wick-heavy
        "min_conditions":  4,
        "vol_spike_ratio": 1.3,
        "daily_ema_period": 50,
        "btc_regime_filter": True,
        "enabled": False,  # 29% win rate in full-year — disabled until edge confirmed
    },
    "SOL-USD": {
        "atr_stop":        2.5, "atr_target": 4.5,  # R:R = 1.80
        "min_conditions":  4,
        "vol_spike_ratio": 1.3,
        "daily_ema_period": 200,
        "enabled": False,  # hypothesis C — SOL consistently weakest, excluded
    },
    "ZEC-USD": {
        "atr_stop":        2.0, "atr_target": 3.5,  # R:R = 1.75
        "min_conditions":  4,
        "vol_spike_ratio": 1.3,
        "daily_ema_period": 200,
        "btc_regime_filter": False,  # ZEC moves independently of BTC — filter blocks good setups
        "enabled": True,
    },
}

# Keep old ASSET_PARAMS alias so backtest.py imports still work
ASSET_PARAMS = {k: {"atr_stop": v["atr_stop"], "atr_target": v["atr_target"]}
                for k, v in ASSET_CONFIG.items()}

# Shared hard gates (not per-asset — these are structural, not tunable)
_MIN_VOL_RATIO     = 0.8
_WHIPSAW_MAX_STOPS = 2
_WHIPSAW_WINDOW_H  = 96   # matches runner.py _WHIPSAW_LOOKBACK_H (was 48 — bug fix)
_MIN_ADX           = 25.0
_MAX_RSI_AT_CROSS  = 65.0
_MAX_PCT_ABOVE_EMA = 4.0
_MAX_CANDLES_SINCE = 4


# ── Daily context helper ──────────────────────────────────────────────────────

def _attach_daily_context(signal_df: pd.DataFrame, daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Forward-fill daily EMA50 and EMA200 onto 1h data.
    Both columns are attached so per-asset configs can choose which to use.
    """
    daily_cols = daily_df[["time", "close", "ema50", "ema200"]].copy()
    daily_cols = daily_cols.rename(columns={
        "close":  "close_1d",
        "ema50":  "ema50_1d",
        "ema200": "ema200_1d",
    })
    # Shift daily timestamps forward by 1 day so that a day's close/EMA values
    # are only visible to 1h rows on the NEXT calendar day.
    # Without this, the July-9 daily close (which only forms at midnight July 10)
    # would be attached to all July-9 intraday candles — look-ahead bias.
    # Shift, then cast back to original dtype so merge_asof doesn't reject [s] vs [us].
    _t_dtype = daily_cols["time"].dtype
    daily_cols["time"] = (daily_cols["time"] + pd.Timedelta(days=1)).astype(_t_dtype)
    merged = pd.merge_asof(
        signal_df.sort_values("time"),
        daily_cols.sort_values("time"),
        on="time",
        direction="backward",
    )
    merged.index = signal_df.index
    return merged


# ── Signal detection ──────────────────────────────────────────────────────────

def _detect_breakout_signal(df: pd.DataFrame, i: int, cfg: dict) -> dict | None:
    """
    Check if row `i` in `df` represents a valid breakout BUY signal.
    Mirrors the logic in agents/breakout_agent.py exactly.
    cfg is the asset's ASSET_CONFIG entry — drives per-asset thresholds.
    Returns None if no signal, or a dict with signal details.
    """
    if i < 12:
        return None

    close_arr = df["close"].values
    ema50_arr = df["ema50"].values

    # Count consecutive candles above EMA50 (look back up to row i)
    candles_above      = 0
    crossed_from_below = False
    look_back          = min(12, i + 1)

    for j in range(i, i - look_back, -1):
        if close_arr[j] > ema50_arr[j]:
            candles_above += 1
        else:
            if candles_above > 0:
                crossed_from_below = True
            break

    if not crossed_from_below or candles_above == 0 or candles_above > _MAX_CANDLES_SINCE:
        return None

    row       = df.iloc[i]
    cross_row = df.iloc[i - candles_above + 1] if i >= candles_above else df.iloc[0]

    def _safe(col):
        try:
            v = float(row[col]) if col in row.index else None
            return None if v is None or (v != v) else v  # NaN check
        except Exception:
            return None

    vol_ratio    = _safe("volume_ratio") or 1.0
    adx_now      = _safe("adx") or 0.0
    close_now    = _safe("close") or 0.0
    ema50_now    = _safe("ema50") or 1.0
    close_4h     = _safe("close_4h")
    ema50_4h     = _safe("ema50_4h")
    cvd_24h      = _safe("cvd_24h") or 0.0
    pct_above    = (close_now - ema50_now) / ema50_now * 100

    rsi_at_cross = float(cross_row["rsi"]) if "rsi" in cross_row.index else 50.0

    # Per-asset thresholds from config
    vol_spike   = cfg.get("vol_spike_ratio", 1.3)
    min_cond    = cfg.get("min_conditions",  3)
    daily_period = cfg.get("daily_ema_period", 200)

    # Hard gates
    if vol_ratio < _MIN_VOL_RATIO:
        return {"blocked": "vol_gate", "vol_ratio": round(vol_ratio, 2)}

    if close_4h is not None and ema50_4h is not None and close_4h < ema50_4h:
        return {"blocked": "4h_trend", "close_4h": round(close_4h, 2), "ema50_4h": round(ema50_4h, 2)}

    # BTC macro regime — block long entries when BTC is below its daily EMA50
    if cfg.get("btc_regime_filter", False):
        btc_close = _safe("btc_close_1d")
        btc_ema50 = _safe("btc_ema50_1d")
        if btc_close is not None and btc_ema50 is not None and btc_close < btc_ema50:
            return {"blocked": "btc_regime"}

    close_1d   = _safe("close_1d")
    daily_ema  = _safe(f"ema{daily_period}_1d")
    if close_1d is not None and daily_ema is not None and close_1d < daily_ema:
        return {"blocked": "daily_trend", "close_1d": round(close_1d, 2),
                f"ema{daily_period}_1d": round(daily_ema, 2)}

    # Scored conditions
    conditions = [
        rsi_at_cross < _MAX_RSI_AT_CROSS,
        adx_now >= _MIN_ADX,
        vol_ratio >= vol_spike,
        cvd_24h > 0,
        pct_above < _MAX_PCT_ABOVE_EMA,
    ]
    n_met = sum(conditions)

    if n_met < min_cond:
        return {"blocked": "conditions", "n_met": n_met}

    confidence = min(0.57 + 0.08 * n_met, 0.89)

    return {
        "signal":       "BUY",
        "candles_above": candles_above,
        "n_conditions": n_met,
        "confidence":   round(confidence, 2),
        "rsi_at_cross": round(rsi_at_cross, 1),
        "adx":          round(adx_now, 1),
        "vol_ratio":    round(vol_ratio, 2),
        "pct_above":    round(pct_above, 2),
        "close_4h":     round(close_4h, 2) if close_4h else None,
        "ema50_4h":     round(ema50_4h, 2) if ema50_4h else None,
        "blocked":      None,
    }


def _simulate_trade(df: pd.DataFrame, entry_i: int, entry_price: float,
                    max_hold_hours: int, atr_stop: float, atr_target: float) -> dict:
    """
    Simulate what would have happened if we entered at entry_i.
    Uses ATR-based stop/target and max_hold.
    """
    atr          = float(df.iloc[entry_i]["atr"])
    stop_price   = round(entry_price - atr_stop * atr, 2)
    target_price = round(entry_price + atr_target * atr, 2)

    for j in range(entry_i + 1, min(entry_i + max_hold_hours + 1, len(df))):
        row = df.iloc[j]
        low  = float(row["low"])
        high = float(row["high"])

        if low <= stop_price:
            gross   = stop_price * (1 - _SL_FEE)
            net_pnl = (gross - entry_price * (1 + _ENTRY_FEE)) / entry_price * 100
            return {"reason": "STOP_LOSS", "exit_price": stop_price,
                    "hold_h": j - entry_i, "pnl_pct": round(net_pnl, 2)}

        if high >= target_price:
            gross   = target_price * (1 - _TP_FEE)
            net_pnl = (gross - entry_price * (1 + _ENTRY_FEE)) / entry_price * 100
            return {"reason": "TAKE_PROFIT", "exit_price": target_price,
                    "hold_h": j - entry_i, "pnl_pct": round(net_pnl, 2)}

    # Max hold — market close, taker fee
    exit_price = float(df.iloc[min(entry_i + max_hold_hours, len(df) - 1)]["close"])
    gross      = exit_price * (1 - _SL_FEE)
    net_pnl    = (gross - entry_price * (1 + _ENTRY_FEE)) / entry_price * 100
    return {"reason": "MAX_HOLD", "exit_price": round(exit_price, 2),
            "hold_h": max_hold_hours, "pnl_pct": round(net_pnl, 2)}


# ── Per-asset scanner ─────────────────────────────────────────────────────────

def _download_and_compute(asset: str, start: str, end: str, interval: str) -> pd.DataFrame | None:
    """Download OHLCV from yfinance and attach all technical indicators."""
    try:
        raw = yf.download(asset, start=start, end=end, interval=interval,
                          progress=False, auto_adjust=True)
        if raw is None or raw.empty:
            return None
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)
        raw.columns = [c.lower() for c in raw.columns]
        raw.index   = pd.to_datetime(raw.index, utc=True)
        raw         = raw.dropna(subset=["close", "open", "high", "low", "volume"])
        if len(raw) < 20:
            return None

        df = raw.copy()
        c  = df["close"]

        # RSI
        df["rsi"] = RSIIndicator(c, window=14).rsi()

        # MACD
        m = MACD(c, window_slow=26, window_fast=12, window_sign=9)
        df["macd"]      = m.macd()
        df["macd_signal"] = m.macd_signal()
        df["macd_diff"] = m.macd_diff()

        # EMA
        df["ema50"]  = EMAIndicator(c, window=50).ema_indicator()
        df["ema200"] = EMAIndicator(c, window=200).ema_indicator()

        # ATR
        df["atr"] = AverageTrueRange(df["high"], df["low"], c, window=14).average_true_range()

        # Bollinger
        bb = BollingerBands(c, window=20, window_dev=2)
        df["bb_pct"] = bb.bollinger_pband()

        # Volume ratio (20-period rolling mean)
        df["vol_ma"] = df["volume"].rolling(20).mean()
        df["volume_ratio"] = df["volume"] / df["vol_ma"]

        # ADX
        df["adx"] = ADXIndicator(df["high"], df["low"], c, window=14).adx()

        # CVD proxy (sum of signed volume over 24 candles)
        df["signed_vol"] = df["volume"] * np.where(c > c.shift(1), 1, -1)
        periods_24h = 24 if interval == "1h" else 6
        df["cvd_24h"] = df["signed_vol"].rolling(periods_24h).sum()

        # VWAP (rolling daily reset approximation)
        df["vwap"] = (df["volume"] * (df["high"] + df["low"] + c) / 3).cumsum() / df["volume"].cumsum()

        # EWMA volatility
        df["ret"] = c.pct_change()
        df["ewma_vol"] = df["ret"].ewm(span=20).std() * np.sqrt(24)

        # Required by attach_higher_timeframe_context
        df["time"]  = df.index
        df["trend"] = np.where(df["ema50"] > df["ema200"], "bull", "bear")

        return df.dropna(subset=["rsi", "ema50", "atr"])
    except Exception as exc:
        print(f"    indicator error: {exc}")
        return None


def scan_asset(asset: str, period: dict) -> dict:
    print(f"\n  Downloading {asset} data (warmup from {period['warmup']})...")

    sig_df   = _download_and_compute(asset, period["warmup"], period["end"], "1h")
    trend_df = _download_and_compute(asset, period["warmup"], period["end"], "4h")
    # Daily data: extend warmup far back so EMA200 (200 trading days ≈ 10 months) is valid.
    # yfinance daily has no 730-day limit, so "2022-01-01" is safe for any period.
    daily_df = _download_and_compute(asset, "2022-01-01", period["end"], "1d")

    if sig_df is None or trend_df is None:
        print(f"  {asset}: no data")
        return {}

    df       = attach_higher_timeframe_context(sig_df, trend_df)
    if daily_df is not None:
        df = _attach_daily_context(df, daily_df)
    # merge_asof returns integer index — restore DatetimeIndex from the "time" column
    if "time" in df.columns:
        df.index = pd.to_datetime(df["time"], utc=True)

    config     = STRATEGY_CONFIG.get(asset, STRATEGY_CONFIG["ETH-USD"])
    max_hold   = config.get("max_hold_hours", 36)
    asset_cfg  = ASSET_CONFIG.get(asset, ASSET_CONFIG["ZEC-USD"])
    atr_stop   = asset_cfg["atr_stop"]
    atr_target = asset_cfg["atr_target"]

    # BTC regime filter — attach BTC daily close vs EMA50 for each 1h bar
    # Must come after asset_cfg is resolved (needs btc_regime_filter flag).
    if asset != "BTC-USD" and asset_cfg.get("btc_regime_filter", False):
        btc_daily = _download_and_compute("BTC-USD", "2022-01-01", period["end"], "1d")
        if btc_daily is not None:
            btc_regime_cols = btc_daily[["time", "close", "ema50"]].copy()
            btc_regime_cols = btc_regime_cols.rename(
                columns={"close": "btc_close_1d", "ema50": "btc_ema50_1d"}
            )
            _bt_dtype = btc_regime_cols["time"].dtype
            btc_regime_cols["time"] = (
                btc_regime_cols["time"] + pd.Timedelta(days=1)
            ).astype(_bt_dtype)
            # reset_index so "time" is unambiguously a column (not also the index)
            df = pd.merge_asof(
                df.reset_index(drop=True).sort_values("time"),
                btc_regime_cols.sort_values("time"),
                on="time",
                direction="backward",
            )
            df.index = pd.to_datetime(df["time"], utc=True)

    # Slice to the actual replay window (after warmup)
    start_ts  = pd.Timestamp(period["start"], tz="UTC")
    df_period = df[df.index >= start_ts].copy()

    if df_period.empty:
        print(f"  {asset}: no data in period window")
        return {}

    print(f"  {asset}: {len(df_period)} hourly candles in period")

    signals          = []
    blocked_vol      = 0
    blocked_4h       = 0
    blocked_daily    = 0
    blocked_btc      = 0
    blocked_cond     = 0
    blocked_whipsaw  = 0
    skip_until       = -1   # don't double-enter
    recent_stop_ts: list[pd.Timestamp] = []  # timestamps of recent stop losses

    # We need to operate on the full df (for lookback), but filter output to period
    full_len   = len(df)
    period_start_idx = df.index.get_loc(df_period.index[0]) if not df_period.empty else 0

    for i in range(period_start_idx, full_len):
        if i < skip_until:
            continue

        result = _detect_breakout_signal(df, i, asset_cfg)
        if result is None:
            continue

        ts    = df.index[i]
        price = float(df.iloc[i]["close"])

        if result.get("blocked") == "vol_gate":
            blocked_vol += 1
            continue
        elif result.get("blocked") == "4h_trend":
            blocked_4h += 1
            continue
        elif result.get("blocked") == "daily_trend":
            blocked_daily += 1
            continue
        elif result.get("blocked") == "btc_regime":
            blocked_btc += 1
            continue
        elif result.get("blocked") == "conditions":
            blocked_cond += 1
            continue

        # Whipsaw guard — block if 2+ stops hit in the last 48h (rapid-fire losses = choppy market)
        cutoff = ts - pd.Timedelta(hours=_WHIPSAW_WINDOW_H)
        recent_stop_ts = [t for t in recent_stop_ts if t >= cutoff]
        if len(recent_stop_ts) >= _WHIPSAW_MAX_STOPS:
            blocked_whipsaw += 1
            continue

        # Valid BUY signal — simulate the trade
        trade = _simulate_trade(df, i, price, max_hold, atr_stop, atr_target)
        if trade["reason"] == "STOP_LOSS":
            recent_stop_ts.append(ts)

        record = {
            "timestamp":    ts.strftime("%Y-%m-%d %H:%M"),
            "price":        round(price, 2),
            "signal":       result,
            "trade":        trade,
        }
        signals.append(record)
        skip_until = i + trade["hold_h"] + 1   # don't re-enter mid-hold

    return {
        "asset":            asset,
        "candles":          len(df_period),
        "signals":          signals,
        "blocked_vol":      blocked_vol,
        "blocked_4h":       blocked_4h,
        "blocked_daily":    blocked_daily,
        "blocked_btc":      blocked_btc,
        "blocked_cond":     blocked_cond,
        "blocked_whipsaw":  blocked_whipsaw,
        "atr_stop":         atr_stop,
        "atr_target":       atr_target,
    }


# ── Live signal gate ─────────────────────────────────────────────────────────

def scan_latest(asset: str) -> dict | None:
    """
    Check if the breakout signal fires on the last closed candle for this asset.
    Used by runner.py as the primary live entry gate — replaces the AI composite
    score threshold. Returns a signal dict or None (no signal / blocked).

    Downloads fresh data on every call (no cache) so the live runner always
    has the most recent candle. Intentionally skips the last row (current
    incomplete candle) and evaluates only fully-closed candles.
    """
    cfg = ASSET_CONFIG.get(asset)
    if cfg is None or not cfg.get("enabled", True):
        return None

    from datetime import date, timedelta
    today    = date.today().isoformat()
    warmup   = (date.today() - timedelta(days=45)).isoformat()

    sig_df   = _download_and_compute(asset, warmup,        today, "1h")
    trend_df = _download_and_compute(asset, warmup,        today, "4h")
    daily_df = _download_and_compute(asset, "2022-01-01",  today, "1d")

    if sig_df is None or trend_df is None or len(sig_df) < 50:
        return None

    df = attach_higher_timeframe_context(sig_df, trend_df)
    if daily_df is not None:
        df = _attach_daily_context(df, daily_df)
    if "time" in df.columns:
        df.index = pd.to_datetime(df["time"], utc=True)

    # BTC regime filter — attach BTC daily EMA50 so _detect_breakout_signal can check
    if asset != "BTC-USD" and cfg.get("btc_regime_filter", False):
        btc_daily = _download_and_compute("BTC-USD", "2022-01-01", today, "1d")
        if btc_daily is not None:
            btc_regime_cols = btc_daily[["time", "close", "ema50"]].copy()
            btc_regime_cols = btc_regime_cols.rename(
                columns={"close": "btc_close_1d", "ema50": "btc_ema50_1d"}
            )
            _bt_dtype = btc_regime_cols["time"].dtype
            btc_regime_cols["time"] = (
                btc_regime_cols["time"] + pd.Timedelta(days=1)
            ).astype(_bt_dtype)
            df = pd.merge_asof(
                df.reset_index(drop=True).sort_values("time"),
                btc_regime_cols.sort_values("time"),
                on="time",
                direction="backward",
            )
            df.index = pd.to_datetime(df["time"], utc=True)

    # n-1 is the current incomplete candle — skip it; evaluate n-2 (last closed)
    i = len(df) - 2
    if i < 12:
        return None

    result = _detect_breakout_signal(df, i, cfg)
    if result is None or result.get("blocked"):
        return None

    ts = df.index[i]
    return {
        "asset":       asset,
        "entry_time":  str(ts),
        "entry_price": float(df.iloc[i]["close"]),
        "conf":        result["confidence"],
        "n_conditions": result["n_conditions"],
        "candles_above": result["candles_above"],
        "adx":         result["adx"],
        "vol_ratio":   result["vol_ratio"],
    }


# ── Reporting ─────────────────────────────────────────────────────────────────

def print_report(period_key: str, period: dict, all_results: dict) -> None:
    print("\n" + "=" * 70)
    print(f"SIGNAL SCANNER — {period['label']}")
    print(f"BTC reference: {period['btc_move']}")
    print("=" * 70)

    total_signals = sum(len(r.get("signals", [])) for r in all_results.values())
    total_wins    = sum(1 for r in all_results.values()
                        for s in r.get("signals", [])
                        if s["trade"]["pnl_pct"] > 0)
    total_pnl     = sum(s["trade"]["pnl_pct"]
                        for r in all_results.values()
                        for s in r.get("signals", []))

    print("\nSummary across all assets:")
    print(f"  Total BUY signals:    {total_signals}")
    if total_signals:
        print(f"  Win rate:             {total_wins/total_signals:.1%}")
        print(f"  Avg P&L per trade:    {total_pnl/total_signals:+.2f}% of position")
        print(f"  Total P&L (sum):      {total_pnl:+.2f}%")

    for asset, r in all_results.items():
        if not r:
            continue
        sigs = r.get("signals", [])
        print(f"\n{'-'*70}")
        bw         = r.get("blocked_whipsaw", 0)
        atr_stop   = r.get("atr_stop", 2.0)
        atr_target = r.get("atr_target", 3.5)
        rr         = atr_target / atr_stop
        bd  = r.get("blocked_daily", 0)
        bb  = r.get("blocked_btc", 0)
        btc_str = f"  btc={bb}" if bb else ""
        print(f"  {asset}  ({len(sigs)} signals  |  "
              f"stop={atr_stop}x  target={atr_target}x  R:R={rr:.2f}  |  "
              f"blocked: vol={r['blocked_vol']}  4h={r['blocked_4h']}  "
              f"daily={bd}{btc_str}  cond={r['blocked_cond']}  whipsaw={bw})")

        if not sigs:
            print("    No signals fired in this period.")
            continue

        wins   = [s for s in sigs if s["trade"]["pnl_pct"] > 0]
        [s for s in sigs if s["trade"]["pnl_pct"] <= 0]
        avg_pnl = sum(s["trade"]["pnl_pct"] for s in sigs) / len(sigs)

        print(f"  Win rate: {len(wins)}/{len(sigs)} = {len(wins)/len(sigs):.1%}    "
              f"Avg P&L: {avg_pnl:+.2f}%")

        print(f"\n  {'Timestamp':<18} {'Price':>10}  {'Signal':>7}  "
              f"{'Conf':>5}  {'Exit':>12}  {'Hold':>5}  {'P&L':>8}")
        print(f"  {'-'*18} {'-'*10}  {'-'*7}  {'-'*5}  {'-'*12}  {'-'*5}  {'-'*8}")

        for s in sigs:
            sig  = s["signal"]
            tr   = s["trade"]
            pnl  = tr["pnl_pct"]
            sign = "+" if pnl >= 0 else ""
            icon = "WIN " if pnl > 0 else "LOSS"
            print(f"  {s['timestamp']:<18} ${s['price']:>9,.2f}  {icon:>7}  "
                  f"{sig['confidence']:>4.0%}  "
                  f"{tr['reason']:>12}  {tr['hold_h']:>4}h  "
                  f"{sign}{pnl:>6.2f}%")

    print("\n" + "=" * 70)
    print("KEY FINDINGS:")
    if total_signals == 0:
        print("  System correctly stayed in HOLD throughout this period.")
        print("  No false breakout entries — all filters working correctly.")
    else:
        wr = total_wins / total_signals
        if wr >= 0.55:
            print(f"  POSITIVE: {wr:.0%} win rate — system edges positive in this regime.")
        elif wr >= 0.40:
            print(f"  MIXED: {wr:.0%} win rate — marginal edge, fee drag matters.")
        else:
            print(f"  CAUTION: {wr:.0%} win rate — system struggles in this regime.")

        total_blk = sum(
            r.get("blocked_4h", 0) + r.get("blocked_vol", 0)
            + r.get("blocked_daily", 0) + r.get("blocked_whipsaw", 0)
            for r in all_results.values()
        )
        daily_blk   = sum(r.get("blocked_daily", 0) for r in all_results.values())
        whipsaw_blk = sum(r.get("blocked_whipsaw", 0) for r in all_results.values())
        if total_blk > 0:
            extras = []
            if daily_blk:   extras.append(f"{daily_blk} by daily 200MA")
            if whipsaw_blk: extras.append(f"{whipsaw_blk} by whipsaw guard")
            estr = f" ({', '.join(extras)})" if extras else ""
            print(f"  Filters blocked {total_blk} false signals{estr}.")

    print("=" * 70)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] in ("--help", "-h"):
        print("Usage: python backtesting/signal_scanner.py <period> [--asset ASSET]")
        print(f"Periods: {list(PERIODS.keys())}")
        sys.exit(0)

    period_key = sys.argv[1]
    if period_key not in PERIODS:
        print(f"Unknown period '{period_key}'. Available: {list(PERIODS.keys())}")
        sys.exit(1)

    asset_arg = next((sys.argv[i + 1] for i, a in enumerate(sys.argv)
                      if a == "--asset" and i + 1 < len(sys.argv)), None)
    # Respect enabled flag in ASSET_CONFIG; --asset overrides it
    enabled_assets = [a for a in ASSETS if ASSET_CONFIG.get(a, {}).get("enabled", True)]
    assets    = [asset_arg] if asset_arg else enabled_assets
    period    = PERIODS[period_key]

    print(f"\nSIGNAL SCANNER — {period['label']}")
    print(f"Assets: {assets}  |  No Claude API calls needed.\n")

    all_results = {}
    for asset in assets:
        result = scan_asset(asset, period)
        if result:
            all_results[asset] = result

    print_report(period_key, period, all_results)


if __name__ == "__main__":
    main()
