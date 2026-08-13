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
    "recent_year": {
        "label":  "Recent Year  (Jul 2025 – Jul 2026)",
        "start":  "2025-07-15",
        "end":    "2026-07-12",
        "warmup": "2025-04-01",
        "btc_move": "Recovery $80k -> $100k+, 2026 rally",
    },
    # 2021 bull cycle — independent of all tuning; tests general momentum mechanism.
    # ZEC went from ~$60 to ~$280 (Mar-Oct 2021). Coinbase data available from Dec 2020.
    "bull_2021": {
        "label":  "2021 Bull Run  (Mar – Nov 2021)",
        "start":  "2021-03-01",
        "end":    "2021-11-10",
        "warmup": "2020-12-08",
        "btc_move": "BTC $30k -> $68k (Mar-Nov 2021), ZEC $60 -> $280",
    },
    # 2022 bear / crypto winter — tests how badly the strategy bleeds in extended bear.
    "bear_2022": {
        "label":  "Crypto Winter  (Jan – Dec 2022)",
        "start":  "2022-01-01",
        "end":    "2022-12-31",
        "warmup": "2021-10-01",
        "btc_move": "BTC $47k -> $16k (-66%), ZEC $150 -> $28",
    },
    # Retrospective holdout — NOT used in any ADX/parameter tuning.
    # ADX=25 was selected using live_period (Jun-Jul 2026) only.
    # This period sits between full_year and recent_year and was never examined.
    "mid_year_holdout": {
        "label":  "Mid-Year Holdout  (Aug 2024 – May 2025)",
        "start":  "2024-08-01",
        "end":    "2025-05-31",
        "warmup": "2024-07-14",   # earliest available from yfinance 730-day window
        "btc_move": "Aug crash -32% -> Trump rally +60% -> Q1 bear -28%",
    },
}

ASSETS = ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD",
          "LINK-USD", "ATOM-USD", "AVAX-USD", "DOT-USD"]

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
        # V3 regime filter — ER-30 (Kaufman Efficiency Ratio, 30-day window).
        # Integrated filter analysis across 4 historical regimes (4.5 years):
        #   No filter: PFs=[1.42, 1.41, 0.52, 1.00], avg=-0.08%/trade
        #   er>=0.20:  PFs=[1.41, 1.03, 1.00, 1.11], avg=+0.31%/trade (best stable threshold)
        #   er>=0.25:  PFs=[0.77, 5.32, 0.77, 1.16]  (small sample instability)
        #
        # v3_candidate_threshold — pre-registered (2026-07-13), LOCKED at 0.20.
        #   Always computed for journaling; never changed on OOS data.
        # v3_enforcement_enabled — False = shadow/research only (log v3_would_block but
        #   don't block trades).
        #   V3 ER-30 is RETIRED as an activation candidate (2026-08-09): integrated
        #   enforcement made the continuous window worse (PF 0.69 vs 0.86 without).
        #   This stays False; the former activation criteria are withdrawn. Turning it
        #   on requires a NEW pre-registered trial ID — see docs/trial_registry.md.
        #   v3_candidate_threshold below is retained as historical trial metadata only.
        "v3_candidate_threshold": 0.20,
        "v3_enforcement_enabled": False,
        "enabled": True,
    },
    # ── Candidate assets for portfolio expansion ─────────────────────────────
    # All enabled=False — backtest-only until edge is confirmed.
    "LINK-USD": {
        "atr_stop": 2.0, "atr_target": 3.5,
        "min_conditions": 4,
        "vol_spike_ratio": 1.3,
        "daily_ema_period": 50,
        "btc_regime_filter": True,
        "enabled": False,
    },
    "ATOM-USD": {
        "atr_stop": 2.0, "atr_target": 3.5,
        "min_conditions": 4,
        "vol_spike_ratio": 1.3,
        "daily_ema_period": 50,
        "btc_regime_filter": True,
        "enabled": False,
    },
    "AVAX-USD": {
        "atr_stop": 2.0, "atr_target": 3.5,
        "min_conditions": 4,
        "vol_spike_ratio": 1.3,
        "daily_ema_period": 50,
        "btc_regime_filter": True,
        "enabled": False,
    },
    "DOT-USD": {
        "atr_stop": 2.0, "atr_target": 3.5,
        "min_conditions": 4,
        "vol_spike_ratio": 1.3,
        "daily_ema_period": 50,
        "btc_regime_filter": True,
        "enabled": False,
    },
}

# Keep old ASSET_PARAMS alias so backtest.py imports still work
ASSET_PARAMS = {k: {"atr_stop": v["atr_stop"], "atr_target": v["atr_target"]}
                for k, v in ASSET_CONFIG.items()}

# How far back daily frames are pulled, for the asset itself and for the BTC
# regime column alike. Both must use the same start, or the BTC gate becomes
# evaluable over a different span than the daily trend gate.
_DAILY_HISTORY_START = "2020-01-01"

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

def _cell(source: pd.Series, col: str) -> float | None:
    """
    Read one indicator value, or None when it is genuinely unavailable.

    Returns None for an absent column and for NaN alike — the caller must not be
    able to tell "the merge produced no value" from "the indicator has not warmed
    up", because both mean the same thing: this gate cannot be evaluated here.

    Deliberately returns None rather than a neutral default. The predecessor
    idiom `_safe(col) or 1.0` also swallowed a legitimate 0.0 (a zero-volume hour
    became volume_ratio 1.0 and cleared the 0.8 volume gate), so the fallback was
    not merely unsound on NaN.
    """
    try:
        if col not in source.index:
            return None
        v = float(source[col])
    except (TypeError, ValueError):
        return None
    return None if v != v else v      # NaN


def gate_input_columns(cfg: dict, *, btc_regime_applicable: bool) -> dict[str, list[str]]:
    """
    The merged-frame columns each DECLARED gate needs, for this config.

    "Declared" is per-asset: a gate the effective config switches off is not
    applicable and its absence is not a defect. Only an APPLICABLE gate whose
    input is missing refuses a signal.
    """
    cols = {
        "vol_gate":    ["volume_ratio"],
        "trend_4h":    ["close_4h", "ema50_4h"],
        "daily_trend": ["close_1d", f"ema{cfg.get('daily_ema_period', 200)}_1d"],
        "scored_conditions": ["rsi", "adx", "volume_ratio", "cvd_24h",
                              "close", "ema50"],
    }
    if btc_regime_applicable:
        cols["btc_regime"] = ["btc_close_1d", "btc_ema50_1d"]
    return cols


def build_merged_frame(asset: str, warmup: str, end: str, asset_cfg: dict, *,
                       btc_regime_applicable: bool
                       ) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """
    Assemble the 1h frame with 4h and daily context merged on, exactly as the
    scanner evaluates it. Extracted so that gate availability is measured on the
    same frame the gates are read from — measuring it anywhere else answers a
    different question.

    Returns (merged_1h, daily) — the daily frame is handed back because the
    regime metrics are computed from it directly, not from the merged columns.
    """
    sig_df   = _download_and_compute(asset, warmup, end, "1h")
    trend_df = _download_and_compute(asset, warmup, end, "4h")
    # Daily data reaches back to _DAILY_HISTORY_START, but only as far as the
    # asset's listing date, so for a recently listed asset the daily EMA is still
    # NaN for its first ~200 days. That is precisely why the gates fail closed
    # and why a registered run declares an effective_start.
    daily_df = _download_and_compute(asset, _DAILY_HISTORY_START, end, "1d")
    if sig_df is None or trend_df is None:
        return None, daily_df

    df = attach_higher_timeframe_context(sig_df, trend_df)
    if daily_df is not None:
        df = _attach_daily_context(df, daily_df)
    # merge_asof returns integer index — restore DatetimeIndex from the "time" column
    if "time" in df.columns:
        df.index = pd.to_datetime(df["time"], utc=True)

    if btc_regime_applicable:
        # Same daily history start as the asset's own daily frame. This used to
        # be hardcoded to "2022-01-01", which left the column NaN for every
        # pre-2022 bar. Under the old fail-open rule that silently dropped the
        # gate; under fail-closed it would refuse every pre-2022 signal instead.
        # Neither is the declared mechanism — so make the input actually cover
        # the window rather than choosing which wrong answer to give.
        btc_daily = _download_and_compute("BTC-USD", _DAILY_HISTORY_START, end, "1d")
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
    return df, daily_df


def asset_gate_availability(asset: str, asset_cfg: dict, end: str) -> dict:
    """
    Canonical, warmup-independent gate availability for one asset+mechanism.

    Built over the asset's FULL history, so the answer does not move when a
    caller happens to pass a later warmup. This is the value a research run
    registers and drift-checks; measuring it off a short-warmup frame would make
    the declared boundary a function of the caller.
    """
    btc_applicable = (asset != "BTC-USD"
                      and bool(asset_cfg.get("btc_regime_filter", False)))
    df, _ = build_merged_frame(asset, _DAILY_HISTORY_START, end, asset_cfg,
                               btc_regime_applicable=btc_applicable)
    if df is None:
        raise StrictSourceError(f"cannot build merged frame for {asset}")
    return gate_availability(df, asset_cfg, btc_regime_applicable=btc_applicable)


def gate_availability(df: pd.DataFrame, cfg: dict, *,
                      btc_regime_applicable: bool) -> dict:
    """
    First timestamp on the MERGED 1h frame at which each declared gate becomes
    evaluable, plus the effective start for the whole mechanism.

    This is the boundary that must be REGISTERED for a research run. Recomputing
    it silently per run would let the evaluation window drift with the data
    cache; the runner declares it and compares.
    """
    per_gate: dict[str, str | None] = {}
    for gate, cols in sorted(gate_input_columns(
            cfg, btc_regime_applicable=btc_regime_applicable).items()):
        missing_col = [c for c in cols if c not in df.columns]
        if missing_col:
            per_gate[gate] = None            # never evaluable on this frame
            continue
        ok = df[cols].notna().all(axis=1)
        per_gate[gate] = (df.index[ok.values.argmax()].isoformat()
                          if bool(ok.any()) else None)

    firsts = [v for v in per_gate.values()]
    effective = (None if any(v is None for v in firsts) or not firsts
                 else max(firsts))
    return {"per_gate": per_gate, "effective_start": effective}


def _detect_breakout_signal(df: pd.DataFrame, i: int, cfg: dict, *,
                            btc_regime_applicable: bool | None = None) -> dict | None:
    """
    Check if row `i` in `df` represents a valid breakout BUY signal.
    Mirrors the logic in agents/breakout_agent.py exactly.
    cfg is the asset's ASSET_CONFIG entry — drives per-asset thresholds.

    Three outcomes, deliberately distinguishable by the caller:

      None                                   — NO SIGNAL (no EMA50 cross here).
      {"blocked": "gate_inputs_unavailable"} — a signal formed, but at least one
                                               input a DECLARED gate needs is not
                                               computable on this bar. Refused.
      {"blocked": <gate>}                    — a declared gate actually rejected it.
      {"signal": "BUY", ...}                 — accepted.

    `gate_inputs_unavailable` is the fix for trial 2026-08-warmup-semantics. This
    function used to fail OPEN on a missing indicator: `_safe(...) or 1.0` fed a
    hard gate a synthetic neutral value, and `x is not None and x < y` skipped the
    gate outright when x was NaN. Both meant that while an indicator was still
    warming up the bar was evaluated by a WEAKER mechanism than the config
    declares — invisibly, because nothing counted it. On the ZEC continuous
    window that admitted 19 trades worth +22.28% before the 200-day daily EMA
    existed, which is where the whole apparent bull_2021 edge came from. A gate
    that cannot be evaluated must refuse the signal, never wave it through.

    btc_regime_applicable:
      None  — derive from `cfg["btc_regime_filter"]`.
      bool  — explicit. Callers pass False for BTC-USD itself, whose own daily
              EMA is never attached as a separate BTC-regime column. This keeps
              "gate not applicable to this asset" distinct from "gate applicable
              but its input is missing"; only the latter refuses a signal.
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

    # Per-asset thresholds from config
    vol_spike    = cfg.get("vol_spike_ratio", 1.3)
    min_cond     = cfg.get("min_conditions",  3)
    daily_period = cfg.get("daily_ema_period", 200)
    if btc_regime_applicable is None:
        btc_regime_applicable = bool(cfg.get("btc_regime_filter", False))

    # ── Required-input contract ───────────────────────────────────────────────
    # Every name below is consumed either by a hard gate or by the scored
    # conditions, so every one of them is result-determining. `notna` is checked
    # on the MERGED 1h row, not on the source frame: `_attach_daily_context`
    # shifts daily stamps +1d and `attach_higher_timeframe_context` shifts 4h
    # stamps +4h (both anti-look-ahead), so availability on the 1h grid lags the
    # source frame. A bar-count heuristic on the daily frame answers a different
    # question — and `regime["ema200_valid"]` is hardcoded to 200 regardless of
    # the configured `daily_ema_period`, so it cannot serve as this check either.
    required = {
        "close":        _cell(row, "close"),
        "ema50":        _cell(row, "ema50"),
        "volume_ratio": _cell(row, "volume_ratio"),
        "adx":          _cell(row, "adx"),
        "cvd_24h":      _cell(row, "cvd_24h"),
        "close_4h":     _cell(row, "close_4h"),
        "ema50_4h":     _cell(row, "ema50_4h"),
        "close_1d":     _cell(row, "close_1d"),
        f"ema{daily_period}_1d": _cell(row, f"ema{daily_period}_1d"),
        # Read off the cross bar, not the signal bar — a different row, so it
        # needs its own availability check.
        "rsi@cross":    _cell(cross_row, "rsi"),
    }
    if btc_regime_applicable:
        required["btc_close_1d"] = _cell(row, "btc_close_1d")
        required["btc_ema50_1d"] = _cell(row, "btc_ema50_1d")

    missing = sorted(k for k, v in required.items() if v is None)
    if missing:
        return {"blocked": "gate_inputs_unavailable", "missing": missing}

    vol_ratio    = required["volume_ratio"]
    adx_now      = required["adx"]
    close_now    = required["close"]
    ema50_now    = required["ema50"]
    close_4h     = required["close_4h"]
    ema50_4h     = required["ema50_4h"]
    cvd_24h      = required["cvd_24h"]
    close_1d     = required["close_1d"]
    daily_ema    = required[f"ema{daily_period}_1d"]
    rsi_at_cross = required["rsi@cross"]
    pct_above    = (close_now - ema50_now) / ema50_now * 100

    # Hard gates. Every operand is known to be present, so each comparison is a
    # real decision — none of them can be skipped by a missing value any more.
    if vol_ratio < _MIN_VOL_RATIO:
        return {"blocked": "vol_gate", "vol_ratio": round(vol_ratio, 2)}

    if close_4h < ema50_4h:
        return {"blocked": "4h_trend", "close_4h": round(close_4h, 2), "ema50_4h": round(ema50_4h, 2)}

    # BTC macro regime — block long entries when BTC is below its daily EMA50
    if btc_regime_applicable and required["btc_close_1d"] < required["btc_ema50_1d"]:
        return {"blocked": "btc_regime"}

    if close_1d < daily_ema:
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
        "close_4h":     round(close_4h, 2),
        "ema50_4h":     round(ema50_4h, 2),
        "blocked":      None,
    }


def _simulate_trade(df: pd.DataFrame, entry_i: int, entry_price: float,
                    max_hold_hours: int, atr_stop: float, atr_target: float) -> dict:
    """
    Simulate what would have happened if we entered at entry_i.
    Uses ATR-based stop/target and max_hold.

    Every returned dict carries `resolved`:
      True  — a stop, target, or a FULLY OBSERVED max-hold decided the outcome.
      False — the data ended before the max-hold horizon elapsed and neither the
              stop nor the target was touched, so the outcome is right-censored
              and unknown.

    A censored trade still reports the mark-to-market pnl at the last candle for
    display, but `resolved=False` means that number is NOT a realised outcome and
    must be excluded from PF, expectancy, bootstrap, episode concentration, and
    closed-trade counts. Callers that ignore `resolved` keep the previous
    behaviour exactly, so historical backtest figures are unchanged.
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
                    "hold_h": j - entry_i, "pnl_pct": round(net_pnl, 2),
                    "resolved": True}

        if high >= target_price:
            gross   = target_price * (1 - _TP_FEE)
            net_pnl = (gross - entry_price * (1 + _ENTRY_FEE)) / entry_price * 100
            return {"reason": "TAKE_PROFIT", "exit_price": target_price,
                    "hold_h": j - entry_i, "pnl_pct": round(net_pnl, 2),
                    "resolved": True}

    # Neither stop nor target hit. Did we actually observe the whole max-hold
    # horizon, or did the data simply run out?
    last_i    = min(entry_i + max_hold_hours, len(df) - 1)
    fully_obs = (entry_i + max_hold_hours) <= (len(df) - 1)

    exit_price = float(df.iloc[last_i]["close"])
    gross      = exit_price * (1 - _SL_FEE)
    net_pnl    = (gross - entry_price * (1 + _ENTRY_FEE)) / entry_price * 100
    return {"reason": "MAX_HOLD" if fully_obs else "PENDING",
            "exit_price": round(exit_price, 2),
            "hold_h": max_hold_hours if fully_obs else (last_i - entry_i),
            "pnl_pct": round(net_pnl, 2),
            "resolved": fully_obs}


# ── Per-asset scanner ─────────────────────────────────────────────────────────

class StrictSourceError(RuntimeError):
    """Coinbase cache was unusable while STRICT_COINBASE_ONLY was set."""


# When True, _fetch_ohlcv refuses to fall back to yfinance and raises instead.
# A registered research run sets this: silently swapping the data provider
# mid-run would make the manifest's SHA-256 hashes describe inputs that were not
# actually used, which is worse than failing.
STRICT_COINBASE_ONLY = False


def _fetch_ohlcv(asset: str, start: str, end: str, interval: str) -> pd.DataFrame | None:
    """
    Fetch OHLCV from Coinbase (preferred) with yfinance as fallback.

    Under STRICT_COINBASE_ONLY the fallback is disabled and a Coinbase failure
    raises StrictSourceError.
    """
    # ── Coinbase path ──────────────────────────────────────────────────────────
    cb_error: Exception | None = None
    try:
        from exchange.coinbase_candles import download as _cb_download
        gran_map = {"1h": "1h", "4h": "4h", "1d": "1d"}
        gran = gran_map.get(interval)
        if gran:
            df = _cb_download(asset, start=start, end=end, granularity=gran, verbose=False)
            if df is not None and not df.empty and len(df) >= 20:
                df = df.set_index("time")
                df.index = pd.to_datetime(df.index, utc=True)
                df.index.name = None  # avoid "time is both index and column" ambiguity
                return df.dropna(subset=["close", "open", "high", "low", "volume"])
    except Exception as exc:
        cb_error = exc      # fall through to yfinance unless strict

    if STRICT_COINBASE_ONLY:
        raise StrictSourceError(
            f"Coinbase cache unusable for {asset} {interval} {start}->{end} "
            f"({cb_error or 'insufficient rows'}) and STRICT_COINBASE_ONLY is set. "
            "Refusing to fall back to yfinance during a registered run."
        )

    # ── yfinance fallback ──────────────────────────────────────────────────────
    try:
        raw = yf.download(asset, start=start, end=end, interval=interval,
                          progress=False, auto_adjust=True)
        if raw is None or raw.empty:
            return None
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)
        raw.columns = [c.lower() for c in raw.columns]
        raw.index   = pd.to_datetime(raw.index, utc=True)
        return raw.dropna(subset=["close", "open", "high", "low", "volume"])
    except Exception:
        return None


def _download_and_compute(asset: str, start: str, end: str, interval: str) -> pd.DataFrame | None:
    """Fetch OHLCV (Coinbase preferred, yfinance fallback) and compute all indicators."""
    try:
        raw = _fetch_ohlcv(asset, start, end, interval)
        if raw is None or len(raw) < 20:
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


def _compute_regime_metrics(daily_df: pd.DataFrame | None, as_of_ts: pd.Timestamp) -> dict:
    """
    Compute regime quality metrics from daily OHLCV *before* the signal date.
    All values use only past data — zero look-ahead.

    Returns:
      er_30   : Kaufman Trend Efficiency Ratio over last 30 trading days.
                er_30 = |net move| / sum(|daily moves|).  Range [0, 1].
                1.0 = perfectly straight trend; 0.0 = pure noise.
      vm_30   : Vol-adjusted momentum = 30-day return / annualised realised vol.
                Positive and large in strong, low-noise uptrends.
      ema50_slope : 5-day fractional change of daily EMA50 (trend angle proxy).
    """
    if daily_df is None or daily_df.empty:
        return {}

    # Strictly trailing: use only daily candles that are FULLY CLOSED before today.
    # A Coinbase daily candle is timestamped at its START (e.g. 2021-03-08 00:00 UTC
    # covers 2021-03-08 00:00 → 2021-03-09 00:00). At signal time 20:00 on March 8,
    # that candle is NOT closed — its close is future relative to the signal.
    # Fix: cut at midnight of the signal's date so only yesterday-and-earlier is used.
    ts = as_of_ts if as_of_ts.tzinfo else as_of_ts.tz_localize("UTC")

    # Both ts and the daily index must be UTC to avoid silent timezone offset in normalize().
    if daily_df.index.tzinfo is None:
        daily_df = daily_df.copy()
        daily_df.index = daily_df.index.tz_localize("UTC")
    elif str(daily_df.index.tzinfo) != "UTC":
        daily_df = daily_df.copy()
        daily_df.index = daily_df.index.tz_convert("UTC")

    day_boundary = ts.normalize()                          # midnight UTC on signal date
    pre_signal   = daily_df[daily_df.index < day_boundary]
    hist         = pre_signal.tail(40)
    if len(hist) < 20:
        return {}

    n_daily_bars = len(pre_signal)
    ema200_valid = n_daily_bars >= 200                     # EMA200 needs ~200 trading days

    closes = hist["close"].astype(float).values

    # ── Trend Efficiency Ratio (30-day) ─────────────────────────────────────
    c30 = closes[-min(30, len(closes)):]
    net_move   = abs(float(c30[-1]) - float(c30[0]))
    gross_path = float(np.sum(np.abs(np.diff(c30))))
    er_30 = round(net_move / gross_path, 3) if gross_path > 0 else 0.0

    # ── Vol-adjusted momentum (30-day) ──────────────────────────────────────
    ret_30    = (float(c30[-1]) - float(c30[0])) / float(c30[0])
    daily_ret = np.diff(c30) / c30[:-1]
    rv        = float(np.std(daily_ret)) * np.sqrt(252) if len(daily_ret) > 1 else 1.0
    vm_30     = round(ret_30 / rv, 3) if rv > 0 else 0.0

    # ── EMA50 slope (5-day fractional change) ───────────────────────────────
    ema50_vals = hist["ema50"].astype(float).values if "ema50" in hist.columns else None
    ema50_slope = None
    if ema50_vals is not None and len(ema50_vals) >= 5:
        e_now, e_5d = float(ema50_vals[-1]), float(ema50_vals[-5])
        ema50_slope = round((e_now - e_5d) / e_5d, 4) if e_5d > 0 else 0.0

    result = {
        "er_30":        er_30,
        "vm_30":        vm_30,
        "n_daily_bars": n_daily_bars,
        "ema200_valid": ema200_valid,
    }
    if ema50_slope is not None:
        result["ema50_slope"] = ema50_slope
    return result


def scan_asset(asset: str, period: dict, *, v3_enforcement: bool | None = None,
               config_override: dict | None = None) -> dict:
    """
    Scan one asset over `period`.

    config_override:
      None — use the asset's own ASSET_CONFIG entry (normal behaviour).
      dict — use these parameters INSTEAD. This is what makes a zero-tuning
             transfer test honest: applying each asset's own tuned stop/target/
             EMA settings measures "per-asset tuned strategies", not "does the
             ZEC mechanism transfer". The override is copied, never mutated.

    v3_enforcement:
      None  — use the asset's configured `v3_enforcement_enabled` (default False).
      True  — run the INTEGRATED path: a V3-blocked signal is skipped before the
              trade is simulated, so it does not advance `skip_until` and does not
              contribute to stop history. Later signals therefore differ from the
              unfiltered path — this is what live enforcement would actually do.
      False — run the UNFILTERED path: every signal is simulated and
              `v3_would_block` is recorded for shadow accounting only.

    This is an explicit, per-call argument precisely so that research code never
    mutates the ASSET_CONFIG global to switch modes: a mutated global leaks into
    every later scan in the same process and silently corrupts comparisons.
    """
    print(f"\n  Downloading {asset} data (warmup from {period['warmup']})...")

    config     = STRATEGY_CONFIG.get(asset, STRATEGY_CONFIG["ETH-USD"])
    asset_cfg  = (dict(config_override) if config_override is not None
                  else ASSET_CONFIG.get(asset, ASSET_CONFIG["ZEC-USD"]))
    # max_hold is part of the MECHANISM, so an override must supply it too.
    # Reading it from STRATEGY_CONFIG regardless meant a "frozen ZEC mechanism"
    # transfer test still ran BTC at its own 48h instead of ZEC's 36h — not a
    # zero-tuning transfer at all.
    max_hold   = (asset_cfg.get("max_hold_hours")
                  if config_override is not None and "max_hold_hours" in asset_cfg
                  else config.get("max_hold_hours", 36))
    atr_stop   = asset_cfg["atr_stop"]
    atr_target = asset_cfg["atr_target"]

    # BTC regime filter — attach BTC daily close vs EMA50 for each 1h bar
    # Must come after asset_cfg is resolved (needs btc_regime_filter flag).
    # BTC-USD itself never gets this column, so for BTC the gate is NOT
    # APPLICABLE rather than unavailable — the distinction decides whether a
    # missing column refuses a signal.
    btc_regime_applicable = (asset != "BTC-USD"
                             and bool(asset_cfg.get("btc_regime_filter", False)))
    df, daily_df = build_merged_frame(
        asset, period["warmup"], period["end"], asset_cfg,
        btc_regime_applicable=btc_regime_applicable)
    if df is None:
        print(f"  {asset}: no data")
        return {}

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
    blocked_v3        = 0
    # Signals refused because a DECLARED gate could not be evaluated on that bar.
    # Every one of these used to be silently admitted under a weaker mechanism.
    blocked_unavail   = 0
    last_unavail_ts: pd.Timestamp | None = None
    missing_seen: dict[str, int] = {}
    skip_until        = -1   # don't double-enter
    recent_stop_ts: list[pd.Timestamp] = []
    v3_threshold      = asset_cfg.get("v3_candidate_threshold")
    # Explicit per-call override wins; otherwise fall back to the asset config.
    # Read once into a local so the value is immutable for this scan.
    v3_enforcement    = (
        asset_cfg.get("v3_enforcement_enabled", False)
        if v3_enforcement is None else bool(v3_enforcement)
    )

    # We need to operate on the full df (for lookback), but filter output to period
    full_len   = len(df)
    period_start_idx = df.index.get_loc(df_period.index[0]) if not df_period.empty else 0

    for i in range(period_start_idx, full_len):
        if i < skip_until:
            continue

        result = _detect_breakout_signal(
            df, i, asset_cfg, btc_regime_applicable=btc_regime_applicable)
        if result is None:
            continue

        ts    = df.index[i]
        price = float(df.iloc[i]["close"])

        if result.get("blocked") == "gate_inputs_unavailable":
            blocked_unavail += 1
            last_unavail_ts = ts
            for name in result.get("missing", []):
                missing_seen[name] = missing_seen.get(name, 0) + 1
            continue
        elif result.get("blocked") == "vol_gate":
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

        # V3 regime filter — always compute for tracking; enforce only if enabled
        regime = _compute_regime_metrics(daily_df, ts)
        er = regime.get("er_30")
        v3_would_block = (v3_threshold is not None and er is not None and er < v3_threshold)
        if v3_would_block and v3_enforcement:
            blocked_v3 += 1
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
            "regime":       regime,
            "v3_would_block": v3_would_block,
            # Which path produced this record. Stamped per-signal so an
            # unfiltered cohort can never be passed off as an integrated one.
            "v3_enforcement": v3_enforcement,
        }
        signals.append(record)
        skip_until = i + trade["hold_h"] + 1   # don't re-enter mid-hold

    return {
        "asset":            asset,
        "candles":          len(df_period),
        "signals":          signals,
        # Authoritative label for this result set. Consumers must check it rather
        # than assuming which path they were handed.
        "v3_enforcement":   v3_enforcement,
        # Which parameter set actually ran — so a per-asset-tuned scan can never
        # be reported as a frozen-mechanism transfer test.
        "mechanism":        "override" if config_override is not None else f"own:{asset}",
        "mechanism_params": {
            **{k: asset_cfg.get(k) for k in
               ("atr_stop", "atr_target", "min_conditions", "vol_spike_ratio",
                "daily_ema_period", "btc_regime_filter")},
            # Recorded from the value actually used, so a transfer test that
            # silently kept the asset's own hold window is visible in the artifact.
            "max_hold_hours": max_hold,
        },
        "blocked_vol":      blocked_vol,
        "blocked_4h":       blocked_4h,
        "blocked_daily":    blocked_daily,
        "blocked_btc":      blocked_btc,
        "blocked_cond":     blocked_cond,
        "blocked_whipsaw":  blocked_whipsaw,
        "blocked_v3":       blocked_v3,
        # Warm-up accounting. `blocked_gate_unavailable` counts signals that
        # formed but could not be judged by the declared mechanism; under the
        # previous fail-open rule every one of them was ACCEPTED instead.
        "blocked_gate_unavailable": blocked_unavail,
        "last_gate_unavailable_ts": (last_unavail_ts.isoformat()
                                     if last_unavail_ts is not None else None),
        "gate_unavailable_by_input": dict(sorted(missing_seen.items())),
        "btc_regime_applicable": btc_regime_applicable,
        "gate_availability": gate_availability(
            df, asset_cfg, btc_regime_applicable=btc_regime_applicable),
        "requested_start":  period["start"],
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
    daily_df = _download_and_compute(asset, "2020-01-01",  today, "1d")

    if sig_df is None or trend_df is None or len(sig_df) < 50:
        return None

    df = attach_higher_timeframe_context(sig_df, trend_df)
    if daily_df is not None:
        df = _attach_daily_context(df, daily_df)
    if "time" in df.columns:
        df.index = pd.to_datetime(df["time"], utc=True)

    # BTC regime filter — attach BTC daily EMA50 so _detect_breakout_signal can check
    btc_regime_applicable = (asset != "BTC-USD"
                             and bool(cfg.get("btc_regime_filter", False)))
    if btc_regime_applicable:
        btc_daily = _download_and_compute("BTC-USD", _DAILY_HISTORY_START, today, "1d")
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

    result = _detect_breakout_signal(df, i, cfg,
                                     btc_regime_applicable=btc_regime_applicable)
    if result is None:
        return None
    if result.get("blocked") == "gate_inputs_unavailable":
        # LIVE SAFETY: a declared gate could not be evaluated on the last closed
        # candle — most plausibly a failed daily-candle download, which used to
        # drop the daily trend veto entirely and let a BUY through during a data
        # outage. Refuse, and say so: a silent None would look like "no setup".
        print(f"[ScanLatest] {asset}: REFUSED — declared gate inputs unavailable: "
              f"{', '.join(result.get('missing', []))}")
        return None
    if result.get("blocked"):
        return None

    ts = df.index[i]

    # V3 regime filter — always compute for shadow journaling regardless of enforcement.
    v3_threshold    = cfg.get("v3_candidate_threshold")   # pre-registered, locked
    v3_enforcement  = cfg.get("v3_enforcement_enabled", False)
    regime = {}
    if daily_df is not None:
        regime = _compute_regime_metrics(daily_df, ts)

    er = regime.get("er_30")
    v3_would_block = (
        v3_threshold is not None
        and er is not None
        and er < v3_threshold
    )
    # Enforcement = shadow AND active gate — only blocks when explicitly enabled
    v3_blocked = v3_would_block and v3_enforcement

    signal_dict = {
        "asset":           asset,
        "entry_time":      str(ts),
        "entry_price":     float(df.iloc[i]["close"]),
        "atr":             float(df.iloc[i].get("atr", 0)),
        "conf":            result["confidence"],
        "n_conditions":    result["n_conditions"],
        "candles_above":   result["candles_above"],
        "adx":             result["adx"],
        "vol_ratio":       result["vol_ratio"],
        "er_30":           er,
        "vm_30":           regime.get("vm_30"),
        "ema50_slope":     regime.get("ema50_slope"),
        "ema200_valid":    regime.get("ema200_valid"),
        "n_daily_bars":    regime.get("n_daily_bars"),
        "v3_candidate_threshold": v3_threshold,
        "v3_would_block":  v3_would_block,
        "v3_enforcement":  v3_enforcement,
        "v3_blocked":      v3_blocked,
    }

    if v3_blocked:
        print(f"[ScanLatest] V3 enforcement blocked {asset}: ER-30={er:.3f} < {v3_threshold:.3f}")

    return signal_dict


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
        bv3        = r.get("blocked_v3", 0)
        atr_stop   = r.get("atr_stop", 2.0)
        atr_target = r.get("atr_target", 3.5)
        rr         = atr_target / atr_stop
        bd  = r.get("blocked_daily", 0)
        bb  = r.get("blocked_btc", 0)
        btc_str = f"  btc={bb}" if bb else ""
        v3_str  = f"  v3_er={bv3}" if bv3 else ""
        print(f"  {asset}  ({len(sigs)} signals  |  "
              f"stop={atr_stop}x  target={atr_target}x  R:R={rr:.2f}  |  "
              f"blocked: vol={r['blocked_vol']}  4h={r['blocked_4h']}  "
              f"daily={bd}{btc_str}  cond={r['blocked_cond']}  whipsaw={bw}{v3_str})")

        if not sigs:
            print("    No signals fired in this period.")
            continue

        wins   = [s for s in sigs if s["trade"]["pnl_pct"] > 0]
        losses = [s for s in sigs if s["trade"]["pnl_pct"] <= 0]
        avg_pnl  = sum(s["trade"]["pnl_pct"] for s in sigs) / len(sigs)
        avg_win  = sum(s["trade"]["pnl_pct"] for s in wins)  / len(wins)  if wins   else 0
        avg_loss = sum(s["trade"]["pnl_pct"] for s in losses) / len(losses) if losses else 0
        pf = abs(avg_win * len(wins)) / abs(avg_loss * len(losses)) if losses and avg_loss else float("inf")
        by_reason = {}
        for s in sigs:
            reason = s["trade"]["reason"]
            by_reason.setdefault(reason, []).append(s["trade"]["pnl_pct"])

        print(f"  Win rate: {len(wins)}/{len(sigs)} = {len(wins)/len(sigs):.1%}    "
              f"Avg P&L: {avg_pnl:+.2f}%    Profit factor: {pf:.2f}")
        print(f"  Avg win: {avg_win:+.2f}%  |  Avg loss: {avg_loss:+.2f}%  |  "
              f"Realized R:R: {abs(avg_win/avg_loss):.2f}" if avg_loss else "")
        reason_parts = [
            f"{reason}: {len(v)} trades (avg {sum(v)/len(v):+.2f}%)"
            for reason, v in sorted(by_reason.items())
        ]
        print(f"  Exits: {', '.join(reason_parts)}")

        # ── Regime metrics breakdown ──────────────────────────────────────────
        er_wins = [s["regime"]["er_30"] for s in wins   if s.get("regime", {}).get("er_30") is not None]
        er_loss = [s["regime"]["er_30"] for s in losses if s.get("regime", {}).get("er_30") is not None]
        if er_wins or er_loss:
            avg_er_w = sum(er_wins) / len(er_wins) if er_wins else None
            avg_er_l = sum(er_loss) / len(er_loss) if er_loss else None
            w_str = f"{avg_er_w:.3f}" if avg_er_w is not None else "n/a"
            l_str = f"{avg_er_l:.3f}" if avg_er_l is not None else "n/a"
            diff_str = (f"  diff={avg_er_w - avg_er_l:+.3f}"
                        if avg_er_w is not None and avg_er_l is not None else "")
            print(f"  ER-30 (wins/losses): {w_str} / {l_str}{diff_str}")

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
