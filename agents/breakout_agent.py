"""
Breakout / Momentum Agent — deterministic, no LLM call.

Detects EMA50 upward crossovers within the last 1-4 candles with supporting
evidence (volume spike, positive CVD, reasonable RSI, ADX confirming trend).

Fires BUY only inside the breakout window — BEFORE RSI goes overbought and
VWAP extension accumulates. Returns NEUTRAL outside that window so the normal
agents handle extended moves without interference.

The orchestrator lowers the composite score threshold when this agent fires BUY,
allowing early-momentum entries that the multi-signal consensus system would
otherwise miss.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.base_agent import BaseAgent
from schemas.signals import AgentName, AgentSignal, SignalType
from tools.price_data import get_raw_df, get_snapshot

# Breakout window: how many consecutive candles above EMA50 we still consider "fresh"
_MAX_CANDLES_SINCE_CROSS = 4
# Minimum quality conditions to emit a BUY (out of 5 total)
_MIN_CONDITIONS = 3
# Volume ratio threshold for a "volume spike" confirmation (scored condition)
_VOL_SPIKE_RATIO = 1.3
# Hard minimum volume gate — breakouts on thin volume are almost always false.
# 0.36x avg (like the BTC entry that closed at MAX_HOLD loss) = noise, not breakout.
_MIN_VOL_RATIO = 0.8
# RSI cap at the crossover candle — we don't want to chase already-overbought entries
_MAX_RSI_AT_CROSS = 65.0
# ADX floor — only trade trending markets
_MIN_ADX = 20.0
# Max % above EMA50 right now — don't chase moves that are already extended
_MAX_PCT_ABOVE_EMA = 4.0


class BreakoutAgent(BaseAgent):
    name = AgentName.BREAKOUT

    def __init__(self) -> None:
        pass  # deterministic — no Claude API needed

    def analyze(self, asset: str) -> AgentSignal:
        df = get_raw_df(asset)
        if df is None or len(df) < 20:
            return AgentSignal(
                agent=self.name, asset=asset, timestamp=self._now(),
                signal=SignalType.NEUTRAL, confidence=0.30,
                reasoning="Insufficient data for breakout detection.",
            )

        close_arr = df["close"].values
        ema50_arr = df["ema50"].values  # 1h EMA50

        # ── Walk backwards to count consecutive candles above EMA50 ───────────
        # Goal: detect the most recent crossover from below to above EMA50.
        candles_above      = 0
        crossed_from_below = False
        look_back          = min(12, len(close_arr))

        for i in range(len(close_arr) - 1, len(close_arr) - 1 - look_back, -1):
            if close_arr[i] > ema50_arr[i]:
                candles_above += 1
            else:
                # Found the last candle that was below EMA50
                if candles_above > 0:
                    crossed_from_below = True
                break

        # ── Reject if not a fresh breakout ───────────────────────────────────
        if not crossed_from_below or candles_above == 0:
            return AgentSignal(
                agent=self.name, asset=asset, timestamp=self._now(),
                signal=SignalType.NEUTRAL, confidence=0.30,
                reasoning="No EMA50 upward crossover detected in last 12 candles.",
                metrics={"candles_above_ema50": candles_above},
            )

        if candles_above > _MAX_CANDLES_SINCE_CROSS:
            return AgentSignal(
                agent=self.name, asset=asset, timestamp=self._now(),
                signal=SignalType.NEUTRAL, confidence=0.30,
                reasoning=(
                    f"EMA50 breakout is {candles_above} candles old "
                    f"(window={_MAX_CANDLES_SINCE_CROSS}h). Move already extended — normal agents apply."
                ),
                metrics={"candles_above_ema50": candles_above},
            )

        # ── Quality checks ────────────────────────────────────────────────────
        def _safe(val) -> float | None:
            try:
                v = float(val)
                return None if math.isnan(v) else v
            except (TypeError, ValueError):
                return None

        # The crossover candle: first candle that went above EMA50
        cross_row = df.iloc[-candles_above]
        last_row  = df.iloc[-1]

        rsi_at_cross = _safe(cross_row["rsi"]          if "rsi"          in cross_row.index else None) or 50.0
        adx_now      = _safe(last_row["adx"]           if "adx"          in last_row.index  else None) or 0.0
        vol_ratio    = _safe(last_row["volume_ratio"]  if "volume_ratio" in last_row.index  else None) or 1.0
        cvd_24h      = _safe(last_row["cvd_24h"]       if "cvd_24h"      in last_row.index  else None) or 0.0
        close_now    = _safe(last_row["close"]) or 0.0
        ema50_now    = _safe(last_row["ema50"]) or 1.0
        pct_above    = (close_now - ema50_now) / ema50_now * 100 if ema50_now else 999.0

        # Hard volume gate — thin-volume breakouts are almost always false moves.
        # This is not a scored condition; it blocks entry entirely below 0.8x avg volume.
        if vol_ratio < _MIN_VOL_RATIO:
            return AgentSignal(
                agent=self.name, asset=asset, timestamp=self._now(),
                signal=SignalType.NEUTRAL, confidence=0.30,
                reasoning=(
                    f"EMA50 crossover detected ({candles_above} candle(s) above) but volume {vol_ratio:.2f}x "
                    f"is below minimum {_MIN_VOL_RATIO}x — low-volume breakouts are false signals. "
                    f"Waiting for volume confirmation."
                ),
                metrics={
                    "candles_above_ema50": candles_above,
                    "vol_ratio":           round(vol_ratio, 2),
                    "vol_gate_blocked":    True,
                },
            )

        # 4h trend gate — only enter if the 4h trend is bullish (close > 4h EMA50).
        # A 1h EMA50 crossover against a 4h downtrend is a dead-cat bounce, not a breakout.
        # Backtest result: this filter improved returns by +1-1.6% across all market regimes.
        snap = get_snapshot(asset)
        if snap:
            close_4h = snap.get("close_4h")
            ema50_4h = snap.get("ema50_4h")
            if close_4h is not None and ema50_4h is not None:
                if close_4h < ema50_4h:
                    return AgentSignal(
                        agent=self.name, asset=asset, timestamp=self._now(),
                        signal=SignalType.NEUTRAL, confidence=0.30,
                        reasoning=(
                            f"EMA50 crossover detected ({candles_above} candle(s) above) but 4h trend is "
                            f"bearish (close_4h ${close_4h:,.2f} < ema50_4h ${ema50_4h:,.2f}). "
                            f"1h crossover against 4h downtrend = likely dead-cat bounce. Blocked."
                        ),
                        metrics={
                            "candles_above_ema50": candles_above,
                            "vol_ratio":           round(vol_ratio, 2),
                            "close_4h":            round(close_4h, 2),
                            "ema50_4h":            round(ema50_4h, 2),
                            "trend_4h_blocked":    True,
                        },
                    )

        conditions: list[tuple[bool, str]] = [
            (rsi_at_cross < _MAX_RSI_AT_CROSS,
             f"RSI at cross {rsi_at_cross:.0f}<{_MAX_RSI_AT_CROSS:.0f} (not overbought)"),
            (adx_now >= _MIN_ADX,
             f"ADX {adx_now:.1f}>={_MIN_ADX:.0f} (trending)"),
            (vol_ratio >= _VOL_SPIKE_RATIO,
             f"Volume {vol_ratio:.2f}x avg (spike)"),
            (cvd_24h > 0,
             f"CVD 24h positive +{cvd_24h:.0f} (buyers dominating)"),
            (pct_above < _MAX_PCT_ABOVE_EMA,
             f"Price {pct_above:+.2f}% vs EMA50 (not extended)"),
        ]

        met     = [lbl for ok, lbl in conditions if ok]
        not_met = [lbl for ok, lbl in conditions if not ok]
        n_met   = len(met)

        base_reasoning = (
            f"EMA50 breakout: {candles_above} candle(s) above EMA50. "
            f"{n_met}/{len(conditions)} confirmed: {', '.join(met)}."
            + (f" Missing: {', '.join(not_met)}." if not_met else "")
        )

        if n_met < _MIN_CONDITIONS:
            return AgentSignal(
                agent=self.name, asset=asset, timestamp=self._now(),
                signal=SignalType.NEUTRAL, confidence=0.35,
                reasoning=f"Weak breakout ({n_met}/{len(conditions)} conditions). " + base_reasoning,
                metrics={
                    "candles_above_ema50": candles_above,
                    "rsi_at_cross":        round(rsi_at_cross, 1),
                    "adx_now":             round(adx_now, 1),
                    "vol_ratio":           round(vol_ratio, 2),
                    "cvd_24h":             round(cvd_24h, 0),
                    "pct_above_ema50":     round(pct_above, 2),
                },
            )

        # Confidence: 0.65 for 3/5, +0.08 per extra condition (max 0.89)
        confidence = min(0.57 + 0.08 * n_met, 0.89)

        return AgentSignal(
            agent=self.name, asset=asset, timestamp=self._now(),
            signal=SignalType.BUY, confidence=confidence,
            reasoning=base_reasoning,
            metrics={
                "candles_above_ema50": candles_above,
                "rsi_at_cross":        round(rsi_at_cross, 1),
                "adx_now":             round(adx_now, 1),
                "vol_ratio":           round(vol_ratio, 2),
                "cvd_24h":             round(cvd_24h, 0),
                "pct_above_ema50":     round(pct_above, 2),
            },
        )
