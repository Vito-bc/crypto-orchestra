"""
Technical Analysis Agent.

Wraps existing backtest signal logic (RSI, MACD, Bollinger Bands,
EMA trend, volume) and adds swing-based support/resistance levels.

Claude receives both the indicator values AND the nearest S/R levels
so it can distinguish "RSI oversold at a key support" (high conviction BUY)
from "RSI oversold mid-range" (low conviction — likely just noise).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.base_agent import BaseAgent
from schemas.signals import AgentName, AgentSignal, SignalType
from tools.price_data import get_snapshot, get_raw_df
from tools.price_levels import get_levels_from_snapshot

_SYSTEM = """You are a quantitative technical analyst for cryptocurrency markets.

You receive technical indicator values AND key support/resistance levels
identified from recent swing highs and lows.

Output a JSON object with exactly these keys:
  signal      : "BUY" | "SELL" | "NEUTRAL"
  confidence  : float between 0.0 and 1.0
  reasoning   : one or two sentences explaining your conclusion
  key_levels  : {"support": <float or null>, "resistance": <float or null>}

Rules for signal:
- BUY: trend up + MACD positive + RSI not overbought + BB not extended
- SELL: multiple bearish signals align simultaneously
- NEUTRAL: mixed signals or price mid-range

Critical rule for confidence:
- If price is AT a known support AND indicators suggest BUY → confidence 0.70-0.85
  (support acts as a floor, stop placement is clean, risk/reward improves)
- If price is AT known resistance AND you would otherwise BUY → NEUTRAL instead
  (price hitting resistance = high reversal risk, bad timing to enter long)
- If price is mid-range (not near any S/R level) → cap confidence at 0.55
  (mid-range entries have poor risk/reward, price can go either way)
- If indicators are mixed → NEUTRAL regardless of S/R

Return ONLY the JSON object, no markdown, no extra text."""


class TechnicalAgent(BaseAgent):
    name = AgentName.TECHNICAL

    def analyze(self, asset: str) -> AgentSignal:
        snapshot = get_snapshot(asset)
        if snapshot is None:
            return AgentSignal(
                agent=self.name,
                asset=asset,
                timestamp=self._now(),
                signal=SignalType.NEUTRAL,
                confidence=0.0,
                reasoning="Could not fetch price data.",
            )

        # S/R detection from raw 1h dataframe
        raw_df = get_raw_df(asset)
        levels = get_levels_from_snapshot(raw_df) if raw_df is not None else {}

        sr_section = levels.get("context", "Support/resistance data unavailable.")
        at_sup     = levels.get("at_support", False)
        at_res     = levels.get("at_resistance", False)
        dist_sup   = levels.get("dist_to_support")
        dist_res   = levels.get("dist_to_resistance")

        user_prompt = f"""Asset: {asset}

--- TECHNICAL INDICATORS ---
Close price:       {snapshot['close']:.2f}
RSI (1h):          {snapshot['rsi_1h']:.2f}
MACD diff (1h):    {snapshot['macd_diff_1h']:.6f}
BB % (1h):         {snapshot['bb_pct_1h']:.3f}   (0=lower band, 1=upper band)
Volume ratio (1h): {snapshot['volume_ratio_1h']:.3f}
EMA50 (1h):        {snapshot['ema50_1h']:.2f}
EMA200 (1h):       {snapshot['ema200_1h']:.2f}
Close (4h):        {snapshot.get('close_4h', 'N/A')}
EMA50 (4h):        {snapshot.get('ema50_4h', 'N/A')}
EMA200 (4h):       {snapshot.get('ema200_4h', 'N/A')}
4h trend:          {snapshot.get('trend_4h', 'N/A')}
4h trend strength: {snapshot.get('trend_strength_4h', 'N/A')}

Rule engine signal: {snapshot['signal']}
Alignment: trend={snapshot['trend_ok']} macd={snapshot['macd_ok']} rsi={snapshot['rsi_ok']} bb={snapshot['bb_ok']} vol={snapshot['volume_ok']}

--- SUPPORT / RESISTANCE LEVELS ---
{sr_section}
At support:    {at_sup}  {f'(distance: {dist_sup:.1f}x ATR)' if dist_sup else ''}
At resistance: {at_res}  {f'(distance: {dist_res:.1f}x ATR)' if dist_res else ''}

Evaluate and produce your JSON output."""

        result = self._ask_claude_json(_SYSTEM, user_prompt)

        return AgentSignal(
            agent=self.name,
            asset=asset,
            timestamp=self._now(),
            signal=SignalType(result["signal"]),
            confidence=float(result["confidence"]),
            reasoning=result["reasoning"],
            key_levels=result.get("key_levels"),
            metrics={
                "rsi":            snapshot["rsi_1h"],
                "macd_diff":      snapshot["macd_diff_1h"],
                "bb_pct":         snapshot["bb_pct_1h"],
                "volume_ratio":   snapshot["volume_ratio_1h"],
                "rule_signal":    snapshot["signal"],
                "at_support":     at_sup,
                "at_resistance":  at_res,
                "nearest_support":    levels.get("nearest_support"),
                "nearest_resistance": levels.get("nearest_resistance"),
            },
        )
