"""
Technical Analysis Agent.

Wraps the existing backtest signal logic (RSI, MACD, Bollinger Bands,
EMA trend, volume) and asks Claude to reason over the raw indicator
values to produce a structured AgentSignal.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.base_agent import BaseAgent
from schemas.signals import AgentName, AgentSignal, SignalType
from tools.price_data import get_snapshot

_SYSTEM = """You are a quantitative technical analyst for cryptocurrency markets.

You receive a dict of technical indicator values for an asset and must output
a JSON object with exactly these keys:
  signal      : "BUY" | "SELL" | "NEUTRAL"
  confidence  : float between 0.0 and 1.0
  reasoning   : one or two sentences explaining your conclusion
  key_levels  : {"support": <float>, "resistance": <float>}

Rules:
- BUY only if trend is up, MACD positive, RSI not overbought, BB not extended
- SELL only if multiple bearish signals align
- NEUTRAL when signals are mixed or data is unreliable
- confidence reflects how strongly the indicators agree (1.0 = all aligned)
- Return ONLY the JSON object, no markdown, no extra text."""


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

        user_prompt = f"""Asset: {asset}

Indicator snapshot:
- Close price:       {snapshot['close']:.2f}
- RSI (1h):          {snapshot['rsi_1h']:.2f}
- MACD diff (1h):    {snapshot['macd_diff_1h']:.6f}
- BB % (1h):         {snapshot['bb_pct_1h']:.3f}   (0=lower band, 1=upper band)
- Volume ratio (1h): {snapshot['volume_ratio_1h']:.3f}
- EMA50 (1h):        {snapshot['ema50_1h']:.2f}
- EMA200 (1h):       {snapshot['ema200_1h']:.2f}
- Close (4h):        {snapshot.get('close_4h', 'N/A')}
- EMA50 (4h):        {snapshot.get('ema50_4h', 'N/A')}
- EMA200 (4h):       {snapshot.get('ema200_4h', 'N/A')}
- 4h trend:          {snapshot.get('trend_4h', 'N/A')}
- 4h trend strength: {snapshot.get('trend_strength_4h', 'N/A')}

Deterministic signal from rule engine: {snapshot['signal']}
Indicator alignment:
  trend_ok={snapshot['trend_ok']}, macd_ok={snapshot['macd_ok']},
  rsi_ok={snapshot['rsi_ok']}, bb_ok={snapshot['bb_ok']},
  volume_ok={snapshot['volume_ok']}, buy_ready={snapshot['buy_ready']}

Evaluate these indicators and produce your JSON output."""

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
                "rsi":          snapshot["rsi_1h"],
                "macd_diff":    snapshot["macd_diff_1h"],
                "bb_pct":       snapshot["bb_pct_1h"],
                "volume_ratio": snapshot["volume_ratio_1h"],
                "rule_signal":  snapshot["signal"],
            },
        )
