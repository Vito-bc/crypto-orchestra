"""
Macro / Regime Agent.

Classifies the current market regime (BULL / BEAR / RANGING) using
the 4h trend context from existing logic, plus BTC dominance.

This agent acts as the VETO layer — if it returns BEAR regime,
the orchestrator will block all BUY decisions regardless of other agents.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.base_agent import BaseAgent
from schemas.signals import AgentName, AgentSignal, MarketRegime, SignalType
from tools.price_data import get_snapshot
from tools.onchain_data import get_onchain_metrics

_SYSTEM = """You are a macro market regime analyst for cryptocurrency markets.

You receive 4-hour trend data and market structure metrics. Your job is to
classify the current market regime and produce a directional signal.

Output a JSON object with exactly these keys:
  signal   : "BUY" | "SELL" | "NEUTRAL"
  regime   : "BULL" | "BEAR" | "RANGING" | "UNKNOWN"
  confidence : float between 0.0 and 1.0
  reasoning  : one or two sentences

Rules:
- BEAR regime → always output signal "SELL" — this acts as a system-wide veto
- BULL regime → output "BUY" if structure is intact, else "NEUTRAL"
- RANGING → "NEUTRAL"
- confidence reflects how clear the regime classification is

IMPORTANT — avoid false BEAR classifications:
- A short-term EMA cross during an obvious bull market (close > EMA200 4h, positive 7d return)
  is a PULLBACK within uptrend, NOT a regime change — classify as RANGING not BEAR
- Only classify BEAR if: close_4h < EMA200_4h AND 7d price change is negative AND the
  trend_strength is below -0.003 (strong sustained downward momentum)
- During strong uptrends, prefer BULL or RANGING; reserve BEAR for genuine breakdowns
- Return ONLY the JSON object, no markdown, no extra text."""


class MacroAgent(BaseAgent):
    name = AgentName.MACRO

    def analyze(self, asset: str) -> AgentSignal:
        snapshot = get_snapshot(asset)
        metrics  = get_onchain_metrics(asset)

        if snapshot is None:
            return AgentSignal(
                agent=self.name,
                asset=asset,
                timestamp=self._now(),
                signal=SignalType.NEUTRAL,
                confidence=0.0,
                reasoning="Could not fetch price data for regime classification.",
                regime=MarketRegime.UNKNOWN,
            )

        user_prompt = f"""Asset: {asset}

4h Trend Context:
- Close (4h):        {snapshot.get('close_4h', 'N/A')}
- EMA50 (4h):        {snapshot.get('ema50_4h', 'N/A')}
- EMA200 (4h):       {snapshot.get('ema200_4h', 'N/A')}
- Trend direction:   {snapshot.get('trend_4h', 'N/A')}
- Trend strength:    {snapshot.get('trend_strength_4h', 'N/A')}
- 1h EMA50:          {snapshot.get('ema50_1h', 'N/A')}
- 1h EMA200:         {snapshot.get('ema200_1h', 'N/A')}
- Price vs EMA50:    {"above" if snapshot["close"] > snapshot["ema50_1h"] else "below"}

Market Structure:
- BTC dominance:     {metrics.get('btc_dominance', 'N/A')}%
- 24h price change:  {metrics.get('price_change_24h', 'N/A')}%
- 7d price change:   {metrics.get('price_change_7d', 'N/A')}%
- Volume/MCap ratio: {metrics.get('volume_market_ratio', 'N/A')}
- Market note:       {metrics.get('exchange_note', 'N/A')}

Classify the regime and produce your JSON output."""

        result = self._ask_claude_json(_SYSTEM, user_prompt)

        return AgentSignal(
            agent=self.name,
            asset=asset,
            timestamp=self._now(),
            signal=SignalType(result["signal"]),
            confidence=float(result["confidence"]),
            reasoning=result["reasoning"],
            regime=MarketRegime(result.get("regime", "UNKNOWN")),
            metrics={
                "trend_4h":       snapshot.get("trend_4h"),
                "btc_dominance":  metrics.get("btc_dominance"),
                "change_24h":     metrics.get("price_change_24h"),
                "change_7d":      metrics.get("price_change_7d"),
            },
        )
