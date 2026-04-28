"""
Sentiment Agent.

Reads market mood from two free sources:
  1. Fear & Greed Index (alternative.me)
  2. Recent CryptoPanic news headlines

Claude interprets these to produce a directional signal.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.base_agent import BaseAgent
from schemas.signals import AgentName, AgentSignal, SignalType
from tools.sentiment_data import get_fear_and_greed, get_recent_headlines
from tools.onchain_data import get_onchain_metrics

_SYSTEM = """You are a crypto market sentiment analyst.

You receive the Fear & Greed Index value, recent price change context, and news headlines.
Your job is to assess whether sentiment supports the current market momentum.

Output a JSON object with exactly these keys:
  signal      : "BUY" | "SELL" | "NEUTRAL"
  confidence  : float between 0.0 and 1.0
  reasoning   : one or two sentences summarizing the sentiment picture

Rules — TREND-FOLLOWING mode (primary):
- Greed (55-74) + positive recent price change → BUY (trend continuation)
- Extreme Greed (75-90) + positive price change → BUY with moderate confidence
- Fear (26-45) + negative recent price change → SELL (momentum down)
- Extreme Fear (0-25) + negative price change → SELL (panic selling confirms downtrend)
- Neutral (46-54) → NEUTRAL regardless of price

Contrarian overrides (only at absolute extremes):
- Extreme Greed (>90) + parabolic move (>20% in 7 days) → SELL (blow-off top signal)
- Extreme Fear (<10) + price already down >30% in 7 days → BUY (capitulation signal)

News always overrides index:
- Headlines about regulation bans, hacks, or major collapses → SELL
- Headlines about ETF approval, institutional buying, protocol upgrades → BUY lean
- confidence 0.3 for mixed signals, 0.7+ when sentiment and momentum clearly align
- Return ONLY the JSON object, no markdown, no extra text."""


class SentimentAgent(BaseAgent):
    name = AgentName.SENTIMENT

    def analyze(self, asset: str) -> AgentSignal:
        fg        = get_fear_and_greed()
        headlines = get_recent_headlines(asset, limit=8)
        metrics   = get_onchain_metrics(asset)

        fg_value = fg.get("value", 50)
        fg_label = fg.get("label", "Unknown")
        change_24h = metrics.get("price_change_24h", 0.0)
        change_7d  = metrics.get("price_change_7d", 0.0)

        headlines_text = "\n".join(f"  - {h}" for h in headlines) if headlines else "  (no headlines available)"

        user_prompt = f"""Asset: {asset}

Fear & Greed Index: {fg_value}/100 -- {fg_label}
Recent price change: {change_24h:+.1f}% (24h), {change_7d:+.1f}% (7d)

Recent news headlines:
{headlines_text}

Analyze sentiment and produce your JSON output."""

        result = self._ask_claude_json(_SYSTEM, user_prompt)

        return AgentSignal(
            agent=self.name,
            asset=asset,
            timestamp=self._now(),
            signal=SignalType(result["signal"]),
            confidence=float(result["confidence"]),
            reasoning=result["reasoning"],
            metrics={
                "fear_greed_value": fg_value,
                "fear_greed_label": fg_label,
                "headline_count":   len(headlines),
                "change_24h":       change_24h,
                "change_7d":        change_7d,
            },
        )
