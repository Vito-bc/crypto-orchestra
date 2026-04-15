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

_SYSTEM = """You are a crypto market sentiment analyst.

You receive the Fear & Greed Index value and recent news headlines.
Analyze the overall market sentiment and produce a directional signal.

Output a JSON object with exactly these keys:
  signal      : "BUY" | "SELL" | "NEUTRAL"
  confidence  : float between 0.0 and 1.0
  reasoning   : one or two sentences summarizing the sentiment picture

Rules:
- Extreme Fear (0-25) + no major negative catalyst → contrarian BUY signal
- Extreme Greed (75-100) → contrarian SELL signal (market may be overextended)
- Fear (26-45) → NEUTRAL or slight BUY lean
- Greed (55-74) → NEUTRAL or slight SELL lean
- Neutral (46-54) → NEUTRAL
- Headlines about regulation bans, hacks, or major collapses → SELL regardless of index
- Headlines about ETF approval, institutional buying, upgrades → BUY lean
- confidence reflects how clearly the sentiment picture is (0.3 for mixed signals)
- Return ONLY the JSON object, no markdown, no extra text."""


class SentimentAgent(BaseAgent):
    name = AgentName.SENTIMENT

    def analyze(self, asset: str) -> AgentSignal:
        fg     = get_fear_and_greed()
        headlines = get_recent_headlines(asset, limit=8)

        fg_value = fg.get("value", 50)
        fg_label = fg.get("label", "Unknown")

        headlines_text = "\n".join(f"  - {h}" for h in headlines) if headlines else "  (no headlines available)"

        user_prompt = f"""Asset: {asset}

Fear & Greed Index: {fg_value}/100 — {fg_label}

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
            },
        )
