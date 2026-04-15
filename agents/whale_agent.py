"""
Whale / On-Chain Agent.

Uses CoinGecko free API to derive exchange flow proxies:
  - High volume + price drop  → likely exchange inflow (bearish)
  - High volume + price rise  → likely exchange outflow / accumulation (bullish)
  - BTC dominance trends      → risk-on/off environment

When GLASSNODE_API_KEY or WHALE_ALERT_API_KEY are set in .env,
this agent will upgrade to real on-chain data automatically.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.base_agent import BaseAgent
from schemas.signals import AgentName, AgentSignal, SignalType
from tools.onchain_data import get_onchain_metrics

_SYSTEM = """You are a crypto on-chain and whale flow analyst.

You receive market structure data that proxies large-player behavior:
volume/market-cap ratio, BTC dominance, price trends, and exchange notes.

Output a JSON object with exactly these keys:
  signal      : "BUY" | "SELL" | "NEUTRAL"
  confidence  : float between 0.0 and 1.0
  reasoning   : one or two sentences

Rules:
- High volume sell-off (volume/mcap > 0.15 + price -3%+) → SELL, confidence 0.7+
- High volume rally (volume/mcap > 0.15 + price +3%+)    → BUY, confidence 0.65+
- Rising BTC dominance (>55%) while altcoin analyzed     → SELL (capital rotating to BTC)
- Falling BTC dominance (<42%)                           → BUY lean for altcoins
- Low volume (<0.04 ratio) = low conviction              → NEUTRAL, confidence 0.3
- Normal market activity                                 → NEUTRAL
- confidence 0.5 = uncertain, 0.8 = strong conviction
- Return ONLY the JSON object, no markdown, no extra text."""


class WhaleAgent(BaseAgent):
    name = AgentName.WHALE

    def analyze(self, asset: str) -> AgentSignal:
        metrics = get_onchain_metrics(asset)

        if "error" in metrics:
            return AgentSignal(
                agent=self.name,
                asset=asset,
                timestamp=self._now(),
                signal=SignalType.NEUTRAL,
                confidence=0.0,
                reasoning=f"On-chain data unavailable: {metrics['error']}",
            )

        user_prompt = f"""Asset: {asset}

On-chain & Market Flow Metrics:
- BTC dominance:       {metrics['btc_dominance']}%
- 24h price change:    {metrics['price_change_24h']}%
- 7d price change:     {metrics['price_change_7d']}%
- Volume (24h USD):    {metrics['volume_24h_usd']:,.0f}
- Market cap (USD):    {metrics['market_cap_usd']:,.0f}
- Volume/MCap ratio:   {metrics['volume_market_ratio']}
  (interpretation: <0.04=low activity, 0.04-0.15=normal, >0.15=high activity)
- Exchange note:       {metrics['exchange_note']}

Analyze whale/institutional flow and produce your JSON output."""

        result = self._ask_claude_json(_SYSTEM, user_prompt)

        return AgentSignal(
            agent=self.name,
            asset=asset,
            timestamp=self._now(),
            signal=SignalType(result["signal"]),
            confidence=float(result["confidence"]),
            reasoning=result["reasoning"],
            metrics=metrics,
        )
