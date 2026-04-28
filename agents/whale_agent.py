"""
Whale / On-Chain Agent.

PRIMARY signal: OKX perpetual futures funding rate (free public API).
  - Extreme positive funding → crowded longs → SELL lean
  - Extreme negative funding → crowded shorts → BUY lean
  - Neutral funding → no positioning edge

SECONDARY signal: CoinGecko volume/market-cap ratio + BTC dominance.

Funding rates are the single most reliable crypto-specific signal for
detecting over-leveraged market positioning before a reversal.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.base_agent import BaseAgent
from schemas.signals import AgentName, AgentSignal, SignalType
from tools.onchain_data import get_onchain_metrics
from tools.funding_data import get_funding_rate

_SYSTEM = """You are a crypto market positioning and whale flow analyst.

You receive two data sources:
  1. Perpetual futures funding rate (primary) — directly measures market over-leveraging
  2. Volume/market-cap ratio + BTC dominance (secondary) — proxies large-player flow

Output a JSON object with exactly these keys:
  signal      : "BUY" | "SELL" | "NEUTRAL"
  confidence  : float between 0.0 and 1.0
  reasoning   : one or two sentences

Funding rate rules (highest priority):
- Extreme positive funding (>+0.03%/8h): market crowded long → SELL, confidence 0.75+
- Moderate positive funding (+0.01% to +0.03%): slight caution → NEUTRAL or mild SELL lean
- Neutral (-0.01% to +0.01%): no edge from positioning → defer to volume metrics
- Moderate negative funding (-0.03% to -0.01%): slight short squeeze risk → NEUTRAL or mild BUY lean
- Extreme negative funding (<-0.03%/8h): market crowded short → BUY, confidence 0.75+

Volume/dominance rules (secondary, applies when funding is neutral):
- High volume sell-off (vol/mcap > 0.15 + price -3%+) → SELL, confidence 0.65+
- High volume rally (vol/mcap > 0.15 + price +3%+)    → BUY, confidence 0.65+
- Rising BTC dominance (>55%) while analyzing altcoin  → SELL (capital rotating to BTC)
- Falling BTC dominance (<42%)                         → BUY lean for altcoins
- Low volume (<0.04 ratio)                             → NEUTRAL, confidence 0.3

When funding is non-neutral, it overrides volume signals.
Return ONLY the JSON object, no markdown, no extra text."""


class WhaleAgent(BaseAgent):
    name = AgentName.WHALE

    def analyze(self, asset: str) -> AgentSignal:
        metrics  = get_onchain_metrics(asset)
        funding  = get_funding_rate(asset)

        funding_error = funding.get("error")
        funding_rate  = funding.get("current_rate_pct", 0.0)
        funding_avg   = funding.get("avg_24h_rate", 0.0)
        funding_trend = funding.get("trend", "stable")
        funding_interp = funding.get("interpretation", "No data.")

        user_prompt = f"""Asset: {asset}

--- FUNDING RATE (Primary Signal) ---
Current rate:     {funding_rate:+.5f}% per 8h
24h avg rate:     {funding_avg:+.5f}% per 8h
Trend:            {funding_trend}
Pre-analysis:     {funding_interp}
{f'Data error:       {funding_error}' if funding_error else ''}

--- MARKET FLOW METRICS (Secondary Signal) ---
BTC dominance:    {metrics.get('btc_dominance', 'N/A')}%
24h price change: {metrics.get('price_change_24h', 'N/A')}%
7d price change:  {metrics.get('price_change_7d', 'N/A')}%
Volume/MCap:      {metrics.get('volume_market_ratio', 'N/A')}
Exchange note:    {metrics.get('exchange_note', 'N/A')}

Analyze market positioning and produce your JSON output."""

        result = self._ask_claude_json(_SYSTEM, user_prompt)

        return AgentSignal(
            agent=self.name,
            asset=asset,
            timestamp=self._now(),
            signal=SignalType(result["signal"]),
            confidence=float(result["confidence"]),
            reasoning=result["reasoning"],
            metrics={
                "funding_rate_pct":   funding_rate,
                "funding_avg_24h":    funding_avg,
                "funding_trend":      funding_trend,
                "funding_signal":     funding.get("signal"),
                "funding_strength":   funding.get("signal_strength"),
                "btc_dominance":      metrics.get("btc_dominance"),
                "volume_mcap_ratio":  metrics.get("volume_market_ratio"),
                "price_change_24h":   metrics.get("price_change_24h"),
            },
        )
