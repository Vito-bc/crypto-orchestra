"""
Whale / Market Positioning Agent.

Data sources (all free, no API keys required):
  1. OKX Funding Rate      — perpetual futures positioning (8h settlements)
  2. Binance Open Interest — rising/falling OI vs price = real vs fake moves
  3. Binance L/S Ratio    — contrarian retail sentiment
  4. Binance Funding Rate  — cross-exchange confirmation
  5. CoinGecko             — BTC dominance + volume/market-cap ratio

Open Interest is the most valuable signal:
  Rising OI + Rising Price  = real buying   → BUY
  Rising OI + Falling Price = real selling  → SELL
  Falling OI + Rising Price = short covering only → NEUTRAL (weak rally)
  Falling OI + Falling Price = liquidations → NEUTRAL (possible bottom)
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
from tools.market_positioning import get_open_interest, get_long_short_ratio, get_binance_funding_rate

_SYSTEM = """You are a crypto market positioning and whale flow analyst.

You receive four data sources ranked by reliability:
  1. Open Interest (OI) — MOST IMPORTANT: measures real money entering/leaving
  2. Funding Rates (OKX + Binance) — measures over-leveraging
  3. Long/Short Ratio — contrarian retail sentiment
  4. Volume/Market-cap + BTC Dominance — macro flow proxy

Output a JSON object with exactly these keys:
  signal      : "BUY" | "SELL" | "NEUTRAL"
  confidence  : float between 0.0 and 1.0
  reasoning   : one or two sentences

--- OPEN INTEREST RULES (highest priority) ---
- Rising OI + Rising price  → BUY, confidence 0.65-0.75 (real demand confirmed)
- Rising OI + Falling price → SELL, confidence 0.65-0.75 (real selling confirmed)
- Falling OI + Rising price → NEUTRAL, confidence 0.35 (just short covering, weak)
- Falling OI + Falling price → NEUTRAL, confidence 0.40 (liquidations, watch for bounce)

--- FUNDING RATE RULES (secondary, boosts/reduces OI confidence) ---
- Both OKX and Binance extreme positive (>+0.03%): add 0.10 to SELL confidence
- Both extreme negative (<-0.03%): add 0.10 to BUY confidence
- Rates diverge between exchanges: reduce confidence by 0.05 (uncertainty)

--- LONG/SHORT RATIO RULES (contrarian, tertiary) ---
- Retail >70% long: slight SELL lean (adds 0.05 to SELL) — crowd is usually wrong at extremes
- Retail >70% short: slight BUY lean (adds 0.05 to BUY) — short squeeze risk

--- WHEN OI DATA UNAVAILABLE ---
Fall back to funding rate as primary signal (original behavior).

Return ONLY the JSON object, no markdown, no extra text."""


class WhaleAgent(BaseAgent):
    name = AgentName.WHALE

    def analyze(self, asset: str) -> AgentSignal:
        # Fetch all sources in sequence (runner already parallelizes agents)
        metrics  = get_onchain_metrics(asset)
        okx_fund = get_funding_rate(asset)
        oi       = get_open_interest(asset)
        ls_ratio = get_long_short_ratio(asset)
        bin_fund = get_binance_funding_rate(asset)

        user_prompt = f"""Asset: {asset}

--- 1. OPEN INTEREST (Primary Signal) ---
OI USD:          ${oi.get('oi_usd', 0):,.0f}
OI 4h change:    {oi.get('oi_change_pct', 0):+.2f}%
OI trend:        {oi.get('oi_trend', 'unknown')}
Price vs OI:     {oi.get('price_vs_oi', 'N/A')}
OI signal:       {oi.get('signal', 'NEUTRAL')} (conf {oi.get('confidence', 0.3):.0%})
Interpretation:  {oi.get('interpretation', 'N/A')}
{f"OI error: {oi.get('error')}" if oi.get('error') else ''}

--- 2. FUNDING RATES (Secondary Signal) ---
OKX rate:        {okx_fund.get('current_rate_pct', 0):+.5f}%/8h  → {okx_fund.get('signal', 'NEUTRAL')}
Binance rate:    {bin_fund.get('rate_pct', 0):+.5f}%/8h  → {bin_fund.get('signal', 'NEUTRAL')}
OKX 24h avg:     {okx_fund.get('avg_24h_rate', 0):+.5f}%
OKX trend:       {okx_fund.get('trend', 'stable')}
OKX analysis:    {okx_fund.get('interpretation', 'N/A')}

--- 3. LONG/SHORT RATIO (Contrarian) ---
Retail long:     {ls_ratio.get('long_pct', 50):.1f}%
Retail short:    {ls_ratio.get('short_pct', 50):.1f}%
L/S signal:      {ls_ratio.get('signal', 'NEUTRAL')}
Analysis:        {ls_ratio.get('interpretation', 'N/A')}

--- 4. MACRO FLOW (Tertiary) ---
BTC dominance:   {metrics.get('btc_dominance', 'N/A')}%
Volume/MCap:     {metrics.get('volume_market_ratio', 'N/A')}
24h price chg:   {metrics.get('price_change_24h', 'N/A')}%
7d price chg:    {metrics.get('price_change_7d', 'N/A')}%
Exchange note:   {metrics.get('exchange_note', 'N/A')}

Analyze all four sources using the priority hierarchy and produce your JSON output."""

        result = self._ask_claude_json(_SYSTEM, user_prompt)

        return AgentSignal(
            agent=self.name,
            asset=asset,
            timestamp=self._now(),
            signal=SignalType(result["signal"]),
            confidence=float(result["confidence"]),
            reasoning=result["reasoning"],
            metrics={
                "oi_usd":            oi.get("oi_usd"),
                "oi_change_pct":     oi.get("oi_change_pct"),
                "oi_trend":          oi.get("oi_trend"),
                "oi_signal":         oi.get("signal"),
                "okx_funding_pct":   okx_fund.get("current_rate_pct"),
                "binance_funding_pct": bin_fund.get("rate_pct"),
                "long_pct":          ls_ratio.get("long_pct"),
                "short_pct":         ls_ratio.get("short_pct"),
                "ls_signal":         ls_ratio.get("signal"),
                "btc_dominance":     metrics.get("btc_dominance"),
                "volume_mcap_ratio": metrics.get("volume_market_ratio"),
            },
        )
