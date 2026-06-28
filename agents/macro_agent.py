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
from tools.onchain_data import get_onchain_metrics, get_dxy_signal

_SYSTEM = """You are a macro market regime analyst for cryptocurrency markets.

You receive 4-hour trend data, market structure metrics, and Bitcoin Dominance.
Your job is to classify the current market regime and produce a directional signal.

Output a JSON object with exactly these keys:
  signal             : "BUY" | "SELL" | "NEUTRAL"
  regime             : "BULL" | "BEAR" | "RANGING" | "UNKNOWN"
  confidence         : float between 0.0 and 1.0
  reasoning          : one or two sentences
  altcoin_multiplier : float (1.0 = normal, 0.7 = reduce alt positions, 1.3 = favour alts)

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

Bitcoin Dominance rules for altcoin_multiplier (apply always, for all non-BTC assets):
- BTC dominance > 60%  → altcoin_multiplier = 0.7  (BTC season — capital flows TO BTC, not alts)
- BTC dominance 54–60% → altcoin_multiplier = 0.85 (slight BTC preference)
- BTC dominance 48–54% → altcoin_multiplier = 1.0  (neutral, balanced rotation)
- BTC dominance < 48%  → altcoin_multiplier = 1.3  (altcoin season — capital rotating to alts)
For BTC-USD itself: always use altcoin_multiplier = 1.0 regardless of dominance.

- Return ONLY the JSON object, no markdown, no extra text."""


class MacroAgent(BaseAgent):
    name = AgentName.MACRO

    def analyze(self, asset: str) -> AgentSignal:
        snapshot = get_snapshot(asset)
        metrics  = get_onchain_metrics(asset)
        dxy      = get_dxy_signal()

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

        btc_dom = metrics.get("btc_dominance", 0.0) or 0.0
        dom_note = (
            "BTC SEASON — capital flows to BTC, reduce alt positions"  if btc_dom > 60 else
            "Slight BTC preference"                                      if btc_dom > 54 else
            "Neutral rotation"                                           if btc_dom > 48 else
            "ALTCOIN SEASON — capital rotating to alts"
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
- BTC dominance:     {btc_dom:.1f}%  → {dom_note}
- 24h price change:  {metrics.get('price_change_24h', 'N/A')}%
- 7d price change:   {metrics.get('price_change_7d', 'N/A')}%
- Volume/MCap ratio: {metrics.get('volume_market_ratio', 'N/A')}
- Market note:       {metrics.get('exchange_note', 'N/A')}

DXY (US Dollar Index) — macro headwind/tailwind:
- DXY current:       {dxy.get('dxy_value', 'N/A')}
- DXY 5d change:     {dxy.get('dxy_change_5d', 'N/A'):+.2f}%  → trend: {dxy.get('trend', 'unknown')}
- DXY signal:        {dxy.get('signal', 'NEUTRAL')}
- DXY analysis:      {dxy.get('interpretation', 'N/A')}
{f"DXY error: {dxy.get('error')}" if dxy.get('error') else ''}

Note: DXY rising = headwind for ALL crypto (correlation -0.72 with BTC over 30d).
Factor DXY into your regime classification — even if 4H trend is BULL, a strongly
rising DXY should lower confidence or push to RANGING if BTC trend is borderline.

Classify the regime and produce your JSON output (include altcoin_multiplier)."""

        result = self._ask_claude_json(_SYSTEM, user_prompt)

        alt_mult = float(result.get("altcoin_multiplier", 1.0))
        # Clamp to sane range in case LLM hallucinates
        alt_mult = max(0.5, min(1.5, alt_mult))

        # Detect local recovery within a bear regime (deterministic, not LLM)
        # True when price has reclaimed the 4h EMA50 OR sustained 7-day upswing > +5%
        close_4h  = float(snapshot.get("close_4h") or 0)
        ema50_4h  = float(snapshot.get("ema50_4h") or 0)
        change_7d = float(metrics.get("price_change_7d") or 0)
        local_recovery = bool(
            (close_4h > 0 and ema50_4h > 0 and close_4h > ema50_4h)
            or change_7d > 5.0
        )

        return AgentSignal(
            agent=self.name,
            asset=asset,
            timestamp=self._now(),
            signal=SignalType(result["signal"]),
            confidence=float(result["confidence"]),
            reasoning=result["reasoning"],
            regime=MarketRegime(result.get("regime", "UNKNOWN")),
            metrics={
                "trend_4h":            snapshot.get("trend_4h"),
                "btc_dominance":       btc_dom,
                "altcoin_multiplier":  alt_mult,
                "change_24h":          metrics.get("price_change_24h"),
                "change_7d":           metrics.get("price_change_7d"),
                "dxy_value":           dxy.get("dxy_value"),
                "dxy_change_5d":       dxy.get("dxy_change_5d"),
                "dxy_trend":           dxy.get("trend"),
                "local_recovery":      local_recovery,
            },
        )
