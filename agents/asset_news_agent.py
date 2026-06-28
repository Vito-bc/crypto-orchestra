"""
Asset News Agent.

Monitors news headlines specifically for the traded asset (BTC/ETH/SOL/ZEC),
not general crypto market news.

Focuses on:
  - Regulatory risks: delisting threats, SEC actions, country bans
  - Protocol risks: hacks, exploits, vulnerabilities
  - Positive catalysts: ETF approvals, exchange listings, institutional adoption
  - ZEC-specific: privacy coin regulatory pressure

Can trigger a CRITICAL VETO (blocks BUY regardless of other agents) when a
delisting, hack, or regulatory ban is detected in the last 24-48 hours.

Sources: Google News RSS, Reddit (asset subreddits), CryptoPanic (optional key).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.base_agent import BaseAgent
from schemas.signals import AgentName, AgentSignal, SignalType
from tools.asset_news import get_asset_headlines

_SYSTEM = """You are an asset-specific news risk analyst for a crypto trading system.

You receive recent news headlines specifically about a single crypto asset.
Your job is to identify whether any news events should influence the trading decision.

Output a JSON object with exactly these keys:
  signal       : "BUY" | "SELL" | "NEUTRAL"
  confidence   : float between 0.0 and 1.0
  reasoning    : one or two sentences
  critical_veto: true | false  (true ONLY for confirmed delisting, hack, or regulatory ban)
  veto_reason  : string or null

Critical veto rules (set critical_veto=true only for high-confidence events):
- Exchange delisting announcement for this specific asset → SELL, critical_veto=true
- Confirmed security exploit or hack of the protocol → SELL, critical_veto=true
- Country/regulatory ban specifically targeting this asset → SELL, critical_veto=true
- These must be confirmed news, not rumours or speculative headlines

Positive signal rules:
- ETF approval, major exchange listing, institutional adoption announcement → BUY lean
- Protocol upgrade, mainnet launch, major partnership → mild BUY lean
- confidence 0.60+ when catalyst is confirmed and specific

Neutral rules:
- General crypto market news that isn't asset-specific → NEUTRAL
- Speculative or unclear headlines → NEUTRAL, confidence 0.3
- No significant news → NEUTRAL, confidence 0.2

ZEC/Zcash specific:
- Privacy coin regulatory pressure is an ongoing risk — treat exchange delisting headlines
  with high seriousness (OKX already delisted ZEC/XMR in early 2025)
- Any mention of "privacy coin" + "ban" or "delist" → SELL, potentially critical_veto=true

Return ONLY the JSON object, no markdown, no extra text."""


class AssetNewsAgent(BaseAgent):
    name = AgentName.NEWS

    def analyze(self, asset: str) -> AgentSignal:
        news = get_asset_headlines(asset, limit=10)

        ages = news.get("headline_ages_days", [])
        headlines_text = (
            "\n".join(
                f"  [{f'{a:.1f}d ago' if a is not None else 'age unknown'}] {h}"
                for h, a in zip(news["headlines"], ages + [None] * len(news["headlines"]))
            )
            if news["headlines"]
            else "  (no recent headlines in last 7 days)"
        )

        flags_text = (
            "NEGATIVE FLAGS DETECTED: " + ", ".join(news["negative_flags"])
            if news["negative_flags"]
            else "No negative keywords detected."
        )

        signals_text = (
            "POSITIVE SIGNALS DETECTED: " + ", ".join(news["positive_signals"])
            if news["positive_signals"]
            else "No positive keywords detected."
        )

        crit_age = news.get("oldest_critical_age_days")
        critical_note = (
            f"*** CRITICAL ALERT: delisting/hack/ban keywords found in article "
            f"{'from ' + str(crit_age) + ' days ago' if crit_age else '(date unknown)'} ***"
            if news["critical_alert"]
            else ""
        )

        user_prompt = f"""Asset: {asset}
Sources: {', '.join(news['sources']) if news['sources'] else 'none'}
{f"Data error: {news['error']}" if news.get('error') else ''}

Recent asset-specific headlines:
{headlines_text}

Keyword analysis:
{flags_text}
{signals_text}
{critical_note}

Analyze these asset-specific headlines and produce your JSON output.
Focus on events that specifically affect {asset}, not general crypto sentiment."""

        result = self._ask_claude_json(_SYSTEM, user_prompt)

        # If critical_veto is triggered, override confidence to high
        critical = bool(result.get("critical_veto", False))
        if critical and result.get("signal") == "SELL":
            result["confidence"] = max(float(result.get("confidence", 0.7)), 0.75)

        return AgentSignal(
            agent=self.name,
            asset=asset,
            timestamp=self._now(),
            signal=SignalType(result.get("signal", "NEUTRAL")),
            confidence=float(result.get("confidence", 0.3)),
            reasoning=result.get("reasoning", "No reasoning provided."),
            metrics={
                "headline_count":   len(news["headlines"]),
                "negative_flags":   news["negative_flags"],
                "positive_signals": news["positive_signals"],
                "critical_alert":   news["critical_alert"],
                "critical_veto":    critical,
                "veto_reason":      result.get("veto_reason"),
                "sources":          news["sources"],
            },
        )
