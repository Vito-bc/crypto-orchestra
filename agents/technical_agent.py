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
- NEUTRAL: mixed signals, price mid-range, OR flat market (ADX < 20)

ADX (Average Directional Index) — trend strength filter (MANDATORY):
- ADX < 20  → market is RANGING/FLAT → output NEUTRAL regardless of other signals
             (false signals dominate in flat markets; momentum indicators lie)
- ADX 20–35 → trending but moderate → normal signal rules apply
- ADX > 35  → strong trend → increase confidence by 0.05-0.10, favour trend-following signals
- ADX > 50  → very strong trend → maximum trend-following confidence, be cautious of reversals

VWAP rules (Volume-Weighted Average Price — daily session anchor):
- Price > VWAP by > +1.5%  → price extended above fair value → bearish lean, reduce BUY confidence
- Price > VWAP by 0–1.5%   → healthy bull position above value area → no change
- Price < VWAP by 0–1.5%   → slight discount → mild BUY lean if other signals agree
- Price < VWAP by > -1.5%  → deep discount / possible breakdown → check CVD before buying

CVD rules (Cumulative Volume Delta — net buy/sell pressure over 24h):
- CVD rising + price rising     → confirmed uptrend (buyers increasing) → BUY confidence +0.05
- CVD falling + price rising    → BEARISH DIVERGENCE — price move unconfirmed by volume; lower BUY confidence by 0.10
- CVD rising + price falling    → BULLISH DIVERGENCE — sellers exhausted; supports BUY, raise confidence +0.05
- CVD falling + price falling   → confirmed downtrend → SELL or NEUTRAL
- CVD near zero or flat         → indecision, no edge from volume

Divergence is the most important CVD signal — when price and CVD move in OPPOSITE directions, trust CVD over price.

Critical rule for confidence:
- If price is AT a known support AND indicators suggest BUY AND ADX ≥ 20 → confidence 0.70-0.85
  (support acts as a floor, stop placement is clean, risk/reward improves)
- If price is AT known resistance AND you would otherwise BUY → NEUTRAL instead
  (price hitting resistance = high reversal risk, bad timing to enter long)
- If price is mid-range (not near any S/R level) → cap confidence at 0.55
  (mid-range entries have poor risk/reward, price can go either way)
- If indicators are mixed → NEUTRAL regardless of S/R
- If ADX < 20 → NEUTRAL always (overrides all other rules)

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

        adx = snapshot.get("adx_1h", 0.0)
        adx_regime = (
            "FLAT/RANGING (< 20 — momentum signals unreliable)"  if adx < 20  else
            "WEAK TREND (20–35)"                                  if adx < 35  else
            "STRONG TREND (35–50)"                                if adx < 50  else
            "VERY STRONG TREND (> 50)"
        )

        # VWAP analysis
        price   = snapshot["close"]
        vwap    = snapshot.get("vwap_1h")
        vwap_pct = ((price - vwap) / vwap * 100) if vwap else None
        vwap_str = (
            f"{vwap:.2f}  ({vwap_pct:+.2f}% from VWAP — "
            f"{'ABOVE' if vwap_pct >= 0 else 'BELOW'} daily fair value)"
            if vwap and vwap_pct is not None else "N/A"
        )

        # CVD trend: compare current 24h CVD to 6h ago
        cvd_now  = snapshot.get("cvd_24h")
        cvd_6h   = snapshot.get("cvd_6h_ago")
        if cvd_now is not None and cvd_6h is not None:
            cvd_trend = "RISING" if cvd_now > cvd_6h else "FALLING"
            cvd_div   = ""
            if cvd_trend == "FALLING" and vwap_pct is not None and vwap_pct > 0:
                cvd_div = " [!] BEARISH DIVERGENCE: price above VWAP but CVD falling"
            elif cvd_trend == "RISING" and vwap_pct is not None and vwap_pct < 0:
                cvd_div = " [+] BULLISH DIVERGENCE: price below VWAP but CVD rising"
            cvd_str = f"{cvd_now:+.0f}  (6h ago: {cvd_6h:+.0f}) -> {cvd_trend}{cvd_div}"
        elif cvd_now is not None:
            cvd_str = f"{cvd_now:+.0f}  (trend: unknown — 6h ago data missing)"
        else:
            cvd_str = "N/A"

        user_prompt = f"""Asset: {asset}

--- TECHNICAL INDICATORS ---
Close price:       {price:.2f}
RSI (1h):          {snapshot['rsi_1h']:.2f}
MACD diff (1h):    {snapshot['macd_diff_1h']:.6f}
BB % (1h):         {snapshot['bb_pct_1h']:.3f}   (0=lower band, 1=upper band)
Volume ratio (1h): {snapshot['volume_ratio_1h']:.3f}
EMA50 (1h):        {snapshot['ema50_1h']:.2f}
EMA200 (1h):       {snapshot['ema200_1h']:.2f}
ADX (1h):          {adx:.1f}  → {adx_regime}
Close (4h):        {snapshot.get('close_4h', 'N/A')}
EMA50 (4h):        {snapshot.get('ema50_4h', 'N/A')}
EMA200 (4h):       {snapshot.get('ema200_4h', 'N/A')}
4h trend:          {snapshot.get('trend_4h', 'N/A')}
4h trend strength: {snapshot.get('trend_strength_4h', 'N/A')}

Rule engine signal: {snapshot['signal']}
Alignment: trend={snapshot['trend_ok']} macd={snapshot['macd_ok']} rsi={snapshot['rsi_ok']} bb={snapshot['bb_ok']} vol={snapshot['volume_ok']}

--- VWAP & CVD ---
VWAP (1h daily):   {vwap_str}
CVD 24h:           {cvd_str}

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
                "adx":            snapshot.get("adx_1h", 0.0),
                "rule_signal":    snapshot["signal"],
                "at_support":     at_sup,
                "at_resistance":  at_res,
                "nearest_support":    levels.get("nearest_support"),
                "nearest_resistance": levels.get("nearest_resistance"),
                "vwap":           vwap,
                "vwap_pct":       vwap_pct,
                "cvd_24h":        cvd_now,
                "cvd_6h_ago":     cvd_6h,
            },
        )
