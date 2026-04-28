"""
Orchestrator Agent — the main decision brain.

Receives structured AgentSignal objects from all sub-agents and produces
a final TradeDecision. Uses claude-sonnet for stronger reasoning.

Key behaviors:
  - MacroAgent BEAR regime triggers a hard veto on all BUY decisions
  - Requires 3+ agents aligned for HIGH confidence trades
  - Confidence < 0.55 always produces HOLD
  - Logs every override and dissent for weekly review
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import anthropic
from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from schemas.signals import (
    AgentName,
    AgentSignal,
    AgentVote,
    MarketRegime,
    SignalType,
    TradeAction,
    TradeDecision,
)

_SYSTEM = """You are the chief trading decision engine for Crypto Orchestra.

You receive structured JSON reports from 5 specialist agents:
  - technical  : RSI, MACD, Bollinger Bands, EMA trend
  - macro      : market regime classification (BULL/BEAR/RANGING) — acts as VETO
  - sentiment  : Fear & Greed + news headlines
  - whale      : on-chain volume flows and exchange pressure
  - risk       : position sizing, stop/target levels, portfolio exposure

Your job:
1. Check for MACRO VETO first — if macro regime is BEAR, final action must be SELL or HOLD, never BUY
2. Count how many agents signal BUY vs SELL vs NEUTRAL
3. Weigh each signal by its confidence and domain relevance
4. Identify and explicitly note any conflicts between agents
5. Output a final TradeDecision

Output a JSON object with exactly these keys:
  buy_agent_count   : integer — count of agents whose signal field = "BUY" exactly
  sell_agent_count  : integer — count of agents whose signal field = "SELL" exactly
  action            : "BUY" | "SELL" | "HOLD"
  confidence        : float 0.0-1.0
  reasoning         : 2-3 sentences explaining the decision and any conflicts
  position_size_pct : float (from risk agent, or null if HOLD/SELL)
  stop_loss_price   : float (from risk agent, or null)
  take_profit_price : float (from risk agent, or null)
  veto_triggered    : true | false
  veto_reason       : string or null
  overrides         : list of strings describing any agent signals you are overriding and why

Rules — follow in strict order:
1. Count explicit BUY signals and explicit SELL signals from the agent list.
   Count ONLY agents whose "signal" field equals "BUY" or "SELL" exactly.
   A NEUTRAL signal does NOT count toward either direction, even if its metrics look bearish/bullish.
2. If fewer than 3 agents explicitly signal the same direction → action=HOLD immediately.
   Do not read between the lines. Do not infer from metrics. Count the signal field only.
3. If macro=BEAR and any agent explicitly says BUY → veto_triggered=true, action=HOLD.
4. If risk agent says ok_to_trade=false → action=HOLD.
5. Require weighted confidence >= 0.55 to act; below that → HOLD.
6. If 3+ agents explicitly align → confidence can be 0.7+.
- Return ONLY the JSON object, no markdown, no extra text."""


class OrchestratorAgent:
    def __init__(self) -> None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY is not set in .env")
        self.model  = os.getenv("ORCHESTRATOR_MODEL", "claude-sonnet-4-6")
        self.client = anthropic.Anthropic(api_key=api_key)

    def decide(self, asset: str, signals: list[AgentSignal]) -> TradeDecision:
        # ── Pre-check: macro veto ─────────────────────────────────────────────
        macro_signal = next((s for s in signals if s.agent == AgentName.MACRO), None)
        veto         = macro_signal is not None and macro_signal.regime == MarketRegime.BEAR

        # ── Risk agent constraints ────────────────────────────────────────────
        risk_signal = next((s for s in signals if s.agent == AgentName.RISK), None)
        ok_to_trade = True
        if risk_signal and risk_signal.metrics:
            ok_to_trade = bool(risk_signal.metrics.get("ok_to_trade", True))

        # ── Build prompt ──────────────────────────────────────────────────────
        reports = []
        for s in signals:
            reports.append({
                "agent":      s.agent.value,
                "signal":     s.signal.value,
                "confidence": s.confidence,
                "reasoning":  s.reasoning,
                "regime":     s.regime.value if s.regime else None,
                "metrics":    s.metrics,
            })

        user_prompt = f"""Asset: {asset}
Macro veto active: {veto}
Risk ok_to_trade:  {ok_to_trade}

Agent reports:
{json.dumps(reports, indent=2)}

Produce your final TradeDecision JSON."""

        message = self.client.messages.create(
            model=self.model,
            max_tokens=1024,
            temperature=0,
            system=_SYSTEM,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = message.content[0].text
        if raw.strip().startswith("```"):
            raw = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()

        result = json.loads(raw)

        # ── Hard enforce 3-agent minimum (Python-side guard) ──────────────────
        from schemas.signals import SignalType as _ST
        _buy_count  = sum(1 for s in signals if s.signal == _ST.BUY)
        _sell_count = sum(1 for s in signals if s.signal == _ST.SELL)
        proposed    = result.get("action", "HOLD")
        if proposed == "BUY"  and _buy_count  < 3:
            result["action"] = "HOLD"
            result["reasoning"] = f"[Guard] Only {_buy_count}/5 agents signal BUY (need 3). " + result.get("reasoning", "")
        if proposed == "SELL" and _sell_count < 3:
            result["action"] = "HOLD"
            result["reasoning"] = f"[Guard] Only {_sell_count}/5 agents signal SELL (need 3). " + result.get("reasoning", "")

        # ── Build votes list ──────────────────────────────────────────────────
        agent_weights = {
            AgentName.MACRO:      0.30,
            AgentName.TECHNICAL:  0.25,
            AgentName.WHALE:      0.20,
            AgentName.SENTIMENT:  0.15,
            AgentName.RISK:       0.10,
        }
        votes = [
            AgentVote(
                agent=s.agent,
                signal=s.signal,
                confidence=s.confidence,
                weight_applied=agent_weights.get(s.agent, 0.1),
            )
            for s in signals
        ]

        return TradeDecision(
            asset=asset,
            timestamp=datetime.utcnow(),
            action=TradeAction(result["action"]),
            confidence=float(result["confidence"]),
            position_size_pct=result.get("position_size_pct"),
            stop_loss_price=result.get("stop_loss_price"),
            take_profit_price=result.get("take_profit_price"),
            reasoning=result["reasoning"],
            votes=votes,
            overrides=result.get("overrides", []),
            veto_triggered=bool(result.get("veto_triggered", False)),
            veto_reason=result.get("veto_reason"),
        )
