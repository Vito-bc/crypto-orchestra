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
from datetime import datetime, timezone
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
    TradeAction,
    TradeDecision,
)

_SYSTEM = """You are the chief trading decision engine for Crypto Orchestra.

You receive structured JSON reports from 6 specialist agents AND a pre-computed composite
confidence score. The composite score is the primary quantitative signal — Python enforces
the action based on it. Your role is qualitative: detect vetoes, explain conflicts, extract
risk levels.

Agents (directional voters):
  - macro      : market regime (BULL/BEAR/RANGING) — hard veto authority; BTC dominance
  - technical  : RSI, MACD, Bollinger Bands, EMA trend, ADX trend-strength, VWAP, CVD
  - whale      : OI, funding rates, L/S ratio, Coinbase premium (institutional)
  - news       : asset-specific news — delisting, hack, regulatory; CRITICAL VETO authority
  - sentiment  : Fear & Greed + general crypto headlines
  - breakout   : deterministic EMA50 crossover detector (first 1-4 candles only, no LLM)

Gate agents (not in composite score, enforced as hard gates before order):
  - risk       : position sizing, stop/target levels, ATR volatility gate — ok_to_trade=false → HOLD

Output a JSON object with exactly these keys:
  action            : "BUY" | "SELL" | "HOLD"  (your recommendation; Python may override via score)
  confidence        : float 0.0-1.0  (reflect the composite score level provided)
  reasoning         : 2-3 sentences explaining key signals, conflicts, and your recommendation
  position_size_pct : float (take from risk agent output; Python will scale it by score tier)
  stop_loss_price   : float (from risk agent, or null)
  take_profit_price : float (from risk agent, or null)
  veto_triggered    : true | false
  veto_reason       : string or null
  overrides         : list of strings noting any conflicts or signals you are discounting

Rules — follow in strict order:
1. If macro=FULL_BEAR (no local recovery) → veto_triggered=true, action=HOLD.
   If macro=LOCAL_RALLY (BEAR + local recovery) → allow BUY with threshold 0.65 and 50% size
   (or 0.45 if breakout_mode=true in the prompt).
2. If risk agent ok_to_trade=false → action=HOLD.
3. Use the composite score AND the regime label AND breakout_mode flag as your primary signals:
   BULL/RANGING normal:   score >= 0.45 → recommend BUY
   BULL/RANGING breakout: score >= 0.35 → recommend BUY (early momentum entry)
   LOCAL_RALLY normal:    score >= 0.65 → recommend BUY
   LOCAL_RALLY breakout:  score >= 0.45 → recommend BUY (breakout in bear local rally)
   score <= -0.35 → recommend SELL (any regime)
   between        → recommend HOLD
4. When breakout_mode=true: mention "Early EMA50 breakout entry" in reasoning.
5. A high composite score (>= 0.75) means strong alignment — Python will automatically
   scale position size up to 2x. Do not inflate position_size_pct yourself.
6. In LOCAL_RALLY mode: note the bear market context in reasoning. Python halves size automatically.
7. Confidence should mirror the composite score (e.g. score 0.60 → confidence ~0.60).

Return ONLY the JSON object, no markdown, no extra text."""

# ── Scoring constants ──────────────────────────────────────────────────────────
_AGENT_WEIGHTS: dict = {
    "macro":     0.26,
    "technical": 0.21,
    "whale":     0.17,
    "news":      0.12,
    "sentiment": 0.08,
    "breakout":  0.16,   # early EMA50 crossover detector — fires only in 1-4 candle window
    "risk":      0.00,   # GATE ONLY — ok_to_trade enforced as hard veto, not a directional vote
}
_BUY_THRESHOLD  =  0.45   # composite score floor for BUY
_SELL_THRESHOLD = -0.35   # composite score ceiling for SELL

# position_size_pct = risk_agent_base × multiplier, capped at _MAX_POSITION_PCT
_SIZE_TIERS: list[tuple[float, float]] = [
    (0.75, 2.00),   # very strong alignment → 2× base size
    (0.65, 1.50),   # strong alignment      → 1.5×
    (0.55, 1.25),   # good alignment        → 1.25×
    (0.45, 1.00),   # threshold             → 1× (base)
]
_MAX_POSITION_PCT = 0.12  # hard cap: never exceed 12% of portfolio per trade


class OrchestratorAgent:
    def __init__(self) -> None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY is not set in .env")
        self.model  = os.getenv("ORCHESTRATOR_MODEL", "claude-sonnet-4-6")
        self.client = anthropic.Anthropic(api_key=api_key)

    def decide(self, asset: str, signals: list[AgentSignal]) -> TradeDecision:
        # ── Pre-check: macro regime tier ─────────────────────────────────────
        # Three tiers instead of a binary veto:
        #   BULL/RANGING  → threshold 0.45, 100% size  (normal)
        #   LOCAL_RALLY   → threshold 0.65,  50% size  (bear + local recovery)
        #   FULL_BEAR     → hard veto, no entries       (bear, no recovery signal)
        macro_signal   = next((s for s in signals if s.agent == AgentName.MACRO), None)
        macro_bear     = macro_signal is not None and macro_signal.regime == MarketRegime.BEAR
        local_recovery = (
            macro_signal is not None
            and macro_signal.metrics is not None
            and bool(macro_signal.metrics.get("local_recovery", False))
        )

        if macro_bear and local_recovery:
            # Bear market but price > EMA50_4h or 7d > +5% — allow with higher bar.
            # Threshold lowered from 0.65 to 0.45: macro (w=0.26) votes SELL in LOCAL_RALLY,
            # making 0.65 arithmetically unreachable (max composite ~0.58). The BEAR context
            # is already captured by 50% size reduction and the regime label itself.
            veto               = False
            _eff_buy_threshold = 0.45
            _eff_size_mult     = 0.50
            _regime_label      = "LOCAL_RALLY"
            print("[Orchestrator] LOCAL_RALLY mode — BEAR regime but local recovery detected. "
                  "Threshold 0.45 (fixed from 0.65 — macro SELL weight made 0.65 unreachable), size 50%.")
        elif macro_bear:
            # Full bear, no local recovery — hard veto
            veto               = True
            _eff_buy_threshold = _BUY_THRESHOLD
            _eff_size_mult     = 1.0
            _regime_label      = "FULL_BEAR"
        else:
            # Bull or ranging — normal operation
            veto               = False
            _eff_buy_threshold = _BUY_THRESHOLD
            _eff_size_mult     = 1.0
            _regime_label      = "BULL"

        # ── Breakout mode: lower threshold when EMA50 just crossed (1-4 candles) ─
        # BreakoutAgent fires BUY only in the first 4 candles after EMA50 crossover.
        # When it fires, we can enter early before RSI overbought + VWAP extension.
        from schemas.signals import SignalType as _ST_early
        breakout_signal = next((s for s in signals if s.agent == AgentName.BREAKOUT), None)
        _breakout_active = (
            not veto
            and breakout_signal is not None
            and breakout_signal.signal == _ST_early.BUY
            and breakout_signal.confidence >= 0.60
        )
        if _breakout_active:
            if _regime_label == "LOCAL_RALLY":
                _eff_buy_threshold = 0.30   # reduced from 0.45 (LOCAL_RALLY breakout)
            else:
                _eff_buy_threshold = 0.35   # reduced from 0.45 (BULL/RANGING)
            print(
                f"[Orchestrator] BREAKOUT MODE — EMA50 crossover {breakout_signal.metrics.get('candles_above_ema50', '?')} "
                f"candle(s) ago, conf={breakout_signal.confidence:.0%}. "
                f"Threshold -> {_eff_buy_threshold:.2f} ({_regime_label})"
            )

        # ── Pre-check: asset news critical veto ───────────────────────────────
        news_signal = next((s for s in signals if s.agent == AgentName.NEWS), None)
        news_critical_veto = (
            news_signal is not None
            and news_signal.metrics is not None
            and bool(news_signal.metrics.get("critical_veto", False))
        )
        if news_critical_veto:
            veto_reason = news_signal.metrics.get("veto_reason") or news_signal.reasoning
            print(f"[Orchestrator] NEWS CRITICAL VETO — {veto_reason}")
            return TradeDecision(
                asset=asset,
                timestamp=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
                action=TradeAction("HOLD"),
                confidence=0.0,
                reasoning=f"[NewsVeto] {veto_reason}",
                votes=[],
                overrides=[f"News critical veto: {veto_reason}"],
                veto_triggered=True,
                veto_reason=veto_reason,
            )

        # ── Risk agent constraints ────────────────────────────────────────────
        risk_signal = next((s for s in signals if s.agent == AgentName.RISK), None)
        ok_to_trade = True
        if risk_signal and risk_signal.metrics:
            ok_to_trade = bool(risk_signal.metrics.get("ok_to_trade", True))

        if not ok_to_trade:
            veto_msg = (
                risk_signal.reasoning if risk_signal and risk_signal.reasoning
                else "Risk agent blocked trade"
            )
            return TradeDecision(
                asset=asset,
                timestamp=datetime.now(timezone.utc),
                action=TradeAction("HOLD"),
                confidence=0.0,
                reasoning=f"[RiskVeto] {veto_msg}",
                votes=[],
                overrides=["Risk agent hard veto: ok_to_trade=False"],
                veto_triggered=True,
                veto_reason=veto_msg,
            )

        # ── Composite score (computed in Python; passed to LLM for context) ───
        # Score = Σ(confidence × weight × direction) for all agents
        # BUY=+1, SELL=-1, NEUTRAL=0. Range: -1.0 to +1.0.
        from schemas.signals import SignalType as _ST
        composite_score = sum(
            s.confidence * _AGENT_WEIGHTS.get(s.agent.value, 0.10) * (
                1 if s.signal == _ST.BUY else -1 if s.signal == _ST.SELL else 0
            )
            for s in signals
        )

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

        _breakout_threshold_note = (
            f"BREAKOUT MODE: threshold lowered to {_eff_buy_threshold:.2f} (EMA50 crossover {breakout_signal.metrics.get('candles_above_ema50', '?')} candle(s) ago)"
            if _breakout_active else "normal"
        )
        user_prompt = f"""Asset: {asset}
Macro regime:      {_regime_label}  (FULL_BEAR=hard veto | LOCAL_RALLY=threshold 0.65 size 50% | BULL=normal)
Macro veto active: {veto}
Risk ok_to_trade:  {ok_to_trade}
Breakout mode:     {_breakout_threshold_note}
Composite score:   {composite_score:+.3f}  (BUY >= {_eff_buy_threshold} · SELL <= {_SELL_THRESHOLD} · else HOLD)

Agent reports:
{json.dumps(reports, indent=2)}

Detect vetoes, explain key signals and conflicts, extract stop/target from risk agent."""

        from agents.base_agent import BaseAgent as _Base

        retry_suffix = (
            "\n\nIMPORTANT: Your previous response could not be parsed as JSON. "
            "Return ONLY a raw JSON object — no markdown, no explanation, just the JSON."
        )
        last_exc: Exception = RuntimeError("No attempts made")
        result: dict = {}

        for attempt in range(2):
            prompt = user_prompt if attempt == 0 else user_prompt + retry_suffix
            message = self.client.messages.create(
                model=self.model,
                max_tokens=1024,
                temperature=0,
                system=_SYSTEM,
                messages=[{"role": "user", "content": prompt}],
            )
            try:
                result = _Base._extract_json(message.content[0].text)
                break
            except (json.JSONDecodeError, ValueError) as exc:
                last_exc = exc
                if attempt == 0:
                    print("[Orchestrator] JSON parse failed, retrying...", file=__import__("sys").stderr)
        else:
            raise ValueError(f"Orchestrator returned invalid JSON after 2 attempts: {last_exc}")

        # Sanitize action field
        _valid_actions = {"BUY", "SELL", "HOLD"}
        raw_action = str(result.get("action", "HOLD")).upper().strip()
        result["action"] = raw_action if raw_action in _valid_actions else "HOLD"

        # Sanitize confidence
        try:
            result["confidence"] = max(0.0, min(1.0, float(result.get("confidence", 0.3))))
        except (TypeError, ValueError):
            result["confidence"] = 0.3

        # Sanitize reasoning
        if not isinstance(result.get("reasoning"), str) or not result["reasoning"].strip():
            result["reasoning"] = "No reasoning provided."

        # ── Score-based action enforcement (replaces hard vote count) ────────
        proposed = result.get("action", "HOLD")
        if proposed == "BUY" and composite_score < _eff_buy_threshold:
            result["action"] = "HOLD"
            result["reasoning"] = (
                f"[Score {composite_score:+.3f} < {_eff_buy_threshold} ({_regime_label})] "
                "Composite score below BUY threshold. "
                + result.get("reasoning", "")
            )
        elif proposed == "SELL" and composite_score > _SELL_THRESHOLD:
            result["action"] = "HOLD"
            result["reasoning"] = (
                f"[Score {composite_score:+.3f} > {_SELL_THRESHOLD}] Composite score above SELL threshold. "
                + result.get("reasoning", "")
            )

        # ── BTC Dominance altcoin multiplier ─────────────────────────────────
        alt_mult = 1.0
        if macro_signal and macro_signal.metrics and asset != "BTC-USD":
            alt_mult = float(macro_signal.metrics.get("altcoin_multiplier", 1.0))
            if alt_mult != 1.0:
                print(
                    f"[Orchestrator] BTC dominance {macro_signal.metrics.get('btc_dominance', '?'):.1f}% "
                    f"-> alt multiplier {alt_mult:.2f}x for {asset}"
                )

        # ── Confidence-scaled position sizing ─────────────────────────────────
        # Higher composite score → bigger position, up to 2× base, capped at 12%.
        # Then apply altcoin_multiplier from BTC dominance regime.
        if result.get("action") == "BUY":
            base_size  = float(result.get("position_size_pct") or 0.05)
            multiplier = next(
                (mult for min_s, mult in _SIZE_TIERS if composite_score >= min_s),
                1.0,
            )
            # _eff_size_mult = 0.5 in LOCAL_RALLY mode (bear market risk reduction)
            scaled_size = min(base_size * multiplier * alt_mult * _eff_size_mult, _MAX_POSITION_PCT)
            result["position_size_pct"] = round(scaled_size, 4)
            size_info = (
                f" [LOCAL_RALLY: size x{_eff_size_mult}]" if _eff_size_mult < 1.0 else ""
            )
            if multiplier > 1.0 or alt_mult != 1.0 or _eff_size_mult < 1.0:
                print(
                    f"[Orchestrator] Score {composite_score:+.2f} -> {multiplier:.2f}x score, "
                    f"{alt_mult:.2f}x BTC-dom, {_eff_size_mult:.2f}x regime: "
                    f"{base_size:.1%} -> {scaled_size:.1%}"
                )
                result["reasoning"] += (
                    f" [Size {multiplier:.2f}x(score) x {alt_mult:.2f}x(dom)"
                    f"{size_info} -> {scaled_size:.1%}]"
                )

        # ── Build votes list ──────────────────────────────────────────────────
        votes = [
            AgentVote(
                agent=s.agent,
                signal=s.signal,
                confidence=s.confidence,
                weight_applied=_AGENT_WEIGHTS.get(s.agent.value, 0.1),
            )
            for s in signals
        ]

        return TradeDecision(
            asset=asset,
            timestamp=datetime.now(timezone.utc),
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
