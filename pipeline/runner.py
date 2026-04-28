"""
Pipeline Runner — orchestrates all agents in parallel then calls the Orchestrator.

Flow:
  1. All 5 sub-agents run concurrently via ThreadPoolExecutor
  2. Results are collected and passed to the OrchestratorAgent
  3. TradeDecision is logged to JSONL and printed
  4. If action is BUY or SELL, Telegram alert is sent

Run for one asset:
    python pipeline/runner.py ETH-USD
Run for all assets (default):
    python pipeline/runner.py
"""

from __future__ import annotations

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.macro_agent      import MacroAgent
from agents.orchestrator     import OrchestratorAgent
from agents.risk_agent       import RiskAgent
from agents.sentiment_agent  import SentimentAgent
from agents.technical_agent  import TechnicalAgent
from agents.whale_agent      import WhaleAgent
from notifications.telegram  import send_telegram_message
from schemas.signals         import AgentSignal, TradeAction, TradeDecision
from tools.price_data        import get_snapshot

# Minimum candle body confirmation: current 1h close must be moving
# in the same direction as the signal before we act.
_MOMENTUM_THRESHOLD = 0.002   # 0.2% candle body required

LOG_DIR       = ROOT / "logs"
DECISIONS_LOG = LOG_DIR / "agent_decisions.jsonl"


def _log_decision(asset: str, signals: list[AgentSignal], decision: TradeDecision) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    record = {
        "logged_at_utc": datetime.now(timezone.utc).isoformat(),
        "asset":         asset,
        "action":        decision.action.value,
        "confidence":    decision.confidence,
        "reasoning":     decision.reasoning,
        "veto_triggered": decision.veto_triggered,
        "veto_reason":   decision.veto_reason,
        "overrides":     decision.overrides,
        "position_size_pct":  decision.position_size_pct,
        "stop_loss_price":    decision.stop_loss_price,
        "take_profit_price":  decision.take_profit_price,
        "votes": [
            {
                "agent":          v.agent.value,
                "signal":         v.signal.value,
                "confidence":     v.confidence,
                "weight_applied": v.weight_applied,
            }
            for v in decision.votes
        ],
        "agent_signals": [
            {
                "agent":     s.agent.value,
                "signal":    s.signal.value,
                "confidence": s.confidence,
                "reasoning": s.reasoning,
                "metrics":   s.metrics,
            }
            for s in signals
        ],
    }
    with DECISIONS_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def _format_telegram(asset: str, decision: TradeDecision) -> str:
    lines = [
        "Crypto Orchestra — Agent Decision",
        f"Asset:      {asset}",
        f"Action:     {decision.action.value}",
        f"Confidence: {decision.confidence:.0%}",
        f"Reasoning:  {decision.reasoning}",
    ]
    if decision.veto_triggered:
        lines.append(f"VETO: {decision.veto_reason}")
    if decision.action.value == "BUY":
        lines += [
            f"Size:       {decision.position_size_pct:.1%}" if decision.position_size_pct else "",
            f"Stop:       {decision.stop_loss_price:.2f}"   if decision.stop_loss_price   else "",
            f"Target:     {decision.take_profit_price:.2f}" if decision.take_profit_price else "",
        ]
    if decision.overrides:
        lines.append("Overrides:  " + "; ".join(decision.overrides))
    return "\n".join(l for l in lines if l)


def _print_decision(asset: str, signals: list[AgentSignal], decision: TradeDecision) -> None:
    print("\n" + "=" * 65)
    print("CRYPTO ORCHESTRA — AGENT DECISION")
    print("=" * 65)
    print(f"Asset:       {asset}")
    print(f"Action:      {decision.action.value}")
    print(f"Confidence:  {decision.confidence:.0%}")
    if decision.veto_triggered:
        print(f"VETO:        {decision.veto_reason}")
    print(f"Reasoning:   {decision.reasoning}")
    print("-" * 65)
    print("Agent Votes:")
    for v in decision.votes:
        bar   = "#" * int(v.confidence * 10)
        print(f"  {v.agent.value:<12} {v.signal.value:<7} {v.confidence:.0%}  [{bar:<10}]  w={v.weight_applied:.2f}")
    if decision.overrides:
        print("-" * 65)
        print("Overrides:")
        for o in decision.overrides:
            print(f"  - {o}")
    if decision.action.value == "BUY":
        print("-" * 65)
        print(f"Position size: {decision.position_size_pct:.1%}" if decision.position_size_pct else "")
        print(f"Stop loss:     {decision.stop_loss_price:.2f}"   if decision.stop_loss_price   else "")
        print(f"Take profit:   {decision.take_profit_price:.2f}" if decision.take_profit_price else "")
    print("=" * 65)
    print(f"Decision log: {DECISIONS_LOG}")


def run_pipeline(asset: str = "ETH-USD") -> TradeDecision:
    """
    Run all sub-agents in parallel, then orchestrate a final decision.
    Returns the TradeDecision for downstream use.
    """
    print(f"\n[Orchestra] Starting pipeline for {asset} …")
    t0 = time.time()

    sub_agents = [
        TechnicalAgent(),
        MacroAgent(),
        SentimentAgent(),
        WhaleAgent(),
        RiskAgent(),
    ]

    signals: list[AgentSignal] = []

    # ── Run all sub-agents concurrently ───────────────────────────────────────
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(agent.run, asset): agent.name for agent in sub_agents}
        for future in as_completed(futures):
            agent_name = futures[future]
            try:
                signal = future.result()
                signals.append(signal)
                print(f"  [{agent_name.value:<12}] {signal.signal.value:<7}  conf={signal.confidence:.0%}")
            except Exception as exc:
                print(f"  [{agent_name.value:<12}] ERROR: {exc}")

    elapsed_agents = time.time() - t0
    print(f"[Orchestra] All agents done in {elapsed_agents:.1f}s — calling orchestrator …")

    # ── Orchestrator makes the final call ─────────────────────────────────────
    orchestrator = OrchestratorAgent()
    decision     = orchestrator.decide(asset, signals)

    elapsed_total = time.time() - t0
    print(f"[Orchestra] Decision ready in {elapsed_total:.1f}s total")

    # ── Entry momentum filter ─────────────────────────────────────────────────
    # Only act if the current 1h candle is already moving in signal direction.
    # Prevents entering right as momentum is reversing.
    if decision.action in (TradeAction.BUY, TradeAction.SELL):
        snap = get_snapshot(asset)
        if snap:
            candle_body = (snap["close"] - snap["open"]) / snap["open"]
            if decision.action == TradeAction.BUY and candle_body < _MOMENTUM_THRESHOLD:
                print(f"[Filter] Momentum check FAILED for BUY — candle body {candle_body:+.3%} < +{_MOMENTUM_THRESHOLD:.1%}. Downgrading to HOLD.")
                decision = TradeDecision(
                    asset=asset,
                    timestamp=decision.timestamp,
                    action=TradeAction.HOLD,
                    confidence=decision.confidence * 0.7,
                    reasoning=f"[Momentum filter] Candle body {candle_body:+.3%} does not confirm BUY direction (need >{_MOMENTUM_THRESHOLD:.1%}). " + decision.reasoning,
                    votes=decision.votes,
                    overrides=decision.overrides + [f"BUY downgraded to HOLD: candle body {candle_body:+.3%} insufficient."],
                    veto_triggered=decision.veto_triggered,
                    veto_reason=decision.veto_reason,
                    position_size_pct=None,
                    stop_loss_price=None,
                    take_profit_price=None,
                )
            elif decision.action == TradeAction.SELL and candle_body > -_MOMENTUM_THRESHOLD:
                print(f"[Filter] Momentum check FAILED for SELL — candle body {candle_body:+.3%} > -{_MOMENTUM_THRESHOLD:.1%}. Downgrading to HOLD.")
                decision = TradeDecision(
                    asset=asset,
                    timestamp=decision.timestamp,
                    action=TradeAction.HOLD,
                    confidence=decision.confidence * 0.7,
                    reasoning=f"[Momentum filter] Candle body {candle_body:+.3%} does not confirm SELL direction (need <-{_MOMENTUM_THRESHOLD:.1%}). " + decision.reasoning,
                    votes=decision.votes,
                    overrides=decision.overrides + [f"SELL downgraded to HOLD: candle body {candle_body:+.3%} insufficient."],
                    veto_triggered=decision.veto_triggered,
                    veto_reason=decision.veto_reason,
                    position_size_pct=None,
                    stop_loss_price=None,
                    take_profit_price=None,
                )
            else:
                print(f"[Filter] Momentum check PASSED — candle body {candle_body:+.3%} confirms {decision.action.value}.")

    # ── Log + notify ──────────────────────────────────────────────────────────
    _log_decision(asset, signals, decision)
    _print_decision(asset, signals, decision)

    if decision.action.value in ("BUY", "SELL"):
        send_telegram_message(_format_telegram(asset, decision))

    return decision


ASSETS = ["BTC-USD", "ETH-USD"]


def run_all_assets() -> dict[str, TradeDecision]:
    """
    Run the full pipeline for every configured asset sequentially.
    Returns a dict of asset → TradeDecision.
    """
    results = {}
    for asset in ASSETS:
        print(f"\n{'='*65}")
        decision = run_pipeline(asset)
        results[asset] = decision
    return results


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_pipeline(sys.argv[1])
    else:
        run_all_assets()
