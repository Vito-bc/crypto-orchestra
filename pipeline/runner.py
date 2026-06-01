"""
Pipeline Runner — orchestrates all agents in parallel then calls the Orchestrator.

Flow:
  1. Check open positions — close any that hit stop/target/max-hold
  2. Check pending limit orders — fill or expire them against current price
     Filled orders → open a tracked position automatically
  3. All 5 sub-agents run concurrently via ThreadPoolExecutor
  4. Orchestrator produces a final TradeDecision
  5. BUY decisions → place a limit order at the nearest support level
     (maker fee 0.2% vs taker 0.4% — saves 0.4% per round trip)
  6. SELL decisions → Telegram alert (advisory)
  7. Decision logged to JSONL

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
from notifications.telegram  import (
    format_limit_order_filled,
    format_limit_order_placed,
    send_telegram_message,
)
from pipeline.limit_orders   import (
    MAKER_FEE_RATE,
    check_and_fill,
    get_open_orders,
    place_limit_order,
)
from pipeline.position_tracker import (
    check_positions,
    get_open_positions,
    open_position_from_order,
)
from schemas.signals         import AgentSignal, TradeAction, TradeDecision
from tools.price_data        import get_raw_df, get_snapshot
from tools.price_levels      import get_levels_from_snapshot

# Minimum candle body confirmation for SELL signals (BUY uses limit orders, no candle check needed)
_MOMENTUM_THRESHOLD = 0.003   # 0.3% — raised from 0.2%; diagnostics show winners avg +0.77%, losers avg -0.22%
# Maximum ATR distance to support where we'll still place a limit order
_MAX_DIST_TO_SUPPORT_ATR = 5.0

LOG_DIR       = ROOT / "logs"
DECISIONS_LOG = LOG_DIR / "agent_decisions.jsonl"


# ── Logging ───────────────────────────────────────────────────────────────────

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
                "agent":      s.agent.value,
                "signal":     s.signal.value,
                "confidence": s.confidence,
                "reasoning":  s.reasoning,
                "metrics":    s.metrics,
            }
            for s in signals
        ],
    }
    with DECISIONS_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def _log_order_event(asset: str, event_type: str, details: dict) -> None:
    """Append a limit-order lifecycle event to the decisions log."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    record = {
        "logged_at_utc": datetime.now(timezone.utc).isoformat(),
        "asset":         asset,
        "event":         event_type,
        **details,
    }
    with DECISIONS_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


# ── Console output ────────────────────────────────────────────────────────────

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
    if decision.action.value == "SELL":
        pass  # SELL is advisory only
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
        bar = "#" * int(v.confidence * 10)
        print(f"  {v.agent.value:<12} {v.signal.value:<7} {v.confidence:.0%}  [{bar:<10}]  w={v.weight_applied:.2f}")
    if decision.overrides:
        print("-" * 65)
        print("Overrides:")
        for o in decision.overrides:
            print(f"  - {o}")
    print("=" * 65)
    print(f"Decision log: {DECISIONS_LOG}")


# ── Position and order checks ─────────────────────────────────────────────────

def _check_open_positions(asset: str, current_price: float) -> None:
    """
    Check every open position for this asset against current price.
    Closes positions that hit their stop, target, or max-hold time.
    Sends a Telegram alert for each close.
    """
    from notifications.telegram import format_position_closed
    closed = check_positions(asset, current_price)
    for record in closed:
        _log_order_event(asset, "POSITION_CLOSED", record)
        send_telegram_message(format_position_closed(record))


def _check_pending_fills(asset: str, current_price: float) -> None:
    """
    Fill orders where current_price <= limit_price; expire stale orders.
    For each fill: open a tracked position and send a Telegram alert.
    """
    filled = check_and_fill(asset, current_price)
    for order in filled:
        print(f"[LimitOrder] FILLED #{order.id} — {asset} limit ${order.limit_price:,.2f}  "
              f"stop ${order.stop_price:,.2f}  target ${order.target_price:,.2f}")
        _log_order_event(asset, "LIMIT_ORDER_FILLED", {
            "order_id":     order.id,
            "limit_price":  order.limit_price,
            "stop_price":   order.stop_price,
            "target_price": order.target_price,
            "fill_price":   current_price,
            "maker_fee":    MAKER_FEE_RATE,
        })
        send_telegram_message(format_limit_order_filled(asset, order, current_price))
        # Open a tracked position so stop/target monitoring starts immediately
        from notifications.telegram import format_position_opened
        pos = open_position_from_order(order, current_price)
        _log_order_event(asset, "POSITION_OPENED", {
            "position_id": pos.id,
            "entry_price": pos.entry_price,
            "stop_price":  pos.stop_price,
            "target_price": pos.target_price,
            "qty_usd":     pos.qty_usd,
        })
        send_telegram_message(format_position_opened(pos))


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run_pipeline(asset: str = "ETH-USD") -> TradeDecision:
    """
    Run all sub-agents in parallel, then orchestrate a final decision.
    Returns the TradeDecision for downstream use.
    """
    print(f"\n[Orchestra] Starting pipeline for {asset} …")
    t0 = time.time()

    # ── 1. Check open positions + pending limit orders ────────────────────────
    snap0 = get_snapshot(asset)
    if snap0:
        current_price = snap0["close"]
        _check_open_positions(asset, current_price)   # stop/target/max-hold
        _check_pending_fills(asset, current_price)    # fills → open positions

    # ── 2. Run sub-agents concurrently ────────────────────────────────────────
    sub_agents = [
        TechnicalAgent(),
        MacroAgent(),
        SentimentAgent(),
        WhaleAgent(),
        RiskAgent(),
    ]
    signals: list[AgentSignal] = []

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

    # ── 3. Orchestrator final decision ────────────────────────────────────────
    orchestrator = OrchestratorAgent()
    decision     = orchestrator.decide(asset, signals)

    elapsed_total = time.time() - t0
    print(f"[Orchestra] Decision ready in {elapsed_total:.1f}s total")

    # ── 4. BUY → place limit order at support ─────────────────────────────────
    # Instead of a market entry, we queue a limit order at the nearest support
    # level. This earns the maker fee (0.2%) vs taker (0.4%), saving 0.4% per
    # round trip — the margin that was blocking backtest profitability.
    if decision.action == TradeAction.BUY:
        # Don't stack if there's already an open position OR a pending order for this asset
        open_pos = get_open_positions(asset)
        if open_pos:
            print(f"[LimitOrder] Already have open position for {asset} — skipping new order.")
            decision = TradeDecision(
                asset=asset, timestamp=decision.timestamp,
                action=TradeAction.HOLD,
                confidence=decision.confidence,
                reasoning=f"[Limit] Position already open for {asset}. " + decision.reasoning,
                votes=decision.votes, overrides=decision.overrides,
                veto_triggered=decision.veto_triggered, veto_reason=decision.veto_reason,
                position_size_pct=None, stop_loss_price=None, take_profit_price=None,
            )
        elif existing := get_open_orders(asset):
            first = existing[0]
            print(f"[LimitOrder] Already have open order #{first.id} at ${first.limit_price:,.2f} — skipping new order.")
            decision = TradeDecision(
                asset=asset, timestamp=decision.timestamp,
                action=TradeAction.HOLD,
                confidence=decision.confidence,
                reasoning=f"[Limit] Pending order #{first.id} already open at ${first.limit_price:,.2f}. " + decision.reasoning,
                votes=decision.votes, overrides=decision.overrides,
                veto_triggered=decision.veto_triggered, veto_reason=decision.veto_reason,
                position_size_pct=None, stop_loss_price=None, take_profit_price=None,
            )
        else:
            raw_df = get_raw_df(asset)
            if raw_df is not None:
                levels  = get_levels_from_snapshot(raw_df)
                support = levels.get("nearest_support")
                dist    = levels.get("dist_to_support")   # in ATR units
                atr     = levels.get("atr", 0.0)

                can_place = (
                    support is not None
                    and atr > 0
                    and (dist is None or dist <= _MAX_DIST_TO_SUPPORT_ATR)
                )

                if can_place:
                    order = place_limit_order(
                        asset=asset,
                        limit_price=support,
                        atr=atr,
                        position_size_pct=decision.position_size_pct,
                        reasoning=decision.reasoning,
                    )
                    dist_str = f"{dist:.1f}x ATR away" if dist else "at support"
                    print(f"[LimitOrder] PLACED #{order.id} — limit ${order.limit_price:,.2f}  "
                          f"stop ${order.stop_price:,.2f}  target ${order.target_price:,.2f}  "
                          f"({dist_str})  expires 24h")
                    _log_order_event(asset, "LIMIT_ORDER_PLACED", {
                        "order_id":    order.id,
                        "limit_price": order.limit_price,
                        "stop_price":  order.stop_price,
                        "target_price": order.target_price,
                        "dist_atr":    dist,
                        "maker_fee":   MAKER_FEE_RATE,
                        "reasoning":   order.reasoning,
                    })
                    send_telegram_message(format_limit_order_placed(asset, order, levels))
                    # Downgrade to HOLD — the live action is the pending order, not an immediate trade
                    decision = TradeDecision(
                        asset=asset, timestamp=decision.timestamp,
                        action=TradeAction.HOLD,
                        confidence=decision.confidence,
                        reasoning=f"[Limit] Order #{order.id} placed at support ${support:,.2f}. " + decision.reasoning,
                        votes=decision.votes, overrides=decision.overrides,
                        veto_triggered=decision.veto_triggered, veto_reason=decision.veto_reason,
                        position_size_pct=None, stop_loss_price=None, take_profit_price=None,
                    )
                else:
                    reason = (f"support ${support:,.2f} is {dist:.1f}x ATR away (max {_MAX_DIST_TO_SUPPORT_ATR}x)"
                              if support and dist else "no support level detected")
                    print(f"[LimitOrder] No order placed — {reason}.")
                    decision = TradeDecision(
                        asset=asset, timestamp=decision.timestamp,
                        action=TradeAction.HOLD,
                        confidence=decision.confidence * 0.6,
                        reasoning=f"[Limit] No viable support for limit order ({reason}). " + decision.reasoning,
                        votes=decision.votes, overrides=decision.overrides + [f"BUY blocked: {reason}."],
                        veto_triggered=decision.veto_triggered, veto_reason=decision.veto_reason,
                        position_size_pct=None, stop_loss_price=None, take_profit_price=None,
                    )

    # ── 5. SELL momentum filter ───────────────────────────────────────────────
    # BUY is now handled by limit orders, so momentum filter only applies to SELL.
    if decision.action == TradeAction.SELL:
        snap = get_snapshot(asset)
        if snap:
            candle_body = (snap["close"] - snap["open"]) / snap["open"]
            if candle_body > -_MOMENTUM_THRESHOLD:
                print(f"[Filter] Momentum check FAILED for SELL — candle body {candle_body:+.3%} > -{_MOMENTUM_THRESHOLD:.1%}. Downgrading to HOLD.")
                decision = TradeDecision(
                    asset=asset, timestamp=decision.timestamp,
                    action=TradeAction.HOLD,
                    confidence=decision.confidence * 0.7,
                    reasoning=f"[Momentum] Candle {candle_body:+.3%} does not confirm SELL (need <-{_MOMENTUM_THRESHOLD:.1%}). " + decision.reasoning,
                    votes=decision.votes,
                    overrides=decision.overrides + [f"SELL → HOLD: candle {candle_body:+.3%}."],
                    veto_triggered=decision.veto_triggered, veto_reason=decision.veto_reason,
                    position_size_pct=None, stop_loss_price=None, take_profit_price=None,
                )
            else:
                print(f"[Filter] Momentum check PASSED — candle body {candle_body:+.3%} confirms SELL.")

    # ── 6. Log + notify ───────────────────────────────────────────────────────
    _log_decision(asset, signals, decision)
    _print_decision(asset, signals, decision)

    if decision.action == TradeAction.SELL:
        send_telegram_message(_format_telegram(asset, decision))

    return decision


ASSETS = ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]


def run_all_assets() -> dict[str, TradeDecision]:
    """Run the full pipeline for every configured asset sequentially."""
    results = {}
    for asset in ASSETS:
        print(f"\n{'='*65}")
        decision = run_pipeline(asset)
        results[asset] = decision
    return results


if __name__ == "__main__":
    # When launched via pythonw.exe (Task Scheduler, no console), redirect
    # stdout+stderr to scheduler.log so runs are always visible in the log file.
    import os as _os
    _is_pythonw = _os.path.basename(sys.executable).lower() == "pythonw.exe"
    if _is_pythonw:
        import io
        _log_path = ROOT / "logs" / "scheduler.log"
        _log_path.parent.mkdir(exist_ok=True)
        _log_fh = open(_log_path, "a", encoding="utf-8", buffering=1)
        sys.stdout = io.TextIOWrapper(_log_fh.buffer, encoding="utf-8", line_buffering=True)
        sys.stderr = sys.stdout

    if len(sys.argv) > 1:
        run_pipeline(sys.argv[1])
    else:
        run_all_assets()
