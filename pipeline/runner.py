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
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.asset_news_agent import AssetNewsAgent
from agents.breakout_agent   import BreakoutAgent
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
    cancel_open_orders,
    check_and_fill,
    get_open_orders,
    place_limit_order,
)
from pipeline.exit_executor import run_exit_executor
from pipeline.position_tracker import (
    TRADE_HISTORY,
    count_recent_stops,
    get_open_positions,
    open_position_from_order,
)
from schemas.signals         import AgentSignal, TradeAction, TradeDecision
from tools.price_data        import get_daily_trend, get_raw_df, get_snapshot
from tools.price_levels      import get_levels_from_snapshot
from backtesting.signal_scanner import scan_latest

# Minimum candle body confirmation for SELL signals (BUY uses limit orders, no candle check needed)
_MOMENTUM_THRESHOLD = 0.003   # 0.3% — raised from 0.2%; diagnostics show winners avg +0.77%, losers avg -0.22%
# Maximum ATR distance to support where we'll still place a limit order
_MAX_DIST_TO_SUPPORT_ATR = 5.0

# ── Entry filters (falling-knife protection) ──────────────────────────────────
# ATR-based bounce confirmation: price must recover 1.5x ATR above stop-exit before re-entry.
# Replaces fixed 2% which was too tight for high-vol assets (ZEC 50% crash) and too loose for BTC.
_BOUNCE_CONFIRMATION_ATR = 1.5
_VELOCITY_VETO_PCT           = -5.0  # block long entry if asset down > 5% in last 24h
_WHIPSAW_STOP_LIMIT          = 2     # block entry if this many stops hit in lookback window
_WHIPSAW_LOOKBACK_H          = 96    # 4-day window: whipsaw clusters span days at trend tops
_CORR_FULL_VETO_THRESHOLD    = 0.65  # 30d corr >= this → BTC BEAR veto applies in full
_CORR_PARTIAL_VETO_THRESHOLD = 0.35  # 30d corr >= this → 50% size cut, entry still allowed
_FUNDING_BLOCK_ANNUALIZED    = 20.0  # block long if OKX funding > 20% annualized (leverage chase)

LOG_DIR       = ROOT / "logs"
DECISIONS_LOG = LOG_DIR / "agent_decisions.jsonl"
_SIGNALS_DB   = LOG_DIR / "signals.db"               # SQLite idempotency store

# Hold extension thresholds (condition-based exit research: Liu & Tsyvinski 2021)
_HOLD_EXT_MIN_SCORE = 3   # out of 5 conditions required to extend
_HOLD_EXT_MIN_ADX   = 20  # minimum ADX — don't extend in choppy/ranging markets
_HOLD_EXT_ATR_MULT  = 1.5 # ATR multiplier for extension trailing stop below HWM


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


# ── Idempotency (SQLite) ──────────────────────────────────────────────────────
# SQLite gives us atomic INSERT with UNIQUE constraint — two concurrent processes
# cannot both claim the same signal_id.
# State machine: claimed → completed.
# Stale claims (>2h old) are eligible for recovery so a crash doesn't permanently
# lose a signal.

import sqlite3 as _sqlite3

def _make_signal_id(asset: str, candle_close_time: str) -> str:
    return f"{asset}:{candle_close_time}:v3"


def _ensure_signals_db() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with _sqlite3.connect(_SIGNALS_DB) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_signals (
                signal_id    TEXT PRIMARY KEY,
                asset        TEXT NOT NULL,
                candle_close TEXT NOT NULL,
                status       TEXT NOT NULL DEFAULT 'claimed',
                claimed_at   TEXT,
                completed_at TEXT
            )
        """)


def _claim_signal(signal_id: str, asset: str, candle_close: str) -> bool:
    """
    Atomically claim this signal_id. Returns True if successfully claimed,
    False if it is already claimed/completed (idempotency gate).
    Stale 'claimed' entries older than 2 hours are treated as recoverable.
    """
    _ensure_signals_db()
    now = datetime.now(timezone.utc).isoformat()
    # First, clean up any stale claims for this specific signal_id
    stale_cutoff = (datetime.now(timezone.utc).timestamp() - 7200)
    with _sqlite3.connect(_SIGNALS_DB) as conn:
        conn.execute(
            "DELETE FROM processed_signals "
            "WHERE signal_id=? AND status='claimed' "
            "AND CAST(strftime('%s', claimed_at) AS INTEGER) < ?",
            (signal_id, stale_cutoff),
        )
    try:
        with _sqlite3.connect(_SIGNALS_DB) as conn:
            conn.execute(
                "INSERT INTO processed_signals "
                "(signal_id, asset, candle_close, status, claimed_at) "
                "VALUES (?, ?, ?, 'claimed', ?)",
                (signal_id, asset, candle_close, now),
            )
        return True
    except _sqlite3.IntegrityError:
        return False  # UNIQUE violation — already claimed or completed


def _complete_signal(signal_id: str) -> None:
    """Mark a successfully processed signal as completed."""
    now = datetime.now(timezone.utc).isoformat()
    with _sqlite3.connect(_SIGNALS_DB) as conn:
        conn.execute(
            "UPDATE processed_signals SET status='completed', completed_at=? "
            "WHERE signal_id=?",
            (now, signal_id),
        )


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

def _quick_hold_eval(asset: str, pos: "Position") -> tuple[int, float]:  # type: ignore[name-defined]
    """
    Lightweight 5-condition check for hold extension decisions. No LLM — pure indicators.
    Returns (conditions_met_count, adx).

    Conditions:
      1. Price above 1h EMA50               (trend intact)
      2. RSI > 48                            (not turning bearish)
      3. MACD diff > 0                       (positive momentum)
      4. CVD 24h > 0                         (net buying pressure)
      5. Price >= entry price                (position not in loss)
    """
    snap = get_snapshot(asset)
    if not snap:
        return 0, 0.0
    close = snap["close"]
    score = 0
    if close > snap.get("ema50_1h", close + 1):          score += 1
    if (snap.get("rsi_1h") or 0) > 48:                   score += 1
    if (snap.get("macd_diff_1h") or 0) > 0:              score += 1
    if (snap.get("cvd_24h") or 0) > 0:                   score += 1
    if close >= pos.entry_price:                          score += 1
    adx = float(snap.get("adx_1h") or 0)
    return score, adx


def _check_open_positions(asset: str, current_price: float) -> None:
    """
    Check every open ledger position for this asset against current price.
    At MAX_HOLD expiry: re-evaluates conditions (score + ADX) before closing.
    If 3+ conditions met and ADX >= 20 → extend 8h with ATR trailing stop.
    Places SELL orders via the two-transaction exit outbox for triggered exits.
    """
    from exchange.coinbase_client import place_market_sell

    def _coinbase_sell_fn(order_id: str, sell_asset: str, qty_base: float) -> str:
        return place_market_sell(
            product_id=sell_asset,
            base_size_coins=qty_base,
            client_order_id=order_id,
        )

    def _on_extension_review(pos) -> bool:
        # pos is a SimpleNamespace wrapping a ledger Row — attribute access works.
        score, adx = _quick_hold_eval(asset, pos)
        if score >= _HOLD_EXT_MIN_SCORE and adx >= _HOLD_EXT_MIN_ADX:
            snap = get_snapshot(asset)
            if snap and snap.get("atr_1h") and (pos.high_water_mark or 0) > 0:
                atr = snap["atr_1h"]
                ext_stop = round(pos.high_water_mark - _HOLD_EXT_ATR_MULT * atr, 2)
                pos.extension_trailing_stop = max(ext_stop, pos.stop_price)
            else:
                pos.extension_trailing_stop = None
            print(
                f"[HoldExt] {asset} EXTEND — score {score}/5  ADX {adx:.0f}>={_HOLD_EXT_MIN_ADX}"
                + (f"  ext-stop ${pos.extension_trailing_stop:,.2f}"
                   if pos.extension_trailing_stop else "")
            )
            return True
        print(
            f"[HoldExt] {asset} CLOSE — score {score}/5  ADX {adx:.0f}  "
            f"(need {_HOLD_EXT_MIN_SCORE}/5 + ADX>={_HOLD_EXT_MIN_ADX})"
        )
        return False

    actions = run_exit_executor(
        asset=asset,
        current_price=current_price,
        coinbase_sell_fn=_coinbase_sell_fn,
        on_extension_review=_on_extension_review,
    )

    for action in actions:
        result = action.get("result")
        reason = action.get("exit_reason")
        pos_id = action["position_id"]
        if result is not None:
            if result.status == "OPEN":
                evt = "EXIT_ORDER_OPEN"
                print(
                    f"[ExitExecutor] EXIT OPEN {asset}  pos={pos_id[:8]}"
                    f"  reason={reason}  order={result.order_id[:8]}"
                    f"  exch={result.exchange_order_id}"
                )
            elif result.status == "SUBMITTING":
                evt = "EXIT_ORDER_SUBMITTING_AMBIGUOUS"
                alert = (
                    f"[ALERT] EXIT network error — order SUBMITTING, reconciler will retry\n"
                    f"asset={asset} pos={pos_id[:8]} reason={reason} order={result.order_id[:8]}"
                )
                print(alert)
                send_telegram_message(alert)
            else:
                evt = "EXIT_ORDER_REJECTED"
                alert = (
                    f"[CRITICAL] EXIT REJECTED — stop-loss NOT executed, manual review required\n"
                    f"asset={asset} pos={pos_id[:8]} reason={reason}\n"
                    f"rejection={result.rejection_reason}"
                )
                print(alert)
                send_telegram_message(alert)
            _log_order_event(asset, evt, {
                "position_id":       pos_id,
                "exit_reason":       reason,
                "order_id":          result.order_id,
                "exchange_order_id": result.exchange_order_id,
                "status":            result.status,
                "rejection_reason":  result.rejection_reason,
            })
        elif action.get("error"):
            alert = (
                f"[CRITICAL] EXIT placement failed — position unprotected\n"
                f"asset={asset} pos={pos_id[:8]} reason={reason}\nerror={action['error']}"
            )
            print(alert)
            send_telegram_message(alert)
            _log_order_event(asset, "EXIT_PLACEMENT_FAILED", {
                "position_id": pos_id,
                "exit_reason": reason,
                "error":       action["error"],
            })
        elif action.get("note") and action["note"] != "active_exit_already_exists":
            print(f"[ExitExecutor] {action['note'].upper()} {asset}  pos={pos_id[:8]}")


def _check_pending_fills(asset: str, current_price: float) -> None:
    """
    Fill orders where current_price <= limit_price; expire stale orders.
    For each fill: open a tracked position and send a Telegram alert.

    Pre-fill guard: entry filters are re-checked at the actual fill price
    before the order executes. This catches the case where the order was
    placed at a higher price (bounce check passed), but price drifted below
    the bounce threshold by the time it fills at the support level.
    If any filter fails at fill time, the order is cancelled instead.
    """
    # Re-validate entry filters at fill price before executing
    if get_open_orders(asset):
        _fill_ok, _fill_reason, _ = _check_entry_filters(asset)
        if not _fill_ok:
            n = cancel_open_orders(asset)
            print(f"[PreFillGuard] CANCELLED {n} order(s) for {asset} — {_fill_reason}")
            _log_order_event(asset, "LIMIT_ORDER_CANCELLED", {
                "reason":              f"Pre-fill guard: {_fill_reason}",
                "current_price":       current_price,
            })
            return

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


# ── Entry filters (falling-knife protection) ──────────────────────────────────

def _calc_btc_correlation(asset: str, hours: int = 720) -> float | None:
    """
    Rolling 30-day Pearson correlation of hourly returns between `asset` and BTC.
    Uses the existing price-data TTL cache — no extra network calls after the
    first pipeline run for that asset.
    Returns None when data is insufficient (< 100 hours).
    """
    asset_df = get_raw_df(asset)
    btc_df   = get_raw_df("BTC-USD")
    if asset_df is None or btc_df is None:
        return None
    try:
        a_ret = asset_df["close"].iloc[-hours:].pct_change().dropna()
        b_ret = btc_df["close"].iloc[-hours:].pct_change().dropna()
        n = min(len(a_ret), len(b_ret))
        if n < 100:
            return None
        corr = float(a_ret.iloc[-n:].corr(b_ret.iloc[-n:]))
        return None if (corr != corr) else corr  # guard NaN
    except Exception:
        return None


def _check_entry_filters(asset: str) -> tuple[bool, str, float]:
    """
    Pre-BUY guards that prevent bad entries.
    Returns (allowed, reason_if_blocked, position_size_modifier).

    position_size_modifier is 1.0 normally, 0.5 when partially vetoed
    (BTC BEAR + partial correlation — entry allowed but size halved).

    1. Correlation-adjusted BTC BEAR veto
       corr >= 0.65       → full block (BTC regime dominates)
       corr 0.35–0.65     → 50% size cut, entry allowed
       corr < 0.35 or neg → BTC BEAR veto lifted (asset decorrelated)

    2. Funding rate leverage veto
       OKX annualized funding > 20% → block (crowded longs, unwind risk)

    3. Bounce confirmation after stop loss
       Price must recover +1.5x ATR above stop-exit price before re-entry.

    4. Velocity veto
       Asset down > 5% in last 24h → no long entry.
    """
    from backtesting.signal_scanner import ASSET_CONFIG as _SCANNER_CFG
    from tools.market_positioning import get_okx_funding_rate

    size_modifier = 1.0
    _asset_cfg = _SCANNER_CFG.get(asset, {})

    # 1. Correlation-adjusted BTC BEAR veto (skip for BTC itself and assets with btc_regime_filter=False)
    if asset != "BTC-USD" and _asset_cfg.get("btc_regime_filter", True):
        btc = get_snapshot("BTC-USD")
        if btc and btc.get("trend_4h") == "bear":
            corr = _calc_btc_correlation(asset)
            corr_str = f"{corr:.2f}" if corr is not None else "unknown"
            if corr is None or corr >= _CORR_FULL_VETO_THRESHOLD:
                return False, (
                    f"BTC BEAR veto — {asset} 30d correlation {corr_str} "
                    f"(>={_CORR_FULL_VETO_THRESHOLD}); BTC bear regime applies"
                ), 1.0
            elif corr >= _CORR_PARTIAL_VETO_THRESHOLD:
                size_modifier = 0.5
                print(f"[EntryFilter] BTC BEAR partial veto — {asset} corr {corr_str}, "
                      "position size reduced to 50%")
            else:
                print(f"[EntryFilter] BTC BEAR veto LIFTED for {asset} — "
                      f"30d correlation {corr_str} (decorrelated from BTC)")
    elif asset != "BTC-USD":
        print(f"[EntryFilter] BTC correlation veto skipped for {asset} (btc_regime_filter=False)")

    # 2. Funding rate leverage veto (skips assets with no OKX perp, e.g. ZEC)
    funding = get_okx_funding_rate(asset)
    if not funding.get("error"):
        ann = funding.get("annualized_pct", 0.0)
        if ann > _FUNDING_BLOCK_ANNUALIZED:
            return False, (
                f"Funding rate veto — {asset} funding {ann:.1f}% annualized "
                f"(>{_FUNDING_BLOCK_ANNUALIZED:.0f}%); leveraged long crowd, wait for unwind"
            ), size_modifier

    # 3. Bounce confirmation after stop loss (ATR-based, not fixed %)
    if TRADE_HISTORY.exists():
        with open(TRADE_HISTORY) as fh:
            lines = fh.readlines()
        for line in reversed(lines[-30:]):
            try:
                rec = json.loads(line.strip())
            except Exception:
                continue
            if rec.get("asset") == asset and rec.get("reason") == "STOP_LOSS":
                stop_exit_price = rec["exit_price"]
                snap = get_snapshot(asset)
                if snap:
                    current     = snap["close"]
                    atr         = snap["atr_1h"]
                    required    = stop_exit_price + _BOUNCE_CONFIRMATION_ATR * atr
                    bounce_atr  = (current - stop_exit_price) / atr if atr else 0
                    if current < required:
                        return False, (
                            f"Bounce confirmation needed — last stop exit ${stop_exit_price:.2f}, "
                            f"current ${current:.2f} ({bounce_atr:+.2f}x ATR); "
                            f"need +{_BOUNCE_CONFIRMATION_ATR}x ATR (${required:.2f}) above stop exit"
                        ), size_modifier
                break

    # 4. Velocity veto — price falling > 5% in last 24h
    raw_df = get_raw_df(asset)
    if raw_df is not None and len(raw_df) >= 25:
        close_now = float(raw_df["close"].iloc[-1])
        close_24h = float(raw_df["close"].iloc[-25])
        chg_24h   = (close_now - close_24h) / close_24h * 100
        if chg_24h <= _VELOCITY_VETO_PCT:
            return False, (
                f"Velocity veto — {asset} down {chg_24h:.1f}% in 24h; "
                "no long entry into active distribution"
            ), size_modifier

    # 5. Daily EMA trend filter — asset-specific period (50 or 200 day).
    # Backtesting: mean-reversion signals in daily downtrends are net losers.
    # ETH uses 50EMA (faster), ZEC uses 200EMA (slower, low-liquidity asset).
    daily = get_daily_trend(asset)
    if daily:
        daily_period = _DAILY_EMA_PERIOD.get(asset, 200)
        c1d      = daily.get("close_1d")
        ema_key  = f"ema{daily_period}_1d"
        daily_ma = daily.get(ema_key)
        if c1d is not None and daily_ma is not None and c1d < daily_ma:
            return False, (
                f"Daily {daily_period}EMA veto — {asset} daily close ${c1d:,.2f} < "
                f"{daily_period}-day EMA ${daily_ma:,.2f}; daily downtrend"
            ), size_modifier

    # 6. Whipsaw guard — 2+ stops in 96h means choppy market regardless of regime
    stop_count = count_recent_stops(asset, hours=_WHIPSAW_LOOKBACK_H)
    if stop_count >= _WHIPSAW_STOP_LIMIT:
        return False, (
            f"Whipsaw guard — {asset} hit {stop_count} stop-losses in last "
            f"{_WHIPSAW_LOOKBACK_H}h; waiting for cleaner price action before re-entry"
        ), size_modifier

    return True, "", size_modifier


# ── Drawdown circuit breakers ────────────────────────────────────────────────

_DD_HALT_PCT        = 12.0   # strategy drawdown from epoch peak → halt all new trades
_DD_QUARTER_PCT     =  8.0   # drawdown from epoch peak → 25% position size
_DD_HALF_PCT        =  5.0   # drawdown from epoch peak → 50% position size
_DAILY_LOSS_HALT    =  2.0   # single-day loss % → 50% position size
_GLOBAL_HALT_PCT    = 40.0   # absolute loss from epoch capital → emergency global halt


def _get_circuit_breaker_state() -> tuple[bool, str, float]:
    """
    Two-level circuit breaker using risk epoch isolation.

    Level 1 — Strategy epoch circuit breaker (primary):
      Drawdown computed only from trades tagged with the current epoch_id.
      Pre-epoch trades (different strategy version, different capital scale)
      do NOT contaminate this calculation.
      Thresholds: HALT at 12%, reduce at 8% and 5%.

    Level 2 — Global absolute protection:
      If epoch equity falls below (1 - _GLOBAL_HALT_PCT/100) × epoch_paper_capital,
      halt regardless of peak — this catches a series of very large individual losses.

    If no epoch is active: falls back to legacy behavior using all trades and
    LIVE_BALANCE_USD env var as the starting equity (backward compatible).

    Fail-closed: any exception reading risk data returns halted=True.

    Returns (trading_halted, reason, size_modifier).
    """
    try:
        return _get_circuit_breaker_state_inner()
    except Exception as _cb_exc:
        _reason = f"CIRCUIT BREAKER SAFE MODE — risk data unreadable: {_cb_exc}"
        print(f"[CircuitBreaker] {_reason}")
        return True, _reason, 0.0


def _get_circuit_breaker_state_inner() -> tuple[bool, str, float]:
    from pipeline.risk_epoch import get_current_epoch, compute_epoch_drawdown

    epoch = get_current_epoch()

    if epoch is None:
        # Legacy fallback: no epoch registered → use all trades, env-var balance
        _paper_start = float(os.getenv("LIVE_BALANCE_USD", "10000.0"))
        if not TRADE_HISTORY.exists():
            return False, "", 1.0
        try:
            raw_lines = [l for l in TRADE_HISTORY.read_text(encoding="utf-8").splitlines() if l.strip()]
        except OSError as e:
            raise RuntimeError(f"trade_history.jsonl unreadable (legacy CB path): {e}") from e
        trades = []
        for _l in raw_lines:
            try:
                trades.append(json.loads(_l))
            except json.JSONDecodeError as e:
                raise RuntimeError(
                    f"Corrupt line in trade_history.jsonl (legacy CB path): {e!r} | line: {_l[:120]!r}"
                ) from e
        if not trades:
            return False, "", 1.0
        equity = _paper_start
        peak   = _paper_start
        for t in sorted(trades, key=lambda x: x.get("exit_time", "")):
            equity += t.get("pnl_usd", 0.0)
            if equity > peak:
                peak = equity
        drawdown_pct = (peak - equity) / peak * 100 if peak > 0 else 0.0
        today = datetime.now(timezone.utc).date().isoformat()
        daily_pnl = sum(t.get("pnl_usd", 0.0) for t in trades if str(t.get("exit_time", ""))[:10] == today)
        daily_loss_pct = abs(daily_pnl) / equity * 100 if equity > 0 and daily_pnl < 0 else 0.0
    else:
        # Epoch-scoped circuit breaker
        equity, peak, drawdown_pct = compute_epoch_drawdown(epoch)
        paper_capital = epoch["paper_capital"]

        # Level 2: global absolute floor (emergency stop, strategy-scale-independent)
        global_floor = paper_capital * (1.0 - _GLOBAL_HALT_PCT / 100.0)
        if equity < global_floor:
            return True, (
                f"GLOBAL HALT [{epoch['epoch_id']}] — equity ${equity:.2f} < "
                f"${global_floor:.2f} ({_GLOBAL_HALT_PCT:.0f}% floor of epoch capital ${paper_capital:.2f})"
            ), 0.0

        today = datetime.now(timezone.utc).date().isoformat()
        from pipeline.risk_epoch import get_epoch_trades
        epoch_trades = get_epoch_trades(epoch["epoch_id"])
        daily_pnl = sum(
            t.get("pnl_usd", 0.0)
            for t in epoch_trades
            if str(t.get("exit_time", t.get("closed_at_utc", "")))[:10] == today
        )
        daily_loss_pct = abs(daily_pnl) / equity * 100 if equity > 0 and daily_pnl < 0 else 0.0

    # Apply thresholds
    if drawdown_pct >= _DD_HALT_PCT:
        label = f"[{epoch['epoch_id']}] " if epoch else ""
        return True, (
            f"CIRCUIT BREAKER {label}— drawdown {drawdown_pct:.1f}% from peak ${peak:.2f}. "
            f"Trading HALTED. Manual review required."
        ), 0.0

    if drawdown_pct >= _DD_QUARTER_PCT:
        return False, f"Circuit breaker (25% size) — drawdown {drawdown_pct:.1f}% from epoch peak", 0.25

    if drawdown_pct >= _DD_HALF_PCT:
        return False, f"Circuit breaker (50% size) — drawdown {drawdown_pct:.1f}% from epoch peak", 0.5

    if daily_loss_pct >= _DAILY_LOSS_HALT:
        return False, f"Daily loss circuit breaker (50% size) — lost {daily_loss_pct:.1f}% today", 0.5

    return False, "", 1.0


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run_pipeline(asset: str = "ETH-USD", *, _skip_exit_check: bool = False) -> TradeDecision:
    """
    Run all sub-agents in parallel, then orchestrate a final decision.
    Returns the TradeDecision for downstream use.

    _skip_exit_check: set by run_all_assets() when EXIT executor already ran before
    the reconciliation gate so we do not call it a second time per tick.
    """
    print(f"\n[Orchestra] Starting pipeline for {asset} …")
    t0 = time.time()

    # ── 1. Check open positions + pending limit orders ────────────────────────
    snap0 = get_snapshot(asset)
    if snap0:
        current_price = snap0["close"]
        if not _skip_exit_check:
            _check_open_positions(asset, current_price)   # stop/target/max-hold
        _check_pending_fills(asset, current_price)    # fills → open positions

    # ── 1.5 Scanner gate — deterministic signal check ────────────────────────
    # The signal_scanner is our validated entry model. It fires only when EMA50
    # cross + 4/5 conditions are met. If no signal, skip agents entirely.
    # When signal fires, agents act as veto-only (macro SELL blocks).
    _scanner_signal = scan_latest(asset)

    # Idempotency: a 30-minute scheduler may call run_pipeline() twice for the
    # same closed hourly candle. Atomically claim the signal before any processing.
    # CLAIMED → COMPLETED; crash leaves a claimed entry that auto-expires after 2h.
    if _scanner_signal is not None:
        _sig_id = _make_signal_id(asset, _scanner_signal["entry_time"])
        if not _claim_signal(_sig_id, asset, _scanner_signal["entry_time"]):
            print(f"[Scanner] Signal {_sig_id} already claimed/processed — skipping (idempotency)")
            return TradeDecision(
                asset=asset, timestamp=datetime.now(timezone.utc),
                action=TradeAction.HOLD, confidence=0.0,
                reasoning=f"Idempotency: candle {_scanner_signal['entry_time']} already evaluated.",
                votes=[], overrides=[],
                veto_triggered=False, veto_reason=None,
                position_size_pct=None, stop_loss_price=None, take_profit_price=None,
            )

        # Signal claimed — log to V3 forward-OOS journal before any trade action.
        # In shadow mode (v3_enforcement_enabled=False), v3_blocked is always False
        # but v3_would_block is still set, letting us track counterfactuals.
        from pipeline.v3_journal import log_v2_signal
        _v3_blocked    = _scanner_signal.get("v3_blocked", False)
        _v3_would_blk  = _scanner_signal.get("v3_would_block", False)
        log_v2_signal(scanner_signal=_scanner_signal, accepted=not _v3_blocked)

        # V3 enforcement blocked this signal
        if _v3_blocked:
            er  = _scanner_signal.get("er_30", 0.0)
            thr = _scanner_signal.get("v3_candidate_threshold", "?")
            print(f"[V3] Enforced block {asset}: ER-30={er:.3f} < {thr} — HOLD")
            _complete_signal(_sig_id)
            return TradeDecision(
                asset=asset, timestamp=datetime.now(timezone.utc),
                action=TradeAction.HOLD, confidence=0.0,
                reasoning=f"[V3] ER-30={er:.3f} below enforcement threshold {thr}.",
                votes=[], overrides=[f"V3 ER enforcement: {er:.3f} < {thr}"],
                veto_triggered=True, veto_reason=f"V3 regime filter: ER-30={er:.3f}",
                position_size_pct=None, stop_loss_price=None, take_profit_price=None,
            )

        # Shadow-mode info log (enforcement off, but would-block is informative)
        if _v3_would_blk:
            er  = _scanner_signal.get("er_30", 0.0)
            thr = _scanner_signal.get("v3_candidate_threshold", "?")
            print(f"[V3 shadow] Would-block {asset}: ER-30={er:.3f} < {thr} — proceeding (enforcement off)")

    if _scanner_signal is None:
        print(f"[Scanner] No signal for {asset} — HOLD (agents skipped)")
        _hold = TradeDecision(
            asset=asset, timestamp=datetime.now(timezone.utc),
            action=TradeAction.HOLD, confidence=0.0,
            reasoning="Scanner gate: no breakout signal on last closed candle.",
            votes=[], overrides=[],
            veto_triggered=False, veto_reason=None,
            position_size_pct=None, stop_loss_price=None, take_profit_price=None,
        )
        _log_decision(asset, [], _hold)
        _print_decision(asset, [], _hold)
        return _hold

    print(f"[Scanner] SIGNAL — {asset} ${_scanner_signal['entry_price']:,.2f}  "
          f"candles_above={_scanner_signal['candles_above']}  "
          f"ADX={_scanner_signal['adx']:.1f}  "
          f"vol={_scanner_signal['vol_ratio']:.2f}x  "
          f"conf={_scanner_signal['conf']:.0%}"
          + (f"  ER-30={_scanner_signal['er_30']:.3f}" if _scanner_signal.get("er_30") is not None else ""))

    # ── 2. Run sub-agents concurrently ────────────────────────────────────────
    sub_agents = [
        TechnicalAgent(),
        MacroAgent(),
        SentimentAgent(),
        WhaleAgent(),
        RiskAgent(),
        AssetNewsAgent(),
        BreakoutAgent(),
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

    # ── 3.5 Scanner elevation — upgrade HOLD to BUY if no macro veto ─────────
    # If scanner fired but orchestrator still says HOLD (composite score too low),
    # we override to BUY — UNLESS macro agent explicitly votes SELL (bear regime).
    # This aligns live entries with the backtested signal_scanner logic.
    if _scanner_signal and decision.action == TradeAction.HOLD and not decision.veto_triggered:
        macro_vote = next(
            (s for s in signals if "macro" in s.agent.value.lower()), None
        )
        if macro_vote and macro_vote.signal.value == "SELL":
            print("[Scanner] Macro SELL veto — keeping HOLD despite scanner signal")
            decision.overrides.append(
                f"Scanner fired but macro SELL ({macro_vote.confidence:.0%}) blocks entry"
            )
        else:
            _default_size = float(os.getenv("TRADE_SIZE_PCT", "0.05"))
            print("[Scanner] Elevating HOLD→BUY — scanner signal confirmed, no macro veto")
            decision = TradeDecision(
                asset=asset, timestamp=decision.timestamp,
                action=TradeAction.BUY,
                confidence=_scanner_signal["conf"],
                reasoning=(
                    f"[Scanner] EMA50 cross {_scanner_signal['candles_above']} candle(s) ago "
                    f"at ${_scanner_signal['entry_price']:,.2f}, "
                    f"ADX={_scanner_signal['adx']:.1f}, "
                    f"vol={_scanner_signal['vol_ratio']:.2f}x, "
                    f"{_scanner_signal['n_conditions']}/5 conditions met. "
                    + decision.reasoning
                ),
                votes=decision.votes,
                overrides=decision.overrides + ["Scanner override: BUY elevated from HOLD"],
                veto_triggered=decision.veto_triggered,
                veto_reason=decision.veto_reason,
                position_size_pct=_default_size,
                stop_loss_price=None, take_profit_price=None,
            )

    # ── 4. BUY → place limit order at support ─────────────────────────────────
    # Instead of a market entry, we queue a limit order at the nearest support
    # level. This earns the maker fee (0.2%) vs taker (0.4%), saving 0.4% per
    # round trip — the margin that was blocking backtest profitability.
    if decision.action == TradeAction.BUY:
        # ── Drawdown circuit breaker (portfolio-level protection)
        _cb_halted, _cb_reason, _cb_size = _get_circuit_breaker_state()
        if _cb_halted:
            print(f"[CircuitBreaker] HALT — {_cb_reason}")
            decision = TradeDecision(
                asset=asset, timestamp=decision.timestamp,
                action=TradeAction.HOLD,
                confidence=decision.confidence,
                reasoning=f"[CircuitBreaker] {_cb_reason}. " + decision.reasoning,
                votes=decision.votes, overrides=decision.overrides,
                veto_triggered=True, veto_reason=_cb_reason,
                position_size_pct=None, stop_loss_price=None, take_profit_price=None,
            )
            _log_decision(asset, signals, decision)
            _print_decision(asset, signals, decision)
            return decision

        # ── Entry filters: correlation-adjusted BTC veto, funding, bounce, velocity
        _entry_ok, _block_reason, _size_mod = _check_entry_filters(asset)
        if not _entry_ok:
            print(f"[EntryFilter] BLOCKED — {_block_reason}")
            decision = TradeDecision(
                asset=asset, timestamp=decision.timestamp,
                action=TradeAction.HOLD,
                confidence=decision.confidence,
                reasoning=f"[EntryFilter] {_block_reason}. " + decision.reasoning,
                votes=decision.votes, overrides=decision.overrides,
                veto_triggered=decision.veto_triggered, veto_reason=decision.veto_reason,
                position_size_pct=None, stop_loss_price=None, take_profit_price=None,
            )
        # Don't stack if there's already an open position OR a pending order for this asset
        elif get_open_positions(asset):
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
                    # EWMA volatility scaling (GARCH-inspired adaptive sizing)
                    # Target 2% daily vol. Scale down when market is volatile, up when calm.
                    _TARGET_DAILY_VOL = 0.02
                    snap_for_vol = get_snapshot(asset)
                    _ewma_vol = snap_for_vol.get("ewma_vol_daily") if snap_for_vol else None
                    if _ewma_vol and _ewma_vol > 0:
                        _vol_scalar = min(_TARGET_DAILY_VOL / max(_ewma_vol, 0.005), 2.0)
                        _vol_scalar = max(_vol_scalar, 0.25)  # floor at 25%
                    else:
                        _vol_scalar = 1.0
                    if abs(_vol_scalar - 1.0) > 0.05:
                        print(f"[VolScaling] EWMA daily vol {_ewma_vol:.2%} -> size x{_vol_scalar:.2f}")

                    # Apply modifiers: correlation veto × circuit breaker × vol scaling (multiplicative)
                    _combined_mod = _size_mod * _cb_size * _vol_scalar
                    _effective_size = (
                        round(decision.position_size_pct * _combined_mod, 4)
                        if decision.position_size_pct and _combined_mod < 1.0
                        else decision.position_size_pct
                    )
                    if _size_mod < 1.0:
                        print(f"[EntryFilter] Size reduced to {_size_mod:.0%} "
                              f"(partial correlation veto — decorrelated but BTC in BEAR)")
                    if _cb_size < 1.0:
                        print(f"[CircuitBreaker] Size reduced to {_cb_size:.0%} — {_cb_reason}")

                    # ── Placement ──────────────────────────────────────────
                    # LIVE mode: SQLite outbox (TX-A before Coinbase call,
                    # atomic reconciliation gate, full recovery on crash).
                    # DRY_RUN mode: old JSON system (fill simulation intact).
                    from exchange.coinbase_client import (
                        is_dry_run as _is_dry_run,
                        place_limit_buy as _place_limit_buy,
                    )
                    from pipeline.limit_orders import _atr_mults

                    _pct = _effective_size or 0.02
                    _bal = float(os.getenv("LIVE_BALANCE_USD", "10000"))
                    _qty_usd = round(_bal * _pct, 2)
                    _stop_mult, _tgt_mult = _atr_mults(asset)
                    _stop_price   = round(support - _stop_mult * atr, 2)
                    _target_price = round(support + _tgt_mult * atr, 2)

                    if _is_dry_run():
                        order = place_limit_order(
                            asset=asset,
                            limit_price=support,
                            atr=atr,
                            position_size_pct=_effective_size,
                            reasoning=decision.reasoning,
                        )
                        _order_id       = order.id
                        _order_stop     = order.stop_price
                        _order_target   = order.target_price
                        _order_reasoning = order.reasoning
                        _notify_order   = order  # PendingOrder for Telegram formatter
                    else:
                        from pipeline.outbox import place_order_outbox, PlacementBlocked
                        from types import SimpleNamespace as _NS
                        try:
                            _result = place_order_outbox(
                                asset=asset,
                                limit_price=support,
                                qty_usd=_qty_usd,
                                stop_price=_stop_price,
                                target_price=_target_price,
                                reasoning=decision.reasoning,
                                coinbase_fn=lambda cid: _place_limit_buy(
                                    asset, _qty_usd, support, client_order_id=cid
                                ),
                            )
                        except PlacementBlocked as _exc:
                            print(f"[Outbox] Placement blocked by gate: {_exc}")
                            decision = TradeDecision(
                                asset=asset, timestamp=decision.timestamp,
                                action=TradeAction.HOLD,
                                confidence=decision.confidence,
                                reasoning=f"[Outbox] Gate blocked: {_exc}. " + decision.reasoning,
                                votes=decision.votes, overrides=decision.overrides,
                                veto_triggered=True,
                                veto_reason=str(_exc),
                                position_size_pct=None, stop_loss_price=None,
                                take_profit_price=None,
                            )
                            _log_decision(asset, signals, decision)
                            _print_decision(asset, signals, decision)
                            return decision
                        _order_id        = _result.order_id
                        _order_stop      = _stop_price
                        _order_target    = _target_price
                        _order_reasoning = decision.reasoning
                        _notify_order    = _NS(
                            id=_result.order_id,
                            limit_price=support,
                            stop_price=_stop_price,
                            target_price=_target_price,
                            reasoning=decision.reasoning,
                        )
                        _result_status   = _result.status

                    # ── Status-aware log + notification ───────────────────
                    dist_str = f"{dist:.1f}x ATR away" if dist else "at support"
                    if _is_dry_run() or _result_status == "OPEN":
                        print(f"[LimitOrder] PLACED #{_order_id} — limit ${support:,.2f}  "
                              f"stop ${_order_stop:,.2f}  target ${_order_target:,.2f}  "
                              f"({dist_str})  expires 24h")
                        _log_order_event(asset, "LIMIT_ORDER_PLACED", {
                            "order_id":    _order_id,
                            "limit_price": support,
                            "stop_price":  _order_stop,
                            "target_price": _order_target,
                            "dist_atr":    dist,
                            "maker_fee":   MAKER_FEE_RATE,
                            "reasoning":   _order_reasoning,
                        })
                        send_telegram_message(format_limit_order_placed(asset, _notify_order, levels))
                    elif _result_status == "SUBMITTING":
                        # Network timeout / ambiguous response — TX-A committed,
                        # TX-B was not.  Startup reconciler will resolve via
                        # client_order_id.  Do NOT send "order placed" — it hasn't
                        # been confirmed by Coinbase.
                        _msg = (
                            f"[{asset}] ⚠️ SUBMISSION UNKNOWN — order #{_order_id} "
                            f"at ${support:,.2f} left in SUBMITTING state. "
                            "Startup reconciler will resolve on next run."
                        )
                        print(f"[LimitOrder] {_msg}")
                        _log_order_event(asset, "LIMIT_ORDER_SUBMISSION_UNKNOWN", {
                            "order_id": _order_id,
                            "limit_price": support,
                            "reasoning": _order_reasoning,
                        })
                        send_telegram_message(_msg)
                    elif _result_status == "REJECTED":
                        _msg = (
                            f"[{asset}] ❌ ORDER REJECTED — #{_order_id} at ${support:,.2f}: "
                            f"{_result.rejection_reason or 'no reason provided'}"
                        )
                        print(f"[LimitOrder] {_msg}")
                        _log_order_event(asset, "LIMIT_ORDER_REJECTED", {
                            "order_id": _order_id,
                            "limit_price": support,
                            "rejection_reason": _result.rejection_reason,
                            "reasoning": _order_reasoning,
                        })
                        send_telegram_message(_msg)
                    # Downgrade to HOLD — the live action is the pending order
                    decision = TradeDecision(
                        asset=asset, timestamp=decision.timestamp,
                        action=TradeAction.HOLD,
                        confidence=decision.confidence,
                        reasoning=f"[Limit] Order #{_order_id} ({_result_status if not _is_dry_run() else 'OPEN'}) at support ${support:,.2f}. " + decision.reasoning,
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
                    overrides=decision.overrides + [f"SELL->HOLD: candle {candle_body:+.3%}."],
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

    # Mark this signal as fully processed (claim → complete)
    if _scanner_signal is not None:
        try:
            _complete_signal(_make_signal_id(asset, _scanner_signal["entry_time"]))
        except Exception:
            pass  # Non-fatal; claim will auto-expire in 2h

    return decision


# BTC and SOL excluded: bounce strategy has negative edge on these assets.
# ETH: best with daily 50EMA filter (faster trend response).
# ZEC: best with daily 200EMA filter (longer context needed for slower asset).
# Re-enable after developing asset-specific entry logic for BTC/SOL.
ASSETS = ["ETH-USD", "ZEC-USD"]

# Cooldown for repeated "no price snapshot" Telegram alerts — avoids storm when
# a data source is down for multiple consecutive ticks (one alert per hour per asset).
_snapshot_alert_cooldown: dict[str, float] = {}
_SNAPSHOT_ALERT_COOLDOWN_S = 3600.0

# Per-asset daily EMA period for the trend gate in _check_entry_filters
_DAILY_EMA_PERIOD: dict[str, int] = {
    "ETH-USD": 50,   # 50EMA is faster — catches ETH tops/bottoms weeks earlier
    "ZEC-USD": 200,  # 200EMA stable for low-liquidity asset
    "BTC-USD": 50,
    "SOL-USD": 200,
}


def _startup_reconciliation():
    """
    Run startup reconciliation and return (entry_allowed, report).

    entry_allowed: False when there are UNRESOLVED items or on exception — fail-closed.
    report: the full ReconciliationReport (None on exception).

    Always runs run_migrations() first.  The caller uses the report to determine
    per-asset EXIT eligibility before deciding whether ENTRY is allowed.
    """
    from pipeline.ledger import run_migrations
    from pipeline.reconciler import run_startup_reconciliation
    from exchange.adapter import make_get_order_fn, make_list_orders_fn
    from exchange.coinbase_client import cancel_order, is_dry_run

    run_migrations()

    try:
        report = run_startup_reconciliation(
            list_orders_fn=make_list_orders_fn(),
            cancel_order_fn=cancel_order,
            get_order_fn=make_get_order_fn(),
        )
    except Exception as exc:
        msg = f"[Startup] Reconciliation failed with exception: {exc}"
        print(msg)
        send_telegram_message(msg)
        return False, None

    mode = "DRY_RUN" if is_dry_run() else "LIVE"
    if report.unresolved:
        msg = (
            f"[Startup {mode}] Reconciliation BLOCKED — "
            f"{len(report.unresolved)} unresolved item(s):\n" +
            "\n".join(f"  • {u.order_id} ({u.asset}): {u.reason}"
                      for u in report.unresolved)
        )
        print(msg)
        send_telegram_message(msg)
        return False, report

    if report.resolved:
        print(
            f"[Startup {mode}] Reconciliation complete — "
            f"{len(report.resolved)} resolved, 0 unresolved"
        )
    else:
        print(f"[Startup {mode}] Reconciliation complete — no pending orders")

    return True, report


def _get_open_position_assets() -> list[str] | None:
    """Return distinct assets with at least one OPEN or CLOSING position.

    Returns None on DB failure — caller must treat this as a global EXIT block,
    not as "no open positions".
    """
    from pipeline.ledger import get_db
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT DISTINCT asset FROM positions WHERE status IN ('OPEN','CLOSING')"
            ).fetchall()
        return [r["asset"] for r in rows]
    except Exception as exc:
        print(f"[ExitExecutor] ERROR reading open positions from ledger: {exc}")
        return None


def run_all_assets(target_asset: str | None = None) -> dict[str, TradeDecision]:
    """
    Startup reconciliation then EXIT executor then ENTRY pipeline.

    Order matters:
      1. run_migrations() inside _startup_reconciliation() — schema before any DB access.
      2. Reconciliation — determines qty certainty and orphan SELL state per asset.
      3. EXIT executor — allowed per-asset when its unresolved items are zero; blocked
         when reconciliation is uncertain about qty or an unknown SELL exists for
         that asset. An UNRESOLVED ENTRY for a different asset does not block EXIT.
      4. ENTRY pipeline — blocked if any UNRESOLVED exists (existing gate unchanged).

    target_asset: if given, only run that one asset.  The reconciliation gate
    still runs regardless — the single-asset CLI path must not bypass it.
    """
    # Step 1: Reconciliation (includes migrations)
    entry_ok, report = _startup_reconciliation()

    # Step 2: Build asset-level and global EXIT blocks.
    # global_exit_block=True means no asset's EXIT is safe — e.g. an orphan SELL
    # with an unresolvable asset could be a double-sell against any position.
    unresolved_assets: set[str] = set()
    global_exit_block = False

    if report is not None:
        for u in report.unresolved:
            if not u.asset or u.asset in ("UNKNOWN", ""):
                # Orphan SELL whose product_id was not readable — cannot map it
                # to a specific position, so any EXIT might be a double-sell.
                global_exit_block = True
            else:
                unresolved_assets.add(u.asset)
    elif not entry_ok:
        # Reconciliation raised an exception — no report available.
        # We cannot reason about any asset's qty safety.
        global_exit_block = True

    # Step 3: EXIT executor — always for ALL open positions, never filtered by
    # target_asset.  target_asset limits only the ENTRY pipeline (Step 5).
    # A decommissioned or CLI-excluded asset with an open position still needs
    # protective EXIT checks.
    all_open_assets = _get_open_position_assets()
    if all_open_assets is None:
        msg = (
            "[ExitExecutor] CRITICAL — could not read open positions from ledger. "
            "All EXIT orders deferred to manual review."
        )
        print(msg)
        send_telegram_message(msg)
        all_open_assets = []
        global_exit_block = True
        entry_ok = False  # DB unreliable — ENTRY would also use a bad ledger state

    for asset in all_open_assets:
        if global_exit_block:
            blocked_reason = "unknown orphan SELL or reconciliation failure — ledger state uncertain"
        elif asset in unresolved_assets:
            blocked_reason = f"reconciliation uncertain for {asset}"
        else:
            blocked_reason = None

        if blocked_reason:
            msg = (
                f"[ExitExecutor] BLOCKED {asset} — {blocked_reason}; "
                "EXIT deferred to manual review."
            )
            print(msg)
            send_telegram_message(msg)
            continue

        try:
            snap0 = get_snapshot(asset)
        except Exception as _snap_exc:
            _snap_now = time.monotonic()
            if _snap_now - _snapshot_alert_cooldown.get(asset, 0.0) >= _SNAPSHOT_ALERT_COOLDOWN_S:
                _snapshot_alert_cooldown[asset] = _snap_now
                msg = (
                    f"[ExitExecutor] CRITICAL — price snapshot raised exception for {asset}: "
                    f"{_snap_exc}. EXIT check skipped; manual review required."
                )
                print(msg)
                send_telegram_message(msg)
            else:
                print(f"[ExitExecutor] snapshot exception for {asset} (alert suppressed — cooldown active)")
            continue

        if snap0:
            _check_open_positions(asset, snap0["close"])
        else:
            _snap_now = time.monotonic()
            if _snap_now - _snapshot_alert_cooldown.get(asset, 0.0) >= _SNAPSHOT_ALERT_COOLDOWN_S:
                _snapshot_alert_cooldown[asset] = _snap_now
                msg = (
                    f"[ExitExecutor] CRITICAL — price snapshot unavailable for {asset}. "
                    "EXIT check skipped this tick; stop-loss may not trigger. "
                    "Manual review required if this persists."
                )
                print(msg)
                send_telegram_message(msg)
            else:
                print(f"[ExitExecutor] snapshot still unavailable for {asset} (alert suppressed — cooldown active)")

    # Step 4: ENTRY gate
    if not entry_ok:
        print("[Startup] Halting — reconciliation blocked new ENTRY orders.")
        return {}

    trade_assets = [target_asset] if target_asset else ASSETS
    results = {}
    for asset in trade_assets:
        print(f"\n{'='*65}")
        decision = run_pipeline(asset, _skip_exit_check=True)
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

    # Always go through run_all_assets() so the reconciliation gate runs
    # regardless of whether a specific asset is given on the command line.
    # Direct run_pipeline() calls bypass the gate and must not be used as
    # a live entry point.
    target = sys.argv[1] if len(sys.argv) > 1 else None
    run_all_assets(target_asset=target)
