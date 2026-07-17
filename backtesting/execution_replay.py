"""
Execution-Parity Replay Engine
================================
Models the ACTUAL live execution mechanics — support-limit placement, 24h GTD
expiry, and fill simulation — so backtest results describe the same strategy
as the live system.

Three fill models produce a RANGE of outcomes rather than a single "precise" number:

  OPTIMISTIC   — fill when hourly low <= limit_price  (best case, full maker)
  CONSERVATIVE — fill when hourly low <= limit_price - 0.1*ATR buffer  (clear penetration)
  MINUTELY     — check 1m candles; resolves within-candle stop/target order  (most realistic)

Key invariants (matching live runner.py):
  • Support computed from swing-low history *up to and including* signal candle.
  • Order NOT filled on the signal candle itself.
  • While an order is PENDING, new signals are ignored (same as `get_open_orders()` check).
  • GTD expiry = signal_ts + 24h.
  • Stop/target priced from fill_price, not from signal close.
  • Within-candle ambiguity (stop and target both inside OHLC): pessimistic stop-first.
  • Fees: maker entry 0.4%, maker TP 0.4%, taker SL/max-hold 0.6%.
"""

from __future__ import annotations

import dataclasses
from enum import Enum
from typing import Optional
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.price_levels import get_levels
from backtesting.signal_scanner import (
    ASSET_CONFIG, PERIODS, _download_and_compute, _detect_breakout_signal,
    _compute_regime_metrics,
)

# ── Fee constants (mirror signal_scanner.py exactly) ─────────────────────────
_MAKER_ENTRY = 0.004   # limit order filled as maker
_MAKER_TP    = 0.004   # take-profit limit order
_TAKER_SL    = 0.006   # stop-loss / max-hold market exit
_TAKER_ENTRY = 0.006   # limit order becomes taker (conservative assumption)

# Live-matching constants
_GTD_HOURS         = 24      # order lifetime
_MAX_DIST_ATR      = 5.0     # support must be within this many ATRs
_CONSERVATIVE_BUFF = 0.10    # extra ATR fraction below limit for conservative model


class FillModel(str, Enum):
    OPTIMISTIC   = "optimistic"
    CONSERVATIVE = "conservative"
    MINUTELY     = "minutely"     # requires 1m candle download


class OrderState(str, Enum):
    PENDING = "PENDING"
    FILLED  = "FILLED"
    EXPIRED = "EXPIRED"   # GTD 24h elapsed without fill


# ── Data classes ─────────────────────────────────────────────────────────────

@dataclasses.dataclass
class LimitOrder:
    signal_i:     int
    signal_ts:    pd.Timestamp
    signal_price: float              # breakout close (reference only)
    limit_price:  float              # order placement = support level
    atr:          float
    expiry_ts:    pd.Timestamp       # signal_ts + 24h
    state:        OrderState = OrderState.PENDING
    fill_i:       Optional[int]            = None
    fill_ts:      Optional[pd.Timestamp]   = None
    fill_price:   Optional[float]          = None
    fill_type:    str                      = "maker"   # "maker" | "taker"


@dataclasses.dataclass
class TradeResult:
    fill_price:   float
    stop_price:   float
    target_price: float
    exit_price:   float
    exit_reason:  str       # STOP_LOSS | TAKE_PROFIT | MAX_HOLD
    hold_h:       int
    pnl_pct:      float
    mae_pct:      float     # max adverse excursion from fill price (%)
    mfe_pct:      float     # max favorable excursion from fill price (%)
    entry_fee:    float
    exit_fee:     float


@dataclasses.dataclass
class SignalResult:
    signal_i:               int
    signal_ts:              pd.Timestamp
    signal_price:           float               # breakout close
    dist_to_support_atr:    Optional[float]     # None = no support found
    limit_price:            Optional[float]     # None = no valid support / too far
    order:                  Optional[LimitOrder]
    trade:                  Optional[TradeResult]
    no_fill_reason:         Optional[str]       # EXPIRED | NO_SUPPORT | TOO_FAR | PENDING_BLOCKED
    price_at_expiry:        Optional[float]     # price when order expired (opportunity cost ref)
    price_move_after_pct:   Optional[float]     # % move in max_hold_h after signal time
    er_30:                  Optional[float]


# ── Core simulation logic ────────────────────────────────────────────────────

def _simulate_filled_trade(
    df: pd.DataFrame,
    fill_i: int,
    fill_price: float,
    atr: float,
    atr_stop: float,
    atr_target: float,
    max_hold_h: int,
    entry_fee: float,
) -> TradeResult:
    """
    Simulate a trade starting from fill_i at fill_price.
    Stop/target priced from fill_price (not signal close).
    Within-candle ambiguity (both stop and target in same OHLC): stop wins (pessimistic).
    Tracks MAE and MFE throughout the hold period.
    """
    stop_price   = fill_price - atr_stop   * atr
    target_price = fill_price + atr_target * atr

    mae_pct = 0.0
    mfe_pct = 0.0

    for j in range(fill_i + 1, min(fill_i + max_hold_h + 1, len(df))):
        row  = df.iloc[j]
        low  = float(row["low"])
        high = float(row["high"])

        # Track MAE/MFE
        adverse   = (fill_price - low)  / fill_price * 100
        favorable = (high - fill_price) / fill_price * 100
        mae_pct   = max(mae_pct, adverse)
        mfe_pct   = max(mfe_pct, favorable)

        # Within-candle: if both stop and target touched, use pessimistic stop-first
        stop_hit   = low  <= stop_price
        target_hit = high >= target_price

        if stop_hit and target_hit:
            # Pessimistic: assume stop fires first
            gross   = stop_price * (1 - _TAKER_SL)
            pnl_pct = (gross - fill_price * (1 + entry_fee)) / fill_price * 100
            return TradeResult(
                fill_price=fill_price, stop_price=stop_price, target_price=target_price,
                exit_price=stop_price, exit_reason="STOP_LOSS", hold_h=j - fill_i,
                pnl_pct=round(pnl_pct, 2), mae_pct=round(mae_pct, 2), mfe_pct=round(mfe_pct, 2),
                entry_fee=entry_fee, exit_fee=_TAKER_SL,
            )
        if stop_hit:
            gross   = stop_price * (1 - _TAKER_SL)
            pnl_pct = (gross - fill_price * (1 + entry_fee)) / fill_price * 100
            return TradeResult(
                fill_price=fill_price, stop_price=stop_price, target_price=target_price,
                exit_price=stop_price, exit_reason="STOP_LOSS", hold_h=j - fill_i,
                pnl_pct=round(pnl_pct, 2), mae_pct=round(mae_pct, 2), mfe_pct=round(mfe_pct, 2),
                entry_fee=entry_fee, exit_fee=_TAKER_SL,
            )
        if target_hit:
            gross   = target_price * (1 - _MAKER_TP)
            pnl_pct = (gross - fill_price * (1 + entry_fee)) / fill_price * 100
            return TradeResult(
                fill_price=fill_price, stop_price=stop_price, target_price=target_price,
                exit_price=target_price, exit_reason="TAKE_PROFIT", hold_h=j - fill_i,
                pnl_pct=round(pnl_pct, 2), mae_pct=round(mae_pct, 2), mfe_pct=round(mfe_pct, 2),
                entry_fee=entry_fee, exit_fee=_MAKER_TP,
            )

    # Max hold — market exit, taker fee
    last_i      = min(fill_i + max_hold_h, len(df) - 1)
    exit_price  = float(df.iloc[last_i]["close"])
    gross       = exit_price * (1 - _TAKER_SL)
    pnl_pct     = (gross - fill_price * (1 + entry_fee)) / fill_price * 100
    return TradeResult(
        fill_price=fill_price, stop_price=stop_price, target_price=target_price,
        exit_price=exit_price, exit_reason="MAX_HOLD", hold_h=max_hold_h,
        pnl_pct=round(pnl_pct, 2), mae_pct=round(mae_pct, 2), mfe_pct=round(mfe_pct, 2),
        entry_fee=entry_fee, exit_fee=_TAKER_SL,
    )


def _try_fill(order: LimitOrder, candle_i: int, df: pd.DataFrame, model: FillModel) -> bool:
    """Return True if the order fills on this candle under the given fill model."""
    low = float(df.iloc[candle_i]["low"])
    if model == FillModel.OPTIMISTIC:
        return low <= order.limit_price
    if model == FillModel.CONSERVATIVE:
        return low <= order.limit_price - _CONSERVATIVE_BUFF * order.atr
    # MINUTELY handled separately via _replay_minutely
    return False


def _price_move_pct(df: pd.DataFrame, from_i: int, horizon_h: int) -> float | None:
    """% price change from df[from_i].close over the next horizon_h candles."""
    to_i = min(from_i + horizon_h, len(df) - 1)
    if to_i <= from_i:
        return None
    p0 = float(df.iloc[from_i]["close"])
    p1 = float(df.iloc[to_i]["close"])
    if p0 == 0:
        return None
    return round((p1 - p0) / p0 * 100, 2)


# ── Main replay engine ────────────────────────────────────────────────────────

def replay_signals(
    df: pd.DataFrame,
    signal_indices: list[int],
    signal_er30s: list[float | None],
    fill_model: FillModel,
    atr_stop: float,
    atr_target: float,
    max_hold_h: int,
    daily_df: pd.DataFrame | None = None,
) -> list[SignalResult]:
    """
    Event machine: for each signal index, model the full limit-order lifecycle.

    Design: the pending order is advanced bar-by-bar *between* signals, not greedily
    to the end of df. This means a signal that fires while an order is still pending
    (within the 24h GTD window) correctly gets PENDING_BLOCKED, mirroring the live
    `get_open_orders()` check in runner.py.

    After the last signal the pending order is resolved to the end of df.
    """
    results: list[SignalResult] = []

    # Mutable active-order state. We use a single dict so nested helpers can
    # mutate it without needing `nonlocal` on every field.
    active: dict = {
        "order": None,      # LimitOrder | None — current pending/filled order
        "result_idx": None, # int | None — index into results[] for this order's SignalResult
        "scan_cursor": -1,  # last bar index we've already scanned for this order
    }

    def _advance_order(up_to_exclusive: int) -> None:
        """Scan active order from scan_cursor+1 up to (but not including) up_to_exclusive."""
        order = active["order"]
        if order is None or order.state != OrderState.PENDING:
            return
        for check_i in range(active["scan_cursor"] + 1, up_to_exclusive):
            check_ts = df.index[check_i]
            if check_ts > order.expiry_ts:
                order.state = OrderState.EXPIRED
                active["scan_cursor"] = check_i
                return
            if fill_model != FillModel.MINUTELY and _try_fill(order, check_i, df, fill_model):
                order.state      = OrderState.FILLED
                order.fill_i     = check_i
                order.fill_ts    = check_ts
                order.fill_price = order.limit_price
                order.fill_type  = "maker" if fill_model == FillModel.OPTIMISTIC else "taker"
                active["scan_cursor"] = check_i
                return
            active["scan_cursor"] = check_i

    def _active_blocks_entry(at_i: int) -> bool:
        """Return True if the active order/trade blocks a new signal at bar at_i."""
        order = active["order"]
        if order is None:
            return False
        if order.state == OrderState.PENDING:
            return True
        if order.state == OrderState.FILLED:
            # Compute trade if not yet done
            r = results[active["result_idx"]]
            if r.trade is None and order.fill_i is not None:
                entry_fee = _MAKER_ENTRY if order.fill_type == "maker" else _TAKER_ENTRY
                r.trade = _simulate_filled_trade(
                    df, order.fill_i, order.fill_price, order.atr,
                    atr_stop, atr_target, max_hold_h, entry_fee,
                )
            if r.trade is not None and order.fill_i is not None:
                return at_i <= order.fill_i + r.trade.hold_h
        return False

    def _finalize_active() -> None:
        """After all signals processed: scan to end of df and write price_at_expiry."""
        _advance_order(len(df))
        order = active["order"]
        if order is None:
            return
        if order.state == OrderState.PENDING:
            order.state = OrderState.EXPIRED
        r = results[active["result_idx"]]
        if order.state == OrderState.FILLED and r.trade is None and order.fill_i is not None:
            entry_fee = _MAKER_ENTRY if order.fill_type == "maker" else _TAKER_ENTRY
            r.trade = _simulate_filled_trade(
                df, order.fill_i, order.fill_price, order.atr,
                atr_stop, atr_target, max_hold_h, entry_fee,
            )
        if order.state == OrderState.EXPIRED and r.price_at_expiry is None:
            exp_i = df.index.searchsorted(order.expiry_ts)
            if exp_i < len(df):
                r.price_at_expiry = float(df.iloc[min(exp_i, len(df)-1)]["close"])
        if r.no_fill_reason is None:
            r.no_fill_reason = None if order.state == OrderState.FILLED else order.state.value

    # ──────────────────────────────────────────────────────────────────────────
    for idx_pos, signal_i in enumerate(signal_indices):
        ts    = df.index[signal_i]
        price = float(df.iloc[signal_i]["close"])
        atr   = float(df.iloc[signal_i]["atr"])
        er30  = signal_er30s[idx_pos] if idx_pos < len(signal_er30s) else None

        # Step 1: Advance pending/filled order to just before this signal
        _advance_order(signal_i)

        # Step 2: Check expiry exactly at signal time
        order = active["order"]
        if order and order.state == OrderState.PENDING and ts > order.expiry_ts:
            order.state = OrderState.EXPIRED

        # Step 3: Block if there's still an active order/trade at this signal
        if _active_blocks_entry(signal_i):
            results.append(SignalResult(
                signal_i=signal_i, signal_ts=ts, signal_price=price,
                dist_to_support_atr=None, limit_price=None,
                order=None, trade=None,
                no_fill_reason="PENDING_BLOCKED",
                price_at_expiry=None,
                price_move_after_pct=_price_move_pct(df, signal_i, max_hold_h),
                er_30=er30,
            ))
            active["scan_cursor"] = signal_i
            continue

        # Step 4: Clear expired order so we can place a new one
        if active["order"] and active["order"].state == OrderState.EXPIRED:
            # Write price_at_expiry into the expired result
            exp_r = results[active["result_idx"]]
            if exp_r.price_at_expiry is None:
                exp_i = df.index.searchsorted(active["order"].expiry_ts)
                if exp_i < len(df):
                    exp_r.price_at_expiry = float(df.iloc[min(exp_i, len(df)-1)]["close"])
            if exp_r.no_fill_reason is None:
                exp_r.no_fill_reason = "EXPIRED"
            active["order"] = None
            active["result_idx"] = None

        # Step 5: Clear finished trade
        if active["order"] and active["order"].state == OrderState.FILLED:
            active["order"] = None
            active["result_idx"] = None

        # Step 6: Compute support at signal candle (strictly from history up to signal_i)
        levels = get_levels(df, signal_i)
        support = levels.get("nearest_support")
        dist_atr = levels.get("dist_to_support")

        if support is None:
            results.append(SignalResult(
                signal_i=signal_i, signal_ts=ts, signal_price=price,
                dist_to_support_atr=None, limit_price=None,
                order=None, trade=None,
                no_fill_reason="NO_SUPPORT",
                price_at_expiry=None,
                price_move_after_pct=_price_move_pct(df, signal_i, max_hold_h),
                er_30=er30,
            ))
            active["scan_cursor"] = signal_i
            continue

        if dist_atr is not None and dist_atr > _MAX_DIST_ATR:
            results.append(SignalResult(
                signal_i=signal_i, signal_ts=ts, signal_price=price,
                dist_to_support_atr=dist_atr, limit_price=support,
                order=None, trade=None,
                no_fill_reason="TOO_FAR",
                price_at_expiry=None,
                price_move_after_pct=_price_move_pct(df, signal_i, max_hold_h),
                er_30=er30,
            ))
            active["scan_cursor"] = signal_i
            continue

        # Step 7: Place limit order — store as active, scan will happen at next iteration
        expiry_ts = ts + pd.Timedelta(hours=_GTD_HOURS)
        new_order = LimitOrder(
            signal_i=signal_i, signal_ts=ts, signal_price=price,
            limit_price=support, atr=atr, expiry_ts=expiry_ts,
        )
        results.append(SignalResult(
            signal_i=signal_i, signal_ts=ts, signal_price=price,
            dist_to_support_atr=dist_atr, limit_price=support,
            order=new_order, trade=None,
            no_fill_reason=None,      # set later when order resolves
            price_at_expiry=None,     # set later when order expires
            price_move_after_pct=_price_move_pct(df, signal_i, max_hold_h),
            er_30=er30,
        ))
        active["order"]      = new_order
        active["result_idx"] = len(results) - 1
        active["scan_cursor"] = signal_i

    # After last signal: advance to end of df and write final state
    _finalize_active()
    return results


# ── Stats computation ────────────────────────────────────────────────────────

def compute_stats(results: list[SignalResult]) -> dict:
    """
    Compute the full stat table from replay results.

    Per the spec:
      signals, orders_placed, fill_rate, no_fill_rate, median_time_to_fill,
      maker_taker_split, pnl_per_filled_order, pnl_per_signal (no-fill=0),
      price_move_after_no_fill, opportunity_cost, mae_avg, mfe_avg, max_drawdown.
    """
    n = len(results)
    if n == 0:
        return {"signals": 0}

    orders_placed = [r for r in results if r.order is not None]
    filled        = [r for r in results if r.order and r.order.state == OrderState.FILLED and r.trade]
    expired       = [r for r in results if r.order and r.order.state == OrderState.EXPIRED]
    no_support    = [r for r in results if r.no_fill_reason == "NO_SUPPORT"]
    too_far       = [r for r in results if r.no_fill_reason == "TOO_FAR"]
    blocked       = [r for r in results if r.no_fill_reason == "PENDING_BLOCKED"]

    fill_rate    = len(filled) / len(orders_placed) if orders_placed else 0.0
    no_fill_rate = 1.0 - fill_rate

    # Time to fill
    times_to_fill: list[float] = []
    for r in filled:
        if r.order and r.order.fill_ts:
            delta_h = (r.order.fill_ts - r.signal_ts).total_seconds() / 3600
            times_to_fill.append(delta_h)
    median_ttf = float(np.median(times_to_fill)) if times_to_fill else None

    # Maker/taker split
    maker_fills = sum(1 for r in filled if r.order and r.order.fill_type == "maker")
    taker_fills = len(filled) - maker_fills

    # P&L
    pnls_filled = [r.trade.pnl_pct for r in filled]
    pnl_per_filled   = float(np.mean(pnls_filled)) if pnls_filled else None
    pnl_per_signal   = float(np.mean(pnls_filled + [0.0] * (n - len(filled)))) if n else None

    # Wins / losses
    wins_filled  = [p for p in pnls_filled if p > 0]
    losses_filled = [p for p in pnls_filled if p <= 0]
    gw = sum(wins_filled)
    gl = abs(sum(losses_filled))
    pf = gw / gl if gl else float("inf")
    wr = len(wins_filled) / len(pnls_filled) if pnls_filled else None

    # MAE / MFE
    maes = [r.trade.mae_pct for r in filled if r.trade]
    mfes = [r.trade.mfe_pct for r in filled if r.trade]

    # Opportunity cost — average % move in max_hold window after no-fill orders
    no_fill_moves = [
        r.price_move_after_pct
        for r in expired
        if r.price_move_after_pct is not None
    ]
    opp_cost_avg = float(np.mean(no_fill_moves)) if no_fill_moves else None

    # Price at expiry vs signal price (unrealised gain that was missed)
    missed_gains = [
        (r.price_at_expiry - r.signal_price) / r.signal_price * 100
        for r in expired
        if r.price_at_expiry and r.signal_price
    ]
    missed_gain_avg = float(np.mean(missed_gains)) if missed_gains else None

    # Max drawdown (from equity curve of filled trades, sorted by signal time)
    if pnls_filled:
        equity = np.cumprod([1 + p / 100 for p in pnls_filled])
        peak   = np.maximum.accumulate(equity)
        dd     = (equity - peak) / peak * 100
        max_dd = float(np.min(dd))
    else:
        max_dd = 0.0

    # Exit reason breakdown
    exit_reasons = {}
    for r in filled:
        if r.trade:
            exit_reasons[r.trade.exit_reason] = exit_reasons.get(r.trade.exit_reason, 0) + 1

    return {
        "signals":              n,
        "orders_placed":        len(orders_placed),
        "filled":               len(filled),
        "expired":              len(expired),
        "no_support":           len(no_support),
        "too_far":              len(too_far),
        "pending_blocked":      len(blocked),
        "fill_rate":            round(fill_rate * 100, 1),
        "no_fill_rate":         round(no_fill_rate * 100, 1),
        "median_ttf_hours":     round(median_ttf, 1) if median_ttf else None,
        "maker_fills":          maker_fills,
        "taker_fills":          taker_fills,
        "win_rate_pct":         round(wr * 100, 1) if wr is not None else None,
        "profit_factor":        round(pf, 3),
        "pnl_per_filled_pct":   round(pnl_per_filled, 2) if pnl_per_filled is not None else None,
        "pnl_per_signal_pct":   round(pnl_per_signal, 2) if pnl_per_signal is not None else None,
        "mae_avg_pct":          round(float(np.mean(maes)), 2) if maes else None,
        "mfe_avg_pct":          round(float(np.mean(mfes)), 2) if mfes else None,
        "max_drawdown_pct":     round(max_dd, 2),
        "exit_reasons":         exit_reasons,
        "opp_cost_avg_pct":     round(opp_cost_avg, 2) if opp_cost_avg is not None else None,
        "missed_gain_avg_pct":  round(missed_gain_avg, 2) if missed_gain_avg is not None else None,
    }


# ── Period runner ─────────────────────────────────────────────────────────────

def replay_period(
    asset: str,
    period: dict,
    model: FillModel = FillModel.OPTIMISTIC,
    verbose: bool = True,
) -> dict:
    """
    Run execution-parity replay for a named period.
    Returns {"model": ..., "stats": ..., "results": list[SignalResult]}.

    Signal detection delegates to scan_asset() to guarantee the same set of
    signals as the canonical backtest (identical skip_until, whipsaw guard, etc.).
    """
    from backtesting.signal_scanner import scan_asset

    asset_cfg  = ASSET_CONFIG.get(asset, ASSET_CONFIG["ZEC-USD"])
    atr_stop   = asset_cfg["atr_stop"]
    atr_target = asset_cfg["atr_target"]
    max_hold_h = period.get("max_hold_hours", 36)

    if verbose:
        print(f"\n[ExecReplay] {asset}  period={period.get('name','?')}  model={model.value}")

    # Step 1: Get exact signal list from scan_asset (same as canonical backtest)
    backtest = scan_asset(asset, period)
    bt_signals = backtest.get("signals", [])
    if not bt_signals:
        if verbose:
            print("  No signals found by scan_asset")
        return {"model": model.value, "stats": {}, "results": []}

    # Step 2: Rebuild the same df to find candle indices for each signal
    warmup_start = period.get("warmup", period["start"])
    df = _download_and_compute(asset, warmup_start, period["end"], "1h")
    if df is None or df.empty:
        return {"model": model.value, "stats": {}, "results": []}

    # Step 3: Map signal timestamps → df indices
    # scan_asset signal timestamps format: "2026-07-06 21:00"
    ts_to_i: dict[str, int] = {
        df.index[i].strftime("%Y-%m-%d %H:%M"): i
        for i in range(len(df))
    }

    signal_indices: list[int] = []
    signal_er30s:   list[float | None] = []

    for sig in bt_signals:
        ts_str = sig.get("timestamp", "")
        if ts_str in ts_to_i:
            signal_indices.append(ts_to_i[ts_str])
            signal_er30s.append(sig.get("regime", {}).get("er_30"))
        else:
            if verbose:
                print(f"  Warning: timestamp '{ts_str}' not found in df index")

    if verbose:
        print(f"  {len(bt_signals)} backtest signals -> {len(signal_indices)} mapped to df")

    results = replay_signals(
        df, signal_indices, signal_er30s, model,
        atr_stop, atr_target, max_hold_h,
    )
    stats = compute_stats(results)
    return {"model": model.value, "stats": stats, "results": results}


def compare_models(asset: str, period: dict, verbose: bool = True) -> dict:
    """
    Compare OPTIMISTIC vs CONSERVATIVE fill models for a period.
    (MINUTELY requires 1m data — deferred.)
    Returns dict keyed by model name.
    """
    out = {}
    for model in [FillModel.OPTIMISTIC, FillModel.CONSERVATIVE]:
        run = replay_period(asset, period, model=model, verbose=verbose)
        out[model.value] = run["stats"]
    return out


def print_comparison(comparison: dict) -> None:
    """Pretty-print the side-by-side model comparison."""
    keys = [
        "signals", "orders_placed", "filled", "fill_rate", "no_fill_rate",
        "median_ttf_hours", "maker_fills", "taker_fills",
        "win_rate_pct", "profit_factor",
        "pnl_per_filled_pct", "pnl_per_signal_pct",
        "mae_avg_pct", "mfe_avg_pct", "max_drawdown_pct",
        "opp_cost_avg_pct", "missed_gain_avg_pct",
        "expired", "no_support", "too_far", "pending_blocked",
    ]
    models = list(comparison.keys())
    print(f"\n{'Metric':35s}  " + "  ".join(f"{m:>14}" for m in models))
    print("-" * (37 + 16 * len(models)))
    for k in keys:
        vals = [str(comparison[m].get(k, "—")) for m in models]
        print(f"  {k:33s}  " + "  ".join(f"{v:>14}" for v in vals))
    print()
    for m in models:
        er = comparison[m].get("exit_reasons", {})
        print(f"  {m} exit reasons: {er}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Execution-parity replay")
    parser.add_argument("--asset", default="ZEC-USD")
    parser.add_argument("--period", default="recent_year",
                        help="Period name from PERIODS dict, or 'jul69' for golden fixture")
    parser.add_argument("--model", default="both",
                        choices=["optimistic", "conservative", "both"])
    args = parser.parse_args()

    if args.period == "jul69":
        # Golden fixture: Jul 6-9 2026 episode
        period = {
            "name":   "jul_6_9_golden",
            "warmup": "2026-05-01",
            "start":  "2026-07-06",
            "end":    "2026-07-10",
        }
    else:
        period = dict(PERIODS[args.period])
        period["name"] = args.period

    if args.model == "both":
        cmp = compare_models(args.asset, period)
        print_comparison(cmp)
    else:
        model_enum = FillModel(args.model)
        run = replay_period(args.asset, period, model=model_enum)
        print_comparison({args.model: run["stats"]})
