"""
V3 Shadow Journal — immutable append-only record for every V2 signal during
the forward OOS window (2026-07-12+).

Append-only design: never overwrite existing lines. Outcomes are added as
separate events. summarise_journal() folds events into per-signal outcomes.

JSONL schema — one JSON object per line:
  type V3_SIGNAL:
    signal_id      — "{asset}:{candle_close}:v3"  (idempotency key)
    asset
    candle_close   — ISO timestamp of last fully-closed 1h candle
    entry_price    — close price at signal time
    atr            — ATR at signal time (used to recover stop/target for resolver)
    adx, vol_ratio, n_conditions, conf
    er_30, vm_30, ema50_slope
    ema200_valid   — bool: were 200+ daily bars available?
    n_daily_bars
    v3_candidate_threshold   — locked at 0.20
    v3_would_block           — True if er_30 < threshold (research field)
    v3_enforcement           — bool: was enforcement on at signal time?
    accepted                 — True if trade was placed (not blocked by enforcement)

  type V3_OUTCOME:
    signal_id
    outcome        — "WIN" | "LOSS" | "MAX_HOLD"
    pnl_pct        — actual P&L (accepted trades) or counterfactual (blocked)
    is_counterfactual — bool

File: logs/v3_journal.jsonl
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT     = Path(__file__).resolve().parents[1]
_JOURNAL = ROOT / "logs" / "v3_journal.jsonl"

# Episode gap threshold: signals more than this many days apart start a new episode
_EPISODE_GAP_DAYS = 30


# ── Write helpers ─────────────────────────────────────────────────────────────

def _write(record: dict) -> None:
    _JOURNAL.parent.mkdir(parents=True, exist_ok=True)
    with _JOURNAL.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=str) + "\n")


def log_v2_signal(*, scanner_signal: dict, accepted: bool) -> str:
    """
    Log a scanner signal at fire time (before any trade is placed).
    Returns the stable signal_id.
    """
    asset        = scanner_signal["asset"]
    candle_close = scanner_signal["entry_time"]
    signal_id    = f"{asset}:{candle_close}:v3"

    record: dict = {
        "type":                    "V3_SIGNAL",
        "logged_at_utc":           datetime.now(timezone.utc).isoformat(),
        "signal_id":               signal_id,
        "asset":                   asset,
        "candle_close":            candle_close,
        "entry_price":             scanner_signal.get("entry_price"),
        "atr":                     scanner_signal.get("atr"),
        "conf":                    scanner_signal.get("conf"),
        "adx":                     scanner_signal.get("adx"),
        "vol_ratio":               scanner_signal.get("vol_ratio"),
        "n_conditions":            scanner_signal.get("n_conditions"),
        "er_30":                   scanner_signal.get("er_30"),
        "vm_30":                   scanner_signal.get("vm_30"),
        "ema50_slope":             scanner_signal.get("ema50_slope"),
        "ema200_valid":            scanner_signal.get("ema200_valid"),
        "n_daily_bars":            scanner_signal.get("n_daily_bars"),
        "v3_candidate_threshold":  scanner_signal.get("v3_candidate_threshold"),
        "v3_would_block":          scanner_signal.get("v3_would_block", False),
        "v3_enforcement":          scanner_signal.get("v3_enforcement", False),
        "accepted":                accepted,
    }
    _write(record)
    return signal_id


def log_outcome(signal_id: str, outcome: str, pnl_pct: float, *, is_counterfactual: bool = False) -> None:
    """Append an outcome record for an accepted or blocked (counterfactual) trade."""
    _write({
        "type":              "V3_OUTCOME",
        "logged_at_utc":     datetime.now(timezone.utc).isoformat(),
        "signal_id":         signal_id,
        "outcome":           outcome,
        "pnl_pct":           round(pnl_pct, 4),
        "is_counterfactual": is_counterfactual,
    })


# ── Read helpers ──────────────────────────────────────────────────────────────

def read_journal() -> list[dict]:
    if not _JOURNAL.exists():
        return []
    entries = []
    with _JOURNAL.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return entries


def _build_signal_view(entries: list[dict]) -> list[dict]:
    """
    Fold all events into one dict per signal_id.
    Outcome events (V3_OUTCOME) are merged into the base V3_SIGNAL record.
    """
    signals:  dict[str, dict] = {}
    outcomes: dict[str, dict] = {}
    for e in entries:
        if e.get("type") == "V3_SIGNAL":
            signals[e["signal_id"]] = dict(e)
        elif e.get("type") == "V3_OUTCOME":
            sid = e["signal_id"]
            # Last outcome wins if there are duplicates (resolver idempotency)
            outcomes[sid] = e

    result = []
    for sid, sig in signals.items():
        if sid in outcomes:
            sig = dict(sig)
            sig["outcome"]           = outcomes[sid]["outcome"]
            sig["pnl_pct"]           = outcomes[sid]["pnl_pct"]
            sig["is_counterfactual"] = outcomes[sid].get("is_counterfactual", False)
        else:
            sig["outcome"]           = None
            sig["pnl_pct"]           = None
            sig["is_counterfactual"] = None
        result.append(sig)

    result.sort(key=lambda s: s.get("candle_close", ""))
    return result


# ── Episode grouping ──────────────────────────────────────────────────────────

def _group_episodes(signals: list[dict]) -> list[list[dict]]:
    """
    Group signals into episodes: signals within _EPISODE_GAP_DAYS of each other
    (measured from one signal's candle_close to the next) belong to the same episode.
    Algorithm is deterministic and machine-checkable.
    """
    if not signals:
        return []

    sorted_sigs = sorted(signals, key=lambda s: s.get("candle_close", ""))
    episodes: list[list[dict]] = []
    current: list[dict] = [sorted_sigs[0]]

    for sig in sorted_sigs[1:]:
        prev_t = pd.Timestamp(current[-1]["candle_close"])
        curr_t = pd.Timestamp(sig["candle_close"])
        gap_days = (curr_t - prev_t).total_seconds() / 86400
        if gap_days <= _EPISODE_GAP_DAYS:
            current.append(sig)
        else:
            episodes.append(current)
            current = [sig]
    episodes.append(current)
    return episodes


# ── Counterfactual resolver ───────────────────────────────────────────────────

def reconcile_pending(asset: str = "ZEC-USD", max_hold_hours: int = 36) -> int:
    """
    Compute outcomes for blocked signals (counterfactuals) that are still PENDING.
    Uses the Coinbase parquet cache — requires data from candle_close onwards.

    Returns the number of outcomes filled in.
    """
    from exchange.coinbase_candles import download as _cb_download
    from backtesting.signal_scanner import ASSET_CONFIG

    cfg        = ASSET_CONFIG.get(asset, {})
    atr_stop   = cfg.get("atr_stop", 2.0)
    atr_target = cfg.get("atr_target", 3.5)

    entries    = read_journal()
    view       = _build_signal_view(entries)
    resolved_ids = {e["signal_id"] for e in entries if e.get("type") == "V3_OUTCOME"}

    to_resolve = [
        s for s in view
        if s.get("asset") == asset
        and not s.get("accepted", True)          # blocked signal
        and s.get("outcome") is None             # no outcome yet
        and s["signal_id"] not in resolved_ids
    ]

    if not to_resolve:
        return 0

    # Download 1h data covering the latest period needed
    earliest = min(s["candle_close"] for s in to_resolve)
    now_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df_1h    = _cb_download(asset, start=earliest, end=now_str, granularity="1h", verbose=False)
    if df_1h.empty:
        return 0

    df_1h = df_1h.set_index("time")
    df_1h.index = pd.to_datetime(df_1h.index, utc=True)

    resolved = 0
    for sig in to_resolve:
        sig_ts = pd.Timestamp(sig["candle_close"], tz="UTC") if "UTC" not in str(sig["candle_close"]) else pd.Timestamp(sig["candle_close"])
        after  = df_1h[df_1h.index > sig_ts].head(max_hold_hours)
        if after.empty:
            continue  # data not yet available — skip for now

        entry_price = float(sig["entry_price"])
        # Recover stop/target from ATR stored in journal (or from first bar if missing)
        atr_val = sig.get("atr") or float(after.iloc[0]["close"]) * 0.02
        stop_price   = entry_price - atr_stop   * float(atr_val)
        target_price = entry_price + atr_target * float(atr_val)

        # Simulate: scan bars sequentially, check stop and target
        outcome = "MAX_HOLD"
        exit_price = float(after.iloc[-1]["close"])
        for _, bar in after.iterrows():
            lo = float(bar["low"])
            hi = float(bar["high"])
            if lo <= stop_price:
                outcome    = "LOSS"
                exit_price = stop_price
                break
            if hi >= target_price:
                outcome    = "WIN"
                exit_price = target_price
                break

        # Same fee model as historical backtest
        _ENTRY_FEE = 0.004
        _TP_FEE    = 0.004
        _SL_FEE    = 0.006
        fee = _TP_FEE if outcome == "WIN" else _SL_FEE
        pnl_pct = (exit_price - entry_price) / entry_price - _ENTRY_FEE - fee

        log_outcome(sig["signal_id"], outcome, pnl_pct * 100, is_counterfactual=True)
        resolved += 1

    return resolved


# ── Summary ───────────────────────────────────────────────────────────────────

def summarise_journal() -> None:
    """
    Print forward OOS statistics. Folds events into per-signal outcomes,
    groups by episode, and checks the 5-point activation criteria.
    """
    entries = read_journal()
    if not entries:
        print("V3 journal is empty.")
        return

    view = _build_signal_view(entries)
    accepted = [s for s in view if s.get("accepted")]
    blocked  = [s for s in view if not s.get("accepted")]
    closed_a = [s for s in accepted if s.get("pnl_pct") is not None]
    closed_b = [s for s in blocked  if s.get("pnl_pct") is not None]

    print(f"\nV3 Forward OOS Journal — {len(view)} signals total")
    print(f"  Accepted  : {len(accepted)} ({len(closed_a)} closed)")
    print(f"  Blocked   : {len(blocked)} shadow ({len(closed_b)} resolved)")

    if not closed_a:
        print("\n  No closed accepted trades yet.")
        return

    wins   = [s for s in closed_a if s["pnl_pct"] > 0]
    losses = [s for s in closed_a if s["pnl_pct"] <= 0]
    gw     = sum(s["pnl_pct"] for s in wins)
    gl     = abs(sum(s["pnl_pct"] for s in losses))
    pf     = gw / gl if gl else float("inf")
    avg    = sum(s["pnl_pct"] for s in closed_a) / len(closed_a)
    wr     = len(wins) / len(closed_a)

    print(f"\n  V3 Accepted trades (n={len(closed_a)})")
    print(f"    Win rate     : {wr:.0%}")
    print(f"    Profit factor: {pf:.3f}")
    print(f"    Avg P&L      : {avg:+.2f}%")

    # Stress test: +0.25% per-trade friction
    adj_returns = [s["pnl_pct"] - 0.25 for s in closed_a]
    adj_w = [r for r in adj_returns if r > 0]
    adj_l = [r for r in adj_returns if r <= 0]
    adj_gw = sum(adj_w)
    adj_gl = abs(sum(adj_l))
    adj_pf  = adj_gw / adj_gl if adj_gl else float("inf")
    adj_avg = sum(adj_returns) / len(adj_returns)
    print(f"\n  After +0.25% friction stress:")
    print(f"    Profit factor: {adj_pf:.3f}  avg: {adj_avg:+.2f}%")

    # Episode grouping: adjacent signals <= 30 days apart = same episode
    episodes = _group_episodes(closed_a)
    ep_pnls  = [sum(s["pnl_pct"] for s in ep if s["pnl_pct"] > 0) for ep in episodes]
    max_ep_contribution = max(ep_pnls) / gw if gw > 0 and ep_pnls else 0.0
    print(f"\n  Episodes ({_EPISODE_GAP_DAYS}d gap rule): {len(episodes)}")
    print(f"    Largest episode's share of gross profit: {max_ep_contribution:.0%}")

    # 5-point activation criteria
    print(f"\n  5-point OOS activation criteria (threshold 0.20 locked):")
    c1 = len(closed_a) >= 20
    c2 = pf > 1.20
    try:
        import numpy as np
        from backtesting.bootstrap_analysis import _block_bootstrap_pf
        pfs = _block_bootstrap_pf([s["pnl_pct"] for s in closed_a], block_size=4, n_iter=10_000)
        p_above = float((pfs[np.isfinite(pfs)] > 1.0).mean() * 100)
        c3 = p_above > 90.0
        c3_str = f"{p_above:.1f}% > 90%"
    except Exception:
        c3 = False
        c3_str = "n/a (bootstrap unavailable)"
    c4 = adj_avg > 0
    c5 = max_ep_contribution < 0.50

    for label, ok, detail in [
        ("n >= 20 closed trades",          c1, f"{len(closed_a)}"),
        ("PF > 1.20",                      c2, f"{pf:.3f}"),
        (f"P_bootstrap(PF>1) > 90%",       c3, c3_str),
        ("Avg > 0% after +0.25% friction", c4, f"{adj_avg:+.2f}%"),
        ("No episode > 50% gross profit",  c5, f"{max_ep_contribution:.0%}"),
    ]:
        status = "PASS" if ok else "FAIL"
        print(f"    [{status}] {label}: {detail}")

    all_pass = all([c1, c2, c3, c4, c5])
    print(f"\n  {'=== ALL CRITERIA MET — V3 activation warranted ===' if all_pass else '--- Criteria not yet met --- keep accumulating OOS data'}")
    print()


if __name__ == "__main__":
    import sys
    if "--reconcile" in sys.argv:
        n = reconcile_pending()
        print(f"Resolved {n} counterfactual outcomes.")
    else:
        summarise_journal()
