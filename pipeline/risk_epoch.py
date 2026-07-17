"""
Risk Epoch Manager — isolates strategy-level drawdown tracking from historical ledger.

Problem:
  Old trades (May-Jun 2026, old orchestrator, $10k paper scale) accumulated -$47.07.
  When LIVE_BALANCE_USD was set to $100, the circuit breaker saw 47% drawdown on $100
  base — a phantom figure from ledger contamination across strategy versions.

Solution:
  An "epoch" marks the start of a new strategy deployment. The strategy-level circuit
  breaker only counts trades from the current epoch. Pre-epoch trades are archived but
  do not affect new strategy's drawdown calculation.

  Global protection (real Coinbase allocation) is enforced separately at a higher
  threshold independent of epoch boundaries.

Epoch record format (appended to risk_epochs.jsonl):
  {
    "event":         "RISK_EPOCH_STARTED",
    "epoch_id":      "ZEC_V2_ADX25:2026-07-12",
    "paper_capital": 100.0,
    "reason":        "...",
    "timestamp":     "2026-07-17T..."
  }

Trade records are tagged with epoch_id in trade_history.jsonl when they close.
Old trade records (no epoch_id field) are treated as pre-epoch and excluded from
strategy-level drawdown.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

ROOT        = Path(__file__).resolve().parents[1]
EPOCHS_FILE = ROOT / "logs" / "risk_epochs.jsonl"
TRADE_HISTORY = ROOT / "logs" / "trade_history.jsonl"


def get_current_epoch() -> Optional[dict]:
    """
    Return the most recently started epoch, or None if no epoch has been started.
    Reads the append-only log and returns the last RISK_EPOCH_STARTED event.
    Raises RuntimeError if the file exists but cannot be read (fail-closed).
    """
    if not EPOCHS_FILE.exists():
        return None
    epoch = None
    try:
        text = EPOCHS_FILE.read_text(encoding="utf-8")
    except OSError as e:
        raise RuntimeError(f"risk_epochs.jsonl exists but cannot be read: {e}") from e
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError as e:
            # Any corrupt line in the risk ledger stops trading — silent skip could return
            # a stale epoch or miss a STARTED event, understating drawdown.
            raise RuntimeError(
                f"Corrupt line in risk_epochs.jsonl — trading halted until fixed: "
                f"{e!r} | line: {line[:120]!r}"
            ) from e
        if rec.get("event") == "RISK_EPOCH_STARTED":
            epoch = rec
    return epoch


def start_new_epoch(
    epoch_id: str,
    paper_capital: float,
    reason: str,
    force: bool = False,
) -> dict:
    """
    Record the start of a new risk epoch. Append-only — does NOT modify trade_history.

    Guards (all enforced before writing):
      1. force=True is blocked when DRY_RUN=false (live mode requires Coinbase reconciliation).
      2. epoch_id must be unique — duplicate would merge old trades into the new epoch.
      3. Open positions / pending orders must be zero unless force=True.

    Args:
        epoch_id:      Unique identifier, e.g. "ZEC_V2_ADX25:2026-07-12"
        paper_capital: Starting paper capital for this epoch (e.g. 100.0)
        reason:        Human-readable reason for starting this epoch
        force:         Skip the open-exposure check. Blocked in live mode.

    Returns:
        The epoch record dict that was written.
    """
    # Guard 1: force is only safe in dry-run mode
    if force:
        _live = os.getenv("DRY_RUN", "true").lower() in ("false", "0", "no")
        if _live:
            raise ValueError(
                "force=True is not allowed when DRY_RUN=false. "
                "Set DRY_RUN=true and verify Coinbase exposure before force-starting an epoch."
            )

    # Guard 2: epoch_id must be unique
    if EPOCHS_FILE.exists():
        try:
            _text = EPOCHS_FILE.read_text(encoding="utf-8")
        except OSError as e:
            raise RuntimeError(f"risk_epochs.jsonl unreadable during uniqueness check: {e}") from e
        _existing_ids: set[str] = set()
        for _line in _text.splitlines():
            _line = _line.strip()
            if not _line:
                continue
            try:
                _rec = json.loads(_line)
                _eid = _rec.get("epoch_id")
                if _eid:
                    _existing_ids.add(_eid)
            except json.JSONDecodeError as e:
                raise RuntimeError(
                    f"Corrupt line in risk_epochs.jsonl during uniqueness check: {e!r}"
                ) from e
        if epoch_id in _existing_ids:
            raise ValueError(
                f"Epoch '{epoch_id}' already exists in risk_epochs.jsonl. "
                "Duplicate epoch_id would merge old trades into this epoch. "
                "Choose a distinct epoch_id (e.g. append a suffix or different date)."
            )

    # Guard 3: no open exposure (unless force=True)
    if not force:
        _positions_file = ROOT / "logs" / "open_positions.json"
        _orders_file    = ROOT / "logs" / "pending_orders.json"

        open_pos_count = 0
        if _positions_file.exists():
            try:
                positions = json.loads(_positions_file.read_text(encoding="utf-8"))
                open_pos_count = sum(1 for p in positions if p.get("status") == "OPEN")
            except (json.JSONDecodeError, OSError):
                open_pos_count = -1  # unreadable → treat as unknown exposure

        pending_count = 0
        if _orders_file.exists():
            try:
                orders = json.loads(_orders_file.read_text(encoding="utf-8"))
                pending_count = sum(1 for o in orders if o.get("status") == "OPEN")
            except (json.JSONDecodeError, OSError):
                pending_count = -1  # unreadable → treat as unknown exposure

        if open_pos_count != 0:
            raise ValueError(
                f"Cannot start new epoch: {open_pos_count} open position(s) detected "
                f"(or exposure file unreadable). Close all positions first, then retry."
            )
        if pending_count != 0:
            raise ValueError(
                f"Cannot start new epoch: {pending_count} pending order(s) detected "
                f"(or orders file unreadable). Cancel all orders first, then retry."
            )

    EPOCHS_FILE.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "event":         "RISK_EPOCH_STARTED",
        "epoch_id":      epoch_id,
        "paper_capital": float(paper_capital),
        "reason":        reason,
        "timestamp":     datetime.now(timezone.utc).isoformat(),
    }
    with EPOCHS_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
    return record


def get_epoch_trades(epoch_id: str) -> list[dict]:
    """
    Return all trade records tagged with this epoch_id from trade_history.jsonl.
    Old records without an epoch_id field are treated as pre-epoch and excluded.
    Raises RuntimeError if the file exists but cannot be read (fail-closed).
    """
    if not TRADE_HISTORY.exists():
        return []
    try:
        text = TRADE_HISTORY.read_text(encoding="utf-8")
    except OSError as e:
        raise RuntimeError(f"trade_history.jsonl exists but cannot be read: {e}") from e
    trades = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError as e:
            # A corrupt trade record could silently drop a loss, understating drawdown.
            raise RuntimeError(
                f"Corrupt line in trade_history.jsonl — trading halted until fixed: "
                f"{e!r} | line: {line[:120]!r}"
            ) from e
        if rec.get("epoch_id") == epoch_id:
            trades.append(rec)
    return trades


def compute_epoch_drawdown(epoch: dict) -> tuple[float, float, float]:
    """
    Compute (equity, peak, drawdown_pct) for the given epoch.

    equity       = paper_capital + sum(pnl_usd for epoch trades)
    peak         = paper_capital + max(0, cumulative high-water mark over equity curve)
    drawdown_pct = (peak - equity) / peak * 100

    Returns (equity, peak, drawdown_pct).
    """
    paper_capital = epoch["paper_capital"]
    trades = get_epoch_trades(epoch["epoch_id"])

    equity = paper_capital
    peak   = paper_capital
    for t in sorted(trades, key=lambda x: x.get("exit_time", x.get("closed_at_utc", ""))):
        equity += t.get("pnl_usd", 0.0)
        if equity > peak:
            peak = equity

    drawdown_pct = (peak - equity) / peak * 100 if peak > 0 else 0.0
    return round(equity, 4), round(peak, 4), round(drawdown_pct, 4)
