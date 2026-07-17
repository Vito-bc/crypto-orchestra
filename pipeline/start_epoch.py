"""
CLI to start a new risk epoch — the controlled, auditable way to reset
the strategy-level circuit breaker after a strategy version change.

Usage:
    python pipeline/start_epoch.py                         # uses defaults
    python pipeline/start_epoch.py --id ZEC_V2:2026-07-12 --capital 100 --reason "V2 ADX25 forward OOS"
    python pipeline/start_epoch.py --dry-run               # show what would be written

What this does:
  1. Reads the current epoch (if any) and shows its final state.
  2. Writes a RISK_EPOCH_STARTED record to logs/risk_epochs.jsonl.
  3. Does NOT touch trade_history.jsonl — old trades are preserved.

What this does NOT do:
  - Does not edit any balances or ledger files.
  - Does not reset the global account protection floor.
  - Does not give permission to take larger risk than the new epoch capital allows.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from pipeline.risk_epoch import compute_epoch_drawdown, get_current_epoch, start_new_epoch


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Start a new risk epoch (controlled circuit-breaker reset)"
    )
    parser.add_argument(
        "--id", dest="epoch_id",
        default=f"ZEC_V2_ADX25:{date.today().isoformat()}",
        help="Epoch identifier, e.g. ZEC_V2_ADX25:2026-07-12"
    )
    parser.add_argument("--capital", type=float, default=100.0,
                        help="Starting paper capital for this epoch (USD)")
    parser.add_argument("--reason", default="",
                        help="Human-readable reason for this epoch start")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be written without writing")
    args = parser.parse_args()

    # Show current state
    current = get_current_epoch()
    if current:
        equity, peak, dd = compute_epoch_drawdown(current)
        print(f"Current epoch:  {current['epoch_id']}")
        print(f"  paper_capital=${current['paper_capital']:.2f}  "
              f"equity=${equity:.2f}  peak=${peak:.2f}  DD={dd:.1f}%")
        print(f"  started:       {current['timestamp'][:16]}")
    else:
        print("No active epoch — this will be the first.")
        print("All 7 pre-epoch trades (-$47.07) will be excluded from the new epoch's circuit breaker.")

    reason = args.reason or (
        "V2 momentum scanner (EMA50 + ADX25 + vol) forward OOS epoch. "
        "Old agent-orchestrator trades excluded — different strategy, different capital scale ($10k→$100)."
    )

    record = {
        "event":         "RISK_EPOCH_STARTED",
        "epoch_id":      args.epoch_id,
        "paper_capital": args.capital,
        "reason":        reason,
    }

    print()
    print(f"New epoch:      {args.epoch_id}")
    print(f"  paper_capital=${args.capital:.2f}")
    print(f"  reason:       {reason}")

    if args.dry_run:
        print()
        print("[DRY RUN] Would write:")
        print(json.dumps(record, indent=2))
        print("No files were modified.")
        return

    written = start_new_epoch(args.epoch_id, args.capital, reason)
    print()
    print(f"Written to logs/risk_epochs.jsonl:")
    print(json.dumps(written, indent=2))
    print()
    print("Circuit breaker will now ignore all pre-epoch trades.")
    print("Scanner and shadow journal continue running normally.")


if __name__ == "__main__":
    main()
