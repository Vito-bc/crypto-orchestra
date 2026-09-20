"""Idempotently append raw forward price paths; never compute aggregate results."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd

from agent_shadow.log_store import DEFAULT_LOG, ShadowLog
from backtesting import signal_scanner as scanner

_HORIZONS = {"4h": 4, "24h": 24, "72h": 72, "7d": 168}


def _attachment_id(candidate_id: str) -> str:
    return hashlib.sha256(f"{candidate_id}|price-path-v1".encode()).hexdigest()[:24]


def _bracket(frame: pd.DataFrame, entry_i: int, candidate: dict) -> dict:
    entry = float(candidate["entry_price"])
    stop = round(entry - float(candidate["atr_stop"]) * float(candidate["atr"]), 2)
    target = round(entry + float(candidate["atr_target"]) * float(candidate["atr"]), 2)
    hold = int(candidate["max_hold_hours"])
    for offset in range(1, hold + 1):
        row = frame.iloc[entry_i + offset]
        if float(row["low"]) <= stop:
            return {
                "outcome": "STOP_LOSS",
                "hours": offset,
                "price": stop,
                "stop_price": stop,
                "target_price": target,
            }
        if float(row["high"]) >= target:
            return {
                "outcome": "TAKE_PROFIT",
                "hours": offset,
                "price": target,
                "stop_price": stop,
                "target_price": target,
            }
    return {
        "outcome": "MAX_HOLD",
        "hours": hold,
        "price": float(frame.iloc[entry_i + hold]["close"]),
        "stop_price": stop,
        "target_price": target,
    }


def attach_price_paths(
    log: ShadowLog,
    *,
    now: datetime | None = None,
    frame_builder: Callable | None = None,
) -> int:
    records = log.records()
    candidates: dict[str, dict] = {}
    attached = {
        record.get("candidate_id")
        for record in records
        if record.get("record_type") == "price_attachment"
    }
    for record in records:
        if record.get("record_type") in {"agent_vote", "orchestrator_decision"}:
            candidates.setdefault(record["candidate_id"], record)
    pending = [value for key, value in candidates.items() if key not in attached]
    if not pending:
        return 0

    current = now or datetime.now(timezone.utc)
    build = frame_builder or scanner.build_merged_frame
    written = 0
    for asset in sorted({item["asset"] for item in pending}):
        asset_items = [item for item in pending if item["asset"] == asset]
        earliest = min(pd.Timestamp(item["candle_time"]) for item in asset_items)
        cfg = scanner.ASSET_CONFIG[asset]
        btc_applicable = asset != "BTC-USD" and bool(cfg.get("btc_regime_filter", False))
        frame, _ = build(
            asset,
            (earliest - pd.Timedelta(days=120)).date().isoformat(),
            (pd.Timestamp(current) + pd.Timedelta(days=1)).date().isoformat(),
            cfg,
            btc_regime_applicable=btc_applicable,
        )
        if frame is None or frame.empty:
            continue
        for candidate in asset_items:
            candle_time = pd.Timestamp(candidate["candle_time"])
            if candle_time not in frame.index:
                continue
            entry_i = frame.index.get_loc(candle_time)
            if not isinstance(entry_i, int):
                continue
            if entry_i + max(_HORIZONS.values()) >= len(frame):
                continue
            horizon_values = {}
            for label, hours in _HORIZONS.items():
                close = float(frame.iloc[entry_i + hours]["close"])
                horizon_values[label] = {
                    "close": close,
                    "return": close / float(candidate["entry_price"]) - 1.0,
                }
            log.append(
                {
                    "record_type": "price_attachment",
                    "attachment_id": _attachment_id(candidate["candidate_id"]),
                    "candidate_id": candidate["candidate_id"],
                    "event_id": candidate["event_id"],
                    "variant_id": candidate["variant_id"],
                    "asset": asset,
                    "candle_time": candidate["candle_time"],
                    "price_path": horizon_values,
                    "atr_bracket": _bracket(frame, entry_i, candidate),
                }
            )
            attached.add(candidate["candidate_id"])
            written += 1
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    args = parser.parse_args(argv)
    written = attach_price_paths(ShadowLog(args.log))
    print(json.dumps({"price_attachments_appended": written}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
