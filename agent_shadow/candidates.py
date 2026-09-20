"""Produce non-trading WIDE candidates with the frozen scanner read path."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

import pandas as pd

from backtesting import signal_scanner as scanner


@dataclass(frozen=True)
class Candidate:
    event_id: str
    asset: str
    candle_time: str
    n_met: int
    entry_price: float
    atr: float
    atr_stop: float
    atr_target: float
    max_hold_hours: int

    def as_dict(self) -> dict:
        return asdict(self)


def _event_id(asset: str, candle_time: str) -> str:
    raw = f"wide-v1|{asset}|{candle_time}".encode()
    return hashlib.sha256(raw).hexdigest()[:24]


def _btc_regime_applicable(asset: str, cfg: dict) -> bool:
    return asset != "BTC-USD" and bool(cfg.get("btc_regime_filter", False))


def produce_candidates(
    assets: Iterable[str],
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    *,
    latest_only: bool = False,
    now: datetime | None = None,
) -> list[Candidate]:
    """Return trigger + declared-hard-gate candidates before ``min_conditions``."""
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize("UTC")
    else:
        start_ts = start_ts.tz_convert("UTC")
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize("UTC")
    else:
        end_ts = end_ts.tz_convert("UTC")

    closed_cutoff: pd.Timestamp | None = None
    if latest_only:
        current = pd.Timestamp(now or datetime.now(timezone.utc))
        if current.tzinfo is None:
            current = current.tz_localize("UTC")
        else:
            current = current.tz_convert("UTC")
        closed_cutoff = current.floor("h") - pd.Timedelta(hours=1)

    found: list[Candidate] = []
    for asset in assets:
        if asset not in scanner.ASSET_CONFIG:
            raise ValueError(f"asset has no declared scanner config: {asset}")
        cfg = scanner.ASSET_CONFIG[asset]
        btc_applicable = _btc_regime_applicable(asset, cfg)
        frame, _ = scanner.build_merged_frame(
            asset,
            start_ts.date().isoformat(),
            end_ts.date().isoformat(),
            cfg,
            btc_regime_applicable=btc_applicable,
        )
        if frame is None or frame.empty:
            continue
        eligible = frame[(frame.index >= start_ts) & (frame.index <= end_ts)]
        if closed_cutoff is not None:
            eligible = eligible[eligible.index <= closed_cutoff]
            if not eligible.empty:
                eligible = eligible.iloc[[-1]]
        for candle_time in eligible.index:
            position = frame.index.get_loc(candle_time)
            if not isinstance(position, int):
                raise ValueError(f"duplicate candle timestamp for {asset}: {candle_time}")
            result = scanner._detect_breakout_signal(
                frame, position, cfg, btc_regime_applicable=btc_applicable
            )
            if result is None:
                continue
            blocked = result.get("blocked")
            if blocked == "conditions":
                n_met = int(result["n_met"])
            elif blocked is None:
                n_met = int(result["n_conditions"])
            else:
                continue
            row = frame.iloc[position]
            stamp = pd.Timestamp(candle_time).isoformat()
            strategy_cfg = scanner.STRATEGY_CONFIG.get(
                asset, scanner.STRATEGY_CONFIG["ETH-USD"]
            )
            found.append(
                Candidate(
                    event_id=_event_id(asset, stamp),
                    asset=asset,
                    candle_time=stamp,
                    n_met=n_met,
                    entry_price=float(row["close"]),
                    atr=float(row["atr"]),
                    atr_stop=float(cfg["atr_stop"]),
                    atr_target=float(cfg["atr_target"]),
                    max_hold_hours=int(strategy_cfg.get("max_hold_hours", 36)),
                )
            )
    found.sort(key=lambda item: (item.candle_time, item.asset))
    return found


def latest_candidates(assets: Iterable[str], *, now: datetime | None = None) -> list[Candidate]:
    current = now or datetime.now(timezone.utc)
    return produce_candidates(
        assets,
        current - timedelta(days=120),
        current + timedelta(days=1),
        latest_only=True,
        now=current,
    )
