"""Produce non-trading WIDE candidates with the frozen scanner read path."""

from __future__ import annotations

import hashlib
import sys
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping

import pandas as pd

from backtesting import signal_scanner as scanner

# How far back ONE run will reach when resuming after a gap, measured back
# from the newest closed bar. Three days covers a weekend-length sleep with
# room to spare; it exists so an unnoticed month-long outage cannot turn one
# hourly invocation into a 700-bar replay. Bars older than this at the time
# of the run are never examined — see ScanResult.truncated, which is logged
# loudly and written to the shadow log.
DEFAULT_LOOKBACK_HOURS = 72

# Warm-up the frozen scanner needs before the first examined bar (unchanged
# from the single-bar path this generalises).
_WARMUP_DAYS = 120


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
    data_providers: dict[str, str] = field(default_factory=dict)

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class ScanResult:
    """
    What one resume-aware scan examined, not only what it found.

    ``examined_through`` is the newest closed bar actually examined per asset.
    It is what the runner's ``scan_checkpoint`` records, so the next run can
    tell "examined, nothing found" apart from "never examined" — a
    distinction candidate records alone cannot make.
    """

    candidates: list[Candidate]
    closed_cutoff: str
    examined_through: dict[str, str]
    bars_examined: dict[str, int]
    cold_start: list[str]
    truncated: dict[str, dict]


def _event_id(asset: str, candle_time: str) -> str:
    raw = f"wide-v1|{asset}|{candle_time}".encode()
    return hashlib.sha256(raw).hexdigest()[:24]


def _cross_event_id(asset: str, frame: pd.DataFrame, position: int) -> str:
    """Key an event to the scanner's EMA50 cross, not its later eligible bars.

    The scanner calls the consecutive above-EMA count ``candles_above`` and
    reads its cross row at ``position - candles_above + 1``. Only call this
    after that scanner has accepted a WIDE candidate, so its 1..4-bar trigger
    and every hard gate have already been checked.
    """
    close = frame["close"].to_numpy()
    ema50 = frame["ema50"].to_numpy()
    candles_above = 0
    for index in range(position, position - scanner._MAX_CANDLES_SINCE, -1):
        if index < 0 or not close[index] > ema50[index]:
            break
        candles_above += 1
    if candles_above == 0:
        raise ValueError("scanner candidate has no EMA50 cross")
    cross_time = pd.Timestamp(frame.index[position - candles_above + 1]).isoformat()
    return hashlib.sha256(f"event-v1|{asset}|{cross_time}".encode()).hexdigest()[:24]


def _btc_regime_applicable(asset: str, cfg: dict) -> bool:
    return asset != "BTC-USD" and bool(cfg.get("btc_regime_filter", False))


def _utc(value: str | pd.Timestamp | datetime) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def closed_cutoff(now: datetime | None = None) -> pd.Timestamp:
    """Open time of the newest 1h bar that has fully closed at ``now``."""
    return _utc(now or datetime.now(timezone.utc)).floor("h") - pd.Timedelta(hours=1)


def _asset_frame(
    asset: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp, strict_coinbase_only: bool
) -> tuple[pd.DataFrame | None, dict, dict[str, str], bool]:
    if asset not in scanner.ASSET_CONFIG:
        raise ValueError(f"asset has no declared scanner config: {asset}")
    cfg = scanner.ASSET_CONFIG[asset]
    btc_applicable = _btc_regime_applicable(asset, cfg)
    providers: dict[str, str] = {}
    token = scanner._PROVIDER_AUDIT.set(providers)
    previous_strict = scanner.STRICT_COINBASE_ONLY
    if strict_coinbase_only:
        scanner.STRICT_COINBASE_ONLY = True
    try:
        frame, _ = scanner.build_merged_frame(
            asset,
            start_ts.date().isoformat(),
            end_ts.date().isoformat(),
            cfg,
            btc_regime_applicable=btc_applicable,
        )
    finally:
        scanner.STRICT_COINBASE_ONLY = previous_strict
        scanner._PROVIDER_AUDIT.reset(token)
    return frame, cfg, providers, btc_applicable


def _candidate_at(
    frame: pd.DataFrame,
    candle_time: pd.Timestamp,
    asset: str,
    cfg: dict,
    btc_applicable: bool,
    providers: dict[str, str],
) -> Candidate | None:
    position = frame.index.get_loc(candle_time)
    if not isinstance(position, int):
        raise ValueError(f"duplicate candle timestamp for {asset}: {candle_time}")
    result = scanner._detect_breakout_signal(
        frame, position, cfg, btc_regime_applicable=btc_applicable
    )
    if result is None:
        return None
    blocked = result.get("blocked")
    if blocked == "conditions":
        n_met = int(result["n_met"])
    elif blocked is None:
        n_met = int(result["n_conditions"])
    else:
        return None
    row = frame.iloc[position]
    stamp = pd.Timestamp(candle_time).isoformat()
    strategy_cfg = scanner.STRATEGY_CONFIG.get(asset, scanner.STRATEGY_CONFIG["ETH-USD"])
    return Candidate(
        event_id=_event_id(asset, stamp),
        asset=asset,
        candle_time=stamp,
        n_met=n_met,
        entry_price=float(row["close"]),
        atr=float(row["atr"]),
        atr_stop=float(cfg["atr_stop"]),
        atr_target=float(cfg["atr_target"]),
        max_hold_hours=int(strategy_cfg.get("max_hold_hours", 36)),
        data_providers=dict(providers),
    )


def produce_candidates(
    assets: Iterable[str],
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    *,
    latest_only: bool = False,
    now: datetime | None = None,
    strict_coinbase_only: bool = False,
) -> list[Candidate]:
    """Return trigger + declared-hard-gate candidates before ``min_conditions``."""
    start_ts = _utc(start)
    end_ts = _utc(end)
    cutoff = closed_cutoff(now) if latest_only else None

    found: list[Candidate] = []
    for asset in assets:
        frame, cfg, providers, btc_applicable = _asset_frame(
            asset, start_ts, end_ts, strict_coinbase_only
        )
        if frame is None or frame.empty:
            continue
        eligible = frame[(frame.index >= start_ts) & (frame.index <= end_ts)]
        if cutoff is not None:
            eligible = eligible[eligible.index <= cutoff]
            if not eligible.empty:
                eligible = eligible.iloc[[-1]]
        for candle_time in eligible.index:
            candidate = _candidate_at(frame, candle_time, asset, cfg, btc_applicable, providers)
            if candidate is not None:
                found.append(candidate)
    found.sort(key=lambda item: (item.candle_time, item.asset))
    return found


def latest_candidates(assets: Iterable[str], *, now: datetime | None = None) -> list[Candidate]:
    current = now or datetime.now(timezone.utc)
    return produce_candidates(
        assets,
        current - timedelta(days=_WARMUP_DAYS),
        current + timedelta(days=1),
        latest_only=True,
        now=current,
        strict_coinbase_only=True,
    )


def scan_since(
    assets: Iterable[str],
    resume_from: Mapping[str, pd.Timestamp | None],
    *,
    now: datetime | None = None,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    per_event: bool = False,
) -> ScanResult:
    """
    Examine every closed bar from each asset's resume point to the newest
    closed bar, instead of only the newest one.

    ``resume_from[asset]`` is the first bar that still needs examining; the
    runner derives it from the shadow log. ``None`` is a cold start: only the
    newest closed bar is examined, exactly as the single-bar path did, rather
    than replaying the whole warm-up window as if it were missed.

    A resume point older than ``lookback_hours`` before the newest closed bar
    is truncated to that bound. The bars in between are never examined; the
    truncation is printed to stderr and returned in ``truncated`` so the
    runner can make it durable. Always Coinbase-strict — this is the shadow's
    only read path.
    """
    if lookback_hours <= 0:
        raise ValueError("look-back cap must be positive")
    current = now or datetime.now(timezone.utc)
    cutoff = closed_cutoff(current)
    floor = cutoff - pd.Timedelta(hours=lookback_hours)
    start_ts = _utc(current - timedelta(days=_WARMUP_DAYS))
    end_ts = _utc(current + timedelta(days=1))

    found: list[Candidate] = []
    examined_through: dict[str, str] = {}
    bars_examined: dict[str, int] = {}
    cold_start: list[str] = []
    truncated: dict[str, dict] = {}
    seen_events: set[str] = set()

    for asset in assets:
        frame, cfg, providers, btc_applicable = _asset_frame(asset, start_ts, end_ts, True)
        bars_examined[asset] = 0
        if frame is None or frame.empty:
            continue
        closed = frame.index[frame.index <= cutoff]
        if len(closed) == 0:
            continue
        resume = resume_from.get(asset)
        if resume is None:
            cold_start.append(asset)
            bars = closed[-1:]
        else:
            resume_ts = _utc(resume)
            if resume_ts < floor:
                lost = int((floor - resume_ts) / pd.Timedelta(hours=1))
                truncated[asset] = {
                    "resume_from": resume_ts.isoformat(),
                    "examined_from": floor.isoformat(),
                    "bars_never_examined": lost,
                }
                print(
                    f"[agent-shadow] LOOK-BACK CAP: {asset} resume point "
                    f"{resume_ts.isoformat()} is more than {lookback_hours}h behind "
                    f"the newest closed bar {cutoff.isoformat()}; examining from "
                    f"{floor.isoformat()} only. {lost} bar(s) will never be examined.",
                    file=sys.stderr,
                )
                resume_ts = floor
            bars = closed[closed >= resume_ts]
        bars_examined[asset] = len(bars)
        if len(bars) == 0:
            continue
        examined_through[asset] = pd.Timestamp(bars[-1]).isoformat()
        for candle_time in bars:
            candidate = _candidate_at(frame, candle_time, asset, cfg, btc_applicable, providers)
            if candidate is not None:
                if per_event:
                    position = frame.index.get_loc(candle_time)
                    event_id = _cross_event_id(asset, frame, position)
                    if event_id in seen_events:
                        continue
                    candidate = replace(candidate, event_id=event_id)
                    seen_events.add(event_id)
                found.append(candidate)

    found.sort(key=lambda item: (item.candle_time, item.asset))
    return ScanResult(
        candidates=found,
        closed_cutoff=cutoff.isoformat(),
        examined_through=examined_through,
        bars_examined=bars_examined,
        cold_start=cold_start,
        truncated=truncated,
    )
