"""
Coinbase Advanced Trade API candle downloader.

Replaces yfinance for all backtesting. Uses the same API key as the live
trading system — data is from the exact exchange we trade on.

History available:
  ZEC-USD: from 2020-12-08 (Coinbase listing date)
  SOL-USD: from ~2021-05
  BTC-USD, ETH-USD: from ~2019

Granularity mapping:
  "1h"  -> ONE_HOUR   (native Coinbase)
  "4h"  -> ONE_HOUR resampled to 4h (Coinbase has no 4h bucket)
  "1d"  -> ONE_DAY    (native Coinbase)

Storage:
  data/candles/{asset}_{granularity}.parquet  (immutable, append-only)
  data/candles/{asset}_{granularity}.sha256   (SHA-256 of the parquet file)

The parquet files are git-ignored (large binary) but reproducible from the
Coinbase API. Run `python exchange/coinbase_candles.py --backfill ZEC-USD`
to download the full history of an asset.
"""

from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import sys

import pandas as pd

ROOT      = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DATA_DIR  = ROOT / "data" / "candles"

# Coinbase max candles per request
_BATCH    = 350

_GRANULARITY_MAP = {
    "1h": "ONE_HOUR",
    "4h": "ONE_HOUR",   # resample after download
    "1d": "ONE_DAY",
}

# Seconds per native granularity
_SECONDS = {
    "ONE_HOUR": 3600,
    "ONE_DAY":  86400,
}


def _get_client():
    from exchange.coinbase_client import _get_client as _base
    return _base()


def _fetch_batch(client, asset: str, gran: str, start_ts: int, end_ts: int) -> list[dict]:
    """Fetch one batch of up to 350 candles. Returns list of raw dicts."""
    resp = client.get_candles(
        product_id=asset,
        start=str(start_ts),
        end=str(end_ts),
        granularity=gran,
    )
    return [c.__dict__ for c in (resp.candles or [])]


def _candles_to_df(raw: list[dict]) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw)
    df["time"] = pd.to_datetime(df["start"].astype(int), unit="s", utc=True)
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[["time", "open", "high", "low", "close", "volume"]].sort_values("time").reset_index(drop=True)


def _resample_4h(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 1h candles into 4h OHLCV bars."""
    df = df.set_index("time")
    resampled = df.resample("4h", origin="start_day").agg(
        open   =("open",   "first"),
        high   =("high",   "max"),
        low    =("low",    "min"),
        close  =("close",  "last"),
        volume =("volume", "sum"),
    ).dropna(subset=["open"]).reset_index()
    return resampled


def _parquet_path(asset: str, granularity: str) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    safe = asset.replace("-", "_")
    return DATA_DIR / f"{safe}_{granularity}.parquet"


def _sha_path(asset: str, granularity: str) -> Path:
    return _parquet_path(asset, granularity).with_suffix(".sha256")


def _write_checksum(path: Path) -> None:
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_suffix(".sha256").write_text(sha)


def _load_existing(asset: str, granularity: str) -> pd.DataFrame:
    p = _parquet_path(asset, granularity)
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_parquet(p)
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], utc=True)
    return df


def download(
    asset: str,
    start: str | datetime,
    end:   str | datetime,
    granularity: str = "1h",
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Download candles for asset between start and end, using Coinbase API.

    Returns a DataFrame with columns: time, open, high, low, close, volume.
    Data is cached to parquet; only missing ranges are fetched.

    granularity: "1h", "4h", or "1d"
    """
    if isinstance(start, str):
        start = pd.Timestamp(start, tz="UTC").to_pydatetime()
    if isinstance(end, str):
        end   = pd.Timestamp(end, tz="UTC").to_pydatetime()

    # 4h: always built from 1h parquet — never makes its own API calls.
    # This avoids duplicate downloads and ensures 4h candles use exactly the
    # same underlying data as 1h signals.
    if granularity == "4h":
        df_1h = download(asset, start, end, granularity="1h", verbose=verbose)
        if df_1h.empty:
            return df_1h
        return _resample_4h(df_1h)

    native_gran = _GRANULARITY_MAP[granularity]
    store_key   = granularity

    existing = _load_existing(asset, store_key)
    if not existing.empty:
        latest_stored = existing["time"].max()
        fetch_from    = latest_stored + timedelta(seconds=_SECONDS.get(native_gran, 3600))
    else:
        fetch_from = start

    fetch_end = end

    if fetch_from >= fetch_end:
        df = existing
    else:
        if verbose:
            print(f"  [Coinbase] {asset} {granularity}: fetching "
                  f"{fetch_from.date()} -> {fetch_end.date()}")

        client   = _get_client()
        step_sec = _SECONDS.get(native_gran, 3600) * _BATCH
        new_rows: list[dict] = []

        t = int(fetch_from.timestamp())
        t_end = int(fetch_end.timestamp())

        while t < t_end:
            batch_end = min(t + step_sec, t_end)
            raw = _fetch_batch(client, asset, native_gran, t, batch_end)
            new_rows.extend(raw)
            t = batch_end
            if t < t_end:
                time.sleep(0.12)  # ~8 req/s well within rate limit

        new_df = _candles_to_df(new_rows)

        if not new_df.empty:
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates("time").sort_values("time").reset_index(drop=True)
        else:
            combined = existing

        if not combined.empty:
            p = _parquet_path(asset, store_key)
            combined.to_parquet(p, index=False)
            _write_checksum(p)

        df = combined

    if df.empty:
        return df

    ts_start = pd.Timestamp(start).tz_convert("UTC") if getattr(start, "tzinfo", None) else pd.Timestamp(start, tz="UTC")
    ts_end   = pd.Timestamp(end).tz_convert("UTC")   if getattr(end,   "tzinfo", None) else pd.Timestamp(end,   tz="UTC")
    df = df[(df["time"] >= ts_start) & (df["time"] <= ts_end)].copy()

    if granularity == "4h":
        df = _resample_4h(df)

    return df.reset_index(drop=True)


def backfill(asset: str, since: str | None = None, granularity: str = "1h") -> pd.DataFrame:
    """
    Download full available history from listing date (or `since`) to now.
    Useful for first-time setup. Run once per asset.
    """
    end   = datetime.now(timezone.utc)
    start = pd.Timestamp(since or "2020-01-01", tz="UTC").to_pydatetime()
    print(f"Backfilling {asset} {granularity} from {start.date()} to {end.date()} ...")
    df = download(asset, start, end, granularity=granularity, verbose=True)
    print(f"  Done — {len(df):,} candles  "
          f"({df['time'].min().date() if not df.empty else 'n/a'} "
          f"-> {df['time'].max().date() if not df.empty else 'n/a'})")
    return df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Coinbase candle downloader")
    parser.add_argument("--backfill", metavar="ASSET", help="Download full history, e.g. ZEC-USD")
    parser.add_argument("--since",    default=None, help="Override start date YYYY-MM-DD")
    parser.add_argument("--gran",     default="1h", choices=["1h", "4h", "1d"])
    args = parser.parse_args()

    if args.backfill:
        backfill(args.backfill, since=args.since, granularity=args.gran)
    else:
        parser.print_help()
