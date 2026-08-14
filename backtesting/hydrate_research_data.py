"""
Materialise the research candle cache from PUBLIC Coinbase data — no credentials.

Why this exists: `research_runner.py --verify` is the only real proof that the
committed numbers can be regenerated, and it could not run in CI. A clean runner
has no `data/candles/`, and `exchange/coinbase_candles.py` fetches through the
authenticated client (`RESTClient(key_file="cdp_api_key.json")`), so wiring the
verify job would have meant putting exchange credentials into CI. That trade is
not worth making for public OHLCV.

Coinbase exposes candles on an unauthenticated endpoint, and the SDK's
`RESTClient()` constructs without keys and offers `get_public_candles`. This
script uses only that. It never touches the trading client, never reads
`cdp_api_key.json`, and cannot place an order.

Contract:

  * The datasets to build, and their exact expected shape, come from the
    COMMITTED manifest — never from CLI arguments. Hydration reproduces a
    declared input set; it does not get to choose one.
  * Every file is verified against the manifest's SHA-256 after writing. A
    mismatch fails the run and leaves the artifact untouched: the whole point is
    that CI cannot quietly re-baseline the inputs.
  * Row counts and coverage boundaries are checked too, so a truncated download
    fails loudly rather than producing a shorter, plausible-looking scan.

Usage:
    python backtesting/hydrate_research_data.py            # build what is missing
    python backtesting/hydrate_research_data.py --check    # verify only, no fetch
"""

from __future__ import annotations

import hashlib
import json
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exchange.coinbase_candles import (  # noqa: E402
    _candles_to_df,
    _parquet_path,
    _resample_4h,
    _write_checksum,
)

MANIFEST = ROOT / "docs" / "research" / "artifacts" / "manifest.json"

# Coinbase caps a candle request; the loader uses the same batch size.
_BATCH = 300
_GRANULARITY = {"1h": "ONE_HOUR", "1d": "ONE_DAY"}
_STEP_SECONDS = {"ONE_HOUR": 3600, "ONE_DAY": 86400}


class HydrationError(RuntimeError):
    """Public data could not be materialised to match the committed manifest."""


def _public_client():
    """
    An unauthenticated Coinbase REST client.

    Constructed with no key material of any kind. If this ever starts requiring
    credentials, the correct response is to fail — not to fall back to the
    trading client.
    """
    from coinbase.rest import RESTClient

    return RESTClient()


def _expected() -> list[dict]:
    if not MANIFEST.exists():
        raise HydrationError(f"no committed manifest at {MANIFEST}")
    return json.loads(MANIFEST.read_text(encoding="utf-8"))["inputs"]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_file(name: str) -> tuple[str, str]:
    """'ZEC_USD_1h.parquet' -> ('ZEC-USD', '1h')"""
    stem = name.removesuffix(".parquet")
    base, _, gran = stem.rpartition("_")
    return base.replace("_", "-"), gran


def _fetch(asset: str, granularity: str, start: pd.Timestamp,
           end: pd.Timestamp) -> pd.DataFrame:
    native = _GRANULARITY[granularity]
    step = _STEP_SECONDS[native] * _BATCH
    client = _public_client()

    rows: list[dict] = []
    t = int(start.timestamp())
    t_end = int(end.timestamp())
    while t < t_end:
        batch_end = min(t + step, t_end)
        resp = client.get_public_candles(
            product_id=asset, start=str(t), end=str(batch_end),
            granularity=native,
        )
        rows.extend(c.__dict__ for c in (resp.candles or []))
        t = batch_end
        # Public endpoints are rate limited; this is a one-off cache build.
        time.sleep(0.15)

    df = _candles_to_df(rows)
    if df.empty:
        raise HydrationError(f"{asset} {granularity}: public endpoint returned nothing")
    return df.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)


def _verify_one(spec: dict) -> tuple[bool, str]:
    """Check a materialised file against its manifest entry."""
    asset, gran = _parse_file(spec["file"])
    path = _parquet_path(asset, gran)
    if not path.exists():
        return False, "missing"

    actual = _sha256(path)
    if actual == spec["sha256"]:
        return True, "sha256 match"

    # Report WHY it differs — a row-count or boundary difference is a truncated
    # download, whereas identical coverage with a different hash is an encoder
    # difference, and the two need different responses.
    try:
        df = pd.read_parquet(path)
        ts = pd.to_datetime(df["time"], utc=True).sort_values()
        detail = (f"rows {len(df)} vs {spec['rows']}, "
                  f"min {ts.iloc[0].isoformat()} vs {spec['min_ts']}, "
                  f"max {ts.iloc[-1].isoformat()} vs {spec['max_ts']}")
    except Exception as exc:
        detail = f"unreadable: {exc}"
    return False, f"sha256 differs ({detail})"


def hydrate(check_only: bool = False) -> int:
    specs = _expected()
    failures: list[str] = []

    for spec in specs:
        asset, gran = _parse_file(spec["file"])
        ok, why = _verify_one(spec)
        if ok:
            print(f"  {spec['file']}: ok ({spec['rows']} rows)")
            continue
        if check_only:
            failures.append(f"{spec['file']}: {why}")
            continue

        print(f"  {spec['file']}: {why} — fetching public candles…")
        # Boundaries come from the manifest, not from a CLI flag: hydration
        # reproduces a declared input set rather than choosing one.
        start = pd.Timestamp(spec["min_ts"])
        end = pd.Timestamp(spec["max_ts"]) + pd.Timedelta(seconds=1)
        if gran == "4h":
            # 4h is always derived from 1h so both use identical underlying data.
            base = _fetch(asset, "1h", start, end)
            df = _resample_4h(base)
        else:
            df = _fetch(asset, gran, start, end)

        path = _parquet_path(asset, gran)
        df.to_parquet(path, index=False)
        _write_checksum(path)

        ok, why = _verify_one(spec)
        if not ok:
            failures.append(f"{spec['file']}: {why}")
        else:
            print(f"    verified {spec['file']}")

    if failures:
        print("\nHYDRATION FAILED — inputs do not match the committed manifest:",
              file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        print("\nThe manifest is the reference. Do NOT regenerate it to match "
              "the download; investigate why the public data differs.",
              file=sys.stderr)
        return 1

    print(f"\nOK: {len(specs)} datasets match the committed manifest.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(hydrate(check_only="--check" in sys.argv))
    except HydrationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)
