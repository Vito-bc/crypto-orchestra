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
  * Identity is the manifest's LOGICAL hash: the canonical encoding of the
    in-scope OHLCV rows. The physical parquet SHA-256 is deliberately not used —
    it moves whenever the exchange completes a candle past the freeze or a
    different writer is used, neither of which changes a single number.
  * An EXISTING file that mismatches is diagnosed, never overwritten. Downloads
    land in a `.partial`, are validated for schema, coverage and logical hash,
    and only then atomically replace the target; a rejected candidate is
    deleted. A bad download therefore cannot destroy a good cache — which it
    could before, because the fetch wrote straight over the target and only
    checked afterwards.

Usage:
    python backtesting/hydrate_research_data.py            # build what is MISSING
    python backtesting/hydrate_research_data.py --check    # verify only, never fetch
    python backtesting/hydrate_research_data.py --repair   # also re-fetch mismatches
"""

from __future__ import annotations

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

# Coinbase caps a candle request; stay well under it.
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


def _check_frame(df: pd.DataFrame, spec: dict) -> tuple[bool, str]:
    """Validate a candidate frame against one manifest entry."""
    from backtesting.research_runner import (
        ProvenanceError,
        logical_sha256,
        scoped_frame,
    )

    try:
        scoped = scoped_frame(df)          # schema, duplicates, finiteness
    except ProvenanceError as exc:
        return False, f"rejected by scope validation: {exc}"

    if len(scoped) != spec["rows"]:
        return False, (f"in-scope rows {len(scoped)} vs manifest {spec['rows']} "
                       "- truncated or over-long download")
    if scoped["time"].iloc[0].isoformat() != spec["min_ts"]:
        return False, (f"scope starts {scoped['time'].iloc[0].isoformat()} "
                       f"vs {spec['min_ts']}")
    if scoped["time"].iloc[-1].isoformat() != spec["max_ts"]:
        return False, (f"scope ends {scoped['time'].iloc[-1].isoformat()} "
                       f"vs {spec['max_ts']}")

    actual = logical_sha256(df)
    if actual != spec["logical_sha256"]:
        return False, (f"logical hash {actual[:16]} vs manifest "
                       f"{spec['logical_sha256'][:16]} - in-scope OHLCV differs")
    return True, "logical hash match"


def _verify_one(spec: dict) -> tuple[bool, str]:
    """Check a materialised file against its manifest entry."""
    asset, gran = _parse_file(spec["file"])
    path = _parquet_path(asset, gran)
    if not path.exists():
        return False, "missing"
    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        return False, f"unreadable: {exc}"
    return _check_frame(df, spec)


def _materialise(spec: dict) -> tuple[bool, str]:
    """
    Fetch one dataset into a candidate file, validate it, then atomically
    replace the target.

    The target is never touched until the candidate has passed.
    """
    asset, gran = _parse_file(spec["file"])
    path = _parquet_path(asset, gran)
    partial = path.with_suffix(".partial")

    try:
        # Boundaries come from the manifest, not from a CLI flag. Fetch a little
        # past the scope end so the boundary bar is definitely included; the
        # logical hash ignores anything beyond it.
        start = pd.Timestamp(spec["min_ts"])
        end = pd.Timestamp(spec["max_ts"]) + pd.Timedelta(days=1)
        if gran == "4h":
            # 4h is always derived from 1h so both use identical underlying data.
            df = _resample_4h(_fetch(asset, "1h", start, end))
        else:
            df = _fetch(asset, gran, start, end)

        df.to_parquet(partial, index=False)
        ok, why = _check_frame(pd.read_parquet(partial), spec)
        if not ok:
            return False, why

        # Atomic within a filesystem: the target is either untouched or the
        # validated candidate, never a half-written file.
        partial.replace(path)
        _write_checksum(path)
        return True, "materialised and verified"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
    finally:
        # A rejected candidate is never left behind to be mistaken for real data.
        if partial.exists():
            try:
                partial.unlink()
            except OSError:
                pass


def hydrate(check_only: bool = False, repair: bool = False) -> int:
    specs = _expected()
    failures: list[str] = []

    for spec in specs:
        asset, gran = _parse_file(spec["file"])
        existed = _parquet_path(asset, gran).exists()
        ok, why = _verify_one(spec)
        if ok:
            print(f"  {spec['file']}: ok ({spec['rows']} in-scope rows)")
            continue
        if check_only:
            failures.append(f"{spec['file']}: {why}")
            continue
        if existed and not repair:
            # DIAGNOSE, do not overwrite. An existing file that disagrees with
            # the manifest is evidence; replacing it destroys the only copy of
            # whatever produced the committed numbers.
            failures.append(
                f"{spec['file']}: {why} - existing file left untouched. "
                "Investigate, or pass --repair to re-fetch it deliberately.")
            continue

        verb = "re-fetching" if existed else "fetching"
        print(f"  {spec['file']}: {why} - {verb} public candles...")
        ok, why = _materialise(spec)
        if ok:
            print(f"    verified {spec['file']}")
        else:
            failures.append(f"{spec['file']}: {why}")

    if failures:
        print("\nHYDRATION FAILED - inputs do not match the committed manifest:",
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
        sys.exit(hydrate(check_only="--check" in sys.argv,
                         repair="--repair" in sys.argv))
    except HydrationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)
