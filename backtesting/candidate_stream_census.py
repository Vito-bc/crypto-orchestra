"""
Candidate-stream census — blind signal-stage counts for a log-only agent shadow.

WHAT THIS IS
------------
A census of how many 1h bars reach each stage of
`signal_scanner._detect_breakout_signal`, per asset per calendar year, over
the frozen continuous research window (`research_runner.RESEARCH_CONFIG`'s
`continuous_window`, 2021-03-01 -> 2026-07-12 inclusive — the same window
`docs/trial_registry.md` and `CLAUDE.md` quote PF/n against). It sizes the
candidate stream a proposed log-only LLM agent shadow would see. It answers
"how many bars", nothing about what any of them would have earned.

WHAT THIS IS NOT
----------------
`_simulate_trade` is never called or imported here, and no column read by
this module reflects any bar after the signal bar's own close. No P&L, PF,
expectancy, or ranking is computed anywhere in this file — grep it.

THE FIVE STAGES
----------------
  1. trigger      `_detect_breakout_signal` returns non-None: an EMA50 cross
                  from below happened within `_MAX_CANDLES_SINCE` candles.
  2. wide         the bar passed every gate this asset's config DECLARES
                  (vol, 4h trend, daily trend, BTC regime where applicable)
                  and reached the scored conditions. This is the WIDE
                  candidate stream: what an agent shadow would see if it ran
                  on every hard-gate pass rather than only on today's
                  n_met>=4 gate. Both `blocked: "conditions"` bars (failed
                  the scored-condition minimum) and accepted `signal: "BUY"`
                  bars belong to this stage — the split by n_met is stage
                  3/4 below, not a second filter.
  3. n_met>=3     stage-2 bars where >=3 of the 5 scored conditions hold.
  4. n_met>=4     stage-2 bars where >=4 hold. Every ASSET_CONFIG entry in
                  this repo declares `min_conditions=4`, so this stage is
                  exactly today's live BUY gate.
  5. unavailable  blocked `"gate_inputs_unavailable"`: a declared gate's
                  input was not computable on that bar (see
                  `_detect_breakout_signal`'s own docstring on trial
                  2026-08-warmup-semantics). Reported separately — it is a
                  refusal, not a rejection by an evaluated gate, and it is
                  NOT netted against stage 1 or stage 2.

Every bar that is not a stage-1 trigger, not `gate_inputs_unavailable`, and
not stage-2 wide fell to one of the four hard gates themselves (vol, 4h
trend, daily trend, BTC regime) — recorded per bar as `hard_gate_block` in
the CSV for anyone who wants that breakdown; the summary table folds them
into one "hard-gate blocked" column because none of the five requested
stages needs the split.

STAGE 4 vs A REGISTERED ARTIFACT'S n_signals — WHY THEY DIFFER
------------------------------------------------------------------
`docs/research/artifacts/results.json`, trial `2026-08-warmup-semantics.v1`,
row `V2-continuous` / `ZEC-USD`, records `n_signals=114` for this same
window. That number is NOT a bar count: `signal_scanner.scan_asset` also
applies `skip_until` (no re-entry while a previously accepted signal's
simulated position would still be open — position duration comes from
`_simulate_trade`, which reads price after the bar) and the whipsaw guard
(2+ stops in 96h, also determined by simulated outcomes). Both mechanisms
consume price paths this census is constitutionally blind to. This census's
stage-4 bar count is therefore an upper bound on n_signals, not a
reproduction of it, and the gap is the count of stage-4 bars that a
simulated position or a whipsaw guard would have suppressed. Only ZEC has an
existing own-mechanism continuous-window artifact to compare against; BTC,
ETH and SOL are scanned here with their own (not frozen-ZEC-transfer)
ASSET_CONFIG for the first time over this window, so their stage-4 counts
have no prior artifact to reconcile against — see the report for the
ZEC reconciliation.

WHY BTC/ETH/SOL RUN AT ALL
---------------------------
`ASSET_CONFIG[asset]["enabled"]` is consulted only by
`signal_scanner.scan_latest` and `signal_scanner.main()` — never by
`build_merged_frame` or `_detect_breakout_signal`. Passing an asset's own
config straight to those two functions runs it exactly "as if enabled",
with no ASSET_CONFIG edit and no override needed.

Every bar in the window is evaluated independently; there is no
`skip_until` / no-re-entry suppression here, because that belongs to trade
simulation, not to a census of how many bars reach each stage.

NETWORK
-------
None. Reads only the committed local parquet cache under `data/candles/`,
hydrated beforehand from `docs/research/artifacts/manifest.json` via
`backtesting/hydrate_research_data.py` (public Coinbase endpoint, no
credentials). Sets `signal_scanner.STRICT_COINBASE_ONLY = True` for the
duration of the run so a cache miss raises instead of silently reaching the
network, and restores the prior value on exit.

Usage:
    python backtesting/candidate_stream_census.py
    python backtesting/candidate_stream_census.py --out docs/research/data
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting import signal_scanner as scanner  # noqa: E402
from backtesting.research_runner import RESEARCH_CONFIG  # noqa: E402

ASSETS = ["ZEC-USD", "BTC-USD", "ETH-USD", "SOL-USD"]

_WINDOW_START = RESEARCH_CONFIG["continuous_window"]["start"]
_WINDOW_END = RESEARCH_CONFIG["continuous_window"]["end"]

_HARD_GATE_BLOCKS = {"vol_gate", "4h_trend", "btc_regime", "daily_trend"}


def _btc_regime_applicable(asset: str, cfg: dict) -> bool:
    return asset != "BTC-USD" and bool(cfg.get("btc_regime_filter", False))


def census_one(asset: str) -> pd.DataFrame:
    """One row per bar in the frozen window, with its stage outcome."""
    cfg = scanner.ASSET_CONFIG[asset]
    btc_applicable = _btc_regime_applicable(asset, cfg)

    df, _ = scanner.build_merged_frame(
        asset, scanner._DAILY_HISTORY_START, _WINDOW_END, cfg,
        btc_regime_applicable=btc_applicable,
    )
    if df is None:
        raise scanner.StrictSourceError(f"cannot build merged frame for {asset}")

    start_ts = pd.Timestamp(_WINDOW_START, tz="UTC")
    end_ts = pd.Timestamp(_WINDOW_END, tz="UTC")
    df_window = df[(df.index >= start_ts) & (df.index <= end_ts)]
    if df_window.empty:
        raise scanner.StrictSourceError(
            f"{asset}: no bars inside {_WINDOW_START}->{_WINDOW_END}")

    start_idx = df.index.get_loc(df_window.index[0])
    end_idx = df.index.get_loc(df_window.index[-1])

    rows = []
    for i in range(start_idx, end_idx + 1):
        result = scanner._detect_breakout_signal(
            df, i, cfg, btc_regime_applicable=btc_applicable)
        ts = df.index[i]
        row = {
            "asset": asset, "year": ts.year, "timestamp": ts.isoformat(),
            "trigger": 0, "unavailable": 0, "hard_gate_block": None,
            "wide": 0, "n_met": None,
        }
        if result is not None:
            row["trigger"] = 1
            blocked = result.get("blocked")
            if blocked == "gate_inputs_unavailable":
                row["unavailable"] = 1
            elif blocked in _HARD_GATE_BLOCKS:
                row["hard_gate_block"] = blocked
            elif blocked == "conditions":
                row["wide"] = 1
                row["n_met"] = result["n_met"]
            elif blocked is None:
                row["wide"] = 1
                row["n_met"] = result["n_conditions"]
            else:
                raise AssertionError(f"unrecognised block reason: {blocked!r}")
        rows.append(row)
    return pd.DataFrame(rows)


def summarise(bars: pd.DataFrame) -> pd.DataFrame:
    """Per (asset, year): stage counts, rates, and the n_met histogram."""
    out = []
    for (asset, year), g in bars.groupby(["asset", "year"], sort=True):
        total = len(g)
        wide = g["wide"] == 1
        stage3 = wide & (g["n_met"] >= 3)
        stage4 = wide & (g["n_met"] >= 4)
        row = {
            "asset": asset, "year": int(year), "bars": total,
            "trigger": int(g["trigger"].sum()),
            "unavailable": int(g["unavailable"].sum()),
            "hard_gate_blocked": int(g["hard_gate_block"].notna().sum()),
            "wide": int(wide.sum()),
            "n_met_ge_3": int(stage3.sum()),
            "n_met_ge_4": int(stage4.sum()),
            "trigger_rate": g["trigger"].sum() / total,
            "wide_rate": wide.sum() / total,
            "n_met_ge_3_rate": stage3.sum() / total,
            "n_met_ge_4_rate": stage4.sum() / total,
            "unavailable_rate": g["unavailable"].sum() / total,
        }
        for n in range(6):
            row[f"wide_n_met_{n}"] = int((wide & (g["n_met"] == n)).sum())
        out.append(row)
    return pd.DataFrame(out)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="docs/research/data",
                        help="directory to write the CSV into")
    args = parser.parse_args()

    prev_strict = scanner.STRICT_COINBASE_ONLY
    scanner.STRICT_COINBASE_ONLY = True
    try:
        all_bars = pd.concat([census_one(a) for a in ASSETS], ignore_index=True)
    finally:
        scanner.STRICT_COINBASE_ONLY = prev_strict

    summary = summarise(all_bars)

    out_dir = ROOT / args.out
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "2026-09-20-candidate-stream.csv"
    summary.to_csv(out_path, index=False)

    print(summary.to_string(index=False))
    print(f"\nwrote {out_path} ({len(summary)} asset-year rows, "
          f"{len(all_bars)} bars censused)")

    zec_from_effective = all_bars[
        (all_bars["asset"] == "ZEC-USD")
        & (all_bars["timestamp"] >= RESEARCH_CONFIG["asset_effective_start"]["ZEC-USD"])
    ]
    zec_stage4 = int(((zec_from_effective["wide"] == 1)
                      & (zec_from_effective["n_met"] >= 4)).sum())
    print(f"\nZEC-USD stage-4 bars from its registered effective_start "
          f"({RESEARCH_CONFIG['asset_effective_start']['ZEC-USD']}) onward: "
          f"{zec_stage4} (registered artifact n_signals=114; the gap is "
          "skip_until position-overlap suppression and the whipsaw guard — "
          "see this module's docstring).")


if __name__ == "__main__":
    main()
