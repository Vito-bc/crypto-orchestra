"""
Phase 7R-3 — synthetic power analysis for the draft STF gates.

Question: if we ran the proposed forward trial, how often would the acceptance
gates say "not falsified" when the strategy has NO edge, and how often would
they reject one that does?

This uses NO historical strategy returns. Trade outcomes are simulated from a
declared model, so nothing here can leak performance from the candidate rule.
What IS taken from history is the *structure* measured by the blinded 7R-1
audit — cluster arrival rate, assets per cluster, overlap — because the gates
are only as strong as the sample structure they will actually see.

THE MODEL
---------
Trend-following payoffs are asymmetric by construction: many small losses, few
large wins. That shape, not the mean, is what makes profit factor unstable at
small n, so the simulation reproduces it explicitly:

  * clusters arrive as a Poisson process at the measured rate;
  * each cluster involves a drawn number of assets (mean matched to 7R-1);
  * within a cluster, win/loss direction is correlated through a Gaussian
    copula — four crypto assets trending together is ONE market event, not four
    independent draws, which is the whole reason "pooled n" overstates evidence;
  * a loss is a fixed stop-sized move; a win is lognormal with a long right
    tail;
  * costs are subtracted from every trade.

Two worlds are simulated: a null in which expectancy after costs is exactly
zero, and an alternative with a genuine per-trade edge. Everything else is held
identical.

Usage:
    python backtesting/stf_power.py            # write the study
    python backtesting/stf_power.py --verify   # reproduce, do not write
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np

TRIAL_ID = "2026-08-stf-power.7R3"
ARTIFACT_DIR = ROOT / "docs" / "research" / "artifacts" / "stf_feasibility"
ARTIFACT = ARTIFACT_DIR / "power.json"
FEASIBILITY = ARTIFACT_DIR / "audit.json"

N_TRIALS = 5_000
SEED = 20260819          # fixed: the study must reproduce byte-for-byte

# ── Declared payoff model ────────────────────────────────────────────────────
LOSS_PCT = 8.0           # a losing trend trade gives back roughly the entry risk
COST_PCT = 1.4           # round-trip from the draft protocol
WIN_RATES = (0.25, 0.35, 0.45)
CORRELATIONS = (0.5, 0.7, 0.9)
HORIZONS_YEARS = (1.5, 3.0, 5.0, 10.0)
EDGE_PCT = 2.0           # alternative world: +2% per trade after costs
POSITION_FRACTION = 0.05  # one position = 5% of equity, per the draft protocol

# ── The draft gates under test ───────────────────────────────────────────────
GATE_MIN_TRADES = 20
GATE_MIN_CLUSTERS = 3
GATE_MIN_YEARS = 1.5
GATE_MIN_PF = 1.30
GATE_LOEO_MIN_PF = 1.00
GATE_MAX_DD_PCT = 25.0
GATE_MAX_ASSET_SHARE = 0.60


class PowerError(RuntimeError):
    """The study cannot be produced as specified."""


def _structure() -> dict:
    """Cluster arrival rate and assets-per-cluster, from the blinded audit."""
    if not FEASIBILITY.exists():
        raise PowerError(
            f"{FEASIBILITY} is missing — run stf_feasibility.py first. The "
            "power study must be calibrated to the measured sample structure, "
            "not to a guess.")
    p = json.loads(FEASIBILITY.read_text(encoding="utf-8"))["portfolio"]
    clusters = p["exposure_clusters"]
    mean_assets = p["mean_concurrent_when_exposed"]
    # A cluster is a market REGIME, not a single trade. Within one, an asset
    # re-enters several times. The first version of this study modelled one
    # trade per asset per cluster and produced ~2.8 trades/year against a
    # measured 11.94, so almost no simulated trial ever reached the minimums —
    # an artefact of the model, not a property of the gates.
    trades_per_asset_per_cluster = p["pooled_closed_trades"] / clusters / mean_assets
    return {
        "clusters_per_year": p["clusters_per_year"],
        "mean_assets_per_cluster": mean_assets,
        "trades_per_asset_per_cluster": round(trades_per_asset_per_cluster, 2),
        "measured_pooled_trades_per_year": p["pooled_closed_trades_per_year"],
        "implied_pooled_trades_per_year": round(
            p["clusters_per_year"] * mean_assets * trades_per_asset_per_cluster, 2),
    }


def _win_size(win_rate: float, edge: float) -> float:
    """
    Mean win that makes expectancy exactly `edge` after costs.

    E = p*W - (1-p)*LOSS - COST  =>  W = (edge + COST + (1-p)*LOSS) / p
    """
    return (edge + COST_PCT + (1.0 - win_rate) * LOSS_PCT) / win_rate


def _simulate_one(rng, years: float, win_rate: float, rho: float,
                  edge: float, structure: dict) -> dict | None:
    """One synthetic trial. Returns its gate inputs, or None if it never ran."""
    lam = structure["clusters_per_year"] * years
    n_clusters = int(rng.poisson(lam))
    if n_clusters == 0:
        return None

    mean_assets = structure["mean_assets_per_cluster"]
    threshold = -np.sqrt(2.0) * _erfinv(2.0 * win_rate - 1.0)   # Phi^-1(1-p)
    mean_win = _win_size(win_rate, edge)
    # Lognormal with a long right tail: sigma large enough that the median win
    # is well below the mean, so a handful of trades carry the profit.
    sigma = 0.9
    mu = np.log(mean_win) - sigma ** 2 / 2.0

    returns: list[float] = []
    cluster_of: list[int] = []
    asset_of: list[int] = []

    for c in range(n_clusters):
        # Assets participating in this cluster: at least one, mean matched.
        k = int(np.clip(rng.poisson(mean_assets - 1) + 1, 1, 4))
        assets = rng.choice(4, size=k, replace=False)
        z = rng.standard_normal()                       # shared regime factor
        for a in assets:
            # An asset re-enters several times inside one regime. Those repeats
            # share the regime factor, so they are NOT independent evidence
            # either — which is the whole reason pooled n overstates the sample.
            n_trades = max(1, int(rng.poisson(
                structure["trades_per_asset_per_cluster"])))
            for _ in range(n_trades):
                e = rng.standard_normal()
                latent = np.sqrt(rho) * z + np.sqrt(1.0 - rho) * e
                if latent > threshold:
                    r = float(rng.lognormal(mu, sigma)) - COST_PCT
                else:
                    r = -LOSS_PCT - COST_PCT
                returns.append(r)
                cluster_of.append(c)
                asset_of.append(int(a))

    if not returns:
        return None

    arr = np.array(returns)
    gross_win = float(arr[arr > 0].sum())
    gross_loss = float(-arr[arr < 0].sum())
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")

    # Leave-one-cluster-out on the BEST cluster.
    cl = np.array(cluster_of)
    best, best_sum = -1, -np.inf
    for c in range(n_clusters):
        s = float(arr[cl == c].sum())
        if s > best_sum:
            best, best_sum = c, s
    rest = arr[cl != best]
    rw = float(rest[rest > 0].sum())
    rl = float(-rest[rest < 0].sum())
    loeo_pf = (rw / rl if rl > 0 else (float("inf") if rw > 0 else 0.0))

    # Equity path in cluster order. One position is POSITION_FRACTION of
    # equity, so a trade returning r% moves equity by POSITION_FRACTION * r%.
    equity, peak, max_dd = 100.0, 100.0, 0.0
    for r in arr:
        equity *= (1.0 + r / 100.0 * POSITION_FRACTION)
        peak = max(peak, equity)
        max_dd = max(max_dd, (peak - equity) / peak * 100.0)

    asset_arr = np.array(asset_of)
    shares = [float(arr[(asset_arr == a) & (arr > 0)].sum()) for a in range(4)]
    top_share = (max(shares) / gross_win) if gross_win > 0 else 0.0

    return {
        "n_trades": len(arr),
        "n_clusters": n_clusters,
        "years": years,
        "expectancy": float(arr.mean()),
        "pf": pf,
        "loeo_pf": loeo_pf,
        "max_dd": max_dd,
        "top_asset_share": top_share,
    }


def _erfinv(x: float) -> float:
    """Inverse error function (Giles' rational approximation, float64)."""
    w = -np.log(np.clip(1.0 - x * x, 1e-300, None))
    if w < 5.0:
        w -= 2.5
        p = 2.81022636e-08
        for c in (3.43273939e-07, -3.5233877e-06, -4.39150654e-06,
                  0.00021858087, -0.00125372503, -0.00417768164,
                  0.246640727, 1.50140941):
            p = p * w + c
    else:
        w = np.sqrt(w) - 3.0
        p = -0.000200214257
        for c in (0.000100950558, 0.00134934322, -0.00367342844,
                  0.00573950773, -0.0076224613, 0.00943887047,
                  1.00167406, 2.83297682):
            p = p * w + c
    return float(p * x)


def _gate_results(t: dict) -> dict:
    """Each drafted gate evaluated separately, so inert ones become visible."""
    return {
        "minimums": (t["n_trades"] >= GATE_MIN_TRADES
                     and t["n_clusters"] >= GATE_MIN_CLUSTERS
                     and t["years"] >= GATE_MIN_YEARS),
        "expectancy": t["expectancy"] > 0,
        "profit_factor": t["pf"] >= GATE_MIN_PF,
        "leave_one_cluster_out": t["loeo_pf"] >= GATE_LOEO_MIN_PF,
        "max_drawdown": t["max_dd"] <= GATE_MAX_DD_PCT,
        "asset_concentration": t["top_asset_share"] <= GATE_MAX_ASSET_SHARE,
    }


def _passes(t: dict) -> bool:
    """Every draft gate, applied exactly as written."""
    return all(_gate_results(t).values())


def _cell(rng, years, win_rate, rho, edge, structure) -> dict:
    passed = reached = ran = 0
    # How often each gate FAILS among trials that reached the minimums. A gate
    # that never fails is not protecting anything.
    binding = {k: 0 for k in ("expectancy", "profit_factor",
                              "leave_one_cluster_out", "max_drawdown",
                              "asset_concentration")}
    for _ in range(N_TRIALS):
        t = _simulate_one(rng, years, win_rate, rho, edge, structure)
        if t is None:
            continue
        ran += 1
        g = _gate_results(t)
        if g["minimums"]:
            reached += 1
            for name in binding:
                if not g[name]:
                    binding[name] += 1
        if all(g.values()):
            passed += 1
    return {
        "pass_rate": round(passed / ran, 4) if ran else None,
        "reached_minimums_rate": round(reached / ran, 4) if ran else None,
        "gate_failure_rate_given_minimums": {
            k: (round(v / reached, 4) if reached else None)
            for k, v in binding.items()
        },
    }


def build_study() -> dict:
    structure = _structure()
    rng = np.random.default_rng(SEED)

    null_cells, alt_cells = {}, {}
    for years in HORIZONS_YEARS:
        for win_rate in WIN_RATES:
            for rho in CORRELATIONS:
                key = f"years={years}|win_rate={win_rate}|rho={rho}"
                null_cells[key] = _cell(rng, years, win_rate, rho, 0.0, structure)
                alt_cells[key] = _cell(rng, years, win_rate, rho, EDGE_PCT, structure)

    def _summary(cells):
        rates = [c["pass_rate"] for c in cells.values() if c["pass_rate"] is not None]
        return {
            "min": round(min(rates), 4),
            "median": round(float(np.median(rates)), 4),
            "max": round(max(rates), 4),
        }

    return {
        "trial_id": TRIAL_ID,
        "purpose": ("power of the draft STF acceptance gates under a declared "
                    "payoff model — no historical strategy returns are used"),
        "calibrated_from": {
            "source": "docs/research/artifacts/stf_feasibility/audit.json",
            **structure,
        },
        "model": {
            "loss_pct": LOSS_PCT,
            "cost_pct": COST_PCT,
            "win_distribution": "lognormal, sigma=0.9 (long right tail)",
            "win_size_rule": "mean win set so expectancy equals the stated edge",
            "correlation": "Gaussian copula on win/loss direction within a cluster",
            "edge_pct_alternative": EDGE_PCT,
            "position_fraction_of_equity": POSITION_FRACTION,
            "n_trials_per_cell": N_TRIALS,
            "seed": SEED,
        },
        "gates": {
            "min_trades": GATE_MIN_TRADES,
            "min_clusters": GATE_MIN_CLUSTERS,
            "min_years": GATE_MIN_YEARS,
            "min_pf": GATE_MIN_PF,
            "loeo_min_pf": GATE_LOEO_MIN_PF,
            "max_dd_pct": GATE_MAX_DD_PCT,
            "max_asset_share": GATE_MAX_ASSET_SHARE,
        },
        "null_world_no_edge": {"cells": null_cells, "summary": _summary(null_cells)},
        "alternative_world_with_edge": {"cells": alt_cells,
                                        "summary": _summary(alt_cells)},
    }


def _serialise(study: dict) -> str:
    return json.dumps(study, indent=2, sort_keys=True) + "\n"


def verify() -> bool:
    if not ARTIFACT.exists():
        raise PowerError(f"no committed study at {ARTIFACT}")
    if _serialise(build_study()) == ARTIFACT.read_text(encoding="utf-8"):
        return True
    print("MISMATCH: power study differs from a fresh run", file=sys.stderr)
    return False


def _report(s: dict) -> None:
    print("\n" + "=" * 78)
    print(f"STF GATE POWER STUDY  ({s['trial_id']})")
    print("Synthetic outcomes only. Structure calibrated to the blinded audit.")
    print("=" * 78)
    print(f"\ncluster rate {s['calibrated_from']['clusters_per_year']}/yr, "
          f"mean {s['calibrated_from']['mean_assets_per_cluster']} assets/cluster")

    print(f"\n{'horizon':>8s} {'win':>5s} {'rho':>5s} "
          f"{'PASS | no edge':>15s} {'PASS | real edge':>17s} {'reached mins':>13s}")
    print("-" * 78)
    for key in s["null_world_no_edge"]["cells"]:
        years = key.split("|")[0].split("=")[1]
        win = key.split("|")[1].split("=")[1]
        rho = key.split("|")[2].split("=")[1]
        n = s["null_world_no_edge"]["cells"][key]
        a = s["alternative_world_with_edge"]["cells"][key]
        print(f"{years:>8s} {win:>5s} {rho:>5s} {n['pass_rate']:>15.1%} "
              f"{a['pass_rate']:>17.1%} {a['reached_minimums_rate']:>13.1%}")

    ns, als = s["null_world_no_edge"]["summary"], s["alternative_world_with_edge"]["summary"]
    print(f"\nfalse-pass rate (no edge)  min {ns['min']:.1%}  "
          f"median {ns['median']:.1%}  max {ns['max']:.1%}")
    print(f"true-pass rate (real edge) min {als['min']:.1%}  "
          f"median {als['median']:.1%}  max {als['max']:.1%}")
    print("=" * 78)


def main() -> None:
    try:
        if "--verify" in sys.argv:
            if verify():
                print("OK: power study reproduces byte-for-byte.")
                return
            sys.exit(1)
        study = build_study()
        _report(study)
        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        ARTIFACT.write_text(_serialise(study), encoding="utf-8")
        print(f"\nwrote {ARTIFACT.relative_to(ROOT)}")
    except PowerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
