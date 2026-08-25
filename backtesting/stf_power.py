"""
Phase 7R-3 — power analysis of the FINAL STF continuation gates.

Question: if the proposed forward trial ran, how often would its gates say "not
falsified" when the strategy has NO edge, and how often would they reject one
that does?

No historical strategy returns are used. Trade outcomes come from a declared
model, so nothing here can leak performance from the candidate rule. What IS
taken from history is the sample STRUCTURE measured by the blinded 7R-1 audit,
over the COMMON-UNIVERSE window — the four-asset portfolio the trial would
actually hold.

WHAT THE STRUCTURE SAYS
-----------------------
Every exposure cluster in the measured window involves all four assets
(unique assets per cluster = 4.0). The four names therefore supply no
diversification at the cluster level: a cluster is one crypto-beta regime, and
the whole sample is 7 of them in 4.9 years. That is the ceiling on evidence,
and it is why the gates are simulated against clusters rather than trades.

THE PAYOFF MODEL
----------------
The strategy has NO stop. Its only exit is the 20-day close breakout, so the
size of a losing trade is a property of price behaviour, not of a risk rule.
Fixing it at one number would smuggle in a mechanism the strategy does not have,
so loss size is a SENSITIVITY AXIS, not a constant.

Wins are lognormal with a long right tail; within a cluster, win/loss direction
is correlated through a Gaussian copula, and repeat entries by the same asset
share the regime factor.

DRAWDOWN IS NOT ASSESSED — AND NO PROXY IS REPORTED
---------------------------------------------------
The protocol measures drawdown on the STF sleeve in calendar time, including
unrealized P&L on open positions. This simulation produces closed trades in an
artificial cluster/asset order, which is NOT a bound on that quantity in either
direction: real positions overlap, so a simultaneous winner can offset a loser
and shrink the trough, while unrealized moves can deepen it.

Three earlier readings of this were wrong and are withdrawn. The first
compounded against total capital rather than the sleeve and concluded the gate
was inert. The second corrected the denominator but called the result a lower
bound, which it is not. The third kept a "sequential realized stress" statistic
and counted how often it exceeded the protocol's 25% limit — a comparison that
looks like a drawdown result and is not one.

This study therefore computes no drawdown quantity at all. The limit itself
lives in `stf_protocol.SLEEVE_MAX_DRAWDOWN_PCT`, for the calendar equity curve
a real forward trial would produce.

Usage:
    python backtesting/stf_power.py            # write the study
    python backtesting/stf_power.py --verify   # reproduce, do not write
"""

from __future__ import annotations

import hashlib
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

_CODE_PATHS = [
    "backtesting/research_runner.py",
    "backtesting/stf_power.py",
    "backtesting/stf_protocol.py",
]

N_TRIALS = 4_000
SEED = 20260819          # fixed: the study must reproduce byte-for-byte

# ── Declared payoff model ────────────────────────────────────────────────────
# Loss size is an AXIS, not a constant: the rule has no stop, so what a loser
# gives back is a property of price behaviour.
LOSS_PCTS = (4.0, 8.0, 12.0)
WIN_RATES = (0.25, 0.35, 0.45)
CORRELATIONS = (0.5, 0.7, 0.9)
HORIZONS_YEARS = (3.0, 5.0)
COST_PCT = 1.4           # round-trip; refine from the 7R-2 probe before use
EDGE_PCT = 2.0           # alternative world: +2% per trade after costs
WIN_SIGMA = 0.9          # lognormal shape — a few trades carry the profit

# Sizing comes from the shared frozen protocol, never a local copy: this module
# modelled 5% of capital while the cost probe measured 2%, so the two described
# different mechanisms and nothing flagged it.
from backtesting.stf_protocol import (  # noqa: E402
    POSITION_FRACTION_OF_CAPITAL,
    POSITION_FRACTION_OF_SLEEVE,
    SLEEVE_FRACTION_OF_CAPITAL,
    SLEEVE_MAX_DRAWDOWN_PCT,
)

# ── FINAL protocol gates (continuation, not success) ─────────────────────────
GATE_MIN_TRADES = 30
GATE_MIN_CLUSTERS = 5
GATE_MIN_YEARS = 3.0
GATE_MIN_PF = 1.30
# Reported, NOT gated: leave-one-cluster-out (invalid at 5-7 clusters),
# asset concentration (measures universe correlation), drawdown (needs a
# calendar equity curve this simulation does not produce).
DIAGNOSTIC_ONLY = ("leave_one_cluster_out", "asset_concentration", "max_drawdown")


class PowerError(RuntimeError):
    """The study cannot be produced as specified."""


def _structure() -> dict:
    """Sample structure from the blinded audit, common-universe window."""
    if not FEASIBILITY.exists():
        raise PowerError(
            f"{FEASIBILITY} is missing — run stf_feasibility.py first. The "
            "power study must be calibrated to the measured sample structure, "
            "not to a guess.")
    raw = FEASIBILITY.read_text(encoding="utf-8")
    c = json.loads(raw)["common_universe"]
    return {
        # The audit is an INPUT to this study, so its identity belongs in this
        # artifact too. Without it a regenerated audit could silently change
        # these numbers while the power artifact still verified.
        "audit_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        "window": f"{c['window_start'][:10]}..{c['window_end'][:10]}",
        "years": c["years"],
        "clusters_per_year": c["clusters_per_year"],
        # UNIQUE assets per cluster, not the mean concurrent count. Using the
        # concurrent figure invents too few assets and too many repeat trades,
        # which misstates correlation and concentration.
        "unique_assets_per_cluster": c["mean_unique_assets_per_cluster"],
        "trades_per_asset_per_cluster": c["trades_per_asset_per_cluster"],
        "measured_pooled_trades_per_year": c["pooled_closed_trades_per_year"],
        "implied_pooled_trades_per_year": round(
            c["clusters_per_year"] * c["mean_unique_assets_per_cluster"]
            * c["trades_per_asset_per_cluster"], 2),
    }


def _win_size(win_rate: float, loss_pct: float, edge: float) -> float:
    """Mean win making expectancy exactly `edge` after costs."""
    return (edge + COST_PCT + (1.0 - win_rate) * loss_pct) / win_rate


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


def _simulate_one(rng, years, win_rate, rho, loss_pct, edge, structure):
    """One synthetic trial. Returns gate inputs, or None if it never ran."""
    n_clusters = int(rng.poisson(structure["clusters_per_year"] * years))
    if n_clusters == 0:
        return None

    n_assets = int(round(structure["unique_assets_per_cluster"]))
    threshold = -np.sqrt(2.0) * _erfinv(2.0 * win_rate - 1.0)   # Phi^-1(1-p)
    mean_win = _win_size(win_rate, loss_pct, edge)
    mu = np.log(mean_win) - WIN_SIGMA ** 2 / 2.0

    returns, cluster_of, asset_of = [], [], []
    for c in range(n_clusters):
        z = rng.standard_normal()                       # shared regime factor
        for a in range(n_assets):
            # 1 + Poisson(mean - 1), not max(1, Poisson(mean)): clamping a
            # Poisson at one RAISES its mean (2.357 became 2.452), so the
            # simulated trade count drifted above the measured one.
            n_trades = 1 + int(rng.poisson(
                max(structure["trades_per_asset_per_cluster"] - 1.0, 0.0)))
            for _ in range(n_trades):
                e = rng.standard_normal()
                latent = np.sqrt(rho) * z + np.sqrt(1.0 - rho) * e
                r = ((float(rng.lognormal(mu, WIN_SIGMA)) - COST_PCT)
                     if latent > threshold else (-loss_pct - COST_PCT))
                returns.append(r)
                cluster_of.append(c)
                asset_of.append(a)

    arr = np.array(returns)
    if arr.size == 0:
        return None

    gross_win = float(arr[arr > 0].sum())
    gross_loss = float(-arr[arr < 0].sum())
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")

    cl = np.array(cluster_of)
    sums = [float(arr[cl == c].sum()) for c in range(n_clusters)]
    best = int(np.argmax(sums))
    rest = arr[cl != best]
    rw, rl = float(rest[rest > 0].sum()), float(-rest[rest < 0].sum())
    loeo_pf = rw / rl if rl > 0 else (float("inf") if rw > 0 else 0.0)

    # No drawdown quantity is computed. Applying these trades one at a time in
    # cluster/asset order would produce a number that looks like a drawdown and
    # bounds nothing: real positions overlap, and the gate is measured on a
    # calendar curve that includes unrealized P&L.
    aa = np.array(asset_of)
    shares = [float(arr[(aa == a) & (arr > 0)].sum()) for a in range(n_assets)]
    top_share = (max(shares) / gross_win) if gross_win > 0 else 0.0

    return {
        "n_trades": int(arr.size), "n_clusters": n_clusters, "years": years,
        "expectancy": float(arr.mean()), "pf": pf, "loeo_pf": loeo_pf,
        "top_asset_share": top_share,
    }


def _gates(t: dict) -> dict:
    """The FINAL gates. Diagnostics are computed but do not decide."""
    return {
        "minimums": (t["n_trades"] >= GATE_MIN_TRADES
                     and t["n_clusters"] >= GATE_MIN_CLUSTERS
                     and t["years"] >= GATE_MIN_YEARS),
        "expectancy": t["expectancy"] > 0,
        "profit_factor": t["pf"] >= GATE_MIN_PF,
    }


def _cell(rng, years, win_rate, rho, loss_pct, edge, structure) -> dict:
    passed = reached = ran = 0
    diag = {"loeo_pf_below_1": 0, "top_asset_share_above_60": 0}
    for _ in range(N_TRIALS):
        t = _simulate_one(rng, years, win_rate, rho, loss_pct, edge, structure)
        if t is None:
            continue
        ran += 1
        g = _gates(t)
        if g["minimums"]:
            reached += 1
            diag["loeo_pf_below_1"] += t["loeo_pf"] < 1.0
            diag["top_asset_share_above_60"] += t["top_asset_share"] > 0.60
        if all(g.values()):
            passed += 1
    return {
        "pass_rate": round(passed / ran, 4) if ran else None,
        "reached_minimums_rate": round(reached / ran, 4) if ran else None,
        "diagnostics_given_minimums": {
            k: (round(v / reached, 4) if reached else None) for k, v in diag.items()
        },
    }


def build_study() -> dict:
    from backtesting.research_runner import environment_fingerprint, sha256_source

    structure = _structure()
    rng = np.random.default_rng(SEED)

    null_cells, alt_cells = {}, {}
    for years in HORIZONS_YEARS:
        for win_rate in WIN_RATES:
            for rho in CORRELATIONS:
                for loss_pct in LOSS_PCTS:
                    key = (f"years={years}|win_rate={win_rate}|rho={rho}"
                           f"|loss={loss_pct}")
                    null_cells[key] = _cell(rng, years, win_rate, rho,
                                            loss_pct, 0.0, structure)
                    alt_cells[key] = _cell(rng, years, win_rate, rho,
                                           loss_pct, EDGE_PCT, structure)

    def _summary(cells):
        rates = [c["pass_rate"] for c in cells.values() if c["pass_rate"] is not None]
        return {"min": round(min(rates), 4),
                "median": round(float(np.median(rates)), 4),
                "max": round(max(rates), 4)}

    files = sorted(({"file": rel, "sha256": sha256_source(ROOT / rel)}
                    for rel in _CODE_PATHS), key=lambda d: d["file"])
    agg = hashlib.sha256()
    for entry in files:
        agg.update(f"{entry['file']}:{entry['sha256']}\n".encode())

    return {
        "trial_id": TRIAL_ID,
        "purpose": ("power of the FINAL STF continuation gates under a declared "
                    "payoff model — no historical strategy returns are used"),
        "code": {"files": files, "code_sha256": agg.hexdigest()},
        "environment": environment_fingerprint(),
        "calibrated_from": {
            "source": "docs/research/artifacts/stf_feasibility/audit.json",
            "basis": "common-universe window (fixed four-asset portfolio)",
            **structure,
        },
        "model": {
            "loss_pcts": list(LOSS_PCTS),
            "loss_is_an_axis_because": ("the rule has no stop; its only exit is "
                                        "the 20-day close breakout, so loss size "
                                        "is a property of price, not of a risk "
                                        "rule"),
            "cost_pct": COST_PCT,
            "win_distribution": f"lognormal, sigma={WIN_SIGMA}",
            "correlation": "Gaussian copula on direction within a cluster",
            "edge_pct_alternative": EDGE_PCT,
            "n_trials_per_cell": N_TRIALS,
            "seed": SEED,
        },
        "sizing": {
            "sleeve_fraction_of_capital": SLEEVE_FRACTION_OF_CAPITAL,
            "position_fraction_of_capital": POSITION_FRACTION_OF_CAPITAL,
            "position_fraction_of_sleeve": POSITION_FRACTION_OF_SLEEVE,
        },
        "gates_evaluated": {
            "min_trades": GATE_MIN_TRADES,
            "min_clusters": GATE_MIN_CLUSTERS,
            "min_years": GATE_MIN_YEARS,
            "min_pf": GATE_MIN_PF,
            "expectancy_above_zero": True,
        },
        "not_assessed": {
            "max_drawdown": ("the protocol measures sleeve drawdown in calendar "
                             "time including unrealized P&L. This simulation "
                             "produces closed trades in an artificial order, "
                             "which is NOT a bound in either direction: "
                             "overlapping winners can offset losers, unrealized "
                             "moves can deepen the trough. NO drawdown quantity "
                             "is computed here and nothing is compared against "
                             "the limit."),
            "sleeve_max_drawdown_pct_limit": SLEEVE_MAX_DRAWDOWN_PCT,
            "where_the_limit_is_evaluated": ("on the calendar sleeve equity "
                                             "curve of a real forward trial, "
                                             "never in this study"),
            "leave_one_cluster_out": "reported as a diagnostic, not a gate",
            "asset_concentration": "reported as a diagnostic, not a gate",
        },
        "diagnostics_are_not_gates": list(DIAGNOSTIC_ONLY),
        "null_world_no_edge": {"cells": null_cells, "summary": _summary(null_cells)},
        "alternative_world_with_edge": {"cells": alt_cells,
                                        "summary": _summary(alt_cells)},
    }


def _serialise(study: dict) -> str:
    return json.dumps(study, indent=2, sort_keys=True) + "\n"

def _atomic_write(path: Path, text: str) -> None:
    """
    Write via a temporary file and replace.

    A direct write_text can leave a truncated artifact if the process dies
    mid-write, and a half-written JSON that still parses is worse than none.
    """
    tmp = path.with_suffix(path.suffix + ".partial")
    try:
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
    finally:
        if tmp.exists():
            tmp.unlink()


def _assert_writable() -> None:
    from backtesting.research_runner import (
        assert_canonical_python,
        assert_code_is_committed,
    )

    assert_canonical_python()
    # The audit is an INPUT, so an uncommitted one would make this study
    # describe a structure that exists on one machine only.
    assert_code_is_committed(_CODE_PATHS + [
        "docs/research/artifacts/stf_feasibility/audit.json"])


def write_artifact() -> tuple[Path, dict]:
    _assert_writable()
    study = build_study()
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    _atomic_write(ARTIFACT, _serialise(study))
    return ARTIFACT, study


def verify() -> bool:
    if not ARTIFACT.exists():
        raise PowerError(f"no committed study at {ARTIFACT}")
    if _serialise(build_study()) == ARTIFACT.read_text(encoding="utf-8"):
        return True
    print("MISMATCH: power study differs from a fresh run", file=sys.stderr)
    return False


def _report(s: dict) -> None:
    c = s["calibrated_from"]
    print("\n" + "=" * 80)
    print(f"STF CONTINUATION-GATE POWER STUDY  ({s['trial_id']})")
    print("Synthetic outcomes. Structure from the blinded common-universe audit.")
    print("=" * 80)
    print(f"\n{c['clusters_per_year']} clusters/yr, "
          f"{c['unique_assets_per_cluster']} UNIQUE assets per cluster, "
          f"{c['trades_per_asset_per_cluster']} trades per asset per cluster")
    print(f"gates: {s['gates_evaluated']}")

    print(f"\n{'yrs':>4s} {'win':>5s} {'rho':>4s} {'loss':>5s} "
          f"{'PASS|no edge':>13s} {'PASS|edge':>10s} {'ratio':>6s} {'mins':>6s}")
    print("-" * 80)
    for key in s["null_world_no_edge"]["cells"]:
        parts = dict(p.split("=") for p in key.split("|"))
        n = s["null_world_no_edge"]["cells"][key]
        a = s["alternative_world_with_edge"]["cells"][key]
        ratio = (a["pass_rate"] / n["pass_rate"]) if n["pass_rate"] else float("inf")
        print(f"{parts['years']:>4s} {parts['win_rate']:>5s} {parts['rho']:>4s} "
              f"{parts['loss']:>5s} {n['pass_rate']:>13.1%} "
              f"{a['pass_rate']:>10.1%} {ratio:>6.2f} "
              f"{a['reached_minimums_rate']:>6.1%}")

    ns = s["null_world_no_edge"]["summary"]
    als = s["alternative_world_with_edge"]["summary"]
    print(f"\nfalse-pass  (no edge)  min {ns['min']:.1%} median {ns['median']:.1%} "
          f"max {ns['max']:.1%}")
    print(f"true-pass   (edge)     min {als['min']:.1%} median {als['median']:.1%} "
          f"max {als['max']:.1%}")
    print("\ndrawdown gate: NOT ASSESSED — no drawdown quantity is computed "
          "here (it needs a calendar equity curve with unrealized P&L)")
    print("=" * 80)


def main() -> None:
    try:
        if "--verify" in sys.argv:
            if verify():
                print("OK: power study reproduces byte-for-byte.")
                return
            sys.exit(1)
        path, study = write_artifact()
        _report(study)
        shown = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
        print(f"\nwrote {shown}")
    except PowerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
