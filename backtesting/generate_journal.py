"""
Obsidian Journal Generator for CryptoOrchestra.

Converts all system data into Obsidian-compatible Markdown notes:
  - logs/trade_history.jsonl       → TradeJournal/
  - logs/agent_decisions.jsonl     → AgentOutputs/
  - backtesting/monte_carlo_per_asset.json → Backtests/
  - Hardcoded strategy history     → Strategies/ and ErrorsAndFixes/

Usage:
    python backtesting/generate_journal.py
    python backtesting/generate_journal.py --vault /path/to/vault
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT  = Path(__file__).resolve().parents[1]
VAULT = ROOT / "obsidian_vault"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ── Helpers ───────────────────────────────────────────────────────────────────

def _ensure(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    print(f"  ✓  {path.relative_to(VAULT)}")


def _fmt_dt(iso: str) -> str:
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return iso


def _pnl_icon(pnl: float) -> str:
    return "🟢 WIN" if pnl > 0 else "🔴 LOSS"


# ── Trade Journal ─────────────────────────────────────────────────────────────

def generate_trade_notes() -> int:
    folder = _ensure(VAULT / "TradeJournal")
    path   = ROOT / "logs" / "trade_history.jsonl"
    if not path.exists():
        print("  No trade_history.jsonl found — skipping TradeJournal")
        return 0

    trades = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                trades.append(json.loads(line))
            except json.JSONDecodeError:
                pass

    for t in trades:
        asset      = t.get("asset", "UNKNOWN")
        reason     = t.get("reason", "UNKNOWN")
        pnl_pct    = t.get("pnl_pct", 0.0)
        pnl_usd    = t.get("pnl_usd", 0.0)
        entry_time = t.get("entry_time", "")
        exit_time  = t.get("closed_at_utc") or t.get("exit_time", "")
        entry_px   = t.get("entry_price", 0.0)
        exit_px    = t.get("exit_price", 0.0)
        hold_h     = t.get("hold_hours", 0.0)
        qty_usd    = t.get("qty_usd", 0.0)
        trade_id   = t.get("id", "unknown")

        date_str = entry_time[:10] if entry_time else "0000-00-00"
        result   = "WIN" if pnl_pct > 0 else "LOSS"
        icon     = _pnl_icon(pnl_pct)
        slug     = f"{date_str}_{asset.replace('-USD','')}_{reason}"
        fname    = folder / f"{slug}.md"

        tags = [
            result.lower(),
            asset.lower().replace("-", ""),
            reason.lower().replace("_", "-"),
        ]
        if hold_h < 12:
            tags.append("quick-exit")
        if abs(pnl_pct) > 5:
            tags.append("large-move")

        note = f"""---
date: {date_str}
asset: {asset}
type: trade
result: {result}
pnl_pct: {pnl_pct:+.2f}
pnl_usd: {pnl_usd:+.2f}
reason: {reason}
hold_hours: {hold_h:.1f}
position_usd: {qty_usd}
trade_id: {trade_id}
tags: [{", ".join(tags)}]
---

# {icon}  {asset} — {date_str}

## Entry / Exit
| Field       | Value |
|-------------|-------|
| Entry time  | {_fmt_dt(entry_time)} |
| Exit time   | {_fmt_dt(exit_time)} |
| Entry price | ${entry_px:,.2f} |
| Exit price  | ${exit_px:,.2f} |
| Hold        | {hold_h:.1f}h |
| Position    | ${qty_usd} |
| **Result**  | **{pnl_pct:+.2f}% ({pnl_usd:+.2f} USD)** |
| Reason      | {reason} |

## What happened
> *Fill in manually or via n8n auto-note*

## Agent consensus at entry
> *Check [[AgentOutputs]] for decisions around {date_str}*

## Lessons
- [ ] Was entry timing correct?
- [ ] Did the stop make sense for this volatility?
- [ ] Did any filter warn against this trade?

## Related
- [[{asset.replace("-USD", "")} Strategy]]
- [[ErrorsAndFixes/{date_str}_{asset.replace("-USD","")}]] *(if applicable)*
- [[Strategies/Per-Asset Parameters]]
"""

        _write(fname, note)

    print(f"  → {len(trades)} trade notes written")
    return len(trades)


# ── Agent Outputs ─────────────────────────────────────────────────────────────

def generate_agent_notes() -> None:
    folder = _ensure(VAULT / "AgentOutputs")
    path   = ROOT / "logs" / "agent_decisions.jsonl"
    if not path.exists():
        print("  No agent_decisions.jsonl — skipping")
        return

    decisions: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                decisions.append(json.loads(line))
            except json.JSONDecodeError:
                pass

    # Group by date
    by_date: dict[str, list[dict]] = {}
    for d in decisions:
        ts   = d.get("logged_at_utc", "")
        date = ts[:10] if ts else "unknown"
        by_date.setdefault(date, []).append(d)

    for date, items in sorted(by_date.items()):
        lines = [f"# Agent Decisions — {date}\n"]
        for d in items:
            action     = d.get("action", "?")
            asset      = d.get("asset", "?")
            conf       = d.get("confidence", 0.0)
            reasoning  = d.get("reasoning", "")[:400]
            icon       = "🟢" if action == "BUY" else ("🔴" if action == "SELL" else "⚪")
            lines.append(f"## {icon} {asset} → {action}  (conf: {conf:.0%})\n")
            lines.append(f"{reasoning}...\n\n")

        _write(folder / f"{date}_decisions.md", "\n".join(lines))

    print(f"  → {len(by_date)} agent-output notes written")


# ── Backtest Results ──────────────────────────────────────────────────────────

def generate_backtest_notes() -> None:
    folder = _ensure(VAULT / "Backtests")

    # Per-asset Monte Carlo results
    mc_path = ROOT / "backtesting" / "monte_carlo_per_asset.json"
    if mc_path.exists():
        mc = json.loads(mc_path.read_text(encoding="utf-8"))
        period = mc.get("period", "full_year")
        assets = mc.get("assets", {})

        rows = []
        for asset, data in assets.items():
            n      = data.get("n_trades", 0)
            stop   = data.get("atr_stop", 2.0)
            target = data.get("atr_target", 3.5)
            mc2    = data.get("mc_2pct") or {}
            wr     = mc2.get("win_rate", 0)
            exp    = mc2.get("expectancy_pct", 0)
            ruin   = mc2.get("ruin_pct", 0)
            rows.append(
                f"| {asset} | {n} | {wr:.1%} | {exp:+.3f}% | {stop}x / {target}x | {ruin:.1f}% |"
            )

        note = f"""---
date: {datetime.now(timezone.utc).strftime("%Y-%m-%d")}
type: backtest
period: {period}
tags: [backtest, monte-carlo, per-asset]
---

# Per-Asset Monte Carlo — {period}

Full year backtest (Aug 2024 – Jun 2025) covering 3 market regimes:
crash → +60% Trump rally → -28% Q1 bear

## Results (2% position sizing, 10,000 simulations)

| Asset | Trades | Win Rate | Expectancy | ATR Stop/Target | Ruin Risk |
|-------|--------|----------|------------|-----------------|-----------|
{chr(10).join(rows)}

## Key Findings
- **ZEC-USD**: Best performer. Near breakeven in multi-regime test. Profitable in bull-only.
- **ETH-USD**: Improved with wider stop (2.5x). Win rate 44% across all regimes.
- **BTC-USD**: Conservative mover, but low win rate (35%) dragged by bear periods.
- **SOL-USD**: High volatility, similar profile to ETH but less consistent.
- **Ruin risk = 0%** across all assets at both 2% and 5% sizing.

## Regime Breakdown
| Period | Win Rate | Avg P&L |
|--------|----------|---------|
| Trump Rally (Nov–Jan 2024) | 48% | -0.40% |
| Q1 2025 Bear (Jan–Apr 2025) | 32% | -1.39% |
| Full Year (Aug 2024–Jun 2025) | 42% | -0.71% |

## Next Steps
- [ ] Walk-forward validation (out-of-sample)
- [ ] Live Coinbase test with $100
- [ ] Monitor ZEC signals in next bull regime

## Related
- [[Strategies/Per-Asset Parameters]]
- [[Strategies/Changelog]]
"""
        _write(folder / "full_year_per_asset_MC.md", note)


# ── Strategy Notes ────────────────────────────────────────────────────────────

def generate_strategy_notes() -> None:
    folder = _ensure(VAULT / "Strategies")

    # Changelog
    changelog = """---
type: strategy
tags: [strategy, changelog, parameters]
---

# CryptoOrchestra — Strategy Changelog

## v4 — Per-Asset ATR Parameters (Jun 30 2026)
**Problem:** ETH and SOL had consistently negative P&L due to intraday wicks hitting 2.0x ATR stops.
**Fix:** ETH/SOL → stop=2.5x, target=4.5x (R:R=1.80). BTC/ZEC → keep stop=2.0x, target=3.5x.
**Result:** ETH win rate 44% across full year. Ruin risk stays 0%.
- [[Backtests/full_year_per_asset_MC]]

## v3 — Whipsaw Guard (Jun 29 2026)
**Problem:** 3 consecutive ZEC stop-losses on May 25-26 2026 (whipsaw cluster).
**Fix:** Block entry if 2+ stops in 96h on same asset (live). 48h window in scanner.
**Result:** 79 false signals blocked across full_year period.
- [[ErrorsAndFixes/ZEC_Whipsaw_May2026]]

## v2 — R:R Ratio Fix (Jun 28 2026)
**Problem:** ATR_STOP=2.5 / ATR_TARGET=2.0 → R:R=0.8. System was losing by math.
**Fix:** stop=2.0x, target=3.5x → R:R=1.75. ETH max_hold 24h→36h.
**Result:** Trump rally avg P&L improved from -0.64% to -0.40%. ZEC became profitable (+0.34%).

## v1 — Initial System (Apr 2026)
6-agent orchestration: breakout, macro, sentiment, fundamental, on-chain, risk.
Conservative DRY_RUN paper trading. First 7 live paper trades accumulated.
"""
    _write(folder / "Changelog.md", changelog)

    # Per-asset parameters
    params = """---
type: strategy
tags: [parameters, atr, per-asset]
---

# Per-Asset Trading Parameters

Last updated: Jun 30 2026

| Asset | ATR Stop | ATR Target | R:R | Max Hold | Notes |
|-------|----------|------------|-----|----------|-------|
| BTC-USD | 2.0x | 3.5x | 1.75 | 48h | Clean mover, no wick problem |
| ETH-USD | 2.5x | 4.5x | 1.80 | 36h | Wick-heavy — wider stop needed |
| SOL-USD | 2.5x | 4.5x | 1.80 | 36h | High volatility, same as ETH |
| ZEC-USD | 2.0x | 3.5x | 1.75 | 36h | Best performer, clean trends |

## Why ETH/SOL use wider stops
ETH and SOL have large intraday wicks — price spikes 2-4% below entry in 2-6h then
recovers. With 2.0x ATR stop, those wicks trigger stop-loss even when direction is correct.
At 2.5x ATR, wicks are absorbed. Target scaled up to 4.5x to maintain R:R > 1.75.

## Position Sizing
- Conservative: 2% per trade ($2 on $100 account — below minimums)
- Testing: 5% per trade ($5 on $100 — above Coinbase minimum ~$1)
- Recommended for $100 live test: 5-10%

## Related
- [[Strategies/Changelog]]
- [[Backtests/full_year_per_asset_MC]]
"""
    _write(folder / "Per-Asset Parameters.md", params)


# ── Errors and Fixes ──────────────────────────────────────────────────────────

def generate_error_notes() -> None:
    folder = _ensure(VAULT / "ErrorsAndFixes")

    zec_whipsaw = """---
date: 2026-05-25
type: error
asset: ZEC-USD
tags: [zec, whipsaw, consecutive-losses, fixed]
---

# ZEC Whipsaw — 3 Stops in 24h (May 25-26 2026)

## What happened
Three consecutive ZEC stop-losses fired within 24 hours:
- 2026-05-25 23:06 → STOP_LOSS (-5.12%, 7h hold)
- 2026-05-26 05:06 → STOP_LOSS (-5.52%, 14h hold)
- 2026-05-26 11:13 → STOP_LOSS (-5.82%, 12h hold)

**Total damage: -$32.92 from $600 position exposure**

## Root cause
Market was in a rapid downtrend (ZEC dropped ~10% in 24h). The EMA50 cross signal
kept firing as price bounced off declining levels, but each bounce failed immediately.
The 4h trend filter was borderline — price just above EMA50 on 4h but momentum was down.

## Fix applied
Whipsaw guard added to `pipeline/runner.py`:
- Block new entry if 2+ stops hit in 96h on same asset
- Same guard mirrored in `signal_scanner.py` (48h window)
- [[Strategies/Changelog#v3]]

## Lesson
In a rapidly declining market, the EMA50 cross on 1h can fire repeatedly as price
bounces during a downtrend. The 4h filter alone is not enough when 4h is borderline.
The whipsaw guard catches this case regardless of regime.
"""
    _write(folder / "ZEC_Whipsaw_May2026.md", zec_whipsaw)

    rr_fix = """---
date: 2026-06-28
type: error
tags: [rr-ratio, parameters, fixed, critical]
---

# R:R Ratio Bug — System Was Losing by Math

## What happened
Initial parameters: ATR_STOP=2.5x, ATR_TARGET=2.0x
This gives R:R = 2.0/2.5 = **0.80** — the system needs >50% win rate just to break even
at R:R=1.0. At R:R=0.8, breakeven requires 55.6% win rate. We had 35-48%.

## Fix applied
Changed to: ATR_STOP=2.0x, ATR_TARGET=3.5x → **R:R = 1.75**
At R:R=1.75, breakeven win rate = 1/(1+1.75) = **36.4%** — achievable.
- [[Strategies/Changelog#v2]]

## Lesson
Always verify R:R = target_mult / stop_mult before running any backtest.
A strategy can have a 48% win rate and still lose money if R:R < 1.0.
"""
    _write(folder / "RR_Ratio_Bug.md", rr_fix)


# ── Research (derived from committed artifacts — never hand-written) ──────────

def _require(pattern: str, text: str, source: Path, what: str) -> "re.Match[str]":
    """
    Regex-extract a fact from a committed doc, or fail loudly.

    generate_journal.py must not retype validation/fee/cost numbers as Python
    literals — that lets the vault silently drift from the documents that
    actually govern them. Deriving by regex means a source-format change is
    caught here (raise) instead of shipping a page with a blank or, worse, a
    stale number that happens to still parse.
    """
    m = re.search(pattern, text, re.DOTALL)
    if not m:
        raise RuntimeError(
            f"generate_journal.py Research page: could not find {what} in "
            f"{source.relative_to(ROOT)}. The source format changed — fix the "
            "parser rather than emit a page with a blank or a stale number."
        )
    return m


def generate_research_notes() -> None:
    folder = _ensure(VAULT / "Research")

    from backtesting.cost_sensitivity import (
        _ARTIFACT_EXPECTANCY_PCT,
        _ARTIFACT_N_CLOSED,
        _ARTIFACT_PF,
        _CANDIDATE_MAKER_RATE,
        _CANDIDATE_TAKER_RATE,
    )
    from pipeline.fees import CURRENT_SCHEDULE

    # ── 1. Current validation status: docs/trial_registry.md + CLAUDE.md ──────
    registry_path = ROOT / "docs" / "trial_registry.md"
    claude_md_path = ROOT / "CLAUDE.md"
    for p in (registry_path, claude_md_path):
        if not p.exists():
            raise RuntimeError(f"Research page needs {p} — not found")
    registry_text = registry_path.read_text(encoding="utf-8")
    claude_text = claude_md_path.read_text(encoding="utf-8")

    m = _require(
        r"V2 momentum \(ZEC\): \*\*PF ([\d.]+) \((-?[\d.]+)%/trade, n=(\d+)\)",
        claude_text, claude_md_path, "the V2 ZEC headline PF/expectancy/n")
    v2_pf, v2_expectancy_pct, v2_n = m.group(1), m.group(2), m.group(3)

    _require(r"Do NOT switch `DRY_RUN=false` on current evidence\.",
              claude_text, claude_md_path, "the DRY_RUN refusal statement")
    _require(r"LIVE\s+\*?\*?NO-GO", registry_text, registry_path,
              "the LIVE NO-GO verdict")
    _require(r"`DRY_RUN=true`", registry_text, registry_path,
              "the DRY_RUN=true statement")

    # ── 2. Operational fee schedule: pipeline/fees.py + fee-tier evidence ──────
    #
    # The evidence filename is derived from CURRENT_SCHEDULE.source rather than
    # hardcoded to one date: this same page previously pinned
    # "fee_tier_2026-09-15.json" as a literal, and that literal silently
    # stopped matching CURRENT_SCHEDULE the moment the 2026-09-22 tier was
    # adopted — the mismatch below would have raised regardless of whether
    # anyone remembered to update this path by hand. Deriving it means the
    # NEXT tier change can't reintroduce the same failure mode.
    m = _require(r"^(docs/operations/fee_tier_[\d-]+\.json)",
                  CURRENT_SCHEDULE.source, ROOT / "pipeline" / "fees.py",
                  "CURRENT_SCHEDULE's evidence file path in its source field")
    # `m` is reassigned by every later _require() call in this function, so
    # the group is captured into its own name now rather than read back off
    # `m` inside the f-string built at the end — that read would silently
    # pick up whatever `m` was rebound to last, not this match.
    evidence_relpath = m.group(1)
    evidence_path = ROOT / evidence_relpath
    if not evidence_path.exists():
        raise RuntimeError(f"Research page needs {evidence_path} — not found")
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    measured = evidence["measured_tier"]
    if (measured["maker_fee_rate"] != CURRENT_SCHEDULE.maker_rate
            or measured["taker_fee_rate"] != CURRENT_SCHEDULE.taker_rate):
        raise RuntimeError(
            "Research page: pipeline/fees.py CURRENT_SCHEDULE "
            f"({CURRENT_SCHEDULE.maker_rate}/{CURRENT_SCHEDULE.taker_rate}) "
            f"disagrees with {evidence_path.name}'s measured_tier "
            f"({measured['maker_fee_rate']}/{measured['taker_fee_rate']}) — "
            "these must describe the same adopted schedule"
        )
    # Whether the adopted cohort was independently audited is itself a fact
    # that changes between schedules (the 2026-09-15 cohort was; the
    # 2026-09-22 one was only reproduced from the local probe log, not
    # separately audited) — read from the evidence file rather than asserted
    # as a generator literal, so an unaudited adoption is never described as
    # audited.
    audited = bool(evidence.get("evidence", {}).get("independently_audited"))
    audited_clause = (
        "a measured, independently audited 4-reading cohort" if audited else
        "a measured 4-reading cohort, reproduced from the local probe log "
        "but not independently audited")

    cost_sensitivity_py = ROOT / "backtesting" / "cost_sensitivity.py"
    cs_source = cost_sensitivity_py.read_text(encoding="utf-8")
    m = _require(r'observed_at="([\d\-:T.+]+)"', cs_source, cost_sensitivity_py,
                  "the candidate reading's observed_at timestamp")
    candidate_observed_at = m.group(1)
    m = _require(r'"pricing_tier":\s*"([^"]+)"', cs_source, cost_sensitivity_py,
                  "the candidate reading's pricing_tier")
    candidate_pricing_tier = m.group(1)
    m = _require(r"4-reading cohort \((\d{4}-\d{2}-\d{2}) → (\d{4}-\d{2}-\d{2})\)",
                  registry_text, registry_path, "the candidate cohort window")
    cohort_start, cohort_end = m.group(1), m.group(2)

    # ── 3. Cost-sensitivity headline: docs/research/2026-09-cost-sensitivity.md ─
    cs_md_path = ROOT / "docs" / "research" / "2026-09-cost-sensitivity.md"
    if not cs_md_path.exists():
        raise RuntimeError(f"Research page needs {cs_md_path} — not found")
    cs_text = cs_md_path.read_text(encoding="utf-8")

    m = _require(
        r"\*\*ADOPTED \(all exit paths[^\n]*\*\*\s*\|\s*0\.6%\s*\|\s*1\.2%\s*\|\s*\*\*\+([\d.]+)%\*\*",
        cs_text, cs_md_path, "the ADOPTED break-even gross move")
    breakeven_adopted = m.group(1)
    m = _require(
        r"\*\*CANDIDATE, not adopted \(all exit paths\)\*\*\s*\|\s*0\.5%\s*\|\s*0\.9%\s*\|\s*\*\*\+([\d.]+)%\*\*",
        cs_text, cs_md_path, "the CANDIDATE break-even gross move")
    breakeven_candidate = m.group(1)

    m = _require(
        r"\*\*PROSPECTIVE SENSITIVITY — ADOPTED \(0\.6%/1\.2%\)\*\*\s*\|\s*114\s*\|\s*\*\*([\d.]+)\*\*\s*\|\s*\*\*(-?[\d.]+)%/trade\*\*",
        cs_text, cs_md_path, "the ADOPTED re-priced PF/expectancy")
    pf_adopted, expectancy_adopted = m.group(1), m.group(2)
    m = _require(
        r"\*\*PROSPECTIVE SENSITIVITY — CANDIDATE, not adopted \(0\.5%/0\.9%\)\*\*\s*\|\s*114\s*\|\s*\*\*([\d.]+)\*\*\s*\|\s*\*\*(-?[\d.]+)%/trade\*\*",
        cs_text, cs_md_path, "the CANDIDATE re-priced PF/expectancy")
    pf_candidate, expectancy_candidate = m.group(1), m.group(2)

    m = _require(r"\| Frozen 1\.0% model \|[^\n]*\*\*(\+?-?[\d.]+)%\*\*\s*\|",
                  cs_text, cs_md_path, "the frozen-model 95% upper bound")
    bound_frozen = float(m.group(1))
    m = _require(r"\| ADOPTED 0\.6%/1\.2% \(measured\) \|[^\n]*\*\*(\+?-?[\d.]+)%\*\*\s*\|",
                  cs_text, cs_md_path, "the ADOPTED 95% upper bound")
    bound_adopted = float(m.group(1))
    m = _require(r"\| CANDIDATE 0\.5%/0\.9% \(not adopted\) \|[^\n]*\*\*(\+?-?[\d.]+)%\*\*\s*\|",
                  cs_text, cs_md_path, "the CANDIDATE 95% upper bound")
    bound_candidate = float(m.group(1))

    if not (bound_frozen > 0 and bound_adopted < 0 and bound_candidate < 0):
        raise RuntimeError(
            "Research page: expected the frozen-model bound above zero and "
            "both operational bounds below zero, but parsed "
            f"frozen={bound_frozen}%, adopted={bound_adopted}%, "
            f"candidate={bound_candidate}% — the write-up's conclusion may "
            "have changed; re-check docs/research/2026-09-cost-sensitivity.md "
            "before regenerating this page"
        )

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    note = f"""---
date: {today}
type: research
tags: [research, validation-status, fees, cost-sensitivity, derived]
---

# Research Status — derived, not hand-written

> Every figure on this page is parsed from a committed source file at
> generation time (`backtesting/generate_journal.py::generate_research_notes`).
> None of it is a literal typed into the generator. If a source file's format
> changes in a way that breaks parsing, vault regeneration fails loudly
> instead of publishing a blank or stale number.

## 1. Current validation status

**DRY_RUN / LIVE NO-GO.** This system trades in paper/shadow mode only. No
real-money trading is authorized.

- V2 momentum (ZEC-USD, frozen mechanism): **PF {v2_pf} ({v2_expectancy_pct}%/trade, n={v2_n})**
  — not profitable at the historical research fee assumption.
- `docs/trial_registry.md` records the **LIVE NO-GO** verdict.
- `CLAUDE.md` is explicit: *"Do NOT switch `DRY_RUN=false` on current evidence."*

Source: [[../../docs/trial_registry.md|docs/trial_registry.md]], `CLAUDE.md`
("Validation Status").

## 2. Operational fee schedule

| | Maker | Taker | Tier |
|---|---:|---:|---|
| **ADOPTED** — `pipeline/fees.py` `CURRENT_SCHEDULE` | {CURRENT_SCHEDULE.maker_rate:.1%} | {CURRENT_SCHEDULE.taker_rate:.1%} | {CURRENT_SCHEDULE.tier_name} |
| **CANDIDATE — NOT ADOPTED** — single {candidate_observed_at} reading | {_CANDIDATE_MAKER_RATE:.1%} | {_CANDIDATE_TAKER_RATE:.1%} | {candidate_pricing_tier} |

The adopted schedule (`{CURRENT_SCHEDULE.schedule_id}`) is {audited_clause} —
see `{evidence_relpath}`. The candidate tier is a single {candidate_observed_at}
reading and is **not written into `pipeline/fees.py`**;
`active_schedule()` still returns only the adopted schedule. Formal adoption
waits for its own 4-reading cohort ({cohort_start} → {cohort_end}) before any
`FeeSchedule` entry is added for it.

Source: `pipeline/fees.py`, `{evidence_relpath}`,
`backtesting/cost_sensitivity.py`.

## 3. Cost-sensitivity headline (trial `2026-09-cost-sensitivity.v1`)

> Prospective cost sensitivity, not an edge test — see
> `docs/research/2026-09-cost-sensitivity.md` for the full write-up and every
> caveat on how these numbers may and may not be used.

**Break-even gross move** (the same across STOP_LOSS/MAX_HOLD/TAKE_PROFIT,
because `close_position()` prices every exit at the taker rate):

| Schedule | Break-even gross move |
|---|---:|
| ADOPTED (measured) | +{breakeven_adopted}% |
| CANDIDATE, not adopted | +{breakeven_candidate}% |

**The frozen V2 ZEC mechanism's own n={_ARTIFACT_N_CLOSED} trades, re-priced**
(same entries/exits, different fee assumption — not a restatement of
`docs/research/artifacts/results.json`):

| Fee model | PF | Expectancy |
|---|---:|---:|
| Frozen artifact (historical fee model) | {_ARTIFACT_PF:.5f} | {_ARTIFACT_EXPECTANCY_PCT * 100:+.2f}%/trade |
| PROSPECTIVE — ADOPTED | {pf_adopted} | {expectancy_adopted}%/trade |
| PROSPECTIVE — CANDIDATE, not adopted | {pf_candidate} | {expectancy_candidate}%/trade |

**One-sided 95% upper bound on the true per-trade edge:**

| Scenario | Upper bound | Side of zero |
|---|---:|---|
| Frozen 1.0% model | {bound_frozen:+.4f}% | **Above zero** |
| ADOPTED, measured | {bound_adopted:+.4f}% | **Below zero** |
| CANDIDATE, not adopted | {bound_candidate:+.4f}% | **Below zero** |

The frozen-model bound sits above zero while both operational-cost bounds sit
below zero: at either the adopted or the candidate operational fee schedule,
this sample rules out a profitable version of the frozen mechanism at 95%
one-sided confidence. This does not change `DRY_RUN`, `LIVE_BALANCE_USD`,
`ASSET_CONFIG`, V3 status, or Phase 7B status.

Source: [[../../docs/research/2026-09-cost-sensitivity.md|docs/research/2026-09-cost-sensitivity.md]].
"""
    _write(folder / "Research Status.md", note)


# ── README ────────────────────────────────────────────────────────────────────

def generate_readme() -> None:
    readme = """# CryptoOrchestra — Second Brain

This vault is the living knowledge base for the CryptoOrchestra multi-agent trading system.
It grows automatically: every trade, backtest, and agent decision is logged here.

## Vault Structure

| Folder | Contents |
|--------|----------|
| [[TradeJournal/]] | One note per closed trade — P&L, hold time, lessons |
| [[Backtests/]] | Scanner results, Monte Carlo outputs, period analyses |
| [[Research/]] | Validation status, fee schedule, and cost-sensitivity headline — derived from committed docs, regenerated each run |
| [[Strategies/]] | Parameters, changelog, approach decisions |
| [[ErrorsAndFixes/]] | Documented mistakes and how they were fixed |
| [[AgentOutputs/]] | Daily agent decision logs |
| [[MarketNotes/]] | Market regime observations |
| [[_Templates/]] | Note templates for manual additions |

## Quick Stats
> Update after each weekly review

- **Total live trades:** 7
- **Live P&L:** -$46.62 (bear market period — all stops)
- **Best asset:** ZEC (48.6% win rate in full-year backtest)
- **System status:** DRY_RUN=true | Targeting live $100 test

## Weekly Review Prompt (copy to Claude)
```
Read the last 20 notes in TradeJournal/ and ErrorsAndFixes/.
Find: 1) most common reason for losses, 2) any repeating pattern,
3) one parameter experiment to try next week.
```

## Key Decisions
- [[Strategies/Changelog]] — full history of parameter changes
- [[Backtests/full_year_per_asset_MC]] — latest Monte Carlo results
- [[ErrorsAndFixes/ZEC_Whipsaw_May2026]] — most important lesson so far
"""
    _write(VAULT / "README.md", readme)


# ── Templates ─────────────────────────────────────────────────────────────────

def generate_templates() -> None:
    folder = _ensure(VAULT / "_Templates")

    trade_tpl = """---
date: {{date}}
asset: {{asset}}
type: trade
result: WIN|LOSS
pnl_pct:
reason: STOP_LOSS|TAKE_PROFIT|MAX_HOLD
hold_hours:
tags: []
---

# {{icon}} {{asset}} — {{date}}

## Entry / Exit
| Field | Value |
|-------|-------|
| Entry time | |
| Exit time  | |
| Entry price | |
| Exit price  | |
| P&L | |

## What happened

## Agent consensus

## Lessons
- [ ]
"""
    _write(folder / "Trade Note.md", trade_tpl)

    weekly_tpl = """---
date: {{week_start}}
type: weekly-review
tags: [weekly-review]
---

# Weekly Review — {{week_start}}

## Trades this week
| Asset | Result | P&L | Notes |
|-------|--------|-----|-------|
| | | | |

## Agent consensus quality
*Were agents aligned on winning trades? Did they disagree on losing ones?*

## Pattern spotted

## Parameter experiment for next week

## Overall mood
"""
    _write(folder / "Weekly Review.md", weekly_tpl)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    global VAULT

    if "--vault" in sys.argv:
        idx  = sys.argv.index("--vault")
        VAULT = Path(sys.argv[idx + 1])

    print(f"\nGenerating Obsidian vault at: {VAULT}\n")

    print("TradeJournal/")
    generate_trade_notes()

    print("\nAgentOutputs/")
    generate_agent_notes()

    print("\nBacktests/")
    generate_backtest_notes()

    print("\nResearch/")
    generate_research_notes()

    print("\nStrategies/")
    generate_strategy_notes()

    print("\nErrorsAndFixes/")
    generate_error_notes()

    print("\n_Templates/")
    generate_templates()

    print("\nREADME.md")
    generate_readme()

    print(f"\nDone. Open {VAULT} as an Obsidian vault.")
    print("Install Obsidian from https://obsidian.md (free) and open this folder.")


if __name__ == "__main__":
    main()
