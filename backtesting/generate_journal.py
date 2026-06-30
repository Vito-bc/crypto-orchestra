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
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT  = Path(__file__).resolve().parents[1]
VAULT = ROOT / "obsidian_vault"

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
