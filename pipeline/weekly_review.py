"""
Weekly Review — reads the last 7 days of agent_decisions.jsonl and
produces a human-readable performance report sent to Telegram.

Answers:
  - How many decisions were made per asset?
  - What was the BUY/SELL/HOLD breakdown?
  - Which agent disagreed most with the final decision?
  - Which agent had the highest average confidence?
  - Were any vetoes triggered?
  - What levels did the orchestrator set (stops/targets)?

Run manually:
    python pipeline/weekly_review.py

Or schedule via Windows Task Scheduler (weekly, e.g. Sundays at 9am).
"""

from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from notifications.telegram import send_telegram_message

LOG_DIR       = ROOT / "logs"
DECISIONS_LOG = LOG_DIR / "agent_decisions.jsonl"


def load_recent(days: int = 7) -> list[dict]:
    if not DECISIONS_LOG.exists():
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    records = []
    with DECISIONS_LOG.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
                ts = datetime.fromisoformat(r["logged_at_utc"])
                if ts >= cutoff:
                    records.append(r)
            except (json.JSONDecodeError, KeyError):
                continue
    return records


def build_report(records: list[dict]) -> str:
    if not records:
        return "Weekly Review: no decisions logged in the last 7 days."

    lines = [
        "CRYPTO ORCHESTRA — WEEKLY REVIEW",
        f"Period: last 7 days  |  Decisions: {len(records)}",
        "=" * 50,
    ]

    # ── Per-asset breakdown ───────────────────────────────
    by_asset: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        by_asset[r["asset"]].append(r)

    for asset, recs in sorted(by_asset.items()):
        actions = Counter(r["action"] for r in recs)
        vetoes  = sum(1 for r in recs if r.get("veto_triggered"))
        avg_conf = sum(r["confidence"] for r in recs) / len(recs)

        lines.append(f"\n{asset}  ({len(recs)} decisions)")
        lines.append(f"  BUY={actions['BUY']}  SELL={actions['SELL']}  HOLD={actions['HOLD']}")
        lines.append(f"  Avg confidence: {avg_conf:.0%}")
        if vetoes:
            lines.append(f"  Vetoes triggered: {vetoes}")

    # ── Agent performance ─────────────────────────────────
    lines.append("\n" + "=" * 50)
    lines.append("AGENT STATS (avg confidence + agreement with final)")

    agent_conf:    dict[str, list[float]] = defaultdict(list)
    agent_agreed:  dict[str, int]         = defaultdict(int)
    agent_total:   dict[str, int]         = defaultdict(int)

    for r in records:
        final_action = r["action"]
        # Map action to signal for comparison
        action_to_signal = {"BUY": "BUY", "SELL": "SELL", "HOLD": "NEUTRAL"}
        final_signal = action_to_signal.get(final_action, "NEUTRAL")

        for vote in r.get("votes", []):
            name = vote["agent"]
            agent_conf[name].append(vote["confidence"])
            agent_total[name] += 1
            # "agreed" = agent signal matches final action direction
            if vote["signal"] == final_signal:
                agent_agreed[name] += 1
            elif final_action == "HOLD" and vote["signal"] == "NEUTRAL":
                agent_agreed[name] += 1

    for agent in sorted(agent_conf.keys()):
        confs  = agent_conf[agent]
        total  = agent_total[agent]
        agreed = agent_agreed[agent]
        avg_c  = sum(confs) / len(confs) if confs else 0
        agree_pct = agreed / total if total else 0
        lines.append(f"  {agent:<12} avg_conf={avg_c:.0%}  agreement={agree_pct:.0%}  ({total} signals)")

    # ── Override summary ──────────────────────────────────
    all_overrides = [o for r in records for o in r.get("overrides", [])]
    if all_overrides:
        lines.append(f"\nTotal overrides this week: {len(all_overrides)}")

    # ── Most recent decision per asset ────────────────────
    lines.append("\n" + "=" * 50)
    lines.append("LATEST DECISION PER ASSET")
    for asset, recs in sorted(by_asset.items()):
        last = recs[-1]
        lines.append(
            f"  {asset}: {last['action']} "
            f"(conf={last['confidence']:.0%}) — {last['reasoning'][:80]}…"
        )

    lines.append("\nGenerated: " + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    return "\n".join(lines)


def main() -> None:
    print("Loading last 7 days of decisions…")
    records = load_recent(days=7)
    report  = build_report(records)

    print("\n" + report)

    sent = send_telegram_message(report)
    print(f"\nTelegram: {'sent' if sent else 'skipped (no token)'}")


if __name__ == "__main__":
    main()
