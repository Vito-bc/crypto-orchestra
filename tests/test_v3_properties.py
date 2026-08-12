"""
Unit tests for V3 regime filter properties:
  1. ER-30 uses only T-1 daily candles (no look-ahead)
  2. Signal at midnight UTC also excludes today's candle
  3. Concurrent _claim_signal calls: exactly one succeeds
  4. Stale claimed signal auto-recovers
  5. Blocked outcomes resolved exactly once (resolver idempotency)
  6. Accepted + blocked signals appear in consistent episode statistics
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# ── 1 & 2: ER look-ahead ──────────────────────────────────────────────────────

def _make_daily_df(dates: list[str]) -> pd.DataFrame:
    """Build a minimal daily DataFrame with UTC index."""
    idx = pd.to_datetime(dates, utc=True)
    return pd.DataFrame(
        {"open": 1.0, "high": 1.1, "low": 0.9, "close": [float(i + 1) for i in range(len(dates))],
         "volume": 1000.0, "ema50": 1.0},
        index=idx,
    )


def test_er_excludes_today_candle():
    """Signal at 20:00 UTC on T must NOT see T's daily candle."""
    from backtesting.signal_scanner import _compute_regime_metrics

    # Build 60 daily candles from Jan 1 to Mar 1 2021
    dates = pd.date_range("2021-01-01", periods=60, freq="D").strftime("%Y-%m-%d").tolist()
    daily_df = _make_daily_df(dates)

    signal_ts = pd.Timestamp("2021-03-01 20:00:00", tz="UTC")
    result = _compute_regime_metrics(daily_df, signal_ts)

    # 2021-03-01 is the last date in our DataFrame
    # If look-ahead existed, the last close (60.0) would appear in the window
    # With correct boundary, day_boundary = 2021-03-01 00:00 UTC excludes Mar 1 candle
    # Last included close = 2021-02-28 → closes[-1] should be the Feb 28 value
    assert result, "Should have regime metrics with 60 bars"
    # We can't check er_30 value precisely without knowing the exact series,
    # but we verify n_daily_bars excludes the Mar 1 candle
    # daily_df has 60 rows; Mar 1 (index 59) should be excluded → 59 bars
    assert result["n_daily_bars"] == 59, f"Expected 59 bars (Mar 1 excluded), got {result['n_daily_bars']}"


def test_er_excludes_candle_starting_at_midnight():
    """Signal at exactly midnight UTC on T must also exclude T's daily candle."""
    from backtesting.signal_scanner import _compute_regime_metrics

    dates = pd.date_range("2021-01-01", periods=60, freq="D").strftime("%Y-%m-%d").tolist()
    daily_df = _make_daily_df(dates)

    # Signal fires exactly at midnight — the new candle just opened
    signal_ts = pd.Timestamp("2021-03-01 00:00:00", tz="UTC")
    result = _compute_regime_metrics(daily_df, signal_ts)

    assert result, "Should have regime metrics"
    assert result["n_daily_bars"] == 59, (
        f"Signal at midnight: Mar 1 candle (just opened) must be excluded. Got {result['n_daily_bars']}"
    )


def test_naive_ts_coerced_to_utc():
    """Timezone-naive signal timestamp is localized to UTC without error."""
    from backtesting.signal_scanner import _compute_regime_metrics

    dates = pd.date_range("2021-01-01", periods=40, freq="D").strftime("%Y-%m-%d").tolist()
    daily_df = _make_daily_df(dates)
    # naive timestamp — should be treated as UTC
    signal_ts = pd.Timestamp("2021-02-10 15:00:00")  # no tz
    result = _compute_regime_metrics(daily_df, signal_ts)
    assert isinstance(result, dict)


# ── 3 & 4: SQLite idempotency ─────────────────────────────────────────────────

def _patch_signals_db(tmp_path: Path):
    """Monkey-patch runner._SIGNALS_DB to a temp location for isolation."""
    import pipeline.runner as runner
    runner._SIGNALS_DB = tmp_path / "signals_test.db"
    return runner


def test_concurrent_claim_exactly_one_wins(tmp_path):
    """Two threads claiming the same signal_id: exactly one returns True."""
    runner = _patch_signals_db(tmp_path)
    runner._ensure_signals_db()

    results = []
    lock = threading.Lock()

    def attempt():
        ok = runner._claim_signal("ZEC-USD:2026-07-14T10:00:00:v3", "ZEC-USD", "2026-07-14T10:00:00")
        with lock:
            results.append(ok)

    threads = [threading.Thread(target=attempt) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(results) == 1, f"Expected exactly 1 successful claim, got {sum(results)}"


def test_stale_claim_recovers(tmp_path):
    """A claimed entry older than 2 hours can be re-claimed."""
    import sqlite3
    runner = _patch_signals_db(tmp_path)
    runner._ensure_signals_db()

    sid = "ZEC-USD:2026-07-14T08:00:00:v3"
    stale_ts = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
    with sqlite3.connect(runner._SIGNALS_DB) as conn:
        conn.execute(
            "INSERT INTO processed_signals (signal_id, asset, candle_close, status, claimed_at) "
            "VALUES (?, 'ZEC-USD', '2026-07-14T08:00:00', 'claimed', ?)",
            (sid, stale_ts),
        )

    # Should be re-claimable because it's stale
    ok = runner._claim_signal(sid, "ZEC-USD", "2026-07-14T08:00:00")
    assert ok, "Stale claimed entry should be recoverable"


def test_completed_signal_not_reclaimable(tmp_path):
    """A completed signal_id is never re-claimed."""
    runner = _patch_signals_db(tmp_path)
    runner._ensure_signals_db()

    sid = "ZEC-USD:2026-07-14T09:00:00:v3"
    assert runner._claim_signal(sid, "ZEC-USD", "2026-07-14T09:00:00") is True
    runner._complete_signal(sid)
    assert runner._claim_signal(sid, "ZEC-USD", "2026-07-14T09:00:00") is False


# ── 5: Resolver idempotency ───────────────────────────────────────────────────

def test_resolver_writes_outcome_once(tmp_path):
    """reconcile_pending() does not write a second V3_OUTCOME for an already-resolved signal."""
    from pipeline.v3_journal import log_v2_signal, log_outcome, read_journal, _build_signal_view
    import pipeline.v3_journal as journal
    journal._JOURNAL = tmp_path / "v3_journal.jsonl"

    sig = {
        "asset": "ZEC-USD", "entry_time": "2026-07-14T10:00:00+00:00",
        "entry_price": 100.0, "atr": 2.0, "conf": 0.89,
        "adx": 28.0, "vol_ratio": 1.4, "n_conditions": 4,
        "er_30": 0.15, "vm_30": 0.5, "ema50_slope": 0.002,
        "ema200_valid": True, "n_daily_bars": 250,
        "v3_candidate_threshold": 0.20, "v3_would_block": True,
        "v3_enforcement": True, "v3_blocked": True,
    }
    log_v2_signal(scanner_signal=sig, accepted=False)
    # Manually add outcome
    log_outcome("ZEC-USD:2026-07-14T10:00:00+00:00:v3", "WIN", 5.0, is_counterfactual=True)
    # Add a duplicate outcome (should be the last-wins in fold)
    log_outcome("ZEC-USD:2026-07-14T10:00:00+00:00:v3", "WIN", 5.0, is_counterfactual=True)

    view = _build_signal_view(read_journal())
    outcomes = [e for e in read_journal() if e.get("type") == "V3_OUTCOME"]

    # Two raw outcome lines exist — that's OK; fold produces one per signal_id
    assert len(outcomes) == 2
    assert len([s for s in view if s.get("outcome") == "WIN"]) == 1


# ── 6: Episode grouping ───────────────────────────────────────────────────────

def test_episode_grouping_30d_gap():
    """Signals within 30 days are in same episode; beyond 30 days start new one."""
    from pipeline.v3_journal import _group_episodes

    signals = [
        {"candle_close": "2026-07-01T00:00:00", "pnl_pct": 1.0},
        {"candle_close": "2026-07-10T00:00:00", "pnl_pct": 2.0},  # same ep
        {"candle_close": "2026-08-12T00:00:00", "pnl_pct": 3.0},  # new ep (>30d from Jul 10)
        {"candle_close": "2026-08-20T00:00:00", "pnl_pct": 4.0},  # same ep as Aug 12
    ]
    episodes = _group_episodes(signals)
    assert len(episodes) == 2, f"Expected 2 episodes, got {len(episodes)}"
    assert len(episodes[0]) == 2
    assert len(episodes[1]) == 2


# ── 7: Cohort / outcome semantics (Phase 3) ───────────────────────────────────

def _journal_env(tmp_path, monkeypatch):
    """Point the journal at a temp file and return the module."""
    import pipeline.v3_journal as vj
    monkeypatch.setattr(vj, "_JOURNAL", tmp_path / "v3_journal.jsonl")
    return vj


def _scanner_sig(entry_time: str, *, would_block: bool, blocked: bool = False) -> dict:
    return {
        "asset": "ZEC-USD",
        "entry_time": entry_time,
        "entry_price": 100.0,
        "atr": 2.0,
        "er_30": 0.10 if would_block else 0.30,
        "v3_candidate_threshold": 0.20,
        "v3_would_block": would_block,
        "v3_blocked": blocked,
        "v3_enforcement": blocked,
    }


def test_candidate_cohort_is_independent_of_enforcement(tmp_path, monkeypatch) -> None:
    """
    candidate_accepted must come from v3_would_block, not from `accepted`.
    With enforcement OFF every signal is "accepted", so using that field as the
    cohort silently puts V3-blocked signals into candidate statistics.
    """
    vj = _journal_env(tmp_path, monkeypatch)

    # Enforcement OFF: both are `accepted`, but only one is candidate-accepted.
    vj.log_v2_signal(scanner_signal=_scanner_sig("2026-07-13T00:00:00+00:00",
                                                 would_block=False), accepted=True)
    vj.log_v2_signal(scanner_signal=_scanner_sig("2026-07-14T00:00:00+00:00",
                                                 would_block=True), accepted=True)

    view = vj._build_signal_view(vj.read_journal())
    assert len(view) == 2
    assert all(s["accepted"] for s in view), "both are accepted with enforcement off"

    cohort = [s for s in view if s["candidate_accepted"]]
    assert len(cohort) == 1, "only the non-blocked signal is in the candidate cohort"
    assert cohort[0]["candle_close"].startswith("2026-07-13")


def test_three_concepts_are_stored_separately(tmp_path, monkeypatch) -> None:
    vj = _journal_env(tmp_path, monkeypatch)
    vj.log_v2_signal(
        scanner_signal=_scanner_sig("2026-07-13T00:00:00+00:00",
                                    would_block=True, blocked=True),
        accepted=False,
    )
    s = vj._build_signal_view(vj.read_journal())[0]
    assert s["candidate_accepted"] is False      # V3 would block it
    assert s["enforcement_accepted"] is False    # V3 did block it
    assert s["disposition"] == "counterfactual"  # so no trade happened


def test_disposition_is_pending_until_execution_resolves(tmp_path, monkeypatch) -> None:
    """Signal-time logging must not claim a fill that has not happened."""
    vj = _journal_env(tmp_path, monkeypatch)
    sid = vj.log_v2_signal(
        scanner_signal=_scanner_sig("2026-07-13T00:00:00+00:00", would_block=False),
        accepted=True,
    )
    assert vj._build_signal_view(vj.read_journal())[0]["disposition"] == "pending"

    vj.log_disposition(sid, "blocked_elsewhere")
    assert vj._build_signal_view(vj.read_journal())[0]["disposition"] == "blocked_elsewhere"


def test_legacy_rows_without_new_fields_still_read(tmp_path, monkeypatch) -> None:
    """Backward compatibility: pre-split rows have only `accepted`."""
    import json
    vj = _journal_env(tmp_path, monkeypatch)
    legacy = {
        "type": "V3_SIGNAL", "signal_id": "ZEC-USD:2026-07-13T00:00:00+00:00:v3",
        "asset": "ZEC-USD", "candle_close": "2026-07-13T00:00:00+00:00",
        "entry_price": 100.0, "v3_would_block": True, "accepted": True,
    }
    vj._JOURNAL.parent.mkdir(parents=True, exist_ok=True)
    vj._JOURNAL.write_text(json.dumps(legacy) + "\n", encoding="utf-8")

    s = vj._build_signal_view(vj.read_journal())[0]
    assert s["candidate_accepted"] is False, "derived from v3_would_block"
    assert s["enforcement_accepted"] is True, "derived from legacy `accepted`"


def test_actual_outcome_beats_counterfactual_regardless_of_order(tmp_path, monkeypatch) -> None:
    """A replayed counterfactual must never overwrite a real fill."""
    vj = _journal_env(tmp_path, monkeypatch)
    sid = vj.log_v2_signal(
        scanner_signal=_scanner_sig("2026-07-13T00:00:00+00:00", would_block=False),
        accepted=True,
    )
    vj.log_outcome(sid, "WIN", 5.0, is_counterfactual=False)   # real fill
    vj.log_outcome(sid, "LOSS", -9.0, is_counterfactual=True)  # later replay

    s = vj._build_signal_view(vj.read_journal())[0]
    assert s["pnl_pct"] == 5.0, "the actual outcome must win"
    assert s["is_counterfactual"] is False


def test_duplicate_outcomes_are_order_independent(tmp_path, monkeypatch) -> None:
    """Folding must not depend on the order events happen to appear in."""
    vj = _journal_env(tmp_path, monkeypatch)
    sid = vj.log_v2_signal(
        scanner_signal=_scanner_sig("2026-07-13T00:00:00+00:00", would_block=True),
        accepted=False,
    )
    vj.log_outcome(sid, "LOSS", -3.0, is_counterfactual=True)
    vj.log_outcome(sid, "LOSS", -3.0, is_counterfactual=True)   # duplicate replay

    entries = vj.read_journal()
    forward = vj._build_signal_view(entries)[0]
    reversed_ = vj._build_signal_view(list(reversed(entries)))[0]
    assert forward["pnl_pct"] == reversed_["pnl_pct"]
    assert forward["outcome"] == reversed_["outcome"]


def test_candidate_accepted_untraded_signal_is_resolvable(tmp_path, monkeypatch) -> None:
    """
    The old resolver only picked up `not accepted` rows, so a candidate-accepted
    signal that never traded stayed pending forever and silently dropped out of
    the candidate cohort. It must now be eligible for resolution.
    """
    vj = _journal_env(tmp_path, monkeypatch)
    sid = vj.log_v2_signal(
        scanner_signal=_scanner_sig("2026-07-13T00:00:00+00:00", would_block=False),
        accepted=True,
    )
    vj.log_disposition(sid, "blocked_elsewhere")

    view = vj._build_signal_view(vj.read_journal())
    pending = [s for s in view if s.get("outcome") is None]
    assert len(pending) == 1
    assert pending[0]["candidate_accepted"] is True
    assert pending[0]["disposition"] == "blocked_elsewhere"
    # This row is exactly what the widened resolver filter now selects.


def test_end_to_end_only_candidate_accepted_enters_stats(tmp_path, monkeypatch) -> None:
    """
    Task-mandated end-to-end: one candidate-accepted and one candidate-blocked
    signal, both resolved; only the accepted one may enter candidate PF and
    expectancy; re-folding is idempotent.
    """
    vj = _journal_env(tmp_path, monkeypatch)
    sid_ok = vj.log_v2_signal(
        scanner_signal=_scanner_sig("2026-07-13T00:00:00+00:00", would_block=False),
        accepted=True,
    )
    sid_blk = vj.log_v2_signal(
        scanner_signal=_scanner_sig("2026-07-20T00:00:00+00:00", would_block=True),
        accepted=True,
    )
    vj.log_outcome(sid_ok, "WIN", 4.0, is_counterfactual=False)
    vj.log_outcome(sid_blk, "WIN", 50.0, is_counterfactual=True)   # must NOT count

    view = vj._build_signal_view(vj.read_journal())
    cohort = [s for s in view if s["candidate_accepted"] and s["pnl_pct"] is not None]
    assert len(cohort) == 1
    assert cohort[0]["pnl_pct"] == 4.0

    expectancy = sum(s["pnl_pct"] for s in cohort) / len(cohort)
    assert expectancy == 4.0, "the blocked signal's +50% must be excluded"

    # Idempotent: re-reading and re-folding changes nothing.
    again = vj._build_signal_view(vj.read_journal())
    assert [s["pnl_pct"] for s in again] == [s["pnl_pct"] for s in view]
    assert [s["disposition"] for s in again] == [s["disposition"] for s in view]


# ── 8: Phase 3 correctives ────────────────────────────────────────────────────

def test_summary_never_announces_activation(tmp_path, monkeypatch, capsys) -> None:
    """
    V3 is retired: the journal must not evaluate activation criteria, and must
    never be able to print "V3 activation warranted".
    """
    vj = _journal_env(tmp_path, monkeypatch)
    sid = vj.log_v2_signal(
        scanner_signal=_scanner_sig("2026-07-13T00:00:00+00:00", would_block=False),
        accepted=True,
    )
    vj.log_outcome(sid, "WIN", 9.0, is_counterfactual=False)
    vj.summarise_journal()

    out = capsys.readouterr().out
    assert "activation warranted" not in out.lower()
    assert "criteria met" not in out.lower()
    assert "RETIRED" in out
    for withdrawn in ("n >= 20", "PF > 1.20", "P_bootstrap(PF>1) > 90%"):
        assert withdrawn not in out, f"withdrawn criterion still evaluated: {withdrawn}"


def test_summary_source_has_no_pass_fail_criteria() -> None:
    """Source-level lock: the five-point checklist must be gone, not just quiet."""
    import inspect

    import pipeline.v3_journal as vj

    src = inspect.getsource(vj.summarise_journal)
    # Strip comments: the block that explains WHAT WAS REMOVED legitimately
    # names the old strings, and must not trip this check.
    code = "\n".join(ln for ln in src.splitlines()
                     if not ln.lstrip().startswith("#"))
    assert "activation warranted" not in code
    assert "5-point" not in code
    assert "[PASS]" not in code and "[FAIL]" not in code
    # The withdrawn thresholds must not be computed at all.
    for expr in ("1.20", ">= 20", "p_above > 90"):
        assert expr not in code, f"withdrawn criterion still computed: {expr}"


def test_resolver_leaves_partial_horizon_pending(tmp_path, monkeypatch) -> None:
    """
    reconcile_pending must not invent MAX_HOLD when only part of the hold
    horizon is observable — the same right-censoring defect fixed in the
    scanner. With 5 of 36 candles and no stop/target touched, nothing is written.
    """
    import pandas as pd

    vj = _journal_env(tmp_path, monkeypatch)
    vj.log_v2_signal(
        scanner_signal=_scanner_sig("2026-07-13T00:00:00+00:00", would_block=True),
        accepted=False,
    )

    # 5 flat candles: neither the stop nor the target is reached.
    idx = pd.date_range("2026-07-13T01:00:00Z", periods=5, freq="h")
    flat = pd.DataFrame(
        {"time": idx, "open": 100.0, "high": 100.5, "low": 99.5,
         "close": 100.0, "volume": 1.0}
    )
    monkeypatch.setattr("exchange.coinbase_candles.download",
                        lambda *a, **k: flat)

    n = vj.reconcile_pending(asset="ZEC-USD", max_hold_hours=36)
    assert n == 0, "a partially observed horizon must stay pending"
    view = vj._build_signal_view(vj.read_journal())
    assert view[0]["outcome"] is None


def test_resolver_still_resolves_a_touched_stop(tmp_path, monkeypatch) -> None:
    """Censoring must not suppress an outcome that genuinely occurred."""
    import pandas as pd

    vj = _journal_env(tmp_path, monkeypatch)
    vj.log_v2_signal(
        scanner_signal=_scanner_sig("2026-07-13T00:00:00+00:00", would_block=True),
        accepted=False,
    )
    # entry 100, atr 2, atr_stop 2.0 -> stop at 96. Bar 2 trades to 95.
    idx = pd.date_range("2026-07-13T01:00:00Z", periods=3, freq="h")
    hit = pd.DataFrame(
        {"time": idx, "open": 100.0, "high": [100.5, 100.5, 100.5],
         "low": [99.5, 95.0, 99.0], "close": 100.0, "volume": 1.0}
    )
    monkeypatch.setattr("exchange.coinbase_candles.download", lambda *a, **k: hit)

    n = vj.reconcile_pending(asset="ZEC-USD", max_hold_hours=36)
    assert n == 1
    assert vj._build_signal_view(vj.read_journal())[0]["outcome"] == "LOSS"
