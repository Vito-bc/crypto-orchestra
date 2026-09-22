"""
The vault generator's parsers are run against the COMMITTED source documents.

Why this file exists: `generate_research_notes()` derives every figure on the
vault's Research page by regex from `CLAUDE.md`, `docs/trial_registry.md`,
`docs/research/2026-09-cost-sensitivity.md`, `pipeline/fees.py` and its fee
evidence file, and `_require()` raises rather than emit a blank or a stale
number. That guard works — but nothing was exercising it in CI, so the first
notice of a broken parser was the nightly Obsidian task failing at 23:00. That
is exactly what happened when PR #26 rewrote CLAUDE.md's "Validation Status"
section: the generator raised on the V2 ZEC headline every night from
2026-09-20 until it was fixed.

These tests read the real files, not fixtures. A fixture would freeze the very
wording this is meant to track, so a document rewrite has to fail HERE, in a
pull request, instead of on the scheduler.
"""
from __future__ import annotations

import re
import shutil
from pathlib import Path

import pytest

import backtesting.generate_journal as gj


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def vault(tmp_path, monkeypatch):
    """Point the generator at a throwaway vault; never the committed one."""
    destination = tmp_path / "vault"
    monkeypatch.setattr(gj, "VAULT", destination)
    return destination


# ── The parser that broke ────────────────────────────────────────────────────

def test_the_v2_headline_parses_out_of_the_committed_claude_md() -> None:
    """
    The exact failure from 2026-09-20: the bullet is now

        - **V2 momentum (ZEC): RETIRED AS AN ACTIVATION CANDIDATE (...),
          on EDGE.** PF 0.761 (-0.62%/trade, n=114) on the continuous ...

    with the status in bold and the numbers after it. If this assertion fails,
    CLAUDE.md's Validation Status wording moved again and the generator's regex
    must move with it — the vault must not publish the previous numbers.
    """
    claude_md = ROOT / "CLAUDE.md"
    pattern = (r"\*\*V2 momentum \(ZEC\): ([^*]+?)\*\*\s*"
               r"PF ([\d.]+) \((-?[\d.]+)%/trade, n=(\d+)\)")

    m = re.search(pattern, claude_md.read_text(encoding="utf-8"), re.DOTALL)

    assert m is not None, (
        "CLAUDE.md no longer matches generate_journal.py's V2 headline regex — "
        "fix the parser, do not loosen this test")
    status = " ".join(m.group(1).split())
    assert "RETIRED" in status
    assert (m.group(2), m.group(3), m.group(4)) == ("0.761", "-0.62", "114")


def test_the_generator_and_this_test_use_the_same_v2_pattern() -> None:
    """A test with its own private copy of the regex proves nothing about CI."""
    source = gj.__file__
    text = Path(source).read_text(encoding="utf-8")
    assert r'r"\*\*V2 momentum \(ZEC\): ([^*]+?)\*\*\s*"' in text
    assert r'r"PF ([\d.]+) \((-?[\d.]+)%/trade, n=(\d+)\)"' in text


# ── The whole Research page, against every committed source it reads ─────────

def test_research_page_generates_from_the_committed_documents(vault) -> None:
    """
    Exercises every `_require()` in `generate_research_notes()` at once: the
    CLAUDE.md headline and DRY_RUN refusal, the registry's LIVE NO-GO verdict
    and candidate cohort window, the fee evidence file's agreement with
    `pipeline/fees.py` `CURRENT_SCHEDULE`, and the cost-sensitivity write-up's
    break-even, re-priced PF/expectancy and 95% upper-bound rows.

    Any of those documents being rewritten fails this test in CI rather than
    at 23:00 on the Windows scheduler.
    """
    gj.generate_research_notes()

    page = (vault / "Research" / "Research Status.md").read_text(encoding="utf-8")

    assert "PF 0.761 (-0.62%/trade, n=114)" in page
    assert "RETIRED" in page
    assert "LIVE NO-GO" in page
    # A parsed-but-empty field is the failure mode `_require` exists to stop;
    # check the rendered page for the blanks a silent miss would leave.
    assert "**PF  (" not in page
    assert "| | |" not in page.replace("| | Maker | Taker | Tier |", "")


def test_research_page_refuses_a_claude_md_whose_headline_moved(
    vault, tmp_path, monkeypatch
) -> None:
    """
    The guard must still fail loudly, not fall back to a literal. Rewrite the
    V2 bullet the way PR #26 did — the generator has to raise rather than
    publish the last numbers it knew.

    The whole set of documents is mirrored into a scratch root so the real
    repository is never written to, and so the mirror keeps whatever files the
    generator reads today without this test having to list them.
    """
    mirror = tmp_path / "root"
    (mirror / "backtesting").mkdir(parents=True)
    shutil.copytree(ROOT / "docs", mirror / "docs")
    shutil.copy2(ROOT / "backtesting" / "cost_sensitivity.py",
                 mirror / "backtesting" / "cost_sensitivity.py")
    (mirror / "CLAUDE.md").write_text(
        (ROOT / "CLAUDE.md").read_text(encoding="utf-8").replace(
            "**V2 momentum (ZEC):", "**V2 momentum on ZEC-USD:"),
        encoding="utf-8")

    monkeypatch.setattr(gj, "ROOT", mirror)

    with pytest.raises(RuntimeError, match="the V2 ZEC headline"):
        gj.generate_research_notes()
