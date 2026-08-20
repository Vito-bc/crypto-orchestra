## What and why

<!-- What changes, and the reasoning behind it. The "why" is the interesting half. -->

## Type

- [ ] Bug fix
- [ ] Safety / risk-gate change
- [ ] Research change (produces or alters a number)
- [ ] Docs / tooling
- [ ] Refactor with no behaviour change

## Checks

- [ ] `ruff check .` clean
- [ ] `python -m pytest -q` passes
- [ ] `python backtesting/research_runner.py --verify-code` passes

## Safety

- [ ] `DRY_RUN` default unchanged (`true`)
- [ ] `LIVE_BALANCE_USD` default unchanged, or the increase is stated and justified above
- [ ] No new path can place, size or cancel a real order
- [ ] Every gate touched still fails **closed** on unreadable or unavailable input

## Research integrity

<!-- Delete this section if the PR touches no number and no manifest-listed file. -->

- [ ] Trial ID pre-registered in `docs/trial_registry.md` **before** the scan was run
- [ ] Acceptance rule was fixed in advance and is unchanged
- [ ] If a manifest-listed file changed, artifacts are regenerated in this PR and `--verify` passes
- [ ] If a published number changed, the superseded artifact is preserved and the change is described below

**Numbers changed by this PR:**

<!-- e.g. "ZEC continuous PF 0.855 → 0.761; supersedes artifacts/superseded/…" — or "none". -->
