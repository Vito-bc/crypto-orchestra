# Candidate-stream sizing for a log-only agent shadow — 2026-09-20

READ-ONLY, BLIND census. No P&L, profit factor, expectancy, or ranking is computed
anywhere in this document or its source script
(`backtesting/candidate_stream_census.py`) — grep it. `_simulate_trade` is never
called or imported. No price after a signal bar's own close is read anywhere in
this task. **The frozen trading gate (`signal_scanner._detect_breakout_signal`,
`ASSET_CONFIG`) is unmodified** — this document calls it, it does not touch it.
This is not a trial registration and selects nothing.

## 1. What this answers

If a log-only LLM agent shadow ran alongside the frozen gate — logging a vote
without ever placing an order — how many bars would it fire on, and roughly
what would it cost per month at current Anthropic list prices? Two independent
questions, both arithmetic:

1. **Volume** — a blind bar-level census of `signal_scanner`'s own pipeline,
   over the frozen continuous research window, per asset per year.
2. **Cost** — call count (from part 1) times a per-call token estimate built
   from one real recorded pipeline cycle, times current list prices.

Part 3 is a one-page design note (no code) for how such a shadow would fit
next to the frozen gate without ever feeding it.

## 2. Method — signal-stage census

**Source:** `backtesting/candidate_stream_census.py`, calling
`signal_scanner.build_merged_frame` and `signal_scanner._detect_breakout_signal`
unmodified. **Window:** `research_runner.RESEARCH_CONFIG["continuous_window"]`,
2021-03-01 → 2026-07-12 inclusive — the same window `docs/trial_registry.md` and
`CLAUDE.md` quote PF/n against. Each asset's own `ASSET_CONFIG` entry is used
(own `atr_stop`/`atr_target`/`min_conditions`/`vol_spike_ratio`/
`daily_ema_period`/`btc_regime_filter`) — `ASSET_CONFIG[asset]["enabled"]` is
read only by `scan_latest` and `main()`, never by `build_merged_frame` or
`_detect_breakout_signal`, so running BTC/ETH/SOL "as if enabled" needed no
override and no `ASSET_CONFIG` edit. **Network:** none — the local parquet
cache under `data/candles/` was hydrated beforehand from the committed
manifest via `backtesting/hydrate_research_data.py` (public Coinbase endpoint,
no credentials), and `STRICT_COINBASE_ONLY` is set for the run so a cache miss
raises instead of silently reaching the network.

Every bar in the window is evaluated independently — there is no
`skip_until` / no-re-entry suppression, because that belongs to trade
simulation. This is what makes it a *census of bars*, not a *count of trades*
(see §4).

### Five stages

| # | Stage | Meaning |
|---|---|---|
| 1 | **trigger** | `_detect_breakout_signal` returns non-`None`: an EMA50 cross from below within `_MAX_CANDLES_SINCE` (4) candles. |
| 2 | **wide** | passed every gate this asset's config *declares* (vol, 4h trend, daily trend, BTC regime where applicable) and reached the scored conditions. This is the WIDE candidate stream an agent shadow would see. |
| 3 | **n_met>=3** | stage-2 bars where ≥3 of the 5 scored conditions hold. |
| 4 | **n_met>=4** | stage-2 bars where ≥4 hold — every `ASSET_CONFIG` entry declares `min_conditions=4`, so this is exactly today's live BUY gate. |
| 5 | **unavailable** | blocked `"gate_inputs_unavailable"` — a declared gate's input was not yet computable (indicator warm-up). Reported separately, never netted against stages 1–4. |

`hard_gate_blocked` (vol/4h-trend/daily-trend/BTC-regime rejections) is the
remainder: `trigger = unavailable + hard_gate_blocked + wide`, verified to
hold exactly on every one of the 24 asset-year rows below.

### Results — per asset, per year

Full table with rates and the n_met histogram:
[`docs/research/data/2026-09-20-candidate-stream.csv`](data/2026-09-20-candidate-stream.csv)
(24 asset-year rows, 185,355 bars censused). 2021 and 2026 are partial
calendar years (window opens 2021-03-01, closes 2026-07-12).

| Asset | Years | Bars | Trigger | Unavailable | Hard-gate blocked | Wide (stage 2) | n_met≥3 | n_met≥4 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| BTC-USD | 2021–2026 | 47,004 | 5,311 | 0 | 4,305 | 1,006 | 797 | 425 |
| ETH-USD | 2021–2026 | 47,004 | 5,346 | 0 | 4,507 | 839 | 619 | 304 |
| SOL-USD | 2021–2026 | 44,345 | 4,981 | 534 | 3,882 | 565 | 447 | 212 |
| ZEC-USD | 2021–2026 | 47,002 | 5,644 | 327 | 4,736 | 581 | 478 | 231 |

`unavailable` concentrates exactly where each asset's registered
`effective_start` (`research_runner.RESEARCH_CONFIG["asset_effective_start"]`)
says it should and nowhere else: BTC/ETH register 2020-07-20, before the
window opens, so both show `unavailable=0` in every year. ZEC registers
2021-06-26 (200-day daily EMA warm-up) — all 327 unavailable bars fall in
2021, before that date. SOL registers 2022-01-03 (also a 200-day EMA, listed
2021-06-17) — 526 of its 534 unavailable bars are 2021, the rest the first
days of 2022. This is not an assumption; it fell out of the census, and it is
the same mechanism trial `2026-08-warmup-semantics.v1` fixed (see §4).

One notable per-year finding: **SOL-USD had zero wide-stage candidates in
2026 year-to-date** (531 triggers, all hard-gate blocked, 0 reaching scored
conditions) — a real observation, not a script artifact; it recurs across the
n_met histogram (§3) as a general widening at the hard-gate stage.

### 3. n_met distribution among stage-2 (wide) candidates

Summed over the full window (per-year breakdown is in the CSV):

| Asset | n_met=0 | n_met=1 | n_met=2 | n_met=3 | n_met=4 | n_met=5 | wide total |
|---|---:|---:|---:|---:|---:|---:|---:|
| BTC-USD | 0 | 2 | 207 | 372 | 346 | 79 | 1,006 |
| ETH-USD | 0 | 2 | 218 | 315 | 257 | 47 | 839 |
| SOL-USD | 0 | 1 | 117 | 235 | 170 | 42 | 565 |
| ZEC-USD | 0 | 5 | 98 | 247 | 199 | 32 | 581 |

For all four assets the mode sits at n_met=3, one condition short of today's
live gate — the wide stream is not a thin sliver above the live gate, it is a
comparably sized population sitting just below it.

### 4. Why stage-4 is an upper bound on a registered artifact's `n_signals`, not a reproduction

`docs/research/artifacts/results.json`, trial `2026-08-warmup-semantics.v1`,
row `V2-continuous` / `ZEC-USD`, records `n_signals=114` (`n_closed=114`,
`n_gate_unavailable=0`) on this exact window, requested from 2021-03-01 and
clipped to `effective_start=2021-06-26`. That number is a **trade** count, not
a bar count: `signal_scanner.scan_asset` additionally applies `skip_until` (no
re-entry while a previously accepted signal's simulated position would still
be open — position duration comes from `_simulate_trade`, which reads price
after the bar) and the whipsaw guard (2+ stops in 96h, also determined by
simulated outcomes). Both consume price paths this census is constitutionally
blind to, by the task's own constraint.

This census's ZEC stage-4 count from `effective_start` (2021-06-26) onward is
**231** bars — a real number the script prints and the CSV confirms — against
the registered `n_signals=114`. The ratio (about 2x) is consistent with a
36-hour max-hold plus stop/whipsaw suppression removing a bit over half of raw
n_met≥4 crossings; decomposing that gap further would require simulating
trades, which this task explicitly forbids. **Only ZEC has an existing
own-mechanism continuous-window artifact to compare against.** BTC, ETH and
SOL are scanned here with their own (not frozen-ZEC-transfer) config for the
first time over this window — their stage-4 counts (425, 304, 212) are new
numbers with no prior artifact to reconcile against, not reproductions of
anything.

## 5. API budget — arithmetic only, no Anthropic calls made

**Pricing** (Anthropic Claude Platform docs, fetched 2026-09-20):
[platform.claude.com/docs/en/about-claude/pricing](https://platform.claude.com/docs/en/about-claude/pricing)

| Model | Input | Output |
|---|---:|---:|
| Claude Haiku 4.5 | $1 / MTok | $5 / MTok |
| Claude Sonnet 4.6 | $3 / MTok | $15 / MTok |

**Token approximation**, same page's FAQ: *"1 token is approximately 4
characters or 0.75 words in English."* Used throughout as `tokens ≈ chars/4`
— an approximation, not a tokenizer run (no `tiktoken` in this environment,
and the Anthropic token-counting endpoint is itself an API call, excluded by
the task's "no Anthropic API calls" constraint).

**Correction to the task's framing:** the codebase runs **6** Claude-calling
sub-agents per asset per cycle (`sentiment`, `whale`, `news`, `technical`,
`macro`, `risk` — each with `self._ask_claude_json` in `agents/base_agent.py`),
not 7. `agents/breakout_agent.py` is the 7th agent CLAUDE.md counts, and it is
explicitly documented there as "Fully deterministic (no LLM)" — confirmed by
grep, it never calls `_ask_claude`. The arithmetic below uses the real **6
haiku + 1 sonnet = 7 API calls per event**; the task's literal "7 haiku + 1
sonnet = 8 calls" framing is given alongside as an upper-bound alternative.

### Per-call size, from one recorded live invocation

Source: `logs/agent_decisions.jsonl`, first record — a real ETH-USD cycle
(2026-04-15T16:31:47Z, action HOLD). Prompts are reconstructed from the exact
system-prompt strings in `agents/macro_agent.py` / `agents/orchestrator.py`
plus this record's own logged values (`btc_dominance=57.14`,
`price_change_24h=-0.31`, `price_change_7d=6.99`, `trend_4h=bull`); fields the
output doesn't echo (raw EMA levels, DXY) are filled with plausible values
consistent with that cycle (ETH ≈$2,348, back-solved from the risk agent's
logged `atr=17.14` / `atr_pct=0.73%`) — flagged here as filled, not measured.
Completions are the record's own real logged JSON payloads.

| Call | Representative source | Prompt chars | Completion chars | Prompt tokens (÷4) | Completion tokens (÷4) |
|---|---|---:|---:|---:|---:|
| Haiku sub-agent (macro, representative) | `agents/macro_agent.py` system + filled user prompt; real logged completion | 2,875 | 355 | 719 | 89 |
| Sonnet orchestrator | `agents/orchestrator.py` system + user prompt built from this record's 5 real agent reports, extrapolated to 6 (+538 chars, one stand-in report) | 6,712 | 1,301 | 1,678 | 325 |

The other 4 real reasoning strings logged in this same cycle range
165–322 characters (whale 278, sentiment 322, risk 165, technical 277),
against the macro reasoning used above (243) — so the haiku completion
estimate sits inside the observed real spread, not at an extreme.

### $/call and $/event

- Haiku call: 719×$1/MTok + 89×$5/MTok = **$0.00116**
- Sonnet call: 1,678×$3/MTok + 325×$15/MTok = **$0.00991**
- **Event cost (6 haiku + 1 sonnet, actual callers): $0.01689**
- Event cost (7 haiku + 1 sonnet, task's literal framing): $0.01805

### $/month, at the census's mature-year candidate rates

"Mature" = each asset's own full calendar years at or after its registered
`effective_start` (BTC/ETH: 2021–2025; ZEC: 2022–2025; SOL: 2023–2025) — the
partial-2026 and pre-warm-up years are excluded from the *rate* so a startup
gap doesn't understate a steady-state monthly budget. Full-window figures
(including the slower early years) are lower; both are in the CSV.

| Rate driving the call | ZEC-only | All 4 assets |
|---|---:|---:|
| Wide (stage 2) candidates/month | 9.13 | 54.47 |
| n_met≥3 (stage 3) candidates/month | 7.56 | 42.70 |

| | ZEC-only, stage 2 | ZEC-only, stage 3 | All-4, stage 2 | All-4, stage 3 |
|---|---:|---:|---:|---:|
| **6h+1s (actual callers)** | **$0.154/mo** | **$0.128/mo** | **$0.920/mo** | **$0.721/mo** |
| 7h+1s (task's literal framing) | $0.165/mo | $0.137/mo | $0.983/mo | $0.771/mo |

For scale, the same 6h+1s call running on today's fixed hourly cadence
(regardless of whether anything fired) would cost ≈$12.34/asset/month,
≈$49.35/month for all four — the event-triggered stage-2 shadow is roughly
98–99% cheaper than an always-on hourly cadence, purely because the candidate
stream is a small fraction of all bars (§2). This directly informs Project
State item 7 in `CLAUDE.md` ("run LLM agents on scanner events rather than
hourly").

**Caveats, stated plainly:** these are order-of-magnitude budget numbers, not
a metered bill. The token approximation is Anthropic's own documented
rule of thumb, not a tokenizer run; the per-call size comes from one
reconstructed cycle, not a sampled distribution; and real prompts will vary
with market conditions (longer news digests, more onchain metrics text, etc.).
At these dollar levels the conclusion is robust to that noise: a log-only
shadow gated on the wide or n_met≥3 stream costs cents to about a dollar a
month, not a line item anyone needs to budget around.

## 6. Design note — a log-only agent shadow beside the frozen gate

**One sentence:** the shadow watches the wide candidate stream, records what
each agent would have said, and is read by nobody's trading decision until a
pre-registered trial says otherwise.

**Where the stream is produced.** `signal_scanner._detect_breakout_signal`
already computes it as a byproduct of every bar it evaluates — stage-2 "wide"
above. A shadow runner needs no new signal logic: it calls the same function
the frozen gate calls, on the same merged frame, and reacts to `blocked in
{"conditions", None}` (i.e. stage-2) or tightens to `n_met>=3` (stage-3) if
volume needs to be lower. It never calls `_simulate_trade` and never touches
`ASSET_CONFIG`, `pipeline/runner.py`, or `pipeline/limit_orders.py` — it is a
new, separate consumer of an existing read path, not a modification of the
frozen mechanism.

**What is logged per vote.** One append-only record per (candidate bar ×
agent), never overwritten:

| Field | Why |
|---|---|
| `agent` | which of the 6 (7 with breakout, still deterministic) voted |
| `direction` | BUY / SELL / NEUTRAL, as returned |
| `confidence` | as returned, unmodified by any downstream weighting |
| `reasoning_digest` | a hash or fixed-length truncation of the free-text reasoning — full text goes to a separate blob store if kept at all, so the structured log stays small and diffable |
| `snapshot_hash` | hash of every input the agent actually read (price snapshot, onchain metrics, DXY, etc.) — makes the vote replayable and makes "the input changed" distinguishable from "the model changed" |
| `candle_time` | the bar's own timestamp — never a wall-clock log time, so late or retried runs don't misalign with the price series |
| `variant_id` | which declared prompt/model/threshold configuration produced this vote (below) |

**How a variant is declared and versioned.** A variant is a frozen tuple —
model ID, system prompt text (hashed), any decision thresholds — assigned an
ID *before* it runs, the same discipline `docs/trial_registry.md` already
applies to strategy mechanisms. Editing a live variant's prompt without a new
ID is exactly the failure mode this project's registry exists to prevent
(compare: ZEC's V2 mechanism is git-tagged `v2-adx25-frozen` precisely so a
later edit can't be silently misattributed to the frozen result). A variant
change — even a one-word prompt edit — mints a new `variant_id`; old and new
run side by side in the log, never blended.

**How the subsequent price path is attached later.** Never at vote time. A
separate, batched job reads `candle_time` + `variant_id` rows after enough
bars have elapsed, looks up the realized forward return over one or more
fixed, pre-declared horizons (e.g. the frozen mechanism's own hold window, for
comparability), and writes it to a joined analysis table — never back into the
vote log itself, which stays append-only and immutable. This mirrors
`_simulate_trade`'s own `resolved: True/False` discipline: a horizon that
hasn't elapsed yet is absent from the analysis table, not backfilled with a
mark-to-market guess.

**The reading rule.** A stream is read as a real result only when **both**:
(a) its forward one-sided 95% lower confidence bound on mean return clears
zero, **and** (b) its Sharpe ratio exceeds `1.645/sqrt(years)` — the same
minimum-detectable-edge arithmetic this project already uses elsewhere
(`docs/research/data/universe_inventory_2026-09-17.md`'s decidable-edge floor,
`1.645 × SD / sqrt(n)`). Anything short of both bounds is observation, full
stop — reported, kept, never spent. **No agent, variant, or threshold is
selected by its own outcome and fed to the trading gate without a
pre-registered trial ID in `docs/trial_registry.md` first** — the same rule
that retired V3 ER-30 for activation (`docs/trial_registry.md`,
`2026-08-warmup-semantics.v1`) applies here before this shadow's output could
ever touch `pipeline/runner.py`.

## 7. Validation

```
venv\Scripts\python.exe backtesting/hydrate_research_data.py         # OK: 8 datasets match manifest
venv\Scripts\python.exe backtesting/candidate_stream_census.py       # 24 asset-year rows, 185,355 bars
venv\Scripts\python.exe -m pytest -q                                 # 1301 passed, 2 skipped, 11 deselected
venv\Scripts\python.exe -m ruff check backtesting/candidate_stream_census.py   # All checks passed!
venv\Scripts\python.exe backtesting/research_runner.py --verify-code # OK: code hashes and environment match the artifact
```

No file under `pipeline/`, `exchange/`, `signal_scanner.py`, `ASSET_CONFIG`,
or `docs/research/artifacts/` was changed. New files: this document,
`backtesting/candidate_stream_census.py`,
`docs/research/data/2026-09-20-candidate-stream.csv`.
