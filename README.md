# Aurora Nutrition — Subscription Health proof artifact

**What it proves:** Aurora's governance rules are executable code that runs on every change and blocks a bad change with exit code 1, whether a person or an AI agent wrote it.

This is part 3 of the Subscription Health capstone package. Part 1 is the ADR set and compliance matrix, and part 2 is the data product specification. Those two are separate documents. All data here is synthetic; see [LINEAGE.md](LINEAGE.md) for provenance, the full data flow and known limitations.

## Requirements

- Python 3.10 or newer, standard library only. Nothing to install.

## How to run

**macOS / Linux**
```bash
python3 generate_data.py                 # synthetic sources -> raw/
python3 pipeline.py                      # raw/ -> products/
python3 policies.py; echo "exit code: $?"          # expect PASS, exit 0

python3 pipeline.py --seed-violation     # plant one violation per policy
python3 policies.py; echo "exit code: $?"          # expect 4 FAILs, exit 1

python3 pipeline.py                      # restore a clean build
```

**Windows PowerShell**
```powershell
python generate_data.py
python pipeline.py
python policies.py; echo "exit code: $LASTEXITCODE"   # expect PASS, exit 0

python pipeline.py --seed-violation
python policies.py; echo "exit code: $LASTEXITCODE"   # expect 4 FAILs, exit 1

python pipeline.py
```

`generate_data.py` takes `--customers` (default 2000) and `--seed` (default 42). The results below use the defaults.

`--seed-violation` with no value plants all four violations. Give it a rule to plant only that one, e.g. `python pipeline.py --seed-violation POL-002`.

## Video demo

```bash
python demo.py          # Windows; use python3 on macOS / Linux
```

An interactive walkthrough in 14 stages. Press Enter to go to each one. It builds clean data, then for each of POL-001 to POL-004 it plants that rule's violation (only that rule fails) and rebuilds clean (the rule passes again). After that it runs POL-005 on a blocked agent PR and on the corrected PR, for both the Red and the Yellow zone, and ends with a summary read from the real exit codes. It leaves `products/` in a clean, passing state.

Options: `--no-clear` keeps earlier stages on screen, `--no-pause` runs straight through for rehearsal, and `--no-color` gives plain text.

## File guide

| Path | What it is |
|---|---|
| `generate_data.py` | Creates five synthetic source files in `raw/` (subscribers, subscription events, billing, consent, transcripts) plus `raw/_manifest.json`. |
| `pipeline.py` | Builds `products/churn_features.csv`, `products/rag_chunks.jsonl` and `products/lineage.json`. `--seed-violation [RULE]` plants all four demo violations, or just one. |
| `demo.py` | Interactive, press-Enter walkthrough for the video: each policy fails on its seeded violation, then passes once it is removed. |
| `governance.py` | Consent lookup, redaction and pseudonymization: the privacy-critical code the pipeline imports (ADR-004 Red zone). |
| `policies.py` | The policy-as-code validator: POL-001 to POL-004 on the data, POL-005 on a pull request. Never imports `pipeline.py` or `governance.py`. Writes `products/policy_report.json`. |
| `config.py` | Shared settings: as-of date, 730-day retention window, 90-day label window, pseudonymization salt. |
| `contracts/churn_features.json` | Interface contract v1.0.0 for the ML team's feature table: 11 columns, types, primary key, freshness and breaking-change policy. |
| `tests/fixtures/pr_agent_*.json` | Sample agent pull requests for POL-005: green, red, yellow (1 approval) and yellow_approved (2 approvals). |
| `LINEAGE.md` | Data-flow diagram, provenance and known limitations. |
| `CLAUDE.md` | Build rules followed by the AI coding assistant. |
| `raw/`, `products/` | Generated output, git-ignored. Rebuild with the commands above. |

## The four seeded violations

`python pipeline.py --seed-violation` runs the normal build, then plants exactly one violation per rule. `policies.py` is not told about them.

| Rule | What is planted | Real-world failure it imitates |
|---|---|---|
| POL-001 | A fake `example.com` email appended to one chunk's text | A redaction regression |
| POL-002 | A feature row for a customer who withdrew `analytics_profiling` consent | A join that drops the consent filter |
| POL-003 | A chunk dated 900 days before the as-of date | A backfill that ignores retention |
| POL-004 | An extra `email` column in `churn_features.csv` | A schema change nobody announced |

The run is recorded in `products/lineage.json` as `"run_mode": "seeded_violations"`, with each planted item listed.

## POL-005: agent pull-request zones

```bash
python3 policies.py --pr tests/fixtures/pr_agent_green.json    # docs only       -> PASS, exit 0
python3 policies.py --pr tests/fixtures/pr_agent_red.json      # Red-zone files  -> FAIL, exit 1
python3 policies.py --pr tests/fixtures/pr_agent_yellow.json   # 1 of 2 approvals -> FAIL, exit 1
python3 policies.py --pr tests/fixtures/pr_agent_yellow_approved.json   # 2 approvals -> PASS, exit 0
```
(On Windows use `python` instead of `python3`.)

Red zone: `governance.py`, `policies.py`, `config.py`, `contracts/`, `.github/`. A PR labeled `yellow` needs at least 2 distinct human approvals. Without `--pr`, POL-005 is skipped.

## Results

These are the actual outputs from running every command above with the defaults on Python 3.14.7. All runs are deterministic, so you should see the same numbers.

**`generate_data.py`**

| File | Rows |
|---|---|
| subscribers.csv | 2,000 |
| subscription_events.csv | 5,045 |
| billing.csv | 19,336 |
| consent.csv | 4,000 |
| transcripts.jsonl | 1,567 |

Measured monthly cancel rate: **5.05%**. Active subscribers at 2026-10-01: **993**.

**`pipeline.py` (clean build)**

| Output | Rows | Excluded |
|---|---|---|
| churn_features.csv | 924 | 997 not scoreable, 79 consent |
| rag_chunks.jsonl | 2,526 | transcripts: 234 consent, 70 retention |

90-day churn label rate in `churn_features`: **14.9%**.

**`policies.py` (clean build):** `RESULT: PASS (4/4 rules passed)`, exit code **0**. POL-005 is skipped.

**`pipeline.py --seed-violation`:** 925 feature rows and 2,527 chunks, one planted row in each. Then `policies.py`:
```
FAIL  POL-001  No raw PII in vector-layer chunks  (1 violations)
        - chunk T0000083-00: raw email detected ('@example.com')
FAIL  POL-002  Withdrawn analytics consent excluded from features  (1 violations)
        - customer_key e36edd34f8d052ecb587: analytics_profiling is 'withdrawn' but row present
FAIL  POL-003  Transcript chunks within 730-day retention window  (1 violations)
        - chunk BACKFILL-00: 2024-04-14 older than 2024-10-01
FAIL  POL-004  churn_features honors its contract  (2 violations)
        - unexpected column(s) ['email'] not in contract
        - forbidden identifier column 'email' present
SKIP  POL-005  Agent PRs cannot touch Red-zone paths  (no --pr file given)

RESULT: FAIL  (0/4 rules passed; failed: POL-001, POL-002, POL-003, POL-004)
```
Exit code **1**. **4 of 4** seeded violations were caught. POL-004 reports two findings for its one planted column.

**POL-005 fixtures (on a clean build)**

| Fixture | POL-005 | Overall | Exit code |
|---|---|---|---|
| pr_agent_green.json | PASS | PASS (5/5) | 0 |
| pr_agent_red.json | FAIL: `governance.py`, `policies.py` | FAIL (4/5) | 1 |
| pr_agent_yellow.json | FAIL: 1 human approval, needs 2 | FAIL (4/5) | 1 |
| pr_agent_yellow_approved.json | PASS | PASS (5/5) | 0 |

**`demo.py --no-pause`:** all 13 outcomes were as expected. The summary reads `6 of 6 seeded violations blocked; every one passed again once removed.` (4 data rules plus 2 POL-005 PRs), with exit code 0.

## AI-assisted build disclosure

The code and documentation in this repository were written with Claude Code (Anthropic), working step by step from the author's prompts. The author directed each step, approved the plans and design decisions, and reviewed the results. Every number in **Results** comes from actually running the commands, not from estimates. The self-review that produced the limitations in `LINEAGE.md` was also AI-assisted.
