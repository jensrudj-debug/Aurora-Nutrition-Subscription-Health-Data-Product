# Lineage — Subscription Health proof artifact

How every row in `products/` was produced, from which source, through which controls, and what decides whether it is published.

## 1. Data flow

```
 generate_data.py  (seed 42, as-of 2026-10-01)
        │
        ▼
 raw/  ─────────────────────────────────────────────────────────────────────────────
  subscribers.csv  subscription_events.csv  billing.csv  consent.csv  transcripts.jsonl
        │                  │                    │             │              │
        │                  │                    │             │              │
        │   ┌──────────────┴────────────────────┴──┐          │              │
        │   │                                      │          │              │
        ▼   ▼                                      │          │              │
 ┌───────────────────────── pipeline.py : churn_features ───────────────────────────┐
 │ 1. Scoreable filter    drop customers who signed up on/after 2026-07-03, or      │
 │                        cancelled before it                                       │
 │ 2. Consent filter      keep only latest analytics_profiling = granted   ◄────────┼── consent.csv
 │                        (governance.consent_lookup / has_consent)                 │
 │ 3. Point-in-time cut   observation_date = as-of − 90 days = 2026-07-03.          │
 │                        Features use only events, billing and transcripts         │
 │                        dated BEFORE it. failed_payments_90d = 2026-04-04..07-02  │
 │ 4. Label               label_churned_next_90d = cancel in [2026-07-03,2026-10-01)│
 │ 5. Pseudonymization    customer_id → customer_key = SHA-256(salt+id)[:20]        │
 │                        (governance.pseudonymize)                                 │
 │ 6. Contract projection columns taken only from contracts/churn_features.json     │
 └──────────────────────────────────────────┬───────────────────────────────────────┘
                                            ▼
                                 products/churn_features.csv
                                            │
 ┌────────────────────────── pipeline.py : rag_chunks ──────────────────────────────┐
 │ 1. Consent filter      keep only latest ai_support_assistant = granted ◄─────────┼── consent.csv
 │ 2. Retention filter    drop transcripts older than 730 days (before 2024-10-01)  │◄─ transcripts.jsonl
 │ 3. Redaction           customer email + regex emails → [EMAIL],                  │◄─ subscribers.csv
 │                        customer phone + regex phones → [PHONE],                  │   (PII to remove)
 │                        first and last name → [NAME]   (governance.redact)        │
 │ 4. Chunking            60-word windows, 15-word overlap                          │
 │ 5. Pseudonymization    customer_id → customer_key                                │
 └──────────────────────────────────────────┬───────────────────────────────────────┘
                                            ▼
                                 products/rag_chunks.jsonl
                                            │
              pipeline.py also writes products/lineage.json
              (row count + SHA-256 for every input and output,
               transforms applied, rows excluded, run_mode)
                                            │
                                            ▼
 ┌──────────────────────────────── policies.py ─────────────────────────────────────┐
 │ reads products/, raw/consent.csv, contracts/, config.py — never pipeline.py or   │
 │ governance.py, so a bug in the pipeline can't also hide in the check             │
 │                                                                                  │
 │  POL-001  no email / phone in chunk text      (independent, broader detector)    │
 │  POL-002  no feature row without analytics_profiling consent (keys recomputed)   │
 │  POL-003  no chunk older than 730 days                                           │
 │  POL-004  churn_features matches its contract (columns, types, nulls, PK)        │
 │  POL-005  agent PRs: no Red-zone paths; Yellow needs 2 approvals  (with --pr)    │
 │                                                                                  │
 │  writes products/policy_report.json                                              │
 └──────────────────────────────┬──────────────────────────┬────────────────────────┘
                                │                          │
                       all rules PASS               any rule FAIL
                         exit code 0                 exit code 1
                                │                          │
                                ▼                          ▼
                            PUBLISH                      BLOCK
               (CI merges / asset check lets      (merge or materialization
                consumers see the new data)        stops; consumers keep the
                                                   last good version)
```

**Who owns what:** `governance.py` holds the consent, redaction and pseudonymization code (ADR-004 Red zone). `config.py` holds the shared settings: as-of date, retention window, label window and salt. `contracts/churn_features.json` is the interface the ML team codes against.

## 2. Provenance

- **All data is synthetic.** Every record comes from `generate_data.py`. No real customer, transcript or payment data was used, and no public dataset was used.
- **The data is reproducible.** It uses fixed random seed `42` and a fixed as-of date of `2026-10-01`. The same seed produces byte-identical files in `raw/` and `products/`. Both folders are git-ignored and rebuilt from the scripts.
- **All PII is fake.** Emails use the reserved `example.com` domain (RFC 2606), and phone numbers use the fictional `555-0100`–`555-0199` range. Names come from small built-in lists.
- **There is no card data.** Billing carries only an opaque `payment_token` (`tok_` + 16 hex characters). No card numbers are generated anywhere.
- **The hidden churn risk is never written out.** Each synthetic customer has a churn-risk score from 0 to 1 that drives their failed payments, skips, pauses, support contacts and cancellation. The score is held in memory only and never written to any file.

## 3. Known limitations

### By design
- **The churn signal is planted.** The behaviour that predicts churn was put there by the generator's hidden risk score, so any model trained on this data will look better than it would on real customers.
- **Redaction is pattern-based.** It removes known values and regex matches. Real transcripts need a named-entity detector such as Microsoft Presidio to catch names, addresses and identifiers that patterns miss.
- **This is one run, not a deployment.** It does not demonstrate 15-minute micro-batch scheduling, Dagster, the Snowflake warehouse, embeddings or the vector index.
- **The salt is in `config.py`** so anyone can reproduce the exact keys. In production it lives in a secrets manager, because anyone holding the salt can re-link a `customer_key` to a customer.

### Rules that pass when they should fail
These gaps came out of a self-review. Each one was reproduced, and none has been fixed.
- **Assistant consent on chunks is unchecked.** No rule checks that `rag_chunks` honours `ai_support_assistant` consent. Only the pipeline filters it, so a chunk from a customer who withdrew passes all rules.
- **Name redaction is unchecked.** POL-001 checks emails and phones but not names, so a chunk with the customer's real name passes.
- **POL-001 scans only the `text` field.** A raw email in any other chunk field passes, and `rag_chunks` has no schema check.
- **POL-001 needs 10 digits for a phone.** A 7-digit number (`555-0147`) passes.
- **Empty outputs pass.** With zero feature rows and zero chunks, all rules pass.
- **POL-004 misses extra values in a CSV row.** A value beyond the header on one row isn't detected.
- **POL-004 doesn't read the contract's `pii` field.** It also doesn't check that `customer_key` looks like a key.
- **POL-003 accepts chunks dated after the as-of date.**
- **POL-005 lets an agent pick its own zone.** Yellow is decided by a label the PR author sets, so an unlabeled agent PR that changes `pipeline.py` passes with no approvals.
- **POL-005 exempts PRs that call themselves anything but `"agent"`.** An `author_type` of `"Agent"` or `"bot"` passes, and an agent listed in `human_approvals` is counted as a human approver.

### Operational
- **Bad data reaches `products/` before validation.** The pipeline writes there before `policies.py` runs, so blocking depends on CI or the orchestrator respecting the exit code. A production design writes to staging and promotes only on PASS.
- **The validator copies what it finds into its report.** A detected phone number appears in stdout and `products/policy_report.json`. Production should mask it.
- **Lineage hashes are recorded but never verified.** If someone edits `products/` after the pipeline runs, nothing detects it.
- **A seeded run's lineage contradicts itself in one line.** It still lists "names, emails, phones never output", although `run_mode: seeded_violations` and the `seeded_violations` list record what was planted.
- **`rows_excluded` for `rag_chunks` counts transcripts, not chunks.** It shows 234 for consent and 70 for retention. Together with the 1,263 transcripts kept, that reconciles to all 1,567.

### Data and modelling
- **The freshness SLA doesn't fit a training snapshot.** The contract says `as_of_date` is no older than 24 hours, but this table is a training snapshot, so `as_of_date` is 2026-07-03, 90 days old. A daily scoring table without the label would meet the SLA.
- **Consent is applied as it stands today, not on the observation date.** 24 of the 79 consent exclusions come from withdrawals after 2026-07-03. That's legally required, but in real data people who withdraw may churn more, which would bias the label rate. Here 2 of those 24 churned, 8% against 14.9% overall. The generator makes consent independent of risk, so this data can't show the bias.
- **The measured cancel rate is slightly understated.** The generator's denominator includes the last cycle before 2026-10-01, which is cut off partway through.
