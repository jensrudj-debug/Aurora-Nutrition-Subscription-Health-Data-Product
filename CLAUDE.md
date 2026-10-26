# CLAUDE.md

## Project

Proof artifact for **Subscription Health**, a data product for Aurora Nutrition, a direct-to-consumer supplement subscription company.

The artifact proves one claim: **governance rules are executable, run on every change, and block a bad change no matter who or what wrote it.**

## Rules

### Runtime
- Python 3.10+, standard library only. No pip installs.

### Data
- All data is synthetic and reproducible: fixed random seed `42`, fixed as-of date `2026-10-01`.
- Fake PII only:
  - Emails use the reserved `example.com` domain.
  - Phone numbers use the fictional `555-01xx` range.
- Never generate card numbers. Billing uses opaque payment tokens.

### Folder layout
- `generate_data.py`, `pipeline.py`, `policies.py` at the repo root.
- `contracts/` — interface contracts.
- `raw/` — generated source data.
- `products/` — pipeline outputs.
- `tests/` — test fixtures.

### Validator
- `policies.py` must never import from `pipeline.py`. It checks outputs independently.
- Every policy rule returns a list of violation strings; an empty list means pass.
- The validator exits with code `1` if any rule fails.

### Code style
- Keep code readable for a student who must explain it on camera: short functions, docstrings, and comments that explain WHY, not what.

### Workflow
- After any change, state the exact command to run to verify it.
