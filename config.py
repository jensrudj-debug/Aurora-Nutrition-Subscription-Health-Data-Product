"""Shared settings for the pipeline and the policy validator.

Both pipeline.py and policies.py import from here, so the rules the
validator enforces (retention window, label window, pseudonym salt) are the
same values the pipeline was built with. policies.py must never import
pipeline.py or governance.py; this file holds settings only, no logic,
so it is the one thing they safely share.
"""

from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).parent
RAW_DIR = ROOT / "raw"
PRODUCTS_DIR = ROOT / "products"
CONTRACTS_DIR = ROOT / "contracts"

# Fixed so every run, on every machine, sees the same "today".
AS_OF_DATE = date(2026, 10, 1)

# Storage limitation: transcripts older than this never enter the RAG corpus
# (compliance matrix, POL-003).
RETENTION_DAYS = 730

# The churn label looks this far ahead. Features are observed this many days
# before AS_OF_DATE so the label window has already happened in our data.
LABEL_WINDOW_DAYS = 90
OBSERVATION_DATE = AS_OF_DATE - timedelta(days=LABEL_WINDOW_DAYS)

# Demo-only salt. In production this value lives in a secrets manager
# (e.g. AWS Secrets Manager) and is injected at runtime: anyone holding the
# salt can re-link a customer_key back to a customer_id.
PSEUDONYM_SALT = "aurora-demo-salt-not-for-production"
