"""Shared settings for the pipeline and the policy validator.

Both pipeline.py and policies.py import from here, so the rules the
validator enforces (retention window, label window, pseudonym key) are the
same numbers the pipeline was built with. policies.py must never import
pipeline.py itself; this file is the only code they share.
"""

import hashlib
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


def pseudonymize(customer_id):
    """Turn a customer_id into a stable, non-reversible customer_key.

    Same input always gives the same key, so tables can still be joined
    downstream without exposing the real ID.
    """
    digest = hashlib.sha256((PSEUDONYM_SALT + customer_id).encode("utf-8")).hexdigest()
    return digest[:20]
