"""Governance controls the pipeline applies: consent, redaction, pseudonymization.

These are the Red-zone functions in ADR-004 — the code an AI agent may
never change. Keeping them in one small module means a single CODEOWNERS
rule (governance.py -> Privacy lead) protects all of them, and a reviewer
can audit them without reading the rest of the pipeline.

pipeline.py imports this module. policies.py does NOT: the validator
checks the outputs with its own, independent logic, so a bug here can't
also hide in the check that is meant to catch it.
"""

import hashlib
import re

from config import PSEUDONYM_SALT

REDACTION_VERSION = "regex-v1"

# Catch-all patterns, applied after the customer's own known values, so PII
# the customer didn't give at signup (a second email, a spouse's phone)
# is still removed.
EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
PHONE_RE = re.compile(r"\(?\b\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}\b")


# --------------------------------------------------------------------------
# Consent
# --------------------------------------------------------------------------

def consent_lookup(consent_rows):
    """Current consent status per (customer_id, purpose).

    The ledger is append-only, so the latest row per purpose wins: a
    customer who granted and later withdrew counts as withdrawn.
    """
    latest = {}
    for row in sorted(consent_rows, key=lambda r: r["updated_ts"]):
        latest[(row["customer_id"], row["purpose"])] = row["status"]
    return latest


def has_consent(consent, customer_id, purpose):
    """Missing consent is treated as no consent: opt-in, never assumed."""
    return consent.get((customer_id, purpose)) == "granted"


# --------------------------------------------------------------------------
# Redaction
# --------------------------------------------------------------------------

def redact(text, sub):
    """Replace a transcript's PII with [EMAIL], [PHONE] and [NAME].

    Emails go first because they contain the customer's name
    (dana.reyes.0042@...); redacting the name first would leave a
    half-masked address behind.
    """
    text = text.replace(sub["email"], "[EMAIL]")
    text = EMAIL_RE.sub("[EMAIL]", text)
    text = text.replace(sub["phone"], "[PHONE]")
    text = PHONE_RE.sub("[PHONE]", text)
    for name in (sub["first_name"], sub["last_name"]):
        text = re.sub(rf"\b{re.escape(name)}\b", "[NAME]", text)
    return text


# --------------------------------------------------------------------------
# Pseudonymization
# --------------------------------------------------------------------------

def pseudonymize(customer_id):
    """Turn a customer_id into a stable, non-reversible customer_key.

    Same input always gives the same key, so tables can still be joined
    downstream without exposing the real ID.
    """
    digest = hashlib.sha256((PSEUDONYM_SALT + customer_id).encode("utf-8")).hexdigest()
    return digest[:20]
