"""Build the Subscription Health data products from raw/ into products/.

Outputs:
    products/churn_features.csv   feature table for the ML team (contract v1.0.0)
    products/rag_chunks.jsonl     redacted transcript chunks for the assistant
    products/lineage.json         where every output came from and how

Usage:
    python generate_data.py   # first, if raw/ is empty
    python pipeline.py
    python pipeline.py --seed-violation   # demo: plant one violation per policy
"""

import argparse
import csv
import hashlib
import json
from datetime import datetime, timedelta, timezone

from config import (
    AS_OF_DATE, CONTRACTS_DIR, LABEL_WINDOW_DAYS, OBSERVATION_DATE,
    PRODUCTS_DIR, RAW_DIR, RETENTION_DAYS,
)
# Consent, redaction and pseudonymization live in governance.py so the
# privacy-critical code is one reviewable, separately owned module.
from governance import REDACTION_VERSION, consent_lookup, has_consent, pseudonymize, redact

PIPELINE_VERSION = "1.0.0"
CHUNK_WORDS = 60
CHUNK_OVERLAP = 15


# --------------------------------------------------------------------------
# Reading inputs
# --------------------------------------------------------------------------

def read_csv(name):
    with (RAW_DIR / name).open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_jsonl(name):
    with (RAW_DIR / name).open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def parse_ts(value):
    """Parse '2026-03-04T10:22:31Z'. (fromisoformat only accepts 'Z' from 3.11.)"""
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def to_midnight(d):
    """A date as a UTC datetime at 00:00, for comparing with timestamps."""
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


# --------------------------------------------------------------------------
# 1. churn_features
# --------------------------------------------------------------------------

def build_churn_features(subscribers, events, billing, transcripts, consent, columns):
    """One row per scoreable, consenting customer at the observation date.

    Point-in-time rule: every feature uses only data from before
    OBSERVATION_DATE. Only the label looks forward, into the 90 days the
    model will later be asked to predict.
    """
    obs = to_midnight(OBSERVATION_DATE)
    as_of = to_midnight(AS_OF_DATE)
    payment_window_start = obs - timedelta(days=90)

    cancel_ts = {e["customer_id"]: parse_ts(e["event_ts"])
                 for e in events if e["event_type"] == "cancel"}
    events_by_customer = group_by(events, "customer_id")
    billing_by_customer = group_by(billing, "customer_id")
    contacts_by_customer = group_by(transcripts, "customer_id")

    rows = []
    excluded = {"not_scoreable": 0, "consent": 0}
    for sub in subscribers:
        cid = sub["customer_id"]
        signup = to_midnight(datetime.strptime(sub["signup_date"], "%Y-%m-%d").date())
        cancelled = cancel_ts.get(cid)

        # Not a customer at the observation date: nothing to predict.
        if signup >= obs or (cancelled and cancelled < obs):
            excluded["not_scoreable"] += 1
            continue
        # Profiling opt-outs are dropped before any feature is computed (POL-002).
        if not has_consent(consent, cid, "analytics_profiling"):
            excluded["consent"] += 1
            continue

        past_events = [e for e in events_by_customer.get(cid, []) if parse_ts(e["event_ts"]) < obs]
        row = {
            "customer_key": pseudonymize(cid),
            "as_of_date": OBSERVATION_DATE.isoformat(),
            "plan_id": sub["plan_id"],
            "monthly_price_usd": f"{float(sub['monthly_price_usd']):.2f}",
            "acquisition_channel": sub["acquisition_channel"],
            "tenure_days": (obs - signup).days,
            "skips_total": sum(e["event_type"] == "skip_shipment" for e in past_events),
            "pauses_total": sum(e["event_type"] == "pause" for e in past_events),
            "failed_payments_90d": sum(
                b["status"] == "failed"
                and payment_window_start <= parse_ts(b["billed_ts"]) < obs
                for b in billing_by_customer.get(cid, [])
            ),
            "support_contacts_total": sum(
                t["created_date"] < OBSERVATION_DATE.isoformat()
                for t in contacts_by_customer.get(cid, [])
            ),
            "label_churned_next_90d": int(bool(cancelled and obs <= cancelled < as_of)),
        }
        # Build strictly from the contract's column list, so a column can't
        # be added here (say, an email) without the contract changing too.
        rows.append({col: row[col] for col in columns})
    return rows, excluded


def group_by(rows, key):
    out = {}
    for row in rows:
        out.setdefault(row[key], []).append(row)
    return out


# --------------------------------------------------------------------------
# 2. rag_chunks
# --------------------------------------------------------------------------

def chunk_words(text, size=CHUNK_WORDS, overlap=CHUNK_OVERLAP):
    """Split text into overlapping word windows.

    The overlap means a sentence cut at a boundary still appears whole in
    one of the two neighbouring chunks, so retrieval doesn't lose it.
    """
    words = text.split()
    step = size - overlap
    starts = range(0, max(len(words) - overlap, 1), step)
    return [" ".join(words[s:s + size]) for s in starts]


def build_rag_chunks(subscribers, transcripts, consent):
    """Consent filter, then retention filter, then redaction, then chunking.

    The order matters: excluded transcripts are never redacted or chunked
    at all, and nothing reaches a chunk before its PII is removed.
    """
    subs = {s["customer_id"]: s for s in subscribers}
    retention_floor = (AS_OF_DATE - timedelta(days=RETENTION_DAYS)).isoformat()

    chunks = []
    excluded = {"consent": 0, "retention": 0}
    for t in transcripts:
        cid = t["customer_id"]
        if not has_consent(consent, cid, "ai_support_assistant"):
            excluded["consent"] += 1
            continue
        if t["created_date"] < retention_floor:
            excluded["retention"] += 1
            continue

        clean = redact(t["text"], subs[cid])
        for i, piece in enumerate(chunk_words(clean)):
            chunks.append({
                "chunk_id": f"{t['transcript_id']}-{i:02d}",
                "source_transcript_id": t["transcript_id"],
                "customer_key": pseudonymize(cid),
                "created_date": t["created_date"],
                "contact_reason": t["contact_reason"],
                "redaction_version": REDACTION_VERSION,
                "text": piece,
            })
    return chunks, excluded


# --------------------------------------------------------------------------
# Demo only: seeded violations
# --------------------------------------------------------------------------

def seed_violations(features, chunks, columns, subscribers, consent):
    """Plant exactly one known violation per policy into the built outputs.

    Each one imitates a mistake that really happens in data teams. The
    point of the demo is that policies.py catches all four without being
    told they are there, the same way it would catch them from a person
    or an AI agent.
    """
    seeded = []

    # 1. POL-001 — a redaction regression: someone "simplifies" the redactor
    # and an email address slips through into text that will be embedded.
    chunks[0]["text"] += " You can also reach me at jordan.olson.9999@example.com."
    seeded.append({"rule": "POL-001", "target": f"chunk {chunks[0]['chunk_id']}",
                   "injection": "fake example.com email appended to chunk text",
                   "imitates": "a redaction regression"})

    # 2. POL-002 — a join that drops the consent filter: a refactor rebuilds
    # the feature table from the full customer list and a profiling
    # opt-out gets back in.
    withdrawn_id = sorted(cid for (cid, purpose), status in consent.items()
                          if purpose == "analytics_profiling" and status != "granted")[0]
    features.append({**features[0], "customer_key": pseudonymize(withdrawn_id)})
    seeded.append({"rule": "POL-002", "target": f"customer_key {pseudonymize(withdrawn_id)}",
                   "injection": "feature row added for a customer who withdrew analytics_profiling",
                   "imitates": "a join that drops the consent filter"})

    # 3. POL-003 — a backfill that ignores retention: an old export is
    # re-loaded "to improve coverage" without re-applying the 730-day window.
    # Copies a clean chunk's text so this injection trips only POL-003.
    old_date = (AS_OF_DATE - timedelta(days=900)).isoformat()
    backfill = {**chunks[1], "chunk_id": "BACKFILL-00", "created_date": old_date}
    chunks.append(backfill)
    seeded.append({"rule": "POL-003", "target": "chunk BACKFILL-00",
                   "injection": f"chunk dated {old_date} (900 days before as-of) added",
                   "imitates": "a backfill that ignores retention"})

    # 4. POL-004 — a schema change nobody announced: someone joins
    # subscribers in "for convenience" and the feature table gains an
    # email column the contract never allowed.
    email_by_key = {pseudonymize(s["customer_id"]): s["email"] for s in subscribers}
    for row in features:
        row["email"] = email_by_key.get(row["customer_key"], "unknown@example.com")
    columns = columns + ["email"]
    seeded.append({"rule": "POL-004", "target": "churn_features.csv header",
                   "injection": "extra 'email' column added to churn_features",
                   "imitates": "a schema change nobody announced"})

    return features, chunks, columns, seeded


# --------------------------------------------------------------------------
# 3. Writing outputs and lineage
# --------------------------------------------------------------------------

def write_csv(path, rows, fieldnames):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path, rows):
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def file_sha256(path):
    """Content hash: if a single byte of a file changes, this changes."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def file_entry(path, rows):
    return {"path": path.relative_to(path.parents[1]).as_posix(),
            "rows": rows, "sha256": file_sha256(path)}


def build_lineage(inputs, features_path, n_features, feature_excl,
                  chunks_path, n_chunks, chunk_excl, contract):
    """Record what each output was derived from and every step applied.

    This is what lets someone answer "where did this row come from?" and
    prove a training snapshot hasn't changed since it was frozen.
    """
    return {
        "pipeline": "pipeline.py",
        "pipeline_version": PIPELINE_VERSION,
        "as_of_date": AS_OF_DATE.isoformat(),
        "inputs": inputs,
        "outputs": {
            "churn_features": {
                **file_entry(features_path, n_features),
                "contract": f"contracts/churn_features.json v{contract['version']}",
                "derived_from": ["raw/subscribers.csv", "raw/subscription_events.csv",
                                 "raw/billing.csv", "raw/transcripts.jsonl", "raw/consent.csv"],
                "transforms": [
                    f"observation_date = as_of_date - {LABEL_WINDOW_DAYS} days "
                    f"= {OBSERVATION_DATE.isoformat()}",
                    "drop customers who signed up on/after observation_date "
                    "or cancelled before it (not scoreable)",
                    "drop customers whose latest analytics_profiling consent is not 'granted'",
                    "features use only events, billing and transcripts dated before observation_date",
                    "failed_payments_90d = failed billing attempts in the 90 days before observation_date",
                    "label_churned_next_90d = 1 if cancel between observation_date and as_of_date",
                    "customer_id -> customer_key = first 20 hex of SHA-256(salt + customer_id)",
                    "columns selected exactly from contract; names, emails, phones never output",
                ],
                "rows_excluded": feature_excl,
            },
            "rag_chunks": {
                **file_entry(chunks_path, n_chunks),
                "derived_from": ["raw/transcripts.jsonl", "raw/subscribers.csv", "raw/consent.csv"],
                "transforms": [
                    "drop transcripts whose latest ai_support_assistant consent is not 'granted'",
                    f"drop transcripts older than {RETENTION_DAYS} days before as_of_date",
                    f"redact ({REDACTION_VERSION}): customer email and regex emails -> [EMAIL], "
                    "customer phone and regex phones -> [PHONE], first and last name -> [NAME]",
                    f"chunk into {CHUNK_WORDS}-word pieces with {CHUNK_OVERLAP}-word overlap",
                    "customer_id -> customer_key = first 20 hex of SHA-256(salt + customer_id)",
                ],
                "rows_excluded": chunk_excl,
            },
        },
    }


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seed-violation", action="store_true",
                        help="after the normal build, plant one known violation per policy "
                             "(demo only; policies.py should then fail)")
    return parser.parse_args()


def main():
    args = parse_args()
    subscribers = read_csv("subscribers.csv")
    events = read_csv("subscription_events.csv")
    billing = read_csv("billing.csv")
    consent_rows = read_csv("consent.csv")
    transcripts = read_jsonl("transcripts.jsonl")
    consent = consent_lookup(consent_rows)

    contract = json.loads((CONTRACTS_DIR / "churn_features.json").read_text(encoding="utf-8"))
    columns = [c["name"] for c in contract["columns"]]

    features, feature_excl = build_churn_features(
        subscribers, events, billing, transcripts, consent, columns)
    chunks, chunk_excl = build_rag_chunks(subscribers, transcripts, consent)

    seeded = []
    if args.seed_violation:
        features, chunks, columns, seeded = seed_violations(
            features, chunks, columns, subscribers, consent)

    PRODUCTS_DIR.mkdir(exist_ok=True)
    features_path = PRODUCTS_DIR / "churn_features.csv"
    chunks_path = PRODUCTS_DIR / "rag_chunks.jsonl"
    write_csv(features_path, features, columns)
    write_jsonl(chunks_path, chunks)

    inputs = {
        name: file_entry(RAW_DIR / name, len(rows))
        for name, rows in [
            ("subscribers.csv", subscribers),
            ("subscription_events.csv", events),
            ("billing.csv", billing),
            ("consent.csv", consent_rows),
            ("transcripts.jsonl", transcripts),
        ]
    }
    lineage = build_lineage(inputs, features_path, len(features), feature_excl,
                            chunks_path, len(chunks), chunk_excl, contract)
    # A seeded run must be impossible to mistake for a real one: the
    # lineage says so, and lists exactly what was planted where.
    lineage["run_mode"] = "seeded_violations" if seeded else "normal"
    lineage["seeded_violations"] = seeded
    (PRODUCTS_DIR / "lineage.json").write_text(json.dumps(lineage, indent=2) + "\n",
                                               encoding="utf-8")

    positives = sum(r["label_churned_next_90d"] for r in features)
    print(f"Wrote {PRODUCTS_DIR}")
    print(f"  churn_features.csv  {len(features):>6,} rows  "
          f"(excluded: {feature_excl['not_scoreable']:,} not scoreable, "
          f"{feature_excl['consent']:,} consent; "
          f"label rate {positives / max(len(features), 1):.1%})")
    print(f"  rag_chunks.jsonl    {len(chunks):>6,} rows  "
          f"(excluded transcripts: {chunk_excl['consent']:,} consent, "
          f"{chunk_excl['retention']:,} retention)")
    print("  lineage.json        written")
    if seeded:
        print()
        print(f"  SEEDED RUN: {len(seeded)} known violations planted (see lineage.json):")
        for s in seeded:
            print(f"    {s['rule']}  {s['injection']}  <- imitates {s['imitates']}")


if __name__ == "__main__":
    main()
