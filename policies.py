"""Policy-as-code validator for the Subscription Health data product.

Checks the published outputs in products/ against four governance rules
(POL-001..004) and, given a pull-request file, who may change what (POL-005).
It reads only products/, raw/consent.csv, contracts/ and config.py, and it
never imports pipeline.py or governance.py: a validator that reuses the
pipeline's code would inherit the pipeline's bugs and approve them.

Every rule returns a list of violation strings. Empty list = PASS.
Any FAIL makes the script exit with code 1, which is what blocks the
change in CI or a Dagster asset check, whoever (or whatever) wrote it.

Usage:
    python policies.py                                         # POL-001..004
    python policies.py --pr tests/fixtures/pr_agent_red.json   # + POL-005
"""

import argparse
import csv
import hashlib
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from config import (
    AS_OF_DATE, CONTRACTS_DIR, PRODUCTS_DIR, PSEUDONYM_SALT, RAW_DIR, RETENTION_DAYS,
)

MAX_EXAMPLES_PRINTED = 5
MAX_VIOLATIONS_IN_REPORT = 100


# --------------------------------------------------------------------------
# POL-001: no raw PII in vector-layer chunks
# --------------------------------------------------------------------------

# These patterns are deliberately NOT the pipeline's redaction patterns.
# If the validator used the same regex as the redactor, any address the
# redactor misses would also be missed here, and the check would pass
# exactly when it should fail. A second, broader detector means a single
# blind spot can't hide in both places at once.
#
# Email: we look only for "@" followed by something shaped like a domain,
# ignoring the part before "@", so odd local parts ("j..doe+x") still count.
AT_DOMAIN_RE = re.compile(r"@\s*[A-Za-z0-9-]+(?:\s*\.\s*[A-Za-z0-9-]+)+")
# Phone: any run of 10 or more digits, with spaces, dots, dashes, brackets
# or "+" allowed between them. Broader than "(612) 555-0147" on purpose:
# it also catches "+1 612.555.0147" and "612 555 01 47".
DIGIT_RUN_RE = re.compile(r"\d(?:[\s().+-]*\d){9,}")


def pol_001_no_pii_in_vector_layer(chunks, **_):
    """No email address or phone number may appear in any chunk's text.

    Chunks are what the assistant puts in front of a language model, so PII
    here would leak into prompts, logs and possibly answers.
    """
    violations = []
    for c in chunks:
        for label, rx in (("email", AT_DOMAIN_RE), ("phone", DIGIT_RUN_RE)):
            m = rx.search(c.get("text", ""))
            if m:
                violations.append(f"chunk {c.get('chunk_id')}: raw {label} detected ({m.group(0)!r})")
    return violations


# --------------------------------------------------------------------------
# POL-002: withdrawn analytics consent excluded from features
# --------------------------------------------------------------------------

def latest_consent(consent_rows, purpose):
    """Current status per customer for one purpose: the latest row wins.

    The consent ledger is append-only, so a later withdrawal overrides an
    earlier grant.
    """
    status = {}
    for row in sorted(consent_rows, key=lambda r: r["updated_ts"]):
        if row["purpose"] == purpose:
            status[row["customer_id"]] = row["status"]
    return status


def expected_customer_key(customer_id):
    """The customer_key the published key spec says this customer must have.

    Recomputed here from the shared salt instead of importing
    governance.pseudonymize: if the pipeline's hashing ever changed,
    its keys would stop matching these and POL-002 would fail loudly
    rather than quietly agree with the bug.
    """
    digest = hashlib.sha256((PSEUDONYM_SALT + customer_id).encode("utf-8")).hexdigest()
    return digest[:20]


def pol_002_consent_respected_in_features(features, consent, **_):
    """Every customer_key in churn_features must have granted analytics_profiling.

    Keys are recomputed here from raw consent rather than trusted from the
    pipeline, so the check works even if the pipeline's filter is wrong.
    A key with no consent record at all also fails: consent is opt-in.
    """
    status_by_key = {expected_customer_key(cid): s
                     for cid, s in latest_consent(consent, "analytics_profiling").items()}
    violations = []
    for row in features:
        key = row.get("customer_key")
        status = status_by_key.get(key, "no consent record")
        if status != "granted":
            violations.append(f"customer_key {key}: analytics_profiling is '{status}' but row present")
    return violations


# --------------------------------------------------------------------------
# POL-003: transcript chunks within the 24-month retention window
# --------------------------------------------------------------------------

def pol_003_retention_window(chunks, **_):
    """No chunk may come from a transcript older than RETENTION_DAYS.

    Storage limitation: data past its retention window must be gone, not
    just hidden. An unreadable date fails too, since we can't prove its age.
    """
    floor = AS_OF_DATE - timedelta(days=RETENTION_DAYS)
    violations = []
    for c in chunks:
        created = parse_date(c.get("created_date"))
        if created is None:
            violations.append(f"chunk {c.get('chunk_id')}: unreadable created_date {c.get('created_date')!r}")
        elif created < floor:
            violations.append(f"chunk {c.get('chunk_id')}: {created} older than {floor}")
    return violations


# --------------------------------------------------------------------------
# POL-004: churn_features honors its contract
# --------------------------------------------------------------------------

FORBIDDEN_COLUMNS = {"email", "phone", "first_name", "last_name", "customer_id", "address"}


def parse_date(value):
    """Return a date for a strict YYYY-MM-DD string, otherwise None."""
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:  # e.g. 2026-02-30
        return None


# Each declared contract type maps to a test that a CSV value really is that type.
TYPE_CHECKS = {
    "int": lambda v: re.fullmatch(r"-?\d+", v) is not None,
    "decimal": lambda v: re.fullmatch(r"-?\d+(\.\d+)?", v) is not None,
    "date": lambda v: parse_date(v) is not None,
    "string": lambda v: isinstance(v, str),
}


def pol_004_feature_contract(features, header, contract, **_):
    """churn_features must match contracts/churn_features.json exactly.

    The ML team codes against the contract, not against whatever the
    pipeline happened to write. A silent rename, a stray identifier column
    or a text value in a number column would break or bias their model.
    """
    violations = []
    expected = [c["name"] for c in contract["columns"]]

    # Name only the difference, so a reviewer sees what changed at a glance.
    unexpected = [c for c in header if c not in expected]
    missing = [c for c in expected if c not in header]
    if unexpected:
        violations.append(f"unexpected column(s) {unexpected} not in contract")
    if missing:
        violations.append(f"missing column(s) {missing} required by contract")
    if not unexpected and not missing and header != expected:
        violations.append("column order differs from contract")
    for col in sorted(FORBIDDEN_COLUMNS & set(header)):
        violations.append(f"forbidden identifier column '{col}' present")

    seen_keys = set()
    pk = contract["primary_key"]
    for line_no, row in enumerate(features, start=2):  # line 1 is the header
        for col in contract["columns"]:
            name = col["name"]
            value = row.get(name)
            if value is None or value == "":
                if not col["nullable"]:
                    violations.append(f"line {line_no}: '{name}' is null but not nullable")
                continue
            if not TYPE_CHECKS[col["type"]](value):
                violations.append(f"line {line_no}: '{name}'={value!r} is not a valid {col['type']}")
        key = row.get(pk)
        if key in seen_keys:
            violations.append(f"line {line_no}: duplicate primary key {pk}={key}")
        seen_keys.add(key)
    return violations


# --------------------------------------------------------------------------
# POL-005: agent PRs cannot touch Red-zone paths (ADR-004)
# --------------------------------------------------------------------------

# The code that enforces privacy and governance. An agent may propose
# changes anywhere else, but never here: a plausible-looking edit to the
# redactor, the validator or the contract could switch off the very checks
# that would catch it. Entries ending in "/" cover a whole folder.
RED_ZONE = ("governance.py", "policies.py", "config.py", "contracts/", ".github/")
YELLOW_LABEL = "yellow"
YELLOW_MIN_APPROVALS = 2


def in_red_zone(path):
    """True if a changed file is a Red-zone file or sits inside a Red-zone folder."""
    path = path.replace("\\", "/").removeprefix("./")
    return any(path.startswith(entry) if entry.endswith("/") else path == entry
               for entry in RED_ZONE)


def pol_005_agent_zones(pr, **_):
    """Agent-authored PRs: never touch Red zone; Yellow needs 2 human approvals.

    Human PRs are not restricted here; they go through normal code review.
    A PR that doesn't say who wrote it is treated as an agent PR, so a
    missing field can't be used to skip the check.
    """
    if pr.get("author_type", "agent") != "agent":
        return []
    violations = []
    for path in pr.get("changed_files", []):
        if in_red_zone(path):
            violations.append(f"agent PR may not modify Red-zone path {path}")
    labels = [label.lower() for label in pr.get("labels", [])]
    # set(): the same reviewer approving twice is still one person.
    approvers = set(pr.get("human_approvals", []))
    if YELLOW_LABEL in labels and len(approvers) < YELLOW_MIN_APPROVALS:
        violations.append(f"Yellow-zone agent PR has {len(approvers)} human approval(s); "
                          f"needs {YELLOW_MIN_APPROVALS}")
    return violations


# --------------------------------------------------------------------------
# Running the rules
# --------------------------------------------------------------------------

RULES = [
    ("POL-001", "No raw PII in vector-layer chunks", pol_001_no_pii_in_vector_layer),
    ("POL-002", "Withdrawn analytics consent excluded from features", pol_002_consent_respected_in_features),
    ("POL-003", "Transcript chunks within 730-day retention window", pol_003_retention_window),
    ("POL-004", "churn_features honors its contract", pol_004_feature_contract),
    ("POL-005", "Agent PRs cannot touch Red-zone paths", pol_005_agent_zones),
]
# Rules that check a pull request rather than data; skipped when no PR is given.
PR_RULES = {"POL-005"}


def load_context(products_dir=PRODUCTS_DIR, raw_dir=RAW_DIR, contracts_dir=CONTRACTS_DIR):
    """Read everything the rules need from disk into one dictionary.

    Rules take keyword arguments and ignore what they don't use (**_), so
    tests can call them with in-memory data instead of files.
    """
    with (products_dir / "churn_features.csv").open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        features = list(reader)
        header = list(reader.fieldnames or [])
    with (products_dir / "rag_chunks.jsonl").open(encoding="utf-8") as f:
        chunks = [json.loads(line) for line in f if line.strip()]
    with (raw_dir / "consent.csv").open(newline="", encoding="utf-8") as f:
        consent = list(csv.DictReader(f))
    contract = json.loads((contracts_dir / "churn_features.json").read_text(encoding="utf-8"))
    return {"features": features, "header": header, "chunks": chunks,
            "consent": consent, "contract": contract}


def run_rules(context):
    """Run every rule and return one result dict per rule."""
    results = []
    for rule_id, title, rule in RULES:
        if rule_id in PR_RULES and context.get("pr") is None:
            results.append({"rule": rule_id, "title": title, "status": "SKIPPED",
                            "violation_count": 0, "violations": [],
                            "reason": "no --pr file given"})
            continue
        violations = rule(**context)
        results.append({
            "rule": rule_id,
            "title": title,
            "status": "FAIL" if violations else "PASS",
            "violation_count": len(violations),
            "violations": violations[:MAX_VIOLATIONS_IN_REPORT],
        })
    return results


def print_results(results):
    for r in results:
        if r["status"] == "SKIPPED":
            print(f"SKIP  {r['rule']}  {r['title']}  ({r['reason']})")
            continue
        print(f"{r['status']}  {r['rule']}  {r['title']}"
              + (f"  ({r['violation_count']} violations)" if r["violation_count"] else ""))
        for v in r["violations"][:MAX_EXAMPLES_PRINTED]:
            print(f"        - {v}")
    ran = [r for r in results if r["status"] != "SKIPPED"]
    failed = [r["rule"] for r in ran if r["status"] == "FAIL"]
    print()
    print(f"RESULT: {'FAIL' if failed else 'PASS'}  "
          f"({len(ran) - len(failed)}/{len(ran)} rules passed"
          + (f"; failed: {', '.join(failed)})" if failed else ")"))


def write_report(results, path, pr_path=None):
    report = {
        "validator": "policies.py",
        "as_of_date": AS_OF_DATE.isoformat(),
        "pr_file": pr_path,
        "overall": "FAIL" if any(r["status"] == "FAIL" for r in results) else "PASS",
        "rules": results,
    }
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


def parse_args():
    parser = argparse.ArgumentParser(description="Run the Subscription Health policy checks.")
    parser.add_argument("--pr", metavar="FILE",
                        help="pull-request JSON to check with POL-005 "
                             "(e.g. tests/fixtures/pr_agent_red.json)")
    return parser.parse_args()


def main():
    args = parse_args()
    # Missing inputs are a failure, not a pass: nothing was proven.
    try:
        context = load_context()
    except FileNotFoundError as e:
        print(f"FAIL  cannot read inputs: {e}. Run generate_data.py and pipeline.py first.")
        return 1
    try:
        context["pr"] = (json.loads(Path(args.pr).read_text(encoding="utf-8"))
                         if args.pr else None)
    except FileNotFoundError:
        print(f"FAIL  cannot read pull-request file: {args.pr}")
        return 1
    results = run_rules(context)
    print_results(results)
    write_report(results, PRODUCTS_DIR / "policy_report.json", args.pr)
    return 1 if any(r["status"] == "FAIL" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
