"""Policy-as-code validator for the Subscription Health data product.

Checks the published outputs in products/ against four governance rules
(POL-001..004) and, given a pull-request file, who may change what (POL-005).
It reads only products/, raw/consent.csv, the ID and name columns of
raw/subscribers.csv (to prove names were redacted), contracts/ and
config.py. It never imports pipeline.py or governance.py: a validator that
reuses the pipeline's code would inherit the pipeline's bugs and approve them.

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
# Local number without an area code ("555-0147"). Needs a separator, so an
# ordinary 7-digit number in a sentence doesn't count as a phone.
LOCAL_PHONE_RE = re.compile(r"(?<!\d)\d{3}[\s.-]\d{4}(?!\d)")

# Every field a chunk may carry, and the shape its value must have. Any
# other field fails: an unexpected field is exactly where a raw email or
# phone would slip past a check that only reads the text. Structured fields
# are checked by shape rather than scanned for PII, because IDs and hex keys
# contain digit runs that would look like phone numbers.
CHUNK_FIELDS = {
    "chunk_id": re.compile(r"[A-Za-z0-9]+-\d{2}"),
    "source_transcript_id": re.compile(r"[A-Za-z0-9]+"),
    "customer_key": re.compile(r"[0-9a-f]{20}"),
    "created_date": None,       # checked by POL-003
    "contact_reason": re.compile(r"[a-z_]+"),
    "redaction_version": re.compile(r"[a-z0-9.-]+"),
    "text": None,               # free text: scanned for PII below
}


def names_by_key(subscribers):
    """customer_key -> (first_name, last_name), for the own-name check."""
    return {expected_customer_key(s["customer_id"]): (s["first_name"], s["last_name"])
            for s in subscribers}


def chunk_schema_violations(c):
    """Fields outside CHUNK_FIELDS, missing fields, or values of the wrong shape."""
    cid = c.get("chunk_id")
    out = [f"chunk {cid}: unexpected field '{f}'" for f in c if f not in CHUNK_FIELDS]
    for field, shape in CHUNK_FIELDS.items():
        if field not in c:
            out.append(f"chunk {cid}: missing field '{field}'")
        elif shape and not (isinstance(c[field], str) and shape.fullmatch(c[field])):
            out.append(f"chunk {cid}: field '{field}' has an unexpected value")
    return out


def text_pii_violations(c, names):
    """Emails, phone numbers and the customer's own name in a chunk's text."""
    cid, text = c.get("chunk_id"), c.get("text", "")
    if not isinstance(text, str):
        return [f"chunk {cid}: text is not a string"]
    out = []
    m = AT_DOMAIN_RE.search(text)
    if m:
        out.append(f"chunk {cid}: raw email detected ({m.group(0)!r})")
    m = DIGIT_RUN_RE.search(text) or LOCAL_PHONE_RE.search(text)
    if m:
        out.append(f"chunk {cid}: raw phone detected ({m.group(0)!r})")
    # The name itself is not printed, so the report doesn't repeat the leak.
    first, last = names.get(c.get("customer_key"), (None, None))
    for label, name in (("first", first), ("last", last)):
        if name and re.search(rf"\b{re.escape(name)}\b", text, re.IGNORECASE):
            out.append(f"chunk {cid}: customer's own {label} name detected")
    return out


def pol_001_no_pii_in_vector_layer(chunks, subscribers, **_):
    """No email, phone or customer name may reach the vector layer.

    Chunks are what the assistant puts in front of a language model, so PII
    here would leak into prompts, logs and possibly answers. Every field is
    covered: free text is scanned, everything else must match its expected
    shape. An empty corpus fails too, since nothing was proven.
    """
    if not chunks:
        return ["rag_chunks.jsonl has no rows"]
    names = names_by_key(subscribers)
    violations = []
    for c in chunks:
        violations += chunk_schema_violations(c)
        violations += text_pii_violations(c, names)
    return violations


# --------------------------------------------------------------------------
# POL-002: withdrawn consent excluded from features and chunks
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


def status_by_key(consent, purpose):
    """Latest consent status for one purpose, keyed by recomputed customer_key."""
    return {expected_customer_key(cid): s for cid, s in latest_consent(consent, purpose).items()}


def pol_002_consent_respected(features, chunks, consent, **_):
    """Each product holds only customers who granted that product's purpose.

    churn_features needs analytics_profiling; rag_chunks needs
    ai_support_assistant. Keys are recomputed here from raw consent rather
    than trusted from the pipeline, so the check works even if the
    pipeline's filter is wrong. A key with no consent record at all also
    fails: consent is opt-in.
    """
    violations = []
    profiling = status_by_key(consent, "analytics_profiling")
    for row in features:
        key = row.get("customer_key")
        status = profiling.get(key, "no consent record")
        if status != "granted":
            violations.append(f"customer_key {key}: analytics_profiling is '{status}' but row present")
    assistant = status_by_key(consent, "ai_support_assistant")
    for c in chunks:
        status = assistant.get(c.get("customer_key"), "no consent record")
        if status != "granted":
            violations.append(f"chunk {c.get('chunk_id')}: ai_support_assistant is '{status}' "
                              f"but chunk present")
    return violations


# --------------------------------------------------------------------------
# POL-003: transcript chunks within the 24-month retention window
# --------------------------------------------------------------------------

def pol_003_retention_window(chunks, **_):
    """No chunk may come from a transcript older than RETENTION_DAYS.

    Storage limitation: data past its retention window must be gone, not
    just hidden. An unreadable date fails too, since we can't prove its age,
    and so does a date after the as-of date: a future date is a broken
    date, and it would keep a chunk "young" forever.
    """
    floor = AS_OF_DATE - timedelta(days=RETENTION_DAYS)
    violations = []
    for c in chunks:
        created = parse_date(c.get("created_date"))
        if created is None:
            violations.append(f"chunk {c.get('chunk_id')}: unreadable created_date {c.get('created_date')!r}")
        elif created < floor:
            violations.append(f"chunk {c.get('chunk_id')}: {created} older than {floor}")
        elif created > AS_OF_DATE:
            violations.append(f"chunk {c.get('chunk_id')}: {created} is after the as-of date {AS_OF_DATE}")
    return violations


# --------------------------------------------------------------------------
# POL-004: churn_features honors its contract
# --------------------------------------------------------------------------

FORBIDDEN_COLUMNS = {"email", "phone", "first_name", "last_name", "customer_id", "address"}
# The only PII classes this port may publish. A contract column declaring
# anything else (e.g. "direct") is a contract nobody should have approved.
ALLOWED_PII = {"none", "pseudonymous"}
PSEUDONYMOUS_KEY_RE = re.compile(r"[0-9a-f]{20}")


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

    # An empty table "passes" every row check, so it has to fail here.
    if not features:
        violations.append("churn_features has no rows")
    for col in contract["columns"]:
        if col.get("pii") not in ALLOWED_PII:
            violations.append(f"contract column '{col['name']}' declares pii={col.get('pii')!r}; "
                              f"allowed: {sorted(ALLOWED_PII)}")

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
        # csv.DictReader files values beyond the header under the key None;
        # they belong to no column, so no column check would ever see them.
        if row.get(None):
            violations.append(f"line {line_no}: {len(row[None])} value(s) beyond the header")
        for col in contract["columns"]:
            name = col["name"]
            value = row.get(name)
            if value is None or value == "":
                if not col["nullable"]:
                    violations.append(f"line {line_no}: '{name}' is null but not nullable")
                continue
            if not TYPE_CHECKS[col["type"]](value):
                violations.append(f"line {line_no}: '{name}'={value!r} is not a valid {col['type']}")
            # A "pseudonymous" column must hold a key, never the raw ID.
            elif col.get("pii") == "pseudonymous" and not PSEUDONYMOUS_KEY_RE.fullmatch(value):
                violations.append(f"line {line_no}: '{name}' is not a 20-hex pseudonymous key")
        key = row.get(pk)
        if key in seen_keys:
            violations.append(f"line {line_no}: duplicate primary key {pk}={key}")
        seen_keys.add(key)
    return violations


# --------------------------------------------------------------------------
# POL-005: agent PRs cannot touch Red-zone paths (ADR-004)
# --------------------------------------------------------------------------

# The code that enforces privacy and governance, and the files that prove
# it works. An agent may propose changes anywhere else, but never here: a
# plausible-looking edit to the redactor, the validator or the contract
# could switch off the very checks that would catch it. The same goes for
# the agent's own build rules (CLAUDE.md), the existing tests and fixtures
# that prove the checks fail, the demo that shows them, and .gitignore,
# which keeps PII-bearing raw/ out of git. New test files elsewhere in
# tests/ stay Green: ADR-004 lets an agent add test scaffolding, just not
# rewrite the proofs that already exist. Entries ending in "/" cover a folder.
RED_ZONE = (
    "governance.py", "policies.py", "config.py", "contracts/", ".github/",
    "CLAUDE.md", "tests/fixtures/", "tests/test_policies.py", "demo.py", ".gitignore",
)
# Feature logic and chunking parameters (ADR-004 Yellow zone). The zone is
# decided by which files change, not by a label the PR's author chooses.
YELLOW_ZONE = ("pipeline.py",)
YELLOW_LABEL = "yellow"
YELLOW_MIN_APPROVALS = 2
# Reviewer names that are clearly not a person. Their approvals don't count.
BOT_NAME_RE = re.compile(r"(?:^|[^a-z])(?:bot|agent|claude|copilot|gpt|codex)(?:$|[^a-z])|\[bot\]")


def in_zone(path, zone):
    """True if a changed file is one of the zone's files or inside one of its folders."""
    path = path.replace("\\", "/").removeprefix("./")
    return any(path.startswith(entry) if entry.endswith("/") else path == entry
               for entry in zone)


def in_red_zone(path):
    return in_zone(path, RED_ZONE)


def is_agent_pr(pr):
    """Only a PR that says exactly "human" is treated as human-written.

    Fail closed: "Agent", "bot", a typo or a missing field all count as an
    agent, so the wording of one field can't switch the check off.
    """
    return str(pr.get("author_type", "")).strip().lower() != "human"


def counted_approvers(pr):
    """Approvals from distinct humans, plus the ones that were not counted.

    An approval doesn't count if it comes from a bot-like name, from the
    PR's author, or from the person who prompted the agent (ADR-004: the
    approver must not be the one who asked for the change).
    """
    excluded = {str(pr.get(k, "")).strip().lower() for k in ("author", "prompted_by")} - {""}
    counted, rejected = set(), []
    for name in pr.get("human_approvals", []):
        key = str(name).strip().lower()
        if not key or key in excluded or BOT_NAME_RE.search(key):
            rejected.append(str(name))
        else:
            counted.add(key)  # set(): the same reviewer twice is still one person
    return counted, rejected


def pr_shape_violations(pr):
    """Problems with the PR file itself, before any zone can be judged.

    Fail closed: a missing or malformed file list means we can't tell what
    changed, so the PR can't be approved. (Looping over a string by mistake
    would check one letter at a time and match nothing.)
    """
    if not isinstance(pr, dict):
        return ["PR file is not a JSON object"]
    out = []
    files = pr.get("changed_files")
    if not isinstance(files, list) or not files:
        out.append("PR file: 'changed_files' must be a non-empty list of paths")
    elif not all(isinstance(f, str) and f.strip() for f in files):
        out.append("PR file: every entry in 'changed_files' must be a path string")
    for field in ("labels", "human_approvals"):
        if field in pr and not isinstance(pr[field], list):
            out.append(f"PR file: '{field}' must be a list")
    return out


def pol_005_agent_zones(pr, **_):
    """Agent-authored PRs: never touch Red zone; Yellow needs 2 human approvals.

    Human PRs are not restricted here; they go through normal code review.
    A PR is Yellow if it changes a Yellow-zone file or carries the yellow
    label. A malformed PR file fails for everyone, human or agent.
    """
    shape = pr_shape_violations(pr)
    if shape:
        return shape
    if not is_agent_pr(pr):
        return []
    violations = []
    changed = pr.get("changed_files", [])
    for path in changed:
        if in_red_zone(path):
            violations.append(f"agent PR may not modify Red-zone path {path}")
    labels = [str(label).lower() for label in pr.get("labels", [])]
    is_yellow = YELLOW_LABEL in labels or any(in_zone(p, YELLOW_ZONE) for p in changed)
    approvers, rejected = counted_approvers(pr)
    if is_yellow and len(approvers) < YELLOW_MIN_APPROVALS:
        note = f" (not counted: {', '.join(rejected)})" if rejected else ""
        violations.append(f"Yellow-zone agent PR has {len(approvers)} human approval(s); "
                          f"needs {YELLOW_MIN_APPROVALS}{note}")
    return violations


# --------------------------------------------------------------------------
# Running the rules
# --------------------------------------------------------------------------

RULES = [
    ("POL-001", "No raw PII in vector-layer chunks", pol_001_no_pii_in_vector_layer),
    ("POL-002", "Withdrawn consent excluded from features and chunks", pol_002_consent_respected),
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
    # Only the ID and name columns: POL-001 needs each customer's own name
    # to prove it was redacted. Emails and phones are found by pattern.
    with (raw_dir / "subscribers.csv").open(newline="", encoding="utf-8") as f:
        subscribers = [{k: r[k] for k in ("customer_id", "first_name", "last_name")}
                       for r in csv.DictReader(f)]
    contract = json.loads((contracts_dir / "churn_features.json").read_text(encoding="utf-8"))
    return {"features": features, "header": header, "chunks": chunks,
            "consent": consent, "subscribers": subscribers, "contract": contract}


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
