"""Regression tests for the policy validator (policies.py).

Each test builds a tiny, valid in-memory dataset, breaks exactly one thing,
and checks that exactly the expected rule fails. That pins down two
promises at once: every known violation is caught, and a clean dataset
raises no false alarms. The tests need no generated data in raw/ or
products/, so they run in a fresh checkout.

Run from the repo root:
    python -m unittest discover -s tests -v
"""

import copy
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"
# policies.py lives at the repo root, one level above this folder.
sys.path.insert(0, str(ROOT))

import policies as P  # noqa: E402  (import after the path tweak above)

KEY_DANA = P.expected_customer_key("C000001")   # granted everything
KEY_OMAR = P.expected_customer_key("C000002")   # withdrew everything


def clean_context():
    """A minimal dataset that must pass POL-001..004."""
    contract = json.loads((ROOT / "contracts" / "churn_features.json").read_text(encoding="utf-8"))
    feature_row = {
        "customer_key": KEY_DANA, "as_of_date": "2026-07-03", "plan_id": "core_multi",
        "monthly_price_usd": "29.00", "acquisition_channel": "search", "tenure_days": "200",
        "skips_total": "1", "pauses_total": "0", "failed_payments_90d": "0",
        "support_contacts_total": "2", "label_churned_next_90d": "0",
    }
    chunk = {
        "chunk_id": "T0000001-00", "source_transcript_id": "T0000001",
        "customer_key": KEY_DANA, "created_date": "2026-01-15",
        "contact_reason": "shipping", "redaction_version": "regex-v1",
        "text": "Customer: This is [NAME] [NAME], email [EMAIL], phone [PHONE]. "
                "My last box arrived late and the seal was damaged.",
    }
    consent = []
    for cid, status in (("C000001", "granted"), ("C000002", "withdrawn")):
        for purpose in ("analytics_profiling", "ai_support_assistant"):
            consent.append({"customer_id": cid, "purpose": purpose, "status": status,
                            "updated_ts": "2025-01-01T00:00:00Z"})
    return {
        "features": [feature_row],
        "header": [c["name"] for c in contract["columns"]],
        "chunks": [chunk],
        "consent": consent,
        "subscribers": [
            {"customer_id": "C000001", "first_name": "Dana", "last_name": "Reyes"},
            {"customer_id": "C000002", "first_name": "Omar", "last_name": "Ali"},
        ],
        "contract": contract,
        "pr": None,
    }


def failed_rules(ctx):
    return {r["rule"] for r in P.run_rules(ctx) if r["status"] == "FAIL"}


def load_pr(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class DataRuleTests(unittest.TestCase):
    """POL-001..004: each break trips exactly the rule meant to catch it."""

    def setUp(self):
        self.ctx = clean_context()

    def assertOnlyFails(self, *rules):
        self.assertEqual(failed_rules(self.ctx), set(rules))

    def test_clean_dataset_passes(self):
        self.assertOnlyFails()

    # --- the four seeded violations used in the demo ---

    def test_pol001_email_in_chunk_text(self):
        self.ctx["chunks"][0]["text"] += " Reach me at jordan.olson.9999@example.com."
        self.assertOnlyFails("POL-001")

    def test_pol002_feature_row_without_profiling_consent(self):
        self.ctx["features"].append({**self.ctx["features"][0], "customer_key": KEY_OMAR})
        self.assertOnlyFails("POL-002")

    def test_pol003_chunk_older_than_retention(self):
        self.ctx["chunks"][0]["created_date"] = "2024-04-14"
        self.assertOnlyFails("POL-003")

    def test_pol004_unannounced_email_column(self):
        self.ctx["header"].append("email")
        self.assertOnlyFails("POL-004")

    # --- gaps closed after the self-review ---

    def test_pol001_seven_digit_phone(self):
        self.ctx["chunks"][0]["text"] += " Call me on 555-0147."
        self.assertOnlyFails("POL-001")

    def test_pol001_obfuscated_email_and_international_phone(self):
        self.ctx["chunks"][0]["text"] += " x@ example . com or +1 612.555.0147"
        self.assertOnlyFails("POL-001")

    def test_pol001_customers_own_name(self):
        self.ctx["chunks"][0]["text"] += " This is Dana Reyes."
        self.assertOnlyFails("POL-001")

    def test_pol001_own_name_any_case(self):
        self.ctx["chunks"][0]["text"] += " ask for reyes"
        self.assertOnlyFails("POL-001")

    def test_pol001_unexpected_chunk_field(self):
        self.ctx["chunks"][0]["customer_email"] = "dana.reyes.0001@example.com"
        self.assertOnlyFails("POL-001")

    def test_pol001_pii_in_structured_field(self):
        self.ctx["chunks"][0]["contact_reason"] = "dana@example.com"
        self.assertOnlyFails("POL-001")

    def test_pol001_missing_chunk_field(self):
        del self.ctx["chunks"][0]["redaction_version"]
        self.assertOnlyFails("POL-001")

    def test_pol001_empty_corpus(self):
        self.ctx["chunks"] = []
        self.assertOnlyFails("POL-001")

    def test_pol002_chunk_without_assistant_consent(self):
        self.ctx["chunks"].append({**self.ctx["chunks"][0], "chunk_id": "T0000002-00",
                                   "customer_key": KEY_OMAR})
        self.assertOnlyFails("POL-002")

    def test_pol002_key_with_no_consent_record(self):
        self.ctx["features"].append({**self.ctx["features"][0],
                                     "customer_key": P.expected_customer_key("C999999")})
        self.assertOnlyFails("POL-002")

    def test_pol003_future_dated_chunk(self):
        self.ctx["chunks"][0]["created_date"] = "2027-01-01"
        self.assertOnlyFails("POL-003")

    def test_pol003_unreadable_date(self):
        self.ctx["chunks"][0]["created_date"] = "2026-02-30"
        self.assertOnlyFails("POL-003")

    def test_pol004_empty_feature_table(self):
        self.ctx["features"] = []
        self.assertOnlyFails("POL-004")

    def test_pol004_value_beyond_header(self):
        # What csv.DictReader produces for a row with one extra value.
        self.ctx["features"][0][None] = ["dana.reyes.0001@example.com"]
        self.assertOnlyFails("POL-004")

    def test_pol004_contract_declares_direct_pii(self):
        self.ctx["contract"]["columns"][2]["pii"] = "direct"
        self.assertOnlyFails("POL-004")

    def test_pol004_bad_type(self):
        self.ctx["features"][0]["tenure_days"] = "abc"
        self.assertOnlyFails("POL-004")

    def test_pol004_null_in_non_nullable_column(self):
        self.ctx["features"][0]["plan_id"] = ""
        self.assertOnlyFails("POL-004")

    def test_pol004_duplicate_primary_key(self):
        self.ctx["features"].append(dict(self.ctx["features"][0]))
        self.assertOnlyFails("POL-004")

    def test_raw_customer_id_as_key_fails_contract_and_consent(self):
        self.ctx["features"][0]["customer_key"] = "C000001"
        self.assertOnlyFails("POL-002", "POL-004")


class PullRequestRuleTests(unittest.TestCase):
    """POL-005: who may change what."""

    def assertBlocked(self, pr):
        self.assertTrue(P.pol_005_agent_zones(pr), f"expected a violation for {pr}")

    def assertAllowed(self, pr):
        self.assertEqual(P.pol_005_agent_zones(pr), [])

    # --- the committed fixtures, exactly as the demo uses them ---

    def test_fixture_green_passes(self):
        self.assertAllowed(load_pr("pr_agent_green.json"))

    def test_fixture_red_blocked(self):
        self.assertBlocked(load_pr("pr_agent_red.json"))

    def test_fixture_yellow_one_approval_blocked(self):
        self.assertBlocked(load_pr("pr_agent_yellow.json"))

    def test_fixture_yellow_two_approvals_passes(self):
        self.assertAllowed(load_pr("pr_agent_yellow_approved.json"))

    def test_fixture_through_run_rules(self):
        ctx = clean_context()
        ctx["pr"] = load_pr("pr_agent_red.json")
        self.assertEqual(failed_rules(ctx), {"POL-005"})

    # --- zones ---

    def test_every_red_zone_entry_is_blocked(self):
        for entry in P.RED_ZONE:
            path = entry + "example.txt" if entry.endswith("/") else entry
            with self.subTest(path=path):
                self.assertBlocked({"author_type": "agent", "changed_files": [path]})

    def test_red_zone_path_spelled_differently(self):
        for path in ("./config.py", ".github\\workflows\\ci.yml", "tests/fixtures/pr_agent_red.json"):
            with self.subTest(path=path):
                self.assertBlocked({"author_type": "agent", "changed_files": [path]})

    def test_new_test_file_is_green(self):
        # ADR-004: agents may add test scaffolding, but not edit existing proofs.
        self.assertAllowed({"author_type": "agent", "changed_files": ["tests/test_new_check.py"]})

    def test_similar_name_outside_red_zone_allowed(self):
        self.assertAllowed({"author_type": "agent", "changed_files": ["contracts_old.md"]})

    def test_yellow_by_path_without_label(self):
        self.assertBlocked({"author_type": "agent", "changed_files": ["pipeline.py"]})

    def test_yellow_by_path_with_two_humans_passes(self):
        self.assertAllowed({"author_type": "agent", "changed_files": ["pipeline.py"],
                            "human_approvals": ["a.person", "b.person"]})

    # --- who wrote it, who approved it ---

    def test_author_type_variants_count_as_agent(self):
        for author_type in ("Agent", "bot", "AI", ""):
            with self.subTest(author_type=author_type):
                self.assertBlocked({"author_type": author_type, "changed_files": ["governance.py"]})

    def test_missing_author_type_counts_as_agent(self):
        self.assertBlocked({"changed_files": ["governance.py"]})

    def test_human_pr_not_restricted(self):
        self.assertAllowed({"author_type": "Human", "changed_files": ["governance.py"]})

    def test_same_reviewer_twice_counts_once(self):
        self.assertBlocked({"author_type": "agent", "changed_files": ["pipeline.py"],
                            "human_approvals": ["retention.lead", "Retention.Lead"]})

    def test_bot_and_agent_approvals_not_counted(self):
        for bot in ("claude-agent", "dependabot[bot]", "copilot"):
            with self.subTest(bot=bot):
                self.assertBlocked({"author_type": "agent", "changed_files": ["pipeline.py"],
                                    "human_approvals": [bot, "retention.lead"]})

    def test_prompter_approval_not_counted(self):
        self.assertBlocked({"author_type": "agent", "prompted_by": "retention.lead",
                            "changed_files": ["pipeline.py"],
                            "human_approvals": ["retention.lead", "analytics.engineer"]})

    # --- malformed PR files fail closed ---

    def test_missing_changed_files(self):
        self.assertBlocked({"author_type": "agent"})

    def test_changed_files_as_string(self):
        self.assertBlocked({"author_type": "agent", "changed_files": "governance.py"})

    def test_empty_changed_files(self):
        self.assertBlocked({"author_type": "agent", "changed_files": []})

    def test_non_string_path(self):
        self.assertBlocked({"author_type": "agent", "changed_files": [42]})

    def test_approvals_not_a_list(self):
        self.assertBlocked({"author_type": "agent", "changed_files": ["pipeline.py"],
                            "human_approvals": "retention.lead, analytics.engineer"})

    def test_malformed_file_fails_even_for_humans(self):
        self.assertBlocked({"author_type": "human", "changed_files": "governance.py"})

    def test_pr_not_an_object(self):
        self.assertBlocked(["governance.py"])


if __name__ == "__main__":
    unittest.main()
