"""Interactive walkthrough: every policy fails on a seeded violation, then passes.

Press Enter to move to each stage. Every stage runs the real commands
(pipeline.py, policies.py) and shows their real output, so what you see
on screen is exactly what CI would see.

Usage:
    python demo.py              # interactive, for the video
    python demo.py --no-pause   # run straight through (rehearsal / testing)
    python demo.py --no-clear   # keep earlier stages on screen
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent
REPORT = ROOT / "products" / "policy_report.json"

# Each data rule: what gets planted and the real-world mistake it imitates.
DATA_STAGES = [
    ("POL-001", "No raw PII in vector-layer chunks",
     "an email address left in a transcript chunk", "a redaction regression"),
    ("POL-002", "Withdrawn consent excluded from features and chunks",
     "a feature row for a customer who opted out of profiling",
     "a join that drops the consent filter"),
    ("POL-003", "Chunks within the 730-day retention window",
     "a transcript chunk 900 days old", "a backfill that ignores retention"),
    ("POL-004", "Feature table honors its contract",
     "an extra 'email' column in churn_features", "a schema change nobody announced"),
]

# POL-005 pairs: a PR that must be blocked, then the corrected PR that passes.
PR_STAGES = [
    ("Red zone",
     "tests/fixtures/pr_agent_red.json",
     "an AI agent's PR edits governance.py and policies.py",
     "tests/fixtures/pr_agent_green.json",
     "the agent's PR touches only README.md and docs/"),
    ("Yellow zone",
     "tests/fixtures/pr_agent_yellow.json",
     "an AI agent's Yellow PR has only 1 human approval",
     "tests/fixtures/pr_agent_yellow_approved.json",
     "the same PR after a second human approves it"),
]


# --------------------------------------------------------------------------
# Screen helpers
# --------------------------------------------------------------------------

class Style:
    """ANSI colours, switched off when the terminal can't show them."""
    on = True

    @classmethod
    def wrap(cls, code, text):
        return f"\033[{code}m{text}\033[0m" if cls.on else text


def green(t): return Style.wrap("32;1", t)
def red(t): return Style.wrap("31;1", t)
def yellow(t): return Style.wrap("33", t)
def bold(t): return Style.wrap("1", t)
def dim(t): return Style.wrap("2", t)


def colourize(line):
    """Colour PASS/FAIL lines so the result is readable from across a room."""
    stripped = line.lstrip()
    if stripped.startswith(("PASS", "RESULT: PASS")):
        return green(line)
    if stripped.startswith(("FAIL", "RESULT: FAIL")):
        return red(line)
    if stripped.startswith("- "):
        return red(line)
    if stripped.startswith("SKIP"):
        return dim(line)
    if "SEEDED RUN" in line or "<- imitates" in line:
        return yellow(line)
    return line


class Demo:
    def __init__(self, pause, clear):
        self.pause_enabled = pause
        self.clear_enabled = clear
        self.stage_no = 0
        # intro + (break, fix) per data rule + (block, corrected) per PR case + summary
        self.total_stages = 1 + 2 * len(DATA_STAGES) + 2 * len(PR_STAGES) + 1
        self.outcomes = []  # (label, expected, actual) for the summary

    def stage(self, title, subtitle=""):
        """Start a new screen with a numbered header."""
        self.stage_no += 1
        if self.clear_enabled:
            os.system("cls" if os.name == "nt" else "clear")
        bar = "=" * 72
        print(bold(bar))
        print(bold(f"  Stage {self.stage_no} of {self.total_stages}:  {title}"))
        if subtitle:
            print(f"  {subtitle}")
        print(bold(bar))
        print()

    def wait(self, prompt="Press Enter to continue"):
        if not self.pause_enabled:
            print()
            return
        try:
            input(dim(f"\n  >> {prompt} ... "))
        except (EOFError, KeyboardInterrupt):
            print("\nDemo stopped.")
            sys.exit(130)

    def run(self, *args, show=True):
        """Run one of the project's scripts, echo the command and its output.

        Uses the same Python interpreter that is running the demo, so it
        works the same on Windows, macOS and Linux.
        """
        shown = "python " + " ".join(args)
        if show:
            print(bold(f"$ {shown}"))
        proc = subprocess.run([sys.executable, *args], cwd=ROOT,
                              capture_output=True, text=True)
        if show:
            for line in (proc.stdout + proc.stderr).rstrip().splitlines():
                print(colourize(line))
        return proc.returncode

    def verdict(self, code, expect_block):
        """Explain the exit code in business terms and record the outcome."""
        print()
        # The exit code is the gate's decision; CI or the orchestrator acts on it.
        if code == 0:
            print(green("  exit code 0  ->  GATE OPEN: safe to publish / the PR can merge"))
        else:
            print(red(f"  exit code {code}  ->  GATE CLOSED: publish blocked / the PR cannot merge"))
        ok = (code != 0) == expect_block
        if not ok:
            print(yellow("  !! Unexpected result for this stage - check the output above."))
        return ok


def rule_statuses():
    """Read which rules failed from the validator's own report file."""
    report = json.loads(REPORT.read_text(encoding="utf-8"))
    return {r["rule"]: r["status"] for r in report["rules"]}


# --------------------------------------------------------------------------
# Stages
# --------------------------------------------------------------------------

def intro(demo):
    demo.stage("Build clean data and validate",
               "Synthetic data (seed 42), then the pipeline, then the policy validator.")
    print("Claim: governance rules are executable, run on every change, and block a bad")
    print("change no matter who - or what - wrote it.\n")
    demo.run("generate_data.py")
    print()
    demo.run("pipeline.py")
    print()
    code = demo.run("policies.py")
    demo.outcomes.append(("Clean build", "PASS", demo.verdict(code, expect_block=False)))
    demo.wait("Press Enter to break POL-001")


def data_rule(demo, rule, title, planted, imitates, next_label):
    # Break it.
    demo.stage(f"{rule} - {title}: BREAK IT",
               f"Plant {planted}  (imitates {imitates}).")
    demo.run("pipeline.py", "--seed-violation", rule)
    print()
    code = demo.run("policies.py")
    failed = sorted(r for r, s in rule_statuses().items() if s == "FAIL")
    caught = demo.verdict(code, expect_block=True) and failed == [rule]
    if failed != [rule]:
        print(yellow(f"  !! Expected only {rule} to fail; failed: {failed or 'none'}"))
    demo.outcomes.append((f"{rule} seeded", "FAIL", caught))
    demo.wait(f"Press Enter to remove the violation and rerun {rule}")

    # Fix it.
    demo.stage(f"{rule} - {title}: FIX IT",
               "Rebuild without the seeded violation and validate again.")
    demo.run("pipeline.py")
    print()
    code = demo.run("policies.py")
    demo.outcomes.append((f"{rule} removed", "PASS", demo.verdict(code, expect_block=False)))
    demo.wait(f"Press Enter to {next_label}")


def pr_rule(demo, zone, bad_pr, bad_desc, good_pr, good_desc, next_label):
    demo.stage(f"POL-005 - {zone}: BLOCK THE AGENT",
               f"Scenario: {bad_desc}.")
    code = demo.run("policies.py", "--pr", bad_pr)
    blocked = demo.verdict(code, expect_block=True) and rule_statuses().get("POL-005") == "FAIL"
    demo.outcomes.append((f"POL-005 {zone} PR", "FAIL", blocked))
    demo.wait("Press Enter to show the corrected PR")

    demo.stage(f"POL-005 - {zone}: CORRECTED PR",
               f"Scenario: {good_desc}.")
    code = demo.run("policies.py", "--pr", good_pr)
    demo.outcomes.append((f"POL-005 {zone} corrected", "PASS",
                          demo.verdict(code, expect_block=False)))
    demo.wait(f"Press Enter to {next_label}")


def summary(demo):
    demo.stage("Summary", "Every result below was read from a real exit code.")
    width = max(len(label) for label, _, _ in demo.outcomes)
    for label, expected, ok in demo.outcomes:
        mark = green("as expected") if ok else red("UNEXPECTED")
        shown = red(expected) if expected == "FAIL" else green(expected)
        print(f"  {label:<{width}}   {shown}   {mark}")
    print()
    all_ok = all(ok for _, _, ok in demo.outcomes)
    caught = sum(1 for label, exp, ok in demo.outcomes if exp == "FAIL" and ok)
    total = sum(1 for _, exp, _ in demo.outcomes if exp == "FAIL")
    print(bold(f"  {caught} of {total} seeded violations blocked; "
               f"every one passed again once removed." if all_ok
               else "  Some stages did not behave as expected - see above."))
    print(dim("\n  products/ has been left in a clean, passing state."))
    return 0 if all_ok else 1


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--no-pause", action="store_true", help="don't wait for Enter")
    parser.add_argument("--no-clear", action="store_true", help="don't clear the screen per stage")
    parser.add_argument("--no-color", action="store_true", help="plain text output")
    return parser.parse_args()


def main():
    args = parse_args()
    Style.on = not args.no_color and sys.stdout.isatty()
    if os.name == "nt":
        os.system("")  # enables ANSI colour codes in the Windows console
    demo = Demo(pause=not args.no_pause, clear=not args.no_clear)

    intro(demo)
    for i, (rule, title, planted, imitates) in enumerate(DATA_STAGES):
        is_last = i == len(DATA_STAGES) - 1
        nxt = "move on to AI-agent pull requests (POL-005)" if is_last \
            else f"break {DATA_STAGES[i + 1][0]}"
        data_rule(demo, rule, title, planted, imitates, nxt)
    for i, stage in enumerate(PR_STAGES):
        nxt = "see the summary" if i == len(PR_STAGES) - 1 else "test a Yellow-zone PR"
        pr_rule(demo, *stage, next_label=nxt)
    return summary(demo)


if __name__ == "__main__":
    sys.exit(main())
