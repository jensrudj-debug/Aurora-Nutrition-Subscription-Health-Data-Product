"""Generate synthetic source data for the Subscription Health proof artifact.

Writes five files to raw/ that stand in for Aurora Nutrition's source systems
(subscription platform, billing processor, consent service, support desk),
plus raw/_manifest.json describing the run.

Every value is fake and reproducible: the same --seed always produces
byte-identical files, so anyone can regenerate the data and get our results.

Usage:
    python generate_data.py                  # 2,000 customers, seed 42
    python generate_data.py --customers 5000 --seed 7
"""

import argparse
import csv
import json
import random
from datetime import datetime, time, timedelta, timezone

from config import AS_OF_DATE, RAW_DIR

GENERATOR_VERSION = "1.0.0"
# The as-of date comes from config.py so the generator, pipeline and
# validator can never disagree about what "today" is.
AS_OF = datetime.combine(AS_OF_DATE, time(0, 0), tzinfo=timezone.utc)
CYCLE_DAYS = 30

PLANS = {
    "core_multi": 29,
    "protein_plus": 49,
    "sleep_stack": 39,
    "bundle_all": 89,
}

# Weighted so MN and CA are well represented; the compliance matrix has to
# hold up for customers in states with their own privacy laws.
STATES = {
    "MN": 20, "CA": 20, "TX": 10, "NY": 10, "FL": 8,
    "WI": 8, "IL": 8, "WA": 6, "CO": 5, "AZ": 5,
}
AREA_CODES = ["612", "651", "213", "415", "512", "212", "305", "414", "312", "206"]
CHANNELS = ["paid_social", "search", "influencer", "referral", "organic"]

FIRST_NAMES = [
    "Dana", "Jordan", "Alex", "Morgan", "Taylor", "Casey", "Riley", "Jamie",
    "Avery", "Quinn", "Priya", "Mateo", "Lena", "Omar", "Grace", "Hiro",
    "Sofia", "Andre", "Maya", "Noah", "Elena", "Kofi", "Ingrid", "Luis",
]
LAST_NAMES = [
    "Reyes", "Olson", "Nguyen", "Johnson", "Patel", "Garcia", "Larson",
    "Kim", "Brooks", "Hansen", "Okafor", "Silva", "Peterson", "Chen",
    "Martinez", "Lindqvist", "Ali", "Novak", "Schmidt", "Rivera",
]

# Hazard model constants. Tuned so roughly 5% of subscribers who are active
# at the start of a 30-day cycle cancel during it (the measured rate is
# printed and saved in the manifest, so we report reality, not the target).
P_FAIL_BASE, P_FAIL_RISK = 0.02, 0.15
P_SKIP_BASE, P_SKIP_RISK = 0.03, 0.20
P_PAUSE_BASE, P_PAUSE_RISK = 0.01, 0.08
P_CONTACT_BASE, P_CONTACT_RISK = 0.03, 0.20
P_CANCEL_BASE, P_CANCEL_RISK = 0.012, 0.12
P_CANCEL_FAILED_BUMP = 0.15  # a failed payment is a strong churn signal


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------

def iso(ts):
    """Format a datetime as an ISO-8601 UTC string, e.g. 2026-03-04T10:22:31Z."""
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def random_ts_between(rng, start, end):
    """Return a random timestamp in [start, end), to the second."""
    seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=rng.randrange(max(seconds, 1)))


def fake_phone(rng):
    """Fictional phone number: 555-0100..555-0199 is reserved for fiction.

    Two formats are used on purpose, so redaction can't pass by only
    handling one way of writing a number.
    """
    area = rng.choice(AREA_CODES)
    last_four = f"01{rng.randrange(100):02d}"
    if rng.random() < 0.7:
        return f"({area}) 555-{last_four}"
    return f"{area}-555-{last_four}"


def payment_token(rng):
    """Opaque processor token. Card numbers are never generated (PCI scope)."""
    return "tok_" + "".join(rng.choice("0123456789abcdef") for _ in range(16))


# --------------------------------------------------------------------------
# Subscribers
# --------------------------------------------------------------------------

def make_subscriber(rng, n):
    """Build one subscriber row plus their hidden churn-risk score.

    The risk score is returned separately and never written to disk:
    the ML model must learn churn from behaviour, not read the answer.
    """
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    plan_id = rng.choice(list(PLANS))
    signup_day = AS_OF.date() - timedelta(days=rng.randint(30, 900))
    signup_ts = random_ts_between(
        rng,
        datetime.combine(signup_day, time(0, 0), tzinfo=timezone.utc),
        datetime.combine(signup_day, time(23, 59, 59), tzinfo=timezone.utc),
    )
    row = {
        "customer_id": f"C{n:06d}",
        "first_name": first,
        "last_name": last,
        # The customer number keeps emails unique; example.com is reserved
        # by RFC 2606 so these can never reach a real inbox.
        "email": f"{first.lower()}.{last.lower()}.{n:04d}@example.com",
        "phone": fake_phone(rng),
        "state": rng.choices(list(STATES), weights=list(STATES.values()))[0],
        "plan_id": plan_id,
        "monthly_price_usd": PLANS[plan_id],
        "acquisition_channel": rng.choice(CHANNELS),
        "signup_date": signup_day.isoformat(),
    }
    # Beta(2, 5): most customers are low-to-moderate risk, a long tail is high.
    risk = rng.betavariate(2, 5)
    return row, risk, signup_ts


# --------------------------------------------------------------------------
# Transcripts
# --------------------------------------------------------------------------

# Each reason maps to the customer's complaint and the agent's resolution,
# so the RAG corpus later contains "problem + what worked" pairs.
DIALOGUE = {
    "price": (
        "Honestly it's getting too expensive for me every month.",
        "I can move you to a lower-cost plan or apply 15% off your next three orders.",
    ),
    "taste": (
        "I really don't like the flavour of the powder anymore.",
        "Let's swap you to the unflavoured version and send a sample pack free.",
    ),
    "shipping": (
        "My last box arrived late and the seal was damaged.",
        "I'm sorry about that, I'll ship a replacement today with expedited delivery.",
    ),
    "too_much_product": (
        "I have a pile of unopened bottles, I can't keep up.",
        "I can switch you to every-other-month delivery so you can catch up.",
    ),
    "billing": (
        "My card payment failed and I don't want to lose my order.",
        "I can retry the charge now and keep your shipment on schedule.",
    ),
    "happy": (
        "Just calling to say the sleep stack has really helped me.",
        "That's great to hear, I'll pass that on and add a loyalty credit to your account.",
    ),
}


def pick_contact_reason(rng, risk, payment_failed):
    """Choose why the customer called, shaped by their situation.

    A failed payment usually triggers a billing call; risky customers
    complain more; low-risk customers are the ones who call to say thanks.
    """
    if payment_failed and rng.random() < 0.6:
        return "billing"
    weights = {
        "price": 1 + 4 * risk,
        "too_much_product": 1 + 3 * risk,
        "taste": 1 + 2 * risk,
        "shipping": 2,
        "billing": 1,
        "happy": 3 * (1 - risk),
    }
    return rng.choices(list(weights), weights=list(weights.values()))[0]


def make_transcript_text(sub, reason):
    """Short agent/customer dialogue in which the customer states their PII.

    Customers really do read out their email and phone to verify identity,
    which is exactly why redaction must run before anything is embedded.
    """
    complaint, resolution = DIALOGUE[reason]
    full_name = f"{sub['first_name']} {sub['last_name']}"
    lines = [
        "Agent: Thanks for contacting Aurora Nutrition, who am I speaking with?",
        f"Customer: This is {full_name}, email {sub['email']}, phone {sub['phone']}.",
        "Agent: Thanks, I've found your account. How can I help today?",
        f"Customer: {complaint}",
        f"Agent: {resolution}",
        "Customer: Okay, that works for me. Thanks.",
    ]
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Lifecycle simulation
# --------------------------------------------------------------------------

def simulate_customer(rng, sub, risk, signup_ts, stats):
    """Walk one customer through 30-day cycles until cancel or the as-of date.

    Returns (events, billing, transcripts) for this customer. Anything that
    would fall after the customer's cancellation or after AS_OF is dropped,
    because source systems cannot record the future.
    """
    events = [_event(sub, "subscription_created", signup_ts)]
    billing, transcripts = [], []
    cycle_start = signup_ts

    while cycle_start < AS_OF:
        cycle_end = cycle_start + timedelta(days=CYCLE_DAYS)
        stats["active_cycles"] += 1
        paused = rng.random() < P_PAUSE_BASE + P_PAUSE_RISK * risk

        # Billing happens early in the cycle; a paused cycle is not charged.
        failed = False
        bill_ts = random_ts_between(rng, cycle_start, cycle_start + timedelta(days=1))
        if not paused and bill_ts < AS_OF:
            failed = rng.random() < P_FAIL_BASE + P_FAIL_RISK * risk
            billing.append(_billing(rng, sub, bill_ts, failed))

        # Decide on cancellation first, so every other event this cycle can
        # be kept only if it happened before the customer left.
        cancel_ts = None
        p_cancel = P_CANCEL_BASE + P_CANCEL_RISK * risk
        if failed:
            p_cancel += P_CANCEL_FAILED_BUMP
        if rng.random() < p_cancel:
            earliest = bill_ts if not paused else cycle_start
            cancel_ts = random_ts_between(rng, earliest, cycle_end)
            if cancel_ts >= AS_OF:
                cancel_ts = None  # would happen after the as-of date: still active
        cutoff = min(cancel_ts or AS_OF, AS_OF)

        cycle_events = []
        if paused:
            cycle_events.append(("pause", random_ts_between(rng, cycle_start, cycle_end)))
        elif rng.random() < P_SKIP_BASE + P_SKIP_RISK * risk:
            cycle_events.append(("skip_shipment", random_ts_between(rng, bill_ts, cycle_end)))
        for event_type, ts in cycle_events:
            if ts < cutoff:
                events.append(_event(sub, event_type, ts))

        if rng.random() < P_CONTACT_BASE + P_CONTACT_RISK * risk:
            contact_ts = random_ts_between(rng, cycle_start, cycle_end)
            if contact_ts < cutoff:
                reason = pick_contact_reason(rng, risk, failed)
                transcripts.append(_transcript(sub, contact_ts, reason))

        if cancel_ts is not None:
            events.append(_event(sub, "cancel", cancel_ts))
            stats["cancels"] += 1
            break
        cycle_start = cycle_end

    return events, billing, transcripts


def _event(sub, event_type, ts):
    """One subscription_events row (event_id is assigned after sorting)."""
    return {
        "event_id": None,
        "customer_id": sub["customer_id"],
        "event_type": event_type,
        "event_ts": iso(ts),
        "plan_id": sub["plan_id"],
    }


def _billing(rng, sub, ts, failed):
    """One billing.csv row: amount, status and an opaque token, no card data."""
    return {
        "billing_id": None,
        "customer_id": sub["customer_id"],
        "billed_ts": iso(ts),
        "amount_usd": f"{sub['monthly_price_usd']:.2f}",
        "status": "failed" if failed else "succeeded",
        "payment_token": payment_token(rng),
    }


def _transcript(sub, ts, reason):
    """One transcripts.jsonl record."""
    return {
        "transcript_id": None,
        "customer_id": sub["customer_id"],
        "created_date": ts.date().isoformat(),
        "contact_reason": reason,
        "text": make_transcript_text(sub, reason),
    }


# --------------------------------------------------------------------------
# Consent
# --------------------------------------------------------------------------

CONSENT_GRANT_RATES = {"analytics_profiling": 0.92, "ai_support_assistant": 0.85}


def make_consent(rng, sub, signup_ts):
    """One consent row per purpose, as the consent service would record it.

    Granted consent is stamped at signup; a withdrawal is stamped at some
    later moment, because customers opt out after they have joined.
    """
    rows = []
    for purpose, grant_rate in CONSENT_GRANT_RATES.items():
        granted = rng.random() < grant_rate
        ts = signup_ts if granted else random_ts_between(rng, signup_ts, AS_OF)
        rows.append({
            "customer_id": sub["customer_id"],
            "purpose": purpose,
            "status": "granted" if granted else "withdrawn",
            "updated_ts": iso(ts),
        })
    return rows


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

def assign_ids(rows, id_field, prefix, sort_key):
    """Sort rows deterministically, then number them, so IDs follow time order."""
    rows.sort(key=sort_key)
    for i, row in enumerate(rows, start=1):
        row[id_field] = f"{prefix}{i:07d}"
    return rows


def write_csv(path, rows, fieldnames):
    """Write rows to CSV with a fixed column order."""
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path, rows):
    """Write one JSON object per line, the usual format for transcript exports."""
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--customers", type=int, default=2000,
                        help="number of subscribers to generate (default 2000)")
    parser.add_argument("--seed", type=int, default=42,
                        help="random seed for reproducibility (default 42)")
    return parser.parse_args()


def main():
    args = parse_args()
    # One seeded generator for everything: never the global random module or
    # uuid4, either of which would make reruns produce different data.
    rng = random.Random(args.seed)
    stats = {"active_cycles": 0, "cancels": 0}

    subscribers, events, billing, transcripts, consent = [], [], [], [], []
    for n in range(1, args.customers + 1):
        sub, risk, signup_ts = make_subscriber(rng, n)
        subscribers.append(sub)
        e, b, t = simulate_customer(rng, sub, risk, signup_ts, stats)
        events += e
        billing += b
        transcripts += t
        consent += make_consent(rng, sub, signup_ts)

    assign_ids(events, "event_id", "E", lambda r: (r["event_ts"], r["customer_id"]))
    assign_ids(billing, "billing_id", "B", lambda r: (r["billed_ts"], r["customer_id"]))
    assign_ids(transcripts, "transcript_id", "T",
               lambda r: (r["created_date"], r["customer_id"]))

    RAW_DIR.mkdir(exist_ok=True)
    write_csv(RAW_DIR / "subscribers.csv", subscribers, list(subscribers[0]))
    write_csv(RAW_DIR / "subscription_events.csv", events,
              ["event_id", "customer_id", "event_type", "event_ts", "plan_id"])
    write_csv(RAW_DIR / "billing.csv", billing,
              ["billing_id", "customer_id", "billed_ts", "amount_usd", "status", "payment_token"])
    write_csv(RAW_DIR / "consent.csv", consent, list(consent[0]))
    write_jsonl(RAW_DIR / "transcripts.jsonl", transcripts)

    monthly_cancel_rate = stats["cancels"] / stats["active_cycles"]
    manifest = {
        "generator": "generate_data.py",
        "generator_version": GENERATOR_VERSION,
        "seed": args.seed,
        "as_of_date": AS_OF.date().isoformat(),
        "customers": args.customers,
        "row_counts": {
            "subscribers.csv": len(subscribers),
            "subscription_events.csv": len(events),
            "billing.csv": len(billing),
            "consent.csv": len(consent),
            "transcripts.jsonl": len(transcripts),
        },
        "measured_monthly_cancel_rate": round(monthly_cancel_rate, 4),
        "active_at_as_of": args.customers - stats["cancels"],
        "note": "All records are synthetic. Hidden churn-risk scores are never written.",
    }
    (RAW_DIR / "_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n",
                                            encoding="utf-8")

    print(f"Wrote {RAW_DIR}")
    for name, count in manifest["row_counts"].items():
        print(f"  {name:<26} {count:>7,} rows")
    print(f"  monthly cancel rate        {monthly_cancel_rate:>7.2%}")
    print(f"  active at {manifest['as_of_date']}      {manifest['active_at_as_of']:>7,}")


if __name__ == "__main__":
    main()
