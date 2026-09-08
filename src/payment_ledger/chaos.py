"""Phase 5: the six failures, injected on purpose.

Section 6 of the design lists six ways a payment pipeline goes wrong and, for each, the thing that
is supposed to catch it. Up to here every one of them was an argument. This module turns each into
an input a run can be replayed from, and the suite around it turns the claim into a verdict.

**A scenario perturbs one side and not the other.** That is the whole mechanism. The generator
emits three artifacts: the webhook stream, the balance transaction list, and the balance the
processor reports. A scenario damages one of them and leaves the rest alone, which is what makes a
divergence appear where a real one would. Damaging all three consistently would produce a run that
is wrong and reconciles, which is exactly the failure nobody catches.

**The expectation is written down before the run, not read off it afterwards.** Every scenario
declares which dbt tests must fail, how many rows each must fail on where that is predictable, and
what has to be true of the row counts. `verify` compares that against what the build actually did,
and it fails both ways: a scenario nobody caught fails, and so does a scenario that broke something
it was not supposed to touch. A chaos suite that only checks "something went wrong" tells you
nothing about whether the right thing went wrong.

**Waves, because one of the six is a matter of timing.** A late arrival is not a corrupt payload,
it is a correct payload that shows up after the day it belongs to was closed. So a scenario is a
list of waves rather than a single set of files, and the runner takes the pipeline through each in
turn. Five of the six need one wave. The late one needs two, and the second is the interesting one.

Everything here stays simulated. A scenario is a deterministic function of the baseline artifacts
and a seed, so a run that caught something can be reproduced exactly, which is the difference
between a chaos suite and an anecdote.
"""

from __future__ import annotations

import argparse
import copy
import json
import random
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from payment_ledger import config
from payment_ledger.generator import walk_daily_balance

ARTIFACTS = ("events", "balance_transactions", "daily_balance")

# What the report job prefixes its measurements with. Spark writes a good deal of its own output to
# the same stream, so the numbers announce themselves rather than being whatever the last line was.
# `jobs/chaos_report.py` cannot import this (only jobs/ and conf/ are mounted into that container),
# so the two are kept in step by a test instead.
MARKER = "CHAOS-REPORT"

# How much of the run a scenario damages. Small on purpose: a scenario that corrupts half the data
# proves only that the pipeline notices catastrophe, and the failure worth catching is the one that
# would otherwise be lost in the aggregate.
DAMAGE_RATE = 0.02
MINIMUM_DAMAGE = 3


@dataclass
class Wave:
    """One delivery of inputs into the pipeline: what arrives, and what the processor says."""

    events: list[dict]
    balance_transactions: list[dict]
    daily_balance: list[dict]

    def copy(self) -> Wave:
        return Wave(
            events=copy.deepcopy(self.events),
            balance_transactions=copy.deepcopy(self.balance_transactions),
            daily_balance=copy.deepcopy(self.daily_balance),
        )

    def rows(self, artifact: str) -> list[dict]:
        return getattr(self, artifact)


# An injection returns the waves to deliver and a record of what it did to produce them.
Injection = tuple[list[Wave], dict]


@dataclass(frozen=True)
class Scenario:
    """One of the six failures, with the detection it is meant to trip.

    `dbt_failures` maps a test name to the number of rows it must fail on. That number is either an
    integer, or the name of a count the injection reports, which is resolved when the scenario is
    written out, or None where it depends on the calendar rather than on the injection: a balance
    difference is cumulative, so how many days it makes wrong is a function of when it happened.

    A test that fails and is not in this mapping is as much a defect as one that is in it and
    passes. The first means the run broke something the scenario was not aiming at, and the second
    means the detection does not work.
    """

    name: str
    failure: str
    detection: str
    inject: Callable[[Wave, random.Random], Injection]
    dbt_failures: dict[str, int | str | None] = field(default_factory=dict)
    metrics: list[list] = field(default_factory=list)
    # How many deliveries the scenario takes. Declared rather than discovered because the Airflow
    # DAG has to lay out its tasks before anything has been injected, and a run whose shape is only
    # known at execution time cannot be drawn. `build` checks the injector agreed.
    waves: int = 1


def take(items: list, rng: random.Random, rate: float = DAMAGE_RATE) -> list:
    """A deterministic sample, never empty when there is anything to sample.

    A scenario that silently injected nothing would pass its own verification while proving
    nothing at all, so an empty sample from a non-empty population is an error rather than a
    quiet no-op.
    """
    if not items:
        return []
    count = max(MINIMUM_DAMAGE, round(len(items) * rate))
    return rng.sample(sorted(items, key=repr), min(count, len(items)))


def charge_transactions(wave: Wave) -> list[dict]:
    return [t for t in wave.balance_transactions if t.get("reporting_category") == "charge"]


# --- the six ------------------------------------------------------------------------------------


def baseline(wave: Wave, rng: random.Random) -> Injection:
    """No injection. The arm that says what an undamaged run looks like.

    Worth its own scenario rather than assumed: every comparison the other six make is against
    these numbers, and a suite whose control arm is a memory of the last time somebody looked is
    not a control arm.
    """
    return [wave], {"injected": "nothing"}


def duplicate_delivery(wave: Wave, rng: random.Random) -> Injection:
    """The same event delivered twice, which is not a fault but the contract.

    A processor retries a webhook it did not get a 2xx for, and it retries for days. The copy is
    byte identical and carries the same event id, and it arrives later than the original rather
    than beside it, because a redelivery that arrives immediately is the easy case.
    """
    damaged = wave.copy()
    chosen = take([e["id"] for e in damaged.events], rng, rate=0.05)
    wanted = set(chosen)

    ordered: list[tuple[float, dict]] = [
        (float(index), event) for index, event in enumerate(damaged.events)
    ]
    for index, event in enumerate(damaged.events):
        if event["id"] in wanted:
            # Somewhere later in the stream, the way a retry lands behind whatever was in flight
            # when it was sent rather than immediately after its original.
            ordered.append((index + rng.randrange(1, 40) + 0.5, copy.deepcopy(event)))

    damaged.events = [event for _, event in sorted(ordered, key=lambda pair: pair[0])]
    return [damaged], {"redelivered_events": len(chosen), "event_ids": sorted(chosen)[:10]}


def out_of_order(wave: Wave, rng: random.Random) -> Injection:
    """Arrival order reversed end to end, which is the worst case rather than a shuffle.

    A refund arrives before the charge it refunds, a dispute closes before it opens, and the event
    that attaches the balance transaction to a charge arrives before the charge exists. Webhooks
    are unordered by contract, so this has to change nothing: silver takes the latest event per
    entity by *event time*, and event time is in the payload, not in the arrival.
    """
    damaged = wave.copy()
    damaged.events = list(reversed(damaged.events))
    return [damaged], {"reversed_events": len(damaged.events)}


def dropped_event(wave: Wave, rng: random.Random) -> Injection:
    """A charge whose webhooks never arrived, while the processor's own list still has it.

    The interesting part of this one is what it does *not* break. The ledger posts from the balance
    transaction list, so the money is untouched and the reconciliation is clean, and every internal
    invariant passes. What is wrong is that the business has a charge it never heard about, and the
    only thing that can say so is a comparison between the two inputs.
    """
    damaged = wave.copy()
    charges = take([t["source"] for t in charge_transactions(damaged)], rng)

    # The whole entity goes dark, not one of its events: the intent, the charge, the update that
    # carries the balance transaction, and anything that refers back to them later.
    silenced = set(charges) | {charge.replace("ch_", "pi_", 1) for charge in charges}

    kept = [e for e in damaged.events if not any(s in json.dumps(e) for s in silenced)]
    removed = len(damaged.events) - len(kept)
    damaged.events = kept

    # A refund or a dispute of a silenced charge names it, so its events went with it, and the
    # movement it produced is now unannounced too. That is what the coverage gap counts, so it is
    # what the expectation has to be written in terms of.
    suffixes = {charge.split("_", 1)[1] for charge in charges}
    blinded = [
        txn for txn in damaged.balance_transactions if txn["source"].split("_", 1)[1] in suffixes
    ]
    return [damaged], {
        "silenced_charges": len(charges),
        "blinded_sources": len({txn["source"] for txn in blinded}),
        "blinded_transactions": len(blinded),
        "removed_events": removed,
        "charge_ids": sorted(charges)[:10],
    }


def rounding_drift(wave: Wave, rng: random.Random) -> Injection:
    """One minor unit of drift on the settled net, which is what a float would have left behind.

    Every amount in this project is an integer in the settlement currency's minor unit, and this
    scenario is the argument for that. The drift is a single cent on a handful of transactions, the
    size of error a currency conversion done in floating point produces, and small enough that any
    reconciliation with a tolerance in it would absorb the lot.

    It lands on charges because a charge is the one category whose entry splits three ways, so the
    identity that has to hold is net + fee - amount = 0. One cent breaks it, and an entry that does
    not balance is not a reporting problem: the ledger created money.
    """
    damaged = wave.copy()
    ids = {t["id"] for t in take(charge_transactions(damaged), rng)}
    for txn in damaged.balance_transactions:
        if txn["id"] in ids:
            txn["net"] += 1
    return [damaged], {
        "drifted_transactions": len(ids),
        "drift_per_transaction": 1,
        "transaction_ids": sorted(ids)[:10],
    }


def reversal_dropped(wave: Wave, rng: random.Random) -> Injection:
    """A dispute the merchant won, whose reversal never made it into the list.

    Winning a dispute returns the amount and keeps the fee, which is the one movement a ledger that
    only ever grows cannot express. Here the stream says the dispute was won and carries the
    reversal expanded inside the payload, and the balance transaction list does not have it. The
    books therefore never give the money back.

    Nothing inside the ledger can see this. Every entry balances, the trial balance is zero, and
    every transaction the list does have is posted. It is visible in exactly two places: against
    the balance the processor reports, and against the copy of the transaction the stream itself
    carried.
    """
    damaged = wave.copy()
    reversals = [
        t for t in damaged.balance_transactions if t.get("reporting_category") == "dispute_reversal"
    ]
    if not reversals:
        raise SystemExit(
            "no dispute was won in this run, so there is no reversal to lose.\n"
            "Generate more transactions (make generate N=1000) and try again."
        )
    lost = {t["id"] for t in take(reversals, rng)}
    damaged.balance_transactions = [t for t in damaged.balance_transactions if t["id"] not in lost]
    return [damaged], {"lost_reversals": len(lost), "transaction_ids": sorted(lost)[:10]}


def late_arrival(wave: Wave, rng: random.Random) -> Injection:
    """A day that closed, and then changed.

    Two waves, and the split is the point. The first is the world as it was when the day closed:
    the events that had arrived, the movements the processor's list held, and the balance it
    reported at that moment. The second delivers what turned up afterwards, dated inside a period
    that has already been closed and reported.

    **The anchor moves with it, and that is not this module perturbing both sides.** At the time of
    the first close the processor had not reported the late movement either, because it had not
    happened as far as anyone knew. So wave one asks the processor's own walk what it would have
    said then, over the movements it knew about. Both sides are honest at both moments, the day
    still changes, and that is the entire reason a restatement exists rather than a correction.

    Nothing fails here. A late arrival is not a defect to be caught, it is the normal condition of
    a payment processor: a dispute opens three weeks after its charge. What is checked is that the
    ledger ends up identical to the undamaged run and that the day it had to move is on the record
    as having moved.
    """
    damaged = wave.copy()
    late = {t["id"] for t in take(damaged.balance_transactions, rng)}
    sources = {t["source"] for t in damaged.balance_transactions if t["id"] in late}

    early = [t for t in damaged.balance_transactions if t["id"] not in late]
    held = [e for e in damaged.events if any(source in json.dumps(e) for source in sources)]
    held_ids = {e["id"] for e in held}

    first = Wave(
        events=[e for e in damaged.events if e["id"] not in held_ids],
        balance_transactions=early,
        daily_balance=walk_daily_balance(early),
    )
    # The second wave carries the whole list, because a list is a query result and not a feed:
    # asking for it again returns everything, the late rows among it.
    second = Wave(
        events=held,
        balance_transactions=damaged.balance_transactions,
        daily_balance=damaged.daily_balance,
    )
    return [first, second], {
        "late_transactions": len(late),
        "held_events": len(held),
        "transaction_ids": sorted(late)[:10],
    }


SCENARIOS: dict[str, Scenario] = {
    scenario.name: scenario
    for scenario in (
        Scenario(
            name="baseline",
            failure="none",
            detection="the control arm the other six are measured against",
            inject=baseline,
            metrics=[
                ["silver_events", "==", "self:bronze_events"],
                ["gold_trial_balance", "==", 0],
                ["gold_reconciliation_breaks", "==", 0],
                ["gold_coverage_gaps", "==", 0],
                # One close, and nothing restated. A run that restates a day nobody changed is a
                # pipeline that is not deterministic, which would make every other arm meaningless.
                ["gold_closes", "==", 1],
                ["gold_restatements", "==", 0],
            ],
        ),
        Scenario(
            name="duplicate_delivery",
            failure="Duplicate delivery",
            detection="dedup on the event id: bronze keeps both, silver holds one, money unmoved",
            inject=duplicate_delivery,
            metrics=[
                ["bronze_deliveries", ">", "self:bronze_events"],
                ["silver_events", "==", "baseline:silver_events"],
                ["gold_postings", "==", "baseline:gold_postings"],
                ["gold_available", "==", "baseline:gold_available"],
                ["gold_reconciliation_breaks", "==", 0],
            ],
        ),
        Scenario(
            name="out_of_order",
            failure="Out-of-order arrival",
            detection="ordering by event time per entity, so arrival order cannot change a result",
            inject=out_of_order,
            metrics=[
                ["silver_events", "==", "baseline:silver_events"],
                ["silver_charges", "==", "baseline:silver_charges"],
                # The date a charge carries, summed. A charge dated by the last event that touched
                # it rather than by its own `created` moves, and reversing the stream is what makes
                # that mistake visible instead of plausible.
                ["charges_created_checksum", "==", "baseline:charges_created_checksum"],
                ["gold_available", "==", "baseline:gold_available"],
                ["gold_reconciliation_breaks", "==", 0],
            ],
        ),
        Scenario(
            name="dropped_event",
            failure="Dropped event",
            detection="a gap against the processor's balance transaction list",
            inject=dropped_event,
            dbt_failures={"assert_the_stream_saw_every_source": "blinded_transactions"},
            metrics=[
                ["silver_events", "<", "baseline:silver_events"],
                ["silver_charges", "<", "baseline:silver_charges"],
                # The money is untouched, which is the finding rather than a let-off: the ledger
                # posts from the list, so a lost webhook costs visibility and not a cent.
                ["gold_postings", "==", "baseline:gold_postings"],
                ["gold_reconciliation_breaks", "==", 0],
                ["gold_coverage_gaps", ">", 0],
            ],
        ),
        Scenario(
            name="late_arrival",
            failure="Late arrival after the close",
            detection="a restatement row, naming the movements that arrived after the day closed",
            inject=late_arrival,
            waves=2,
            metrics=[
                # The books end up exactly where the undamaged run ended up. Late is not wrong.
                ["silver_events", "==", "baseline:silver_events"],
                ["charges_created_checksum", "==", "baseline:charges_created_checksum"],
                ["gold_postings", "==", "baseline:gold_postings"],
                ["gold_available", "==", "baseline:gold_available"],
                ["gold_reconciliation_breaks", "==", 0],
                ["gold_coverage_gaps", "==", 0],
                # And the days that moved on the way there are on the record as having moved.
                ["gold_closes", "==", 2],
                ["gold_restatements", ">", 0],
            ],
        ),
        Scenario(
            name="rounding_drift",
            failure="Currency and rounding",
            detection="integer minor units and an entry that has to sum to zero, with no tolerance",
            inject=rounding_drift,
            dbt_failures={"assert_entries_balance": "drifted_transactions"},
            metrics=[
                ["gold_postings", "==", "baseline:gold_postings"],
                # Downstream of the failed invariant, so the run refuses to publish it at all.
                # A daily close built on entries that do not balance is a report of made up money.
                ["gold_close_rows", "==", None],
                ["gold_reconciliation_breaks", "==", None],
            ],
        ),
        Scenario(
            name="reversal_dropped",
            failure="Reversal",
            detection="signed postings against the reported balance, and against the stream's copy",
            inject=reversal_dropped,
            dbt_failures={
                "assert_no_unexplained_difference": None,
                "assert_the_list_holds_every_expanded_transaction": "lost_reversals",
            },
            metrics=[
                ["silver_events", "==", "baseline:silver_events"],
                ["gold_postings", "<", "baseline:gold_postings"],
                ["gold_reconciliation_breaks", ">", 0],
                ["gold_coverage_gaps", ">", 0],
            ],
        ),
    )
}


# --- writing a scenario out ---------------------------------------------------------------------


def read_wave(source: Path) -> Wave:
    def rows(name: str) -> list[dict]:
        path = source / f"{name}.jsonl"
        if not path.exists():
            raise SystemExit(
                f"{path} is missing. A scenario is a perturbation of a generated run, so\n"
                "generate one first: make generate N=1000"
            )
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]

    return Wave(*(rows(name) for name in ARTIFACTS))


def write_wave(wave: Wave, out: Path) -> dict[str, int]:
    out.mkdir(parents=True, exist_ok=True)
    counts = {}
    for name in ARTIFACTS:
        path = out / f"{name}.jsonl"
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            for row in wave.rows(name):
                handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
        counts[name] = len(wave.rows(name))
    return counts


def source_run(source: Path) -> dict:
    """The generated run an arm was built from, as the generator recorded it.

    A run is described completely by its transaction count, its seed and its calendar, and the
    digest is what makes that checkable. Reading it here rather than asking the caller means an arm
    cannot claim a size it was not built at.
    """
    manifest = source / "run.json"
    if not manifest.exists():
        raise SystemExit(
            f"{manifest} is missing, so an arm built from {source} could not say what run it"
            " damaged. Generate the run first: make generate"
        )
    run = json.loads(manifest.read_text(encoding="utf-8"))
    return {
        "transactions": run["transactions"],
        "seed": run["seed"],
        "days": run["days"],
        "events": run["sha256"]["events"],
    }


def build(name: str, source: Path, out: Path, seed: int) -> dict:
    scenario = SCENARIOS[name]
    waves, injected = scenario.inject(read_wave(source), random.Random(seed))
    if len(waves) != scenario.waves:
        raise SystemExit(
            f"{name} injected {len(waves)} waves and declares {scenario.waves}."
            " The DAG lays its tasks out from the declaration, so a run of a different shape would"
            " deliver into tasks that are not there."
        )

    # An expectation written as the name of a count the injection reports is resolved here, so the
    # manifest on disk holds a number. The scenario still declared it in advance; what it declared
    # was that this test must fail on exactly the rows the injection damaged, which is a stronger
    # statement than a count typed in by hand and kept up to date by hope.
    expected: dict[str, int | None] = {}
    for test, rows in scenario.dbt_failures.items():
        expected[test] = injected[rows] if isinstance(rows, str) else rows

    manifest = {
        "simulated": True,
        "scenario": scenario.name,
        "failure": scenario.failure,
        "detection": scenario.detection,
        "seed": seed,
        # Which generated run this arm damaged. Carried because the suite runs the pipeline once
        # per scenario, so it is affordable at a size the ledger itself is not, and a page showing
        # both has to be able to say which figure came from which run rather than letting a reader
        # assume they are the same one.
        "source_run": source_run(source),
        "injected": injected,
        "waves": [],
        "expect": {
            "dbt_failures": expected,
            "metrics": scenario.metrics,
        },
    }
    for number, wave in enumerate(waves, start=1):
        manifest["waves"].append(write_wave(wave, out / f"wave{number}"))

    (out / "scenario.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest


# --- the verdict ----------------------------------------------------------------------------------


def dbt_failures(run_results: Path) -> dict[str, int]:
    """Which tests failed in the last build, and on how many rows.

    Read from dbt's own artifact rather than from its stdout. The log line is formatting and the
    artifact is the record, and a suite that greps a log is one release note away from passing a
    run that failed.
    """
    if not run_results.exists():
        raise SystemExit(f"{run_results} is missing, so there is nothing to check the run against.")

    results = json.loads(run_results.read_text(encoding="utf-8")).get("results", [])
    failed: dict[str, int] = {}
    for result in results:
        if result.get("status") in ("fail", "error"):
            failed[test_name(result.get("unique_id", ""))] = int(result.get("failures") or 0)
    return failed


def test_name(unique_id: str) -> str:
    """The readable name out of a dbt node id.

    A singular test is `test.<package>.<name>`, and a generated one is
    `test.<package>.<name>.<checksum>`, so the last segment is the name in one case and ten hex
    digits in the other. Taking the third segment gives the name in both, which matters on the run
    where something fails that no scenario expected: the whole point of naming it is that whoever
    reads the verdict can go and look at it.
    """
    parts = unique_id.split(".")
    return parts[2] if len(parts) > 2 else parts[-1]


def resolve(operand, report: dict, base: dict):
    if isinstance(operand, str) and operand.startswith("baseline:"):
        return base.get(operand.split(":", 1)[1])
    if isinstance(operand, str) and operand.startswith("self:"):
        return report.get(operand.split(":", 1)[1])
    return operand


def compare(left, op: str, right) -> bool:
    if left is None or right is None:
        # Null means the table was never written, which is a real answer: a model downstream of a
        # failed invariant is skipped on purpose, and a scenario can require exactly that. It only
        # compares equal to another null.
        return (left is None and right is None) if op == "==" else False
    return {
        "==": left == right,
        "!=": left != right,
        ">": left > right,
        "<": left < right,
        ">=": left >= right,
        "<=": left <= right,
    }[op]


def verify(manifest: dict, report: dict, failures: dict[str, int], base: dict) -> list[str]:
    """Every way this run failed to be the run the scenario said it would be."""
    problems: list[str] = []
    expected = manifest["expect"]["dbt_failures"] or {}

    for name, rows in expected.items():
        if name not in failures:
            problems.append(f"{name} was supposed to fail and did not")
        elif rows is not None and failures[name] != rows:
            problems.append(f"{name} failed on {failures[name]} rows, expected {rows}")
    for name, rows in sorted(failures.items()):
        if name not in expected:
            problems.append(f"{name} failed on {rows} rows and was not supposed to fail at all")

    for left, op, right in manifest["expect"]["metrics"]:
        got = resolve(f"self:{left}", report, base)
        want = resolve(right, report, base)
        if not compare(got, op, want):
            problems.append(f"{left} is {got}, expected {op} {want} ({right})")
    return problems


# --- the command line -----------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inject one of the six failures, and check it.")
    parser.add_argument("command", choices=("list", "build", "verify"))
    parser.add_argument("scenario", nargs="?", choices=sorted(SCENARIOS), default=None)
    parser.add_argument(
        "--source",
        type=Path,
        default=config.REPO_ROOT / "data" / "generated",
        help="the generated run to perturb (default: %(default)s)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=config.REPO_ROOT / "data" / "chaos",
        help="where scenarios are written (default: %(default)s)",
    )
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args(argv)

    if args.command == "list":
        width = max(len(name) for name in SCENARIOS)
        for scenario in SCENARIOS.values():
            print(f"  {scenario.name:<{width}}  {scenario.failure}")
            print(f"  {'':<{width}}  caught by: {scenario.detection}")
        return 0

    if args.scenario is None:
        parser.error(f"{args.command} needs a scenario. `list` shows them.")

    out = args.out / args.scenario

    if args.command == "build":
        manifest = build(args.scenario, args.source, out, args.seed)
        print(f"{manifest['scenario']}: {manifest['failure']}")
        print(f"  injected  {json.dumps(manifest['injected'], sort_keys=True)}")
        for number, counts in enumerate(manifest["waves"], start=1):
            print(
                f"  wave {number}    {counts['events']} events,"
                f" {counts['balance_transactions']} balance transactions,"
                f" {counts['daily_balance']} reported days"
            )
        print(f"  written   {out}")
        return 0

    manifest = json.loads((out / "scenario.json").read_text(encoding="utf-8"))
    report = json.loads((out / "report.json").read_text(encoding="utf-8"))
    base_path = args.out / "baseline" / "report.json"
    base = json.loads(base_path.read_text(encoding="utf-8")) if base_path.exists() else {}
    failures = dbt_failures(out / "run_results.json")

    problems = verify(manifest, report, failures, base)
    print(f"{manifest['scenario']}: {manifest['failure']}")
    print(f"  caught by  {manifest['detection']}")
    for name, rows in sorted(failures.items()):
        print(f"  dbt failed {name} on {rows} rows")
    if problems:
        print("\nthis run is not the run the scenario described:")
        for problem in problems:
            print(f"  {problem}")
        return 1
    print("  verdict    as described")
    return 0


if __name__ == "__main__":
    sys.exit(main())
