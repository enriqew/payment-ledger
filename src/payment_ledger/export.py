"""Phase 6: what leaves the warehouse, and what it is allowed to say.

A dashboard reads files, not a lakehouse. That is the whole of the deployment story here: the
portfolio it feeds is a static site with no backend, so the interface between this project and
anything that shows it is a handful of JSON files somebody copies. Which means the interesting work
is not the copying. It is deciding what those files may contain and refusing to write them when the
run behind them does not deserve to be read.

**The export refuses rather than warns.** A ledger whose entries do not balance and a day the
reconciliation cannot explain are both reasons a figure should never reach a page, and a dashboard
has no way to find that out afterwards. So the checks are here, on the way out, and they are the
same statements the invariants make: the trial balance sums to zero, and no day carries an
unexplained difference. If either fails, nothing is written.

**Every file says it is simulated, not only the manifest.** These get copied one at a time into
another repository, and a payload that only knows it is simulated because of a sibling file is one
rename away from being presented as real.

**No float, all the way to the file.** Money is an integer in the settlement currency's minor unit
at every layer, and an export is the last place that can quietly stop being true: a JSON writer
handed a Decimal, an average computed for convenience, a percentage. The validator walks every
value in every artifact and refuses a float wherever it finds one.

**Byte identical for the same run.** There is no timestamp anywhere in the export. What identifies
it is the fingerprint of the generated run it was built from, which is a stronger thing to carry:
two exports of the same run are the same bytes, and an export whose numbers moved is an export
whose input moved.

**Bounded by the calendar, not by volume.** Every artifact is a day, an account or a scenario, so a
hundred thousand transactions produce the same sixty-odd rows as a thousand. That is what makes a
static file an honest interface instead of a truncation nobody mentions.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterator
from pathlib import Path

from payment_ledger import chaos, config

SCHEMA_VERSION = 1

# Read off the suite rather than typed here, so an arm added tomorrow is one the export
# requires instead of one it silently drops.
EXPECTED_ARMS = len(chaos.SCENARIOS)

# Said in the files themselves rather than only in a README, because a file travels and a README
# does not.
NOTICE = (
    "Simulated data. Event schemas come from a payment processor's test mode; the volume is"
    " generated. Nothing here describes real payments, real money or real customers."
)

SETTLEMENT_CURRENCY = "eur"
AMOUNTS = "integer minor units of the settlement currency"

TABLES = (
    "account_balances",
    "money_flow",
    "daily_close",
    "reconciliation",
    "coverage_gaps",
    "restatements",
)

# What each artifact keeps, and in this order. Narrowing here rather than in the dump is the point
# of the split: the job decides which tables leave, this decides what they are allowed to say.
DAILY_CLOSE_FIELDS = (
    "close_date",
    "currency",
    "balance_pending",
    "balance_available",
    "gross_sales",
    "refunds",
    "processing_fees",
    "disputes",
)
RECONCILIATION_FIELDS = (
    "close_date",
    "currency",
    "ledger_total",
    "reported_total",
    "total_difference",
    "unexplained_difference",
    "ledger_closed_the_day",
    "processor_reported_the_day",
)
ACCOUNT_FIELDS = ("account", "currency", "balance", "postings")
FLOW_FIELDS = ("source", "target", "currency", "amount")
GAP_FIELDS = (
    "finding",
    "balance_transaction_id",
    "source_id",
    "reporting_category",
    "net",
    "currency",
    "noticed_on",
)
RESTATEMENT_FIELDS = (
    "close_date",
    "currency",
    "pending_then",
    "pending_now",
    "available_then",
    "available_now",
    "late_transactions",
    "fully_explained_by_what_arrived_late",
)

# The counts a scenario's page shows. The full report has more; these are the ones that mean
# something to somebody who is not holding the pipeline in their head.
CHAOS_COUNTS = (
    "bronze_deliveries",
    "bronze_events",
    "silver_events",
    "silver_charges",
    "gold_postings",
    "gold_trial_balance",
    "gold_reconciliation_breaks",
    "gold_coverage_gaps",
    "gold_restatements",
)


class Refused(SystemExit):
    """The export did not happen, and the message says what would have been published."""


def artifact(**payload) -> dict:
    """One file: what it is, that it is simulated, and its rows."""
    return {"schema_version": SCHEMA_VERSION, "simulated": True, "notice": NOTICE, **payload}


def narrow(rows: list[dict], fields: tuple[str, ...]) -> list[dict]:
    """Only the named columns, in the named order, and an error if one is missing.

    A model that renames a column would otherwise export a page of nulls, which reads as a quiet
    zero rather than as the breakage it is.
    """
    narrowed = []
    for row in rows:
        missing = [field for field in fields if field not in row]
        if missing:
            raise Refused(f"the export wants {', '.join(missing)} and the table does not have it")
        narrowed.append({field: row[field] for field in fields})
    return narrowed


def read_tables(tables: Path) -> dict[str, list[dict]]:
    dumped = {}
    for name in TABLES:
        path = tables / f"{name}.json"
        if not path.exists():
            raise Refused(
                f"{path} is missing. The gold tables are dumped by a Spark job first:\n"
                "  make export"
            )
        dumped[name] = json.loads(path.read_text(encoding="utf-8"))
    return dumped


def read_chaos(root: Path) -> list[dict]:
    """The chaos suite as the dashboard's centrepiece, or nothing at all.

    All seven arms or none, deliberately. A page showing three of them looks exactly like a page
    showing the suite, and a partial chaos suite is the one shape that misleads by being true.
    """
    if not root.exists():
        return []

    # Every arm but the control one measures itself against the baseline's counts, so judging one
    # without that file would call a scenario undescribed for want of something to compare to.
    measured = root / "baseline" / "report.json"
    baseline = json.loads(measured.read_text(encoding="utf-8")) if measured.exists() else {}

    arms = []
    for directory in sorted(root.iterdir()):
        scenario = directory / "scenario.json"
        report = directory / "report.json"
        results = directory / "run_results.json"
        if not (scenario.exists() and report.exists() and results.exists()):
            continue

        manifest = json.loads(scenario.read_text(encoding="utf-8"))
        counts = json.loads(report.read_text(encoding="utf-8"))
        failed = chaos.dbt_failures(results)
        arms.append(
            {
                "scenario": manifest["scenario"],
                "failure": manifest["failure"],
                "detection": manifest["detection"],
                "injected": {
                    key: value
                    for key, value in manifest["injected"].items()
                    if isinstance(value, int)
                },
                "expected_to_fail": manifest["expect"]["dbt_failures"],
                "failed": failed,
                "as_described": not chaos.verify(manifest, counts, failed, baseline),
                "counts": {key: counts.get(key) for key in CHAOS_COUNTS},
            }
        )
    # In the order the suite runs them, which is the order the design lists the failures in, rather
    # than the alphabetical order the directories happen to have. A page renders what it is given.
    order = list(chaos.SCENARIOS)
    return sorted(arms, key=lambda arm: order.index(arm["scenario"]))


def assemble(tables: dict[str, list[dict]], run: dict, arms: list[dict]) -> dict[str, dict]:
    """The artifacts, from the rows and the run that produced them."""
    accounts = narrow(tables["account_balances"], ACCOUNT_FIELDS)
    flow = narrow(tables["money_flow"], FLOW_FIELDS)
    daily = narrow(tables["daily_close"], DAILY_CLOSE_FIELDS)
    reconciliation = narrow(tables["reconciliation"], RECONCILIATION_FIELDS)
    gaps = narrow(tables["coverage_gaps"], GAP_FIELDS)
    restatements = narrow(tables["restatements"], RESTATEMENT_FIELDS)

    files = {
        "accounts.json": artifact(
            rows=sorted(accounts, key=lambda row: (row["currency"], row["account"])),
            # Carried rather than left to the reader to add up, because it is the one number on
            # this page that has to be zero and a page that does not show it is not showing a
            # trial balance.
            total=sum(row["balance"] for row in accounts),
        ),
        # Where the money went rather than where it ended up, which is the same postings read as a
        # graph. Sorted heaviest first so a reader of the file sees the trunk before the twigs.
        "flow.json": artifact(
            rows=sorted(flow, key=lambda row: (-row["amount"], row["source"], row["target"]))
        ),
        "daily_close.json": artifact(
            rows=sorted(daily, key=lambda row: (row["close_date"], row["currency"]))
        ),
        "reconciliation.json": artifact(
            rows=sorted(reconciliation, key=lambda row: (row["close_date"], row["currency"])),
            days_reconciled=len(reconciliation),
            days_with_an_unexplained_difference=sum(
                1 for row in reconciliation if row["unexplained_difference"] != 0
            ),
        ),
        # Empty on a healthy run, which is the statement that the two inputs agreed and that no
        # published day has moved. It is not a table nobody finished.
        "findings.json": artifact(coverage_gaps=gaps, restatements=restatements),
        "chaos.json": artifact(arms=arms),
    }
    files["manifest.json"] = artifact(
        run={key: run[key] for key in ("transactions", "seed", "days", "start", "sha256")},
        settlement_currency=SETTLEMENT_CURRENCY,
        amounts=AMOUNTS,
        files={name: count_of(payload) for name, payload in sorted(files.items())},
    )
    return files


def count_of(payload: dict) -> int:
    for key in ("rows", "arms"):
        if key in payload:
            return len(payload[key])
    return len(payload.get("coverage_gaps", [])) + len(payload.get("restatements", []))


DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def dated(value) -> Iterator[tuple[str, object]]:
    """Every field in a payload whose name says it is a day."""
    if isinstance(value, dict):
        for key, item in value.items():
            if key.endswith(("_date", "_on")):
                yield key, item
            yield from dated(item)
    elif isinstance(value, list):
        for item in value:
            yield from dated(item)


def walk(value) -> Iterator:
    if isinstance(value, dict):
        for item in value.values():
            yield from walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from walk(item)
    else:
        yield value


def weighed(flow: list[dict], accounts: list[dict]) -> list[str]:
    """The flow against the balances, checked here as well as in the warehouse.

    A dbt test already says these agree, and a failing test does not stop `make export` from
    reading the table it was testing: the model is built before the test that judges it. So the
    check is repeated on the way out, where it can refuse. Nothing in a Sankey diagram objects to
    arrows that do not add up, which is exactly why something else has to.
    """
    problems = []
    net: dict[tuple[str, str], int] = {}
    for link in flow:
        if link["amount"] <= 0:
            problems.append(
                f"the flow carries {link['amount']} from {link['source']}, not a weight"
            )
        net[(link["target"], link["currency"])] = (
            net.get((link["target"], link["currency"]), 0) + link["amount"]
        )
        net[(link["source"], link["currency"])] = (
            net.get((link["source"], link["currency"]), 0) - link["amount"]
        )

    for account in accounts:
        moved = net.pop((account["account"], account["currency"]), 0)
        if moved != account["balance"]:
            problems.append(
                f"{account['account']} holds {account['balance']} and the flow moved {moved} into"
                " it, so the diagram and the ledger are telling different stories"
            )
    for account, _ in net:
        problems.append(f"the flow touches {account}, which the trial balance does not have")
    return problems


def validate(files: dict[str, dict]) -> None:
    """Everything the export promises, checked before a single byte is written."""
    problems: list[str] = []

    for name, payload in sorted(files.items()):
        if payload.get("simulated") is not True or payload.get("schema_version") != SCHEMA_VERSION:
            problems.append(f"{name} does not declare itself simulated and versioned")
        for value in walk(payload):
            if isinstance(value, float):
                problems.append(f"{name} carries a floating point value, which money never is")
                break
        # A day is a plain ISO date and not a timestamp, because the moment one carries a time of
        # day the reader has to guess a timezone, and a daily close that means a different day on
        # two machines is the bug the whole pipeline runs in UTC to avoid.
        for key, value in dated(payload):
            if not isinstance(value, str) or not DAY.match(value):
                problems.append(f"{name} has {key} = {value!r}, which is not a plain ISO day")
                break

    total = files["accounts.json"]["total"]
    if total != 0:
        problems.append(f"the trial balance is {total} and not zero, so the ledger created money")

    problems += weighed(files["flow.json"]["rows"], files["accounts.json"]["rows"])

    unexplained = files["reconciliation.json"]["days_with_an_unexplained_difference"]
    if unexplained:
        problems.append(f"{unexplained} days carry an unexplained difference against the processor")

    arms = files["chaos.json"]["arms"]
    if arms and len(arms) != EXPECTED_ARMS:
        problems.append(
            f"the chaos suite exported {len(arms)} arms of {EXPECTED_ARMS}."
            " All of them or none: a partial suite reads exactly like a whole one"
        )
    for arm in arms:
        if not arm["as_described"]:
            problems.append(f"the {arm['scenario']} arm did not do what it said it would")

    if problems:
        raise Refused(
            "nothing was written. The export refuses rather than publishing this:\n  "
            + "\n  ".join(problems)
        )


def write(files: dict[str, dict], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    for name, payload in sorted(files.items()):
        (out / name).write_text(
            json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8"
        )


def build(tables: Path, run_manifest: Path, chaos_root: Path) -> dict[str, dict]:
    run = json.loads(run_manifest.read_text(encoding="utf-8"))
    files = assemble(read_tables(tables), run, read_chaos(chaos_root))
    validate(files)
    return files


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Assemble and check what a dashboard may read.")
    parser.add_argument("--tables", type=Path, default=config.REPO_ROOT / "export" / "tables")
    parser.add_argument("--out", type=Path, default=config.REPO_ROOT / "export")
    parser.add_argument(
        "--run", type=Path, default=config.REPO_ROOT / "data" / "generated" / "run.json"
    )
    parser.add_argument("--chaos", type=Path, default=config.REPO_ROOT / "data" / "chaos")
    args = parser.parse_args(argv)

    files = build(args.tables, args.run, args.chaos)
    write(files, args.out)

    print(f"export v{SCHEMA_VERSION} written to {args.out}")
    for name, count in sorted(files["manifest.json"]["files"].items()):
        print(f"  {count:>6}  {name}")
    arms = files["chaos.json"]["arms"]
    print(f"\n  {len(arms) or 'no'} chaos arms carried" + (", all as described" if arms else ""))
    print("  trial balance 0, no unexplained difference, no float anywhere.")
    print("  simulated data. nothing here describes real payments.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
