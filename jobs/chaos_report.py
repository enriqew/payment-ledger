"""What one arm of the chaos suite ended up holding, as a number per table.

The dbt artifact says which invariants fired. It says nothing about the layers underneath it, and
half of what a scenario claims is about those: that bronze kept both copies of a redelivered event
and silver kept one, that reversing the arrival order changed no result at all, that a dropped
webhook cost visibility and not a cent. Those are row counts, and this is where they come from.

Every count is taken defensively. A model downstream of a failed invariant is never built, so the
table is simply not there, and that absence is a real answer rather than an error: `rounding_drift`
requires the daily close to be missing, because publishing a close over entries that do not balance
would be reporting money the ledger invented. A missing table reports as null and null compares
equal to nothing except null.

The report goes to stdout behind a marker rather than to a file. The repository is mounted read
only into this container on purpose, and a job that needs to write to the tree to be measurable is
a job that has to be trusted with the tree.
"""

from __future__ import annotations

import argparse
import json
import sys

from pyspark.sql import SparkSession

MARKER = "CHAOS-REPORT"

# One query per number, so a table that does not exist costs exactly the counts that came from it.
# `{b}`, `{s}` and `{g}` are the three namespaces of one arm.
MEASUREMENTS = {
    "bronze_deliveries": "SELECT count(*) FROM {b}.events",
    "bronze_events": "SELECT count(DISTINCT event_id) FROM {b}.events",
    "silver_events": "SELECT count(*) FROM {s}.events",
    "silver_deliveries": "SELECT sum(deliveries) FROM {s}.events",
    "silver_charges": "SELECT count(*) FROM {s}.charges",
    "silver_refunds": "SELECT count(*) FROM {s}.refunds",
    "silver_disputes": "SELECT count(*) FROM {s}.disputes",
    "silver_expanded_transactions": "SELECT count(*) FROM {s}.balance_transactions",
    "list_transactions": "SELECT count(*) FROM {s}.balance_transaction_list",
    "reported_days": "SELECT count(*) FROM {s}.reported_balance",
    # The sum of the dates the charges carry. A charge dated by the last event that touched it
    # rather than by its own `created` moves in time without changing any count, so a checksum over
    # the dates is what makes that visible: it is the cheapest statement that reversing the arrival
    # order left every charge on the day it actually happened.
    "charges_created_checksum": "SELECT sum(unix_timestamp(created)) FROM {s}.charges",
    "gold_postings": "SELECT count(*) FROM {g}.ledger_postings",
    "gold_entries": "SELECT count(DISTINCT entry_id) FROM {g}.ledger_postings",
    # Zero on any ledger worth the name, in any currency, whatever else went wrong.
    "gold_trial_balance": "SELECT sum(amount) FROM {g}.ledger_postings",
    "gold_available": (
        "SELECT sum(amount) FROM {g}.ledger_postings WHERE account = 'asset:balance_available'"
    ),
    "gold_close_rows": "SELECT count(*) FROM {g}.daily_close",
    "gold_reconciliation_breaks": (
        "SELECT count(*) FROM {g}.reconciliation WHERE unexplained_difference != 0"
    ),
    "gold_reconciliation_items": "SELECT count(*) FROM {g}.reconciliation_items",
    "gold_coverage_gaps": "SELECT count(*) FROM {g}.coverage_gaps",
    "gold_restatements": "SELECT count(*) FROM {g}.restatements",
    # How many times the close has been taken in this arm. One for every scenario but the late
    # arrival, which closes the day, receives what was held back, and closes it again.
    "gold_closes": "SELECT count(DISTINCT closed_at) FROM {g}.daily_close_log",
}


def measure(spark: SparkSession, namespaces: dict[str, str]) -> dict[str, int | None]:
    report: dict[str, int | None] = {}
    for name, query in MEASUREMENTS.items():
        try:
            value = spark.sql(query.format(**namespaces)).collect()[0][0]
        except Exception:  # noqa: BLE001 - a missing table is an answer, not a failure
            value = None
        report[name] = None if value is None else int(value)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Count what one arm of the chaos suite holds.")
    parser.add_argument(
        "--prefix",
        default="",
        help="namespace prefix for this arm, so `x_` reads x_bronze, x_silver and x_gold",
    )
    parser.add_argument("--catalog", default="lakehouse")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("chaos-report").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    namespaces = {
        key: f"{args.catalog}.{args.prefix}{layer}"
        for key, layer in (("b", "bronze"), ("s", "silver"), ("g", "gold"))
    }
    report = measure(spark, namespaces)

    for name, value in report.items():
        print(f"  {name:<30} {'-' if value is None else value}")
    # The marker line is read by whoever started the container, and one of the two readers does not
    # get lines: the DAG's operator hands over the stream in the pieces it arrived in. stdout into a
    # pipe is block buffered, so the table above could fill a block halfway through this line and
    # send it in two writes, and the verdict then read half a report. Emptying the buffer first and
    # writing the line on its own makes it one write, which arrives as one piece.
    sys.stdout.flush()
    print(f"{MARKER} {json.dumps(report, sort_keys=True)}", flush=True)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
