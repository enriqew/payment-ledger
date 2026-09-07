"""Phase 6: the gold tables out of the lakehouse, as plain rows.

This job makes one decision and no others: which tables leave the warehouse. It does not rename a
column, derive a figure or decide what a dashboard should be told, because the moment a dump starts
computing there are two definitions of the ledger and the one nobody reads is the one that stays
right. Everything the export means is assembled and checked on the host, in
`payment_ledger.export`, which is a module a test can import.

**The export does not grow with volume.** Every table here is bounded by the calendar or by the
chart of accounts, not by the number of transactions: a hundred thousand charges and a thousand
produce the same sixty-odd daily rows and the same six accounts. That is what makes a static file a
reasonable interface at all, and it is checked rather than assumed.

**Written to a directory of its own, not into `data/`.** The repository is mounted read only into
this container on purpose. Exporting is the one thing a job here has to write to the tree, so it
gets one writable path with an obvious name, instead of the tree becoming writable.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import SparkSession

DEFAULT_NAMESPACE = "lakehouse.gold"
DEFAULT_OUT = "/opt/payment-ledger/export/tables"

# What a dashboard is allowed to read, and nothing else. `ledger_postings` is deliberately absent:
# it is the only table here that grows with volume, and a page that wanted individual postings
# would be asking for a query engine rather than for a file.
TABLES = (
    "account_balances",
    "daily_close",
    "reconciliation",
    "coverage_gaps",
    "restatements",
)

# A table that has outgrown a static file. The bound is the calendar plus a year of slack, and
# tripping it means something is being exported that should not be.
MAX_ROWS = 5_000


def encode(value):
    """Rows into JSON without inventing a type.

    Dates and timestamps become ISO strings, which is the one representation every reader agrees
    on. Nothing else is converted: an integer stays an integer all the way to the file, because a
    monetary value that passes through a float on the way out is a monetary value that can arrive
    wrong.
    """
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"{type(value).__name__} has no agreed representation in the export")


def dump(spark: SparkSession, namespace: str, table: str, out: Path) -> int:
    collected = spark.sql(f"SELECT * FROM {namespace}.{table}").collect()
    rows = [row.asDict(recursive=True) for row in collected]
    if len(rows) > MAX_ROWS:
        raise SystemExit(
            f"{table} has {len(rows)} rows, over the {MAX_ROWS} a static file is meant to hold.\n"
            "The export is supposed to be bounded by the calendar. Something is not."
        )

    path = out / f"{table}.json"
    path.write_text(
        json.dumps(rows, default=encode, sort_keys=True, indent=1) + "\n", encoding="utf-8"
    )
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Dump the gold tables a dashboard reads.")
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--out", default=DEFAULT_OUT)
    args = parser.parse_args(argv)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    spark = SparkSession.builder.appName("export-tables").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\ndumping {args.namespace} to {out}")
    for table in TABLES:
        print(f"  {dump(spark, args.namespace, table, out):>6}  {table}")
    print("\nrows only. what they mean is assembled and checked on the host.")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
