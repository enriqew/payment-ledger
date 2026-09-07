"""Phase 4: the other side of the comparison.

This loads what the processor says its balance was, per day and per currency. It is the anchor the
ledger is weighed against, and the whole reason it is worth anything is that it is computed
**independently of the pipeline**: the generator walks the same simulated money with different
code and never consults a single Iceberg table. Two walks that shared an implementation would
agree by construction and prove nothing at all.

**What this is not.** It is not Stripe's books. Section 3 of the design is explicit about why the
live `/v1/balance` cannot be the anchor here: against generated volume it reflects a handful of
triggered test events and nothing the generator produced, and a stranger who clones this
repository has no account of their own. So the claim this supports is that the reconciliation
detects and attributes a divergence. It is not, and must never be reported as, evidence that the
ledger agrees with a real processor.

Loaded, like the balance transaction list, straight into silver: a daily balance is a query result
and not an at-least-once delivery, so there is no arrival record for bronze to keep.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

DEFAULT_SOURCE = "/opt/payment-ledger/data/generated/daily_balance.jsonl"
DEFAULT_TABLE = "lakehouse.silver.reported_balance"

REPORTED = StructType(
    [
        StructField("date", StringType()),
        StructField("currency", StringType()),
        StructField("pending", LongType()),
        StructField("available", LongType()),
    ]
)

DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    as_of_date  DATE      COMMENT 'the day the processor is reporting on',
    currency    STRING,
    pending     BIGINT    COMMENT 'settlement minor units, captured but not yet available',
    available   BIGINT    COMMENT 'settlement minor units, available for payout',
    payload     STRING    COMMENT 'the reported row, kept as it arrived'
)
USING iceberg
PARTITIONED BY (months(as_of_date))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
"""

MERGE = """
MERGE INTO {table} AS target
USING {batch} AS source
ON target.as_of_date = source.as_of_date AND target.currency = source.currency
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""


def load(spark: SparkSession, source: str, table: str) -> None:
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(DDL.format(table=table))

    rows = (
        spark.read.text(source)
        .withColumnRenamed("value", "payload")
        .withColumn("row", F.from_json(F.col("payload"), REPORTED))
        .select(
            F.to_date(F.col("row.date")).alias("as_of_date"),
            F.col("row.currency").alias("currency"),
            F.col("row.pending").alias("pending"),
            F.col("row.available").alias("available"),
            F.col("payload"),
        )
    )

    rows.createOrReplaceTempView("reported_balance_batch")
    spark.sql(MERGE.format(table=table, batch="reported_balance_batch"))


def report(spark: SparkSession, table: str) -> None:
    row = spark.sql(
        f"SELECT count(*) AS days, min(as_of_date) AS first_day, max(as_of_date) AS last_day"
        f" FROM {table}"
    ).collect()[0]
    print(f"\n{row['days']} reported balance rows, {row['first_day']} to {row['last_day']}")

    for last in spark.sql(
        f"SELECT currency, pending, available FROM {table}"
        f" WHERE as_of_date = (SELECT max(as_of_date) FROM {table}) ORDER BY currency"
    ).collect():
        print(
            f"  the processor closes at {last['available']} available"
            f" and {last['pending']} pending, in {last['currency']}"
        )
    print("  simulated, and computed without consulting the pipeline.")
    print("  not a real processor's books, and no report may say otherwise.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load the balance the processor reports.")
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("reported-balance").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    load(spark, args.source, args.table)
    report(spark, args.table)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
