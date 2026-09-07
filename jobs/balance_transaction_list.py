"""Phase 3: the one input that does not arrive as a webhook.

`/v1/balance_transactions` is a list endpoint, and this loads what it would return. It exists
because of the finding phase 2 measured: a webhook carries `balance_transaction` as an id, so the
fee, the net and `available_on` are not in the event stream at all, and every posting the ledger
makes comes off exactly those fields. Without this table there is no ledger, only a log of things
that happened to money nobody can account for.

**There is no bronze layer for it, on purpose.** Bronze exists to keep the arrival record of an
at-least-once stream, because a redelivery is evidence and collapsing it destroys evidence. A list
endpoint has no redeliveries to keep: it is a query result, and asking twice is not two events.
So it lands in silver directly, merged on the transaction id so that re-asking is free, and the
raw json is kept in a column the way bronze would have kept it.

The load also does something the two sources make possible: the dispute payloads embed their
balance transactions expanded, so the same money is described twice, independently. The job
reports whether the two agree.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

DEFAULT_SOURCE = "/opt/payment-ledger/data/generated/balance_transactions.jsonl"
DEFAULT_TABLE = "lakehouse.silver.balance_transaction_list"

# The stream's own copy lives beside the list, whichever namespace that is. Not a constant: the
# chaos suite runs each scenario into a namespace of its own, and a cross-check pinned to the
# default one would compare a damaged run against an undamaged neighbour and report agreement.
STREAM_TABLE = "balance_transactions"

FEE_DETAIL = StructType(
    [
        StructField("amount", LongType()),
        StructField("currency", StringType()),
        StructField("description", StringType()),
        StructField("type", StringType()),
    ]
)

TRANSACTION = StructType(
    [
        StructField("id", StringType()),
        StructField("source", StringType()),
        StructField("type", StringType()),
        StructField("reporting_category", StringType()),
        StructField("amount", LongType()),
        StructField("fee", LongType()),
        StructField("net", LongType()),
        StructField("currency", StringType()),
        StructField("created", LongType()),
        StructField("available_on", LongType()),
        StructField("status", StringType()),
        StructField("description", StringType()),
        StructField("fee_details", ArrayType(FEE_DETAIL)),
    ]
)

DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    balance_transaction_id STRING    COMMENT 'txn_..., what every posting is derived from',
    source_id              STRING    COMMENT 'the charge, refund or dispute it belongs to',
    type                   STRING,
    reporting_category     STRING,
    amount                 BIGINT    COMMENT 'settlement minor units, signed',
    fee                    BIGINT,
    net                    BIGINT    COMMENT 'amount minus fee, and the only figure that moves',
    currency               STRING    COMMENT 'settlement, which is not the presentment currency',
    created                TIMESTAMP,
    available_on           TIMESTAMP COMMENT 'when the money stops being pending',
    status                 STRING,
    description            STRING,
    fee_details            ARRAY<STRUCT<amount: BIGINT, currency: STRING,
                                        description: STRING, type: STRING>>,
    payload                STRING    COMMENT 'the response row, kept the way bronze keeps one',
    loaded_at              TIMESTAMP COMMENT 'when the pipeline first saw this movement'
)
USING iceberg
PARTITIONED BY (days(created))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
"""

# Every column is refreshed except the one that says when the row first arrived, which is what
# makes a late arrival visible at all. `UPDATE SET *` would stamp the whole list with the time of
# the most recent fetch, and a restatement could then no longer say what turned up after a day
# closed. The list is a query result, so a refetch legitimately restates everything else about a
# movement: a transaction matures from pending to available, and that has to be allowed through.
MERGE = """
MERGE INTO {table} AS target
USING {batch} AS source
ON target.balance_transaction_id = source.balance_transaction_id
WHEN MATCHED THEN UPDATE SET
    target.source_id          = source.source_id,
    target.type               = source.type,
    target.reporting_category = source.reporting_category,
    target.amount             = source.amount,
    target.fee                = source.fee,
    target.net                = source.net,
    target.currency           = source.currency,
    target.created            = source.created,
    target.available_on       = source.available_on,
    target.status             = source.status,
    target.description        = source.description,
    target.fee_details        = source.fee_details,
    target.payload            = source.payload
WHEN NOT MATCHED THEN INSERT *
"""


def load(spark: SparkSession, source: str, table: str) -> None:
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(DDL.format(table=table))

    raw = spark.read.text(source).withColumnRenamed("value", "payload")
    rows = raw.withColumn("txn", F.from_json(F.col("payload"), TRANSACTION)).select(
        F.col("txn.id").alias("balance_transaction_id"),
        F.col("txn.source").alias("source_id"),
        F.col("txn.type"),
        F.col("txn.reporting_category"),
        F.col("txn.amount"),
        F.col("txn.fee"),
        F.col("txn.net"),
        F.col("txn.currency"),
        F.col("txn.created").cast("timestamp").alias("created"),
        F.col("txn.available_on").cast("timestamp").alias("available_on"),
        F.col("txn.status"),
        F.col("txn.description"),
        F.col("txn.fee_details"),
        F.col("payload"),
        F.current_timestamp().alias("loaded_at"),
    )

    rows.createOrReplaceTempView("balance_transaction_batch")
    spark.sql(MERGE.format(table=table, batch="balance_transaction_batch"))


def report(spark: SparkSession, table: str) -> None:
    total = spark.sql(f"SELECT count(*) AS n FROM {table}").collect()[0]["n"]
    print(f"\n{total} balance transactions loaded into {table}")

    for row in spark.sql(
        f"SELECT reporting_category, count(*) AS n, sum(net) AS net"
        f" FROM {table} GROUP BY reporting_category ORDER BY 1"
    ).collect():
        print(f"  {row['n']:>7}  {row['reporting_category']:<18} net {row['net']:>12}")

    # The same money, described twice by two independent paths. The dispute payloads embed their
    # balance transactions expanded; the list returns them again. If those two ever disagree, one
    # of them is wrong and the ledger is built on whichever it happened to read.
    carried = f"{table.rsplit('.', 1)[0]}.{STREAM_TABLE}"
    if not spark.catalog.tableExists(carried):
        print("\nno stream-carried balance transactions to check against yet")
        return

    check = spark.sql(
        f"""
        SELECT
            count(*)                                              AS carried,
            count(l.balance_transaction_id)                       AS found_in_list,
            sum(CASE WHEN s.net = l.net AND s.amount = l.amount
                     AND s.fee = l.fee THEN 1 ELSE 0 END)         AS agreeing
        FROM {carried} s
        LEFT JOIN {table} l USING (balance_transaction_id)
        """
    ).collect()[0]
    print(
        f"\ncross-check: {check['carried']} balance transactions the event stream carried"
        f" expanded,\n  {check['found_in_list']} of them are in the list and"
        f" {check['agreeing']} agree on amount, fee and net"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load what /v1/balance_transactions returns.")
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("balance-transaction-list").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    load(spark, args.source, args.table)
    report(spark, args.table)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
