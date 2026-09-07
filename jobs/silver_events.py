"""Phase 2: `bronze.events` into `silver.events`, one row per event that happened.

Bronze holds deliveries. Silver holds events. The difference between the two row counts is the
duplicate-delivery failure, and this is the job that turns one into the other without losing the
evidence: every row here records how many deliveries collapsed into it and when the first and last
of them arrived.

**Deduplication is a MERGE on the table's own key, not a watermarked `dropDuplicates`.** That is
the whole argument of this file. A watermark forgets: set it to an hour and a redelivery that
arrives ninety minutes late passes straight through as a second event, and the ledger doubles a
charge for a reason nobody will find. Stripe retries a failed webhook for up to three days, so the
watermark that would actually be safe is three days of state in a streaming shuffle. `MERGE INTO`
against the key that already exists in the table is bounded by the table rather than by a guess
about how late a retry can be, and it is correct at any lateness.

**Typing is per event, not per object.** The fields lifted out are the ones every payload carries
in the same place: the entity, the charge it belongs to, the amount, the currency, the status. The
payload string is kept whole beside them, because a typed column is a decision and the raw event is
the appeal.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

DEFAULT_SOURCE = "lakehouse.bronze.events"
DEFAULT_TABLE = "lakehouse.silver.events"
DEFAULT_CHECKPOINT = "/opt/payment-ledger/checkpoints/silver_events"

BALANCE_TRANSACTION = StructType(
    [
        StructField("id", StringType()),
        StructField("amount", LongType()),
        StructField("fee", LongType()),
        StructField("net", LongType()),
        StructField("currency", StringType()),
        StructField("available_on", LongType()),
        StructField("created", LongType()),
        StructField("status", StringType()),
        StructField("type", StringType()),
        StructField("reporting_category", StringType()),
        StructField("source", StringType()),
    ]
)

# One permissive schema across every object type. `from_json` nulls what a given payload does not
# carry, so a charge simply has no `balance_transactions` and a balance has no `amount`, which is
# the honest representation of the difference between them.
OBJECT = StructType(
    [
        StructField("id", StringType()),
        StructField("object", StringType()),
        StructField("amount", LongType()),
        StructField("amount_refunded", LongType()),
        StructField("refunded", BooleanType()),
        StructField("currency", StringType()),
        StructField("status", StringType()),
        StructField("charge", StringType()),
        StructField("payment_intent", StringType()),
        StructField("balance_transaction", StringType()),
        StructField("balance_transactions", ArrayType(BALANCE_TRANSACTION)),
        StructField("created", LongType()),
        StructField("disputed", BooleanType()),
        StructField("dispute", StringType()),
        StructField("reason", StringType()),
    ]
)

ENVELOPE = StructType([StructField("data", StructType([StructField("object", OBJECT)]))])

DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    event_id               STRING    COMMENT 'the dedup key: one row per distinct event',
    event_type             STRING,
    created                TIMESTAMP COMMENT 'event time, which is what silver orders by',
    entity_type            STRING    COMMENT 'charge, refund, dispute, payment_intent, balance',
    entity_id              STRING,
    charge_id              STRING    COMMENT 'the charge this is ultimately about',
    payment_intent_id      STRING,
    amount                 BIGINT    COMMENT 'presentment minor units, never a float',
    currency               STRING    COMMENT 'presentment, which is not the settlement currency',
    status                 STRING,
    balance_transaction_id STRING    COMMENT 'an id, because the webhook does not carry the object',
    amount_refunded        BIGINT,
    refunded               BOOLEAN,
    reason                 STRING,
    livemode               BOOLEAN,
    api_version            STRING,
    request_id             STRING,
    idempotency_key        STRING,
    payload                STRING    COMMENT 'the delivered json; a typed column is a decision',
    deliveries             INT       COMMENT 'how many bronze rows collapsed into this one',
    first_seen_at          TIMESTAMP,
    last_seen_at           TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(created))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
"""

MERGE = """
MERGE INTO {table} AS target
USING {batch} AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET
    target.deliveries    = target.deliveries + source.deliveries,
    target.first_seen_at = least(target.first_seen_at, source.first_seen_at),
    target.last_seen_at  = greatest(target.last_seen_at, source.last_seen_at)
WHEN NOT MATCHED THEN INSERT *
"""


def ensure_table(spark: SparkSession, table: str) -> None:
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(DDL.format(table=table))


def typed(bronze):
    """Bronze deliveries into typed silver rows, still one row per delivery."""
    parsed = bronze.withColumn("event", F.from_json(F.col("payload"), ENVELOPE)).withColumn(
        "obj", F.col("event.data.object")
    )

    entity_type = (
        F.when(F.col("obj.object").isNotNull(), F.col("obj.object"))
        .otherwise(F.split(F.col("event_type"), r"\.").getItem(0))
        .alias("entity_type")
    )

    # The charge an event is ultimately about, resolved the same way the producer resolves the
    # partition key: a dispute belongs to the charge it disputes, not to itself.
    charge_id = F.coalesce(
        F.col("obj.charge"),
        F.when(F.col("obj.object") == "charge", F.col("obj.id")),
    ).alias("charge_id")

    return parsed.select(
        F.col("event_id"),
        F.col("event_type"),
        F.col("created"),
        entity_type,
        F.col("obj.id").alias("entity_id"),
        charge_id,
        F.col("obj.payment_intent").alias("payment_intent_id"),
        F.col("obj.amount").alias("amount"),
        F.col("obj.currency").alias("currency"),
        F.col("obj.status").alias("status"),
        F.col("obj.balance_transaction").alias("balance_transaction_id"),
        F.col("obj.amount_refunded").alias("amount_refunded"),
        F.col("obj.refunded").alias("refunded"),
        F.col("obj.reason").alias("reason"),
        F.col("livemode"),
        F.col("api_version"),
        F.col("request_id"),
        F.col("idempotency_key"),
        F.col("payload"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
    )


def collapse(rows):
    """Deliveries of one event into one row, counting how many there were.

    Which delivery's content wins has to be decided rather than left to whichever the shuffle
    hands over first, so it is the earliest one by Kafka coordinate. Redeliveries of an event are
    byte-identical by construction, so this only matters on the day one is not.
    """
    order = Window.partitionBy("event_id").orderBy("kafka_partition", "kafka_offset")
    counts = Window.partitionBy("event_id")

    return (
        rows.withColumn("_rank", F.row_number().over(order))
        .withColumn("deliveries", F.count("*").over(counts).cast("int"))
        .withColumn("first_seen_at", F.min("kafka_timestamp").over(counts))
        .withColumn("last_seen_at", F.max("kafka_timestamp").over(counts))
        .where(F.col("_rank") == 1)
        .drop("_rank", "kafka_partition", "kafka_offset", "kafka_timestamp")
    )


def merge_batch(table: str):
    """Upsert one micro-batch. Idempotent, so a replayed batch changes nothing but the counters."""

    def apply(batch, batch_id: int) -> None:
        # Persisted because MERGE scans its source more than once, and without this the batch is
        # recomputed per scan: the window that counts deliveries runs twice and the streaming
        # source reports double the rows it actually read.
        collapsed = collapse(typed(batch)).persist()
        try:
            view = f"silver_batch_{batch_id}"
            collapsed.createOrReplaceTempView(view)
            batch.sparkSession.sql(MERGE.format(table=table, batch=view))
        finally:
            collapsed.unpersist()

    return apply


def report(spark: SparkSession, source: str, table: str) -> None:
    """The duplicate-delivery failure, as two numbers rather than as a claim."""
    row = spark.sql(
        f"SELECT (SELECT count(*) FROM {source}) AS deliveries, "
        f"       (SELECT count(*) FROM {table}) AS events, "
        f"       (SELECT coalesce(sum(deliveries), 0) FROM {table}) AS accounted"
    ).collect()[0]

    print(f"\nbronze {row['deliveries']} deliveries -> silver {row['events']} events")
    print(f"  every delivery accounted for: {row['accounted'] == row['deliveries']}")
    if row["deliveries"] != row["events"]:
        print(f"  {row['deliveries'] - row['events']} redeliveries collapsed")

    # The gap the phase 4 join exists to close, measured instead of described. A charge names its
    # balance transaction and never carries it, so the fee behind that id is not in the stream.
    gap = spark.sql(
        f"SELECT count(*) AS referenced FROM {table} WHERE balance_transaction_id IS NOT NULL"
    ).collect()[0]
    print(
        f"\n{gap['referenced']} rows name a balance transaction the event stream does not carry."
        "\n  That is the join phase 4 makes against the balance transaction list, not a defect."
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Deduplicate and type bronze into silver.")
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--checkpoint", default=DEFAULT_CHECKPOINT)
    parser.add_argument(
        "--await-events",
        action="store_true",
        help="keep running as bronze grows, instead of draining and exiting",
    )
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("silver-events").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark, args.table)

    stream = spark.readStream.format("iceberg").load(args.source)

    writer = stream.writeStream.foreachBatch(merge_batch(args.table)).option(
        "checkpointLocation", args.checkpoint
    )
    query = (
        writer.trigger(processingTime="10 seconds").start()
        if args.await_events
        else writer.trigger(availableNow=True).start()
    )

    query.awaitTermination()
    report(spark, args.source, args.table)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
