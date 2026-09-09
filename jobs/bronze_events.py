"""Phase 1: the Kafka topic into `bronze.events`.

Bronze is the arrival record, not the truth. It answers "what was delivered, when, on which
offset", and it answers it for every delivery including the ones that are duplicates of each
other. Nothing is deduplicated, corrected or reshaped here.

That is not laziness about quality. The duplicate-delivery failure is *detected* by bronze holding
more rows than silver, so a bronze layer that quietly collapsed a redelivery would destroy the
evidence it exists to keep. Same reason the payload is stored as the exact string that arrived:
the moment the envelope is reparsed and rewritten, the pipeline is testing our JSON writer instead
of the processor's.

Run it with `make bronze`, which submits this inside the Spark container so the host needs no JVM.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

DEFAULT_TABLE = "lakehouse.bronze.events"
DEFAULT_TOPIC = "stripe.events.raw"
DEFAULT_BOOTSTRAP = "kafka:19092"
DEFAULT_CHECKPOINT = "/opt/payment-ledger/checkpoints/bronze_events"

# What one micro-batch may take off the topic. It matters only when there is a backlog: a live
# stream never reaches it, and a first run against a topic holding millions reaches it at once.
DEFAULT_MAX_OFFSETS_PER_BATCH = 500_000

# Only the envelope. The body of `data.object` differs per event type, and typing it here would
# mean guessing at seven object shapes; that belongs in silver, where each type gets its own
# table and its own schema derived from a real payload.
ENVELOPE = StructType(
    [
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField("created", LongType()),
        StructField("livemode", BooleanType()),
        StructField("api_version", StringType()),
        StructField(
            "request",
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("idempotency_key", StringType()),
                ]
            ),
        ),
    ]
)

DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    event_id         STRING    COMMENT 'evt_..., the delivery identity and the dedup key',
    event_type       STRING,
    created          TIMESTAMP COMMENT 'event time, as the processor stamped it',
    livemode         BOOLEAN,
    api_version      STRING,
    request_id       STRING,
    idempotency_key  STRING    COMMENT 'set when the event came from an API call we made',
    payload          STRING    COMMENT 'the delivered json, byte for byte',
    kafka_key        STRING    COMMENT 'the partition key: the charge the event is about',
    kafka_partition  INT,
    kafka_offset     BIGINT,
    kafka_timestamp  TIMESTAMP COMMENT 'when the broker took it',
    ingested_at      TIMESTAMP COMMENT 'when this job read it'
)
USING iceberg
PARTITIONED BY (days(ingested_at))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
"""


def build_session(app_name: str = "bronze-events") -> SparkSession:
    """Catalog and object store wiring live in `conf/spark-defaults.conf`, not here."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def ensure_table(spark: SparkSession, table: str) -> None:
    """Create the namespace and table explicitly rather than letting the writer infer them.

    Same reason the topic is created by `kafka-init` instead of by auto-creation: an inferred
    schema turns a typo into an empty table nobody notices, and partitioning is a decision worth
    writing down. `ingested_at` is the partition column because bronze is ordered by arrival. An
    event that lands three weeks late belongs to the day it arrived, not to the day it describes.
    Event-time partitioning is silver's problem, and it is a different problem.
    """
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(DDL.format(table=table))


def envelope_columns(raw):
    """Kafka records into bronze rows: the envelope parsed out, the payload left alone."""
    parsed = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).withColumn("event", F.from_json(F.col("payload"), ENVELOPE))

    return parsed.select(
        F.col("event.id").alias("event_id"),
        F.col("event.type").alias("event_type"),
        F.col("event.created").cast("timestamp").alias("created"),
        F.col("event.livemode").alias("livemode"),
        F.col("event.api_version").alias("api_version"),
        F.col("event.request.id").alias("request_id"),
        F.col("event.request.idempotency_key").alias("idempotency_key"),
        F.col("payload"),
        F.col("kafka_key"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    )


def report(spark: SparkSession, table: str) -> None:
    """What bronze holds, and how far it already is from what silver will hold.

    The gap between the two counts is the duplicate-delivery failure, measured rather than
    asserted. On a first run over the committed fixtures it is zero, because the capture wrote one
    file per event id. It stops being zero the moment the chaos suite starts replaying.
    """
    row = spark.sql(
        f"SELECT count(*) AS deliveries, count(DISTINCT event_id) AS events FROM {table}"
    ).collect()[0]
    print(f"\nbronze holds {row['deliveries']} deliveries of {row['events']} distinct events")
    if row["deliveries"] != row["events"]:
        print(f"  {row['deliveries'] - row['events']} of them are redeliveries, kept on purpose")

    print("\nby type:")
    rows = spark.sql(
        f"SELECT event_type, count(*) AS n FROM {table} GROUP BY event_type ORDER BY n DESC, 1"
    ).collect()
    for r in rows:
        print(f"  {r['n']:>4}  {r['event_type']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stream the raw event topic into bronze.")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--checkpoint", default=DEFAULT_CHECKPOINT)
    parser.add_argument(
        "--max-offsets-per-batch",
        type=int,
        default=DEFAULT_MAX_OFFSETS_PER_BATCH,
        help="events one micro-batch may take off the topic (default: %(default)s)",
    )
    parser.add_argument(
        "--starting-offsets",
        default="earliest",
        help="only consulted on the first run; after that the checkpoint decides",
    )
    parser.add_argument(
        "--await-events",
        action="store_true",
        help="keep running and write as events arrive, instead of draining and exiting",
    )
    args = parser.parse_args(argv)

    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark, args.table)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        # A micro-batch is bounded here for the same reason it is in silver, and unlike silver's
        # source the Kafka one implements the interface `Trigger.AvailableNow` needs, so the limit
        # is honoured and a backlog is drained a batch at a time rather than in one. Draining four
        # and a half million events in a single batch is an OutOfMemoryError, and the number of
        # events waiting on a topic is not something a job gets to assume.
        .option("maxOffsetsPerTrigger", str(args.max_offsets_per_batch))
        # A record the job cannot read is a real event that has to be looked at, not a reason to
        # skip ahead. Nothing here may silently advance past data it failed to consume.
        .option("failOnDataLoss", "true")
        .load()
    )

    writer = (
        envelope_columns(raw)
        .writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint)
        # The input is not sorted by ingest day, so the writer keeps a file open per partition
        # rather than demanding a global sort it would only have to undo.
        .option("fanout-enabled", "true")
    )

    query = (
        writer.trigger(processingTime="10 seconds").toTable(args.table)
        if args.await_events
        else writer.trigger(availableNow=True).toTable(args.table)
    )

    query.awaitTermination()
    report(spark, args.table)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
