"""Phase 2: the deduplicated event log into one row per thing.

`silver.events` is a log: many events about one charge. The ledger wants the charge. This job
projects the log onto four tables, taking each entity's state from its **latest event by event
time**, which is the sentence the out-of-order failure turns on. A refund that arrives before the
charge it refunds still loses to the charge on arrival order and still wins on event time, and
event time is the only one of the two the processor guarantees anything about.

**The projection is recomputed, not maintained.** A streaming "latest per key" has to hold state
for every key it has ever seen, because the event that corrects one can land at any time, and a
dispute lands weeks later by design. Recomputing over a deduplicated log is bounded by the log
instead of by a guess about how long to remember, and the log is already the thing this project
promises to be able to reproduce a closed period from. Same argument as the gold layer in section
2 of the design, one layer earlier.

**`created` is the entity's, not the last event's.** A charge is dated by when the charge was
created, which is a different column from when the event that last touched it happened. Taking the
latter would date a charge by the refund that arrived five days later, move it into a different
partition, and quietly change what every daily report says about the day it was actually taken.

`silver.balance_transactions` will look thin, and that is the finding rather than a bug. A webhook
carries `balance_transaction` as an id; only a dispute embeds the object. Everything else is a
reference the event stream cannot resolve, which is exactly what phase 4 joins against the balance
transaction list to close.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

DEFAULT_SOURCE = "lakehouse.silver.events"
DEFAULT_NAMESPACE = "lakehouse.silver"

# Latest event per entity, by event time. The tiebreak on event_id is not cosmetic: without a
# total order the projection would differ between two runs over identical input, and a ledger you
# cannot rebuild identically is not a ledger you can reconcile.
LATEST = """
    SELECT *, row_number() OVER (
        PARTITION BY {key} ORDER BY created DESC, event_id DESC
    ) AS _rank,
    count(*) OVER (PARTITION BY {key}) AS events
    FROM {source}
    WHERE entity_type = '{entity}' AND {key} IS NOT NULL
"""

CHARGES = """
CREATE OR REPLACE TABLE {ns}.charges
USING iceberg
PARTITIONED BY (days(created))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
AS SELECT
    entity_id              AS charge_id,
    payment_intent_id,
    entity_created         AS created,
    amount,
    currency,
    status,
    balance_transaction_id,
    coalesce(amount_refunded, 0) AS amount_refunded,
    coalesce(refunded, false)    AS refunded,
    event_type             AS last_event_type,
    created                AS last_event_at,
    events
FROM ({latest}) WHERE _rank = 1
"""

REFUNDS = """
CREATE OR REPLACE TABLE {ns}.refunds
USING iceberg
PARTITIONED BY (days(created))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
AS SELECT
    entity_id              AS refund_id,
    charge_id,
    payment_intent_id,
    entity_created         AS created,
    amount,
    currency,
    status,
    balance_transaction_id,
    reason,
    created                AS last_event_at,
    events
FROM ({latest}) WHERE _rank = 1
"""

DISPUTES = """
CREATE OR REPLACE TABLE {ns}.disputes
USING iceberg
PARTITIONED BY (days(created))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
AS SELECT
    entity_id              AS dispute_id,
    charge_id,
    payment_intent_id,
    entity_created         AS created,
    amount,
    currency,
    status,
    reason,
    created                AS last_event_at,
    events
FROM ({latest}) WHERE _rank = 1
"""

# Only the dispute payloads carry these expanded, so this is everything the event stream can say
# about money that actually moved.
BALANCE_TRANSACTIONS = """
CREATE OR REPLACE TABLE {ns}.balance_transactions
USING iceberg
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
AS SELECT
    txn.id                                          AS balance_transaction_id,
    txn.source                                      AS source_id,
    event_id                                        AS source_event_id,
    txn.type,
    txn.reporting_category,
    txn.amount,
    txn.fee,
    txn.net,
    txn.currency,
    CAST(txn.created AS TIMESTAMP)                  AS created,
    CAST(txn.available_on AS TIMESTAMP)             AS available_on,
    txn.status
FROM (
    SELECT event_id, explode(
        from_json(
            payload,
            'STRUCT<data: STRUCT<object: STRUCT<balance_transactions: ARRAY<STRUCT<
                 id: STRING, amount: BIGINT, fee: BIGINT, net: BIGINT, currency: STRING,
                 available_on: BIGINT, created: BIGINT, status: STRING, type: STRING,
                 reporting_category: STRING, source: STRING>>>>>'
        ).data.object.balance_transactions
    ) AS txn
    FROM ({latest}) WHERE _rank = 1
)
"""


def build(spark: SparkSession, source: str, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    charges = LATEST.format(source=source, entity="charge", key="entity_id")
    refunds = LATEST.format(source=source, entity="refund", key="entity_id")
    disputes = LATEST.format(source=source, entity="dispute", key="entity_id")

    spark.sql(CHARGES.format(ns=namespace, latest=charges))
    spark.sql(REFUNDS.format(ns=namespace, latest=refunds))
    spark.sql(DISPUTES.format(ns=namespace, latest=disputes))
    spark.sql(BALANCE_TRANSACTIONS.format(ns=namespace, latest=disputes))


def report(spark: SparkSession, source: str, namespace: str) -> None:
    print()
    for name in ("charges", "refunds", "disputes", "balance_transactions"):
        count = spark.sql(f"SELECT count(*) AS n FROM {namespace}.{name}").collect()[0]["n"]
        print(f"  {count:>7}  {namespace}.{name}")

    # Every posting the design's ledger model makes comes off a balance transaction, so the share
    # of referenced ones the stream can actually resolve is the size of the phase 4 join.
    row = spark.sql(
        f"""
        SELECT
            (SELECT count(*) FROM {source} WHERE balance_transaction_id IS NOT NULL) AS referenced,
            (SELECT count(*) FROM {namespace}.balance_transactions)                  AS carried
        """
    ).collect()[0]
    print(
        f"\n{row['referenced']} events name a balance transaction; the stream carries"
        f" {row['carried']} of them expanded."
        "\n  A webhook does not carry the money. That gap is the join phase 4 exists to make."
    )

    print("\nthe out-of-order check, on real rows:")
    late = spark.sql(
        f"""
        SELECT count(*) AS n
        FROM {namespace}.disputes d JOIN {namespace}.charges c USING (charge_id)
        WHERE d.created > c.created
        """
    ).collect()[0]["n"]
    total = spark.sql(f"SELECT count(*) AS n FROM {namespace}.disputes").collect()[0]["n"]
    print(f"  {late} of {total} disputes are dated after the charge they contest")
    print("  ordered by event time per entity, which is not the order they arrived in")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Project the event log onto one row per entity.")
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("silver-entities").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    build(spark, args.source, args.namespace)
    report(spark, args.source, args.namespace)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
