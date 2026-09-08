"""Phase 1: put the captured payloads on the wire.

The producer reads `fixtures/events/` and publishes each file to Kafka **byte for byte**. It does
not parse and re-serialise the payload, because a fixture that is reformatted on the way to the
broker is no longer the payload the processor sent, and every schema decision downstream would be
made against our own JSON writer rather than against Stripe's.

Running it needs no Stripe account. That is the whole reason phase 0 committed the fixtures.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from collections.abc import Iterator
from pathlib import Path

from payment_ledger import config

log = logging.getLogger("producer")

DEFAULT_TOPIC = "stripe.events.raw"
DEFAULT_BOOTSTRAP = "localhost:9092"


def fixtures_root() -> Path:
    root = Path(config.setting("FIXTURES_DIR", "fixtures/events") or "fixtures/events")
    return root if root.is_absolute() else config.REPO_ROOT / root


def iter_payloads(source: Path) -> Iterator[tuple[str, bytes]]:
    """Every event to publish, in a stable order, as the exact bytes it is stored as.

    Two sources, one shape. A directory is the captured fixture tree, one file per event, which is
    what phase 0 committed. A `.jsonl` file is a generated run, one event per line, which is where
    volume comes from. Neither is parsed and re-serialised on the way out: what reaches the broker
    is what is on disk.
    """
    if source.is_dir():
        for path in sorted(source.rglob("*.json")):
            yield str(path), path.read_bytes()
        return

    with source.open("rb") as handle:
        for number, line in enumerate(handle, start=1):
            line = line.strip()
            if line:
                yield f"{source}:{number}", line


def ledger_key(event: dict) -> str:
    """The partition key: the charge an event is ultimately about.

    A dispute is not keyed by the dispute id but by the charge it disputes, so the whole life of
    one charge (succeeded, refunded, disputed, closed) lands in one partition and arrives in
    order. Nothing downstream is allowed to depend on that. Webhooks are unordered by contract and
    silver sorts by event time per entity regardless. It is worth doing anyway because it makes
    the duplicate of an event sit next to its original, which is the cheapest place to catch one.
    """
    obj = event.get("data", {}).get("object", {})
    if not isinstance(obj, dict):
        return str(event.get("id", "unknown"))

    for field in ("charge", "payment_intent", "id"):
        value = obj.get(field)
        if isinstance(value, str) and value:
            return value
    return str(event.get("id", "unknown"))


def _headers(event: dict) -> list[tuple[str, bytes]]:
    """Type and id as headers, so a consumer can route without parsing the body."""
    return [
        ("event_id", str(event.get("id", "")).encode("utf-8")),
        ("event_type", str(event.get("type", "")).encode("utf-8")),
    ]


def deliver(producer, **message) -> None:
    """Hand one message to the client, waiting when its queue is full.

    `produce` is asynchronous: it enqueues locally and returns. At fifty nine fixtures, or at a few
    thousand generated events, that queue never fills and this function looks like ceremony. At
    eight hundred thousand the broker is the slower end, librdkafka's queue hits its hundred
    thousand message ceiling, and `produce` raises `BufferError` rather than blocking.

    Waiting for the queue to drain is the back pressure. Raising the ceiling would move the failure
    rather than remove it, and dropping the message would lose an event that the ledger notices
    much later, as a gap against the processor's balance, which is the most expensive way to find
    out about it.
    """
    while True:
        try:
            producer.produce(**message)
            return
        except BufferError:
            producer.poll(0.5)


def publish(
    root: Path,
    *,
    bootstrap: str,
    topic: str,
    limit: int | None = None,
    dry_run: bool = False,
    progress: int = 0,
) -> Counter:
    """Publish every fixture. Returns a count by event type."""
    counts: Counter = Counter()
    producer = None
    if not dry_run:
        from confluent_kafka import Producer

        producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                # The fixtures are the ledger's input. A silently dropped one would show up much
                # later as an unexplained gap against the processor's balance, so the producer is
                # configured to fail rather than to be fast.
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 20,
            }
        )

    failures: list[str] = []

    def on_delivery(err, msg) -> None:
        if err is not None:
            failures.append(f"{msg.key()!r}: {err}")

    for index, (where, raw) in enumerate(iter_payloads(root)):
        if limit is not None and index >= limit:
            break
        try:
            event = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"{where} is not valid json: {exc}") from exc

        counts[str(event.get("type", "unknown"))] += 1
        if producer is None:
            continue

        deliver(
            producer,
            topic=topic,
            key=ledger_key(event).encode("utf-8"),
            value=raw,
            headers=_headers(event),
            on_delivery=on_delivery,
        )
        producer.poll(0)
        if progress and (index + 1) % progress == 0:
            log.info("%s events queued", f"{index + 1:,}")

    if producer is not None:
        # Long enough for the client to hand over what a large run leaves in flight. The earlier
        # thirty seconds was sized for fifty nine fixtures and would abandon a queue of hundreds of
        # thousands, reporting as unsent messages that were merely still on their way.
        remaining = producer.flush(timeout=300)
        if remaining:
            raise SystemExit(f"{remaining} messages were still unsent after 300s. Is Kafka up?")
    if failures:
        raise SystemExit("deliveries failed:\n  " + "\n  ".join(failures))

    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish the captured fixtures to Kafka.")
    parser.add_argument(
        "--bootstrap",
        default=config.setting("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP),
        help="broker to publish to (default: %(default)s)",
    )
    parser.add_argument(
        "--topic",
        default=config.setting("KAFKA_TOPIC", DEFAULT_TOPIC),
        help="topic to publish to (default: %(default)s)",
    )
    parser.add_argument(
        "--events",
        type=Path,
        default=None,
        help="the captured fixture directory, or a generated .jsonl (default: the fixtures)",
    )
    parser.add_argument("--limit", type=int, default=None, help="publish at most this many")
    parser.add_argument(
        "--progress",
        type=int,
        default=250_000,
        help="say how far along every this many events, or 0 for silence (default: %(default)s)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="read and count the fixtures without contacting a broker",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    root = args.events or fixtures_root()
    if not root.exists():
        print(
            f"nothing to publish at {root}.\n"
            "Either clone the committed fixtures, or generate a run with"
            " `python -m payment_ledger.generator`."
        )
        return 1

    counts = publish(
        root,
        bootstrap=args.bootstrap,
        topic=args.topic,
        limit=args.limit,
        dry_run=args.dry_run,
        progress=args.progress,
    )

    total = sum(counts.values())
    where = "counted" if args.dry_run else f"published to {args.topic} on {args.bootstrap}"
    print(f"{total} events from {root.name} {where}")
    for event_type, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {count:>4}  {event_type}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
