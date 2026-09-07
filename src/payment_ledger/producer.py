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


def iter_payloads(root: Path) -> Iterator[tuple[Path, bytes]]:
    """Every captured event, in a stable order, as the exact bytes on disk."""
    for path in sorted(root.rglob("*.json")):
        yield path, path.read_bytes()


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


def publish(
    root: Path,
    *,
    bootstrap: str,
    topic: str,
    limit: int | None = None,
    dry_run: bool = False,
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

    for index, (path, raw) in enumerate(iter_payloads(root)):
        if limit is not None and index >= limit:
            break
        try:
            event = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"{path} is not valid json: {exc}") from exc

        counts[str(event.get("type", "unknown"))] += 1
        if producer is None:
            continue

        producer.produce(
            topic=topic,
            key=ledger_key(event).encode("utf-8"),
            value=raw,
            headers=_headers(event),
            on_delivery=on_delivery,
        )
        producer.poll(0)

    if producer is not None:
        remaining = producer.flush(timeout=30)
        if remaining:
            raise SystemExit(f"{remaining} messages were still unsent after 30s. Is Kafka up?")
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
    parser.add_argument("--limit", type=int, default=None, help="publish at most this many")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="read and count the fixtures without contacting a broker",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    root = fixtures_root()
    if not root.exists():
        print(f"no fixtures at {root}. run `make capture` first, or clone the committed ones.")
        return 1

    counts = publish(
        root,
        bootstrap=args.bootstrap,
        topic=args.topic,
        limit=args.limit,
        dry_run=args.dry_run,
    )

    total = sum(counts.values())
    where = "counted" if args.dry_run else f"published to {args.topic} on {args.bootstrap}"
    print(f"{total} events {where}")
    for event_type, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {count:>4}  {event_type}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
