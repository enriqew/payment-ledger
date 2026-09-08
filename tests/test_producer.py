"""What the producer puts on the wire, and where.

None of this needs a broker. The one part that does, the delivery itself, is covered by the
`--dry-run` path plus a recording double, because a test that only passes when Kafka is up is a
test CI will end up skipping.
"""

from __future__ import annotations

import json

import pytest

from payment_ledger import producer
from payment_ledger.config import REPO_ROOT


def event(**overrides) -> dict:
    base = {
        "id": "evt_1",
        "type": "charge.succeeded",
        "data": {"object": {"id": "ch_1"}},
    }
    base.update(overrides)
    return base


# --- the partition key ------------------------------------------------------------------------


def test_a_charge_is_keyed_by_itself():
    assert producer.ledger_key(event()) == "ch_1"


def test_a_dispute_is_keyed_by_the_charge_it_disputes():
    """The point of the key. A dispute keyed by `dp_...` would land in a partition of its own,
    away from the charge whose money it moves."""
    disputed = event(
        type="charge.dispute.created",
        data={"object": {"id": "dp_1", "charge": "ch_1"}},
    )
    assert producer.ledger_key(disputed) == "ch_1"


def test_a_refund_is_keyed_by_the_charge_it_refunds():
    refund = event(type="refund.created", data={"object": {"id": "re_1", "charge": "ch_1"}})
    assert producer.ledger_key(refund) == "ch_1"


def test_a_payment_intent_event_is_keyed_by_the_intent_when_there_is_no_charge():
    intent = event(type="payment_intent.created", data={"object": {"id": "pi_1"}})
    assert producer.ledger_key(intent) == "pi_1"


def test_an_event_about_nothing_falls_back_to_its_own_id():
    """`balance.available` carries a balance object with no id at all."""
    balance = event(type="balance.available", data={"object": {"available": []}})
    assert producer.ledger_key(balance) == "evt_1"


def test_a_payload_with_no_object_still_produces_a_key():
    assert producer.ledger_key({"id": "evt_9", "data": {"object": None}}) == "evt_9"


# --- what is read off disk --------------------------------------------------------------------


def test_payloads_reach_kafka_as_the_exact_bytes_on_disk(tmp_path):
    """Not reparsed and not reformatted. A fixture rewritten by our own json writer would make
    every schema decision downstream a decision about our writer."""
    body = b'{"id":"evt_1","type":"charge.succeeded","data":{"object":{"id":"ch_1"}}}\n'
    (tmp_path / "charge.succeeded").mkdir()
    (tmp_path / "charge.succeeded" / "evt_1.json").write_bytes(body)

    assert [raw for _, raw in producer.iter_payloads(tmp_path)] == [body]


def test_malformed_json_stops_the_run(tmp_path):
    (tmp_path / "evt_bad.json").write_bytes(b"{not json")
    with pytest.raises(SystemExit):
        producer.publish(tmp_path, bootstrap="unused", topic="t", dry_run=True)


# --- the delivery -----------------------------------------------------------------------------


class RecordingProducer:
    """Stands in for confluent_kafka.Producer, recording instead of sending."""

    def __init__(self, config):
        self.config = config
        self.produced = []

    def produce(self, *, topic, key, value, headers, on_delivery):
        self.produced.append((topic, key, value, dict(headers)))
        on_delivery(None, None)

    def poll(self, _timeout):
        return 0

    def flush(self, timeout=None):
        return 0


@pytest.fixture
def recorded(monkeypatch):
    """Intercept the import inside `publish` so no broker is contacted."""
    made: list[RecordingProducer] = []

    class FakeModule:
        @staticmethod
        def Producer(config):  # noqa: N802 - mirrors the confluent_kafka name
            made.append(RecordingProducer(config))
            return made[-1]

    monkeypatch.setitem(__import__("sys").modules, "confluent_kafka", FakeModule)
    return made


def test_every_fixture_is_published_once_with_its_key_and_headers(tmp_path, recorded):
    body = json.dumps(event(type="charge.dispute.created", data={"object": {"charge": "ch_7"}}))
    (tmp_path / "evt_1.json").write_text(body, encoding="utf-8")

    producer.publish(tmp_path, bootstrap="broker:9092", topic="stripe.events.raw")

    (topic, key, value, headers) = recorded[0].produced[0]
    assert topic == "stripe.events.raw"
    assert key == b"ch_7"
    assert value == body.encode("utf-8")
    assert headers["event_id"] == b"evt_1"
    assert headers["event_type"] == b"charge.dispute.created"


def test_the_producer_is_configured_to_fail_rather_than_lose_a_message(tmp_path, recorded):
    """A dropped event surfaces much later as an unexplained gap against the processor's own
    balance, which is one of the six failures and a miserable one to debug backwards."""
    (tmp_path / "evt_1.json").write_text(json.dumps(event()), encoding="utf-8")
    producer.publish(tmp_path, bootstrap="broker:9092", topic="t")

    config = recorded[0].config
    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True


def test_dry_run_contacts_no_broker(tmp_path, recorded):
    (tmp_path / "evt_1.json").write_text(json.dumps(event()), encoding="utf-8")
    counts = producer.publish(tmp_path, bootstrap="broker:9092", topic="t", dry_run=True)

    assert counts == {"charge.succeeded": 1}
    assert recorded == []


# --- against what is actually committed ---------------------------------------------------------


def test_the_committed_fixtures_group_their_disputes_onto_the_charge():
    """Over the real payloads, not invented ones: every dispute event captured in phase 0 keys to
    a charge, so a charge and the dispute against it share a partition."""
    root = producer.fixtures_root()
    disputes = sorted((root / "charge.dispute.created").glob("*.json")) if root.exists() else []
    if not disputes:
        pytest.skip("no dispute fixtures captured")

    for path in disputes:
        assert producer.ledger_key(json.loads(path.read_text(encoding="utf-8"))).startswith("ch_")


def test_the_topic_the_producer_writes_to_is_the_one_the_stack_creates():
    """The reason `kafka-init` creates the topic instead of relying on auto-creation: a typo in a
    topic name would otherwise be a stream that is silently empty rather than an error."""
    compose = (REPO_ROOT / "docker" / "docker-compose.yml").read_text(encoding="utf-8")
    assert f"--topic {producer.DEFAULT_TOPIC}" in compose


class QueueFullOnce:
    """A client whose local queue is full the first time and drains on the next poll."""

    def __init__(self, full_for: int = 1):
        self.full_for = full_for
        self.produced: list[dict] = []
        self.polls = 0

    def produce(self, **message):
        if self.full_for > 0:
            self.full_for -= 1
            raise BufferError("Local: Queue full")
        self.produced.append(message)

    def poll(self, timeout=0):
        self.polls += 1


def test_a_full_queue_is_waited_out_rather_than_dropped():
    """`produce` enqueues locally and raises once the broker is the slower end. At a few thousand
    events that never happens; at eight hundred thousand it does, and an event dropped here is one
    the ledger only misses days later as a gap against the processor's balance."""
    client = QueueFullOnce(full_for=3)
    producer.deliver(client, topic="t", key=b"k", value=b"v")

    assert len(client.produced) == 1
    assert client.polls == 3, "the queue was not given time to drain between attempts"


def test_a_client_that_accepts_at_once_is_not_polled():
    client = QueueFullOnce(full_for=0)
    producer.deliver(client, topic="t", key=b"k", value=b"v")

    assert len(client.produced) == 1
    assert client.polls == 0
