"""The fixture store has to be idempotent: a redelivery must not become a second fixture."""

from __future__ import annotations

import json

from payment_ledger.capture import fixture_path, write_fixture

EVENT = {
    "id": "evt_3MtwBwLkdIwHu7ix28a3tqPa",
    "type": "charge.succeeded",
    "created": 1_700_000_000,
    "data": {"object": {"id": "ch_3MtwBw", "amount": 2000, "currency": "usd"}},
}


def test_fixture_path_groups_by_event_type(tmp_path):
    path = fixture_path(tmp_path, "charge.dispute.created", "evt_1")
    assert path == tmp_path / "charge.dispute.created" / "evt_1.json"


def test_path_segments_from_the_wire_cannot_escape_the_fixtures_dir(tmp_path):
    """Both segments arrive in a request body, so neither is trusted as a path."""
    path = fixture_path(tmp_path, "../../etc", "../../../passwd")
    assert path.parent.parent == tmp_path
    assert ".." not in path.parts


def test_first_capture_writes_the_event(tmp_path):
    path, already_seen = write_fixture(tmp_path, EVENT)

    assert already_seen is False
    assert json.loads(path.read_text(encoding="utf-8")) == EVENT


def test_redelivery_rewrites_in_place_and_is_reported(tmp_path):
    write_fixture(tmp_path, EVENT)
    path, already_seen = write_fixture(tmp_path, EVENT)

    assert already_seen is True
    assert list(tmp_path.rglob("*.json")) == [path]


def test_distinct_events_of_one_type_are_separate_fixtures(tmp_path):
    write_fixture(tmp_path, EVENT)
    write_fixture(tmp_path, {**EVENT, "id": "evt_other"})

    assert len(list((tmp_path / "charge.succeeded").glob("*.json"))) == 2
