"""What a scenario is allowed to do, and what a verdict is allowed to say.

Two different things are checked here. The first is that each injection damages exactly what it
claims to damage and leaves the rest of the run alone, because a scenario that quietly perturbs a
second thing produces a detection nobody can attribute. The second is that the verdict is strict in
both directions: a detection that did not fire is a failure, and so is a failure nobody asked for.
"""

from __future__ import annotations

import json
import random

import pytest

from payment_ledger import chaos
from payment_ledger import generator as g

RUN = g.Run(transactions=250, seed=1, days=14)


@pytest.fixture(scope="module")
def wave():
    sim = g.simulate(RUN)
    return chaos.Wave(sim.events, sim.balance_transactions, sim.daily_balance())


@pytest.fixture(scope="module")
def source(tmp_path_factory, wave):
    out = tmp_path_factory.mktemp("generated")
    for name in chaos.ARTIFACTS:
        g.write_jsonl(out / f"{name}.jsonl", wave.rows(name))
    return out


def inject(name, wave, seed=1):
    return chaos.SCENARIOS[name].inject(wave.copy(), random.Random(seed))


# --- the suite is a suite -------------------------------------------------------------------------


def test_every_scenario_asserts_something():
    """A scenario with no expectation cannot fail, and a chaos suite whose arms cannot fail is a
    long way to run a pipeline twice."""
    for scenario in chaos.SCENARIOS.values():
        assert scenario.dbt_failures or scenario.metrics, f"{scenario.name} asserts nothing"


def test_every_scenario_says_what_catches_it():
    for scenario in chaos.SCENARIOS.values():
        assert scenario.detection.strip()
        assert scenario.failure.strip()


def test_the_control_arm_injects_nothing(wave):
    waves, _ = inject("baseline", wave)
    assert len(waves) == 1
    for name in chaos.ARTIFACTS:
        assert waves[0].rows(name) == wave.rows(name)


# --- one side, and not the other ------------------------------------------------------------------


@pytest.mark.parametrize("name", ["duplicate_delivery", "out_of_order", "dropped_event"])
def test_a_stream_scenario_leaves_the_processor_alone(name, wave):
    """The anchor is what a divergence is measured against. A scenario that damaged the stream and
    the reported balance together would produce a run that is wrong and reconciles."""
    waves, _ = inject(name, wave)
    assert waves[0].balance_transactions == wave.balance_transactions
    assert waves[0].daily_balance == wave.daily_balance


@pytest.mark.parametrize("name", ["rounding_drift", "reversal_dropped"])
def test_a_list_scenario_leaves_the_stream_and_the_anchor_alone(name, wave):
    waves, _ = inject(name, wave)
    assert waves[0].events == wave.events
    assert waves[0].daily_balance == wave.daily_balance


# --- what each one actually does ------------------------------------------------------------------


def test_a_redelivery_is_the_same_event_again(wave):
    waves, injected = inject("duplicate_delivery", wave)
    stream = waves[0].events

    assert len(stream) == len(wave.events) + injected["redelivered_events"]
    assert {e["id"] for e in stream} == {e["id"] for e in wave.events}
    # Byte for byte, because a redelivery that differs from its original is a different event and
    # deduplicating it would be luck rather than design.
    by_id = {}
    for event in stream:
        by_id.setdefault(event["id"], []).append(json.dumps(event, sort_keys=True))
    for copies in by_id.values():
        assert len(set(copies)) == 1


def test_a_redelivery_arrives_later_than_its_original(wave):
    waves, _ = inject("duplicate_delivery", wave)
    stream = waves[0].events
    seen: dict[str, int] = {}
    gaps = []
    for position, event in enumerate(stream):
        if event["id"] in seen:
            gaps.append(position - seen[event["id"]])
        seen.setdefault(event["id"], position)

    assert gaps, "nothing was redelivered"
    assert all(gap > 1 for gap in gaps), "a copy landed immediately behind its original"


def test_out_of_order_keeps_every_event_and_reverses_it(wave):
    waves, _ = inject("out_of_order", wave)
    assert waves[0].events == list(reversed(wave.events))


def test_a_dropped_charge_takes_its_whole_entity_with_it(wave):
    waves, injected = inject("dropped_event", wave)
    stream = json.dumps(waves[0].events)

    assert injected["silenced_charges"] > 0
    assert injected["removed_events"] >= injected["silenced_charges"]
    assert len(waves[0].events) == len(wave.events) - injected["removed_events"]
    for charge in injected["charge_ids"]:
        assert charge not in stream


def test_a_dropped_charge_is_still_in_the_processors_list(wave):
    """The whole detection depends on this: the money is in the list and the stream never
    mentioned it."""
    waves, injected = inject("dropped_event", wave)
    sources = {t["source"] for t in waves[0].balance_transactions}

    assert injected["blinded_sources"] >= injected["silenced_charges"]
    assert injected["blinded_transactions"] >= injected["blinded_sources"]
    for charge in injected["charge_ids"]:
        assert charge in sources


def test_the_drift_is_one_minor_unit_and_only_on_the_net(wave):
    waves, injected = inject("rounding_drift", wave)
    before = {t["id"]: t for t in wave.balance_transactions}

    drifted = 0
    for txn in waves[0].balance_transactions:
        original = before[txn["id"]]
        if txn == original:
            continue
        drifted += 1
        assert txn["net"] == original["net"] + 1
        assert txn["amount"] == original["amount"]
        assert txn["fee"] == original["fee"]
        assert original["reporting_category"] == "charge"

    assert drifted == injected["drifted_transactions"] > 0


def test_only_a_reversal_goes_missing(wave):
    waves, injected = inject("reversal_dropped", wave)
    kept = {t["id"] for t in waves[0].balance_transactions}
    lost = [t for t in wave.balance_transactions if t["id"] not in kept]

    assert len(lost) == injected["lost_reversals"] > 0
    assert all(t["reporting_category"] == "dispute_reversal" for t in lost)


def test_the_stream_still_says_the_dispute_was_won(wave):
    """Which is what makes it findable. The reversal is expanded inside the closing event, so the
    stream carries a copy of the transaction the list is missing."""
    waves, injected = inject("reversal_dropped", wave)
    kept = {t["id"] for t in waves[0].balance_transactions}
    lost = [t["id"] for t in wave.balance_transactions if t["id"] not in kept]
    stream = json.dumps(waves[0].events)

    assert lost
    assert all(transaction_id in stream for transaction_id in lost)
    assert injected["lost_reversals"] == len(lost)


# --- reproducible, or it is an anecdote -----------------------------------------------------------


@pytest.mark.parametrize("name", sorted(chaos.SCENARIOS))
def test_the_same_seed_injects_the_same_damage(name, wave):
    first, injected = inject(name, wave)
    second, again = inject(name, wave)

    assert injected == again
    for one, other in zip(first, second, strict=True):
        for artifact in chaos.ARTIFACTS:
            assert one.rows(artifact) == other.rows(artifact)


def test_a_different_seed_injects_different_damage(wave):
    """Same amount of damage, somewhere else. A seed that changed nothing would make a suite
    that reruns look like a suite that covers more."""
    first, one = inject("rounding_drift", wave, seed=1)
    second, other = inject("rounding_drift", wave, seed=2)

    assert one["drifted_transactions"] == other["drifted_transactions"]
    assert one["transaction_ids"] != other["transaction_ids"]
    assert first[0].balance_transactions != second[0].balance_transactions


# --- the manifest ---------------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(chaos.SCENARIOS))
def test_building_a_scenario_writes_a_wave_and_its_expectation(name, source, tmp_path):
    out = tmp_path / name
    manifest = chaos.build(name, source, out, seed=1)

    assert manifest["simulated"] is True
    assert manifest["waves"]
    for number in range(1, len(manifest["waves"]) + 1):
        for artifact in chaos.ARTIFACTS:
            assert (out / f"wave{number}" / f"{artifact}.jsonl").exists()
    assert json.loads((out / "scenario.json").read_text(encoding="utf-8")) == manifest


@pytest.mark.parametrize("name", sorted(chaos.SCENARIOS))
def test_an_expectation_on_disk_is_a_number(name, source, tmp_path):
    """A scenario may declare a row count by naming what the injection reports. What lands in the
    manifest has to be the number, or `verify` would compare a count against a label."""
    manifest = chaos.build(name, source, tmp_path / name, seed=1)
    for rows in manifest["expect"]["dbt_failures"].values():
        assert rows is None or isinstance(rows, int)


def test_the_control_arm_is_the_generated_run_byte_for_byte(source, tmp_path):
    manifest = chaos.build("baseline", source, tmp_path / "baseline", seed=1)
    assert manifest["injected"] == {"injected": "nothing"}
    for artifact in chaos.ARTIFACTS:
        original = (source / f"{artifact}.jsonl").read_bytes()
        written = (tmp_path / "baseline" / "wave1" / f"{artifact}.jsonl").read_bytes()
        assert written == original


# --- the verdict ----------------------------------------------------------------------------------


HEALTHY = {
    "expect": {
        "dbt_failures": {"assert_entries_balance": 3},
        "metrics": [["silver_events", "==", "baseline:silver_events"]],
    }
}


def test_a_detection_that_fired_as_described_is_no_problem():
    problems = chaos.verify(
        HEALTHY, {"silver_events": 10}, {"assert_entries_balance": 3}, {"silver_events": 10}
    )
    assert problems == []


def test_a_detection_that_did_not_fire_is_a_problem():
    problems = chaos.verify(HEALTHY, {"silver_events": 10}, {}, {"silver_events": 10})
    assert any("was supposed to fail" in problem for problem in problems)


def test_a_detection_that_fired_on_the_wrong_rows_is_a_problem():
    problems = chaos.verify(
        HEALTHY, {"silver_events": 10}, {"assert_entries_balance": 4}, {"silver_events": 10}
    )
    assert any("expected 3" in problem for problem in problems)


def test_a_failure_nobody_asked_for_is_a_problem():
    """The half of the check that a suite watching only for damage would not have."""
    failures = {"assert_entries_balance": 3, "assert_no_unexplained_difference": 7}
    problems = chaos.verify(HEALTHY, {"silver_events": 10}, failures, {"silver_events": 10})
    assert any("not supposed to fail" in problem for problem in problems)


def test_a_count_that_moved_against_the_baseline_is_a_problem():
    problems = chaos.verify(
        HEALTHY, {"silver_events": 9}, {"assert_entries_balance": 3}, {"silver_events": 10}
    )
    assert any("silver_events is 9" in problem for problem in problems)


def test_a_table_the_run_refused_to_publish_compares_equal_to_nothing_else():
    """A model downstream of a failed invariant is skipped, so its count is absent rather than
    zero, and absent is a thing a scenario is allowed to require."""
    manifest = {"expect": {"dbt_failures": {}, "metrics": [["gold_close_rows", "==", None]]}}
    assert chaos.verify(manifest, {"gold_close_rows": None}, {}, {}) == []
    assert chaos.verify(manifest, {"gold_close_rows": 0}, {}, {}) != []


def test_a_generated_test_is_named_by_its_name_and_not_by_its_checksum():
    """dbt appends a checksum to the id of a test it generated from a yaml rule, and not to one
    written as a file. The verdict names both the same way, because a failure nobody expected is
    exactly the one somebody has to be able to find."""
    assert chaos.test_name("test.payment_ledger.assert_entries_balance") == "assert_entries_balance"
    assert (
        chaos.test_name("test.payment_ledger.accepted_values_ledger_postings_account__x.d8d31996a3")
        == "accepted_values_ledger_postings_account__x"
    )


# --- the late one, which is the only scenario with a second wave ----------------------------------


def test_a_late_arrival_is_two_waves_of_the_same_run(wave):
    waves, injected = inject("late_arrival", wave)
    assert len(waves) == 2

    first, second = waves
    assert injected["late_transactions"] > 0
    assert injected["held_events"] > 0
    # Nothing is lost between them: the whole run is delivered, in two goes.
    assert len(first.events) + len(second.events) == len(wave.events)
    assert second.balance_transactions == wave.balance_transactions
    assert second.daily_balance == wave.daily_balance


def test_the_first_wave_is_the_world_as_it_was_when_the_day_closed(wave):
    """The anchor of the first wave is not the run's anchor. At the moment of that close the
    processor had not reported the late movement either, so comparing the books against the final
    balance would fail for a reason that has nothing to do with lateness."""
    waves, injected = inject("late_arrival", wave)
    first = waves[0]

    assert (
        len(first.balance_transactions)
        == len(wave.balance_transactions) - injected["late_transactions"]
    )
    assert first.daily_balance != wave.daily_balance
    assert first.daily_balance == g.walk_daily_balance(first.balance_transactions)


def test_what_is_held_back_is_held_back_on_both_sides(wave):
    """A movement whose events arrived but whose row did not, or the other way round, would be a
    coverage gap rather than a late arrival, and it would fail an invariant instead of restating a
    day."""
    waves, _ = inject("late_arrival", wave)
    first, second = waves

    early = json.dumps(first.events)
    for txn in second.balance_transactions:
        if txn not in first.balance_transactions:
            assert txn["source"] not in early


# --- the design says six --------------------------------------------------------------------------


def test_every_failure_the_design_lists_has_a_scenario():
    """Section 6 of the design is a table of six failures and what catches each. It was a table of
    claims until this phase, and the way it goes back to being one is a row nobody wired up."""
    design = (g.config.REPO_ROOT / "docs" / "DESIGN.md").read_text(encoding="utf-8")
    section = design.split("## 6. The six failures")[1].split("\n## ")[0]
    listed = {
        line.split("|")[1].strip()
        for line in section.splitlines()
        if line.startswith("|") and not set(line) <= set("|- ")
    } - {"Failure"}

    covered = {s.failure for s in chaos.SCENARIOS.values() if s.name != "baseline"}
    assert len(listed) == 6
    assert listed == covered


def test_the_number_of_waves_is_declared_and_kept(wave):
    """The DAG draws its tasks from the declaration, before anything has been injected. An
    injector that produced a wave the declaration does not know about would deliver into a task
    that is not there."""
    for name, scenario in chaos.SCENARIOS.items():
        waves, _ = inject(name, wave)
        assert len(waves) == scenario.waves, name
