"""The wiring of the suite, checked without a container in sight.

Running an arm needs Docker, so what CI can catch is the mistake that makes the whole exercise
worthless: an arm that writes into the tables another arm reads. Every table, topic and checkpoint
an arm touches has to carry that arm's name, and if one of them does not, six scenarios quietly
become six runs over the same lakehouse agreeing with each other.

The rest of what is checked here is drift between things that have to say the same word in two
places: the marker the report job prints and the one the suite reads, the measurements the job
takes and the metrics the scenarios assert on, the jobs the suite submits and the files in `jobs/`.
"""

from __future__ import annotations

import json
import re

from payment_ledger import chaos, chaos_run
from payment_ledger.config import REPO_ROOT

REPORT_JOB = (REPO_ROOT / "jobs" / "chaos_report.py").read_text(encoding="utf-8")
SCENARIO = "dropped_event"


def flat(steps) -> str:
    """Every step of an arm as one string, which is all these checks need to look at."""
    return " ".join(" ".join(chaos_run.compose_argv(step)) for step in steps)


def every_step(scenario: str = SCENARIO):
    return [
        *chaos_run.wave_steps(scenario, "wave1"),
        chaos_run.dbt_step(scenario),
        chaos_run.report_step(scenario),
    ]


# --- an arm keeps to itself -----------------------------------------------------------------------


def test_every_table_an_arm_touches_carries_its_name():
    """The one that matters. A table without the arm's prefix is an arm reading somebody else's
    lakehouse, and the suite would report agreement it did not earn."""
    tables = re.findall(r"lakehouse\.([\w]+)\.", flat(every_step()))

    assert tables
    for table in tables:
        assert table.startswith(f"{SCENARIO}_"), f"lakehouse.{table} is not this arm's"


def test_the_dbt_build_reads_and_writes_this_arms_schemas_only():
    step = chaos_run.dbt_step(SCENARIO)
    variables = json.loads(step.command[step.command.index("--vars") + 1])

    assert step.env == {"DBT_SCHEMA": f"{SCENARIO}_gold"}
    assert variables == {"silver_schema": f"{SCENARIO}_silver"}
    assert step.tolerate, "a failing invariant is the expected outcome of four of the six"


def test_the_checkpoints_of_an_arm_are_under_its_own_directory():
    """`--reset` removes a directory per arm. A checkpoint written outside it would survive the
    reset and make the next suite skip the events it had already consumed."""
    checkpoints = re.findall(r"/opt/payment-ledger/checkpoints/\S+", flat(every_step()))

    assert checkpoints
    for checkpoint in checkpoints:
        assert checkpoint.startswith(f"/opt/payment-ledger/checkpoints/{SCENARIO}/")


def test_an_arm_publishes_to_a_topic_of_its_own():
    assert chaos_run.topic(SCENARIO) == f"stripe.events.{SCENARIO}"
    assert chaos_run.topic("baseline") != chaos_run.topic(SCENARIO)


def test_a_wave_reads_its_own_files():
    steps = chaos_run.wave_steps(SCENARIO, "wave2")
    assert f"/{SCENARIO}/wave2/balance_transactions.jsonl" in flat(steps)
    assert f"/{SCENARIO}/wave2/daily_balance.jsonl" in flat(steps)


def test_two_arms_share_nothing():
    one = flat(every_step("baseline"))
    other = flat(every_step("rounding_drift"))
    for name in re.findall(r"lakehouse\.[\w.]+", one):
        assert name not in other


# --- the suite submits jobs that exist ------------------------------------------------------------


def test_every_job_the_suite_submits_is_a_file_in_jobs():
    submitted = set(re.findall(r"/opt/payment-ledger/jobs/([\w.]+\.py)", flat(every_step())))
    assert submitted
    assert submitted <= {path.name for path in (REPO_ROOT / "jobs").glob("*.py")}


def test_the_suite_runs_the_same_jobs_the_ordinary_run_does():
    """A chaos suite that exercised a different pipeline from the one that runs in anger would be
    testing itself. The only job here the ordinary run does not submit is the one that counts what
    the arm ended up holding."""
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    ordinary = set(re.findall(r"/opt/payment-ledger/jobs/([\w.]+\.py)", makefile))
    submitted = set(re.findall(r"/opt/payment-ledger/jobs/([\w.]+\.py)", flat(every_step())))

    assert submitted - ordinary == {"chaos_report.py"}


# --- the two halves say the same word -------------------------------------------------------------


def test_the_report_job_and_the_suite_agree_on_the_marker():
    assert f'MARKER = "{chaos.MARKER}"' in REPORT_JOB


def test_every_metric_a_scenario_asserts_on_is_one_the_report_job_measures():
    """A scenario naming a count nobody takes would pass by comparing None against None."""
    measured = set(re.findall(r'^    "(\w+)":', REPORT_JOB, re.MULTILINE))

    assert measured
    for scenario in chaos.SCENARIOS.values():
        for left, _, right in scenario.metrics:
            assert left in measured, f"{scenario.name} asserts on {left}, which nobody measures"
            if isinstance(right, str):
                assert right.split(":", 1)[1] in measured


def test_every_test_a_scenario_expects_to_fail_exists_in_the_project():
    """An expectation naming a test that does not exist can never be met, and a typo in one would
    fail every arm for a reason that has nothing to do with the pipeline."""
    tests = {path.stem for path in (REPO_ROOT / "dbt" / "tests").glob("*.sql")}

    for scenario in chaos.SCENARIOS.values():
        for name in scenario.dbt_failures:
            assert name in tests, f"{scenario.name} expects {name}, which is not a test"


def test_the_control_arm_runs_first():
    """Every other arm compares itself against the baseline's counts, so an order that ran it last
    would compare against whatever the previous suite left on disk."""
    assert next(iter(chaos.SCENARIOS)) == "baseline"
