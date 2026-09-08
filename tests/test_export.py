"""What the export promises, and what it refuses to publish.

The artifacts here are the only thing anybody outside this repository ever reads, and they are read
by a static page with no way to ask a question back. So the contract is not "these files exist": it
is that a figure on that page cannot be one the run did not earn. Every check below is a way the
export is supposed to say no.
"""

from __future__ import annotations

import json
import re

import pytest

from payment_ledger import export

ACCOUNTS = [
    {
        "account": "asset:balance_available",
        "currency": "eur",
        "balance": 100,
        "postings": 2,
        "first_posting_on": "2026-01-01",
        "last_posting_on": "2026-01-02",
    },
    {
        "account": "revenue:gross_sales",
        "currency": "eur",
        "balance": -100,
        "postings": 2,
        "first_posting_on": "2026-01-01",
        "last_posting_on": "2026-01-02",
    },
]

DAILY = [
    {
        "close_date": "2026-01-01",
        "currency": "eur",
        "balance_pending": 100,
        "balance_available": 0,
        "gross_sales": 100,
        "refunds": 0,
        "processing_fees": 2,
        "disputes": 0,
        "bank_movement": 0,
    }
]

RECONCILIATION = [
    {
        "close_date": "2026-01-01",
        "currency": "eur",
        "ledger_total": 100,
        "reported_total": 100,
        "total_difference": 0,
        "unexplained_difference": 0,
        "ledger_closed_the_day": True,
        "processor_reported_the_day": True,
        "unposted_net": 0,
    }
]

FLOW = [
    {
        "source": "revenue:gross_sales",
        "target": "asset:balance_available",
        "currency": "eur",
        "amount": 100,
    },
]

RUN = {
    "transactions": 1000,
    "seed": 1,
    "days": 30,
    "start": "2026-01-01",
    "counts": {"events": 4602},
    "sha256": {"events": "abc123"},
}


def tables(**overrides) -> dict:
    dumped = {
        "account_balances": ACCOUNTS,
        "money_flow": FLOW,
        "daily_close": DAILY,
        "reconciliation": RECONCILIATION,
        "coverage_gaps": [],
        "restatements": [],
    }
    dumped.update(overrides)
    return {name: json.loads(json.dumps(rows)) for name, rows in dumped.items()}


def build(**overrides) -> dict:
    return export.assemble(tables(**overrides), RUN, [])


# --- what a file has to carry ---------------------------------------------------------------------


def test_every_file_says_it_is_simulated():
    """Not only the manifest. These get copied one at a time into another repository, and a payload
    that knows it is simulated because of a sibling file is one rename away from being real."""
    for name, payload in build().items():
        assert payload["simulated"] is True, name
        assert payload["schema_version"] == export.SCHEMA_VERSION, name
        assert "Simulated data" in payload["notice"], name


def test_the_manifest_carries_the_run_that_produced_it():
    manifest = build()["manifest.json"]
    assert manifest["run"]["transactions"] == 1000
    assert manifest["run"]["seed"] == 1
    assert manifest["run"]["sha256"] == {"events": "abc123"}
    assert manifest["amounts"] == export.AMOUNTS


def test_the_manifest_counts_every_file_it_ships():
    files = build()
    assert set(files["manifest.json"]["files"]) == set(files) - {"manifest.json"}
    assert files["manifest.json"]["files"]["accounts.json"] == 2


def test_there_is_no_timestamp_anywhere():
    """An export of the same run is the same bytes. What identifies it is the fingerprint of the
    run, so an export whose numbers moved is an export whose input moved."""
    once = json.dumps(build(), sort_keys=True)
    again = json.dumps(build(), sort_keys=True)
    assert once == again
    assert "exported_at" not in once


def test_a_file_carries_only_the_fields_the_contract_names():
    """The dumps have more columns than the contract does. A page that quietly gained a field from
    an upstream model is a page nobody decided to publish."""
    files = build()
    assert set(files["accounts.json"]["rows"][0]) == set(export.ACCOUNT_FIELDS)
    assert set(files["daily_close.json"]["rows"][0]) == set(export.DAILY_CLOSE_FIELDS)
    assert set(files["reconciliation.json"]["rows"][0]) == set(export.RECONCILIATION_FIELDS)


def test_a_renamed_column_upstream_is_refused_rather_than_exported_as_null():
    missing = [{key: value for key, value in ACCOUNTS[0].items() if key != "balance"}]
    with pytest.raises(export.Refused, match="balance"):
        build(account_balances=missing)


# --- what it refuses ------------------------------------------------------------------------------


def test_a_trial_balance_that_does_not_sum_to_zero_is_refused():
    """The one number on that page that has to be zero. A dashboard has no way to find out
    afterwards that the ledger created money."""
    broken = json.loads(json.dumps(ACCOUNTS))
    broken[0]["balance"] = 101

    with pytest.raises(export.Refused, match="trial balance"):
        export.validate(build(account_balances=broken))


def test_a_day_the_reconciliation_cannot_explain_is_refused():
    broken = json.loads(json.dumps(RECONCILIATION))
    broken[0]["unexplained_difference"] = -1

    with pytest.raises(export.Refused, match="unexplained difference"):
        export.validate(build(reconciliation=broken))


def test_a_float_anywhere_is_refused():
    """Money is an integer in minor units at every layer, and an export is the last place that can
    quietly stop being true."""
    broken = json.loads(json.dumps(DAILY))
    broken[0]["balance_available"] = 0.1

    with pytest.raises(export.Refused, match="floating point"):
        export.validate(build(daily_close=broken))


def test_a_day_that_is_not_a_plain_iso_day_is_refused():
    """A timestamp makes the reader guess a timezone, and a close that means a different day on two
    machines is the bug the whole pipeline runs in UTC to avoid."""
    broken = json.loads(json.dumps(DAILY))
    broken[0]["close_date"] = "2026-01-01 00:00:00"

    with pytest.raises(export.Refused, match="plain ISO day"):
        export.validate(build(daily_close=broken))


def test_a_healthy_run_passes_every_check():
    export.validate(build())


# --- the flow is the ledger, not a picture of it --------------------------------------------------


def test_a_flow_that_moves_money_the_balances_do_not_hold_is_refused():
    """Nothing in a Sankey diagram objects to arrows that do not add up, which is exactly why
    something else has to. The warehouse checks this too, and a failing dbt test does not stop the
    export from reading the table it was testing."""
    broken = json.loads(json.dumps(FLOW))
    broken[0]["amount"] = 99

    with pytest.raises(export.Refused, match="different stories"):
        export.validate(build(money_flow=broken))


def test_a_flow_touching_an_account_the_trial_balance_lacks_is_refused():
    broken = json.loads(json.dumps(FLOW))
    broken.append(
        {"source": "revenue:gross_sales", "target": "asset:bank", "currency": "eur", "amount": 0}
    )
    broken[0]["target"] = "asset:bank"

    with pytest.raises(export.Refused, match="which the trial balance does not have"):
        export.validate(build(money_flow=broken))


def test_a_link_with_no_weight_is_refused():
    broken = json.loads(json.dumps(FLOW))
    broken[0]["amount"] = 0

    with pytest.raises(export.Refused, match="not a weight"):
        export.validate(build(money_flow=broken))


# --- the chaos suite, all of it or none of it -----------------------------------------------------


def arm(scenario: str, described: bool = True) -> dict:
    return {
        "scenario": scenario,
        "failure": "Duplicate delivery",
        "detection": "dedup on the event id",
        "injected": {"redelivered_events": 230},
        "expected_to_fail": {},
        "failed": {},
        "as_described": described,
        "counts": {"gold_postings": 5608},
    }


def with_arms(arms: list[dict]) -> dict:
    return export.assemble(tables(), RUN, arms)


def test_a_partial_chaos_suite_is_refused():
    """The one shape that misleads by being true: three arms on a page look exactly like the
    suite."""
    with pytest.raises(export.Refused, match="partial suite"):
        export.validate(with_arms([arm("baseline"), arm("out_of_order")]))


def test_no_chaos_at_all_is_allowed():
    """An export taken before the suite has been run says nothing about it, which is honest. What
    is not allowed is saying something about part of it."""
    files = with_arms([])
    export.validate(files)
    assert files["chaos.json"]["arms"] == []


def test_a_whole_suite_is_carried():
    arms = [arm(name) for name in ["a", "b", "c", "d", "e", "f", "g"][: export.EXPECTED_ARMS]]
    files = with_arms(arms)
    export.validate(files)
    assert len(files["chaos.json"]["arms"]) == export.EXPECTED_ARMS


def test_an_arm_that_did_not_do_what_it_said_is_refused():
    arms = [arm(name) for name in ["a", "b", "c", "d", "e", "f", "g"][: export.EXPECTED_ARMS]]
    arms[2]["as_described"] = False

    with pytest.raises(export.Refused, match="did not do what it said"):
        export.validate(with_arms(arms))


def test_the_expected_arm_count_comes_from_the_suite_itself():
    """Typed here, it would drift the moment a seventh failure is added, and the export would drop
    an arm without saying so."""
    from payment_ledger import chaos

    assert len(chaos.SCENARIOS) == export.EXPECTED_ARMS


# --- the job and the contract agree ---------------------------------------------------------------


def test_every_table_the_contract_reads_is_one_the_job_dumps():
    from payment_ledger.config import REPO_ROOT

    job = (REPO_ROOT / "jobs" / "export_tables.py").read_text(encoding="utf-8")
    dumped = set(re.findall(r'"(\w+)"', job.split("TABLES = (")[1].split(")")[0]))

    assert set(export.TABLES) == dumped


def test_the_export_is_written_as_one_file_per_artifact(tmp_path):
    files = build()
    export.write(files, tmp_path)

    for name in files:
        assert json.loads((tmp_path / name).read_text(encoding="utf-8")) == files[name]


def test_the_arms_come_out_in_the_order_the_suite_runs_them(tmp_path):
    """Alphabetical is not an order anybody chose. A page renders what it is given, and the design
    lists the six failures in the order they are worth reading."""
    from payment_ledger import chaos

    for name in reversed(list(chaos.SCENARIOS)):
        directory = tmp_path / name
        directory.mkdir()
        (directory / "scenario.json").write_text(
            json.dumps(
                {
                    "scenario": name,
                    "failure": "x",
                    "detection": "y",
                    "injected": {},
                    "expect": {"dbt_failures": {}, "metrics": []},
                }
            ),
            encoding="utf-8",
        )
        (directory / "report.json").write_text("{}", encoding="utf-8")
        (directory / "run_results.json").write_text('{"results": []}', encoding="utf-8")

    arms = export.read_chaos(tmp_path)
    assert [arm["scenario"] for arm in arms] == list(chaos.SCENARIOS)
