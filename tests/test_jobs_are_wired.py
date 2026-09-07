"""Checks over the Spark jobs that do not need Spark.

The jobs themselves are exercised by running them, which needs Docker and is not something CI has.
What CI can still catch is drift between the three places that have to agree: the tables a job
creates, the tables `make reset` drops, and the job files the Makefile submits. Every one of those
is a mistake that leaves a run silently reporting the previous run's numbers, which is the exact
failure mode this pipeline exists to make impossible.
"""

from __future__ import annotations

import re

from payment_ledger.config import REPO_ROOT

JOBS = sorted((REPO_ROOT / "jobs").glob("*.py"))
MAKEFILE = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")

CREATES = re.compile(r"CREATE (?:OR REPLACE )?TABLE (?:IF NOT EXISTS )?([\w.{}]+)")
DROPS = re.compile(r"DROP TABLE IF EXISTS ([\w.]+)")
# A table name in a job is often a format placeholder resolved from a default at the top of the
# file, so the defaults are what tell us the real name.
DEFAULTS = re.compile(r'^DEFAULT_(?:TABLE|NAMESPACE)\s*=\s*"([\w.]+)"', re.MULTILINE)


def created_tables() -> set[str]:
    tables: set[str] = set()
    for path in JOBS:
        source = path.read_text(encoding="utf-8")
        defaults = DEFAULTS.findall(source)
        for name in CREATES.findall(source):
            if "{table}" in name:
                tables.update(defaults)
            elif "{ns}" in name:
                tables.update(f"{ns}{name.replace('{ns}', '')}" for ns in defaults)
            else:
                tables.add(name)
    return tables


def test_there_is_at_least_one_job_to_check():
    assert JOBS


def test_reset_drops_every_table_the_jobs_create():
    """Otherwise the next run reports on rows the last one left behind."""
    dropped = set(DROPS.findall(MAKEFILE))
    missing = created_tables() - dropped

    assert not missing, f"make reset leaves behind: {', '.join(sorted(missing))}"


def test_every_job_the_makefile_submits_exists():
    submitted = set(re.findall(r"/opt/payment-ledger/jobs/([\w.]+\.py)", MAKEFILE))
    assert submitted
    assert submitted <= {path.name for path in JOBS}


def test_nothing_a_job_declares_is_floating_point():
    """Money is integer minor units at every layer, and a schema declaration is where that stops
    being true first. Only the places that actually declare a type are read: the DDL bodies, the
    inline `STRUCT<...>` schemas, and the PySpark type constructors."""
    declarations = re.compile(
        r"CREATE (?:OR REPLACE )?TABLE.*?USING iceberg|STRUCT<.*?>|\w+Type\(\)",
        re.DOTALL | re.IGNORECASE,
    )
    floating = re.compile(r"\b(DOUBLE|FLOAT|REAL|DoubleType|FloatType)\b", re.IGNORECASE)

    # A column comment is prose, not a declaration, and the prose says "never a float" on purpose.
    comments = re.compile(r"COMMENT\s+'[^']*'")

    for path in JOBS:
        for block in declarations.findall(path.read_text(encoding="utf-8")):
            assert not floating.search(comments.sub("", block)), (
                f"{path.name} declares a floating point type"
            )


def test_the_checkpoints_the_jobs_use_are_all_under_the_reset_path():
    """`make reset` clears the checkpoint directory wholesale, so a job that put its checkpoint
    somewhere else would quietly survive a reset and skip the events it already consumed."""
    for path in JOBS:
        for checkpoint in re.findall(r'DEFAULT_CHECKPOINT\s*=\s*"([^"]+)"', path.read_text()):
            assert checkpoint.startswith("/opt/payment-ledger/checkpoints/")


# --- the dbt project, checked the same way ------------------------------------------------------

DBT = REPO_ROOT / "dbt"
EPHEMERAL = re.compile(r"materialized\s*=\s*'ephemeral'")


def materialised_gold_models() -> list:
    """The gold models that become a table. An ephemeral one is a shared definition compiled into
    its readers, so there is nothing in the catalog for a reset to drop."""
    return [
        path
        for path in sorted((DBT / "models" / "gold").glob("*.sql"))
        if not EPHEMERAL.search(path.read_text(encoding="utf-8"))
    ]


GOLD_MODELS = materialised_gold_models()


def test_reset_drops_every_gold_model():
    """dbt recreates its tables, but `make reset` still has to drop them: a model removed from the
    project would otherwise leave its table behind and the next run would report on it."""
    dropped = set(DROPS.findall(MAKEFILE))
    expected = {f"lakehouse.gold.{path.stem}" for path in GOLD_MODELS}

    assert expected, "no materialised gold models found"
    assert expected <= dropped, f"make reset leaves behind: {sorted(expected - dropped)}"


def test_the_chart_of_accounts_matches_what_the_ledger_can_post_to():
    """Two places name the accounts: the model that emits them and the accepted_values test that
    closes the list. They drift silently, and the way they drift is that a new account stops being
    checked, which is the opposite of what the test is for.

    `expense:unclassified` is excluded on purpose. The model can emit it, the accepted list must
    not contain it, and that asymmetry is the mechanism: an unmodelled reporting category posts
    there and fails the build instead of vanishing.
    """
    model = (DBT / "models" / "gold" / "ledger_postings.sql").read_text(encoding="utf-8")
    emitted = set(re.findall(r"'((?:asset|revenue|contra_revenue|expense):[a-z_]+)'", model))

    schema = (DBT / "models" / "gold" / "_models.yml").read_text(encoding="utf-8")
    accepted = set(re.findall(r'-\s+"((?:asset|revenue|contra_revenue|expense):[a-z_]+)"', schema))

    assert "expense:unclassified" in emitted, "the catch-all account is gone from the model"
    assert "expense:unclassified" not in accepted, "the catch-all is accepted, so nothing fails"
    assert emitted - {"expense:unclassified"} == accepted


def test_the_invariants_are_singular_tests_that_fail_the_build():
    """A generic test can be configured to warn. These are `.sql` files under `tests/`, which dbt
    can only pass or fail, and a failure stops every model downstream of them."""
    invariants = {path.stem for path in (DBT / "tests").glob("*.sql")}

    assert {
        "assert_entries_balance",
        "assert_every_balance_transaction_is_posted",
        "assert_available_balance_is_explained",
    } <= invariants


def test_no_dbt_model_divides_a_monetary_value():
    """Integer minor units survive addition and subtraction. A division is where a ledger quietly
    becomes approximate, so there is not one anywhere in gold."""
    for path in GOLD_MODELS + sorted((DBT / "models" / "staging").glob("*.sql")):
        body = re.sub(r"--.*", "", path.read_text(encoding="utf-8"))
        assert "/" not in body, f"{path.name} divides something"


def test_the_itemisation_does_not_hang_off_the_table_the_invariant_guards():
    """dbt skips everything downstream of a failed test. If `reconciliation_items` read
    `reconciliation`, the run that failed the fourth invariant would also skip the table that
    explains why, which is the one moment anybody wants to read it. Both read a shared model
    instead, and this is the test that keeps them siblings."""
    items = (DBT / "models" / "gold" / "reconciliation_items.sql").read_text(encoding="utf-8")
    refs = set(re.findall(r"ref\(\s*'([\w]+)'\s*\)", items))

    assert "int_reconciliation" in refs
    assert "reconciliation" not in refs


def test_the_reconciliation_grants_no_tolerance():
    """Both sides are integers in minor units, so a tolerance is not rounding relief, it is a
    place for a real discrepancy to live. The test compares against zero and nothing else."""
    check = (DBT / "tests" / "assert_no_unexplained_difference.sql").read_text(encoding="utf-8")
    body = re.sub(r"--.*", "", check)

    assert "unexplained_difference != 0" in body
    assert "abs(" not in body.lower()
