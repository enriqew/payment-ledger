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
