"""The audit's path rules.

`.gitignore` says these files should not be here; this says they cannot be, which is a different
claim. `git add -f` walks past an ignore rule, and so does a file added before its rule existed.
The audit is checked against what git actually tracks, because that is what gets pushed.
"""

from __future__ import annotations

import importlib.util

import pytest

from payment_ledger.config import REPO_ROOT

spec = importlib.util.spec_from_file_location(
    "audit_publishable", REPO_ROOT / "scripts" / "audit_publishable.py"
)
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def tracked(relative: str) -> bool:
    return any(pattern.search(relative) for pattern in audit.NEVER_TRACKED)


@pytest.mark.parametrize(
    "relative",
    [
        ".env",
        ".env.local",
        ".env.production",
        "certs/server.pem",
        "id_rsa.key",
        "credentials",
        "gcp/service-account-prod.json",
        "dbt/profiles.yml",
        "airflow/airflow.cfg",
    ],
)
def test_these_can_never_be_tracked(relative):
    assert tracked(relative)


@pytest.mark.parametrize(
    "relative",
    [
        ".env.example",
        "README.md",
        "docker/docker-compose.yml",
        "src/payment_ledger/config.py",
        "fixtures/events/charge.succeeded/evt_1.json",
    ],
)
def test_these_are_fine(relative):
    assert not tracked(relative)


def test_the_repository_as_it_stands_is_publishable():
    """The same check CI and the pre-commit hook run, so a failure here means do not push."""
    assert audit.main() == 0
