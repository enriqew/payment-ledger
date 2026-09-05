"""A standing check over whatever is currently in `fixtures/`.

`test_redact.py` proves the rules are right. This proves they were actually applied, which is a
different claim: a fixture could have been captured before a rule existed, written by hand, or
copied in from somewhere else. It runs on `make test` and in CI, so the answer to "is this repo
safe to publish today" is a command rather than a reading of the diff.

It passes on an empty `fixtures/` tree by design. Nothing captured is not a failure, and the
capture depends on a Stripe account that CI does not have.
"""

from __future__ import annotations

import json

import pytest

from payment_ledger.config import REPO_ROOT
from payment_ledger.redact import FORBIDDEN_ANYWHERE, FORBIDDEN_IN_PAYLOADS

FIXTURES = REPO_ROOT / "fixtures"

# The same rules the capture applies on write and the audit applies to the whole tree. A payload
# is held to both scopes: nothing about a fixture makes it a place to show what a key looks like.
FORBIDDEN = {**FORBIDDEN_ANYWHERE, **FORBIDDEN_IN_PAYLOADS}


def fixture_files():
    return sorted(FIXTURES.rglob("*.json")) if FIXTURES.exists() else []


@pytest.mark.parametrize("path", fixture_files(), ids=lambda p: p.name)
def test_no_committed_fixture_carries_a_secret_or_an_identifier(path):
    text = path.read_text(encoding="utf-8")

    # The match itself is never reported. Putting a secret in a test log moves it somewhere
    # worse than the file it was found in; naming the rule is enough to act on.
    hits = [name for name, pattern in FORBIDDEN.items() if pattern.search(text)]

    assert not hits, f"{path.relative_to(REPO_ROOT)} contains {', '.join(hits)}"


@pytest.mark.parametrize("path", fixture_files(), ids=lambda p: p.name)
def test_no_committed_fixture_came_from_live_mode(path):
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload.get("livemode") is False, (
        f"{path.relative_to(REPO_ROOT)} does not say livemode: false"
    )
