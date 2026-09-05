"""What may be committed, enforced where the fixture is written.

The fixtures in this repository are public. Every payload that lands in `fixtures/` is on its way
to a public git history, where a mistake is permanent in a way a mistake in a working directory is
not. So the rules live here, at the one point where a payload becomes a file, rather than in a
review checklist.

Two different jobs, deliberately kept apart:

**The guard refuses.** A payload with `livemode: true` is not redacted, it is rejected. Test-mode
data is the entire premise of the project, so a live payload arriving at all means something is
wrong upstream (the wrong CLI profile, the wrong key) and quietly cleaning it up would hide that.

**Redaction rewrites.** A test-mode payload still carries things that identify the account it came
from: the account id, and the receipt URL, whose path is a token that opens the receipt for anyone
holding it. Those are replaced with same-shaped placeholders. Personal fields are blanked for the
same reason, even though `stripe trigger` leaves most of them null.

What is never touched: object ids (`evt_`, `ch_`, `txn_`, `py_`), amounts, currencies, fees,
timestamps and `request.idempotency_key`. Those are what the pipeline joins, sums and deduplicates
on, so redacting them would leave fixtures that no longer exercise the thing they exist to test.
The idempotency key in particular is load-bearing: it is a random uuid, not a credential, and it is
half the duplicate-delivery story.

No marker key is added to a redacted payload. The envelopes are meant to be genuine, and a fixture
carrying an invented field is no longer a real one. What was replaced is documented in
`fixtures/README.md` and logged as it happens.
"""

from __future__ import annotations

import re
from typing import Any

ACCOUNT_PLACEHOLDER = "acct_00000000000000"
RECEIPT_PLACEHOLDER = "https://pay.stripe.com/receipts/redacted"
REDACTED_TEXT = "[redacted]"

# Personal fields. Blanked wherever they appear, since Stripe reuses these names across
# billing_details, shipping, owner and dispute evidence, and there is no value in any of them for
# a pipeline that only ever adds up money.
PERSONAL_KEYS = frozenset(
    {
        "email",
        "receipt_email",
        "customer_email",
        "phone",
        "name",
        "line1",
        "line2",
        "postal_code",
    }
)

ACCOUNT_ID = re.compile(r"acct_[A-Za-z0-9]+")
RECEIPT_URL = re.compile(r"https://pay\.stripe\.com/receipts/\S+")


# The rules, as patterns, so the same definition backs the capture, the test suite and the
# pre-publish audit. A rule that only exists in a checklist is a rule that gets skipped on the day
# it matters.
#
# Split by blast radius. A live credential is never acceptable anywhere in the tree, including in a
# test that means to use a fake one. The rest identify the sandbox account rather than granting
# access to it, so they are forbidden in the payloads themselves and tolerated in the places whose
# job is to show what one looks like.
FORBIDDEN_ANYWHERE = {
    "a live secret key": re.compile(r"sk_live_[A-Za-z0-9]+"),
    "a live restricted key": re.compile(r"rk_live_[A-Za-z0-9]+"),
    "a live publishable key": re.compile(r"pk_live_[A-Za-z0-9]+"),
}

FORBIDDEN_IN_PAYLOADS = {
    "a test secret key": re.compile(r"sk_test_[A-Za-z0-9]{16,}"),
    "a test restricted key": re.compile(r"rk_test_[A-Za-z0-9]+"),
    "a webhook signing secret": re.compile(r"whsec_[A-Za-z0-9_\-]+"),
    "a real account id": re.compile(r"acct_(?!00000000000000)[A-Za-z0-9]+"),
    "a receipt url with its token": re.compile(r"pay\.stripe\.com/receipts/\S*[?/][A-Za-z0-9]{8,}"),
    "an email address": re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
}


class LiveModeError(Exception):
    """A payload arrived with `livemode: true`. Test mode is the premise, so this is a hard stop."""


def assert_test_mode(payload: Any) -> None:
    """Reject a payload that says it came from live mode, at any depth.

    Checked recursively rather than only at the top level: the event envelope carries `livemode`
    and so does the object inside it, and a mismatch between the two is itself a reason to stop.
    """
    if isinstance(payload, dict):
        if payload.get("livemode") is True:
            raise LiveModeError(
                "refusing to write a fixture with livemode: true. "
                "This repository commits test-mode payloads only. "
                "Check which account the CLI is authenticated against with `make whoami`."
            )
        for value in payload.values():
            assert_test_mode(value)
    elif isinstance(payload, list):
        for item in payload:
            assert_test_mode(item)


def _scrub_text(text: str) -> str:
    text = RECEIPT_URL.sub(RECEIPT_PLACEHOLDER, text)
    return ACCOUNT_ID.sub(ACCOUNT_PLACEHOLDER, text)


def redact(payload: Any, key: str | None = None) -> Any:
    """Return a copy of the payload with identifying values replaced.

    A null personal field stays null. Blanking it to a placeholder would invent a value the
    processor did not send, and "this field is usually absent" is part of the shape the pipeline
    has to handle.
    """
    if isinstance(payload, dict):
        return {k: redact(v, key=k) for k, v in payload.items()}
    if isinstance(payload, list):
        return [redact(item, key=key) for item in payload]
    if isinstance(payload, str):
        if key in PERSONAL_KEYS:
            return REDACTED_TEXT
        return _scrub_text(payload)
    return payload


def prepare_for_fixture(event: dict) -> tuple[dict, bool]:
    """Guard, then redact. Returns the payload to write and whether anything changed."""
    assert_test_mode(event)
    cleaned = redact(event)
    return cleaned, cleaned != event
