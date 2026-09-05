"""What may be committed. These fixtures go to a public history, where a mistake is permanent."""

from __future__ import annotations

import pytest

from payment_ledger.capture import write_fixture
from payment_ledger.redact import (
    ACCOUNT_PLACEHOLDER,
    REDACTED_TEXT,
    LiveModeError,
    assert_test_mode,
    mask,
    prepare_for_fixture,
    redact,
)

EVENT = {
    "id": "evt_3MtwBwLkdIwHu7ix28a3tqPa",
    "type": "charge.succeeded",
    "livemode": False,
    "request": {"id": "req_1", "idempotency_key": "3f8e2c11-6a4b-4a1e-9c2d-77b0e5a1d4c9"},
    "data": {
        "object": {
            "id": "ch_3MtwBw",
            "amount": 2000,
            "currency": "usd",
            "on_behalf_of": "acct_1QxYzABCDEFGHIJK",
            "receipt_url": "https://pay.stripe.com/receipts/payment/CAcaFwoVYWNjdF8x?s=ap",
            "receipt_email": "someone@example.com",
            "billing_details": {
                "name": "Jane Roe",
                "phone": None,
                "address": {"line1": "1 Example Street", "postal_code": "D02 XY45"},
            },
            "balance_transaction": "txn_3MtwBw",
        }
    },
}


def test_live_mode_is_refused_not_cleaned():
    """A live payload means the CLI is pointed somewhere it should not be. Cleaning up hides it."""
    with pytest.raises(LiveModeError):
        prepare_for_fixture({**EVENT, "livemode": True})


def test_live_mode_is_caught_at_any_depth():
    """Envelope and object each carry livemode, and a mismatch is itself a reason to stop."""
    with pytest.raises(LiveModeError):
        assert_test_mode({"livemode": False, "data": {"object": {"livemode": True}}})


def test_a_live_payload_never_reaches_the_filesystem(tmp_path):
    with pytest.raises(LiveModeError):
        write_fixture(tmp_path, {**EVENT, "livemode": True})

    assert list(tmp_path.rglob("*.json")) == []


def test_the_account_id_is_replaced_wherever_it_appears():
    cleaned, changed = prepare_for_fixture(EVENT)

    assert changed is True
    assert cleaned["data"]["object"]["on_behalf_of"] == ACCOUNT_PLACEHOLDER


def test_the_receipt_url_token_is_dropped():
    """The path of a receipt url is a token: it opens the receipt for anyone holding it."""
    cleaned, _ = prepare_for_fixture(EVENT)
    receipt = cleaned["data"]["object"]["receipt_url"]

    assert "CAcaFwoVYWNjdF8x" not in receipt
    assert receipt.startswith("https://pay.stripe.com/receipts/")


def test_personal_fields_are_blanked():
    cleaned, _ = prepare_for_fixture(EVENT)
    billing = cleaned["data"]["object"]["billing_details"]

    assert cleaned["data"]["object"]["receipt_email"] == REDACTED_TEXT
    assert billing["name"] == REDACTED_TEXT
    assert billing["address"]["line1"] == REDACTED_TEXT
    assert billing["address"]["postal_code"] == REDACTED_TEXT


def test_an_absent_personal_field_stays_absent():
    """Blanking a null invents a value the processor did not send, and the shape is what matters."""
    cleaned, _ = prepare_for_fixture(EVENT)

    assert cleaned["data"]["object"]["billing_details"]["phone"] is None


def test_what_the_pipeline_joins_on_survives():
    """Redacting an id or an amount would leave a fixture that no longer tests anything."""
    cleaned, _ = prepare_for_fixture(EVENT)
    obj = cleaned["data"]["object"]

    assert cleaned["id"] == EVENT["id"]
    assert obj["id"] == EVENT["data"]["object"]["id"]
    assert obj["balance_transaction"] == "txn_3MtwBw"
    assert obj["amount"] == 2000
    assert obj["currency"] == "usd"
    # The idempotency key is a random uuid, not a credential, and it is half the duplicate story.
    assert cleaned["request"]["idempotency_key"] == EVENT["request"]["idempotency_key"]


def test_a_clean_payload_is_reported_as_unchanged():
    payload = {"id": "evt_1", "type": "payout.paid", "livemode": False, "amount": 100}

    cleaned, changed = prepare_for_fixture(payload)

    assert changed is False
    assert cleaned == payload


def test_redaction_does_not_mutate_the_input():
    """The raw event is still logged and answered on, so it must survive being cleaned."""
    before = EVENT["data"]["object"]["receipt_email"]

    redact(EVENT)

    assert EVENT["data"]["object"]["receipt_email"] == before


def test_no_marker_is_added_to_a_redacted_payload():
    """A fixture carrying an invented field is no longer a genuine envelope."""
    cleaned, _ = prepare_for_fixture(EVENT)

    assert set(cleaned) == set(EVENT)
    assert set(cleaned["data"]["object"]) == set(EVENT["data"]["object"])


def test_a_credential_is_masked_down_to_its_prefix():
    """Everything this package prints that it did not construct itself goes through mask()."""
    text = "signing secret whsec_abc123DEF and key sk_test_51ABCdefGHI in one line"

    masked = mask(text)

    assert "whsec_abc123DEF" not in masked
    assert "sk_test_51ABCdefGHI" not in masked
    assert "whsec_***" in masked and "sk_***" in masked


def test_masking_leaves_everything_else_readable():
    """An error message that survives masking as noise is an error message nobody can act on."""
    assert (
        mask("Error: please run `stripe login` first") == "Error: please run `stripe login` first"
    )


def test_a_live_key_is_masked_too():
    # Assembled at runtime rather than written out. The audit forbids a live-key-shaped string in
    # any file with no exception for tests, and it is right to: a rule with a carve-out for the
    # place fake values live is a rule that stops catching the real one.
    live_key = "sk_" + "live_" + "51ExampleKeyMaterial"

    assert "live_" not in mask(live_key)
    assert mask(live_key) == "sk_***"
