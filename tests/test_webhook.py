"""Signature verification is the one thing phase 0 has to get right, so it is tested first."""

from __future__ import annotations

import pytest

from payment_ledger.webhook import (
    SignatureError,
    expected_signature,
    parse_signature_header,
    verify,
)

SECRET = "whsec_test_secret"
PAYLOAD = b'{"id":"evt_1","type":"charge.succeeded"}'
NOW = 1_700_000_000


def header_for(payload: bytes = PAYLOAD, timestamp: int = NOW, secret: str = SECRET) -> str:
    return f"t={timestamp},v1={expected_signature(payload, timestamp, secret)}"


def test_valid_signature_verifies():
    parsed = verify(PAYLOAD, header_for(), SECRET, now=NOW)
    assert parsed.timestamp == NOW


def test_tampered_payload_is_rejected():
    header = header_for()
    with pytest.raises(SignatureError, match="no signature"):
        verify(PAYLOAD + b" ", header, SECRET, now=NOW)


def test_wrong_secret_is_rejected():
    with pytest.raises(SignatureError, match="no signature"):
        verify(PAYLOAD, header_for(), "whsec_other", now=NOW)


def test_stale_timestamp_is_rejected():
    """Replay protection: a valid signature from an hour ago is still refused."""
    header = header_for(timestamp=NOW - 3600)
    with pytest.raises(SignatureError, match="tolerance"):
        verify(PAYLOAD, header, SECRET, tolerance_seconds=300, now=NOW)


def test_future_timestamp_is_rejected():
    header = header_for(timestamp=NOW + 3600)
    with pytest.raises(SignatureError, match="tolerance"):
        verify(PAYLOAD, header, SECRET, tolerance_seconds=300, now=NOW)


def test_rotation_verifies_when_any_signature_matches():
    """Two v1 entries during a signing-secret rotation: matching either one is enough."""
    good = expected_signature(PAYLOAD, NOW, SECRET)
    header = f"t={NOW},v1=deadbeef,v1={good}"
    assert verify(PAYLOAD, header, SECRET, now=NOW).signatures == ("deadbeef", good)


def test_signature_is_over_raw_bytes_not_reserialised_json():
    """The payload signed is exactly what arrived, whitespace included.

    Verifying against `json.dumps(json.loads(body))` is the classic webhook bug: it works
    until the sender's serialisation differs from yours by a space.
    """
    spaced = b'{"id": "evt_1", "type": "charge.succeeded"}'
    header = header_for(payload=spaced)
    assert verify(spaced, header, SECRET, now=NOW)
    with pytest.raises(SignatureError):
        verify(PAYLOAD, header, SECRET, now=NOW)


@pytest.mark.parametrize(
    "header, message",
    [
        ("v1=abc", "no timestamp"),
        (f"t={NOW}", "no v1 signature"),
        ("t=not-a-number,v1=abc", "not an integer"),
        (f"t={NOW},v0=abc", "no v1 signature"),
    ],
)
def test_malformed_headers_are_rejected(header: str, message: str):
    with pytest.raises(SignatureError, match=message):
        parse_signature_header(header)
