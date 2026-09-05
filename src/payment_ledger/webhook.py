"""Stripe webhook signature verification.

Implemented against the documented scheme rather than pulled from the SDK, because the whole
project is about not taking delivery guarantees on faith, and this is the one place where the
payload's authenticity is established.

The header looks like:

    Stripe-Signature: t=1614556800,v1=5257a869e7...,v1=<second signature during rotation>

The signed payload is the timestamp, a literal dot, and the **raw request body**. Re-serialising
the parsed JSON and signing that is the classic way to make verification fail for reasons that
look like a key problem, so `verify` takes bytes and never sees a dict.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from dataclasses import dataclass

SCHEME = "v1"


class SignatureError(Exception):
    """Raised when a payload does not verify. The message is safe to log."""


@dataclass(frozen=True)
class SignatureHeader:
    timestamp: int
    signatures: tuple[str, ...]


def parse_signature_header(header: str) -> SignatureHeader:
    """Split a `Stripe-Signature` header into its timestamp and its v1 signatures."""
    timestamp: int | None = None
    signatures: list[str] = []

    for part in header.split(","):
        key, _, value = part.strip().partition("=")
        if key == "t":
            try:
                timestamp = int(value)
            except ValueError as exc:
                raise SignatureError("signature timestamp is not an integer") from exc
        elif key == SCHEME and value:
            signatures.append(value)

    if timestamp is None:
        raise SignatureError("signature header has no timestamp")
    if not signatures:
        raise SignatureError(f"signature header has no {SCHEME} signature")

    return SignatureHeader(timestamp=timestamp, signatures=tuple(signatures))


def expected_signature(payload: bytes, timestamp: int, secret: str) -> str:
    signed_payload = str(timestamp).encode("utf-8") + b"." + payload
    return hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()


def verify(
    payload: bytes,
    header: str,
    secret: str,
    tolerance_seconds: int = 300,
    now: float | None = None,
) -> SignatureHeader:
    """Verify a raw request body against its signature header.

    Returns the parsed header on success and raises `SignatureError` on any failure. A header
    carrying several v1 signatures verifies if **any** of them matches, which is what makes a
    signing-secret rotation survivable.
    """
    parsed = parse_signature_header(header)

    age = (time.time() if now is None else now) - parsed.timestamp
    if tolerance_seconds and abs(age) > tolerance_seconds:
        raise SignatureError(
            f"signature timestamp is {age:.0f}s away from now, outside the "
            f"{tolerance_seconds}s tolerance"
        )

    expected = expected_signature(payload, parsed.timestamp, secret)
    if not any(hmac.compare_digest(expected, candidate) for candidate in parsed.signatures):
        raise SignatureError("no signature in the header matches the payload")

    return parsed
