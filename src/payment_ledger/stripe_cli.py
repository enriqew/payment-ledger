"""Locating and driving the Stripe CLI.

Two jobs, both about removing manual steps from the capture, because every manual step is a
chance to capture the wrong thing.

The first is finding the binary. The CLI is not packaged for every machine, so a copy dropped in
`.tools/` (gitignored) counts as installed here. That keeps the capture runnable without asking
the host for a system-wide install.

The second is the signing secret. `stripe listen --print-secret` returns the secret the CLI will
use to sign forwarded deliveries, without starting a listener, so the receiver can resolve it
itself instead of the operator copying a `whsec_...` out of one terminal into a dotfile. Copying
it by hand is how a receiver ends up verifying against a stale secret and rejecting every
delivery for what looks like a signature bug.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
from pathlib import Path

log = logging.getLogger("stripe_cli")

REPO_ROOT = Path(__file__).resolve().parents[2]
VENDORED_DIR = REPO_ROOT / ".tools"

# Deliberately wider than the secrets Stripe is known to issue. Matching too narrowly would
# silently truncate a secret at its first unexpected character, and a truncated secret fails
# verification in exactly the way a wrong secret does.
SECRET_PATTERN = re.compile(r"whsec_[A-Za-z0-9_\-]+")

INSTALL_HINT = (
    "The Stripe CLI was not found.\n"
    "Install it (https://docs.stripe.com/stripe-cli) or drop the binary in .tools/, then run\n"
    "`make login` once to authenticate against a test-mode sandbox."
)


class StripeCliError(RuntimeError):
    """The CLI is missing, unauthenticated, or answered with something unexpected."""


def find_cli() -> Path | None:
    """Return the CLI to use, or None if there is not one.

    An explicit `STRIPE_CLI` wins, then the vendored copy, then whatever is on PATH. The
    vendored copy is checked before PATH so a version pinned for this repo is not silently
    overridden by an older system install.
    """
    explicit = os.environ.get("STRIPE_CLI")
    if explicit:
        candidate = Path(explicit)
        return candidate if candidate.exists() else None

    for name in ("stripe.exe", "stripe"):
        candidate = VENDORED_DIR / name
        if candidate.exists():
            return candidate

    found = shutil.which("stripe")
    return Path(found) if found else None


def require_cli() -> Path:
    cli = find_cli()
    if cli is None:
        raise StripeCliError(INSTALL_HINT)
    return cli


def parse_secret(output: str) -> str:
    """Pull the `whsec_...` out of whatever the CLI printed.

    Matched rather than taken as the whole of stdout: the CLI is free to add a banner or an
    upgrade notice around it, and a secret with a stray newline attached fails verification in a
    way that reads like a key mismatch.
    """
    match = SECRET_PATTERN.search(output)
    if not match:
        raise StripeCliError(
            "`stripe listen --print-secret` did not return a whsec_ secret. It printed:\n"
            f"{output.strip() or '(nothing)'}\n"
            "If that is a login prompt, run `make login` first."
        )
    return match.group(0)


def print_secret(timeout: int = 30) -> str:
    """Ask the CLI for the webhook signing secret it will forward with."""
    cli = require_cli()
    try:
        completed = subprocess.run(  # noqa: S603 - the executable is resolved, not user input
            [str(cli), "listen", "--print-secret"],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise StripeCliError(
            f"`stripe listen --print-secret` did not answer within {timeout}s."
        ) from exc

    if completed.returncode != 0:
        raise StripeCliError(
            "`stripe listen --print-secret` failed:\n"
            f"{(completed.stderr or completed.stdout).strip()}\n"
            "If it is asking for credentials, run `make login` first."
        )

    return parse_secret(completed.stdout + completed.stderr)


def spawn_listener(port: int, path: str = "/webhooks") -> subprocess.Popen[bytes]:
    """Start `stripe listen`, forwarding test-mode deliveries at the local receiver.

    Spawned by the receiver so the capture is one command instead of two terminals that have to
    be started in the right order. The listener is a child process, so it dies with the receiver
    rather than being left forwarding at a closed port.
    """
    cli = require_cli()
    target = f"localhost:{port}{path}"
    log.info("starting `stripe listen --forward-to %s`", target)
    return subprocess.Popen(  # noqa: S603 - the executable is resolved, not user input
        [str(cli), "listen", "--forward-to", target],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
