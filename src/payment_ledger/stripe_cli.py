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

The secret is held in memory and written nowhere. Every error path below echoes the output of a
command whose entire job is printing a credential, so all of it goes through `mask` first, and the
listener's own stdout goes to devnull for the same reason.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
from pathlib import Path

from payment_ledger import config
from payment_ledger.redact import mask

log = logging.getLogger("stripe_cli")

REPO_ROOT = Path(__file__).resolve().parents[2]
VENDORED_DIR = REPO_ROOT / ".tools"

# Deliberately wider than the secrets Stripe is known to issue. Matching too narrowly would
# silently truncate a secret at its first unexpected character, and a truncated secret fails
# verification in exactly the way a wrong secret does.
SECRET_PATTERN = re.compile(r"whsec_[A-Za-z0-9_\-]+")

LIVE_KEY_PATTERN = re.compile(r"^(sk|rk)_live_")
TEST_KEY_PREFIXES = ("sk_test_", "rk_test_")

INSTALL_HINT = (
    "The Stripe CLI was not found.\n"
    "Install it (https://docs.stripe.com/stripe-cli) or drop the binary in .tools/, then either\n"
    "put a sandbox key in .env as STRIPE_API_KEY or run `make login` once."
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


def api_key() -> str | None:
    """The sandbox key the CLI should run as, or None to use whatever `stripe login` stored.

    Set `STRIPE_API_KEY` in this repository's gitignored `.env` and every CLI call below is made
    with `--api-key`. That skips the browser pairing entirely, and it keeps the credential in one
    place that belongs to this project rather than in the machine-wide `~/.config/stripe`, which
    is the difference between one thing to rotate and two.

    A live key is refused rather than used. This project exists to be read, and nothing in it has
    any business touching real money: the receiver already rejects a `livemode: true` payload, and
    this is the same rule one step earlier, before a request is made at all.

    The value is validated but never returned in an error message.
    """
    configured = (config.setting("STRIPE_API_KEY", "") or "").strip()
    if not configured:
        return None

    if LIVE_KEY_PATTERN.match(configured):
        raise StripeCliError(
            "STRIPE_API_KEY is a live-mode key, and this project refuses to run against live "
            "mode.\nUse a sandbox key from the test-mode dashboard instead."
        )
    if not configured.startswith(TEST_KEY_PREFIXES):
        raise StripeCliError(
            "STRIPE_API_KEY is set to something that is not a test-mode Stripe key.\n"
            "Unset it to fall back on `make login`, or set it to the sk_test_... shown on the\n"
            "sandbox's API keys page."
        )
    return configured


def command(*args: str) -> list[str]:
    """Build a CLI invocation, carrying the configured key when there is one."""
    cli = require_cli()
    key = api_key()
    return [str(cli), *args, *(["--api-key", key] if key else [])]


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
            f"{mask(output.strip()) or '(nothing)'}\n"
            "If that is a login prompt, run `make login` first."
        )
    return match.group(0)


def print_secret(timeout: int = 30) -> str:
    """Ask the CLI for the webhook signing secret it will forward with."""
    try:
        completed = subprocess.run(  # noqa: S603 - the executable is resolved, not user input
            command("listen", "--print-secret"),
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
            f"{mask((completed.stderr or completed.stdout).strip())}\n"
            "If it is asking for credentials, run `make login` first."
        )

    return parse_secret(completed.stdout + completed.stderr)


def spawn_listener(port: int, path: str = "/webhooks") -> subprocess.Popen[bytes]:
    """Start `stripe listen`, forwarding test-mode deliveries at the local receiver.

    Spawned by the receiver so the capture is one command instead of two terminals that have to
    be started in the right order. The listener is a child process, so it dies with the receiver
    rather than being left forwarding at a closed port.
    """
    target = f"localhost:{port}{path}"
    log.info("starting `stripe listen --forward-to %s`", target)
    return subprocess.Popen(  # noqa: S603 - the executable is resolved, not user input
        command("listen", "--forward-to", target),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


# The event types the ledger is built from. The disputes resolve asynchronously, so the two
# `charge.dispute.*` deliveries arrive minutes apart and the receiver has to still be running.
TRIGGERS = (
    "charge.succeeded",
    "charge.refunded",
    "charge.dispute.created",
    "charge.dispute.closed",
    "payout.paid",
    "payout.failed",
)


def _run(*args: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - the executable is resolved, not user input
        command(*args),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def whoami() -> int:
    """Report which account the CLI will act as, and how it was told to.

    Deliberately not `stripe config --list`, which prints the stored key. This prints the account
    and the mode and stops there.
    """
    key = api_key()
    source = "STRIPE_API_KEY" if key else "the stored `stripe login` credentials"
    completed = _run("get", "/v1/account", timeout=30)
    if completed.returncode != 0:
        print(f"not authenticated, using {source}.")
        print(mask((completed.stderr or completed.stdout).strip()))
        return 1

    body = completed.stdout
    livemode = bool(re.search(r'"livemode"\s*:\s*true', body))
    account = re.search(r'"id":\s*"(acct_[A-Za-z0-9]+)"', body)
    print(f"authenticated via {source}")
    print(f"  account   {account.group(1) if account else 'unknown'}")
    print(f"  mode      {'LIVE' if livemode else 'test'}")
    return 1 if livemode else 0


def trigger(names: tuple[str, ...] = TRIGGERS) -> int:
    """Make the sandbox emit the event types the ledger is built from."""
    failures = 0
    for name in names:
        completed = _run("trigger", name)
        status = "ok" if completed.returncode == 0 else "failed"
        print(f"{status:>7}  {name}")
        if completed.returncode != 0:
            failures += 1
            print(mask((completed.stderr or completed.stdout).strip()))
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    import sys

    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] not in ("whoami", "trigger"):
        print("usage: python -m payment_ledger.stripe_cli {whoami|trigger}")
        return 2

    try:
        return whoami() if args[0] == "whoami" else trigger()
    except StripeCliError as exc:
        print(exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
