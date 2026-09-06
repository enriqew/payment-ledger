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

import json
import logging
import os
import re
import shutil
import subprocess
import time
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


# What `stripe trigger` can produce on its own. The disputes resolve asynchronously, so the two
# `charge.dispute.*` deliveries arrive minutes apart and the receiver has to still be running.
TRIGGERS = (
    "charge.succeeded",
    "charge.refunded",
    "charge.dispute.created",
    "charge.dispute.closed",
    "balance.available",
)

# The payout leg, kept separate because it does not depend on the CLI but on the sandbox.
# `stripe trigger` ships fixtures for these two, but creating a payout needs an external bank
# account, and a fresh sandbox has none: the API answers "you don't have any external accounts in
# that currency". Attaching one is a dashboard step that no API key of ours is allowed to do, so
# these are attempted apart and their failure is explained instead of read as a broken command.
#
# `payout.paid` and `payout.failed` have no trigger fixture at all. They follow a real payout
# reaching a terminal state, which is why section 3 of the design lists them as captured rather
# than triggered.
PAYOUT_TRIGGERS = ("payout.created", "payout.updated")

NO_EXTERNAL_ACCOUNT = "external accounts"

PAYOUT_HINT = (
    "No payout could be created: this sandbox has no external bank account.\n"
    "Add a test one under Settings > Payouts in the sandbox dashboard and run this again.\n"
    "Everything else in phase 0 works without it."
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


def trigger(names: tuple[str, ...] = TRIGGERS + PAYOUT_TRIGGERS) -> int:
    """Make the sandbox emit the event types the ledger is built from.

    Returns non-zero only for a failure that is not the known missing-bank-account one, so a
    sandbox without payouts configured still reports the rest of the capture as the success it is.
    """
    failures = 0
    no_external_account = False

    for name in names:
        completed = _run("trigger", name)
        body = (completed.stderr or completed.stdout).strip()
        if completed.returncode == 0:
            print(f"     ok  {name}")
            continue
        if NO_EXTERNAL_ACCOUNT in body:
            print(f"skipped  {name}")
            no_external_account = True
            continue
        print(f" failed  {name}")
        print(mask(body))
        failures += 1

    if no_external_account:
        print(f"\n{PAYOUT_HINT}")
    return 1 if failures else 0


# Responding to a dispute in test mode is driven by the evidence text, not by a card number.
# These three values are Stripe's, documented under Testing > Disputes.
ESCALATE = "escalate_inquiry_evidence"
WIN = "winning_evidence"

INQUIRY_PREFIX = "warning_"
SETTLED = ("won", "lost")


def _dispute(dispute_id: str) -> dict:
    completed = _run("get", f"/v1/disputes/{dispute_id}", timeout=30)
    return json.loads(completed.stdout)


def _await_settlement(dispute_id: str, done: tuple[str, ...], tries: int = 25) -> dict:
    """Poll one dispute until it leaves the states we are waiting on.

    Every transition here is asynchronous on Stripe's side, and each one is its own webhook
    delivery. Polling the object is only how this process knows when to move on; the payloads the
    capture is after arrive at the receiver regardless.
    """
    dispute = _dispute(dispute_id)
    for _ in range(tries):
        if dispute.get("status") in done:
            return dispute
        time.sleep(6)
        dispute = _dispute(dispute_id)
    return dispute


def disputes(lose: bool = True) -> int:
    """Drive one dispute from inquiry to a settled chargeback.

    `stripe trigger charge.dispute.created` does not produce a chargeback. It produces an
    inquiry, whose statuses are all prefixed `warning_` and whose closure moves no money at all:
    `balance_transactions` comes back empty. A ledger built on those payloads would never see the
    reversal it exists to account for.

    The lifecycle is therefore driven the whole way here. Escalating the inquiry turns it into a
    real chargeback, and settling that one is what produces `charge.dispute.funds_withdrawn` and a
    balance transaction with the money on it.
    """
    before = {
        d["id"]
        for d in json.loads(_run("get", "/v1/disputes", "-d", "limit=30").stdout).get("data", [])
    }

    if _run("trigger", "charge.dispute.created").returncode != 0:
        print("could not create an inquiry")
        return 1

    inquiry = None
    for _ in range(20):
        time.sleep(3)
        current = json.loads(_run("get", "/v1/disputes", "-d", "limit=30").stdout).get("data", [])
        new = [d for d in current if d["id"] not in before]
        if new:
            inquiry = new[0]
            break

    if inquiry is None:
        print("the inquiry never appeared")
        return 1
    print(f"inquiry      {inquiry['id']}  {inquiry['status']}")

    _run(
        "post",
        f"/v1/disputes/{inquiry['id']}",
        "-d",
        f"evidence[uncategorized_text]={ESCALATE}",
        "-d",
        "submit=true",
    )
    escalated = _await_settlement(inquiry["id"], done=("needs_response",))
    print(f"escalated    {escalated['status']}")
    if escalated["status"].startswith(INQUIRY_PREFIX):
        print("the inquiry did not escalate; nothing further to do")
        return 1

    if lose:
        # Accepting liability is the documented way to lose one on purpose.
        _run("post", f"/v1/disputes/{inquiry['id']}/close")
    else:
        _run(
            "post",
            f"/v1/disputes/{inquiry['id']}",
            "-d",
            f"evidence[uncategorized_text]={WIN}",
            "-d",
            "submit=true",
        )

    settled = _await_settlement(inquiry["id"], done=SETTLED)
    movements = [(b["amount"], b["currency"]) for b in settled.get("balance_transactions", [])]
    print(f"settled      {settled['status']}  moved {movements or 'nothing'}")
    return 0 if settled["status"] in SETTLED else 1


def main(argv: list[str] | None = None) -> int:
    import sys

    actions = {
        "whoami": whoami,
        "trigger": trigger,
        "dispute-lost": lambda: disputes(lose=True),
        "dispute-won": lambda: disputes(lose=False),
    }

    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] not in actions:
        print(f"usage: python -m payment_ledger.stripe_cli {{{'|'.join(actions)}}}")
        return 2

    try:
        return actions[args[0]]()
    except StripeCliError as exc:
        print(exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
