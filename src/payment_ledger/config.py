"""Environment-backed settings, resolved once.

Reads a `.env` file if one is present, then the real environment, which wins. Kept
dependency-free on purpose: phase 0 runs on a bare interpreter so that capturing real payloads
never blocks on installing anything.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def load_dotenv(path: Path | None = None) -> dict[str, str]:
    """Parse a `.env` file into a dict without overriding the real environment.

    Supports `KEY=value`, blank lines and `#` comments. Values may be wrapped in single or
    double quotes. Anything else is left alone rather than guessed at.
    """
    path = path or REPO_ROOT / ".env"
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


_FILE_ENV = load_dotenv()


def _get(key: str, default: str | None = None) -> str | None:
    return os.environ.get(key) or _FILE_ENV.get(key) or default


def setting(key: str, default: str | None = None) -> str | None:
    """Read one setting: the real environment first, then `.env`, then the default."""
    return _get(key, default)


def _flag(key: str, default: bool) -> bool:
    raw = _get(key)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def resolve_webhook_secret() -> str:
    """The signing secret, from the environment if set and from the CLI otherwise.

    Configuration wins over the CLI so a fixed secret can be pinned (a test, a replay against a
    recorded capture). Asking the CLI is the normal path: it is the same value the listener will
    sign with by construction, which a hand-copied one is only until the next restart.
    """
    configured = _get("STRIPE_WEBHOOK_SECRET", "")
    if configured:
        if not configured.startswith("whsec_"):
            # The value is never echoed, not even a prefix of it. Whatever it turns out to be, it
            # was set in a variable meant to hold a credential, and the message is just as
            # actionable without it.
            raise SystemExit(
                "STRIPE_WEBHOOK_SECRET is set to something that is not a signing secret.\n"
                "Unset it to let the CLI supply one, or set it to the whsec_... that\n"
                "`stripe listen --print-secret` prints."
            )
        return configured

    from payment_ledger.stripe_cli import StripeCliError, print_secret

    try:
        return print_secret()
    except StripeCliError as exc:
        raise SystemExit(
            f"{exc}\n\n"
            "Alternatively, run `stripe listen --forward-to localhost:4242/webhooks` yourself\n"
            "and put the whsec_... it prints into .env as STRIPE_WEBHOOK_SECRET."
        ) from exc


@dataclass(frozen=True)
class CaptureSettings:
    """Settings for the phase 0 webhook capture receiver."""

    webhook_secret: str
    host: str
    port: int
    tolerance_seconds: int
    fixtures_dir: Path
    spawn_listener: bool

    @classmethod
    def from_env(cls) -> CaptureSettings:
        fixtures = Path(_get("FIXTURES_DIR", "fixtures/events") or "fixtures/events")
        if not fixtures.is_absolute():
            fixtures = REPO_ROOT / fixtures

        return cls(
            webhook_secret=resolve_webhook_secret(),
            host=_get("CAPTURE_HOST", "127.0.0.1") or "127.0.0.1",
            port=int(_get("CAPTURE_PORT", "4242") or 4242),
            tolerance_seconds=int(_get("WEBHOOK_TOLERANCE_SECONDS", "300") or 300),
            fixtures_dir=fixtures,
            spawn_listener=_flag("CAPTURE_SPAWN_LISTEN", True),
        )
