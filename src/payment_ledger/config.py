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


@dataclass(frozen=True)
class CaptureSettings:
    """Settings for the phase 0 webhook capture receiver."""

    webhook_secret: str
    host: str
    port: int
    tolerance_seconds: int
    fixtures_dir: Path

    @classmethod
    def from_env(cls) -> CaptureSettings:
        secret = _get("STRIPE_WEBHOOK_SECRET", "")
        if not secret or secret == "whsec_replace_me":
            raise SystemExit(
                "STRIPE_WEBHOOK_SECRET is not set.\n"
                "Run `stripe listen --forward-to localhost:4242/webhooks`, copy the whsec_... it\n"
                "prints, and put it in .env. The secret is per listen session, so it changes\n"
                "every time the CLI is restarted."
            )

        fixtures = Path(_get("FIXTURES_DIR", "fixtures/events") or "fixtures/events")
        if not fixtures.is_absolute():
            fixtures = REPO_ROOT / fixtures

        return cls(
            webhook_secret=secret,
            host=_get("CAPTURE_HOST", "127.0.0.1") or "127.0.0.1",
            port=int(_get("CAPTURE_PORT", "4242") or 4242),
            tolerance_seconds=int(_get("WEBHOOK_TOLERANCE_SECONDS", "300") or 300),
            fixtures_dir=fixtures,
        )
