"""Phase 0: capture real test-mode webhook payloads into committed fixtures.

Run it alongside `stripe listen --forward-to localhost:4242/webhooks`. Every delivery that
verifies is written to `fixtures/events/<type>/<event_id>.json`, which makes the fixture set
idempotent by construction: a redelivery of the same event rewrites its own file instead of
adding a second one.

That is deliberate. Redelivery is the first of the six failures the pipeline has to handle, and
it shows up here on its own, before anything is injected. The receiver logs it when it happens so
the behaviour is visible rather than silently absorbed.
"""

from __future__ import annotations

import json
import logging
import re
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from payment_ledger.config import CaptureSettings
from payment_ledger.webhook import SignatureError, verify

log = logging.getLogger("capture")

MAX_BODY_BYTES = 1 << 20  # Stripe events are a few KB; a megabyte is already generous.
SAFE_SEGMENT = re.compile(r"[^A-Za-z0-9._-]")


def fixture_path(root: Path, event_type: str, event_id: str) -> Path:
    """Where one event is stored.

    Both segments come off the wire, so both are sanitised before they touch the filesystem.
    `charge.dispute.created` stays readable as a directory name; anything unexpected is
    flattened rather than trusted.
    """
    safe_type = SAFE_SEGMENT.sub("_", event_type) or "unknown"
    safe_id = SAFE_SEGMENT.sub("_", event_id) or "unknown"
    return root / safe_type / f"{safe_id}.json"


def write_fixture(root: Path, event: dict) -> tuple[Path, bool]:
    """Persist one event. Returns its path and whether it had already been captured."""
    path = fixture_path(root, event.get("type", "unknown"), event.get("id", "unknown"))
    already_seen = path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(event, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path, already_seen


class CaptureHandler(BaseHTTPRequestHandler):
    settings: CaptureSettings  # injected by serve()

    protocol_version = "HTTP/1.1"
    server_version = "payment-ledger-capture"

    def do_POST(self) -> None:  # noqa: N802 - name fixed by BaseHTTPRequestHandler
        if self.path.rstrip("/") not in ("/webhooks", ""):
            self._respond(HTTPStatus.NOT_FOUND, "unknown path")
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._respond(HTTPStatus.BAD_REQUEST, "bad content-length")
            return

        if length <= 0 or length > MAX_BODY_BYTES:
            self._respond(HTTPStatus.BAD_REQUEST, "empty or oversized body")
            return

        # The raw bytes are what the signature covers. They are never re-serialised.
        body = self.rfile.read(length)

        signature = self.headers.get("Stripe-Signature")
        if not signature:
            self._respond(HTTPStatus.BAD_REQUEST, "missing Stripe-Signature header")
            return

        try:
            verify(
                body,
                signature,
                self.settings.webhook_secret,
                tolerance_seconds=self.settings.tolerance_seconds,
            )
        except SignatureError as exc:
            log.warning("rejected delivery: %s", exc)
            self._respond(HTTPStatus.BAD_REQUEST, f"signature rejected: {exc}")
            return

        try:
            event = json.loads(body)
        except json.JSONDecodeError as exc:
            self._respond(HTTPStatus.BAD_REQUEST, f"body is not json: {exc}")
            return

        path, already_seen = write_fixture(self.settings.fixtures_dir, event)
        if already_seen:
            log.info(
                "redelivery of %s (%s), fixture rewritten in place",
                event.get("id"),
                event.get("type"),
            )
        else:
            log.info(
                "captured %s (%s) -> %s",
                event.get("id"),
                event.get("type"),
                path.relative_to(self.settings.fixtures_dir.parent.parent),
            )

        self._respond(HTTPStatus.OK, "ok")

    def _respond(self, status: HTTPStatus, message: str) -> None:
        payload = message.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt: str, *args) -> None:
        """Silence the default access log; the handler logs what matters itself."""
        log.debug(fmt, *args)


def serve(settings: CaptureSettings | None = None) -> None:
    settings = settings or CaptureSettings.from_env()
    settings.fixtures_dir.mkdir(parents=True, exist_ok=True)

    handler = type("BoundCaptureHandler", (CaptureHandler,), {"settings": settings})
    httpd = ThreadingHTTPServer((settings.host, settings.port), handler)

    log.info(
        "listening on http://%s:%d/webhooks, writing to %s",
        settings.host,
        settings.port,
        settings.fixtures_dir,
    )
    log.info("point the CLI at it: stripe listen --forward-to localhost:%d/webhooks", settings.port)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        log.info("stopping")
    finally:
        httpd.server_close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    serve()


if __name__ == "__main__":
    main()
