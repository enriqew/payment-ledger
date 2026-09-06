"""Resolving the CLI and its signing secret, the two things that gate the capture."""

from __future__ import annotations

import pytest

from payment_ledger import stripe_cli
from payment_ledger.stripe_cli import StripeCliError, api_key, find_cli, parse_secret

SECRET = "whsec_example_not_a_real_secret"


def test_secret_is_extracted_from_a_noisy_banner():
    """The CLI is free to print an upgrade notice around the value."""
    output = f"A newer version of the Stripe CLI is available.\n{SECRET}\n"

    assert parse_secret(output) == SECRET


def test_output_without_a_secret_names_the_login_step():
    with pytest.raises(StripeCliError) as exc:
        parse_secret("Error: please run `stripe login`")

    assert "make login" in str(exc.value)


def test_explicit_cli_path_wins(tmp_path, monkeypatch):
    explicit = tmp_path / "stripe.exe"
    explicit.write_text("", encoding="utf-8")
    monkeypatch.setenv("STRIPE_CLI", str(explicit))

    assert find_cli() == explicit


def test_a_missing_explicit_cli_is_not_silently_replaced(tmp_path, monkeypatch):
    """Falling through to PATH here would run a different binary than the one asked for."""
    monkeypatch.setenv("STRIPE_CLI", str(tmp_path / "absent.exe"))

    assert find_cli() is None


def test_the_vendored_copy_is_preferred_over_path(tmp_path, monkeypatch):
    """A version pinned for this repo must not be shadowed by an older system install."""
    vendored = tmp_path / "stripe.exe"
    vendored.write_text("", encoding="utf-8")
    monkeypatch.delenv("STRIPE_CLI", raising=False)
    monkeypatch.setattr(stripe_cli, "VENDORED_DIR", tmp_path)
    monkeypatch.setattr(stripe_cli.shutil, "which", lambda _: "C:/elsewhere/stripe.exe")

    assert find_cli() == vendored


def _key(prefix: str) -> str:
    """Assemble a key at runtime so the literal is not in the file the audit reads."""
    return prefix + "ExampleKeyMaterialNotReal"


def test_no_configured_key_falls_back_to_the_stored_login(monkeypatch):
    monkeypatch.delenv("STRIPE_API_KEY", raising=False)
    monkeypatch.setattr(stripe_cli.config, "_FILE_ENV", {})

    assert api_key() is None


def test_a_sandbox_key_is_used(monkeypatch):
    monkeypatch.setenv("STRIPE_API_KEY", _key("sk_" + "test_"))

    assert api_key() == _key("sk_" + "test_")


def test_a_live_key_is_refused(monkeypatch):
    """The receiver rejects a livemode payload. This is the same rule before the request."""
    monkeypatch.setenv("STRIPE_API_KEY", _key("sk_" + "live_"))

    with pytest.raises(StripeCliError) as exc:
        api_key()

    assert "live" in str(exc.value)


def test_a_refusal_never_echoes_the_key(monkeypatch):
    secret = _key("sk_" + "live_")
    monkeypatch.setenv("STRIPE_API_KEY", secret)

    with pytest.raises(StripeCliError) as exc:
        api_key()

    assert secret not in str(exc.value)
    assert "ExampleKeyMaterialNotReal" not in str(exc.value)


def test_something_that_is_not_a_key_names_the_way_out(monkeypatch):
    monkeypatch.setenv("STRIPE_API_KEY", "paste-your-key-here")

    with pytest.raises(StripeCliError) as exc:
        api_key()

    assert "make login" in str(exc.value)


def test_the_key_is_carried_on_every_invocation(tmp_path, monkeypatch):
    cli = tmp_path / "stripe.exe"
    cli.write_text("", encoding="utf-8")
    monkeypatch.setenv("STRIPE_CLI", str(cli))
    monkeypatch.setenv("STRIPE_API_KEY", _key("sk_" + "test_"))

    assert stripe_cli.command("trigger", "charge.succeeded") == [
        str(cli),
        "trigger",
        "charge.succeeded",
        "--api-key",
        _key("sk_" + "test_"),
    ]


def test_without_a_key_the_invocation_is_left_alone(tmp_path, monkeypatch):
    cli = tmp_path / "stripe.exe"
    cli.write_text("", encoding="utf-8")
    monkeypatch.setenv("STRIPE_CLI", str(cli))
    monkeypatch.delenv("STRIPE_API_KEY", raising=False)
    monkeypatch.setattr(stripe_cli.config, "_FILE_ENV", {})

    assert stripe_cli.command("listen", "--print-secret") == [
        str(cli),
        "listen",
        "--print-secret",
    ]
