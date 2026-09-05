# Payment Ledger

[![ci](https://github.com/enriqew/payment-ledger/actions/workflows/ci.yml/badge.svg)](https://github.com/enriqew/payment-ledger/actions/workflows/ci.yml)

A reconciliation pipeline for payment events. It consumes a payment processor's webhook stream,
builds a double-entry ledger from it, and weighs that ledger against the balance the processor
itself reports.

**What this is not.** It is not affiliated with, endorsed by or connected to Stripe; it uses a
public API in test mode. It does not process real payments and it never touches real money or real
customer data. Event *schemas* come from Stripe's **test mode**, captured with the Stripe CLI
against a sandbox account and redacted before they are committed. Event *volume* comes from a
generator that replays those captured shapes across a simulated calendar. Both halves are stated
wherever a number is reported.

The subject of the project is not throughput. It is the six failures that make money hard, each one
injected deliberately and each one caught:

| Failure | Why it happens | What the pipeline does |
|---|---|---|
| Duplicate delivery | Webhooks are at-least-once; a timeout on the receiver means a redelivery | Deduplicates on `event.id`. Bronze keeps both, silver keeps one, the ledger is unchanged |
| Out-of-order arrival | A refund can land before the charge it refunds | Orders by event time per entity, not by arrival |
| Late arrival after the close | A dispute opens weeks after the charge | Restates the closed period and keeps both versions |
| Dropped event | A delivery is lost and never retried inside its window | Detected as a gap against the processor's own balance transaction list |
| Currency and rounding | Multi-currency settlement in integer minor units | No float touches a monetary value at any layer |
| Reversal | A won dispute moves the balance backwards, and the fee stays | Signed postings, no assumption that balances only grow |

Full design: [`docs/DESIGN.md`](docs/DESIGN.md).

## Status

**Phase 0 of 7.** The scaffold, the local stack and the webhook capture are in place. Nothing
downstream of the capture exists yet. The roadmap is at the bottom of `docs/DESIGN.md`, and this
line is updated as phases land rather than in advance.

## Stack

Kafka into Spark Structured Streaming into Apache Iceberg (bronze and silver), dbt for the gold
ledger, Airflow for the daily close. Everything runs locally on Docker Compose against MinIO, so a
full run costs nothing and there is no always-on service.

## Requirements

- Docker with Compose v2
- Python 3.12 on the host (the host only runs the capture receiver, the generator and the tests;
  Spark runs inside containers, which is why the host interpreter does not need to match Spark's)
- The [Stripe CLI](https://docs.stripe.com/stripe-cli), for the capture step. A copy dropped in
  `.tools/` (gitignored) is used ahead of anything on PATH, so it needs no system-wide install.
  `make login` authenticates it against a **test mode** sandbox account, once

## Quick start

**No Stripe account is needed to run this.** The captured payloads are committed and the generator
produces the volume, so a clone is enough. An account is needed only to refresh the fixtures.

```bash
make install          # host venv for the receiver, the generator and the tests
make check            # audit, lint and tests, the same three CI runs

make up               # kafka, minio, iceberg rest catalog
make ps               # check everything is healthy
make down             # stop, keeping volumes; make clean drops them too
```

`cp .env.example .env` only if you want to override a default. Nothing in it is required.

Services appear in the stack with the phase that needs them, pinned when there is a job to run
against them rather than guessed at in advance. Spark arrives in phase 1, Airflow in phase 5.

## Capturing real test-mode events

This is the step that gives every schema in the pipeline a real payload behind it.

```bash
make login      # once, opens a browser and authenticates against the sandbox
make capture    # verifies each signature, writes to fixtures/events/
make trigger    # in a second terminal: make the sandbox emit the event types the ledger needs
```

`make capture` binds its port and then starts `stripe listen --forward-to localhost:4242/webhooks`
as a child process, so there is no ordering to get right and the listener dies with the receiver.
It also asks the CLI for the signing secret (`stripe listen --print-secret`) rather than having it
copied into `.env` by hand, which is how a receiver ends up verifying against a stale secret and
rejecting every delivery for what looks like a key problem. Set `STRIPE_WEBHOOK_SECRET` to pin a
specific secret, or `CAPTURE_SPAWN_LISTEN=0` to run the listener yourself.

`make fixtures` counts what has been captured, by event type.

Each event lands at `fixtures/events/<type>/<event_id>.json`, so a redelivery of the same event
overwrites its own file rather than creating a second one. The receiver logs a redelivery when it
sees one, which is worth watching: it is the first of the six failures showing up on its own,
before anything is injected on purpose.

`make trigger` walks the event types the ledger needs: `charge.succeeded`, `charge.refunded`,
`charge.dispute.created`, `charge.dispute.closed`, `payout.paid`, `payout.failed` and
`balance.available`. Some of them the CLI can produce directly; the dispute lifecycle needs the
sandbox to advance on its own, so those arrive late and the receiver simply keeps listening.

**Signature verification is not optional here.** The receiver rejects any request whose
`Stripe-Signature` header does not verify against the signing secret, and rejects timestamps outside
a five-minute tolerance. A capture step that accepted anything would be capturing its own
assumptions instead of the processor's payloads.

## Layout

```
src/payment_ledger/
  config.py        environment-backed settings, one place
  webhook.py       Stripe-Signature parsing and verification (stdlib hmac)
  stripe_cli.py    finding the cli, its signing secret, and the listener subprocess
  capture.py       the receiver: verify, write fixture, log
  redact.py        what may be committed: the live-mode guard and the redaction rules
scripts/
  audit_publishable.py   the same rules, over every tracked file
docker/            the local stack
conf/              spark defaults, iceberg catalog wiring
dbt/               gold ledger models and the invariant tests   (phase 3)
airflow/dags/      ledger_daily, ledger_chaos                   (phase 5)
fixtures/events/   captured test-mode payloads, redacted and committed
tests/
```

## Publishing

This repository is public, and the payloads under `fixtures/` go to a public history where a
mistake is permanent. Three things follow, all enforced rather than remembered:

- A payload with `livemode: true` is **refused** at the point it would become a file, not cleaned
  up. It means the CLI is authenticated somewhere it should not be, which is worth stopping over.
- Test-mode payloads are redacted on write: the account id, the receipt URL's token and personal
  fields go; ids, amounts, fees and `request.idempotency_key` stay, because those are what the
  pipeline joins and deduplicates on. Details in [`fixtures/README.md`](fixtures/README.md).
- `make audit` scans every tracked file against those same rules and fails on a finding, locally
  and in CI. It never prints what it matched.

The reasoning is in [`docs/DESIGN.md`](docs/DESIGN.md), section 7.

## Licence

MIT, see [`LICENSE`](LICENSE).
