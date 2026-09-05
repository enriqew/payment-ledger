# Payment Ledger

A reconciliation pipeline for payment events. It consumes a payment processor's webhook stream,
builds a double-entry ledger from it, and weighs that ledger against the balance the processor
itself reports.

**What this is not.** It does not process real payments and it never touches real money or real
customer data. Event *schemas* come from Stripe's **test mode**, captured with the Stripe CLI
against a sandbox account. Event *volume* comes from a generator that replays those captured shapes
across a simulated calendar. Both halves are stated wherever a number is reported.

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
- The [Stripe CLI](https://docs.stripe.com/stripe-cli), authenticated against a **test mode**
  sandbox account, for the capture step

## Quick start

```bash
make install          # host venv for the capture receiver, generator and tests
cp .env.example .env  # then fill in STRIPE_WEBHOOK_SECRET, see below

make up               # kafka, minio, iceberg rest catalog
make ps               # check everything is healthy
make down             # stop, keeping volumes; make clean drops them too
```

Services appear in the stack with the phase that needs them, pinned when there is a job to run
against them rather than guessed at in advance. Spark arrives in phase 1, Airflow in phase 5.

## Capturing real test-mode events

This is the step that gives every schema in the pipeline a real payload behind it. It needs three
terminals.

```bash
# 1. forward test-mode webhooks to the local receiver.
#    the command prints a signing secret (whsec_...); put it in .env as STRIPE_WEBHOOK_SECRET
stripe listen --forward-to localhost:4242/webhooks

# 2. run the receiver, which verifies each signature and writes the event to fixtures/events/
make capture

# 3. make the sandbox emit the event types the ledger is built from
make trigger
```

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
  capture.py       the receiver: verify, write fixture, log
docker/            the local stack
conf/              spark defaults, iceberg catalog wiring
dbt/               gold ledger models and the invariant tests   (phase 3)
airflow/dags/      ledger_daily, ledger_chaos                   (phase 5)
fixtures/events/   captured test-mode payloads, committed
tests/
```

## Licence

MIT, see [`LICENSE`](LICENSE).
