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

**Phase 1 of 7.** The captured payloads reach Kafka and land in an Iceberg `bronze.events` table
on MinIO, keyed by the charge they are about and keeping every delivery including the duplicates.
Silver, the ledger and the reconciliation do not exist yet. The roadmap is at the bottom of
`docs/DESIGN.md`, and this line is updated as phases land rather than in advance.

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
make install          # host venv for the receiver, the producer, the generator and the tests
make check            # audit, lint and tests, the same three CI runs

make up               # kafka, minio, iceberg rest catalog
make ps               # check everything is healthy

make produce          # the committed fixtures onto the kafka topic
make bronze           # drain the topic into bronze.events

make down             # stop, keeping volumes; make clean drops them too
```

`make bronze` submits `jobs/bronze_events.py` inside the Spark container, so the host never needs a
JVM or a matching Python. The first run downloads the Iceberg and Kafka connector jars into a
volume; every run after that is offline. It drains what is on the topic and exits, and the
checkpoint means running it twice does not write the same offsets twice.

Over the 59 payloads captured in phase 0, the first run writes 59 rows. Run `make produce` again
and the second `make bronze` reports 118 deliveries of 59 distinct events, which is the
duplicate-delivery failure showing up as a number instead of a claim. Bronze keeps both on
purpose; collapsing them is silver's job and the gap between the two counts is how the duplicate
is detected at all.

`cp .env.example .env` only if you want to override a default. Nothing in it is required.

**Without `make`** (Windows, mostly), every target is one line; `make help` lists them and the
Makefile shows the command. The three that matter:

```bash
python -m venv .venv && .venv/Scripts/python.exe -m pip install -e ".[dev]"
.venv/Scripts/python.exe scripts/audit_publishable.py && .venv/Scripts/python.exe -m pytest
docker compose -f docker/docker-compose.yml up -d
```

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

`make trigger` walks what the CLI can actually produce: `charge.succeeded`, `charge.refunded`,
`charge.dispute.created`, `charge.dispute.closed` and `balance.available`. The captured set comes
out wider than that, because triggering one event pulls its whole lifecycle behind it.

`make dispute` exists because a triggered dispute is an **inquiry**, not a chargeback: its statuses
are all prefixed `warning_` and closing one moves no money at all. It drives one inquiry through to
a settled chargeback, which is the only way the reversal the ledger has to account for ever
appears. `payout.paid` and `payout.failed` have no fixture in the CLI, and the payout events that
do need an external bank account on the sandbox, so the payout leg is not captured yet. Section 3
of the design says so rather than implying otherwise.

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
  producer.py      the captured fixtures onto the kafka topic
scripts/
  audit_publishable.py   the same rules, over every tracked file
docker/            the local stack
jobs/
  bronze_events.py the kafka topic into bronze.events, structured streaming
conf/              spark defaults, iceberg catalog wiring, log4j
dbt/               gold ledger models and the invariant tests   (phase 3)
airflow/dags/      ledger_daily, ledger_chaos                   (phase 5)
fixtures/events/   captured test-mode payloads, redacted and committed
tests/
```

## Publishing

This repository is public. The payloads it commits are mock data from Stripe's test mode, so
nothing in them is worth stealing; **the thing to protect is credentials**, and none of the
following is left to memory.

**No credential can enter the tree.** `scripts/audit_publishable.py` checks every tracked file for
a key or a signing secret, in any mode, with no exception for the files where fake values live: a
rule with a carve-out for tests is a rule that stops catching the real one. It also refuses a set
of *paths* outright, `.env` and `*.pem` and `dbt/profiles.yml` among them, checked against what git
actually tracks rather than against `.gitignore`, since `git add -f` walks straight past an ignore
rule. Most of those paths belong to phases not built yet, which is the point: the moment to refuse
a credentials file is before one exists.

**No credential can be printed.** The signing secret is held in memory and written nowhere. Every
error path that echoes output from the Stripe CLI, a command whose whole job is printing a
credential, runs it through `mask()` first, and the listener's own stdout goes to devnull. The
audit never prints what it matched either: a secret in a CI log is somewhere worse than the file it
came from.

**Three places, one moment each.** The pre-commit hook (`make hooks`) refuses the commit, CI
refuses the push, and `make audit` answers the question on demand. CI matters least of the three:
it runs after the push, and a secret that reached a public history is not undone by deleting it,
it is undone by rotating it.

Secondary, since the data is mock either way: a payload arriving with `livemode: true` is refused
rather than cleaned up, because it means the CLI is authenticated somewhere it should not be. Test
payloads still have the account id, the receipt URL's token and personal fields replaced on write,
while ids, amounts, fees and `request.idempotency_key` are left alone, since those are what the
pipeline joins and deduplicates on. See [`fixtures/README.md`](fixtures/README.md).

Reasoning in [`docs/DESIGN.md`](docs/DESIGN.md), section 7.

## Licence

MIT, see [`LICENSE`](LICENSE).
