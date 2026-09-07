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

**Phase 2 of 7.** A run of any size goes end to end: the generator simulates N transactions from
the captured shapes, the producer puts them on Kafka, Spark Structured Streaming lands every
delivery in `bronze.events`, and silver deduplicates that into one row per event and one row per
charge, refund and dispute. The ledger and the reconciliation are next. The roadmap is at the
bottom of `docs/DESIGN.md`, and this line is updated as phases land rather than in advance.

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

make up               # kafka, minio, iceberg rest catalog, spark
make ps               # check everything is healthy

make e2e              # 1000 simulated transactions, all the way into bronze
make e2e N=100000 SEED=7 DAYS=90

make down             # stop, keeping volumes; make clean drops them too
```

`make bronze` submits `jobs/bronze_events.py` inside the Spark container, so the host never needs a
JVM or a matching Python. The first run downloads the Iceberg and Kafka connector jars into a
volume; every run after that is offline. It drains what is on the topic and exits, and the
checkpoint means running it twice does not write the same offsets twice.

The pieces are also separate targets: `make generate` simulates a run, `make produce` publishes one
(or the committed fixtures, with no argument), `make bronze` drains the topic, `make silver`
deduplicates and types it, and `make reset` throws away everything a previous run left behind.

Over the 59 payloads captured in phase 0, `make produce && make bronze` writes 59 rows. Run
`make produce` a second time and bronze reports 118 deliveries of 59 distinct events, which is the
duplicate-delivery failure showing up as a number instead of a claim. Bronze keeps both on
purpose; collapsing them is silver's job and the gap between the two counts is how the duplicate
is detected at all.

## Running it at any size

The captured payloads give the pipeline real schemas and 59 events. Volume comes from the
generator, which deep-copies those captured payloads and overwrites the fields carrying money,
identity and time. Nothing is built out of a reading of the API documentation, because a payload
written from the docs tests the reading rather than the API.

`make e2e` resets before it runs, so a run always starts from nothing: the topic, the streaming
checkpoint, the bronze table and its files all go. Forget any one of them and the next run reports
the previous one's numbers.

A run is completely described by `N`, `SEED` and `DAYS`, and `data/generated/run.json` records
those three beside a sha256 of each artifact. The same three inputs produce the same digests, which
is what makes a failure scenario something to replay rather than something that was seen once.

**Everything the generator produces is simulated and says so.** Ids carry a `sim` infix
(`ch_sim000000000042`), `description` reads `(simulated by payment-ledger generator)`, and
`livemode` stays false. No figure from this data may be reported as production behaviour.

At `N=1000` over 30 days, one run produces:

| Artifact | Rows | What it is |
|---|---|---|
| `events.jsonl` | 4602 | the webhook stream: 1000 charges, 92 of them refunded, 43 disputed |
| `balance_transactions.jsonl` | 1152 | what `/v1/balance_transactions` would return |
| `daily_balance.jsonl` | 62 | the balance the processor reports, per day and currency |

**The second and third artifacts are the interesting ones.** The captured payloads make something
plain that a schema document would not: a webhook does not carry the money. `charge.updated` carries
`balance_transaction` as an *id*, so the fee, the net and `available_on` are not in the event stream
at all, and only a dispute embeds its balance transactions expanded. A ledger built on webhooks
alone cannot compute a fee. The pipeline has to join the event stream against the balance
transaction list, and phase 4 reconciles the result against a daily balance walked by different
code over the same simulated money.

No timing is quoted here, deliberately. The subject of this project is correctness under failure,
and a wall clock off a laptop container invites exactly the reading the design's reporting rules
refuse.

## What silver does with it

Bronze holds deliveries. Silver holds events. `make silver` runs two jobs, and the split between
them is the argument.

`silver_events.py` streams `bronze.events` and merges into `silver.events` on `event_id`. Not a
watermarked `dropDuplicates`, because a watermark forgets: set it to an hour and a redelivery
ninety minutes late becomes a second event, and the ledger doubles a charge for a reason nobody
will find looking at the ledger. Stripe retries a failed webhook for up to three days, so the
watermark that would actually be safe is three days of state carried in a streaming shuffle.
Merging on the key the table already holds is bounded by the table instead of by a guess about
lateness, and it is correct however late a retry is. Each silver row keeps how many deliveries
collapsed into it and when the first and last one arrived, so bronze's evidence is summarised
rather than thrown away.

`silver_entities.py` projects that log onto `silver.charges`, `silver.refunds`, `silver.disputes`
and `silver.balance_transactions`, taking each entity's state from its latest event **by event
time**. That clause is the out-of-order failure. The projection is recomputed rather than
maintained, because a streaming "latest per key" has to remember every key it has ever seen (the
event that corrects one arrives weeks later by design) while a recomputation is bounded by the log.

At `N=1000`, one run lands:

| Table | Rows |
|---|---|
| `bronze.events` | 4602 deliveries |
| `silver.events` | 4602 events |
| `silver.charges` | 1000 |
| `silver.refunds` | 92 |
| `silver.disputes` | 43 |
| `silver.balance_transactions` | 60 |

Publish the same run a second time and bronze goes to 9204 while silver stays at 4602, every row
now reading `deliveries = 2`. That is the duplicate-delivery failure caught, as a query rather than
a claim, and nothing downstream of silver can tell it happened except by asking.

**`silver.balance_transactions` is deliberately thin, and the number is the point.** 1540 events
name a balance transaction and the stream carries 60 of them expanded, because only a dispute
embeds the object and everything else is an id. That gap is the size of the join phase 4 makes
against the balance transaction list, and it is why the generator emits that list as an artifact of
its own.

All 43 disputes are dated after the charge they contest, which is the ordering silver is built to
respect and the arrival order it refuses to trust.

**An entity is dated by itself, not by the last event that touched it.** The two are easy to
collapse into one column and the consequence is quiet: a refunded charge would move to the day it
was refunded, leave the day it was actually taken, and change what that day's report says with
nothing looking wrong. It showed up as `max(created)` on `silver.charges` reading five days past a
thirty day calendar, which is the kind of thing a query finds and a test suite does not, so there
are now tests for both halves of it.

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
  producer.py      fixtures or a generated run, onto the kafka topic
  generator.py     simulated volume, replayed from the captured shapes
scripts/
  audit_publishable.py   the same rules, over every tracked file
docker/            the local stack
jobs/
  bronze_events.py   the kafka topic into bronze.events, structured streaming
  silver_events.py   bronze deliveries into one row per event, merged on the key
  silver_entities.py the event log into one row per charge, refund and dispute
conf/              spark defaults, iceberg catalog wiring, log4j
dbt/               gold ledger models and the invariant tests   (phase 3)
airflow/dags/      ledger_daily, ledger_chaos                   (phase 5)
fixtures/events/   captured test-mode payloads, redacted and committed
data/generated/    what a run writes; reproducible from its seed, so never committed
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
