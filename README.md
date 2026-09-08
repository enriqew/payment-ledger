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

**Phase 6 of 7.** A run of any size goes end to end and comes out as a double-entry ledger that has
been weighed against the processor's own reported balance: the generator simulates N transactions
from the captured shapes, the producer puts them on Kafka, Spark Structured Streaming lands every
delivery in `bronze.events`, silver deduplicates that into one row per event and one per charge,
refund and dispute, and dbt builds the postings, the trial balance, the daily close and the
reconciliation, with all four invariants as tests that stop the build. The six failures in the
table above are injected on purpose, one namespace per scenario, and each is checked against what
it said it would do: `make chaos`. What a dashboard may read, and what the export refuses to
publish, is `make export` and [`docs/EXPORT.md`](docs/EXPORT.md). What is left is the write-up. The
roadmap is at the bottom of `docs/DESIGN.md`, and this line is updated as phases land rather than
in advance.

## Stack

Kafka into Spark Structured Streaming into Apache Iceberg (bronze and silver), dbt for the gold
ledger, Airflow for the chaos suite. Everything runs locally on Docker Compose against MinIO, so a
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

make e2e              # 1000 simulated transactions, to a ledger that reconciles and exports
make e2e N=100000 SEED=7 DAYS=90

make down             # stop, keeping volumes; make clean drops them too
```

`make bronze` submits `jobs/bronze_events.py` inside the Spark container, so the host never needs a
JVM or a matching Python. The first run downloads the Iceberg and Kafka connector jars into a
volume; every run after that is offline. It drains what is on the topic and exits, and the
checkpoint means running it twice does not write the same offsets twice.

The pieces are also separate targets: `make generate` simulates a run, `make produce` publishes one
(or the committed fixtures, with no argument), `make bronze` drains the topic, `make silver`
deduplicates and types it, `make gold` builds the ledger and runs the invariants, `make export`
assembles what a dashboard reads, and `make reset` throws away everything a previous run left
behind.

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

## The ledger

`make gold` runs dbt against the lakehouse through a Spark Thrift server and builds three tables.
Every posting comes off a balance transaction and nothing else, which is why `make silver` also
loads what `/v1/balance_transactions` returns: a webhook names its balance transaction and never
carries it, so the fee, the net and `available_on` are simply not in the event stream.

Each balance transaction makes exactly two entries, and an entry is a set of postings summing to
zero per currency. **Recognition** at `created` puts `net` into `asset:balance_pending` and books
the profit and loss side of it in the same breath. **Availability** at `available_on` moves the
same money into `asset:balance_available`. The second entry is why the pending balance is an
account and not a caption: funds captured today are not available today, the balance transaction
carries the date on which they become so, and a ledger that ignores it reports cash that cannot be
paid out.

At `N=1000` the trial balance comes out as:

| Account | Balance (eur minor units) | Postings |
|---|---:|---:|
| `asset:balance_available` | 28,877,624 | 1152 |
| `asset:balance_pending` | 0 | 2304 |
| `contra_revenue:refunds` | 3,104,418 | 92 |
| `expense:disputes` | 1,276,705 | 60 |
| `expense:processing_fees` | 531,875 | 1000 |
| `revenue:gross_sales` | -33,790,622 | 1000 |
| **total** | **0** | 5608 |

5608 postings across 2304 entries, and the whole ledger sums to zero. `asset:balance_pending`
finishing at zero is the maturation working: every transaction that was created also matured.
`revenue:gross_sales` is negative because revenue is a credit, and turning it positive to make a
dashboard friendlier is exactly how a ledger stops summing to zero.

**The invariants are tests that fail the build, and they have been watched failing.** A test that
has only ever passed is not evidence, so each was broken on purpose:

| Invariant | Broken by | What dbt did |
|---|---|---|
| Entries sum to zero | dropping the fee leg from a charge | `FAIL 1000`, one per charge, 10 downstream nodes skipped |
| Every balance transaction is posted | filtering disputes out of the model | `FAIL 43`, exactly the dropped disputes, 10 nodes skipped |

Both times the build stopped before `account_balances` or `daily_close` were written, which is the
point: a broken ledger does not get published and then corrected.

The third invariant, that the available balance never goes negative without a dispute or a refund
explaining it, passes and has not been observed failing. Every way of breaking the ledger badly
enough to trip it trips the first invariant first, which is worth knowing rather than glossing:
entries summing to zero is the tighter net.

## The reconciliation

The fourth invariant is the only one that can see outside the books, and that is the whole point of
it. The other three prove the ledger is internally consistent, and internally consistent is not the
same as right.

`gold.reconciliation` compares the ledger's daily close against the balance the processor reports,
day by day and currency by currency. The reported side is loaded by `make silver` from what the
generator computed by walking the same money with **separate code that never reads an Iceberg
table**: two walks sharing an implementation would agree by construction and prove nothing. Over
all 62 days of an `N=1000` run the two agree exactly, closing at 28,877,624 available and nothing
pending.

**No tolerance is granted.** Both sides are integers in minor units, so there is nothing to round
and a tolerance is only somewhere for a real discrepancy to live. A cent fails the run like a
million.

**What it catches that nothing else does.** A balance transaction the processor never reported was
injected into the list, and the ledger posted it happily:

| Check | Result |
|---|---|
| Entries sum to zero | PASS, both of its entries balanced |
| Every balance transaction is posted | PASS, it was posted |
| Available balance is explained | PASS |
| Both sides close the same days | PASS |
| **No unexplained difference** | **FAIL 62**, and the build exits 1 |

The books were internally consistent and wrong by exactly one transaction, and only the comparison
against something the pipeline did not produce noticed.

`gold.reconciliation_items` is the itemisation. It keys on the day the difference **moved**, not on
the days it is wrong: a balance difference is cumulative, so one bad transaction on the third makes
every day after it wrong by the same amount and itemising all of them means listing the whole run.
On the injected fault it narrowed 1153 transactions to 29 rows on a single day, and flagged the two
whose net matched the movement exactly. One of them was the phantom. The other had the same net by
coincidence, which is worth saying rather than claiming the model points at one row: it hands a
human two candidates instead of a run to read.

It is also deliberately not built from `reconciliation`. dbt skips everything downstream of a
failed test, so hanging the itemisation off the table the invariant guards would take the detail
away at exactly the moment somebody needs it. Both read a shared ephemeral model instead, and the
first version of this got that wrong: the run that failed also skipped the table explaining why.

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

## The chaos suite

The six failures at the top of this README were six claims until phase 5. Each is now a scenario
that damages a real run, takes it through the whole pipeline, and is judged against what it said
would happen.

```bash
make scenarios                          # the six, and what is supposed to catch each
make chaos                              # all seven arms, injected, run and judged
make chaos-one SCENARIO=dropped_event   # one of them
```

**A scenario perturbs one side and not the other.** The generator emits three artifacts: the
webhook stream, the balance transaction list, and the balance the processor reports. A scenario
damages one and leaves the rest alone, which is what makes a divergence appear where a real one
would. Damaging all three consistently produces a run that is wrong and reconciles, which is the
failure nobody catches.

**The expectation is written before the run, and checked both ways.** Every scenario declares which
dbt tests must fail and, where the injection determines it, on exactly how many rows. The verdict
fails when a detection did not fire, and equally when a test fires that no scenario asked for. A
suite that only checks "something went wrong" says nothing about whether the right thing did.

This is one `N=1000` run, seed 1, all seven arms:

| Arm | What it does to the run | What the build did |
|---|---|---|
| baseline | nothing | 41 tests pass. 4602 events, 5608 postings, trial balance 0, 28,877,624 available |
| Duplicate delivery | redelivers 230 events, later in the stream | bronze holds 4832 deliveries of 4602 events, silver holds 4602, the ledger is unchanged to the posting. Nothing fails |
| Out-of-order arrival | reverses arrival order end to end | every count identical to the baseline, including the checksum over the dates the charges carry. Nothing fails |
| Dropped event | silences 20 charges, 84 events | `assert_the_stream_saw_every_source` **FAIL 21**. 980 charges in silver, and the ledger is identical to the baseline: 5608 postings, reconciliation clean |
| Late arrival after the close | holds 23 movements and their 64 events back to a second wave | nothing fails. Two closes taken, **60 days restated**, all 60 explained by what arrived late, and the final books equal the undamaged run exactly |
| Currency and rounding | adds one minor unit to the net of 20 charges | `assert_entries_balance` **FAIL 20**, trial balance 20 instead of 0, and the daily close and the reconciliation are **never published** |
| Reversal | drops 3 won-dispute reversals from the list | `assert_the_list_holds_every_expanded_transaction` **FAIL 3** and `assert_no_unexplained_difference` **FAIL 43**. Every entry still balances and the trial balance is still 0 |

**A namespace per arm, and nothing dropped between them.** Each scenario runs into
`<scenario>_bronze`, `<scenario>_silver` and `<scenario>_gold`, with a Kafka topic and a Spark
checkpoint of its own. When the suite finishes, the damaged run and the run it was measured against
are both in the catalog, so a difference of twenty charges is a query rather than a claim.

**Two of the six are not caught by an invariant, and that is the finding.** A dropped webhook does
not move a cent. The ledger posts from the balance transaction list, so every entry balances and
every day reconciles exactly, and what is missing is the business's own record of what the money
was for. Only a comparison between the two inputs sees it, which is `gold.coverage_gaps`. And a
late arrival is not a defect at all: it is the normal condition of a payment processor, and what it
has to produce is a restatement rather than a failure.

**What a restatement looks like.** On the late arrival arm, the third of January moved from
4,045,390 pending to 4,044,210 and names the one movement that landed after it closed. The fourth
of January moved by the same 1,180 and names nothing, which is the point: a balance is cumulative,
so the day the number entered is the day worth reading and every day after it inherits the shift.
A day that moved by something other than what arrived late fails the build.

### The same suite as a DAG

```bash
LEDGER_HOST_ROOT="$(pwd)" make airflow   # on Windows: pwd -W
make dag
```

`airflow/dags/chaos_suite.py` lays the same seven arms out as 84 tasks: inject, create the
topic, publish, the five Spark jobs, the ledger, the measurement and the verdict, per wave. It is not a second
implementation. The steps come from the same functions the command line uses, and what differs is
only how a step becomes a running container.

It has been run as a whole, not only parsed: one trigger, 84 tasks, every arm reaching the same
verdict it reaches from the command line, with the same tests failing on the same rows.

Scheduling is not what it is for. One machine runs one Spark job at a time and nothing here runs on
a clock. What the DAG has is a task boundary around every step, so a suite that goes wrong says
which step went wrong instead of leaving it at the end of a log.

The scheduler starts sibling containers through the Docker socket, which is why it has to be told
where the repository lives **on the host**: the daemon binds host paths and knows nothing about the
inside of the Airflow container. That is the honest cost of orchestrating containers from within
one, and it is the reason the service definitions appear a second time inside the DAG.

## What a dashboard reads

A page reads files. The one this feeds is a static site with no backend, so the interface is a
handful of JSON files somebody copies, and the interesting part is not the copying. It is deciding
what those files may contain and refusing to write them when the run behind them does not deserve
to be read.

```bash
make export     # dump the gold tables, assemble the artifacts, check every promise
```

One `N=1000` run produces 37 KB:

| File | Rows | What one row is |
|---|---|---|
| `manifest.json` | 1 | the run behind the export, its fingerprint and a count per file |
| `accounts.json` | 6 | the trial balance, with the signs double entry uses |
| `flow.json` | 5 | where the money went, account to account |
| `daily_close.json` | 62 | what the books say that day closed at, and what moved |
| `reconciliation.json` | 62 | the ledger weighed against the balance the processor reports |
| `findings.json` | 0 | coverage gaps and restatements, empty when the inputs agreed |
| `chaos.json` | 7 | the six failures injected on purpose, and what fired |

**It refuses rather than warns.** A trial balance that does not sum to zero, a day the
reconciliation cannot explain, a float where money should be, a day written as a timestamp rather
than a plain ISO day, or a chaos suite carrying some of its arms but not all of them, and nothing
is written at all. A page has no way to find any of that out afterwards, so the checks are on the
way out.

**Bounded by the calendar, not by volume.** Every artifact is a day, an account or a scenario, so a
hundred thousand transactions produce the same 62 daily rows as a thousand. `ledger_postings` is
deliberately not exported: it is the one table that grows with volume, and a page that wanted
individual postings would be asking for a query engine rather than for a file.

**Byte identical for the same run.** There is no timestamp anywhere in it. What identifies an
export is the fingerprint of the generated run it was built from, recorded in the manifest beside
the seed and the transaction count, so an export whose numbers moved is an export whose input
moved and a diff says so.

The full contract, including what a consumer may and may not do with the files, is
[`docs/EXPORT.md`](docs/EXPORT.md). The export itself is not committed, for the same reason
`data/` is not: it is recomputable from the seed, and what is worth committing is the contract.

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
  chaos.py         the six failures as inputs, and the verdict on a run that met one
  chaos_run.py     each scenario through the pipeline, into a lakehouse of its own
  export.py        the artifacts a dashboard reads, and every promise they have to keep
scripts/
  audit_publishable.py   the same rules, over every tracked file
docker/            the local stack
jobs/
  bronze_events.py   the kafka topic into bronze.events, structured streaming
  silver_events.py   bronze deliveries into one row per event, merged on the key
  silver_entities.py the event log into one row per charge, refund and dispute
  balance_transaction_list.py  what /v1/balance_transactions returns, which webhooks do not carry
  reported_balance.py          the balance the processor reports, which is the anchor
  chaos_report.py              what one arm of the suite ended up holding, as a count per table
  export_tables.py             the gold tables out of the lakehouse, as plain rows
conf/              spark defaults, iceberg catalog wiring, log4j
dbt/               gold ledger models and the invariant tests   (phase 3)
airflow/dags/      chaos_suite, the same seven arms as a DAG    (phase 5)
fixtures/events/   captured test-mode payloads, redacted and committed
data/generated/    what a run writes; reproducible from its seed, so never committed
data/chaos/        what each scenario was given, and the verdict on what came back
export/            what a dashboard reads; recomputable, so the contract is what is committed
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

**The rule held when it was inconvenient, which is the only test of one.** dbt insists on a file
called `profiles.yml`, and the audit rejects that name wherever it appears. Moving it somewhere the
rule does not look would have been evasion, and carving out an exception would have been the exact
thing the previous paragraph refuses. So the profile is rendered at container start from the
environment, into a directory that exists only inside the container: nothing named `profiles.yml`
is ever tracked, and the thing it would have held (a host, a port, a schema) is deployment config
rather than a credential.

Secondary, since the data is mock either way: a payload arriving with `livemode: true` is refused
rather than cleaned up, because it means the CLI is authenticated somewhere it should not be. Test
payloads still have the account id, the receipt URL's token and personal fields replaced on write,
while ids, amounts, fees and `request.idempotency_key` are left alone, since those are what the
pipeline joins and deduplicates on. See [`fixtures/README.md`](fixtures/README.md).

Reasoning in [`docs/DESIGN.md`](docs/DESIGN.md), section 7.

## Licence

MIT, see [`LICENSE`](LICENSE).
