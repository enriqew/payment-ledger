# Design

The canonical technical document for this repository. It covers what the pipeline does, why it is
shaped the way it is, and what is built so far.

---

## 1. The problem

A payment processor emits a webhook stream. From that stream alone, answer two questions and be
able to defend the answer:

1. What is the balance, per currency, on any given day?
2. Does that balance agree with what the processor itself reports, and if not, exactly why?

The second question is the whole project. Anything can compute a running total. What is hard is
being able to say, when the total disagrees with the source of truth, which events account for the
difference.

## 2. Why a layered lakehouse, when payments are real time

Worth answering directly, because "streaming payments" and "bronze, silver, gold" sound like they
belong to different projects.

**There are two different paths in a payment system, and this is not the fast one.** The real-time
path is authorisation: a card is presented, a risk decision is made, an authorisation is returned,
all inside a second. That path is a low-latency online service with a transactional store behind
it. A Kafka-to-Spark-to-Iceberg pipeline has no business anywhere near it, and this project does
not pretend otherwise.

What this project models is the **derived** path: the ledger, the reporting, the settlement, the
reconciliation. That path is not real time even in principle, because money is not:

- Funds captured today are not available today. The balance transaction carries an `available_on`
  date, so "the balance" is a question about a calendar, not about this instant.
- Payouts run on a schedule.
- Disputes open weeks after the charge they contest.
- Accounting closes a period, and a period is a boundary in time by definition.

Reconciliation is a **comparison at a point in time**. There is no such thing as a continuously
reconciled ledger, because the thing being reconciled against is itself published on a cadence.

**Medallion is not a batch pattern anyway.** The layers are about how refined the data is, not how
often it moves. Each layer here has its own latency, and they are different on purpose:

| Layer | Latency | Why |
|---|---|---|
| bronze | continuous, seconds | Every delivery is durably recorded the moment it arrives, before anything interprets it. This is the audit trail, and it is the only defence against a bug downstream: you can always go back to what actually arrived. |
| silver | continuous, seconds | Deduplication and typing are streaming operations with a watermark. A near-real-time balance can be served from here. |
| gold | periodic, on a close | The ledger, the close and the reconciliation. Periodic because the close is periodic. |

So the streaming half is real: bronze and silver are Spark Structured Streaming, and a duplicate
delivery is caught within seconds of arriving. The batch half is real too, and it is the domain
asking for it, not the architecture giving up.

**The layering earns its keep specifically because of late data.** A naive streaming aggregate that
increments a balance as events arrive is wrong for this problem in a way that is hard to fix later:
when a dispute lands three weeks after its charge, the balance for the original day changes. An
append-only running total either silently absorbs the correction into today, which makes any
historical report a lie, or it cannot express the correction at all.

The gold layer here is therefore **recomputed, not incrementally appended**, and Iceberg is chosen
for exactly that: snapshots make "what did the report say before the late event arrived" a
recoverable fact rather than a reconstruction. See section 5.

**The alternative was considered.** A stateful stream processor holding a running balance and
emitting a changelog is a legitimate design, and it wins on latency. It loses on the two things
this project is actually about: reproducing a closed period exactly as it was reported, and
producing an auditable trail of what changed and why. Latency is not the scarce resource in
reconciliation. Defensibility is.

## 3. Data sources

**Schemas come from the real API.** The Stripe CLI runs against a test-mode sandbox:
`stripe listen --forward-to localhost:4242/webhooks` plus `stripe trigger <event>` yields genuine
event envelopes for `charge.succeeded`, `charge.refunded`, `charge.dispute.created`,
`charge.dispute.closed`, `payout.paid`, `payout.failed` and `balance.available`. Those captures are
committed under `fixtures/events/` and are the ground truth for every schema downstream.

**Volume comes from a generator.** `stripe trigger` yields a handful of events, not a stream. The
generator replays the captured shapes across a simulated calendar at configurable volume, with a
chaos module that injects failures on demand. It is the only synthetic part of the system.

**The reconciliation anchor is external.** The processor's own balance transaction list and
`/v1/balance` are what the derived ledger is weighed against, so "correct" is not the pipeline
agreeing with itself.

**The money movement primitive is `balance_transaction`, not `charge`.** Every charge, refund,
dispute and payout carries one, with `amount`, `fee`, `fee_details[]`, `net`, `currency`, `status`
(pending or available) and `available_on`. Building a ledger on the charge object misses fees,
adjustments and the pending-to-available transition.

## 4. Ledger model

Accounts, minimal but complete:

| Account | Meaning |
|---|---|
| `asset:balance_pending` | captured, not yet available |
| `asset:balance_available` | available for payout |
| `asset:bank` | paid out |
| `revenue:gross_sales` | |
| `contra_revenue:refunds` | |
| `expense:processing_fees` | |
| `expense:disputes` | dispute amounts and dispute fees |

Postings, in minor units, signed, derived from the balance transaction:

- charge: `+net` to `balance_pending`, `+fee` to `processing_fees`, `-amount` to `gross_sales`
- available_on: `-net` from `balance_pending`, `+net` to `balance_available`
- refund: `+amount` to `refunds`, `-amount` from `balance_available`
- dispute opened: `+amount` and `+fee` to `disputes`, `-(amount+fee)` from `balance_available`
- dispute won: reverse the amount, keep the fee
- payout: `+amount` to `bank`, `-amount` from `balance_available`

**Invariants, enforced as dbt tests that fail the build:**

1. Postings for one event sum to zero, per currency.
2. Every balance transaction has at least one posting. No money enters without a record.
3. `balance_available` never goes negative without a dispute or adjustment explaining it.
4. A closed day's derived balance equals the processor's reported balance, or the difference is
   itemised in the reconciliation table. An unexplained delta fails the run.

Amounts are integers in the currency's minor unit at every layer. No float touches a monetary
value, and the export schema check enforces it.

## 5. Restatement

`gold.daily_close` records what the report said on the day it closed. When a late event changes a
closed day, the close is kept and a restatement row is written: what the day said then, what it
says now, and which events caused the difference. Iceberg snapshots make the "then" recoverable.

This is the honest answer to whether the gold layer is incremental. It is not. It is recomputed,
and the record of what changed is itself a table.

## 6. The six failures

Each is injected deliberately by the chaos module and each must be caught.

| Failure | Detection |
|---|---|
| Duplicate delivery | Dedup on `event.id`. Bronze count exceeds silver count; the ledger is unchanged |
| Out-of-order arrival | Ordering by event time per entity; a refund cannot post against a charge that has not been seen |
| Late arrival after the close | Restatement row, with the causing events named |
| Dropped event | A gap against the processor's balance transaction list |
| Currency and rounding | Integer minor units end to end; FX and rounding drift accounted for, not absorbed |
| Reversal | Signed postings; a won dispute returns the amount and keeps the fee |

## 7. Roadmap

| Phase | Deliverable | State |
|---|---|---|
| 0 | Scaffold, local stack, webhook capture into fixtures | **in progress** |
| 1 | Kafka producer and Spark streaming into `bronze.events` | not started |
| 2 | Generator with the calendar simulation; silver dedup and typing | not started |
| 3 | dbt ledger models and the four invariants as failing tests | not started |
| 4 | Reconciliation against the processor balance | not started |
| 5 | Chaos suite, one Airflow DAG, one Iceberg namespace per scenario | not started |
| 6 | Export contract and the artifacts a dashboard reads | not started |
| 7 | Write-up | not started |

Services appear in `docker/docker-compose.yml` with the phase that needs them. Kafka, MinIO and the
Iceberg REST catalog are there now; Spark arrives with phase 1 and Airflow with phase 5, pinned
when there is a job to run against them.

## 8. Reporting rules

- Test mode and synthetic volume are stated wherever a figure is reported.
- No throughput or latency number is published unless it came out of a real run, with that run's
  configuration beside it.
- Nothing here processes real payments, real money or real customer data.
