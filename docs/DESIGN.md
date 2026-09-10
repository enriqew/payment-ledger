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

**Bronze keeps every delivery, and that is what makes the first failure detectable.** Nothing is
deduplicated on the way in. The gap between the row count in bronze and the distinct event count
in silver is not untidiness to be cleaned up later, it is the measurement: a bronze layer that
collapsed a redelivery would be destroying the evidence it exists to hold. The payload is stored
as the exact string that arrived, for the same reason. Reparse and rewrite it and every schema
decision downstream is being made about our own JSON writer rather than about the processor's.

**Bronze is partitioned by arrival, not by event time.** An event that lands three weeks late
belongs to the day it arrived, because the question bronze answers is what was delivered and when.
Placing that dispute against the charge it contests is silver's problem, and it is a different
problem: the arrival record must not pretend to know an ordering the transport never promised.

**Deduplication is a MERGE on the key, not a watermarked `dropDuplicates`.** The streaming
operator is the obvious tool and it is the wrong one, because a watermark forgets. Set it to an
hour and a redelivery ninety minutes late passes through as a second event, and the ledger doubles
a charge for a reason nobody will find looking at the ledger. Stripe retries a failed webhook for
up to three days, so the watermark that would actually be safe is three days of state carried in a
streaming shuffle. Merging against the key the table already holds is bounded by the table instead
of by a guess about how late a retry can be, and it is correct at any lateness. Silver keeps the
count of collapsed deliveries and both timestamps, so the evidence bronze holds is summarised
rather than thrown away.

**The entity projection is recomputed, not maintained.** `silver.events` is a log; the ledger wants
one row per charge. A streaming "latest per key" has to hold state for every key it has ever seen,
because the event that corrects one can arrive at any time and a dispute arrives weeks later by
design. Recomputing the projection over a deduplicated log is bounded by the log, and the log is
already the thing this project promises to be able to rebuild a closed period from. It is the same
argument the gold layer makes, one layer earlier.

**The topic is keyed by the charge, not by the event.** A dispute goes to the partition of the
charge it disputes, so the whole life of one charge (succeeded, refunded, disputed, closed) lands
in one partition and arrives in order. Nothing downstream is permitted to depend on that. Webhooks
are unordered by contract and silver sorts by event time per entity regardless. It is worth doing
anyway because it puts a duplicate next to its original, which is the cheapest place to catch
one.

## 3. Data sources

**Schemas come from the real API.** The Stripe CLI runs against a test-mode sandbox:
`stripe listen --forward-to localhost:4242/webhooks` plus `stripe trigger <event>` yields genuine
event envelopes for `charge.succeeded`, `charge.refunded`, `charge.dispute.created`,
`charge.dispute.closed` and `balance.available`. Triggering one of those produces the whole
lifecycle around it, so the captured set is wider than the list: a triggered charge also delivers
its `payment_intent.created`, `payment_intent.succeeded` and `charge.updated`. Those captures are
committed under `fixtures/events/`, redacted as section 9 describes, and are the ground truth for
every schema downstream. They are committed precisely so that cloning the repository is enough:
the capture refreshes them and needs an account, running the pipeline does not.

**A triggered dispute is an inquiry, not a chargeback.** `stripe trigger charge.dispute.created`
produces a dispute whose statuses are all prefixed `warning_`, and closing one moves no money:
`balance_transactions` comes back empty. Building the reversal accounting on those payloads would
have meant never seeing the reversal at all. Escalating the inquiry turns it into a real
chargeback, and settling that is what delivers `charge.dispute.funds_withdrawn` and a balance
transaction carrying the amount. `make dispute` drives that lifecycle end to end.

**The event currency is not the settlement currency.** The captured lost dispute is a charge of
`100 usd` whose balance transaction is `-86 eur`, because the sandbox settles in EUR. Nothing
downstream may take the amount off the charge and treat it as money that moved. The amount that
moved is on the balance transaction, in the currency the balance is denominated in, which is the
reason section 4 makes `balance_transaction` the primitive rather than `charge`.

**The payout leg is not captured, and that is a gap worth naming.** `payout.paid` and
`payout.failed` have no trigger fixture: they follow a real payout reaching a terminal state.
Even `payout.created` and `payout.updated`, which do have fixtures, need an external bank account
on the sandbox, and a fresh sandbox has none. So the payout shapes below are read off the API
reference rather than off a payload this repository has seen. Section 5 depends on payouts to close
the cash side of the ledger, which means that side is designed and not yet evidenced. Attaching a
test bank account to the sandbox closes it, and until it is closed nothing here should be read as
if it were.

**A webhook does not carry the money.** The captured payloads settle this and it changes what the
pipeline has to be. `charge.updated` carries `balance_transaction` as an **id**, not as an object,
so the fee, the net and `available_on` are nowhere in the event stream. Only a dispute embeds its
balance transactions expanded, which is how the fee of 2460 on a lost one is visible at all. A
ledger built on the webhook stream alone therefore cannot compute a fee, no matter how carefully it
reads the events: it has to join them against the balance transaction list. That join is not an
optimisation, it is the reason the reconciliation in section 4 has two sides.

**Volume comes from a generator.** `stripe trigger` yields a handful of events, not a stream. The
generator replays the captured shapes across a simulated calendar at configurable volume, with a
chaos module that injects failures on demand. It is the only synthetic part of the system.

Every generated event is a **deep copy of a captured payload** with the fields carrying money,
identity and time overwritten. Nothing is constructed from a reading of the API reference, because
a payload written out of the documentation tests the reading rather than the API. The constants
follow the same rule and are read off real payloads instead of off the pricing page: the usd
settlement rate is the one that turns the captured 100 usd charge into its 86 eur balance
transaction, the dispute fee is the 2000 plus 460 of VAT that the captured lost dispute itemises,
and `available_on` is midnight UTC of the seventh day after, because that is what the captured
payload does.

**A run is reproducible and everything in it says it is simulated.** Transaction count, seed and
calendar length are the whole input, and `run.json` records them beside a sha256 of each artifact,
so the same three produce the same digests. That is what makes a chaos scenario in phase 5 a thing
to replay rather than a thing that was observed once. Generated ids carry a `sim` infix,
`description` reads `(simulated by payment-ledger generator)` and `livemode` stays false, so no
artifact of a run can be mistaken for a captured payload or reported as production behaviour.

**The reconciliation anchor is independent, not external.** This is the one place where the
honest description is weaker than the appealing one, so it is worth being exact about.

The live `/v1/balance` cannot be the anchor for a synthetic run. Once volume comes from a
generator, the processor's real balance reflects a handful of triggered test events and nothing
the generator produced, so comparing the two would fail for a reason that has nothing to do with
the ledger. It also cannot be the anchor for a repository anyone can clone, because a stranger
running this has no account of their own.

So the generator emits two artifacts and keeps them apart: the event stream, and the processor's
reported balance, computed by its own accounting from the same simulated calendar without
consulting the pipeline. `gold.reconciliation` weighs the ledger against that, day by day and
currency by currency, and grants **no tolerance**: both sides are integers in minor units, so
there is nothing to round and a tolerance is only somewhere for a real discrepancy to live.

`gold.reconciliation_items` is the itemisation the fourth invariant refers to. It keys on the day
the difference **moved**, not on the days it is wrong: a balance difference is cumulative, so one
bad transaction on the third makes every day after it wrong by the same amount, and listing the
transactions of every wrong day means listing the whole run. It is empty on a healthy run, which is
the statement that every day agreed rather than a table nobody finished. It is also deliberately
not built from `reconciliation`: dbt skips everything downstream of a failed test, so hanging the
itemisation off the table the invariant guards would take the detail away at exactly the moment
somebody needs to read it.

The chaos module perturbs one side and not the other, which is what makes a divergence appear at
all. Section 6 has what each scenario damages and what the run is required to do about it.

What this design proves: that the reconciliation detects a divergence, attributes it to specific
events, and refuses to close a day it cannot explain. What it does not prove: that the ledger
agrees with Stripe's own books. No published figure may claim otherwise.

The bridge back to something genuinely external is a separate, optional step for whoever does have
an account: reconcile the small set of real captured events against the real `/v1/balance` and
`/v1/balance_transactions`. It runs on demand, never in CI, and its result is reported as what it
is, a check over a handful of events rather than over the generated volume.

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

Postings are in minor units, signed, and derived from the balance transaction and nothing else.
Every balance transaction produces exactly **two entries**, and an entry is a set of postings that
sums to zero per currency.

**Recognition**, at `created`. `+net` to `balance_pending`, plus whatever balances it:

| Category | The counterpart |
|---|---|
| charge | `+fee` to `processing_fees` and `-amount` to `gross_sales`, which balances because `net = amount - fee` |
| refund | `-net` to `contra_revenue:refunds` |
| dispute | `-net` to `expense:disputes`, which is the amount and the dispute fee together |
| dispute reversal | `-net` to `expense:disputes` again; the reversal carries no fee, so the amount comes back and the fee stays |
| payout | `-net` to `asset:bank` |
| anything else | `expense:unclassified`, which is not in the accepted chart of accounts, so an unmodelled category fails the build rather than going missing |

**Availability**, at `available_on`. `-net` from `balance_pending`, `+net` to `balance_available`.

**This corrects what this section said before the payloads were captured.** It used to post a
refund and a dispute straight against the available balance, as if only charges passed through
pending. Every balance transaction carries an `available_on`, refunds and disputes included, so
posting them against available on the day they are created reports cash as withdrawn a week before
it actually is. Making maturation a uniform second entry is also what turns `balance_pending` into
a real account rather than a caption on a report.

**Invariants, enforced as dbt tests that fail the build:**

1. Postings for one entry sum to zero, per currency.
2. Every balance transaction has at least one posting. No money enters without a record.
3. `balance_available` never goes negative without a dispute or a refund explaining it, counted
   cumulatively: a dispute on the third still explains a negative balance on the fifth.
4. A closed day's derived balance equals the processor's reported balance, or the difference is
   itemised in the reconciliation table. An unexplained delta fails the run.

All four are singular tests in `dbt/tests/`, which dbt can only pass or fail, and a failure stops
every model downstream rather than publishing a gold table nobody should read.

**The fourth is the only one that can see outside the books, and that is the whole point of it.**
The first three prove the ledger is internally consistent, and internally consistent is not the
same as right: post a transaction the processor never saw, and both of its entries balance, the
trial balance still sums to zero, and the ledger is confidently wrong by exactly that transaction.
Nothing inside the system notices. Only a comparison against something the pipeline did not
produce does, which is why the anchor being computed independently is not a detail.

Amounts are integers in the currency's minor unit at every layer. No float touches a monetary
value, no model in gold contains a division, and there is a test for both.

## 5. Restatement

`gold.daily_close` records what the report said on the day it closed. When a late event changes a
closed day, the close is kept and a restatement row is written: what the day said then, what it
says now, and which movements caused the difference.

This is the honest answer to whether the gold layer is incremental. It is not. It is recomputed,
and the record of what changed is itself a table.

**Built in phase 5, and one thing about it changed on contact.** The plan said Iceberg snapshots
would make the "then" recoverable, which is true and is not usable from a model: time travel needs
a snapshot id as a literal, so "the close before this one" is not something SQL can ask for.
`gold.daily_close_log` is an appended table instead, one full close per run, which anybody can
query without knowing which snapshot to name. It is the only table in gold that is appended rather
than recomputed, and it has no incremental filter on purpose: a run that changed nothing still
writes its close, because "this day was looked at again and did not move" is a different statement
from "nobody looked".

`gold.restatements` compares the last two closes. Every day the late arrival touched is a row, not
only the day it landed on, because money that appears on the third is pending from the third and
available from the tenth and changes the closing balance of every day after it. What is kept short
is the attribution: each row names the movements that started or matured on **that** day, so the
table shows where the movement entered instead of repeating the same ids down the column.

**The cause is attributed by arrival, not by amount.** `silver.balance_transaction_list` stamps
`loaded_at` when a movement first reaches the pipeline and keeps it across refetches, so what
counts as late is a fact rather than a guess. Matching a difference to the transaction whose net
happens to equal it looks convincing and picks the wrong one as soon as two are the same size.

A restatement is not a failure and does not fail the build. What fails the build is a day that
moved by something other than what arrived late (`assert_every_restatement_is_explained`), because
that is the ledger rewriting a published figure for a reason nothing accounts for.

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

Until phase 5 that table was six claims. Each row is now a scenario in `src/payment_ledger/chaos.py`
and a test keeps the two lists equal, so a row nobody wired up fails the build.

**One failure per run, which is why the suite is seven runs and not one.** Six failures plus the
undamaged control, each taken through the whole pipeline on its own. Injecting them together would
be one run and would answer nothing, for two separate reasons.

The first is attribution. Put a duplicated event and a dropped one into the same run, watch three
tests go red, and there is no way to say which injection produced which failure. That matters
because the suite does not check that something went wrong. It checks that exactly what the
scenario declared went wrong and that nothing else did, and neither half of that sentence can be
evaluated on a run carrying more than one cause.

The second is the control. Every arm compares its counts against the baseline's, so the baseline
has to be a run in which nothing was injected. An arm that shared a run with an injection would be
measured against a yardstick that had itself been bent.

The cost follows from this and is worth stating plainly, because it is the only reason the suite
and the ledger are ever run at different sizes: six failures cost seven complete pipelines, not
one. The suite therefore runs at whatever size is affordable seven times over, the export records
the suite's run and the ledger's separately, and a page reading both is expected to say so when
they differ.

**A scenario perturbs one side and not the other.** The generator emits three artifacts: the
webhook stream, the balance transaction list, and the balance the processor reports. A scenario
damages one and leaves the rest alone, which is what makes a divergence appear where a real one
would. Damaging all three consistently produces a run that is wrong and reconciles, which is
exactly the failure nobody catches.

**The expectation is written before the run and checked both ways.** Every scenario declares which
dbt tests must fail and, where the injection determines it, on exactly how many rows; plus what has
to be true of the row counts at every layer. The verdict fails when a detection did not fire *and*
when a test fired that no scenario asked for. A suite that only checks "something went wrong" says
nothing about whether the right thing went wrong.

**A namespace per arm, and nothing dropped between them.** Each scenario runs into
`<scenario>_bronze`, `<scenario>_silver` and `<scenario>_gold`, with a topic and a checkpoint of its
own, so when the suite finishes the damaged run and the run it was measured against are both in the
catalog and a difference of nine rows is a query rather than a claim. The one thing that would make
the whole exercise worthless is an arm reading another arm's tables, so that is a test rather than
a habit.

**Two of the six are not caught by an invariant, and that is the finding.** A dropped webhook does
not move a cent: the ledger posts from the balance transaction list, so every entry balances and
the day reconciles exactly. What is missing is the business's record of what the money was for, and
only a comparison between the two inputs can see it. That comparison is `gold.coverage_gaps`, which
also catches the opposite direction: a movement the stream carried expanded inside a dispute
payload that the list does not have. And a late arrival is not a defect at all. It is the normal
condition of a payment processor, and what it must produce is a restatement rather than a failure.

## 7. This repository is public

Decided before the first payload was captured, which is the only cheap moment to decide it.

**What is actually at risk is credentials, not data.** Worth stating plainly, because the two get
conflated. The payloads here are mock: `stripe trigger` invents the charges, the card is 4242, the
customers do not exist. Nothing in a fixture is worth stealing. What would matter is a secret key
or a webhook signing secret reaching a public history, and that is undone by rotating the
credential, not by deleting the commit. So the effort goes there.

**No credential can enter the tree.** `scripts/audit_publishable.py` checks every tracked file for
a key or a signing secret in any mode, with no exception for the files whose job is to hold fake
values: a rule with a carve-out for the tests is a rule that stops catching the real thing. It also
refuses a set of paths outright (`.env`, `*.pem`, `*.key`, `credentials`, `service-account*.json`,
`dbt/profiles.yml`, `airflow.cfg`) checked against what git tracks rather than against
`.gitignore`, which is a default and not a guarantee: `git add -f` ignores it, and so does a file
committed before its rule existed. Most of those paths belong to phases 3 and 5, which is
deliberate. The moment to refuse a credentials file is before one exists.

Phase 3 is where that stopped being theoretical. dbt requires a file called `profiles.yml` and the
audit rejects that name anywhere, not only at `dbt/`. Both available shortcuts were the thing the
rule exists to prevent: putting the file where the pattern does not look is evasion, and adding an
exception for the one place it would be harmless is how a rule stops catching the real thing. The
profile is rendered at container start from the environment instead, so nothing by that name is
ever tracked, and what it contains is a host and a port rather than a secret.

**No credential can be printed.** The signing secret is resolved from the CLI into memory and
written nowhere. Every error path that echoes CLI output goes through `mask()` first, because
`stripe listen --print-secret` is a command whose entire purpose is printing a credential, and the
listener's own stdout goes to devnull. Neither the audit nor the test suite reports the text it
matched. A secret in a CI log is in a worse place than the file it was found in.

**Three enforcement points, in order of usefulness.** The pre-commit hook (`make hooks`) refuses
the commit; CI refuses the push; `make audit` answers on demand. CI is the weakest of the three
because it runs after the push has already happened.

**It has to run with no Stripe account.** That is what makes a public repository worth opening.
The committed fixtures and the generator are the default input; the live capture is an optional
refresh, and no phase may require a network call to Stripe. This is what forces the anchor in
section 3 to be independent rather than external.

**Payload handling, which matters less.** A payload with `livemode: true` is refused at the point
it would become a file rather than cleaned up, since it means the CLI is authenticated somewhere it
should not be and sanitising that away hides it. Test payloads still have the account id, the
receipt URL's token and personal fields replaced on write. That last part guards against little
given the data is mock, and it is kept because it costs nothing and covers the day the CLI is
pointed at a real account by mistake. Ids, amounts, currencies, fees, timestamps and
`request.idempotency_key` are never touched: they are what the pipeline joins, sums and
deduplicates on, and a fixture with a redacted id tests nothing. Null stays null and no marker key
is added, so the envelopes stay genuine.

**Not affiliated with Stripe.** The repository uses a public API in test mode and says so in the
first paragraph of the README. Nothing here may read as official, endorsed or connected.

**Every figure has a run behind it.** Repeated from section 9 because a public repository is where
an unverified number does real damage. No throughput and no latency figure is published unless it
came out of a measured run, with that run's configuration beside it.

## 8. Roadmap

| Phase | Deliverable | State |
|---|---|---|
| 0 | Scaffold, local stack, webhook capture into fixtures | done |
| 1 | Kafka producer and Spark streaming into `bronze.events` | done |
| 2 | Generator with the calendar simulation; silver dedup and typing | done |
| 3 | dbt ledger models and the invariants as failing tests | done |
| 4 | Reconciliation against the processor balance | done |
| 5 | Chaos suite, one Airflow DAG, one Iceberg namespace per scenario | done |
| 6 | Export contract and the artifacts a dashboard reads | done |
| 7 | Write-up | **next** |

The export contract is its own document, [`EXPORT.md`](EXPORT.md), because it is the one thing here
read by somebody who is not working on the pipeline. Two properties of it are design decisions
rather than conveniences. It is **bounded by the calendar and not by volume**, so a hundred thousand
transactions produce the same sixty-odd daily rows as a thousand and a static file stays an honest
interface instead of a truncation nobody mentions. And it **refuses rather than warns**: a trial
balance that does not sum to zero, a day the reconciliation cannot explain, a float where money
should be, or a chaos suite carrying some of its arms but not all of them, and nothing is written
at all. A page has no way to find any of that out afterwards.

`gold.money_flow` was added when the page wanted a Sankey diagram of where the money went, and it
is in the warehouse rather than in the page for the reason the contract gives: a figure a browser
computed is a second implementation of the accounting that nothing checks. It is aggregated from
the pairing inside each entry, the one leg the money left from and the legs it went to, rather than
by subtracting one account balance from another. That subtraction happens to give the right answer
on a healthy run and is wrong in general, because it absorbs a won dispute paying money back. The
model rests on every entry having exactly one source leg, which is true of the entries this ledger
makes and is not true of double entry in general, so a test refuses the build if it ever stops
being true rather than letting the model quietly approximate.

Services appear in `docker/docker-compose.yml` with the phase that needs them. Kafka, MinIO, the
Iceberg REST catalog, Spark, a Spark Thrift server, dbt and Airflow are all there now.

Phase 5 said "one namespace per scenario" and built three: `<scenario>_bronze`, `<scenario>_silver`
and `<scenario>_gold`. The layers are namespaces here, and collapsing an arm into one of them would
have meant renaming a table to avoid `bronze.events` colliding with `silver.events`, which is a
worse trade than a prefix.

The other correction phase 5 made to this plan was `DROP NAMESPACE ... CASCADE`, which is the
obvious way to take an arm back to nothing and does not work: Spark passes the cascade to the
catalog, the Iceberg REST catalog ignores it, and the drop comes back as
`NamespaceNotEmptyException`. The reset drops the tables one by one, and it reads which tables
those are out of the `make reset` recipe rather than keeping a second list beside it.

Two more things surfaced the same way, in phase 2, and both are the kind that only appear when a
job runs. Iceberg's streaming source writes its initial offset into the Spark checkpoint using the
**table's own FileIO**, and a checkpoint is a local path, so an `S3FileIO` catalog refuses it
outright and the silver stream dies before its first batch; `ResolvingFileIO` picks per scheme and
handles both. And the catalog caches table metadata for thirty seconds, which is longer than these
jobs take, so a job that writes a table and then reports on it reads the snapshot from before its
own write: silver reported zero rows into a table already holding four thousand six hundred.

Worth recording, since the point of pinning a service before there is a job for it was to avoid
guessing: phase 1 found that three things in that compose file had never actually run. The Iceberg
image was pinned to a tag that was never published, and both init containers passed their scripts
in a shape Compose word-splits before `sh` ever sees them, so the topic and the bucket were being
created by nothing. A service written down in advance is a plan, not a working stack, and the
difference only shows up the first time something is submitted against it.

## 9. Reporting rules

- Test mode and synthetic volume are stated wherever a figure is reported.
- No throughput or latency number is published unless it came out of a real run, with that run's
  configuration beside it.
- Nothing here processes real payments, real money or real customer data.
