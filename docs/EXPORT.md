# The export contract

What leaves the warehouse, what it means, and what it is not allowed to say.

A dashboard reads files. The page this feeds is a static site with no backend, so the interface
between the pipeline and anything that shows it is a handful of JSON files somebody copies. The
copying is not the interesting part. Deciding what those files may contain, and refusing to write
them when the run behind them does not deserve to be read, is.

Produced by `make export`, which is two steps: a Spark job dumps the gold tables as plain rows, and
`payment_ledger.export` assembles the artifacts, checks every promise below, and writes them. If a
check fails, nothing is written at all.

## The files

| File | Rows | What one row is |
|---|---|---|
| `manifest.json` | 1 | the run behind the export, the schema version and a count per file |
| `accounts.json` | one per account and currency | the trial balance, with the signs double entry uses |
| `flow.json` | one per pair of accounts money moved between | where the money went, rather than where it ended up |
| `daily_close.json` | one per day and currency | what the books say that day closed at, and what moved |
| `reconciliation.json` | one per day and currency | the ledger weighed against the balance the processor reports |
| `findings.json` | 0 on a healthy run | coverage gaps and restatements, empty when the two inputs agreed and no published day moved |
| `chaos.json` | one per scenario, or none | the six failures injected on purpose, what fired and whether it was what the scenario said |

The contract is the JSON files at the top of `export/`. `export/tables/` beside them holds the raw
dumps the Spark job wrote, which are an intermediate and not part of it: they have columns the
contract does not name, and nothing outside this repository should read them.

`chaos.json` carries its arms in the order the suite runs them, which is the order the design lists
the failures in rather than the alphabetical order the directories happen to have.

Every file carries `schema_version`, `simulated: true` and a `notice` saying what the data is.
That is repeated in each file rather than kept in the manifest because these get copied one at a
time, and a payload that knows it is simulated only because of a sibling file is one rename away
from being presented as real.

## What the export promises

**The flow is the ledger, not a picture of it.** `flow.json` is aggregated from the pairing inside
each entry: the one leg money left from and the legs it went to. It is not derived by subtracting
one account balance from another, which happens to give the right answer on a run like this one and
is wrong in general, because it absorbs a won dispute paying money back. What flows into an account
minus what flows out of it is what that account holds, which is checked in the warehouse by
`assert_the_flow_conserves` and again here on the way out. A diagram that lost a euro between two
accounts would still draw, and would still look convincing.

**Amounts are integers in the settlement currency's minor unit.** No float appears anywhere in any
artifact, and the validator walks every value in every file to say so. An average, a percentage or
a Decimal handed to a JSON writer would all break it, which is the point: an export is the last
place where integer money can quietly stop being integer.

**The trial balance sums to zero, or nothing is written.** `accounts.json` carries the total rather
than leaving a reader to add it up, because it is the one number on the page that has to be zero.

**No day carries an unexplained difference, or nothing is written.** A page has no way to find out
afterwards that the ledger disagreed with the processor.

**A day is a plain ISO day.** `2026-01-14`, never a timestamp. A time of day makes the reader guess
a timezone, and a close that means a different day on two machines is the bug the pipeline runs in
UTC to avoid.

**The chaos suite is all of it or none of it.** An export taken before the suite has run carries no
arms and says so, which is honest. An export carrying three of seven is the one shape that misleads
by being true: three arms on a page look exactly like the suite. Every arm carried must also have
done what its scenario said it would.

**Only the fields named in the contract.** The dumps have more columns than the export does.
Narrowing here rather than in the job is deliberate: the job decides which tables leave, and this
decides what they are allowed to say, so a page cannot quietly gain a field nobody decided to
publish. A column that disappears upstream is refused rather than exported as nulls.

**Bounded by the calendar, not by volume.** Every artifact is a day, an account or a scenario. A
hundred thousand transactions produce the same sixty-odd daily rows as a thousand, which is what
makes a static file an honest interface instead of a truncation nobody mentions. `ledger_postings`
is deliberately not exported: it is the one table that grows with volume, and a page that wanted
individual postings would be asking for a query engine rather than for a file.

**Byte identical for the same run.** There is no timestamp anywhere in the export. What identifies
it is the fingerprint of the generated run it was built from, recorded in the manifest beside the
seed and the transaction count. Two exports of the same run are the same bytes, so an export whose
numbers moved is an export whose input moved, and that is visible in a diff.

## Consuming it

Copy the files into the page's own repository and read them there. Nothing is fetched at runtime
and there is no service to call.

Check `schema_version` and fail loudly on a value you do not know. The version changes when a field
is removed or its meaning changes; adding a file or a field does not change it.

Do not recompute money from these files. Every figure is already the ledger's own, and a page that
sums, averages or converts is a second implementation of the accounting that nothing checks. If a
figure is wanted that is not here, it belongs in a gold model with a test behind it.

`revenue:gross_sales` is negative because revenue is a credit. Turning it positive to make a chart
friendlier is exactly how a ledger stops summing to zero.

## What it is not

Not real money. Event schemas come from a payment processor's test mode, captured against a sandbox
account; the volume is simulated by a generator. The reconciliation anchor is computed by walking
the same simulated money with separate code, which proves the reconciliation detects and attributes
a divergence. It does not prove the ledger agrees with a real processor's books, and no figure from
this export may be reported as if it did.
