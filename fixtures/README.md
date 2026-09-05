# Fixtures

Real Stripe **test-mode** webhook payloads, captured from a sandbox account with the Stripe CLI.
They are committed so that cloning this repository is enough to run the pipeline: refreshing them
needs an account, using them does not.

**The data in them is mock.** `stripe trigger` invents the charges, the card is 4242, and the
customers do not exist. The envelopes are genuine, the money is not. What actually needs protecting
in this repository is credentials, which never appear in a payload at all; see the Publishing
section of the root README.

```
events/<event.type>/<event.id>.json
```

One file per event id, which makes the set idempotent by construction. A redelivery of an event
rewrites its own file instead of adding a second one.

## What has been changed, and what has not

These are genuine envelopes, with a few edits applied automatically as each file is written (see
`src/payment_ledger/redact.py`). None of them is guarding anything valuable, since the payloads are
mock; they are kept because they cost nothing and they cover the day the CLI is pointed at a real
account by mistake:

| Field | What it becomes | Why |
|---|---|---|
| Any `acct_...` | `acct_00000000000000` | Identifies the sandbox account |
| `receipt_url` | The URL without its path | That path is a token: it opens the receipt for anyone holding it |
| `email`, `receipt_email`, `customer_email`, `phone`, `name`, `line1`, `line2`, `postal_code` | `[redacted]` | Personal fields, and worth nothing to a pipeline that adds up money |

A field that arrived null stays null. Blanking it would invent a value the processor did not send,
and "this field is usually absent" is part of the shape the pipeline has to handle.

**Never touched:** object ids (`evt_`, `ch_`, `txn_`, `py_`, `di_`), amounts, currencies, fees,
`available_on`, `created`, `status` and `request.idempotency_key`. Those are what the pipeline
joins, sums and deduplicates on. A fixture with a redacted id would no longer test anything, and
the idempotency key in particular is a random uuid rather than a credential and is half the
duplicate-delivery story.

No marker is added to a redacted file. A fixture carrying an invented field is no longer a real
envelope, so what was replaced is recorded here rather than inside the payload.

## What can never be here

A payload with `livemode: true` is refused at the point it would become a file, not cleaned up.
Test mode is the premise of the project, so a live payload arriving means the CLI is authenticated
somewhere it should not be, and that is worth stopping over rather than sanitising away.

`make audit` checks the whole tree against the same rules, and the test suite checks whatever is
currently in this directory. Both fail the build on a finding.

## Refreshing them

Needs the Stripe CLI authenticated against a test-mode sandbox:

```bash
make login      # once
make capture    # starts the receiver and `stripe listen`
make trigger    # in a second terminal
make fixtures   # count what landed, by type
```
