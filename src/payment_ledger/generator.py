"""Phase 2: volume, from the shapes phase 0 captured.

The captured fixtures give the pipeline real schemas and 59 events. A ledger is not interesting at
59 events, and a Stripe sandbox will not hand over a hundred thousand of them, so volume comes from
here: every generated event is a **deep copy of a real captured payload** with the fields that
carry money, identity and time overwritten. Nothing is invented from a schema document, because a
payload built from a reading of the docs tests the reading rather than the API.

**Everything this produces is simulated and says so.** Ids carry a `sim` infix (`ch_sim000000001`),
`description` reads `(simulated by payment-ledger generator)`, and `livemode` stays false because
the templates were captured in test mode and nothing here changes it. No figure derived from this
data may ever be reported as production behaviour.

**A run is reproducible.** Seed and transaction count are the whole input: the same pair produces
byte-identical events, balance transactions and daily balances. That is what makes a chaos
scenario in phase 5 a thing you can replay rather than a thing you observed once.

**Three artifacts, and the third is the point.**

1. `events.jsonl`, the webhook stream, which is what a processor pushes at you.
2. `balance_transactions.jsonl`, which is what `/v1/balance_transactions` would return.
3. `daily_balance.jsonl`, the balance the processor reports per day and per currency.

The second exists because of something the captured payloads make plain: **a webhook does not carry
the money.** `charge.updated` carries `balance_transaction` as an *id*, not as an object, so fees,
net and `available_on` are simply not in the event stream. Only the dispute embeds its balance
transactions expanded. A ledger built on webhooks alone therefore cannot compute a fee, and the
pipeline has to join the event stream against the balance transaction list, which is exactly the
join the reconciliation in phase 4 depends on.

The third is the reconciliation anchor, and it is computed **independently of the pipeline** by
walking the same simulated money with different code. That proves the reconciliation detects and
attributes a divergence. It does not prove the ledger agrees with Stripe's books, and no number out
of this project may claim it does.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import random
import sys
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from payment_ledger import config

DAY = 86_400

# The sandbox the fixtures came from is an Irish account, so it settles in euro. Presentment
# currencies are what a customer is charged in.
SETTLEMENT_CURRENCY = "eur"

# Ten-thousandths of a euro per unit of the presentment currency, held fixed for the whole run so
# a rerun reconciles to the cent. The usd rate is not a guess: the captured lost dispute is a
# charge of 100 usd whose balance transaction is -86 eur, which is this number.
RATES = {"eur": 10_000, "usd": 8_600, "gbp": 11_700}
CURRENCY_WEIGHTS = {"eur": 60, "usd": 25, "gbp": 15}

# Stripe's standard European card rate, 1.5% + EUR 0.25, applied to the settled amount.
FEE_BPS = 150
FEE_FIXED = 25

# Straight off the captured payload rather than off the pricing page: a lost dispute on that
# account carries fee 2460, itemised as a EUR 20.00 dispute fee plus EUR 4.60 of Irish VAT.
DISPUTE_FEE = 2000
DISPUTE_VAT = 460

# Also derived rather than assumed. In the captured dispute, `available_on` is midnight UTC of the
# seventh day after the balance transaction was created, which is the rolling delay a new account
# gets.
AVAILABLE_AFTER_DAYS = 7

REFUND_RATE = 0.08
DISPUTE_RATE = 0.04
DISPUTE_LOSS_RATE = 0.6

SIMULATED_DESCRIPTION = "(simulated by payment-ledger generator)"


def half_up(numerator: int, denominator: int) -> int:
    """Integer division rounding halves away from zero. No float touches a monetary value."""
    if numerator < 0:
        return -((-numerator * 2 + denominator) // (denominator * 2))
    return (numerator * 2 + denominator) // (denominator * 2)


def settle(amount: int, currency: str) -> int:
    """A presentment amount in its own minor units, into settlement minor units."""
    return half_up(amount * RATES[currency], 10_000)


def processing_fee(settled: int) -> int:
    return half_up(settled * FEE_BPS, 10_000) + FEE_FIXED


def available_on(created: int) -> int:
    """Midnight UTC of the seventh day after, which is what the captured payload does."""
    when = datetime.fromtimestamp(created, UTC) + timedelta(days=AVAILABLE_AFTER_DAYS)
    return int(datetime.combine(when.date(), time.min, UTC).timestamp())


@dataclass(frozen=True)
class Run:
    """Everything that decides what a run produces. Two equal Runs produce equal output."""

    transactions: int = 1000
    seed: int = 1
    days: int = 30
    start: date = date(2026, 1, 1)

    @property
    def start_ts(self) -> int:
        return int(datetime.combine(self.start, time.min, UTC).timestamp())


class Templates:
    """The captured payloads, loaded once and copied per event.

    One template per type is enough: the parts that differ between two real charges are exactly
    the parts this module overwrites.
    """

    def __init__(self, root: Path):
        self.root = root
        self._cache: dict[str, dict] = {}

    def get(self, event_type: str) -> dict:
        if event_type not in self._cache:
            files = sorted((self.root / event_type).glob("*.json"))
            if not files:
                raise SystemExit(
                    f"no captured template for {event_type} in {self.root}.\n"
                    "The generator replays captured shapes, so it needs the fixtures that phase 0"
                    " committed."
                )
            self._cache[event_type] = json.loads(files[0].read_text(encoding="utf-8"))
        return copy.deepcopy(self._cache[event_type])


def sim_id(prefix: str, n: int) -> str:
    """A Stripe-shaped id that could never be mistaken for one."""
    return f"{prefix}_sim{n:012d}"


class Simulation:
    """One run: the events, the balance transactions, and the balance reported per day."""

    def __init__(self, run: Run, templates: Templates):
        self.run = run
        self.templates = templates
        self.rng = random.Random(run.seed)
        self.events: list[dict] = []
        self.balance_transactions: list[dict] = []
        self._counter = 0

    # -- helpers ---------------------------------------------------------------------------

    def _next(self) -> int:
        self._counter += 1
        return self._counter

    def _amount(self) -> int:
        """Skewed small, the way real card volume is, and never a round number of currency."""
        bucket = self.rng.random()
        if bucket < 0.55:
            return self.rng.randrange(199, 5_000)
        if bucket < 0.9:
            return self.rng.randrange(5_000, 25_000)
        return self.rng.randrange(25_000, 400_000)

    def _currency(self) -> str:
        return self.rng.choices(list(CURRENCY_WEIGHTS), weights=list(CURRENCY_WEIGHTS.values()))[0]

    def _emit(self, event_type: str, created: int, patch, *, previous: dict | None = None) -> None:
        """Copy the captured template of this type, overwrite what identifies it, keep the rest."""
        event = self.templates.get(event_type)
        n = self._next()
        event["id"] = sim_id("evt", n)
        event["created"] = created
        event["type"] = event_type
        event["livemode"] = False
        event["request"] = {
            "id": sim_id("req", n),
            # Real Stripe sets this only for events caused by an API call we made. The generator
            # is the caller here, so it sets one, and phase 5 gets to replay it.
            "idempotency_key": f"sim-{self.run.seed}-{n:012d}",
        }
        if previous is not None:
            event["data"]["previous_attributes"] = previous
        patch(event["data"]["object"])
        self.events.append(event)

    def _balance_transaction(self, **fields) -> dict:
        """The processor's own record of a money movement, in the shape the dispute payload uses."""
        txn = {
            "object": "balance_transaction",
            "currency": SETTLEMENT_CURRENCY,
            "exchange_rate": None,
            "balance_type": "payments",
            "status": "pending",
            **fields,
        }
        txn["available_on"] = available_on(txn["created"])
        self.balance_transactions.append(txn)
        return txn

    # -- the lifecycle ---------------------------------------------------------------------

    def _charge(self, ids: dict, amount: int, currency: str, created: int) -> None:
        settled = settle(amount, currency)
        fee = processing_fee(settled)

        txn = self._balance_transaction(
            id=ids["txn"],
            created=created,
            amount=settled,
            fee=fee,
            net=settled - fee,
            source=ids["charge"],
            type="charge",
            reporting_category="charge",
            description=f"Charge {ids['charge']}",
            fee_details=[
                {
                    "amount": fee,
                    "application": None,
                    "currency": SETTLEMENT_CURRENCY,
                    "description": "Stripe processing fees",
                    "type": "stripe_fee",
                }
            ],
        )

        def patch_intent(obj):
            obj.update(
                id=ids["intent"],
                created=created,
                amount=amount,
                currency=currency,
                description=SIMULATED_DESCRIPTION,
            )

        def patch_charge(obj, balance_txn=None, **extra):
            obj.update(
                id=ids["charge"],
                created=created,
                amount=amount,
                amount_captured=amount,
                currency=currency,
                payment_intent=ids["intent"],
                payment_method=ids["method"],
                balance_transaction=balance_txn,
                description=SIMULATED_DESCRIPTION,
                dispute=None,
                disputed=False,
                **extra,
            )
            card = obj.get("payment_method_details", {}).get("card")
            if isinstance(card, dict):
                card["amount_authorized"] = amount
                if isinstance(card.get("overcapture"), dict):
                    card["overcapture"]["maximum_amount_capturable"] = amount

        self._emit("payment_intent.created", created, patch_intent)
        self._emit("charge.succeeded", created, patch_charge)
        # The balance transaction is attached a moment later, in its own event. That is not a
        # detail: it is why the charge and its fee arrive separately and why the ledger cannot
        # post the fee from `charge.succeeded`.
        self._emit(
            "charge.updated",
            created + 1,
            lambda obj: patch_charge(obj, balance_txn=txn["id"]),
            previous={"balance_transaction": None},
        )
        self._emit("payment_intent.succeeded", created + 1, patch_intent)
        return None

    def _refund(self, ids: dict, amount: int, currency: str, created: int, charged: int) -> None:
        """`created` is when the refund happened; `charged` is when the charge it refunds did.

        Two dates, and collapsing them is a real mistake with a quiet consequence. The charge
        object rides along inside `charge.refunded`, and if its `created` is rewritten to the
        refund's, the charge moves to the day it was refunded: it leaves the day it was actually
        taken, changes what that day's report says, and does it without anything looking wrong.
        """
        settled = settle(amount, currency)
        self._balance_transaction(
            id=ids["refund_txn"],
            created=created,
            amount=-settled,
            # Stripe keeps the processing fee on a refund. The ledger has to show that the money
            # came back and the fee did not.
            fee=0,
            net=-settled,
            source=ids["refund"],
            type="refund",
            reporting_category="refund",
            description=f"REFUND FOR CHARGE ({ids['charge']})",
            fee_details=[],
        )

        def patch_charge(obj):
            obj.update(
                id=ids["charge"],
                created=charged,
                amount=amount,
                amount_captured=amount,
                amount_refunded=amount,
                refunded=True,
                currency=currency,
                payment_intent=ids["intent"],
                balance_transaction=ids["txn"],
                description=SIMULATED_DESCRIPTION,
            )

        def patch_refund(obj):
            obj.update(
                id=ids["refund"],
                created=created,
                amount=amount,
                currency=currency,
                charge=ids["charge"],
                payment_intent=ids["intent"],
                balance_transaction=ids["refund_txn"],
                status="succeeded",
            )

        self._emit(
            "charge.refunded",
            created,
            patch_charge,
            previous={"amount_refunded": 0, "refunded": False},
        )
        self._emit("refund.created", created, patch_refund)
        self._emit("charge.refund.updated", created, patch_refund)
        self._emit("refund.updated", created, patch_refund)

    def _dispute(self, ids: dict, amount: int, currency: str, opened: int, lost: bool) -> None:
        settled = settle(amount, currency)
        fee = DISPUTE_FEE + DISPUTE_VAT
        closed = opened + self.rng.randrange(3, 11) * DAY

        withdrawal = self._balance_transaction(
            id=ids["dispute_txn"],
            created=opened,
            amount=-settled,
            fee=fee,
            net=-(settled + fee),
            source=ids["dispute"],
            type="adjustment",
            reporting_category="dispute",
            description=f"Chargeback withdrawal for {ids['charge']}",
            fee_details=[
                {
                    "amount": DISPUTE_FEE,
                    "application": None,
                    "currency": SETTLEMENT_CURRENCY,
                    "description": "Dispute fee",
                    "type": "stripe_fee",
                },
                {
                    "amount": DISPUTE_VAT,
                    "application": None,
                    "currency": SETTLEMENT_CURRENCY,
                    "description": "VAT",
                    "type": "tax",
                },
            ],
        )

        reinstated = None
        if not lost:
            # Winning returns the amount and keeps the fee, which is the reversal the ledger has
            # to survive: a balance that only ever grows cannot express this.
            reinstated = self._balance_transaction(
                id=ids["dispute_reversal_txn"],
                created=closed,
                amount=settled,
                fee=0,
                net=settled,
                source=ids["dispute"],
                type="adjustment",
                reporting_category="dispute_reversal",
                description=f"Chargeback reversal for {ids['charge']}",
                fee_details=[],
            )

        def patch_dispute(obj, *, status, transactions):
            obj.update(
                id=ids["dispute"],
                created=opened,
                amount=amount,
                currency=currency,
                charge=ids["charge"],
                payment_intent=ids["intent"],
                status=status,
                balance_transaction=ids["dispute_txn"],
                balance_transactions=transactions,
                is_charge_refundable=False,
            )

        opened_txns = [_public(withdrawal)]
        closed_txns = opened_txns + ([_public(reinstated)] if reinstated else [])

        self._emit(
            "charge.dispute.created",
            opened,
            lambda o: patch_dispute(o, status="needs_response", transactions=[]),
        )
        self._emit(
            "charge.dispute.funds_withdrawn",
            opened,
            lambda o: patch_dispute(o, status="needs_response", transactions=opened_txns),
        )
        self._emit(
            "charge.dispute.updated",
            opened + DAY,
            lambda o: patch_dispute(o, status="under_review", transactions=opened_txns),
        )
        self._emit(
            "charge.dispute.closed",
            closed,
            lambda o: patch_dispute(o, status="lost" if lost else "won", transactions=closed_txns),
        )

    # -- the calendar ----------------------------------------------------------------------

    def run_simulation(self) -> Simulation:
        for _ in range(self.run.transactions):
            n = self._next()
            ids = {
                "intent": sim_id("pi", n),
                "charge": sim_id("ch", n),
                "method": sim_id("pm", n),
                "txn": sim_id("txn", n),
                "refund": sim_id("re", n),
                "refund_txn": sim_id("txn", n + 10**6),
                "dispute": sim_id("du", n),
                "dispute_txn": sim_id("txn", n + 2 * 10**6),
                "dispute_reversal_txn": sim_id("txn", n + 3 * 10**6),
            }
            amount = self._amount()
            currency = self._currency()
            created = (
                self.run.start_ts
                + self.rng.randrange(self.run.days) * DAY
                + self.rng.randrange(DAY)
            )

            self._charge(ids, amount, currency, created)

            roll = self.rng.random()
            if roll < REFUND_RATE:
                self._refund(
                    ids,
                    amount,
                    currency,
                    created + self.rng.randrange(1, 6) * DAY,
                    charged=created,
                )
            elif roll < REFUND_RATE + DISPUTE_RATE:
                # Weeks later on purpose. A dispute landing long after its charge is the late
                # arrival the whole restatement design exists for, and it costs nothing to make
                # it show up here rather than waiting for the chaos suite to inject it.
                self._dispute(
                    ids,
                    amount,
                    currency,
                    created + self.rng.randrange(5, 26) * DAY,
                    lost=self.rng.random() < DISPUTE_LOSS_RATE,
                )

        self._emit_daily_balance()
        # Delivered in arrival order, which is not event order: a dispute opened three weeks after
        # its charge arrives three weeks later, and the pipeline has to cope with that.
        self.events.sort(key=lambda e: (e["created"], e["id"]))
        self.balance_transactions.sort(key=lambda t: (t["created"], t["id"]))
        return self

    def daily_balance(self) -> list[dict]:
        """The balance the processor reports, walked independently of the event stream.

        This is the reconciliation anchor. It is deliberately computed from the balance
        transactions rather than from the events, because if both sides came from the same walk
        the comparison in phase 4 would prove nothing at all.
        """
        return walk_daily_balance(self.balance_transactions)

    def _emit_daily_balance(self) -> None:
        """One `balance.available` event per day, carrying what the processor would report."""
        for row in self.daily_balance():
            when = int(datetime.combine(date.fromisoformat(row["date"]), time.min, UTC).timestamp())

            def patch(obj, row=row):
                obj.update(
                    available=[
                        {
                            "amount": row["available"],
                            "currency": row["currency"],
                            "source_types": {"card": row["available"]},
                        }
                    ],
                    pending=[
                        {
                            "amount": row["pending"],
                            "currency": row["currency"],
                            "source_types": {"card": row["pending"]},
                        }
                    ],
                )

            self._emit("balance.available", when, patch)


def walk_daily_balance(transactions: list[dict]) -> list[dict]:
    """What the processor would report, per day and per currency, from a set of movements.

    A module level function rather than a method because phase 5 needs to walk a *subset*: a
    scenario that holds an event back until after the day closed has to say what the processor
    reported on the day it closed, which is the balance without the transaction that had not
    happened yet. Same walk, fewer movements.
    """
    pending: dict[tuple[int, str], int] = defaultdict(int)
    available: dict[tuple[int, str], int] = defaultdict(int)

    for txn in transactions:
        pending[(txn["created"] // DAY, txn["currency"])] += txn["net"]
        available[(txn["available_on"] // DAY, txn["currency"])] += txn["net"]

    touched = list(pending) + list(available)
    if not touched:
        return []
    # Every day in the range, not only the ones with movement. A day the processor reports a
    # balance on and the ledger has no row for is a gap in the reconciliation, not a day off.
    days = range(min(day for day, _ in touched), max(day for day, _ in touched) + 1)
    currencies = sorted({cur for _, cur in touched})

    rows: list[dict] = []
    running_pending: dict[str, int] = defaultdict(int)
    running_available: dict[str, int] = defaultdict(int)
    for day in days:
        for currency in currencies:
            # Money is pending from the day it is created until the day it becomes available,
            # so a day's pending balance is everything created up to it minus everything that
            # has already matured.
            running_pending[currency] += pending[(day, currency)] - available[(day, currency)]
            running_available[currency] += available[(day, currency)]
            rows.append(
                {
                    # Explicitly UTC. `date.fromtimestamp` reads the machine's timezone, which
                    # would date every row of the anchor a day earlier for anyone west of
                    # Greenwich while the ledger side stays in UTC, and the reconciliation would
                    # fail on their laptop and nowhere else.
                    "date": datetime.fromtimestamp(day * DAY, UTC).date().isoformat(),
                    "currency": currency,
                    "pending": running_pending[currency],
                    "available": running_available[currency],
                }
            )
    return rows


def _public(txn: dict | None) -> dict | None:
    """A balance transaction as it appears embedded in a dispute payload."""
    return None if txn is None else copy.deepcopy(txn)


def simulate(run: Run, templates: Templates | None = None) -> Simulation:
    root = Path(config.setting("FIXTURES_DIR", "fixtures/events") or "fixtures/events")
    if not root.is_absolute():
        root = config.REPO_ROOT / root
    return Simulation(run, templates or Templates(root)).run_simulation()


def digest(path: Path) -> str:
    """A fingerprint of one artifact, so two runs can be compared without diffing them."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_jsonl(path: Path, rows: Iterator[dict] | list[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
            count += 1
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate simulated volume from captured shapes.")
    parser.add_argument("-n", "--transactions", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--days", type=int, default=30, help="calendar the run is spread over")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2026, 1, 1))
    parser.add_argument(
        "--out",
        type=Path,
        default=config.REPO_ROOT / "data" / "generated",
        help="directory for the three artifacts (default: %(default)s)",
    )
    args = parser.parse_args(argv)

    run = Run(transactions=args.transactions, seed=args.seed, days=args.days, start=args.start)
    sim = simulate(run)

    events = write_jsonl(args.out / "events.jsonl", sim.events)
    txns = write_jsonl(args.out / "balance_transactions.jsonl", sim.balance_transactions)
    balance = write_jsonl(args.out / "daily_balance.jsonl", sim.daily_balance())

    # The manifest is what makes "reproducible" checkable instead of claimed. Rerun with the same
    # seed and count and the three digests come back identical.
    manifest = {
        "simulated": True,
        "transactions": run.transactions,
        "seed": run.seed,
        "days": run.days,
        "start": run.start.isoformat(),
        "counts": {"events": events, "balance_transactions": txns, "daily_balance": balance},
        "sha256": {
            name: digest(args.out / f"{name}.jsonl")
            for name in ("events", "balance_transactions", "daily_balance")
        },
    }
    (args.out / "run.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    print(
        f"{run.transactions} transactions, seed {run.seed}, "
        f"{run.days} days from {run.start.isoformat()}"
    )
    print(f"  {events:>7} events              -> {args.out / 'events.jsonl'}")
    print(f"  {txns:>7} balance transactions -> {args.out / 'balance_transactions.jsonl'}")
    print(f"  {balance:>7} daily balance rows   -> {args.out / 'daily_balance.jsonl'}")
    print(f"\nrun fingerprint  events {manifest['sha256']['events'][:16]}")
    print("simulated data. every id carries a sim infix and no figure from it is a real one.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
