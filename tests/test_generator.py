"""What the generator promises: reproducibility, integer money, and nothing that looks real.

The arithmetic tests check against the **captured** payloads rather than against a pricing page.
Every constant in the generator that could have been guessed at was instead read off a real test
mode payload, and these are the assertions that keep it that way.
"""

from __future__ import annotations

import json

import pytest

from payment_ledger import generator as g
from payment_ledger.redact import FORBIDDEN_ANYWHERE, FORBIDDEN_IN_PAYLOADS

SMALL = g.Run(transactions=40, seed=1, days=14)


@pytest.fixture(scope="module")
def sim():
    return g.simulate(SMALL)


def walk(value):
    """Every scalar in a nested payload."""
    if isinstance(value, dict):
        for item in value.values():
            yield from walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from walk(item)
    else:
        yield value


# --- reproducibility ----------------------------------------------------------------------------


def test_the_same_run_produces_the_same_bytes():
    """Seed and transaction count are the whole input. Without this a chaos scenario in phase 5 is
    something that was observed once rather than something that can be replayed."""
    first, second = g.simulate(SMALL), g.simulate(SMALL)

    assert first.events == second.events
    assert first.balance_transactions == second.balance_transactions
    assert first.daily_balance() == second.daily_balance()


def test_a_different_seed_produces_a_different_run():
    other = g.simulate(g.Run(transactions=40, seed=2, days=14))
    assert other.events != g.simulate(SMALL).events


def test_the_run_scales_to_whatever_it_is_asked_for():
    for count in (1, 7, 250):
        run = g.simulate(g.Run(transactions=count, seed=3, days=5))
        charges = [e for e in run.events if e["type"] == "charge.succeeded"]
        assert len(charges) == count


# --- the arithmetic, checked against the captured payloads ---------------------------------------


def test_settlement_reproduces_the_captured_cross_currency_dispute():
    """The captured lost dispute is a charge of 100 usd whose balance transaction is -86 eur. The
    usd rate exists to reproduce exactly that, so this is the test that stops it drifting."""
    assert g.settle(100, "usd") == 86


def test_availability_reproduces_the_captured_available_on():
    """Read off the captured payload rather than assumed: midnight UTC of the seventh day."""
    assert g.available_on(1788676706) == 1789257600


def test_the_dispute_fee_is_the_one_the_captured_payload_itemises():
    assert g.DISPUTE_FEE + g.DISPUTE_VAT == 2460


def test_rounding_goes_half_up_in_both_directions():
    assert g.half_up(5, 10) == 1
    assert g.half_up(4, 10) == 0
    assert g.half_up(-5, 10) == -1
    assert g.half_up(-4, 10) == 0


def test_no_float_reaches_a_monetary_value(sim):
    """The claim the whole project rests on, checked over the payloads rather than asserted in a
    design document."""
    for event in sim.events:
        assert not [v for v in walk(event) if isinstance(v, float)]
    for txn in sim.balance_transactions:
        assert not [v for v in walk(txn) if isinstance(v, float)]


def test_a_charge_balance_transaction_nets_out(sim):
    charges = [t for t in sim.balance_transactions if t["type"] == "charge"]
    assert charges
    for txn in charges:
        assert txn["net"] == txn["amount"] - txn["fee"]
        assert txn["fee"] == sum(d["amount"] for d in txn["fee_details"])


def test_a_lost_dispute_takes_the_amount_and_the_fee(sim):
    """The reversal case. A won dispute returns the amount and keeps the fee, which is why a
    balance that only grows cannot express this."""
    withdrawals = [t for t in sim.balance_transactions if t["reporting_category"] == "dispute"]
    assert withdrawals
    for txn in withdrawals:
        assert txn["amount"] < 0
        assert txn["fee"] == 2460
        assert txn["net"] == txn["amount"] - txn["fee"]

    reversals = [
        t for t in sim.balance_transactions if t["reporting_category"] == "dispute_reversal"
    ]
    for txn in reversals:
        assert txn["amount"] > 0
        assert txn["fee"] == 0


# --- the reconciliation anchor --------------------------------------------------------------------


def test_the_reported_balance_accounts_for_every_movement(sim):
    """Once the last balance transaction has matured, available is the sum of every net and
    nothing is left pending. A reported balance that did not close out would make the phase 4
    comparison meaningless."""
    rows = sim.daily_balance()
    final = rows[-1]

    assert final["pending"] == 0
    assert final["available"] == sum(t["net"] for t in sim.balance_transactions)


def test_the_reported_balance_has_a_row_for_every_day(sim):
    """A day the processor reports on and the ledger has no row for is a gap in the
    reconciliation, not a day off."""
    from datetime import date, timedelta

    days = [date.fromisoformat(row["date"]) for row in sim.daily_balance()]
    assert days == [days[0] + timedelta(days=i) for i in range(len(days))]


# --- nothing here may look real -------------------------------------------------------------------


def test_every_generated_id_is_visibly_simulated(sim):
    for event in sim.events:
        assert "_sim" in event["id"]
        assert "_sim" in event["request"]["id"]


def test_nothing_generated_claims_to_be_live(sim):
    for event in sim.events:
        assert event["livemode"] is False
        obj = event["data"]["object"]
        assert obj.get("livemode", False) is False


def test_a_generated_charge_says_it_was_simulated(sim):
    charge = next(e for e in sim.events if e["type"] == "charge.succeeded")
    assert charge["data"]["object"]["description"] == g.SIMULATED_DESCRIPTION


def test_generated_payloads_carry_nothing_that_must_not_be_published(sim):
    """The templates were redacted on capture, and copying one must not undo that."""
    rules = {**FORBIDDEN_ANYWHERE, **FORBIDDEN_IN_PAYLOADS}
    body = "\n".join(json.dumps(e, sort_keys=True) for e in sim.events)

    # The match is never printed, for the same reason the audit never prints one.
    assert not [name for name, pattern in rules.items() if pattern.search(body)]


# --- the calendar ---------------------------------------------------------------------------------


def test_events_arrive_in_arrival_order_not_in_charge_order(sim):
    created = [e["created"] for e in sim.events]
    assert created == sorted(created)


def test_a_dispute_lands_well_after_the_charge_it_contests(sim):
    """The late arrival the restatement design exists for, showing up on the calendar rather than
    waiting to be injected by the chaos suite."""
    charges = {
        e["data"]["object"]["id"]: e["created"]
        for e in sim.events
        if e["type"] == "charge.succeeded"
    }
    disputes = [e for e in sim.events if e["type"] == "charge.dispute.created"]
    if not disputes:
        pytest.skip("no dispute fell out of this run")

    for event in disputes:
        opened = event["created"]
        charged = charges[event["data"]["object"]["charge"]]
        assert opened - charged >= 5 * g.DAY


def test_a_charge_keeps_its_own_date_through_a_refund(sim):
    """The charge object rides inside `charge.refunded`. If its `created` were rewritten to the
    refund's, the charge would move to the day it was refunded, leave the day it was taken, and
    change that day's report with nothing looking wrong."""
    dates: dict[str, set[int]] = {}
    for event in sim.events:
        obj = event["data"]["object"]
        if obj.get("object") == "charge":
            dates.setdefault(obj["id"], set()).add(obj["created"])

    drifting = {charge: seen for charge, seen in dates.items() if len(seen) > 1}
    assert not drifting, f"{len(drifting)} charges are dated differently by different events"


def test_a_refund_is_dated_after_the_charge_it_refunds(sim):
    charged = {
        e["data"]["object"]["id"]: e["data"]["object"]["created"]
        for e in sim.events
        if e["type"] == "charge.succeeded"
    }
    refunds = [e for e in sim.events if e["type"] == "refund.created"]
    if not refunds:
        pytest.skip("no refund fell out of this run")

    for event in refunds:
        obj = event["data"]["object"]
        assert obj["created"] > charged[obj["charge"]]
