-- Invariant 3. `balance_available` never goes negative without something explaining it.
--
-- A negative available balance is not automatically wrong: a lost dispute takes money that has
-- already been paid out, and the balance goes below zero legitimately. What is wrong is a
-- negative balance with nothing behind it, which means the ledger is either double counting a
-- withdrawal or missing an inbound transaction.
--
-- The explanation is cumulative, not same-day. A dispute on the third of the month is still what
-- explains a negative balance on the fifth, and asking only about the day itself would fail the
-- build on a correct ledger.

select
    close_date,
    currency,
    balance_available,
    disputes_to_date,
    refunds_to_date
from {{ ref('daily_close') }}
where balance_available < 0
  and disputes_to_date = 0
  and refunds_to_date = 0
