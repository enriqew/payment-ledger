-- A balance transaction the stream showed us, and the processor's list does not have.
--
-- Dispute payloads embed their balance transactions expanded, which makes them the one case where
-- the event stream carries the money itself instead of an id. That redundancy is worth something
-- exactly once: when the list is missing a row, the stream still has a copy of it, and the two can
-- be put side by side.
--
-- The failure this catches is a reversal that never landed. Winning a dispute returns the amount
-- and keeps the fee, and if that reversal is absent from the list the ledger never gives the money
-- back. Nothing inside the books notices, because every entry it does have balances. The
-- reconciliation notices days later and calls it an unexplained difference. This names the
-- transaction, on the day the run happened.

select
    balance_transaction_id,
    source_id,
    reporting_category,
    net,
    currency,
    noticed_on
from {{ ref('coverage_gaps') }}
where finding = 'transaction_missing_from_the_list'
