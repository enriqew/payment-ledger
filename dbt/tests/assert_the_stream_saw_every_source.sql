-- Every movement the processor recorded was announced as an event first.
--
-- A dropped webhook is the quietest failure in the list, because the ledger does not flinch: the
-- postings come off the balance transaction list, so the money is right, every entry balances and
-- the day reconciles to the cent. What is gone is the record of what the money was for. The charge
-- is not in `silver.charges`, so nothing downstream of the entity layer knows the customer, the
-- payment method or the description, and the only way to find out is to notice the gap.
--
-- It fails the build rather than warning, for the same reason the others do. A gap that is a
-- warning is a gap that is still there a month later, by which time the events are past the
-- processor's replay window and the record cannot be recovered at all.

select
    balance_transaction_id,
    source_id,
    reporting_category,
    net,
    currency,
    noticed_on
from {{ ref('coverage_gaps') }}
where finding = 'source_missing_from_the_stream'
