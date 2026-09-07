-- The processor's own answer to "what was the balance on this day", narrowed and renamed to line
-- up with the ledger's daily close.
--
-- Nothing in this model derives anything. That is the point: the moment the anchor is computed
-- from the same tables the ledger is built on, the comparison in `reconciliation` stops being a
-- comparison and becomes a tautology.

select
    as_of_date  as close_date,
    currency,
    pending,
    available
from {{ source('silver', 'reported_balance') }}
