-- Invariant 1. Postings for one entry sum to zero, per currency.
--
-- The one that makes it double entry rather than a list of amounts. A row here means money was
-- created or destroyed by the ledger itself, which no amount of downstream reporting can repair,
-- so it fails the build and gold is not published.

select
    entry_id,
    currency,
    sum(amount) as imbalance,
    count(*)    as postings
from {{ ref('ledger_postings') }}
group by entry_id, currency
having sum(amount) != 0
