-- Invariant 2. Every balance transaction has at least one posting.
--
-- No money enters without a record. A balance transaction the ledger never posted is money the
-- processor moved and the books do not know about, and it would show up in phase 4 as an
-- unexplained difference days later, at which point finding it means walking backwards through
-- everything. Cheaper to refuse the build.

select
    txn.balance_transaction_id,
    txn.reporting_category,
    txn.net
from {{ ref('stg_balance_transactions') }} as txn
left join {{ ref('ledger_postings') }} as posting
    on txn.balance_transaction_id = posting.balance_transaction_id
where posting.balance_transaction_id is null
