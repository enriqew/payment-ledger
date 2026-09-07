{{ config(materialized='table', file_format='iceberg') }}

-- The trial balance: where the money sits right now, per account and per currency.
--
-- The signs read the way double entry reads and not the way a dashboard would like them to.
-- `revenue:gross_sales` is negative because revenue is a credit, and turning it positive here to
-- make it look friendlier is exactly how a ledger stops summing to zero.

select
    account,
    currency,
    sum(amount)                                   as balance,
    count(*)                                      as postings,
    min(posted_on)                                as first_posting_on,
    max(posted_on)                                as last_posting_on
from {{ ref('ledger_postings') }}
group by account, currency
