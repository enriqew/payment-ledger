{{ config(materialized='table', file_format='iceberg') }}

-- The itemisation. For every day the difference between the books and the report *moved*, the
-- balance transactions that touched that day, so the cause can be chased to rows instead of
-- argued about in aggregate.
--
-- **Keyed on the movement, not on the difference.** A balance difference is cumulative: one bad
-- transaction on the third of January makes every day after it wrong by the same amount, and
-- listing the transactions of every wrong day means listing the whole run. The day the difference
-- moved is the day the cause is on, and usually there is exactly one of them.
--
-- A day is touched twice, once when a transaction is created and the money becomes pending, and
-- again when it matures and becomes available. Both legs are listed, because a difference can come
-- from either and the two are a week apart.
--
-- **This table is empty on a healthy run, and that is what it should look like.** An empty
-- reconciliation detail is not a table nobody finished. It is the statement that every day agreed.

with moved as (

    select close_date, currency, unexplained_difference, unexplained_movement
    from {{ ref('int_reconciliation') }}
    where unexplained_movement != 0

),

posted as (

    select
        balance_transaction_id,
        count(*)                 as postings,
        count(distinct entry_id) as entries
    from {{ ref('ledger_postings') }}
    group by balance_transaction_id

)

select
    moved.close_date,
    moved.currency,
    moved.unexplained_difference,
    moved.unexplained_movement,
    txn.balance_transaction_id,
    txn.source_id,
    txn.reporting_category,
    txn.net,
    case
        when cast(txn.created as date) = moved.close_date then 'created'
        else 'matured'
    end                                              as touched_the_day_by,
    coalesce(posted.postings, 0)                     as postings,
    coalesce(posted.entries, 0)                      as entries,
    -- Two entries is what a correct transaction has, recognition and availability. Any other
    -- count is the shape of the defect, and it is worth reading before the amounts are.
    coalesce(posted.entries, 0) != 2                 as posting_count_is_wrong,
    -- The candidate. A transaction whose net is exactly the amount the day moved by is the one
    -- to look at first, and on a single-cause difference it is the only row that lights up.
    txn.net = moved.unexplained_movement             as net_matches_the_movement
from moved
inner join {{ ref('stg_balance_transactions') }} as txn
    on txn.currency = moved.currency
    and (
        cast(txn.created as date) = moved.close_date
        or cast(txn.available_on as date) = moved.close_date
    )
left join posted
    on posted.balance_transaction_id = txn.balance_transaction_id
