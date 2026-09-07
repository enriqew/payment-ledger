{{ config(materialized='table', file_format='iceberg') }}

-- Where the two inputs disagree about what happened.
--
-- The pipeline has two independent sources of truth about the same money, and the ledger is built
-- on one of them. The webhook stream says what the processor told us as it happened; the balance
-- transaction list says what the processor's own books hold. A ledger built on the list alone
-- cannot notice that the stream never mentioned a charge, and a ledger built on the stream alone
-- cannot compute a fee. Comparing them is the only way either gap becomes visible.
--
-- Two findings, in opposite directions:
--
--   `source_missing_from_the_stream`      money the processor recorded whose charge, refund or
--                                         dispute never arrived as an event. The ledger is intact,
--                                         because it posts from the list. What is missing is the
--                                         business's own record of the thing the money was for.
--
--   `transaction_missing_from_the_list`   a balance transaction the stream carried expanded, in a
--                                         dispute payload, that the list does not have. The ledger
--                                         never posted it, so the money is genuinely absent from
--                                         the books and the reported balance will say so days
--                                         later. This says so now, and names the transaction.
--
-- **A sibling of the ledger rather than a child of it, on purpose.** dbt skips everything
-- downstream of a failed test, and a gap in the inputs does not make the postings wrong. Hanging
-- this off `ledger_postings` would mean either taking the ledger away over a missing webhook or
-- losing the coverage report on the run where the ledger broke. Both read the sources.
--
-- **Empty on a healthy run.** That is the statement that the two inputs agree, not a table nobody
-- finished.

with list as (

    select * from {{ ref('stg_balance_transactions') }}
    -- Only the categories the silver layer projects into an entity of their own. A payout has no
    -- entity table here (the sandbox never issued one, so nothing was built on a guess), and
    -- reporting it as an unannounced source would be this model inventing a finding.
    where reporting_category in ('charge', 'refund', 'dispute', 'dispute_reversal')

),

-- Everything the event stream ended up knowing about, by the id the money points at. A dispute
-- and its reversal both point at the dispute, which is why one row per entity is enough.
announced as (

    select distinct charge_id  as source_id from {{ source('silver', 'charges') }}
    union
    select distinct refund_id  as source_id from {{ source('silver', 'refunds') }}
    union
    select distinct dispute_id as source_id from {{ source('silver', 'disputes') }}

),

-- What the stream itself carried expanded. Only dispute payloads embed a balance transaction, so
-- this is a small set, and it is the only place the stream can be checked against the list on the
-- money rather than on the entity.
expanded as (

    select * from {{ source('silver', 'balance_transactions') }}

),

unannounced as (

    select
        'source_missing_from_the_stream'  as finding,
        list.balance_transaction_id,
        list.source_id,
        list.reporting_category,
        list.net,
        list.currency,
        cast(list.created as date)        as noticed_on
    from list
    left join announced
        on announced.source_id = list.source_id
    where announced.source_id is null

),

unlisted as (

    select
        'transaction_missing_from_the_list' as finding,
        expanded.balance_transaction_id,
        expanded.source_id,
        expanded.reporting_category,
        expanded.net,
        expanded.currency,
        cast(expanded.created as date)      as noticed_on
    from expanded
    left join list
        on list.balance_transaction_id = expanded.balance_transaction_id
    where list.balance_transaction_id is null

)

select * from unannounced
union all
select * from unlisted
