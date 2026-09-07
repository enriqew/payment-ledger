{{ config(materialized='table', file_format='iceberg') }}

-- Double-entry postings, derived from balance transactions and nothing else.
--
-- Every balance transaction produces exactly two entries, and an entry is a set of postings that
-- sums to zero per currency. That is not a stylistic choice: it is what makes the first invariant
-- checkable, and an entry that does not balance is a bug the build refuses to publish.
--
--   recognition   at `created`:      the money exists and is pending, and the profit and loss side
--                                    of it is booked in the same breath.
--   availability  at `available_on`: the same money stops being pending and becomes available.
--
-- The second entry is why the pending balance is a real account rather than a caption. Funds
-- captured today are not available today; the balance transaction carries the date on which they
-- become so, and a ledger that ignores it reports cash that cannot be paid out.

with txn as (

    select * from {{ ref('stg_balance_transactions') }}

),

-- The balance leg of recognition: whatever actually moved, still pending.
recognition_balance as (

    select
        concat(balance_transaction_id, ':recognition')  as entry_id,
        'recognition'                                   as entry_type,
        balance_transaction_id,
        source_id,
        reporting_category,
        'asset:balance_pending'                         as account,
        net                                             as amount,
        currency,
        created                                         as posted_at
    from txn

),

-- A charge is the one category that splits three ways, because the fee is charged at the same
-- moment the sale is recognised and the two are different accounts. net + fee - amount = 0 holds
-- by construction, which is what makes the entry balance.
recognition_charge_fee as (

    select
        concat(balance_transaction_id, ':recognition')  as entry_id,
        'recognition'                                   as entry_type,
        balance_transaction_id,
        source_id,
        reporting_category,
        'expense:processing_fees'                       as account,
        fee                                             as amount,
        currency,
        created                                         as posted_at
    from txn
    where reporting_category = 'charge'

),

recognition_charge_revenue as (

    select
        concat(balance_transaction_id, ':recognition')  as entry_id,
        'recognition'                                   as entry_type,
        balance_transaction_id,
        source_id,
        reporting_category,
        'revenue:gross_sales'                           as account,
        -amount                                         as amount,
        currency,
        created                                         as posted_at
    from txn
    where reporting_category = 'charge'

),

-- Everything else balances against a single counterpart. `expense:unclassified` is deliberate:
-- a reporting category nobody has modelled lands there, the accepted_values test on `account`
-- fails, and the build stops. Silently dropping an unknown category is how money goes missing.
recognition_counterpart as (

    select
        concat(balance_transaction_id, ':recognition')  as entry_id,
        'recognition'                                   as entry_type,
        balance_transaction_id,
        source_id,
        reporting_category,
        case reporting_category
            when 'refund'           then 'contra_revenue:refunds'
            when 'dispute'          then 'expense:disputes'
            when 'dispute_reversal' then 'expense:disputes'
            when 'payout'           then 'asset:bank'
            else 'expense:unclassified'
        end                                             as account,
        -net                                            as amount,
        currency,
        created                                         as posted_at
    from txn
    where reporting_category != 'charge'

),

-- Maturation. The same money, moved out of pending and into available on the day the processor
-- says it becomes available, which is a date on the balance transaction and not a guess.
availability_out as (

    select
        concat(balance_transaction_id, ':availability') as entry_id,
        'availability'                                  as entry_type,
        balance_transaction_id,
        source_id,
        reporting_category,
        'asset:balance_pending'                         as account,
        -net                                            as amount,
        currency,
        available_on                                    as posted_at
    from txn

),

availability_in as (

    select
        concat(balance_transaction_id, ':availability') as entry_id,
        'availability'                                  as entry_type,
        balance_transaction_id,
        source_id,
        reporting_category,
        'asset:balance_available'                       as account,
        net                                             as amount,
        currency,
        available_on                                    as posted_at
    from txn

),

postings as (

    select * from recognition_balance
    union all select * from recognition_charge_fee
    union all select * from recognition_charge_revenue
    union all select * from recognition_counterpart
    union all select * from availability_out
    union all select * from availability_in

)

select
    -- One account can appear at most once per entry, so this is the natural key rather than a
    -- surrogate anyone has to trust.
    concat_ws('|', entry_id, account) as posting_id,
    entry_id,
    entry_type,
    balance_transaction_id,
    source_id,
    reporting_category,
    account,
    amount,
    currency,
    posted_at,
    cast(posted_at as date)           as posted_on
from postings
