{{ config(materialized='ephemeral') }}

-- The day by day comparison, shared by `reconciliation` and `reconciliation_items`.
--
-- It exists as its own model for a reason worth stating: dbt skips everything downstream of a
-- failed test, so when the fourth invariant fails on `reconciliation`, anything built from
-- `reconciliation` is skipped with it. That would take the itemisation away at exactly the moment
-- somebody needs to read it. Both tables hang off this instead, as siblings, so the detail of a
-- difference is on disk even on the run that refused to publish.
--
-- Ephemeral because it is a shared definition and not a thing to query: two tables that disagreed
-- about what a difference is would be worse than either of them alone.

-- The ledger weighed against the balance the processor reports, per day and per currency.
--
-- This is the check the double-entry invariants cannot make. They prove the books are internally
-- consistent, and internally consistent is not the same as right: post a transaction twice and
-- both entries still balance, the trial balance still sums to zero, and the ledger is confidently
-- wrong by exactly one transaction. Nothing inside the books can see that. Only something outside
-- them can, which is what this model is.
--
-- **No tolerance is granted.** Both sides are integers in the settlement currency's minor unit, so
-- there is no rounding to absorb and a tolerance would only be a place for a real discrepancy to
-- hide. A difference of one cent fails the run exactly like a difference of a million.

with ledger as (

    select close_date, currency, balance_pending, balance_available
    from {{ ref('daily_close') }}

),

reported as (

    select close_date, currency, pending, available
    from {{ ref('stg_reported_balance') }}

),

-- Every day either side knows about. A day one side has and the other does not is itself a
-- finding, so the join is a union of both rather than a lookup from one into the other.
spine as (

    select close_date, currency from ledger
    union
    select close_date, currency from reported

),

-- Money the pipeline received and never posted. Invariant 2 fails the build before this can be
-- anything but zero, so the column is here to put a figure on the money rather than to excuse it:
-- on the day a run does break that way, this says how much walked out and when.
unposted as (

    select
        cast(txn.created as date) as close_date,
        txn.currency,
        sum(txn.net)              as net
    from {{ ref('stg_balance_transactions') }} as txn
    left join (
        select distinct balance_transaction_id from {{ ref('ledger_postings') }}
    ) as posted
        on txn.balance_transaction_id = posted.balance_transaction_id
    where posted.balance_transaction_id is null
    group by cast(txn.created as date), txn.currency

),

joined as (

    select
        spine.close_date,
        spine.currency,
        coalesce(ledger.balance_pending, 0)    as ledger_pending,
        coalesce(ledger.balance_available, 0)  as ledger_available,
        coalesce(reported.pending, 0)          as reported_pending,
        coalesce(reported.available, 0)        as reported_available,
        ledger.close_date is not null          as ledger_closed_the_day,
        reported.close_date is not null        as processor_reported_the_day,
        coalesce(unposted.net, 0)              as unposted_net_today
    from spine
    left join ledger
        on spine.close_date = ledger.close_date and spine.currency = ledger.currency
    left join reported
        on spine.close_date = reported.close_date and spine.currency = reported.currency
    left join unposted
        on spine.close_date = unposted.close_date and spine.currency = unposted.currency

),

running as (

    select
        *,
        sum(unposted_net_today) over (
            partition by currency order by close_date
            rows between unbounded preceding and current row
        ) as unposted_net
    from joined

),

measured as (

select
    close_date,
    currency,
    ledger_pending,
    ledger_available,
    ledger_pending + ledger_available          as ledger_total,
    reported_pending,
    reported_available,
    reported_pending + reported_available      as reported_total,
    ledger_pending - reported_pending          as pending_difference,
    ledger_available - reported_available      as available_difference,
    (ledger_pending + ledger_available)
        - (reported_pending + reported_available) as total_difference,
    ledger_closed_the_day,
    processor_reported_the_day,
    unposted_net,
    -- What is left once the itemised causes are taken out. A transaction the ledger never posted
    -- makes the ledger smaller than the report by its net, so adding that net back is what
    -- accounting for it means. Whatever survives has no name, and something with no name in a
    -- reconciliation is the thing worth waking up for.
    (ledger_pending + ledger_available)
        - (reported_pending + reported_available)
        + unposted_net                          as unexplained_difference
from running

)

select
    *,
    -- A balance difference is cumulative: one bad transaction on the third makes every day after
    -- it wrong by the same amount, and itemising all of them would list the whole run. What
    -- localises the cause is the day the difference *moved*, so that is carried as its own column
    -- and it is what `reconciliation_items` keys off.
    unexplained_difference - coalesce(
        lag(unexplained_difference) over (partition by currency order by close_date), 0
    )                                           as unexplained_movement
from measured
