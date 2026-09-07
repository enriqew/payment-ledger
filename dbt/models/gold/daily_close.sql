{{ config(materialized='table', file_format='iceberg') }}

-- What the books say for each day, which is the thing phase 4 weighs against the balance the
-- processor reports.
--
-- Built on a full date spine rather than on the days that happen to have postings. A day with no
-- movement still has a balance, and a reconciliation that silently skips it cannot tell the
-- difference between "nothing happened" and "we lost the day".

with postings as (

    select * from {{ ref('ledger_postings') }}

),

spine as (

    select explode(sequence(min(posted_on), max(posted_on), interval 1 day)) as close_date
    from postings

),

currencies as (

    select distinct currency from postings

),

grid as (

    select close_date, currency
    from spine
    cross join currencies

),

movements as (

    select
        posted_on                                                            as close_date,
        currency,
        sum(case when account = 'revenue:gross_sales'      then -amount else 0 end) as gross_sales,
        sum(case when account = 'contra_revenue:refunds'   then amount  else 0 end) as refunds,
        sum(case when account = 'expense:processing_fees'  then amount  else 0 end) as processing_fees,
        sum(case when account = 'expense:disputes'         then amount  else 0 end) as disputes,
        sum(case when account = 'asset:balance_pending'    then amount  else 0 end) as pending_movement,
        sum(case when account = 'asset:balance_available'  then amount  else 0 end) as available_movement,
        sum(case when account = 'asset:bank'               then amount  else 0 end) as bank_movement
    from postings
    group by posted_on, currency

),

daily as (

    select
        grid.close_date,
        grid.currency,
        coalesce(movements.gross_sales, 0)         as gross_sales,
        coalesce(movements.refunds, 0)             as refunds,
        coalesce(movements.processing_fees, 0)     as processing_fees,
        coalesce(movements.disputes, 0)            as disputes,
        coalesce(movements.pending_movement, 0)    as pending_movement,
        coalesce(movements.available_movement, 0)  as available_movement,
        coalesce(movements.bank_movement, 0)       as bank_movement
    from grid
    left join movements
        on grid.close_date = movements.close_date
       and grid.currency   = movements.currency

)

select
    close_date,
    currency,
    gross_sales,
    refunds,
    processing_fees,
    disputes,
    pending_movement,
    available_movement,
    bank_movement,
    sum(pending_movement) over (
        partition by currency order by close_date
        rows between unbounded preceding and current row
    ) as balance_pending,
    sum(available_movement) over (
        partition by currency order by close_date
        rows between unbounded preceding and current row
    ) as balance_available,
    sum(bank_movement) over (
        partition by currency order by close_date
        rows between unbounded preceding and current row
    ) as balance_bank,
    -- Carried so the third invariant can ask whether a negative available balance has anything
    -- behind it, rather than only whether something happened on that one day.
    sum(disputes) over (
        partition by currency order by close_date
        rows between unbounded preceding and current row
    ) as disputes_to_date,
    sum(refunds) over (
        partition by currency order by close_date
        rows between unbounded preceding and current row
    ) as refunds_to_date
from daily
