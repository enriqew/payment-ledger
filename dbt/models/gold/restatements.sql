{{ config(materialized='table', file_format='iceberg') }}

-- A day that closed, and then changed: what it said then, what it says now, and what arrived in
-- between to make the difference.
--
-- **The close is kept, not corrected.** A pipeline that silently recomputes a published day leaves
-- whoever read the first version with no way to find out it moved, and no way to explain the
-- version they quoted. Recomputing is right; recomputing quietly is not. This is that difference
-- written down.
--
-- **Every day the late arrival touched, not only the day it landed on.** Money that appears on the
-- third of the month is pending from the third and available from the tenth, so it changes the
-- closing balance of every day from the third onwards. All of those days now say something
-- different from what they said, and a restatement that listed one of them would be describing the
-- cause rather than the effect. What is kept short is the attribution: each day names the
-- transactions that started or matured on *it*, so reading down the table shows where the movement
-- entered rather than the same handful of ids repeated forever.
--
-- The cause is not inferred from the amounts. A movement carries the moment the pipeline first saw
-- it, and that stamp is deliberately not refreshed when the list is fetched again, so what counts
-- as late is a fact about arrival rather than a guess from a figure. Attribution by amount looks
-- convincing and picks the wrong transaction the moment two of them are the same size.
--
-- Empty when nothing was restated, and empty on the first close of a fresh lakehouse, because a
-- first close has no previous version to differ from.

with log as (

    select * from {{ ref('daily_close_log') }}

),

ranked as (

    select
        closed_at,
        row_number() over (order by closed_at desc) as recency
    from (select distinct closed_at from log)

),

latest as (

    select log.* from log
    inner join ranked on ranked.closed_at = log.closed_at and ranked.recency = 1

),

previous as (

    select log.* from log
    inner join ranked on ranked.closed_at = log.closed_at and ranked.recency = 2

),

moved as (

    select
        latest.close_date,
        latest.currency,
        previous.closed_at                                    as closed_then,
        latest.closed_at                                      as restated_at,
        previous.balance_pending                              as pending_then,
        latest.balance_pending                                as pending_now,
        latest.balance_pending - previous.balance_pending      as pending_movement,
        previous.balance_available                            as available_then,
        latest.balance_available                              as available_now,
        latest.balance_available - previous.balance_available  as available_movement
    from latest
    inner join previous
        on previous.close_date = latest.close_date
       and previous.currency   = latest.currency
    where latest.balance_pending   != previous.balance_pending
       or latest.balance_available != previous.balance_available

),

-- What reached the list after the day was last closed. `loaded_at` is stamped when a movement
-- first arrives and survives every refetch, so this is when the pipeline learned of it and not
-- when it last looked.
late as (

    select
        balance_transaction_id,
        net,
        currency,
        cast(created as date)      as created_on,
        cast(available_on as date) as available_from,
        loaded_at
    from {{ source('silver', 'balance_transaction_list') }}

),

attributed as (

    select
        moved.close_date,
        moved.currency,
        -- Late money is pending from the day it was created until the day it matures, and
        -- available from then on. These two are what the day's balances should have moved by if
        -- the late arrivals are the whole story.
        sum(case
            when late.created_on <= moved.close_date and late.available_from > moved.close_date
            then late.net else 0
        end)                                                  as late_pending_effect,
        sum(case
            when late.available_from <= moved.close_date then late.net else 0
        end)                                                  as late_available_effect,
        sum(case
            when late.created_on = moved.close_date or late.available_from = moved.close_date
            then 1 else 0
        end)                                                  as late_transactions,
        collect_list(case
            when late.created_on = moved.close_date or late.available_from = moved.close_date
            then late.balance_transaction_id
        end)                                                  as caused_by
    from moved
    left join late
        on late.currency = moved.currency
       and late.loaded_at > moved.closed_then
    group by moved.close_date, moved.currency

)

select
    moved.close_date,
    moved.currency,
    moved.closed_then,
    moved.restated_at,
    moved.pending_then,
    moved.pending_now,
    moved.pending_movement,
    moved.available_then,
    moved.available_now,
    moved.available_movement,
    coalesce(attributed.late_transactions, 0)   as late_transactions,
    coalesce(attributed.late_pending_effect, 0) as late_pending_effect,
    attributed.caused_by,
    -- A restated day whose balances moved by exactly what the late arrivals do to it is fully
    -- explained. One where they do not has a second cause, and that is the row worth reading
    -- before any of the amounts are.
    coalesce(attributed.late_pending_effect, 0)   = moved.pending_movement
        and coalesce(attributed.late_available_effect, 0) = moved.available_movement
        as fully_explained_by_what_arrived_late
from moved
left join attributed
    on attributed.close_date = moved.close_date
   and attributed.currency   = moved.currency
