{{ config(materialized='incremental', file_format='iceberg', incremental_strategy='append') }}

-- What the close said, each time it was taken.
--
-- The one table in gold that is appended rather than recomputed, and the reason is the whole of
-- section 5 of the design. Everything else here is derived from the current state of the inputs,
-- so it answers "what is true now" and forgets that it ever said anything else. A close is a
-- statement made on a date. When a late event changes a day that was already reported, the honest
-- record is not the corrected figure on its own: it is both figures and the fact that they differ.
--
-- **No `is_incremental()` filter, deliberately.** Each run appends the whole close, not the rows
-- that changed, because a run that changed nothing is itself a fact worth having: it is what makes
-- the difference between "this day was restated" and "nobody looked at this day again".
--
-- Iceberg snapshots hold the same history and would need no table at all. They are not used for
-- this because time travel needs a snapshot id as a literal, so reading "the close before this
-- one" is not something a model can express. A table anyone can query beats a mechanism that
-- requires knowing which snapshot to ask for.

select
    -- dbt's own id for the run that produced the close, so a restatement can be traced back to the
    -- build that caused it rather than only to a wall clock.
    '{{ invocation_id }}'                                  as run_id,
    cast('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as timestamp) as closed_at,
    close_date,
    currency,
    balance_pending,
    balance_available,
    gross_sales,
    refunds,
    processing_fees,
    disputes
from {{ ref('daily_close') }}
