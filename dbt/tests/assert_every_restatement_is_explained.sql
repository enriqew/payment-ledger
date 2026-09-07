-- A published day may change. It may not change for no reason.
--
-- Restating a closed day is the correct handling of an event that arrived after the close, not a
-- failure, so the presence of restatement rows is fine and this test says nothing about how many
-- there are. What it refuses is a day whose balances moved by something other than what arrived
-- late: that is the ledger quietly rewriting a figure somebody has already read, and the cause is
-- not late data but a change in the pipeline itself.
--
-- The distinction is the whole reason `restatements` attributes by arrival time rather than by
-- amount. If the movement and the late arrivals agree, the day is explained. If they do not, this
-- fails the build, and it fails it on the run that caused it rather than on the reconciliation
-- days later.

select
    close_date,
    currency,
    pending_movement,
    available_movement,
    late_transactions,
    late_pending_effect
from {{ ref('restatements') }}
where not fully_explained_by_what_arrived_late
