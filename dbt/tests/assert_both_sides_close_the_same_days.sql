-- A day one side knows about and the other does not is a hole in the reconciliation, not a
-- difference in it: there is nothing to compare, so the amount cannot be zero or non-zero, and a
-- naive join would silently drop the day rather than report it.
--
-- It is also how a calendar bug hides. The ledger derives its days from posting timestamps and the
-- processor reports its own, so a timezone that is not UTC on one side shifts every boundary by
-- hours and moves whichever transactions sit near midnight. Both sides run in UTC on purpose, and
-- this is the test that says so.

select
    close_date,
    currency,
    ledger_closed_the_day,
    processor_reported_the_day
from {{ ref('reconciliation') }}
where not ledger_closed_the_day
   or not processor_reported_the_day
