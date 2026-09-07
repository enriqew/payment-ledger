-- Invariant 4. A closed day's derived balance equals the processor's reported balance, or the
-- difference is itemised. An unexplained delta fails the run.
--
-- The only invariant that can see outside the books. The other three prove the ledger is
-- internally consistent, and a ledger that posts a transaction twice is internally consistent and
-- wrong by exactly one transaction: both entries balance, the trial balance still sums to zero,
-- and nothing inside the system notices. This is what notices.
--
-- No tolerance, deliberately. Both sides are integers in minor units, so there is nothing to round
-- and a tolerance is only somewhere for a real discrepancy to live.

select
    close_date,
    currency,
    ledger_total,
    reported_total,
    total_difference,
    unposted_net,
    unexplained_difference
from {{ ref('reconciliation') }}
where unexplained_difference != 0
