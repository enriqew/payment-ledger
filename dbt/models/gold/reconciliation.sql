{{ config(materialized='table', file_format='iceberg') }}

-- The ledger weighed against the balance the processor reports, per day and per currency.
--
-- This is the check the double-entry invariants cannot make. They prove the books are internally
-- consistent, and internally consistent is not the same as right: post a transaction the processor
-- never saw and both of its entries still balance, the trial balance still sums to zero, and the
-- ledger is confidently wrong by exactly that transaction. Nothing inside the books can see it.
-- Only something outside them can, which is what this is.
--
-- **No tolerance is granted.** Both sides are integers in the settlement currency's minor unit, so
-- there is no rounding to absorb and a tolerance would only be a place for a real discrepancy to
-- hide. A difference of one cent fails the run exactly like a difference of a million.
--
-- The comparison itself lives in `int_reconciliation`, which this and `reconciliation_items` both
-- read. See that model for why.

select * from {{ ref('int_reconciliation') }}
