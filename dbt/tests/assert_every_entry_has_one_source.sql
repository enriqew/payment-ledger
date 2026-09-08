-- An entry moves money from one place. If it ever moved it from two, `money_flow` would have to
-- decide how much of each positive leg came from each negative one, and no rule for that is
-- written down anywhere.
--
-- This is not a property of double entry in general: an entry balancing at zero says nothing about
-- how many legs are on each side. It is a property of the entries this ledger actually makes, and
-- the flow model is built on it, so it fails the build rather than letting that model quietly
-- start approximating.

select
    entry_id,
    currency,
    count(*) as source_legs
from {{ ref('ledger_postings') }}
where amount < 0
group by entry_id, currency
having count(*) > 1
