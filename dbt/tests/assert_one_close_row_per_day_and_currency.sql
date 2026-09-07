-- The daily close is what the reconciliation reads, so a duplicated day would double a balance
-- and a missing one would look like a day where nothing happened. Written out rather than pulled
-- from dbt_utils: one composite key check is not worth a package, and a package the build fetches
-- is a network call in a repository that promises to run without one.

select
    close_date,
    currency,
    count(*) as rows_for_the_day
from {{ ref('daily_close') }}
group by close_date, currency
having count(*) > 1
