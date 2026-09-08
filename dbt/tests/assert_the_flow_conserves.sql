-- What flowed into an account minus what flowed out of it is what the account holds.
--
-- The check that makes the flow the ledger rather than a picture of it. A diagram that lost a euro
-- somewhere between two accounts would still draw, and would still look convincing, because
-- nothing in a Sankey diagram objects to arrows that do not add up. This objects.
--
-- Both sides are full outer joined on purpose: an account that appears in one and not the other is
-- exactly the kind of gap the comparison exists to find, and an inner join would hide it by
-- dropping the row.

with moved as (

    select target as account, currency, sum(amount) as amount
    from {{ ref('money_flow') }}
    group by target, currency

    union all

    select source as account, currency, -sum(amount) as amount
    from {{ ref('money_flow') }}
    group by source, currency

),

flowed as (

    select account, currency, sum(amount) as net_flow
    from moved
    group by account, currency

),

held as (

    select account, currency, balance
    from {{ ref('account_balances') }}

)

select
    coalesce(flowed.account, held.account)   as account,
    coalesce(flowed.currency, held.currency) as currency,
    coalesce(flowed.net_flow, 0)             as net_flow,
    coalesce(held.balance, 0)                as balance
from flowed
full outer join held
    on held.account = flowed.account
   and held.currency = flowed.currency
where coalesce(flowed.net_flow, 0) != coalesce(held.balance, 0)
