-- The money primitive, narrowed to the columns the ledger posts from.
--
-- `amount`, `fee` and `net` are integers in the settlement currency's minor unit and stay
-- integers through every model downstream. No cast to a decimal, no division, no float.

select
    balance_transaction_id,
    source_id,
    type,
    reporting_category,
    amount,
    fee,
    net,
    currency,
    created,
    available_on,
    status
from {{ source('silver', 'balance_transaction_list') }}
