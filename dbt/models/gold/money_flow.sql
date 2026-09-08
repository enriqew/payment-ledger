{{ config(materialized='table', file_format='iceberg') }}

-- Where the money went, account to account, as a flow rather than as a balance.
--
-- A trial balance says where money ended up. It cannot say how it got there: that gross sales
-- became a pending balance minus a fee, that part of the pending balance left again as refunds and
-- disputes, and that whatever survived matured into the available balance. The postings already
-- hold that, one entry at a time, and this is the aggregate of it.
--
-- **Derived from the pairing inside an entry, not from subtracting balances.** Subtracting one
-- account from another happens to give the right answer on this run and is wrong in general: it
-- would silently absorb a won dispute paying money back, and it would break the moment a payout
-- account has anything in it. An entry is a set of postings that sums to zero, so the negative leg
-- is where the money came from and the positive legs are where it went, which is the flow itself.
--
-- **One source per entry, and that is checked rather than assumed.** With a single negative leg the
-- allocation is exact: every positive leg took its amount from that one source. An entry with two
-- negative legs would need the amounts split between them by a rule nobody has written down, so
-- `assert_every_entry_has_one_source` refuses the build instead of letting this model approximate.
--
-- **Reciprocal flows are netted.** A dispute takes money out of the pending balance and a won
-- dispute puts it back, so the pair would otherwise draw two arrows in opposite directions between
-- the same accounts. What is true of the run is the difference, and the sign says which way it
-- points.

with postings as (

    select * from {{ ref('ledger_postings') }}

),

-- The one leg of an entry that money left.
sources as (

    select entry_id, currency, account as source, -amount as available
    from postings
    where amount < 0

),

uses as (

    select entry_id, currency, account as target, amount
    from postings
    where amount > 0

),

paired as (

    select
        sources.source,
        uses.target,
        sources.currency,
        sum(uses.amount) as amount
    from uses
    inner join sources
        on sources.entry_id = uses.entry_id
       and sources.currency = uses.currency
    group by sources.source, uses.target, sources.currency

),

-- Both directions between one pair of accounts, collapsed onto the direction the money actually
-- went on balance.
netted as (

    select
        least(source, target)     as low,
        greatest(source, target)  as high,
        currency,
        sum(case when source < target then amount else -amount end) as signed_amount
    from paired
    group by least(source, target), greatest(source, target), currency

)

select
    case when signed_amount >= 0 then low else high end   as source,
    case when signed_amount >= 0 then high else low end   as target,
    currency,
    abs(signed_amount)                                    as amount
from netted
where signed_amount != 0
