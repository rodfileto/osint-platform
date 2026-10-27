{{ config(severity='warn') }}

-- socio_history invariant: the periods still open at the latest month must
-- match the latest-month socio rows exactly.

with hist_current as (
    select count(*) as n_current
    from {{ ref('socio_history') }}
    where is_current
),

socio_latest as (
    select count(*) as n_latest
    from {{ ref('socio') }}
)

select
    hist_current.n_current,
    socio_latest.n_latest
from hist_current, socio_latest
where hist_current.n_current != socio_latest.n_latest
