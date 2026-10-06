-- One row per (index_name, date). Daily rows come from PSX via the extract job
-- (raw.index_price_history); the static seed covers only dates before PSX's
-- earliest row for that index.

with psx as (

    select
        symbol as index_name,
        date,
        open,
        high,
        low,
        close,
        volume,
        is_anomaly,
        'psx' as source
    from {{ source('raw', 'index_price_history') }}
    where is_latest = true

),

seed as (

    select
        index_name,
        date,
        open,
        high,
        low,
        close,
        volume,
        false as is_anomaly,
        'seed' as source
    from {{ ref('seed_index_price_history') }}

),

psx_start as (

    select index_name, min(date) as first_psx_date
    from psx
    group by index_name

),

stitched as (

    select * from psx

    union all

    -- Seed rows only before PSX's first date for that index; an index with no
    -- PSX rows yet (left join miss) keeps its whole seed history.
    select seed.*
    from seed
    left join psx_start
        on seed.index_name = psx_start.index_name
    where psx_start.first_psx_date is null
        or seed.date < psx_start.first_psx_date

)

select
    index_name,
    date,
    open,
    high,
    low,
    close,
    volume,
    (close / lag(close) over (partition by index_name order by date) - 1) * 100 as change_pct,
    is_anomaly,
    source
from stitched
