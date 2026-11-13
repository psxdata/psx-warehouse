-- One row per (index_name, date). Daily rows come from PSX via the extract job
-- (raw.index_price_history); the static seed covers only dates before PSX's
-- earliest row for that index.
--
-- is_anomaly is recomputed for index rows instead of taking the SDK's strict
-- OHLC flag. PSX's official index open and close come from the opening and
-- closing sessions and can sit just outside the continuous-session high/low
-- (KSE-100 closed at 165,124.41 on 2026-10-12, below the day's 165,168.50 low).
-- Flagging those dropped real closes from int_market_returns. Tolerated:
-- open/close up to index_ohlc_tolerance (fraction, default 1%) outside the
-- range. Still flagged: non-positive or missing prices, high below low,
-- anything further out, and negative volume (the SDK's other check). The SDK's
-- flag is kept as sdk_is_anomaly.

{% set tol = var('index_ohlc_tolerance', 0.01) %}

with psx as (

    select
        symbol as index_name,
        date,
        open,
        high,
        low,
        close,
        volume,
        not coalesce(
            low > 0
            and high >= low
            and open between low * (1 - {{ tol }}) and high * (1 + {{ tol }})
            and close between low * (1 - {{ tol }}) and high * (1 + {{ tol }})
            and coalesce(volume, 0) >= 0,
            false
        ) as is_anomaly,
        is_anomaly as sdk_is_anomaly,
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
        false as sdk_is_anomaly,
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
    sdk_is_anomaly,
    source
from stitched
