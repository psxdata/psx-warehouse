{{ config(materialized='table') }}

with market_level as (

    -- KSE-100 close from the stitched index history (PSX daily extract since
    -- 2016-10-06, seed before that). Previously derived from the constituent
    -- snapshots' current_index, which the SDK never populates (PSX's
    -- constituents table has no index-level column), leaving this model empty.
    select
        date,
        close as current_index
    from {{ ref('stg_index_price_history') }}
    where index_name = 'KSE100'  -- hardcoded per Global Constraints: this project's extraction is currently scoped to a single tracked index (extract/config.py's INDEX_NAMES default), but that's a config default, not a schema guarantee - don't assume it's the only row forever
        and close is not null
        and not is_anomaly

),

returns as (

    select
        date,
        current_index,
        (current_index - lag(current_index) over (order by date))
            / nullif(lag(current_index) over (order by date), 0) as daily_return_pct
    from market_level

)

select
    date,
    current_index,
    daily_return_pct,
    (current_index - lag(current_index, 63) over (order by date))
        / nullif(lag(current_index, 63) over (order by date), 0) as trailing_return_63d
from returns
