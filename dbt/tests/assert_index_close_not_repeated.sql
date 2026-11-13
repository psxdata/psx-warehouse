-- A PSX index close identical to the previous session's close is almost
-- certainly a stale row (the index moves every session). The OHLC range check in
-- stg_index_price_history can't catch it, so warn here.
{{ config(severity='warn') }}

with ordered as (

    select
        index_name,
        date,
        close,
        lag(close) over (partition by index_name order by date) as prev_close
    from {{ ref('stg_index_price_history') }}
    where source = 'psx'

)

select index_name, date, close, prev_close
from ordered
where close = prev_close
