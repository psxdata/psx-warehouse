select
    index_name,
    date,
    open,
    high,
    low,
    close,
    volume,
    change_pct,
    is_anomaly,
    source
from {{ ref('stg_index_price_history') }}
