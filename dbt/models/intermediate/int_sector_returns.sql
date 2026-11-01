{{ config(materialized='table') }}

with ticker_returns as (

    select
        o.ticker_key,
        o.date,
        o.daily_return_pct,
        dt.sector_code
    from {{ ref('int_ohlcv_pit') }} o
    inner join {{ ref('dim_tickers') }} dt on o.ticker_key = dt.dbt_scd_id

),

snapshot_weights as (

    -- Beginning-of-period cap weights: lag market_cap_m by one day so a day's
    -- weight doesn't already embed that same day's return. Using same-day
    -- (effectively end-of-period) caps would systematically over-weight
    -- risers, biasing the sector return series upward.
    select
        ticker_key,
        snapshot_date as date,
        coalesce(
            lag(market_cap_m) over (partition by ticker_key order by snapshot_date),
            first_value(market_cap_m) over (partition by ticker_key order by snapshot_date)
        ) as market_cap_m,
        min(snapshot_date) over (partition by ticker_key) as first_snapshot_date,
        first_value(market_cap_m) over (partition by ticker_key order by snapshot_date) as first_market_cap_m
    from {{ ref('int_index_constituents_pit') }}
    where index_name = 'KSE100'  -- hardcoded per Global Constraints: this project's extraction is currently scoped to a single tracked index (extract/config.py's INDEX_NAMES default), but that's a config default, not a schema guarantee - don't assume it's the only row forever

),

weights as (

    -- Constituent snapshots only exist since go-live (2026-09-18), so a 63-day
    -- return window could not complete for months. Bootstrap: before a
    -- ticker's first snapshot, approximate its weight with its earliest known
    -- market cap (approximate, constant weight; real snapshot weights take
    -- over from the first snapshot on). Tickers never in KSE100 stay excluded.
    select ticker_key, date, market_cap_m
    from snapshot_weights

    union all

    select o.ticker_key, o.date, sw.first_market_cap_m as market_cap_m
    from {{ ref('int_ohlcv_pit') }} o
    inner join (
        select distinct ticker_key, first_snapshot_date, first_market_cap_m
        from snapshot_weights
    ) sw on o.ticker_key = sw.ticker_key
    where o.date < sw.first_snapshot_date
      -- Only before the global go-live date, so a ticker added to the index
      -- later never gets retroactive constant weights (that would rewrite
      -- past sector returns). Tickers that left KSE100 before go-live are
      -- excluded: the bootstrap series carries survivorship bias.
      and o.date < (select min(date) from snapshot_weights)

),

weighted as (

    select
        tr.sector_code,
        tr.date,
        tr.daily_return_pct * w.market_cap_m as weighted_return,
        w.market_cap_m
    from ticker_returns tr
    inner join weights w on tr.ticker_key = w.ticker_key and tr.date = w.date
    where tr.daily_return_pct is not null
      and w.market_cap_m is not null
      and w.market_cap_m > 0

),

sector_return as (

    select
        sector_code,
        date,
        sum(weighted_return) / sum(market_cap_m) as daily_return_pct
    from weighted
    group by sector_code, date

)

select
    sector_code,
    date,
    daily_return_pct,
    -- Compounds daily returns forward via log-sum rather than lagging a single
    -- price level (there is no one "sector price" to lag - this is a synthetic
    -- cap-weighted series). The count() guard nulls out incomplete windows
    -- (bootstrap period) instead of silently compounding a partial 63 days.
    -- ln(x) is undefined for x <= 0, so daily_return_pct <= -1 is guarded to
    -- NULL before it reaches ln() - this also excludes exactly-100%-loss
    -- days, not just the zero/negative-argument case. count() skips NULLs,
    -- so a guarded-out day also makes that window ineligible to complete
    -- via the existing = 63 check below, rather than silently compounding
    -- past a poisoned value.
    case
        when count(case when daily_return_pct > -1 then daily_return_pct end) over (
            partition by sector_code order by date rows between 62 preceding and current row
        ) = 63
        then exp(sum(case when daily_return_pct > -1 then ln(1 + daily_return_pct) end) over (
            partition by sector_code order by date rows between 62 preceding and current row
        )) - 1
        else null
    end as trailing_return_63d
from sector_return
