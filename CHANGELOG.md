# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- Daily index OHLCV extract: each `INDEX_NAMES` entry (default `KSE100`) is
  fetched with `psxdata.stocks(<index>)` into a new append-only raw table
  `index_price_history` (same schema and hash-diff/`is_latest` pattern as
  `stock_history`). A failure is logged and skipped; it does not fail the
  job. `fact_index_ohlcv` was stuck at 2026-09-04 because it only read the
  static seed; it now advances daily (#58).
- `stg_index_price_history` combines both sources: PSX rows wherever PSX
  has data, seed rows only before each index's first PSX date (2016-10-06
  for KSE100). `change_pct` is recomputed over the combined series. New
  columns `is_anomaly` and `source` (`psx`/`seed`) on `fact_index_ohlcv`.
- `dbt_utils.recency` warn test on `fact_index_ohlcv` (latest date per index
  older than 5 days).

- `extract/motherduck_io.py` supports a plain local `.duckdb` file (no
  MotherDuck account needed) as well as MotherDuck cloud — selected purely
  by whether `MOTHERDUCK_TOKEN` is set. New `DUCKDB_PATH` env var for the
  local-file mode. `dbt/profiles.example.yml` now has separate `dev`
  (local file) and `dev_motherduck` (cloud) targets.
- `scripts/run_local.sh`: local equivalent of the production Cloud Run
  Job's entrypoint (extract, then `dbt build`), for scheduling a local
  "daily sync" via cron. Documented in `DEPLOYMENT.md`.

- Terraform-managed GCP production infrastructure: BigQuery raw dataset,
  one service account with project-level BigQuery roles and a
  resource-scoped `run.invoker` binding, a Cloud Run Job, and a Cloud
  Scheduler job triggering it daily.
- Repository scaffolding: Python/dbt project structure, CI (lint + dbt
  parse), GitHub issue/PR templates, dependabot, branch protection on `main`.
- Raw-layer extraction for three new tables, on both BigQuery and
  MotherDuck backends: `raw.symbols` (ticker attributes, hash-diffed so
  unchanged rows aren't rewritten, with delisting detection when a
  symbol drops out of a fresh fetch), `raw.sectors` (daily sector
  summary), and `raw.screener` (daily valuation/fundamentals snapshot).
- dbt marts layer: staging models for all five raw sources
  (`stg_stock_history`, `stg_symbols`, `stg_sectors`, `stg_index_constituents`,
  `stg_screener`); a Type-2 `dim_tickers` snapshot over `stg_symbols`
  tracking ticker-attribute history with delisting handling; three
  Type-1 dimensions (`dim_sectors`, `dim_indices`, `dim_date`); five
  fact tables (`fact_ohlcv`, `fact_restatement_history`,
  `fact_index_membership`, `fact_sector_daily`, `fact_valuation_daily`)
  joined as-of to `dim_tickers` via a shared `as_of_ticker_join` macro;
  and full test coverage (not-null, uniqueness, referential-integrity,
  and accepted-range tests) across staging, the snapshot, and all marts.
- Technical/analytical indicators layer: `int_technical_indicators` (moving averages, RSI-14, MACD,
  Bollinger Bands, rolling volatility, trailing returns), `int_drawdown` (all-time-to-date max drawdown),
  and `fact_technical_indicators` (marts fact table with MA-crossover event detection).
- Cross-sectional/multi-comparison analytics layer: a point-in-time (`_pit`)
  foundation (`int_ohlcv_pit`, `int_index_constituents_pit`, `int_screener_pit`)
  fixing look-ahead bias from restated historical data; market and cap-weighted
  sector return series (`int_market_returns`, `int_sector_returns`); rolling
  252-trading-day beta (full/upside/downside) and correlation vs. index/sector
  (`int_ticker_relationships`, `fact_ticker_relationships`); sector-to-sector
  correlation (`int_sector_correlation`, `fact_sector_correlation`);
  sector-rotation scoring (`fact_sector_rotation`); and daily cross-sectional
  rankings for momentum, relative strength, value (P/E), and low-volatility,
  ranked over the point-in-time KSE-100 universe
  (`fact_cross_sectional_rankings`).
- Index-level price history: `fact_index_ohlcv` (daily open/high/low/close/
  volume/change% for KSE-100, 2010-01-04 onward), sourced from a committed
  dbt seed rather than `extract/` — the `psxdata` SDK has no historical
  index-price endpoint (only current-state daily snapshots). A one-time prep
  script (`dbt/seeds/prepare_seed_index_price_history.py`) normalizes a
  raw investing.com-style export into the seed CSV; `dbt seed` then loads it
  identically on any target, including BigQuery once it's live.

### Changed

- `fact_index_ohlcv` open/high/low/close/volume for 2016-10-06 to
  2026-09-04 now come from PSX instead of the investing.com seed. Closes
  agree within 0.22%; opens differ by more than 0.1% on 720 days.
- `fetch_latest_hashes`, `load_stock_history_rows` and
  `supersede_stock_history_keys` take an optional `table=` argument
  (default `stock_history`) in both backends and the `RawStorage` protocol.

- Extraction now runs weekdays only (Mon-Fri) instead of daily. PSX doesn't
  trade on weekends, so a Saturday/Sunday run only ever re-fetched an
  unchanged Friday close. `schedule_cron` default is now `0 18 * * 1-5`.

### Fixed

- Require `psxdata>=1.1.1` (lockfile bumped from 1.1.0). PSX now rejects data
  requests that lack its per-page request token, so every extraction run on
  1.1.0 got HTTP 403 and failed with zero OHLCV rows; 1.1.1 sends the token.
- Docker image now actually installs dbt-core/dbt-bigquery and bakes in
  `dbt_utils` at build time; previously the image could only run extraction,
  never dbt.
