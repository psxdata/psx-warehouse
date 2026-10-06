"""Extraction job entrypoint: KSE-100 constituents + OHLCV history -> raw.

Run via `python -m extract.main`. Orchestrates:
  1. Fetch + load each configured index's constituent snapshot.
  2. Sequentially fetch OHLCV history per constituent symbol, diff it
     per-ticker against current raw state, accumulate changes.
  3. Flush accumulated changes once: one batch load + one targeted UPDATE.
  4. Fetch each configured index's own daily OHLCV into raw.index_price_history
     (non-fatal: a failure here is logged and skipped).
"""
from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timezone
from typing import Any, Protocol

import pandas as pd
import psxdata
from psxdata.exceptions import PSXDataError

from extract import bigquery_io, config, motherduck_io
from extract.bigquery_io import INDEX_PRICE_HISTORY_TABLE
from extract.diff import (
    add_row_hashes,
    add_symbol_row_hashes,
    diff_against_latest,
    diff_symbols_against_latest,
)
from extract.storage import RawStorage

logger = logging.getLogger(__name__)


class ExtractionFailed(Exception):
    """Raised for conditions that should exit the job non-zero."""


def _fetch_margin_eligible_symbols() -> set[str]:
    """Union of symbols appearing in any of eligible_scrips()'s 9 category
    tables. Category labels (table_0..table_8) are fallback positional
    keys, not real names (see psxdata's own scraper docstring) -- only
    presence across the union is meaningful, not which table a symbol is in.

    Returns an empty set (not a raised exception) if eligible_scrips()
    itself fails -- found by independent review: an earlier draft left
    this call unguarded, so a PSXDataError here would propagate all the
    way out of run() uncaught (main() only catches ConfigError and
    ExtractionFailed), crashing the whole process before the OHLCV loop
    even starts, contradicting this function's own intended "non-fatal"
    design. An empty set here just means every symbol gets
    is_margin_eligible=False for this run -- self-healing, since the next
    run's fresh eligible_scrips() call retries independently.
    """
    try:
        tables = psxdata.eligible_scrips(cache=False)
    except PSXDataError as exc:
        logger.warning(
            "eligible_scrips() failed, is_margin_eligible defaults to False this run: %s",
            exc,
        )
        return set()
    symbols: set[str] = set()
    for df in tables.values():
        if not df.empty and "symbol" in df.columns:
            symbols.update(df["symbol"].astype(str).tolist())
    return symbols


class _StorageModule(RawStorage, Protocol):
    """RawStorage plus the one extra method main() needs before run().

    load_config() is deliberately not part of RawStorage itself
    (extract/storage.py): each backend owns its own concrete config type,
    so the shared Protocol can't declare a single return type for it. This
    narrower local Protocol adds just that one method for main()'s use,
    without touching RawStorage or run()'s signature — a module satisfying
    _StorageModule still structurally satisfies RawStorage everywhere else.
    """

    def load_config(self) -> Any: ...


def _get_storage(backend: str) -> _StorageModule:
    """Resolve a backend name to its storage module.

    Deliberately the only place in extract/ that knows which backend names
    exist — config.py stays backend-agnostic so adding a backend never
    requires editing it.
    """
    if backend == "motherduck":
        return motherduck_io
    if backend == "bigquery":
        return bigquery_io
    raise ExtractionFailed(f"Unsupported BACKEND: {backend!r}")


def _extract_index_prices(
    index_names: tuple[str, ...],
    storage: RawStorage,
    client: Any,
    backend_cfg: Any,
    run_started_at: datetime,
) -> None:
    """Load each index's own daily OHLCV into raw.index_price_history.

    PSX's historical endpoint accepts an index name (e.g. KSE100) as the
    symbol, so this reuses the stock-history fetch, hash-diff and write path
    against a separate table. Failures are logged and skipped rather than
    raised: ticker OHLCV stays the run's critical path, and staleness is
    caught downstream by the recency test on fact_index_ohlcv.
    """
    for index_name in index_names:
        try:
            history_df = psxdata.stocks(index_name, cache=False)
        except PSXDataError as exc:
            logger.warning("Skipping index prices for %s: fetch failed (%s)", index_name, exc)
            continue

        if history_df.empty:
            logger.warning("Skipping index prices for %s: no history returned", index_name)
            continue

        # PSX has returned exact duplicate rows for KSE100 (2024-01-22); two
        # rows with one (symbol, date) key would both land as is_latest.
        history_df = history_df.drop_duplicates().copy()
        history_df["symbol"] = index_name

        try:
            hashed_df = add_row_hashes(history_df)
        except (ValueError, TypeError, KeyError) as exc:
            logger.warning(
                "Skipping index prices for %s: malformed row data (%s)", index_name, exc
            )
            continue

        existing = storage.fetch_latest_hashes(
            client, backend_cfg, index_name, table=INDEX_PRICE_HISTORY_TABLE
        )
        rows_to_insert, superseded_keys = diff_against_latest(hashed_df, existing)

        if rows_to_insert.empty:
            logger.info("Index prices for %s: no changes detected", index_name)
            continue

        storage.load_stock_history_rows(
            client, backend_cfg, rows_to_insert, table=INDEX_PRICE_HISTORY_TABLE
        )
        storage.supersede_stock_history_keys(
            client, backend_cfg, superseded_keys, run_started_at,
            table=INDEX_PRICE_HISTORY_TABLE,
        )
        logger.info("Index prices for %s: %d rows written", index_name, len(rows_to_insert))


def run(cfg: config.Config, storage: RawStorage, backend_cfg: Any) -> None:
    """Execute one extraction run against the given configuration.

    Raises:
        ExtractionFailed: If a configured index's constituents can't be
            fetched or come back empty, or if zero OHLCV rows are fetched
            across every ticker in the run. Storage write failures are not
            caught here — they propagate as uncaught exceptions, which is
            the intended fail-loud behavior — except for storage.load_
            sectors_rows/load_screener_rows, whose narrow KeyError (a
            malformed payload missing a required column) is caught so one
            bad snapshot doesn't abort the rest of the run.
    """
    run_started_at = datetime.now(timezone.utc)
    client = storage.get_client(backend_cfg)
    storage.ensure_dataset(client, backend_cfg)

    today = date.today()
    all_symbols: set[str] = set()

    for index_name in cfg.index_names:
        try:
            constituents_df = psxdata.indices(index_name, cache=False)
        except PSXDataError as exc:
            raise ExtractionFailed(
                f"Failed to fetch constituents for index {index_name}: {exc}"
            ) from exc

        if constituents_df.empty:
            raise ExtractionFailed(f"Index {index_name} returned no constituents")

        storage.load_index_constituents(
            client, backend_cfg, constituents_df, index_name, today
        )
        all_symbols.update(constituents_df["symbol"].tolist())

    try:
        symbols_df = psxdata.symbols(cache=False)
    except PSXDataError as exc:
        logger.warning("Skipping raw.symbols this run: symbols() failed (%s)", exc)
        symbols_df = pd.DataFrame()

    if not symbols_df.empty:
        try:
            margin_eligible = _fetch_margin_eligible_symbols()
            symbols_df = symbols_df.copy()
            symbols_df["is_margin_eligible"] = symbols_df["symbol"].isin(margin_eligible)
            hashed_symbols_df = add_symbol_row_hashes(symbols_df)
        except (ValueError, TypeError, KeyError) as exc:
            logger.warning(
                "Skipping raw.symbols this run: malformed symbol data (%s)", exc
            )
            hashed_symbols_df = pd.DataFrame()

        if not hashed_symbols_df.empty:
            existing_symbols = storage.fetch_latest_symbol_hashes(client, backend_cfg)
            symbols_to_insert, changed_symbol_keys, delisted_symbol_keys = (
                diff_symbols_against_latest(hashed_symbols_df, existing_symbols)
            )

            if not symbols_to_insert.empty:
                storage.load_symbols_rows(client, backend_cfg, symbols_to_insert)
            superseded_symbol_keys = changed_symbol_keys + delisted_symbol_keys
            storage.supersede_symbol_keys(
                client, backend_cfg, superseded_symbol_keys, run_started_at
            )

    try:
        sectors_df = psxdata.sectors(cache=False)
        if not sectors_df.empty:
            storage.load_sectors_rows(client, backend_cfg, sectors_df, today)
        else:
            logger.warning("sectors() returned no data, skipping raw.sectors this run")
    except PSXDataError as exc:
        logger.warning("Skipping raw.sectors this run: sectors() failed (%s)", exc)
    except KeyError as exc:
        logger.warning("Skipping raw.sectors this run: malformed sectors data (%s)", exc)

    try:
        screener_df = psxdata.screener(cache=False)
        if not screener_df.empty:
            storage.load_screener_rows(client, backend_cfg, screener_df, today)
        else:
            logger.warning("screener() returned no data, skipping raw.screener this run")
    except PSXDataError as exc:
        logger.warning("Skipping raw.screener this run: screener() failed (%s)", exc)
    except KeyError as exc:
        logger.warning("Skipping raw.screener this run: malformed screener data (%s)", exc)

    rows_to_insert_parts: list[pd.DataFrame] = []
    all_superseded_keys: list[tuple[str, str]] = []
    total_fetched_rows = 0

    for symbol in sorted(all_symbols):
        try:
            history_df = psxdata.stocks(symbol, cache=False)
        except PSXDataError as exc:
            logger.warning("Skipping %s: fetch failed (%s)", symbol, exc)
            continue

        if history_df.empty:
            logger.warning("Skipping %s: no history returned", symbol)
            continue

        total_fetched_rows += len(history_df)
        history_df = history_df.copy()
        history_df["symbol"] = symbol

        try:
            hashed_df = add_row_hashes(history_df)
        except (ValueError, TypeError, KeyError) as exc:
            logger.warning("Skipping %s: malformed row data (%s)", symbol, exc)
            continue

        existing = storage.fetch_latest_hashes(client, backend_cfg, symbol)
        rows_to_insert, superseded_keys = diff_against_latest(hashed_df, existing)

        if not rows_to_insert.empty:
            rows_to_insert_parts.append(rows_to_insert)
        all_superseded_keys.extend(superseded_keys)

    if total_fetched_rows == 0:
        raise ExtractionFailed("Zero OHLCV rows fetched across all tickers")

    if rows_to_insert_parts:
        all_rows_to_insert = pd.concat(rows_to_insert_parts, ignore_index=True)
        storage.load_stock_history_rows(client, backend_cfg, all_rows_to_insert)
        storage.supersede_stock_history_keys(
            client, backend_cfg, all_superseded_keys, run_started_at
        )
        logger.info("Extraction complete: %d rows written", len(all_rows_to_insert))
    else:
        logger.info("Extraction complete: no changes detected, nothing written")

    _extract_index_prices(
        cfg.index_names, storage, client, backend_cfg, run_started_at
    )


def main() -> int:
    """Process entrypoint. Returns the process exit code."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    try:
        cfg = config.load_config()
        storage = _get_storage(cfg.backend)
        backend_cfg = storage.load_config()
        run(cfg, storage, backend_cfg)
    except (config.ConfigError, ExtractionFailed) as exc:
        logger.error(str(exc))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
