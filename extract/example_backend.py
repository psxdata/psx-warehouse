"""TEMPLATE for a new raw-storage backend — not wired into anything.

Copy this file, rename it (e.g. ``postgres_io.py``), fill in each function
body for your target store, then register it in ``extract/main.py``'s
``_get_storage()``. See CONTRIBUTING.md ("Adding a new raw-storage backend")
for the full walkthrough of why this shape exists and what each function is
responsible for.

This file is intentionally never imported by ``extract/main.py`` and every
function body just raises ``NotImplementedError`` — it exists to be read and
copied, not run. It still has to type-check and lint cleanly (CI runs
``mypy extract/`` and ``ruff check extract/`` over this whole directory), so
treat its signatures as load-bearing: match them exactly when you copy this.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

import pandas as pd

from extract.config import ConfigError


@dataclass(frozen=True)
class ExampleConfig:
    """Resolved configuration for this backend.

    Each backend owns its own concrete config type rather than sharing one
    across backends — a MotherDuck-specific field (say, an account region)
    has no business living in a dataclass a BigQuery config also uses, and
    vice versa. ``extract/config.py`` never imports this type; it only knows
    the ``BACKEND`` name string.
    """

    example_connection_string: str


def load_config() -> ExampleConfig:
    """Load this backend's configuration from environment variables.

    Follow ``bigquery_io.load_config()``/``motherduck_io.load_config()``'s
    pattern exactly: read every required env var, raise ``ConfigError`` with
    a specific message naming the missing variable if it's absent, apply
    defaults for optional ones. Never reach into another backend's env vars.

    Raises:
        ConfigError: If a required variable is missing.
    """
    example_connection_string = os.environ.get("EXAMPLE_CONNECTION_STRING", "").strip()
    if not example_connection_string:
        raise ConfigError("EXAMPLE_CONNECTION_STRING environment variable is required")

    return ExampleConfig(example_connection_string=example_connection_string)


def get_client(cfg: ExampleConfig) -> Any:
    """Construct and return this backend's connection/client object.

    Called once per run by ``extract/main.py``; the returned object is
    threaded through every other function below as their first argument
    (dependency injection, so tests can pass a mock instead of patching
    imports — see ``bigquery_io.py``'s own module docstring).
    """
    raise NotImplementedError("TODO: connect using cfg.example_connection_string")


def ensure_dataset(client: Any, cfg: ExampleConfig) -> None:
    """Create the raw dataset/schema/database if it doesn't already exist.

    Called once per run, before anything is written. Should be a cheap
    no-op on every run after the first (e.g. ``CREATE ... IF NOT EXISTS``).
    """
    raise NotImplementedError("TODO: create the raw dataset/schema if absent")


def fetch_latest_hashes(
    client: Any, cfg: ExampleConfig, symbol: str, table: str = "stock_history"
) -> dict[tuple[str, str], str]:
    """Return {(symbol, date): row_hash} for one symbol's current rows.

    ``table`` is ``stock_history`` for tickers or ``index_price_history``
    for index-level OHLCV (same schema, index name in the symbol column);
    the same applies to the next two functions.

    Used to diff a freshly-fetched OHLCV history against what's already
    stored, so unchanged trading days are never rewritten — see
    ``extract/diff.py``'s hash-join diff. Only rows where ``is_latest`` (or
    your backend's equivalent "current version" marker) is true.
    """
    raise NotImplementedError("TODO: query current row hashes for this symbol")


def load_stock_history_rows(
    client: Any, cfg: ExampleConfig, rows_df: pd.DataFrame, table: str = "stock_history"
) -> None:
    """Batch-append new/changed OHLCV rows. Append-only, never UPDATE/DELETE
    a row in place — restatement history is a real signal worth keeping (see
    DECISIONS.md "Raw layer & data modeling"). ``rows_df`` already carries a
    computed row_hash column from ``extract/diff.py``.
    """
    raise NotImplementedError("TODO: append rows_df to the given table")


def supersede_stock_history_keys(
    client: Any,
    cfg: ExampleConfig,
    keys: list[tuple[str, str]],
    run_started_at: datetime,
    table: str = "stock_history",
) -> None:
    """Flip ``is_latest`` false for the given (symbol, date) keys' PRIOR rows.

    Critical ordering detail, learned the hard way in this project's own
    history (see DECISIONS.md, 2026-08-31 "Raw layer"): filter on
    ``loaded_at < run_started_at`` (passed in, captured before this run's own
    writes), never a bare "is_latest = true" match — otherwise a changed key
    flips BOTH the old row and the row this same run just inserted, and the
    next run permanently loses the current value.
    """
    raise NotImplementedError("TODO: mark prior rows for these keys as superseded")


def load_index_constituents(
    client: Any,
    cfg: ExampleConfig,
    df: pd.DataFrame,
    index_name: str,
    snapshot_date: date,
) -> None:
    """Plain append of one index's constituent snapshot for one date.

    No diffing needed here (unlike stock history) — each run's snapshot is
    its own immutable record of index membership on that date.
    """
    raise NotImplementedError("TODO: append the constituents snapshot")


def fetch_latest_symbol_hashes(client: Any, cfg: ExampleConfig) -> dict[str, str]:
    """Return {symbol: row_hash} for every symbol's current ticker-attribute row.

    Same hash-diff purpose as ``fetch_latest_hashes``, but keyed on symbol
    alone (ticker attributes like name/sector aren't per-date).
    """
    raise NotImplementedError("TODO: query current row hashes for all symbols")


def load_symbols_rows(client: Any, cfg: ExampleConfig, rows_df: pd.DataFrame) -> None:
    """Batch-append new/changed ticker-attribute rows. Same append-only,
    hash-diffed pattern as ``load_stock_history_rows``.
    """
    raise NotImplementedError("TODO: append rows_df to the symbols table")


def supersede_symbol_keys(
    client: Any,
    cfg: ExampleConfig,
    keys: list[str],
    run_started_at: datetime,
) -> None:
    """Same superseding pattern as ``supersede_stock_history_keys``, keyed on
    symbol alone. Same ``loaded_at < run_started_at`` ordering requirement.
    """
    raise NotImplementedError("TODO: mark prior rows for these symbols as superseded")


def load_sectors_rows(
    client: Any, cfg: ExampleConfig, df: pd.DataFrame, snapshot_date: date
) -> None:
    """Plain append of one day's sector-summary snapshot. Not hash-diffed —
    every run's snapshot is its own record, like ``load_index_constituents``.
    """
    raise NotImplementedError("TODO: append the sectors snapshot")


def load_screener_rows(
    client: Any, cfg: ExampleConfig, df: pd.DataFrame, snapshot_date: date
) -> None:
    """Plain append of one day's screener (valuation/fundamentals) snapshot.
    Same non-diffed, one-record-per-run pattern as ``load_sectors_rows``.
    """
    raise NotImplementedError("TODO: append the screener snapshot")


if TYPE_CHECKING:
    # Delete this block once you've renamed the file and wired it into
    # main.py's _get_storage() — it exists only to prove, at copy time,
    # that this template genuinely satisfies RawStorage structurally, the
    # same way extract/storage.py itself checks both real backends.
    from extract import example_backend as _example_backend_check
    from extract.storage import RawStorage

    _verify_example_backend: RawStorage = _example_backend_check
