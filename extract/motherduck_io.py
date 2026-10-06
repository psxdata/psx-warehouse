"""DuckDB-family I/O for local development — either a plain local .duckdb
file or a MotherDuck cloud database (see load_config()), chosen purely by
which environment variables are set. Mirrors bigquery_io.py's function
shapes so both satisfy extract.storage.RawStorage. Tests for the read/write
functions run against a real local DuckDB file either way — the SQL dialect
is identical, only the connection string differs.
"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone

import duckdb
import pandas as pd

from extract.config import ConfigError

STOCK_HISTORY_TABLE = "stock_history"
# Index-level OHLCV (e.g. KSE100). Same schema as stock_history, with the index
# name in the symbol column, so the stock_history functions below serve both
# tables via their table= argument.
INDEX_PRICE_HISTORY_TABLE = "index_price_history"
INDEX_CONSTITUENTS_TABLE = "index_constituents"
SYMBOLS_TABLE = "symbols"
SECTORS_TABLE = "sectors"
SCREENER_TABLE = "screener"

# Single source of truth for each table's raw-layer column order. Used both
# to reindex the insert payload and to build an explicit named-column INSERT
# (see load_stock_history_rows/load_index_constituents) so the two never
# drift apart -- a positional "INSERT INTO ... SELECT *" against a
# hand-duplicated column list would silently swap same-typed adjacent
# columns (e.g. open/high/low/close) if the DDL and payload orders diverged.
# This tuple's order must match the DDL below exactly; the DDL is the
# authoritative reference.
_STOCK_HISTORY_COLUMNS = (
    "history_id", "symbol", "date", "open", "high", "low", "close",
    "volume", "is_anomaly", "row_hash", "is_latest", "loaded_at", "superseded_at",
)

_INDEX_CONSTITUENTS_COLUMNS = (
    "index_name", "symbol", "snapshot_date", "current_index", "idx_weight",
    "idx_point", "market_cap_m", "freefloat_m", "shares_m", "loaded_at",
)

_SYMBOLS_COLUMNS = (
    "ticker_attr_id", "symbol", "name", "sector_name",
    "is_etf", "is_debt", "is_gem", "is_margin_eligible",
    "row_hash", "is_latest", "loaded_at", "superseded_at",
)

_SECTORS_COLUMNS = (
    "sector_code", "sector_name", "advance", "decline", "unchanged",
    "turnover", "market_cap_b", "snapshot_date", "loaded_at",
)

_SCREENER_COLUMNS = (
    "symbol", "sector", "listed_in", "market_cap", "price", "pe_ratio",
    "dividend_yield", "free_float", "volume_avg_30d", "change_1y_pct",
    "snapshot_date", "loaded_at",
)

# loaded_at/superseded_at are TIMESTAMP WITH TIME ZONE (DuckDB's TIMESTAMPTZ),
# not plain TIMESTAMP: a plain TIMESTAMP column stores naive wall-clock time,
# so a tz-aware datetime.now(timezone.utc) value gets silently reinterpreted
# through the connection's SESSION timezone on insert (verified: with session
# TimeZone=Asia/Karachi, a UTC value was stored shifted to local time,
# losing the UTC offset). TIMESTAMPTZ stores an absolute instant regardless
# of session timezone, matching BigQuery's TIMESTAMP semantics.
_CREATE_STOCK_HISTORY_SQL = f"""
    CREATE TABLE IF NOT EXISTS {STOCK_HISTORY_TABLE} (
        history_id VARCHAR NOT NULL,
        symbol VARCHAR NOT NULL,
        date DATE NOT NULL,
        open DOUBLE NOT NULL,
        high DOUBLE NOT NULL,
        low DOUBLE NOT NULL,
        close DOUBLE NOT NULL,
        volume BIGINT NOT NULL,
        is_anomaly BOOLEAN NOT NULL,
        row_hash VARCHAR NOT NULL,
        is_latest BOOLEAN NOT NULL,
        loaded_at TIMESTAMP WITH TIME ZONE NOT NULL,
        superseded_at TIMESTAMP WITH TIME ZONE
    )
"""

_CREATE_INDEX_PRICE_HISTORY_SQL = f"""
    CREATE TABLE IF NOT EXISTS {INDEX_PRICE_HISTORY_TABLE} (
        history_id VARCHAR NOT NULL,
        symbol VARCHAR NOT NULL,
        date DATE NOT NULL,
        open DOUBLE NOT NULL,
        high DOUBLE NOT NULL,
        low DOUBLE NOT NULL,
        close DOUBLE NOT NULL,
        volume BIGINT NOT NULL,
        is_anomaly BOOLEAN NOT NULL,
        row_hash VARCHAR NOT NULL,
        is_latest BOOLEAN NOT NULL,
        loaded_at TIMESTAMP WITH TIME ZONE NOT NULL,
        superseded_at TIMESTAMP WITH TIME ZONE
    )
"""

_CREATE_INDEX_CONSTITUENTS_SQL = f"""
    CREATE TABLE IF NOT EXISTS {INDEX_CONSTITUENTS_TABLE} (
        index_name VARCHAR NOT NULL,
        symbol VARCHAR NOT NULL,
        snapshot_date DATE NOT NULL,
        current_index DOUBLE,
        idx_weight DOUBLE,
        idx_point DOUBLE,
        market_cap_m DOUBLE,
        freefloat_m DOUBLE,
        shares_m DOUBLE,
        loaded_at TIMESTAMP WITH TIME ZONE NOT NULL
    )
"""

_CREATE_SYMBOLS_SQL = f"""
    CREATE TABLE IF NOT EXISTS {SYMBOLS_TABLE} (
        ticker_attr_id VARCHAR NOT NULL,
        symbol VARCHAR NOT NULL,
        name VARCHAR NOT NULL,
        sector_name VARCHAR NOT NULL,
        is_etf BOOLEAN NOT NULL,
        is_debt BOOLEAN NOT NULL,
        is_gem BOOLEAN NOT NULL,
        is_margin_eligible BOOLEAN NOT NULL,
        row_hash VARCHAR NOT NULL,
        is_latest BOOLEAN NOT NULL,
        loaded_at TIMESTAMP WITH TIME ZONE NOT NULL,
        superseded_at TIMESTAMP WITH TIME ZONE
    )
"""

_CREATE_SECTORS_SQL = f"""
    CREATE TABLE IF NOT EXISTS {SECTORS_TABLE} (
        sector_code VARCHAR NOT NULL,
        sector_name VARCHAR NOT NULL,
        advance BIGINT,
        decline BIGINT,
        unchanged BIGINT,
        turnover DOUBLE,
        market_cap_b DOUBLE,
        snapshot_date DATE NOT NULL,
        loaded_at TIMESTAMP WITH TIME ZONE NOT NULL
    )
"""

_CREATE_SCREENER_SQL = f"""
    CREATE TABLE IF NOT EXISTS {SCREENER_TABLE} (
        symbol VARCHAR NOT NULL,
        sector VARCHAR,
        listed_in VARCHAR,
        market_cap DOUBLE,
        price DOUBLE,
        pe_ratio DOUBLE,
        dividend_yield DOUBLE,
        free_float DOUBLE,
        volume_avg_30d DOUBLE,
        change_1y_pct DOUBLE,
        snapshot_date DATE NOT NULL,
        loaded_at TIMESTAMP WITH TIME ZONE NOT NULL
    )
"""


@dataclass(frozen=True)
class MotherDuckConfig:
    """Resolved configuration for the DuckDB-family backend — either a
    local DuckDB file or a MotherDuck cloud database. Which one is used is
    controlled entirely by whether MOTHERDUCK_TOKEN is set (see
    load_config()); this dataclass just carries the resolved DuckDB
    connection string either way, so get_client() doesn't need to know
    which mode it's in.
    """

    connection_string: str


def load_config() -> MotherDuckConfig:
    """Load this backend's configuration from environment variables.

    Two modes, chosen by whether MOTHERDUCK_TOKEN is set:

    MotherDuck (cloud) — MOTHERDUCK_TOKEN present:
        MOTHERDUCK_TOKEN: MotherDuck service token (read by DuckDB's
            MotherDuck extension directly from the environment, not
            embedded in the connection string — see get_client()).
        MD_DATABASE: MotherDuck database name (e.g. "raw_dev"). Required
            in this mode.

    Local file (no MotherDuck account needed) — MOTHERDUCK_TOKEN absent:
        DUCKDB_PATH: path to a local .duckdb file (created if it doesn't
            exist yet, including inside a Docker container's mounted
            volume). Defaults to "./warehouse.duckdb" if unset.

    Raises:
        ConfigError: If MOTHERDUCK_TOKEN is set but MD_DATABASE is missing.
    """
    motherduck_token = os.environ.get("MOTHERDUCK_TOKEN", "").strip()

    if motherduck_token:
        md_database = os.environ.get("MD_DATABASE", "").strip()
        if not md_database:
            raise ConfigError(
                "MD_DATABASE environment variable is required when MOTHERDUCK_TOKEN is set"
            )
        return MotherDuckConfig(connection_string=f"md:{md_database}")

    duckdb_path = os.environ.get("DUCKDB_PATH", "").strip() or "./warehouse.duckdb"
    return MotherDuckConfig(connection_string=duckdb_path)


def get_client(cfg: MotherDuckConfig) -> duckdb.DuckDBPyConnection:
    """Connect to the configured DuckDB database — local file or MotherDuck
    cloud, whichever load_config() resolved.

    The MotherDuck token itself is never embedded in the connection string:
    DuckDB's MotherDuck extension reads motherduck_token from the
    environment on its own, so this function's body is identical for both
    modes and never sees the token — it never appears in a traceback or
    log line that captures the connection string.
    """
    return duckdb.connect(cfg.connection_string)


def ensure_dataset(client: duckdb.DuckDBPyConnection, cfg: MotherDuckConfig) -> None:
    """Create all six raw tables if they don't already exist.

    cfg is unused here (the database is already selected by the connection
    string in get_client()) — kept in the signature for RawStorage shape
    parity with bigquery_io.ensure_dataset. Unlike BigQuery's lazy
    CREATE_IF_NEEDED on first load, all six tables (stock_history,
    index_price_history, index_constituents, symbols, sectors, screener) are created eagerly
    here, so fetch_latest_hashes never needs to handle a missing-table case.
    """
    client.execute(_CREATE_STOCK_HISTORY_SQL)
    client.execute(_CREATE_INDEX_PRICE_HISTORY_SQL)
    client.execute(_CREATE_INDEX_CONSTITUENTS_SQL)
    client.execute(_CREATE_SYMBOLS_SQL)
    client.execute(_CREATE_SECTORS_SQL)
    client.execute(_CREATE_SCREENER_SQL)


def fetch_latest_hashes(
    client: duckdb.DuckDBPyConnection,
    cfg: MotherDuckConfig,
    symbol: str,
    table: str = STOCK_HISTORY_TABLE,
) -> dict[tuple[str, str], str]:
    """Fetch (symbol, date) -> row_hash for one symbol's is_latest rows.

    No missing-table handling needed here (unlike bigquery_io's NotFound
    catch) — ensure_dataset() always creates all six tables eagerly before
    this is ever called.
    """
    rows = client.execute(
        f"SELECT symbol, date, row_hash FROM {table} "
        "WHERE symbol = ? AND is_latest = TRUE",
        [symbol],
    ).fetchall()
    return {(row[0], row[1].strftime("%Y-%m-%d")): row[2] for row in rows}


def load_stock_history_rows(
    client: duckdb.DuckDBPyConnection,
    cfg: MotherDuckConfig,
    rows_df: pd.DataFrame,
    table: str = STOCK_HISTORY_TABLE,
) -> None:
    """Insert new/changed OHLCV rows into stock_history.

    rows_df must already carry symbol/date/open/high/low/close/volume/
    is_anomaly/row_hash columns (see diff.diff_against_latest). Adds
    history_id, is_latest=True, loaded_at=now, superseded_at=NULL — same
    shape as bigquery_io.load_stock_history_rows.
    """
    if rows_df.empty:
        return
    now = datetime.now(timezone.utc)

    payload = rows_df.copy()
    payload["date"] = pd.to_datetime(payload["date"]).dt.date
    payload["history_id"] = [str(uuid.uuid4()) for _ in range(len(payload))]
    payload["is_latest"] = True
    payload["loaded_at"] = now
    payload["superseded_at"] = None
    payload = payload[list(_STOCK_HISTORY_COLUMNS)]

    client.register("stock_history_payload", payload)
    try:
        column_list = ", ".join(_STOCK_HISTORY_COLUMNS)
        client.execute(
            f"INSERT INTO {table} ({column_list}) "
            f"SELECT {column_list} FROM stock_history_payload"
        )
    finally:
        client.unregister("stock_history_payload")


def supersede_stock_history_keys(
    client: duckdb.DuckDBPyConnection,
    cfg: MotherDuckConfig,
    keys: list[tuple[str, str]],
    run_started_at: datetime,
    table: str = STOCK_HISTORY_TABLE,
) -> None:
    """Flip is_latest=FALSE and set superseded_at for the given keys.

    keys are (symbol, "YYYY-MM-DD") tuples, as returned by
    diff.diff_against_latest. run_started_at must be captured (UTC) before
    this run's ticker loop started. Without the loaded_at < ? guard, a
    changed key's just-inserted replacement row (loaded_at >= run_started_at)
    would also match is_latest=TRUE for the same key and get flipped
    alongside the genuinely-prior row — the exact Critical bug the
    BigQuery write path had until the 2026-08-31 review caught it. The
    guard excludes it by construction: the replacement is always written
    during this same run (loaded_at >= run_started_at), while any
    pre-existing row from a prior run has loaded_at < run_started_at.
    """
    if not keys:
        return
    composite_keys = [f"{symbol}|{date_str}" for symbol, date_str in keys]
    placeholders = ", ".join(["?"] * len(composite_keys))
    client.execute(
        f"""
        UPDATE {table}
        SET is_latest = FALSE, superseded_at = CURRENT_TIMESTAMP
        WHERE is_latest = TRUE
          AND (symbol || '|' || CAST(date AS VARCHAR)) IN ({placeholders})
          AND loaded_at < ?
        """,
        [*composite_keys, run_started_at],
    )


def load_index_constituents(
    client: duckdb.DuckDBPyConnection,
    cfg: MotherDuckConfig,
    df: pd.DataFrame,
    index_name: str,
    snapshot_date: date,
) -> None:
    """Insert one index's constituent snapshot into index_constituents.

    Always inserts (no dedup) — same semantics as
    bigquery_io.load_index_constituents: idx_weight/idx_point/market_cap_m
    move day to day even when membership doesn't.
    """
    if df.empty:
        return
    now = datetime.now(timezone.utc)

    payload = df.copy()
    payload["index_name"] = index_name
    payload["snapshot_date"] = snapshot_date
    payload["loaded_at"] = now
    optional_cols = (
        "current_index", "idx_weight", "idx_point",
        "market_cap_m", "freefloat_m", "shares_m"
    )
    for optional_col in optional_cols:
        if optional_col not in payload.columns:
            payload[optional_col] = pd.NA
    payload = payload[list(_INDEX_CONSTITUENTS_COLUMNS)]

    client.register("index_constituents_payload", payload)
    try:
        column_list = ", ".join(_INDEX_CONSTITUENTS_COLUMNS)
        client.execute(
            f"INSERT INTO {INDEX_CONSTITUENTS_TABLE} ({column_list}) "
            f"SELECT {column_list} FROM index_constituents_payload"
        )
    finally:
        client.unregister("index_constituents_payload")


def fetch_latest_symbol_hashes(
    client: duckdb.DuckDBPyConnection, cfg: MotherDuckConfig
) -> dict[str, str]:
    """Fetch symbol -> row_hash for all is_latest rows in symbols."""
    rows = client.execute(
        f"SELECT symbol, row_hash FROM {SYMBOLS_TABLE} WHERE is_latest = TRUE"
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def load_symbols_rows(
    client: duckdb.DuckDBPyConnection, cfg: MotherDuckConfig, rows_df: pd.DataFrame
) -> None:
    """Insert new/changed ticker-attribute rows into symbols."""
    if rows_df.empty:
        return
    now = datetime.now(timezone.utc)

    payload = rows_df.copy()
    payload["ticker_attr_id"] = [str(uuid.uuid4()) for _ in range(len(payload))]
    payload["is_latest"] = True
    payload["loaded_at"] = now
    payload["superseded_at"] = None
    payload = payload[list(_SYMBOLS_COLUMNS)]

    client.register("symbols_payload", payload)
    try:
        column_list = ", ".join(_SYMBOLS_COLUMNS)
        client.execute(
            f"INSERT INTO {SYMBOLS_TABLE} ({column_list}) "
            f"SELECT {column_list} FROM symbols_payload"
        )
    finally:
        client.unregister("symbols_payload")


def supersede_symbol_keys(
    client: duckdb.DuckDBPyConnection,
    cfg: MotherDuckConfig,
    keys: list[str],
    run_started_at: datetime,
) -> None:
    """Flip is_latest=FALSE and set superseded_at for the given symbols.

    Same dual-purpose use (changed + delisted keys) as bigquery_io's
    version -- see that function's docstring for why the loaded_at guard
    is safe for both cases.
    """
    if not keys:
        return
    placeholders = ", ".join(["?"] * len(keys))
    client.execute(
        f"""
        UPDATE {SYMBOLS_TABLE}
        SET is_latest = FALSE, superseded_at = CURRENT_TIMESTAMP
        WHERE is_latest = TRUE
          AND symbol IN ({placeholders})
          AND loaded_at < ?
        """,
        [*keys, run_started_at],
    )


def load_sectors_rows(
    client: duckdb.DuckDBPyConnection,
    cfg: MotherDuckConfig,
    df: pd.DataFrame,
    snapshot_date: date,
) -> None:
    """Insert one day's sector summary into sectors. Always inserts."""
    if df.empty:
        return
    now = datetime.now(timezone.utc)

    payload = df.copy()
    payload["snapshot_date"] = snapshot_date
    payload["loaded_at"] = now
    optional_cols = ("advance", "decline", "unchanged", "turnover", "market_cap_b")
    for optional_col in optional_cols:
        if optional_col not in payload.columns:
            payload[optional_col] = pd.NA
    payload = payload[list(_SECTORS_COLUMNS)]

    client.register("sectors_payload", payload)
    try:
        column_list = ", ".join(_SECTORS_COLUMNS)
        client.execute(
            f"INSERT INTO {SECTORS_TABLE} ({column_list}) "
            f"SELECT {column_list} FROM sectors_payload"
        )
    finally:
        client.unregister("sectors_payload")


def load_screener_rows(
    client: duckdb.DuckDBPyConnection,
    cfg: MotherDuckConfig,
    df: pd.DataFrame,
    snapshot_date: date,
) -> None:
    """Insert one day's screener snapshot into screener. Always inserts."""
    if df.empty:
        return
    now = datetime.now(timezone.utc)

    payload = df.copy()
    payload["snapshot_date"] = snapshot_date
    payload["loaded_at"] = now
    optional_cols = (
        "sector", "listed_in", "market_cap", "price", "pe_ratio",
        "dividend_yield", "free_float", "volume_avg_30d", "change_1y_pct",
    )
    for optional_col in optional_cols:
        if optional_col not in payload.columns:
            payload[optional_col] = pd.NA
    payload = payload[list(_SCREENER_COLUMNS)]

    client.register("screener_payload", payload)
    try:
        column_list = ", ".join(_SCREENER_COLUMNS)
        client.execute(
            f"INSERT INTO {SCREENER_TABLE} ({column_list}) "
            f"SELECT {column_list} FROM screener_payload"
        )
    finally:
        client.unregister("screener_payload")
