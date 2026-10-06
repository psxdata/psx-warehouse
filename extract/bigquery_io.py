"""BigQuery I/O for the extraction pipeline — thin wrapper around
google-cloud-bigquery. All functions take a bigquery.Client as their first
argument (dependency injection) so callers construct it once via
get_client() and tests pass a mock instead of patching imports.
"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

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


@dataclass(frozen=True)
class BigQueryConfig:
    """Resolved BigQuery-backend configuration."""

    gcp_project: str
    bq_dataset: str
    bq_location: str


def load_config() -> BigQueryConfig:
    """Load BigQuery-backend configuration from environment variables.

    Required:
        GCP_PROJECT: GCP project ID.
        BQ_DATASET: BigQuery dataset name (e.g. "raw").

    Optional:
        BQ_LOCATION: BigQuery dataset location, used only if the dataset
            doesn't exist yet and needs to be created. Defaults to "US".

    Raises:
        ConfigError: If a required variable is missing.
    """
    gcp_project = os.environ.get("GCP_PROJECT", "").strip()
    if not gcp_project:
        raise ConfigError("GCP_PROJECT environment variable is required")

    bq_dataset = os.environ.get("BQ_DATASET", "").strip()
    if not bq_dataset:
        raise ConfigError("BQ_DATASET environment variable is required")

    bq_location = os.environ.get("BQ_LOCATION", "US").strip() or "US"

    return BigQueryConfig(
        gcp_project=gcp_project, bq_dataset=bq_dataset, bq_location=bq_location
    )

STOCK_HISTORY_SCHEMA = [
    bigquery.SchemaField("history_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("open", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("high", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("low", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("close", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("volume", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("is_anomaly", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("row_hash", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("is_latest", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("superseded_at", "TIMESTAMP", mode="NULLABLE"),
]

INDEX_CONSTITUENTS_SCHEMA = [
    bigquery.SchemaField("index_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("current_index", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("idx_weight", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("idx_point", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("market_cap_m", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("freefloat_m", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("shares_m", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

SYMBOLS_SCHEMA = [
    bigquery.SchemaField("ticker_attr_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("sector_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("is_etf", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("is_debt", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("is_gem", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("is_margin_eligible", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("row_hash", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("is_latest", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("superseded_at", "TIMESTAMP", mode="NULLABLE"),
]

SECTORS_SCHEMA = [
    bigquery.SchemaField("sector_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("sector_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("advance", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("decline", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("unchanged", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("turnover", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("market_cap_b", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

SCREENER_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("sector", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("listed_in", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("market_cap", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("price", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("pe_ratio", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("dividend_yield", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("free_float", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("volume_avg_30d", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("change_1y_pct", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]


def _generate_table_id(project: str, dataset: str, table: str) -> str:
    """Helper function to create table id on the go"""
    return f"{project}.{dataset}.{table}"


def get_client(cfg: BigQueryConfig) -> bigquery.Client:
    """Create a BigQuery client using Application Default Credentials."""
    return bigquery.Client(project=cfg.gcp_project)


def ensure_dataset(client: bigquery.Client, cfg: BigQueryConfig) -> None:
    """Create the raw dataset if it doesn't already exist."""
    dataset_ref = bigquery.DatasetReference(cfg.gcp_project, cfg.bq_dataset)
    ds = bigquery.Dataset(dataset_ref)
    ds.location = cfg.bq_location
    client.create_dataset(ds, exists_ok=True)


def fetch_latest_hashes(
    client: bigquery.Client,
    cfg: BigQueryConfig,
    symbol: str,
    table: str = STOCK_HISTORY_TABLE,
) -> dict[tuple[str, str], str]:
    """Fetch (symbol, date) -> row_hash for one symbol's is_latest rows.

    Scoped to a single symbol (not the whole table) so this stays cheap in
    both memory and BigQuery scan bytes as the warehouse grows — see the
    raw-layer spec's "BigQuery scan cost" section.

    Returns:
        Empty dict if raw.stock_history doesn't exist yet (first-ever run)
        or the symbol has no rows yet.
    """
    table_id = _generate_table_id(cfg.gcp_project, cfg.bq_dataset, table)
    query = f"""
        SELECT symbol, date, row_hash
        FROM `{table_id}`
        WHERE symbol = @symbol AND is_latest = TRUE
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("symbol", "STRING", symbol)]
    )
    try:
        rows = client.query(query, job_config=job_config).result()
    except NotFound:
        return {}
    return {
        (row["symbol"], row["date"].strftime("%Y-%m-%d")): row["row_hash"]
        for row in rows
    }


def load_stock_history_rows(
    client: bigquery.Client,
    cfg: BigQueryConfig,
    rows_df: pd.DataFrame,
    table: str = STOCK_HISTORY_TABLE,
) -> None:
    """Batch-load new/changed OHLCV rows into raw.stock_history.

    rows_df must already carry symbol/date/open/high/low/close/volume/
    is_anomaly/row_hash columns (see diff.diff_against_latest). This adds
    history_id, is_latest=True, loaded_at=now, superseded_at=NULL and runs
    one load job (create-if-needed, append, partitioned on date, clustered
    on symbol).
    """
    if rows_df.empty:
        return
    table_id = _generate_table_id(cfg.gcp_project, cfg.bq_dataset, table)
    now = datetime.now(timezone.utc)

    payload = rows_df.copy()
    payload["date"] = pd.to_datetime(payload["date"]).dt.date
    payload["history_id"] = [str(uuid.uuid4()) for _ in range(len(payload))]
    payload["is_latest"] = True
    payload["loaded_at"] = now
    payload["superseded_at"] = None
    payload = payload[[field.name for field in STOCK_HISTORY_SCHEMA]]

    job_config = bigquery.LoadJobConfig(
        schema=STOCK_HISTORY_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        time_partitioning=bigquery.TimePartitioning(field="date"),
        clustering_fields=["symbol"],
    )
    job = client.load_table_from_dataframe(payload, table_id, job_config=job_config)
    job.result()


def supersede_stock_history_keys(
    client: bigquery.Client,
    cfg: BigQueryConfig,
    keys: list[tuple[str, str]],
    run_started_at: datetime,
    table: str = STOCK_HISTORY_TABLE,
) -> None:
    """Flip is_latest=FALSE and set superseded_at for the given keys.

    keys are (symbol, "YYYY-MM-DD") tuples, as returned by
    diff.diff_against_latest. Matched via a composite "SYMBOL|YYYY-MM-DD"
    string parameter — simpler and equally safe/parameterized compared to
    an array-of-structs query parameter.

    run_started_at must be captured (UTC) before this run's ticker loop
    started, and is required so the WHERE clause can exclude the
    just-inserted replacement row for a changed key. main.py's flush order
    calls load_stock_history_rows() (which inserts the new row with
    is_latest=True, loaded_at=now()) BEFORE this function runs. Without the
    loaded_at < @run_started_at guard, the new row's is_latest=True and
    composite key both match this UPDATE's predicate, so it would flip both
    the old row AND the just-inserted row to is_latest=FALSE, leaving zero
    is_latest=TRUE rows for that key. Since the new row is always written
    during this same run (loaded_at >= run_started_at), the guard excludes
    it while still correctly superseding any pre-existing row from a prior
    run (loaded_at < run_started_at).
    """
    if not keys:
        return
    table_id = _generate_table_id(cfg.gcp_project, cfg.bq_dataset, table)
    composite_keys = [f"{symbol}|{date_str}" for symbol, date_str in keys]
    query = f"""
        UPDATE `{table_id}`
        SET is_latest = FALSE, superseded_at = CURRENT_TIMESTAMP()
        WHERE is_latest = TRUE
          AND CONCAT(symbol, '|', CAST(date AS STRING)) IN UNNEST(@keys)
          AND loaded_at < @run_started_at
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("keys", "STRING", composite_keys),
            bigquery.ScalarQueryParameter("run_started_at", "TIMESTAMP", run_started_at),
        ]
    )
    client.query(query, job_config=job_config).result()


def load_index_constituents(
    client: bigquery.Client,
    cfg: BigQueryConfig,
    df: pd.DataFrame,
    index_name: str,
    snapshot_date: date,
) -> None:
    """Batch-load one index's constituent snapshot into raw.index_constituents.

    Always inserts (no dedup) — idx_weight/idx_point/market_cap_m move day
    to day even when membership doesn't, so every snapshot carries real
    information.
    """
    if df.empty:
        return
    table_id = _generate_table_id(cfg.gcp_project, cfg.bq_dataset, INDEX_CONSTITUENTS_TABLE)
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
    payload = payload[[field.name for field in INDEX_CONSTITUENTS_SCHEMA]]

    job_config = bigquery.LoadJobConfig(
        schema=INDEX_CONSTITUENTS_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        time_partitioning=bigquery.TimePartitioning(field="snapshot_date"),
    )
    job = client.load_table_from_dataframe(payload, table_id, job_config=job_config)
    job.result()


def fetch_latest_symbol_hashes(client: bigquery.Client, cfg: BigQueryConfig) -> dict[str, str]:
    """Fetch symbol -> row_hash for all is_latest rows in raw.symbols.

    Whole-table, not scoped like fetch_latest_hashes -- symbols() itself
    returns every row in one call, so there is no per-ticker loop to bound
    memory against; the whole current-state table is at most ~1029 rows.
    """
    table_id = _generate_table_id(cfg.gcp_project, cfg.bq_dataset, SYMBOLS_TABLE)
    query = f"""
        SELECT symbol, row_hash
        FROM `{table_id}`
        WHERE is_latest = TRUE
    """
    try:
        rows = client.query(query).result()
    except NotFound:
        return {}
    return {row["symbol"]: row["row_hash"] for row in rows}


def load_symbols_rows(
    client: bigquery.Client, cfg: BigQueryConfig, rows_df: pd.DataFrame
) -> None:
    """Batch-load new/changed ticker-attribute rows into raw.symbols."""
    if rows_df.empty:
        return
    table_id = _generate_table_id(cfg.gcp_project, cfg.bq_dataset, SYMBOLS_TABLE)
    now = datetime.now(timezone.utc)

    payload = rows_df.copy()
    payload["ticker_attr_id"] = [str(uuid.uuid4()) for _ in range(len(payload))]
    payload["is_latest"] = True
    payload["loaded_at"] = now
    payload["superseded_at"] = None
    payload = payload[[field.name for field in SYMBOLS_SCHEMA]]

    job_config = bigquery.LoadJobConfig(
        schema=SYMBOLS_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    job = client.load_table_from_dataframe(payload, table_id, job_config=job_config)
    job.result()


def supersede_symbol_keys(
    client: bigquery.Client,
    cfg: BigQueryConfig,
    keys: list[str],
    run_started_at: datetime,
) -> None:
    """Flip is_latest=FALSE and set superseded_at for the given symbols.

    Used for BOTH changed keys (a replacement row was just inserted by
    load_symbols_rows) and delisted keys (no replacement row exists at
    all) -- the loaded_at < @run_started_at guard is what makes this safe
    for changed keys (excludes the just-inserted replacement, same
    reasoning as supersede_stock_history_keys), and is trivially satisfied
    for delisted keys since there is no new row to exclude.
    """
    if not keys:
        return
    table_id = _generate_table_id(cfg.gcp_project, cfg.bq_dataset, SYMBOLS_TABLE)
    query = f"""
        UPDATE `{table_id}`
        SET is_latest = FALSE, superseded_at = CURRENT_TIMESTAMP()
        WHERE is_latest = TRUE
          AND symbol IN UNNEST(@keys)
          AND loaded_at < @run_started_at
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("keys", "STRING", keys),
            bigquery.ScalarQueryParameter("run_started_at", "TIMESTAMP", run_started_at),
        ]
    )
    client.query(query, job_config=job_config).result()


def load_sectors_rows(
    client: bigquery.Client,
    cfg: BigQueryConfig,
    df: pd.DataFrame,
    snapshot_date: date,
) -> None:
    """Batch-load one day's sector summary into raw.sectors.

    Always inserts (no dedup) -- advance/decline/turnover/market_cap_b
    move day to day, same reasoning as load_index_constituents.
    """
    if df.empty:
        return
    table_id = _generate_table_id(cfg.gcp_project, cfg.bq_dataset, SECTORS_TABLE)
    now = datetime.now(timezone.utc)

    payload = df.copy()
    payload["snapshot_date"] = snapshot_date
    payload["loaded_at"] = now
    optional_cols = ("advance", "decline", "unchanged", "turnover", "market_cap_b")
    for optional_col in optional_cols:
        if optional_col not in payload.columns:
            payload[optional_col] = pd.NA
    payload = payload[[field.name for field in SECTORS_SCHEMA]]

    job_config = bigquery.LoadJobConfig(
        schema=SECTORS_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        time_partitioning=bigquery.TimePartitioning(field="snapshot_date"),
    )
    job = client.load_table_from_dataframe(payload, table_id, job_config=job_config)
    job.result()


def _clean_screener_sector(value: object) -> str | None:
    """psxdata.screener()'s sector column is a numeric PSX sector code
    coerced to float64 by the SDK, but SCREENER_SCHEMA stores it as STRING
    to join against raw.sectors.sector_code -- convert without leaving a
    stray trailing '.0' on what's really an identifier, not a number.
    """
    if pd.isna(value):
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def load_screener_rows(
    client: bigquery.Client,
    cfg: BigQueryConfig,
    df: pd.DataFrame,
    snapshot_date: date,
) -> None:
    """Batch-load one day's screener snapshot into raw.screener.

    Always inserts (no dedup) -- price/market_cap/pe_ratio move daily.
    """
    if df.empty:
        return
    table_id = _generate_table_id(cfg.gcp_project, cfg.bq_dataset, SCREENER_TABLE)
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
    payload["sector"] = payload["sector"].apply(_clean_screener_sector)
    payload = payload[[field.name for field in SCREENER_SCHEMA]]

    job_config = bigquery.LoadJobConfig(
        schema=SCREENER_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        time_partitioning=bigquery.TimePartitioning(field="snapshot_date"),
    )
    job = client.load_table_from_dataframe(payload, table_id, job_config=job_config)
    job.result()
