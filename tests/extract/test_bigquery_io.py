"""Unit tests for extract.bigquery_io. All BigQuery calls are mocked —
no live GCP access."""
from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest
from google.api_core.exceptions import NotFound

from extract.bigquery_io import (
    INDEX_PRICE_HISTORY_TABLE,
    BigQueryConfig,
    ensure_dataset,
    fetch_latest_hashes,
    fetch_latest_symbol_hashes,
    load_index_constituents,
    load_screener_rows,
    load_sectors_rows,
    load_stock_history_rows,
    load_symbols_rows,
    supersede_stock_history_keys,
    supersede_symbol_keys,
)
from extract.bigquery_io import load_config as bq_load_config
from extract.config import ConfigError


def test_bq_load_config_reads_required_and_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GCP_PROJECT", "psx-warehouse-prod")
    monkeypatch.setenv("BQ_DATASET", "raw")
    monkeypatch.delenv("BQ_LOCATION", raising=False)

    cfg = bq_load_config()

    assert cfg == BigQueryConfig(
        gcp_project="psx-warehouse-prod", bq_dataset="raw", bq_location="US"
    )


def test_bq_load_config_reads_custom_location(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GCP_PROJECT", "p")
    monkeypatch.setenv("BQ_DATASET", "raw")
    monkeypatch.setenv("BQ_LOCATION", "asia-south1")

    cfg = bq_load_config()

    assert cfg.bq_location == "asia-south1"


def test_bq_load_config_raises_when_gcp_project_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    monkeypatch.setenv("BQ_DATASET", "raw")

    with pytest.raises(ConfigError, match="GCP_PROJECT"):
        bq_load_config()


def test_bq_load_config_raises_when_bq_dataset_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GCP_PROJECT", "p")
    monkeypatch.delenv("BQ_DATASET", raising=False)

    with pytest.raises(ConfigError, match="BQ_DATASET"):
        bq_load_config()


def _cfg() -> BigQueryConfig:
    return BigQueryConfig(gcp_project="proj", bq_dataset="raw", bq_location="US")


def test_ensure_dataset_creates_with_exists_ok() -> None:
    client = MagicMock()

    ensure_dataset(client, _cfg())

    client.create_dataset.assert_called_once()
    args, kwargs = client.create_dataset.call_args
    assert kwargs.get("exists_ok") is True
    assert args[0].dataset_id == "raw"
    assert args[0].location == "US"


def test_fetch_latest_hashes_builds_dict_from_query_result() -> None:
    client = MagicMock()
    client.query.return_value.result.return_value = [
        {"symbol": "ENGRO", "date": date(2024, 1, 5), "row_hash": "abc123"},
        {"symbol": "ENGRO", "date": date(2024, 1, 4), "row_hash": "def456"},
    ]

    result = fetch_latest_hashes(client, _cfg(), "ENGRO")

    assert result == {
        ("ENGRO", "2024-01-05"): "abc123",
        ("ENGRO", "2024-01-04"): "def456",
    }
    query_arg = client.query.call_args[0][0]
    assert "raw.stock_history" in query_arg
    assert "is_latest = TRUE" in query_arg
    job_config = client.query.call_args[1]["job_config"]
    assert job_config.query_parameters[0].value == "ENGRO"


def test_fetch_latest_hashes_returns_empty_dict_when_table_missing() -> None:
    client = MagicMock()
    client.query.side_effect = NotFound("table not found")

    result = fetch_latest_hashes(client, _cfg(), "ENGRO")

    assert result == {}


def test_load_stock_history_rows_noop_on_empty_dataframe() -> None:
    client = MagicMock()

    load_stock_history_rows(client, _cfg(), pd.DataFrame())

    client.load_table_from_dataframe.assert_not_called()


def test_load_stock_history_rows_adds_required_columns_and_loads() -> None:
    client = MagicMock()
    rows_df = pd.DataFrame({
        "symbol": ["ENGRO"],
        "date": [pd.Timestamp("2024-01-05")],
        "open": [481.99], "high": [496.0], "low": [474.01], "close": [481.38],
        "volume": [4496408], "is_anomaly": [False], "row_hash": ["abc123"],
    })

    load_stock_history_rows(client, _cfg(), rows_df)

    client.load_table_from_dataframe.assert_called_once()
    payload, table_id = client.load_table_from_dataframe.call_args[0]
    assert table_id == "proj.raw.stock_history"
    assert payload["is_latest"].iloc[0] == True  # noqa: E712
    assert payload["superseded_at"].iloc[0] is None or pd.isna(payload["superseded_at"].iloc[0])
    assert isinstance(payload["history_id"].iloc[0], str) and len(payload["history_id"].iloc[0]) > 0
    assert payload["date"].iloc[0] == date(2024, 1, 5)
    job_config = client.load_table_from_dataframe.call_args[1]["job_config"]
    assert job_config.clustering_fields == ["symbol"]
    client.load_table_from_dataframe.return_value.result.assert_called_once()


def test_supersede_stock_history_keys_noop_on_empty_list() -> None:
    client = MagicMock()
    run_started_at = datetime(2024, 1, 5, 12, 0, 0, tzinfo=timezone.utc)

    supersede_stock_history_keys(client, _cfg(), [], run_started_at)

    client.query.assert_not_called()


def test_supersede_stock_history_keys_builds_composite_key_update() -> None:
    client = MagicMock()
    run_started_at = datetime(2024, 1, 5, 12, 0, 0, tzinfo=timezone.utc)

    supersede_stock_history_keys(
        client, _cfg(), [("ENGRO", "2024-01-04")], run_started_at
    )

    query_arg = client.query.call_args[0][0]
    assert "SET is_latest = FALSE" in query_arg
    assert "raw.stock_history" in query_arg
    assert "is_latest = TRUE" in query_arg
    assert "loaded_at < @run_started_at" in query_arg

    job_config = client.query.call_args[1]["job_config"]
    params_by_name = {p.name: p for p in job_config.query_parameters}
    assert params_by_name["keys"].values == ["ENGRO|2024-01-04"]
    assert params_by_name["keys"].array_type == "STRING"
    assert params_by_name["run_started_at"].value == run_started_at
    assert params_by_name["run_started_at"].type_ == "TIMESTAMP"

    client.query.return_value.result.assert_called_once()


def test_load_index_constituents_noop_on_empty_dataframe() -> None:
    client = MagicMock()

    load_index_constituents(client, _cfg(), pd.DataFrame(), "KSE100", date(2024, 1, 5))

    client.load_table_from_dataframe.assert_not_called()


def test_load_index_constituents_adds_index_and_snapshot_columns() -> None:
    client = MagicMock()
    df = pd.DataFrame({
        "symbol": ["ENGRO"], "current_index": [45000.0], "idx_weight": [5.2],
        "idx_point": [2000.0], "market_cap_m": [150000.0], "freefloat_m": [40.0],
    })

    load_index_constituents(client, _cfg(), df, "KSE100", date(2024, 1, 5))

    payload, table_id = client.load_table_from_dataframe.call_args[0]
    assert table_id == "proj.raw.index_constituents"
    assert payload["index_name"].iloc[0] == "KSE100"
    assert payload["snapshot_date"].iloc[0] == date(2024, 1, 5)
    assert pd.isna(payload["shares_m"].iloc[0])


def test_fetch_latest_symbol_hashes_builds_dict_from_query_result() -> None:
    client = MagicMock()
    client.query.return_value.result.return_value = [
        {"symbol": "ENGRO", "row_hash": "h1"},
        {"symbol": "LUCK", "row_hash": "h2"},
    ]

    result = fetch_latest_symbol_hashes(client, _cfg())

    assert result == {"ENGRO": "h1", "LUCK": "h2"}


def test_fetch_latest_symbol_hashes_returns_empty_dict_when_table_missing() -> None:
    client = MagicMock()
    client.query.side_effect = NotFound("no such table")

    result = fetch_latest_symbol_hashes(client, _cfg())

    assert result == {}


def test_load_symbols_rows_skips_empty_dataframe() -> None:
    client = MagicMock()

    load_symbols_rows(client, _cfg(), pd.DataFrame())

    client.load_table_from_dataframe.assert_not_called()


def test_load_symbols_rows_loads_with_generated_columns() -> None:
    client = MagicMock()
    df = pd.DataFrame([{
        "symbol": "ENGRO", "name": "Engro Corporation", "sector_name": "Chemical",
        "is_etf": False, "is_debt": False, "is_gem": False,
        "is_margin_eligible": True, "row_hash": "h1",
    }])

    load_symbols_rows(client, _cfg(), df)

    client.load_table_from_dataframe.assert_called_once()
    payload = client.load_table_from_dataframe.call_args[0][0]
    assert payload["is_latest"].iloc[0] == True  # noqa: E712 -- numpy.bool_, not Python bool
    assert payload["superseded_at"].iloc[0] is None
    assert "ticker_attr_id" in payload.columns


def test_supersede_symbol_keys_skips_empty_keys() -> None:
    client = MagicMock()

    supersede_symbol_keys(client, _cfg(), [], datetime.now(timezone.utc))

    client.query.assert_not_called()


def test_supersede_symbol_keys_runs_update_with_params() -> None:
    client = MagicMock()
    run_started_at = datetime.now(timezone.utc)

    supersede_symbol_keys(client, _cfg(), ["ENGRO", "DELISTEDCO"], run_started_at)

    client.query.assert_called_once()
    query_arg = client.query.call_args[0][0]
    assert "loaded_at < @run_started_at" in query_arg
    _, kwargs = client.query.call_args
    params_by_name = {p.name: p for p in kwargs["job_config"].query_parameters}
    assert params_by_name["keys"].values == ["ENGRO", "DELISTEDCO"]
    assert params_by_name["run_started_at"].value == run_started_at


def test_load_sectors_rows_skips_empty_dataframe() -> None:
    client = MagicMock()

    load_sectors_rows(client, _cfg(), pd.DataFrame(), date(2026, 9, 2))

    client.load_table_from_dataframe.assert_not_called()


def test_load_sectors_rows_loads_with_snapshot_date() -> None:
    client = MagicMock()
    df = pd.DataFrame([{
        "sector_code": "101", "sector_name": "Chemical",
        "advance": 5, "decline": 2, "unchanged": 1,
        "turnover": 123456.0, "market_cap_b": 789.0,
    }])

    load_sectors_rows(client, _cfg(), df, date(2026, 9, 2))

    client.load_table_from_dataframe.assert_called_once()
    payload = client.load_table_from_dataframe.call_args[0][0]
    assert payload["snapshot_date"].iloc[0] == date(2026, 9, 2)


def test_load_sectors_rows_fills_missing_optional_columns() -> None:
    client = MagicMock()
    df = pd.DataFrame([{"sector_code": "101", "sector_name": "Chemical"}])

    load_sectors_rows(client, _cfg(), df, date(2026, 9, 2))

    client.load_table_from_dataframe.assert_called_once()
    payload = client.load_table_from_dataframe.call_args[0][0]
    for optional_col in ("advance", "decline", "unchanged", "turnover", "market_cap_b"):
        assert pd.isna(payload[optional_col].iloc[0])


def test_load_sectors_rows_raises_on_missing_required_column() -> None:
    client = MagicMock()
    df = pd.DataFrame([{"sector_name": "Chemical"}])

    with pytest.raises(KeyError):
        load_sectors_rows(client, _cfg(), df, date(2026, 9, 2))


def test_load_screener_rows_skips_empty_dataframe() -> None:
    client = MagicMock()

    load_screener_rows(client, _cfg(), pd.DataFrame(), date(2026, 9, 2))

    client.load_table_from_dataframe.assert_not_called()


def test_load_screener_rows_loads_with_snapshot_date() -> None:
    client = MagicMock()
    df = pd.DataFrame([{
        "symbol": "ENGRO", "sector": "14", "listed_in": "Main Board",
        "market_cap": 1000.0, "price": 300.5, "pe_ratio": 8.2,
        "dividend_yield": 5.1, "free_float": 45.0,
        "volume_avg_30d": 200000.0, "change_1y_pct": 12.3,
    }])

    load_screener_rows(client, _cfg(), df, date(2026, 9, 2))

    client.load_table_from_dataframe.assert_called_once()
    payload = client.load_table_from_dataframe.call_args[0][0]
    assert payload["snapshot_date"].iloc[0] == date(2026, 9, 2)


def test_load_screener_rows_coerces_float_sector_code_to_clean_string() -> None:
    """Regression test: psxdata.screener() returns sector as float64 (SDK
    coerces the raw numeric PSX sector code via coerce_numeric), but
    SCREENER_SCHEMA declares sector STRING -- previously caused a real
    production pyarrow.ArrowTypeError on client.load_table_from_dataframe.
    """
    client = MagicMock()
    df = pd.DataFrame([
        {"symbol": "ENGRO", "sector": 14.0},
        {"symbol": "LUCK", "sector": float("nan")},
    ])

    load_screener_rows(client, _cfg(), df, date(2026, 9, 2))

    client.load_table_from_dataframe.assert_called_once()
    payload = client.load_table_from_dataframe.call_args[0][0]
    assert payload["sector"].iloc[0] == "14"
    assert payload["sector"].dtype == object
    assert pd.isna(payload["sector"].iloc[1])


def test_load_screener_rows_fills_missing_optional_columns() -> None:
    client = MagicMock()
    df = pd.DataFrame([{"symbol": "ENGRO"}])

    load_screener_rows(client, _cfg(), df, date(2026, 9, 2))

    client.load_table_from_dataframe.assert_called_once()
    payload = client.load_table_from_dataframe.call_args[0][0]
    optional_cols = (
        "sector", "listed_in", "market_cap", "price", "pe_ratio",
        "dividend_yield", "free_float", "volume_avg_30d", "change_1y_pct",
    )
    for optional_col in optional_cols:
        assert pd.isna(payload[optional_col].iloc[0])


def test_stock_history_functions_route_to_index_price_history_table() -> None:
    client = MagicMock()
    client.query.return_value.result.return_value = []
    rows_df = pd.DataFrame({
        "symbol": ["KSE100"],
        "date": [pd.Timestamp("2024-01-05")],
        "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
        "volume": [1], "is_anomaly": [False], "row_hash": ["abc"],
    })
    run_started_at = datetime(2024, 1, 5, 12, 0, 0, tzinfo=timezone.utc)

    fetch_latest_hashes(client, _cfg(), "KSE100", table=INDEX_PRICE_HISTORY_TABLE)
    assert "raw.index_price_history" in client.query.call_args[0][0]

    load_stock_history_rows(client, _cfg(), rows_df, table=INDEX_PRICE_HISTORY_TABLE)
    assert client.load_table_from_dataframe.call_args[0][1] == "proj.raw.index_price_history"

    supersede_stock_history_keys(
        client, _cfg(), [("KSE100", "2024-01-04")], run_started_at,
        table=INDEX_PRICE_HISTORY_TABLE,
    )
    assert "raw.index_price_history" in client.query.call_args[0][0]
