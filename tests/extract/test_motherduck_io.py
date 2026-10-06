"""Unit tests for extract.motherduck_io. Table/write-path logic runs
against a real local temp DuckDB file (MotherDuck and local DuckDB are the
identical engine/dialect — only the connection string differs), not live
MotherDuck. get_client() itself is tested separately with duckdb.connect
mocked, since it's the one function that actually needs a real md: URL."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from extract.config import ConfigError
from extract.motherduck_io import (
    INDEX_PRICE_HISTORY_TABLE,
    SCREENER_TABLE,
    SECTORS_TABLE,
    STOCK_HISTORY_TABLE,
    SYMBOLS_TABLE,
    MotherDuckConfig,
    ensure_dataset,
    fetch_latest_hashes,
    fetch_latest_symbol_hashes,
    get_client,
    load_config,
    load_index_constituents,
    load_screener_rows,
    load_sectors_rows,
    load_stock_history_rows,
    load_symbols_rows,
    supersede_stock_history_keys,
    supersede_symbol_keys,
)


def _cfg() -> MotherDuckConfig:
    return MotherDuckConfig(connection_string="unused")


def test_load_config_reads_motherduck_vars_when_token_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "tok123")
    monkeypatch.setenv("MD_DATABASE", "raw_dev")

    cfg = load_config()

    assert cfg == MotherDuckConfig(connection_string="md:raw_dev")


def test_load_config_raises_when_database_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "tok123")
    monkeypatch.delenv("MD_DATABASE", raising=False)

    with pytest.raises(ConfigError, match="MD_DATABASE"):
        load_config()


def test_load_config_uses_local_file_when_token_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
    monkeypatch.setenv("DUCKDB_PATH", "/data/warehouse.duckdb")

    cfg = load_config()

    assert cfg == MotherDuckConfig(connection_string="/data/warehouse.duckdb")


def test_load_config_local_file_defaults_when_path_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
    monkeypatch.delenv("DUCKDB_PATH", raising=False)

    cfg = load_config()

    assert cfg == MotherDuckConfig(connection_string="./warehouse.duckdb")


@patch("extract.motherduck_io.duckdb")
def test_get_client_builds_md_connection_string(mock_duckdb: MagicMock) -> None:
    """The token must not appear in the connection string — DuckDB's
    MotherDuck extension reads motherduck_token from the environment on its
    own, and load_config() already requires MOTHERDUCK_TOKEN to be set
    there. Keeping it out of the connection string means it never shows up
    in a traceback or log line that captures the string."""
    cfg = MotherDuckConfig(connection_string="md:raw_dev")

    get_client(cfg)

    mock_duckdb.connect.assert_called_once_with("md:raw_dev")


@patch("extract.motherduck_io.duckdb")
def test_get_client_connects_to_local_path(mock_duckdb: MagicMock) -> None:
    cfg = MotherDuckConfig(connection_string="/data/warehouse.duckdb")

    get_client(cfg)

    mock_duckdb.connect.assert_called_once_with("/data/warehouse.duckdb")


def test_ensure_dataset_creates_both_tables(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))

    ensure_dataset(conn, _cfg())

    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    assert tables == {
        "stock_history", "index_constituents", "symbols", "sectors", "screener",
        "index_price_history",
    }


def test_ensure_dataset_is_idempotent(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))

    ensure_dataset(conn, _cfg())
    ensure_dataset(conn, _cfg())  # must not raise on the second call

    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    assert tables == {
        "stock_history", "index_constituents", "symbols", "sectors", "screener",
        "index_price_history",
    }


def test_fetch_latest_hashes_returns_dict_for_is_latest_rows(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    conn.execute(
        "INSERT INTO stock_history VALUES "
        "('h1', 'ENGRO', '2024-01-05', 481.99, 496.0, 474.01, 481.38, 4496408, "
        "FALSE, 'abc123', TRUE, '2024-01-05 10:00:00', NULL)"
    )

    result = fetch_latest_hashes(conn, _cfg(), "ENGRO")

    assert result == {("ENGRO", "2024-01-05"): "abc123"}


def test_fetch_latest_hashes_scoped_to_requested_symbol(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    conn.execute(
        "INSERT INTO stock_history VALUES "
        "('h1', 'LUCK', '2024-01-05', 1.0, 1.0, 1.0, 1.0, 1, FALSE, 'zzz', TRUE, "
        "'2024-01-05 10:00:00', NULL)"
    )

    result = fetch_latest_hashes(conn, _cfg(), "ENGRO")

    assert result == {}


def test_fetch_latest_hashes_excludes_non_latest_rows(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    conn.execute(
        "INSERT INTO stock_history VALUES "
        "('h1', 'ENGRO', '2024-01-05', 1.0, 1.0, 1.0, 1.0, 1, FALSE, 'old', FALSE, "
        "'2024-01-05 10:00:00', '2024-01-06 10:00:00')"
    )

    result = fetch_latest_hashes(conn, _cfg(), "ENGRO")

    assert result == {}


def test_load_stock_history_rows_noop_on_empty_dataframe(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())

    load_stock_history_rows(conn, _cfg(), pd.DataFrame())

    count = conn.execute(f"SELECT COUNT(*) FROM {STOCK_HISTORY_TABLE}").fetchone()[0]
    assert count == 0


def test_load_stock_history_rows_inserts_with_required_columns(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    rows_df = pd.DataFrame({
        "symbol": ["ENGRO"],
        "date": [pd.Timestamp("2024-01-05")],
        "open": [481.99], "high": [496.0], "low": [474.01], "close": [481.38],
        "volume": [4496408], "is_anomaly": [False], "row_hash": ["abc123"],
    })

    load_stock_history_rows(conn, _cfg(), rows_df)

    row = conn.execute(
        f"SELECT symbol, date, is_latest, superseded_at, history_id FROM {STOCK_HISTORY_TABLE}"
    ).fetchone()
    assert row[0] == "ENGRO"
    assert row[1] == date(2024, 1, 5)
    assert row[2] is True
    assert row[3] is None
    assert isinstance(row[4], str) and len(row[4]) > 0


def test_supersede_stock_history_keys_noop_on_empty_list(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())

    supersede_stock_history_keys(conn, _cfg(), [], datetime.now(timezone.utc))

    count = conn.execute(f"SELECT COUNT(*) FROM {STOCK_HISTORY_TABLE}").fetchone()[0]
    assert count == 0


def test_supersede_stock_history_keys_flips_prior_row(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    run_started_at = datetime.now(timezone.utc)
    conn.execute(
        "INSERT INTO stock_history VALUES "
        "('old-id', 'ENGRO', '2024-01-05', 1.0, 1.0, 1.0, 1.0, 1, FALSE, "
        "'stale-hash', TRUE, ?, NULL)",
        [run_started_at - timedelta(hours=1)],
    )

    supersede_stock_history_keys(conn, _cfg(), [("ENGRO", "2024-01-05")], run_started_at)

    row = conn.execute(
        f"SELECT is_latest, superseded_at FROM {STOCK_HISTORY_TABLE} WHERE history_id = 'old-id'"
    ).fetchone()
    assert row[0] is False
    assert row[1] is not None


def test_supersede_stock_history_keys_excludes_just_inserted_row(tmp_path: Path) -> None:
    """Regression guard mirroring the BigQuery Critical bug fix: inserting a
    changed row THEN superseding its key must flip only the prior row, not
    the just-inserted replacement — both share (symbol, date) and
    is_latest=TRUE at the moment supersede runs."""
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    run_started_at = datetime.now(timezone.utc)
    # Prior run's row — loaded before this run started.
    conn.execute(
        "INSERT INTO stock_history VALUES "
        "('old-id', 'ENGRO', '2024-01-05', 1.0, 1.0, 1.0, 1.0, 1, FALSE, "
        "'stale-hash', TRUE, ?, NULL)",
        [run_started_at - timedelta(hours=1)],
    )
    # This run's replacement row for the same key — loaded_at >= run_started_at.
    rows_df = pd.DataFrame({
        "symbol": ["ENGRO"], "date": [pd.Timestamp("2024-01-05")],
        "open": [2.0], "high": [2.0], "low": [2.0], "close": [2.0],
        "volume": [2], "is_anomaly": [False], "row_hash": ["fresh-hash"],
    })
    load_stock_history_rows(conn, _cfg(), rows_df)

    supersede_stock_history_keys(conn, _cfg(), [("ENGRO", "2024-01-05")], run_started_at)

    remaining_latest = conn.execute(
        f"SELECT row_hash FROM {STOCK_HISTORY_TABLE} WHERE is_latest = TRUE"
    ).fetchall()
    assert remaining_latest == [("fresh-hash",)]


def test_load_index_constituents_noop_on_empty_dataframe(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())

    load_index_constituents(conn, _cfg(), pd.DataFrame(), "KSE100", date(2024, 1, 5))

    count = conn.execute("SELECT COUNT(*) FROM index_constituents").fetchone()[0]
    assert count == 0


def test_load_index_constituents_adds_index_and_snapshot_columns(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    df = pd.DataFrame({
        "symbol": ["ENGRO"], "current_index": [45000.0], "idx_weight": [5.2],
        "idx_point": [2000.0], "market_cap_m": [150000.0], "freefloat_m": [40.0],
    })

    load_index_constituents(conn, _cfg(), df, "KSE100", date(2024, 1, 5))

    row = conn.execute(
        "SELECT index_name, snapshot_date, shares_m FROM index_constituents"
    ).fetchone()
    assert row[0] == "KSE100"
    assert row[1] == date(2024, 1, 5)
    assert row[2] is None


def test_loaded_at_round_trips_as_utc_regardless_of_session_timezone(tmp_path: Path) -> None:
    """Regression guard for the naive-TIMESTAMP bug: loaded_at/superseded_at
    must be TIMESTAMP WITH TIME ZONE so a tz-aware UTC value survives a
    round trip unchanged even when the connection's session TimeZone is set
    to something other than UTC (e.g. Asia/Karachi, UTC+5) — a plain
    TIMESTAMP column would silently reinterpret the UTC value as local
    wall-clock time and shift it by the offset."""
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    conn.execute("SET TimeZone = 'Asia/Karachi'")

    known_utc = datetime(2024, 1, 5, 23, 30, 0, tzinfo=timezone.utc)
    rows_df = pd.DataFrame({
        "symbol": ["ENGRO"], "date": [pd.Timestamp("2024-01-05")],
        "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
        "volume": [1], "is_anomaly": [False], "row_hash": ["abc"],
    })
    with patch("extract.motherduck_io.datetime") as mock_datetime:
        mock_datetime.now.return_value = known_utc
        load_stock_history_rows(conn, _cfg(), rows_df)

    loaded_at = conn.execute(
        f"SELECT loaded_at FROM {STOCK_HISTORY_TABLE} WHERE symbol = 'ENGRO'"
    ).fetchone()[0]

    assert loaded_at.utctimetuple()[:6] == known_utc.utctimetuple()[:6]
    assert loaded_at.astimezone(timezone.utc) == known_utc


def test_load_index_constituents_always_inserts_no_dedup(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    df = pd.DataFrame({"symbol": ["ENGRO"]})

    load_index_constituents(conn, _cfg(), df, "KSE100", date(2024, 1, 5))
    load_index_constituents(conn, _cfg(), df, "KSE100", date(2024, 1, 5))

    count = conn.execute("SELECT COUNT(*) FROM index_constituents").fetchone()[0]
    assert count == 2


def test_ensure_dataset_creates_symbols_table(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())

    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    assert "symbols" in tables


def test_fetch_latest_symbol_hashes_empty_when_no_rows(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())

    result = fetch_latest_symbol_hashes(conn, _cfg())

    assert result == {}


def test_load_and_fetch_symbols_roundtrip(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    df = pd.DataFrame([{
        "symbol": "ENGRO", "name": "Engro Corporation", "sector_name": "Chemical",
        "is_etf": False, "is_debt": False, "is_gem": False,
        "is_margin_eligible": True, "row_hash": "h1",
    }])

    load_symbols_rows(conn, _cfg(), df)
    result = fetch_latest_symbol_hashes(conn, _cfg())

    assert result == {"ENGRO": "h1"}


def test_supersede_symbol_keys_flips_is_latest_and_respects_run_started_at(
    tmp_path: Path,
) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    df = pd.DataFrame([{
        "symbol": "ENGRO", "name": "Engro Corporation", "sector_name": "Chemical",
        "is_etf": False, "is_debt": False, "is_gem": False,
        "is_margin_eligible": True, "row_hash": "h1",
    }])
    load_symbols_rows(conn, _cfg(), df)

    new_run = datetime.now(timezone.utc)
    supersede_symbol_keys(conn, _cfg(), ["ENGRO"], new_run)

    row = conn.execute(
        f"SELECT is_latest, superseded_at FROM {SYMBOLS_TABLE} WHERE symbol = 'ENGRO'"
    ).fetchone()
    assert row[0] is False
    assert row[1] is not None


def test_supersede_symbol_keys_excludes_just_inserted_row(tmp_path: Path) -> None:
    """Regression guard mirroring the BigQuery Critical bug fix: inserting a
    changed row THEN superseding its key must flip only the prior row, not
    the just-inserted replacement — both share symbol and is_latest=TRUE at
    the moment supersede runs."""
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    run_started_at = datetime.now(timezone.utc)
    # Prior run's row — loaded before this run started.
    conn.execute(
        "INSERT INTO symbols VALUES "
        "('old-id', 'ENGRO', 'Engro Corporation', 'Chemical', FALSE, FALSE, "
        "FALSE, TRUE, 'stale-hash', TRUE, ?, NULL)",
        [run_started_at - timedelta(hours=1)],
    )
    # This run's replacement row for the same key — loaded_at >= run_started_at.
    rows_df = pd.DataFrame([{
        "symbol": "ENGRO", "name": "Engro Corporation", "sector_name": "Chemical",
        "is_etf": False, "is_debt": False, "is_gem": False,
        "is_margin_eligible": True, "row_hash": "fresh-hash",
    }])
    load_symbols_rows(conn, _cfg(), rows_df)

    supersede_symbol_keys(conn, _cfg(), ["ENGRO"], run_started_at)

    remaining_latest = conn.execute(
        f"SELECT row_hash FROM {SYMBOLS_TABLE} WHERE is_latest = TRUE"
    ).fetchall()
    assert remaining_latest == [("fresh-hash",)]


def test_supersede_symbol_keys_skips_empty_keys(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    df = pd.DataFrame([{
        "symbol": "ENGRO", "name": "Engro Corporation", "sector_name": "Chemical",
        "is_etf": False, "is_debt": False, "is_gem": False,
        "is_margin_eligible": True, "row_hash": "h1",
    }])
    load_symbols_rows(conn, _cfg(), df)

    supersede_symbol_keys(conn, _cfg(), [], datetime.now(timezone.utc))

    row = conn.execute(
        f"SELECT is_latest FROM {SYMBOLS_TABLE} WHERE symbol = 'ENGRO'"
    ).fetchone()
    assert row[0] is True


def test_ensure_dataset_creates_sectors_table(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())

    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    assert "sectors" in tables


def test_load_sectors_rows_inserts(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    df = pd.DataFrame([{
        "sector_code": "101", "sector_name": "Chemical",
        "advance": 5, "decline": 2, "unchanged": 1,
        "turnover": 123456.0, "market_cap_b": 789.0,
    }])

    load_sectors_rows(conn, _cfg(), df, date(2026, 9, 2))

    row = conn.execute(f"SELECT sector_code, snapshot_date FROM {SECTORS_TABLE}").fetchone()
    assert row == ("101", date(2026, 9, 2))


def test_load_sectors_rows_fills_missing_optional_columns(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    df = pd.DataFrame([{"sector_code": "101", "sector_name": "Chemical"}])

    load_sectors_rows(conn, _cfg(), df, date(2026, 9, 2))

    row = conn.execute(
        "SELECT sector_code, sector_name, advance, decline, unchanged, "
        f"turnover, market_cap_b FROM {SECTORS_TABLE}"
    ).fetchone()
    assert row[0] == "101"
    assert row[1] == "Chemical"
    assert row[2] is None
    assert row[3] is None
    assert row[4] is None
    assert row[5] is None
    assert row[6] is None


def test_ensure_dataset_creates_screener_table(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())

    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    assert "screener" in tables


def test_load_screener_rows_inserts(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    df = pd.DataFrame([{
        "symbol": "ENGRO", "sector": "14", "listed_in": "Main Board",
        "market_cap": 1000.0, "price": 300.5, "pe_ratio": 8.2,
        "dividend_yield": 5.1, "free_float": 45.0,
        "volume_avg_30d": 200000.0, "change_1y_pct": 12.3,
    }])

    load_screener_rows(conn, _cfg(), df, date(2026, 9, 2))

    row = conn.execute(f"SELECT symbol, snapshot_date FROM {SCREENER_TABLE}").fetchone()
    assert row == ("ENGRO", date(2026, 9, 2))


def test_load_screener_rows_fills_missing_optional_columns(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    df = pd.DataFrame([{"symbol": "ENGRO"}])

    load_screener_rows(conn, _cfg(), df, date(2026, 9, 2))

    row = conn.execute(
        "SELECT symbol, sector, listed_in, market_cap, price, pe_ratio, "
        f"dividend_yield, free_float, volume_avg_30d, change_1y_pct FROM {SCREENER_TABLE}"
    ).fetchone()
    assert row[0] == "ENGRO"
    for value in row[1:]:
        assert value is None


def test_load_screener_rows_raises_on_missing_required_column(tmp_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    df = pd.DataFrame([{"sector": "14", "price": 300.5}])

    with pytest.raises(KeyError):
        load_screener_rows(conn, _cfg(), df, date(2026, 9, 2))


def test_stock_history_functions_route_to_index_price_history_table(
    tmp_path: Path,
) -> None:
    import duckdb

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ensure_dataset(conn, _cfg())
    run_started_at = datetime.now(timezone.utc)
    conn.execute(
        f"INSERT INTO {INDEX_PRICE_HISTORY_TABLE} VALUES "
        "('old-id', 'KSE100', '2024-01-05', 1.0, 1.0, 1.0, 1.0, 1, FALSE, "
        "'old-hash', TRUE, ?, NULL)",
        [run_started_at - timedelta(hours=1)],
    )
    rows_df = pd.DataFrame({
        "symbol": ["KSE100"],
        "date": [pd.Timestamp("2024-01-05")],
        "open": [2.0], "high": [2.0], "low": [2.0], "close": [2.0],
        "volume": [2], "is_anomaly": [False], "row_hash": ["new-hash"],
    })

    assert fetch_latest_hashes(
        conn, _cfg(), "KSE100", table=INDEX_PRICE_HISTORY_TABLE
    ) == {("KSE100", "2024-01-05"): "old-hash"}
    load_stock_history_rows(conn, _cfg(), rows_df, table=INDEX_PRICE_HISTORY_TABLE)
    supersede_stock_history_keys(
        conn, _cfg(), [("KSE100", "2024-01-05")], run_started_at,
        table=INDEX_PRICE_HISTORY_TABLE,
    )

    assert fetch_latest_hashes(
        conn, _cfg(), "KSE100", table=INDEX_PRICE_HISTORY_TABLE
    ) == {("KSE100", "2024-01-05"): "new-hash"}
    stock_count = conn.execute(f"SELECT COUNT(*) FROM {STOCK_HISTORY_TABLE}").fetchone()[0]
    assert stock_count == 0
