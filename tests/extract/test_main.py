"""Unit tests for extract.main orchestration. psxdata is mocked at the
module level; run()-level tests inject a mock storage object directly
rather than patching a specific backend module — no live PSX or GCP
access."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from psxdata.exceptions import PSXConnectionError

from extract import bigquery_io, config, motherduck_io
from extract.diff import add_row_hashes
from extract.main import (
    ExtractionFailed,
    _extract_index_prices,
    _get_storage,
    main,
    run,
)

INDEX_TABLE = "index_price_history"


@pytest.fixture(autouse=True)
def _stub_index_prices(request: pytest.FixtureRequest):
    """run()-level tests stub psxdata.stocks per ticker; keep the index-price
    step (which also calls psxdata.stocks) out of their way. Tests of the
    step itself call the real _extract_index_prices imported above."""
    with patch("extract.main._extract_index_prices") as stub:
        yield stub


def _cfg() -> config.Config:
    return config.Config(backend="bigquery", index_names=("KSE100",))


def _constituents_df() -> pd.DataFrame:
    return pd.DataFrame({"symbol": ["ENGRO", "LUCK"], "idx_weight": [5.0, 3.0]})


def _history_df(close: float) -> pd.DataFrame:
    return pd.DataFrame({
        "date": [pd.Timestamp("2024-01-05")],
        "open": [100.0], "high": [105.0], "low": [99.0], "close": [close],
        "volume": [1000], "is_anomaly": [False],
    })


@patch("extract.main.psxdata")
def test_run_writes_batched_changes_once(mock_psxdata: MagicMock) -> None:
    mock_psxdata.indices.return_value = _constituents_df()
    mock_psxdata.stocks.side_effect = [_history_df(101.0), _history_df(102.0)]
    mock_storage = MagicMock()
    mock_storage.fetch_latest_hashes.return_value = {}

    run(_cfg(), mock_storage, MagicMock())

    mock_storage.load_index_constituents.assert_called_once()
    mock_storage.load_stock_history_rows.assert_called_once()
    written_df = mock_storage.load_stock_history_rows.call_args[0][2]
    assert len(written_df) == 2
    assert sorted(written_df["symbol"].tolist()) == ["ENGRO", "LUCK"]
    mock_storage.supersede_stock_history_keys.assert_called_once()


@patch("extract.main.psxdata")
def test_run_skips_ticker_on_fetch_failure_and_continues(
    mock_psxdata: MagicMock,
) -> None:
    mock_psxdata.indices.return_value = _constituents_df()
    mock_psxdata.stocks.side_effect = [PSXConnectionError("down"), _history_df(102.0)]
    mock_storage = MagicMock()
    mock_storage.fetch_latest_hashes.return_value = {}

    run(_cfg(), mock_storage, MagicMock())

    written_df = mock_storage.load_stock_history_rows.call_args[0][2]
    assert len(written_df) == 1
    assert written_df["symbol"].iloc[0] == "LUCK"


@patch("extract.main.psxdata")
def test_run_raises_when_constituents_fetch_fails(
    mock_psxdata: MagicMock,
) -> None:
    mock_psxdata.indices.side_effect = PSXConnectionError("down")
    mock_storage = MagicMock()

    with pytest.raises(ExtractionFailed, match="constituents"):
        run(_cfg(), mock_storage, MagicMock())

    mock_storage.load_stock_history_rows.assert_not_called()


@patch("extract.main.psxdata")
def test_run_raises_when_constituents_empty(
    mock_psxdata: MagicMock,
) -> None:
    mock_psxdata.indices.return_value = pd.DataFrame()

    with pytest.raises(ExtractionFailed, match="no constituents"):
        run(_cfg(), MagicMock(), MagicMock())


@patch("extract.main.psxdata")
def test_run_raises_when_zero_rows_fetched_across_all_tickers(
    mock_psxdata: MagicMock,
) -> None:
    mock_psxdata.indices.return_value = _constituents_df()
    mock_psxdata.stocks.return_value = pd.DataFrame()
    mock_storage = MagicMock()
    mock_storage.fetch_latest_hashes.return_value = {}

    with pytest.raises(ExtractionFailed, match="Zero OHLCV"):
        run(_cfg(), mock_storage, MagicMock())


@patch("extract.main.psxdata")
def test_run_writes_nothing_when_no_changes_detected(
    mock_psxdata: MagicMock,
) -> None:
    mock_psxdata.indices.return_value = _constituents_df()
    fresh = _history_df(101.0)
    mock_psxdata.stocks.return_value = fresh
    from extract.diff import add_row_hashes

    # fetch_latest_hashes is scoped per-symbol (bigquery_io.py), so the mock
    # must key its response on the symbol argument rather than return one
    # fixed dict — otherwise a run with >1 ticker can't express "no changes
    # detected for any ticker" (only the first ticker's hash would match).
    existing_hashes = {
        symbol: add_row_hashes(fresh.assign(symbol=symbol))["row_hash"].iloc[0]
        for symbol in ("ENGRO", "LUCK")
    }
    mock_storage = MagicMock()
    mock_storage.fetch_latest_hashes.side_effect = (
        lambda client, backend_cfg, symbol: {
            (symbol, "2024-01-05"): existing_hashes[symbol]
        }
    )

    run(_cfg(), mock_storage, MagicMock())

    mock_storage.load_stock_history_rows.assert_not_called()
    mock_storage.supersede_stock_history_keys.assert_not_called()


@patch("extract.main.psxdata")
def test_run_writes_changed_row_and_supersedes_its_key(
    mock_psxdata: MagicMock,
) -> None:
    """Regression guard for Finding 1: a CHANGED (not new) row must be both
    inserted (load_stock_history_rows) and superseded (supersede_stock_
    history_keys with that key present) — the exact path where the old fix
    superseded the just-inserted row instead of only the prior one."""
    mock_psxdata.indices.return_value = pd.DataFrame({"symbol": ["ENGRO"], "idx_weight": [5.0]})
    fresh = _history_df(101.0)
    mock_psxdata.stocks.return_value = fresh
    mock_storage = MagicMock()
    mock_storage.fetch_latest_hashes.return_value = {
        ("ENGRO", "2024-01-05"): "stale-hash-that-does-not-match"
    }

    run(_cfg(), mock_storage, MagicMock())

    mock_storage.load_stock_history_rows.assert_called_once()
    written_df = mock_storage.load_stock_history_rows.call_args[0][2]
    assert len(written_df) == 1
    assert written_df["symbol"].iloc[0] == "ENGRO"

    mock_storage.supersede_stock_history_keys.assert_called_once()
    call_args = mock_storage.supersede_stock_history_keys.call_args[0]
    superseded_keys = call_args[2]
    assert ("ENGRO", "2024-01-05") in superseded_keys
    run_started_at = call_args[3]
    assert run_started_at is not None


@patch("extract.main.psxdata")
def test_run_skips_ticker_with_malformed_row_and_continues(
    mock_psxdata: MagicMock,
) -> None:
    """Finding 3: a malformed OHLCV row (None open price) raises TypeError
    inside compute_row_hash, which is not a PSXDataError. The run must not
    abort — it should skip that ticker and still write the other valid
    ticker's data."""
    mock_psxdata.indices.return_value = _constituents_df()
    malformed_df = pd.DataFrame({
        "date": [pd.Timestamp("2024-01-05")],
        "open": [None], "high": [105.0], "low": [99.0], "close": [101.0],
        "volume": [1000], "is_anomaly": [False],
    })
    mock_psxdata.stocks.side_effect = [malformed_df, _history_df(102.0)]
    mock_storage = MagicMock()
    mock_storage.fetch_latest_hashes.return_value = {}

    run(_cfg(), mock_storage, MagicMock())

    mock_storage.load_stock_history_rows.assert_called_once()
    written_df = mock_storage.load_stock_history_rows.call_args[0][2]
    assert len(written_df) == 1
    assert written_df["symbol"].iloc[0] == "LUCK"


def test_get_storage_selects_bigquery() -> None:
    assert _get_storage("bigquery") is bigquery_io


def test_get_storage_selects_motherduck() -> None:
    assert _get_storage("motherduck") is motherduck_io


def test_get_storage_raises_on_unknown_backend() -> None:
    with pytest.raises(ExtractionFailed, match="snowflake"):
        _get_storage("snowflake")


@patch("extract.main.config.load_config", side_effect=config.ConfigError("GCP_PROJECT missing"))
def test_main_returns_1_on_config_error(mock_load_config: MagicMock) -> None:
    assert main() == 1


@patch("extract.main.bigquery_io")
@patch(
    "extract.main.run",
    side_effect=ExtractionFailed("Zero OHLCV rows fetched across all tickers"),
)
@patch("extract.main.config.load_config", return_value=_cfg())
def test_main_returns_1_on_extraction_failed(
    mock_load_config: MagicMock, mock_run: MagicMock, mock_bigquery_io: MagicMock
) -> None:
    assert main() == 1


@patch("extract.main.bigquery_io")
@patch("extract.main.run")
@patch("extract.main.config.load_config", return_value=_cfg())
def test_main_returns_0_on_success(
    mock_load_config: MagicMock, mock_run: MagicMock, mock_bigquery_io: MagicMock
) -> None:
    assert main() == 0


@patch("extract.main.psxdata")
def test_run_loads_new_symbols_and_supersedes_delisted(mock_psxdata: MagicMock) -> None:
    mock_psxdata.indices.return_value = _constituents_df()
    mock_psxdata.stocks.return_value = _history_df(101.0)
    mock_psxdata.symbols.return_value = pd.DataFrame([{
        "symbol": "ENGRO", "name": "Engro Corporation", "sector_name": "Chemical",
        "is_etf": False, "is_debt": False, "is_gem": False,
    }])
    mock_psxdata.eligible_scrips.return_value = {
        "table_0": pd.DataFrame([{"symbol": "ENGRO", "name": "Engro Corporation"}])
    }
    mock_psxdata.sectors.return_value = pd.DataFrame()
    mock_psxdata.screener.return_value = pd.DataFrame()
    mock_storage = MagicMock()
    mock_storage.fetch_latest_hashes.return_value = {}
    mock_storage.fetch_latest_symbol_hashes.return_value = {"DELISTEDCO": "old-hash"}

    run(_cfg(), mock_storage, MagicMock())

    mock_storage.load_symbols_rows.assert_called_once()
    loaded_df = mock_storage.load_symbols_rows.call_args[0][2]
    assert loaded_df.iloc[0]["is_margin_eligible"] == True  # noqa: E712

    mock_storage.supersede_symbol_keys.assert_called_once()
    superseded_keys = mock_storage.supersede_symbol_keys.call_args[0][2]
    assert "DELISTEDCO" in superseded_keys


@patch("extract.main.psxdata")
def test_run_continues_when_symbols_fetch_fails(mock_psxdata: MagicMock) -> None:
    mock_psxdata.indices.return_value = _constituents_df()
    mock_psxdata.stocks.return_value = _history_df(101.0)
    mock_psxdata.symbols.side_effect = PSXConnectionError("down")
    mock_psxdata.sectors.return_value = pd.DataFrame()
    mock_psxdata.screener.return_value = pd.DataFrame()
    mock_storage = MagicMock()
    mock_storage.fetch_latest_hashes.return_value = {}

    run(_cfg(), mock_storage, MagicMock())  # must not raise

    mock_storage.load_symbols_rows.assert_not_called()
    mock_storage.load_stock_history_rows.assert_called_once()


@patch("extract.main.psxdata")
def test_run_continues_when_symbols_payload_is_malformed(mock_psxdata: MagicMock) -> None:
    """Everything after the symbols() fetch (isin filter, row hashing) must
    be guarded too -- a malformed payload (missing columns) should be
    skipped, not crash the whole run before the OHLCV loop even starts."""
    mock_psxdata.indices.return_value = _constituents_df()
    mock_psxdata.stocks.return_value = _history_df(101.0)
    mock_psxdata.symbols.return_value = pd.DataFrame([{"symbol": "ENGRO"}])
    mock_psxdata.eligible_scrips.return_value = {}
    mock_psxdata.sectors.return_value = pd.DataFrame()
    mock_psxdata.screener.return_value = pd.DataFrame()
    mock_storage = MagicMock()
    mock_storage.fetch_latest_hashes.return_value = {}

    run(_cfg(), mock_storage, MagicMock())  # must not raise

    mock_storage.load_symbols_rows.assert_not_called()
    mock_storage.load_stock_history_rows.assert_called_once()


@patch("extract.main.psxdata")
def test_run_continues_when_eligible_scrips_fetch_fails(mock_psxdata: MagicMock) -> None:
    """Regression guard from independent review: eligible_scrips() failing
    must not crash the whole run -- symbols() itself succeeded, so
    raw.symbols should still get written, just with is_margin_eligible
    defaulting to False for everyone this run."""
    mock_psxdata.indices.return_value = _constituents_df()
    mock_psxdata.stocks.return_value = _history_df(101.0)
    mock_psxdata.symbols.return_value = pd.DataFrame([{
        "symbol": "ENGRO", "name": "Engro Corporation", "sector_name": "Chemical",
        "is_etf": False, "is_debt": False, "is_gem": False,
    }])
    mock_psxdata.eligible_scrips.side_effect = PSXConnectionError("down")
    mock_psxdata.sectors.return_value = pd.DataFrame()
    mock_psxdata.screener.return_value = pd.DataFrame()
    mock_storage = MagicMock()
    mock_storage.fetch_latest_hashes.return_value = {}
    mock_storage.fetch_latest_symbol_hashes.return_value = {}

    run(_cfg(), mock_storage, MagicMock())  # must not raise

    mock_storage.load_symbols_rows.assert_called_once()
    loaded_df = mock_storage.load_symbols_rows.call_args[0][2]
    assert loaded_df.iloc[0]["is_margin_eligible"] == False  # noqa: E712


@patch("extract.main.psxdata")
def test_run_loads_sectors_and_screener(mock_psxdata: MagicMock) -> None:
    mock_psxdata.indices.return_value = _constituents_df()
    mock_psxdata.stocks.return_value = _history_df(101.0)
    mock_psxdata.symbols.return_value = pd.DataFrame()
    mock_psxdata.eligible_scrips.return_value = {}
    mock_psxdata.sectors.return_value = pd.DataFrame(
        [{"sector_code": "14", "sector_name": "Chemical"}]
    )
    mock_psxdata.screener.return_value = pd.DataFrame([{"symbol": "ENGRO", "price": 300.5}])
    mock_storage = MagicMock()
    mock_storage.fetch_latest_hashes.return_value = {}

    run(_cfg(), mock_storage, MagicMock())

    mock_storage.load_sectors_rows.assert_called_once()
    mock_storage.load_screener_rows.assert_called_once()


@patch("extract.main.psxdata")
def test_run_continues_when_sectors_fetch_fails(mock_psxdata: MagicMock) -> None:
    mock_psxdata.indices.return_value = _constituents_df()
    mock_psxdata.stocks.return_value = _history_df(101.0)
    mock_psxdata.symbols.return_value = pd.DataFrame()
    mock_psxdata.eligible_scrips.return_value = {}
    mock_psxdata.sectors.side_effect = PSXConnectionError("down")
    mock_psxdata.screener.return_value = pd.DataFrame()
    mock_storage = MagicMock()
    mock_storage.fetch_latest_hashes.return_value = {}

    run(_cfg(), mock_storage, MagicMock())  # must not raise

    mock_storage.load_sectors_rows.assert_not_called()
    mock_storage.load_stock_history_rows.assert_called_once()


@patch("extract.main.psxdata")
def test_run_continues_when_sectors_payload_is_malformed(mock_psxdata: MagicMock) -> None:
    """A sectors() payload missing a required column (e.g. sector_code)
    raises KeyError at the reindex inside load_sectors_rows -- that must be
    skipped, not crash the whole run before the OHLCV loop even starts."""
    mock_psxdata.indices.return_value = _constituents_df()
    mock_psxdata.stocks.return_value = _history_df(101.0)
    mock_psxdata.symbols.return_value = pd.DataFrame()
    mock_psxdata.eligible_scrips.return_value = {}
    mock_psxdata.sectors.return_value = pd.DataFrame(
        [{"sector_code": "14", "sector_name": "Chemical"}]
    )
    mock_psxdata.screener.return_value = pd.DataFrame([{"symbol": "ENGRO", "price": 300.5}])
    mock_storage = MagicMock()
    mock_storage.fetch_latest_hashes.return_value = {}
    mock_storage.load_sectors_rows.side_effect = KeyError("sector_code")

    run(_cfg(), mock_storage, MagicMock())  # must not raise

    mock_storage.load_sectors_rows.assert_called_once()
    mock_storage.load_screener_rows.assert_called_once()
    mock_storage.load_stock_history_rows.assert_called_once()


@patch("extract.main.psxdata")
def test_run_continues_when_screener_payload_is_malformed(mock_psxdata: MagicMock) -> None:
    """A screener() payload missing a required column (e.g. symbol) raises
    KeyError at the reindex inside load_screener_rows -- that must be
    skipped, not crash the whole run before the OHLCV loop even starts."""
    mock_psxdata.indices.return_value = _constituents_df()
    mock_psxdata.stocks.return_value = _history_df(101.0)
    mock_psxdata.symbols.return_value = pd.DataFrame()
    mock_psxdata.eligible_scrips.return_value = {}
    mock_psxdata.sectors.return_value = pd.DataFrame(
        [{"sector_code": "14", "sector_name": "Chemical"}]
    )
    mock_psxdata.screener.return_value = pd.DataFrame([{"symbol": "ENGRO", "price": 300.5}])
    mock_storage = MagicMock()
    mock_storage.fetch_latest_hashes.return_value = {}
    mock_storage.load_screener_rows.side_effect = KeyError("symbol")

    run(_cfg(), mock_storage, MagicMock())  # must not raise

    mock_storage.load_screener_rows.assert_called_once()
    mock_storage.load_stock_history_rows.assert_called_once()


def _index_df() -> pd.DataFrame:
    return pd.DataFrame({
        "date": [pd.Timestamp("2024-01-04"), pd.Timestamp("2024-01-05")],
        "open": [60000.0, 60100.0], "high": [60500.0, 60600.0],
        "low": [59900.0, 60000.0], "close": [60100.0, 60400.0],
        "volume": [1000, 2000], "is_anomaly": [False, False],
    })


@patch("extract.main.psxdata")
def test_run_extracts_index_prices(
    mock_psxdata: MagicMock, _stub_index_prices: MagicMock
) -> None:
    mock_psxdata.indices.return_value = _constituents_df()
    mock_psxdata.stocks.return_value = _history_df(101.0)
    storage = MagicMock()
    storage.fetch_latest_hashes.return_value = {}

    run(_cfg(), storage, MagicMock())

    _stub_index_prices.assert_called_once()
    assert _stub_index_prices.call_args[0][0] == ("KSE100",)


@patch("extract.main.psxdata")
def test_extract_index_prices_loads_new_rows_to_index_table(
    mock_psxdata: MagicMock,
) -> None:
    mock_psxdata.stocks.return_value = _index_df()
    storage = MagicMock()
    storage.fetch_latest_hashes.return_value = {}

    _extract_index_prices(("KSE100",), storage, MagicMock(), MagicMock(), MagicMock())

    mock_psxdata.stocks.assert_called_once_with("KSE100", cache=False)
    assert storage.fetch_latest_hashes.call_args[1]["table"] == INDEX_TABLE
    written = storage.load_stock_history_rows.call_args[0][2]
    assert storage.load_stock_history_rows.call_args[1]["table"] == INDEX_TABLE
    assert written["symbol"].tolist() == ["KSE100", "KSE100"]
    assert written["row_hash"].notna().all()


@patch("extract.main.psxdata")
def test_extract_index_prices_drops_exact_duplicate_rows(
    mock_psxdata: MagicMock,
) -> None:
    df = _index_df()
    mock_psxdata.stocks.return_value = pd.concat([df, df.iloc[[1]]], ignore_index=True)
    storage = MagicMock()
    storage.fetch_latest_hashes.return_value = {}

    _extract_index_prices(("KSE100",), storage, MagicMock(), MagicMock(), MagicMock())

    written = storage.load_stock_history_rows.call_args[0][2]
    assert len(written) == 2


@patch("extract.main.psxdata")
def test_extract_index_prices_supersedes_changed_row(mock_psxdata: MagicMock) -> None:
    df = _index_df()
    mock_psxdata.stocks.return_value = df
    hashed = add_row_hashes(df.assign(symbol="KSE100"))
    storage = MagicMock()
    storage.fetch_latest_hashes.return_value = {
        ("KSE100", "2024-01-04"): hashed["row_hash"].iloc[0],
        ("KSE100", "2024-01-05"): "stale-hash",
    }
    run_started_at = MagicMock()

    _extract_index_prices(("KSE100",), storage, MagicMock(), MagicMock(), run_started_at)

    written = storage.load_stock_history_rows.call_args[0][2]
    assert len(written) == 1
    keys, started = storage.supersede_stock_history_keys.call_args[0][2:4]
    assert keys == [("KSE100", "2024-01-05")]
    assert started is run_started_at
    assert storage.supersede_stock_history_keys.call_args[1]["table"] == INDEX_TABLE


@patch("extract.main.psxdata")
def test_extract_index_prices_writes_nothing_when_unchanged(
    mock_psxdata: MagicMock,
) -> None:
    df = _index_df()
    mock_psxdata.stocks.return_value = df
    hashed = add_row_hashes(df.assign(symbol="KSE100"))
    storage = MagicMock()
    storage.fetch_latest_hashes.return_value = {
        ("KSE100", d.strftime("%Y-%m-%d")): h
        for d, h in zip(hashed["date"], hashed["row_hash"])
    }

    _extract_index_prices(("KSE100",), storage, MagicMock(), MagicMock(), MagicMock())

    storage.load_stock_history_rows.assert_not_called()
    storage.supersede_stock_history_keys.assert_not_called()


@patch("extract.main.psxdata")
def test_extract_index_prices_warns_and_continues_on_fetch_failure(
    mock_psxdata: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    mock_psxdata.stocks.side_effect = [PSXConnectionError("down"), _index_df()]
    storage = MagicMock()
    storage.fetch_latest_hashes.return_value = {}

    _extract_index_prices(
        ("KSE100", "KMI30"), storage, MagicMock(), MagicMock(), MagicMock()
    )

    assert "KSE100" in caplog.text
    written = storage.load_stock_history_rows.call_args[0][2]
    assert written["symbol"].unique().tolist() == ["KMI30"]


@patch("extract.main.psxdata")
def test_extract_index_prices_skips_empty_and_malformed(
    mock_psxdata: MagicMock,
) -> None:
    malformed = _index_df().drop(columns=["close"])
    mock_psxdata.stocks.side_effect = [pd.DataFrame(), malformed]
    storage = MagicMock()

    _extract_index_prices(
        ("KSE100", "KMI30"), storage, MagicMock(), MagicMock(), MagicMock()
    )

    storage.load_stock_history_rows.assert_not_called()
