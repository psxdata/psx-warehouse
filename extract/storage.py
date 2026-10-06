"""Structural interface both raw-layer storage backends (bigquery_io,
motherduck_io) satisfy. A Protocol, not an ABC, because each backend module
is a flat module of top-level functions, not a class — a module object
satisfies a Protocol structurally as long as its top-level functions match
the Protocol's method signatures, with no inheritance needed.

Config is typed loosely here (Any) since each backend owns its own concrete
config type (BigQueryConfig, MotherDuckConfig, ...) — this Protocol's job is
catching drift in the six functions' names and call shape, not enforcing a
shared config shape across fundamentally different backends.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Protocol

import pandas as pd


class RawStorage(Protocol):
    def get_client(self, cfg: Any) -> Any: ...

    def ensure_dataset(self, client: Any, cfg: Any) -> None: ...

    def fetch_latest_hashes(
        self, client: Any, cfg: Any, symbol: str, table: str = ...
    ) -> dict[tuple[str, str], str]: ...

    def load_stock_history_rows(
        self, client: Any, cfg: Any, rows_df: pd.DataFrame, table: str = ...
    ) -> None: ...

    def supersede_stock_history_keys(
        self,
        client: Any,
        cfg: Any,
        keys: list[tuple[str, str]],
        run_started_at: datetime,
        table: str = ...,
    ) -> None: ...

    def load_index_constituents(
        self,
        client: Any,
        cfg: Any,
        df: pd.DataFrame,
        index_name: str,
        snapshot_date: date,
    ) -> None: ...

    def fetch_latest_symbol_hashes(self, client: Any, cfg: Any) -> dict[str, str]: ...

    def load_symbols_rows(self, client: Any, cfg: Any, rows_df: pd.DataFrame) -> None: ...

    def supersede_symbol_keys(
        self,
        client: Any,
        cfg: Any,
        keys: list[str],
        run_started_at: datetime,
    ) -> None: ...

    def load_sectors_rows(
        self, client: Any, cfg: Any, df: pd.DataFrame, snapshot_date: date
    ) -> None: ...

    def load_screener_rows(
        self, client: Any, cfg: Any, df: pd.DataFrame, snapshot_date: date
    ) -> None: ...


if TYPE_CHECKING:
    # Static-only conformance checks: if either backend's function shapes
    # ever drift from RawStorage, mypy fails these assignments. Not a
    # runtime import — TYPE_CHECKING is always False at import time.
    from extract import bigquery_io as _bigquery_io_check
    from extract import motherduck_io as _motherduck_io_check

    _verify_bigquery_io: RawStorage = _bigquery_io_check
    _verify_motherduck_io: RawStorage = _motherduck_io_check
