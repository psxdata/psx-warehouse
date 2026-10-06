# Contributing

## Adding a new raw-storage backend

The extraction pipeline (`extract/`) writes to a "raw storage" backend —
today that's BigQuery (`extract/bigquery_io.py`, production) and MotherDuck
(`extract/motherduck_io.py`, local development). Both are plain modules of
top-level functions, not classes, and both satisfy the same structural
interface: `extract/storage.py`'s `RawStorage` `Protocol`.

A `Protocol` here instead of an abstract base class because a backend module
is a flat module, not an instance of anything — Python's structural typing
lets a module satisfy a `Protocol` just by having functions with matching
names and signatures, no inheritance required. This also means the two
existing backends share zero runtime code with each other; each is free to
use whatever library and SQL dialect makes sense for its store.

### The shape every backend has

1. **A config dataclass and a `load_config()` function**, reading that
   backend's own environment variables. Each backend owns its config type
   completely — `extract/config.py` (the backend-agnostic top-level config)
   never imports it, and no backend reaches into another backend's env vars.
   Compare `BigQueryConfig`/`bigquery_io.load_config()` against
   `MotherDuckConfig`/`motherduck_io.load_config()`: same pattern, entirely
   different fields.
2. **The ten `RawStorage` functions** (`get_client`, `ensure_dataset`,
   `fetch_latest_hashes`, `load_stock_history_rows`,
   `supersede_stock_history_keys`, `load_index_constituents`,
   `fetch_latest_symbol_hashes`, `load_symbols_rows`, `supersede_symbol_keys`,
   `load_sectors_rows`, `load_screener_rows`) — see
   `extract/example_backend.py` for what each one is actually responsible
   for; its docstrings are the real spec, not repeated here. The three
   `*stock_history*` functions also take a `table=` argument: the same code
   writes `index_price_history` (index-level OHLCV, same schema).
3. **Registration in `extract/main.py`'s `_get_storage()`** — the *only*
   place in `extract/` that knows which backend names exist. Add one `if
   backend == "yourbackend": return yourbackend_io` line; nothing else in
   `main.py` or `config.py` changes.

### Two patterns worth copying deliberately

- **Append-only, hash-diffed writes for anything with real history**
  (`stock_history`, `index_price_history`, `symbols`): never `UPDATE`/`DELETE` a row in place.
  Restatements are a real signal worth keeping, not noise to overwrite —
  see `DECISIONS.md` ("Raw layer & data modeling") for the incident that
  established this. `fetch_latest_*_hashes` + `load_*_rows` +
  `supersede_*_keys` together implement this: fetch what's currently
  marked latest, diff the freshly-scraped data against it (`extract/diff.py`
  is backend-agnostic and does this diffing), append only what's new or
  changed, then flip the old rows' "latest" marker off.
- **The supersede ordering bug already found once — don't reintroduce it.**
  `supersede_stock_history_keys`/`supersede_symbol_keys` take a
  `run_started_at` timestamp, captured *before* this run's own writes, and
  must filter on `loaded_at < run_started_at`, never a bare "is currently
  latest" match. Get this wrong and a changed key flips both the old row
  *and* the row this same run just inserted — the next run permanently
  loses the current value. This was a real Critical bug caught by review in
  this project's own history (see `DECISIONS.md`, 2026-08-31); it's exactly
  the kind of thing that looks correct in isolation and only breaks on the
  second run against real changing data.

### Testing convention

`motherduck_io.py`'s own tests run against a local temporary DuckDB file, not
live MotherDuck — same engine and SQL dialect, no network dependency, no
`MOTHERDUCK_TOKEN` needed in CI. If your new backend's engine has an
embeddable/local mode, prefer that same shortcut. If it doesn't (a real
network-only service), that's a real difference worth calling out in your
PR rather than working around — see `DECISIONS.md`'s note that this
shortcut is "DuckDB-family-specific," not assumed to generalize.

### Starting point

Copy `extract/example_backend.py`, rename it, and work through its
functions top to bottom — every one raises `NotImplementedError` with a
`TODO` and a docstring explaining what it needs to do. It's kept
mypy/ruff-clean in CI even though nothing in it is implemented, so you can
check your work incrementally as you fill it in rather than only at the end.
