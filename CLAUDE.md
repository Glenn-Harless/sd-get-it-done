# SD Get It Done — 311 Service Request Dashboard

## Project Overview
San Diego Get It Done 311 service request analysis. ~3M event-level transactional records (lat/lng, timestamps, resolution tracking) from the city's open data portal, covering May 2016 to present.

## Architecture

### Project Structure
```
pipeline/       # Data ingestion (API) + transformation (DuckDB)
data/raw/       # Raw API responses (gitignored)
data/processed/ # requests.parquet — full dataset (~94MB, committed to git)
data/aggregated/# Pre-aggregated parquets for map, trends, etc.
dashboard/      # Streamlit app with DuckDB queries
```

### Dashboard (Streamlit Cloud)
- **DuckDB, not Polars** — Streamlit Cloud free tier has 1GB RAM. Polars loads the full parquet into memory (~1GB). DuckDB queries lazily with column pruning + predicate pushdown, keeping peak RAM ~50-100MB.
- `query()` helper: fresh `duckdb.connect()` per call, returns pandas DataFrame. Thread-safe, ~1ms overhead, OS file cache keeps parquet hot.
- `_where_clause()`: builds shared SQL WHERE from sidebar filters, used across all tab queries.
- Each tab runs 1-3 small SQL queries returning ~10-30 row DataFrames. No full dataset ever in memory.
- Map tab: `ORDER BY RANDOM() LIMIT 200000` for sampling.

### Pipeline
- `pipeline/ingest.py` — fetches from SD open data API via httpx
- `pipeline/transform.py` — DuckDB transforms
- `pipeline/build.py` — orchestrates ingest + transform
- Entry point: `uv run python -m pipeline.build` or `gid-build`

### Deployment
- `requirements.txt` for Streamlit Cloud (reads this, not pyproject.toml)
- GitHub Actions monthly refresh (1st of month), commits both `data/processed/requests.parquet` and `data/aggregated/`
- Parquet file is under GitHub's 100MB limit, committed directly

## Gotchas
- `.gitignore` negation: use `dir/*` (not `dir/`) when you need `!dir/file` exceptions. The directory-level ignore blocks all negation patterns for files inside.
- DuckDB `MEDIAN()` works on integer columns — no cast needed for `resolution_days`.
- `request_dow` is 0=Sun through 6=Sat.
- `date_requested` is TIMESTAMP — use `HOUR(date_requested)` for hour extraction.
