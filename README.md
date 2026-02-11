# San Diego "Get It Done" 311 Service Request Analysis

## Overview
Analyze every non-emergency problem report submitted by San Diego residents since May 2016 via the Get It Done app/web portal. Explore response times, geographic patterns, equity gaps, and seasonal trends across problem types like potholes, graffiti, illegal dumping, and streetlight outages.

**Live Dashboard**: https://sd-get-it-done.streamlit.app/

## Key Questions
- Which neighborhoods wait longest for issue resolution?
- Do wealthier zip codes get faster service?
- What are the seasonal patterns for different problem types?
- Which problem categories are chronically backlogged?

## Quick Start

```bash
# Install dependencies (requires uv: https://docs.astral.sh/uv/)
uv sync

# Run the full pipeline (download + transform + aggregate)
uv run python -m pipeline.build

# Launch the dashboard
uv run streamlit run dashboard/app.py
```

The first run downloads ~1GB of CSV data from data.sandiego.gov, so give it a few minutes.

## Project Structure

```
pipeline/
  ingest.py       # Download CSVs from data.sandiego.gov
  transform.py    # Clean, enrich, load into DuckDB, export aggregations
  build.py        # Orchestrator: ingest -> transform
dashboard/
  app.py          # Streamlit dashboard (overview, map, trends, equity)
data/
  raw/            # Downloaded CSVs (gitignored)
  processed/      # Cleaned Parquet (gitignored)
  aggregated/     # Pre-computed aggregations for dashboard (gitignored)
db/
  get_it_done.duckdb  # DuckDB analytical database (gitignored)
```

## Data Source
- **Get It Done 311 Reports**: https://data.sandiego.gov/datasets/get-it-done-311/
  - Format: CSV (open requests + closed by year, 2016-2026)
  - Fields: submission date/time, problem type, location (lat/lng), resolution date, status, neighborhood, council district
  - Coverage: May 2016 - present, updated daily

## Tech Stack
- **Storage**: DuckDB + Parquet
- **Transform**: DuckDB SQL
- **Dashboard**: Streamlit + pydeck
- **Automation**: GitHub Actions daily cron
