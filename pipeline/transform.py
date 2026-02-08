"""Clean, enrich, and aggregate Get It Done 311 data using DuckDB."""

from __future__ import annotations

from pathlib import Path

import duckdb

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"
AGGREGATED_DIR = Path(__file__).resolve().parent.parent / "data" / "aggregated"
DB_PATH = Path(__file__).resolve().parent.parent / "db" / "get_it_done.duckdb"

# Column names from the data dictionary
COLUMNS = [
    "service_request_id",
    "service_request_parent_id",
    "sap_notification_number",
    "date_requested",
    "case_age_days",
    "case_record_type",
    "service_name",
    "service_name_detail",
    "date_closed",
    "status",
    "lat",
    "lng",
    "street_address",
    "zipcode",
    "council_district",
    "comm_plan_code",
    "comm_plan_name",
    "park_name",
    "case_origin",
    "referred",
    "iamfloc",
    "floc",
    "public_description",
]


def transform(*, db_path: Path | None = None) -> None:
    """Load raw CSVs, clean, compute derived fields, export Parquet."""
    db = db_path or DB_PATH
    db.parent.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    AGGREGATED_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db))

    # ── Load all raw CSVs into a single table ──
    csv_files = sorted(RAW_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {RAW_DIR}")

    csv_list = ", ".join(f"'{f}'" for f in csv_files)
    print(f"Loading {len(csv_files)} CSV files ...")

    con.execute("DROP TABLE IF EXISTS raw_requests")
    con.execute(f"""
        CREATE TABLE raw_requests AS
        SELECT * FROM read_csv(
            [{csv_list}],
            header = true,
            ignore_errors = true,
            filename = true,
            union_by_name = true
        )
    """)

    row_count = con.execute("SELECT count(*) FROM raw_requests").fetchone()[0]
    print(f"  Loaded {row_count:,} rows")

    # ── Clean & enrich ──
    con.execute("DROP TABLE IF EXISTS requests")
    con.execute("""
        CREATE TABLE requests AS
        SELECT
            service_request_id,
            service_request_parent_id,
            sap_notification_number,

            -- Parse dates
            TRY_CAST(date_requested AS TIMESTAMP) AS date_requested,
            TRY_CAST(date_closed AS TIMESTAMP)    AS date_closed,

            TRY_CAST(case_age_days AS INTEGER)     AS case_age_days,
            case_record_type,
            service_name,
            service_name_detail,
            status,

            -- Location
            TRY_CAST(lat AS DOUBLE) AS lat,
            TRY_CAST(lng AS DOUBLE) AS lng,
            street_address,
            zipcode,
            TRY_CAST(council_district AS INTEGER) AS council_district,
            TRY_CAST(comm_plan_code AS INTEGER)   AS comm_plan_code,
            comm_plan_name,
            park_name,

            case_origin,
            referred,

            -- Derived fields
            CASE
                WHEN TRY_CAST(date_closed AS TIMESTAMP) IS NOT NULL
                     AND TRY_CAST(date_requested AS TIMESTAMP) IS NOT NULL
                THEN DATE_DIFF(
                    'day',
                    TRY_CAST(date_requested AS TIMESTAMP),
                    TRY_CAST(date_closed AS TIMESTAMP)
                )
            END AS resolution_days,

            YEAR(TRY_CAST(date_requested AS TIMESTAMP))    AS request_year,
            MONTH(TRY_CAST(date_requested AS TIMESTAMP))   AS request_month,
            QUARTER(TRY_CAST(date_requested AS TIMESTAMP)) AS request_quarter,
            DAYOFWEEK(TRY_CAST(date_requested AS TIMESTAMP)) AS request_dow,
            DATE_TRUNC('month', TRY_CAST(date_requested AS TIMESTAMP)) AS request_month_start,

            -- Source file tracking
            filename AS source_file

        FROM raw_requests
        WHERE TRY_CAST(date_requested AS TIMESTAMP) IS NOT NULL
    """)

    clean_count = con.execute("SELECT count(*) FROM requests").fetchone()[0]
    print(f"  Cleaned: {clean_count:,} rows (dropped {row_count - clean_count:,} unparseable)")

    # ── Export cleaned data as Parquet ──
    processed_path = PROCESSED_DIR / "requests.parquet"
    con.execute(f"COPY requests TO '{processed_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    print(f"  Exported processed data -> {processed_path}")

    # ── Pre-compute aggregations ──
    _build_aggregations(con)

    con.close()
    print("Transform complete.")


def _build_aggregations(con: duckdb.DuckDBPyConnection) -> None:
    """Build pre-computed aggregation Parquet files for the dashboard."""

    # 1) Response time by neighborhood (community plan)
    con.execute(f"""
        COPY (
            SELECT
                comm_plan_name,
                council_district,
                COUNT(*)                                AS total_requests,
                COUNT(date_closed)                      AS closed_requests,
                ROUND(AVG(resolution_days), 1)          AS avg_resolution_days,
                MEDIAN(resolution_days)                 AS median_resolution_days,
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY resolution_days) AS p90_resolution_days,
                ROUND(COUNT(date_closed) * 100.0 / COUNT(*), 1) AS close_rate_pct
            FROM requests
            WHERE comm_plan_name IS NOT NULL AND comm_plan_name != ''
            GROUP BY comm_plan_name, council_district
            ORDER BY total_requests DESC
        ) TO '{AGGREGATED_DIR}/response_by_neighborhood.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] response_by_neighborhood")

    # 2) Volume by service_name over time (monthly)
    con.execute(f"""
        COPY (
            SELECT
                request_month_start,
                service_name,
                COUNT(*) AS request_count
            FROM requests
            WHERE service_name IS NOT NULL AND service_name != ''
            GROUP BY request_month_start, service_name
            ORDER BY request_month_start, request_count DESC
        ) TO '{AGGREGATED_DIR}/volume_by_service_monthly.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] volume_by_service_monthly")

    # 3) Resolution rates by council district
    con.execute(f"""
        COPY (
            SELECT
                council_district,
                service_name,
                COUNT(*)                                AS total_requests,
                COUNT(date_closed)                      AS closed_requests,
                ROUND(AVG(resolution_days), 1)          AS avg_resolution_days,
                MEDIAN(resolution_days)                 AS median_resolution_days,
                ROUND(COUNT(date_closed) * 100.0 / COUNT(*), 1) AS close_rate_pct
            FROM requests
            WHERE council_district IS NOT NULL
            GROUP BY council_district, service_name
            ORDER BY council_district, total_requests DESC
        ) TO '{AGGREGATED_DIR}/resolution_by_district.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] resolution_by_district")

    # 4) Monthly trends (overall)
    con.execute(f"""
        COPY (
            SELECT
                request_month_start,
                COUNT(*)                        AS total_requests,
                COUNT(date_closed)              AS closed_requests,
                ROUND(AVG(resolution_days), 1)  AS avg_resolution_days,
                MEDIAN(resolution_days)         AS median_resolution_days
            FROM requests
            GROUP BY request_month_start
            ORDER BY request_month_start
        ) TO '{AGGREGATED_DIR}/monthly_trends.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] monthly_trends")

    # 5) Top problem types overall
    con.execute(f"""
        COPY (
            SELECT
                service_name,
                COUNT(*)                        AS total_requests,
                COUNT(date_closed)              AS closed_requests,
                ROUND(AVG(resolution_days), 1)  AS avg_resolution_days,
                MEDIAN(resolution_days)         AS median_resolution_days,
                ROUND(COUNT(date_closed) * 100.0 / COUNT(*), 1) AS close_rate_pct
            FROM requests
            WHERE service_name IS NOT NULL AND service_name != ''
            GROUP BY service_name
            ORDER BY total_requests DESC
        ) TO '{AGGREGATED_DIR}/top_problem_types.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] top_problem_types")

    # 6) Heatmap data (lat/lng points sampled for map performance)
    con.execute(f"""
        COPY (
            SELECT
                lat,
                lng,
                service_name,
                request_year,
                comm_plan_name,
                council_district
            FROM requests
            WHERE lat IS NOT NULL
              AND lng IS NOT NULL
              AND lat BETWEEN 32.5 AND 33.3
              AND lng BETWEEN -117.7 AND -116.8
        ) TO '{AGGREGATED_DIR}/map_points.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    print("  [agg] map_points")

    # 7) Yearly volume
    con.execute(f"""
        COPY (
            SELECT
                request_year,
                COUNT(*) AS total_requests,
                COUNT(date_closed) AS closed_requests
            FROM requests
            WHERE request_year IS NOT NULL
            GROUP BY request_year
            ORDER BY request_year
        ) TO '{AGGREGATED_DIR}/yearly_volume.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] yearly_volume")

    # 8) Case origin (how reports are submitted)
    con.execute(f"""
        COPY (
            SELECT
                CASE
                    WHEN case_origin IN ('Mobile') THEN 'Mobile App'
                    WHEN case_origin IN ('Web') THEN 'Web'
                    WHEN case_origin IN ('Phone') THEN 'Phone'
                    ELSE 'Other'
                END AS channel,
                COUNT(*) AS request_count
            FROM requests
            GROUP BY channel
            ORDER BY request_count DESC
        ) TO '{AGGREGATED_DIR}/case_origin.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] case_origin")

    # 9) Day-of-week / hour patterns
    con.execute(f"""
        COPY (
            SELECT
                request_dow,
                HOUR(date_requested) AS request_hour,
                COUNT(*) AS request_count
            FROM requests
            GROUP BY request_dow, HOUR(date_requested)
            ORDER BY request_dow, request_hour
        ) TO '{AGGREGATED_DIR}/day_hour_patterns.parquet' (FORMAT PARQUET)
    """)
    print("  [agg] day_hour_patterns")


if __name__ == "__main__":
    transform()
