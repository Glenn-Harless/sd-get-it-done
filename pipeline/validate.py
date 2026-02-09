"""Validate processed Get It Done 311 data and print a quality report."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

PARQUET = Path(__file__).resolve().parent.parent / "data" / "processed" / "requests.parquet"
AGGREGATED_DIR = Path(__file__).resolve().parent.parent / "data" / "aggregated"

# San Diego bounding box
SD_LAT_MIN, SD_LAT_MAX = 32.5, 33.3
SD_LNG_MIN, SD_LNG_MAX = -117.7, -116.8


def _q(sql: str) -> list:
    """Run a query against the parquet file, return all rows."""
    con = duckdb.connect()
    result = con.execute(sql).fetchall()
    con.close()
    return result


def _scalar(sql: str):
    """Run a query and return a single scalar value."""
    rows = _q(sql)
    return rows[0][0] if rows else None


def validate() -> int:
    """Run all checks, print report. Returns count of issues found."""
    if not PARQUET.exists():
        print(f"ERROR: {PARQUET} not found. Run the pipeline first.")
        return -1

    src = f"'{PARQUET}'"
    issues = 0

    total = _scalar(f"SELECT COUNT(*) FROM {src}")
    print("=" * 64)
    print("  Get It Done 311 — Data Validation Report")
    print("=" * 64)
    print(f"\nDataset: {total:,} rows")

    date_range = _q(
        f"SELECT MIN(date_requested)::DATE, MAX(date_requested)::DATE FROM {src}"
    )
    print(f"Date range: {date_range[0][0]} to {date_range[0][1]}")

    # ── 1. Negative resolution days ──────────────────────────────
    neg = _scalar(
        f"SELECT COUNT(*) FROM {src} WHERE resolution_days < 0"
    )
    print(f"\n{'─' * 64}")
    print("1. Negative resolution days (date_closed < date_requested)")
    if neg:
        issues += neg
        print(f"   FAIL  {neg:,} records")
        rows = _q(f"""
            SELECT service_request_id, service_name, resolution_days,
                   date_requested::DATE, date_closed::DATE
            FROM {src}
            WHERE resolution_days < 0
            ORDER BY resolution_days
            LIMIT 5
        """)
        for r in rows:
            print(f"         ID {r[0]}: {r[1]}, {r[2]}d ({r[3]} -> {r[4]})")
    else:
        print("   PASS  No negative resolution days")

    # ── 2. Geographic outliers ───────────────────────────────────
    geo_outliers = _scalar(f"""
        SELECT COUNT(*) FROM {src}
        WHERE lat IS NOT NULL AND lng IS NOT NULL
          AND (lat < {SD_LAT_MIN} OR lat > {SD_LAT_MAX}
               OR lng < {SD_LNG_MIN} OR lng > {SD_LNG_MAX})
    """)
    print(f"\n{'─' * 64}")
    print("2. Geographic outliers (outside San Diego bounds)")
    if geo_outliers:
        issues += geo_outliers
        print(f"   FAIL  {geo_outliers:,} records outside [{SD_LAT_MIN}-{SD_LAT_MAX}] lat, [{SD_LNG_MIN}-{SD_LNG_MAX}] lng")
        extremes = _q(f"""
            SELECT MIN(lat), MAX(lat), MIN(lng), MAX(lng) FROM {src}
            WHERE lat IS NOT NULL AND lng IS NOT NULL
              AND (lat < {SD_LAT_MIN} OR lat > {SD_LAT_MAX}
                   OR lng < {SD_LNG_MIN} OR lng > {SD_LNG_MAX})
        """)
        e = extremes[0]
        print(f"         Lat range: {e[0]:.4f} to {e[1]:.4f}")
        print(f"         Lng range: {e[2]:.4f} to {e[3]:.4f}")
    else:
        print("   PASS  All coordinates within San Diego bounds")

    # ── 3. Closed without date_closed ────────────────────────────
    closed_no_date = _scalar(f"""
        SELECT COUNT(*) FROM {src}
        WHERE status = 'Closed' AND date_closed IS NULL
    """)
    print(f"\n{'─' * 64}")
    print("3. Status/date consistency (Closed but no date_closed)")
    if closed_no_date:
        issues += closed_no_date
        print(f"   FAIL  {closed_no_date:,} records marked Closed with NULL date_closed")
    else:
        print("   PASS  All closed records have date_closed")

    # ── 4. Extreme resolution times ──────────────────────────────
    extreme_res = _scalar(f"""
        SELECT COUNT(*) FROM {src}
        WHERE resolution_days > 730
    """)
    max_res = _scalar(f"SELECT MAX(resolution_days) FROM {src}")
    print(f"\n{'─' * 64}")
    print("4. Extreme resolution times (> 2 years)")
    if extreme_res:
        issues += extreme_res
        print(f"   WARN  {extreme_res:,} records with resolution > 730 days (max: {max_res:,}d)")
        buckets = _q(f"""
            SELECT
                CASE
                    WHEN resolution_days BETWEEN 731 AND 1095 THEN '2-3 years'
                    WHEN resolution_days BETWEEN 1096 AND 1825 THEN '3-5 years'
                    ELSE '5+ years'
                END AS bucket,
                COUNT(*) AS cnt
            FROM {src}
            WHERE resolution_days > 730
            GROUP BY bucket
            ORDER BY MIN(resolution_days)
        """)
        for b in buckets:
            print(f"         {b[0]}: {b[1]:,}")
    else:
        print("   PASS  No extreme resolution times")

    # ── 5. Missing critical fields ───────────────────────────────
    print(f"\n{'─' * 64}")
    print("5. Missing critical fields")
    fields = [
        ("service_name", "IS NULL OR service_name = ''"),
        ("council_district", "IS NULL"),
        ("lat/lng", "IS NULL", "lat IS NULL OR lng IS NULL"),
        ("comm_plan_name", "IS NULL OR comm_plan_name = ''"),
        ("status", "IS NULL OR status = ''"),
    ]
    any_missing = False
    for item in fields:
        name = item[0]
        condition = item[2] if len(item) > 2 else f"{name} {item[1]}"
        cnt = _scalar(f"SELECT COUNT(*) FROM {src} WHERE {condition}")
        pct = cnt / total * 100
        marker = "WARN" if pct > 1 else "INFO" if cnt > 0 else "PASS"
        if cnt > 0:
            any_missing = True
            issues += cnt
        print(f"   {marker}  {name}: {cnt:,} missing ({pct:.1f}%)")
    if not any_missing:
        print("   PASS  No missing critical fields")

    # ── 6. Duplicate service_request_id ──────────────────────────
    dupes = _scalar(f"""
        SELECT COUNT(*) FROM (
            SELECT service_request_id
            FROM {src}
            WHERE service_request_id IS NOT NULL
            GROUP BY service_request_id
            HAVING COUNT(*) > 1
        )
    """)
    print(f"\n{'─' * 64}")
    print("6. Duplicate service_request_id")
    if dupes:
        total_duped_rows = _scalar(f"""
            SELECT SUM(cnt) FROM (
                SELECT COUNT(*) AS cnt
                FROM {src}
                WHERE service_request_id IS NOT NULL
                GROUP BY service_request_id
                HAVING COUNT(*) > 1
            )
        """)
        issues += total_duped_rows
        print(f"   FAIL  {dupes:,} IDs appear multiple times ({total_duped_rows:,} total rows)")
    else:
        print("   PASS  No duplicate IDs")

    # ── 7. Status distribution sanity ────────────────────────────
    print(f"\n{'─' * 64}")
    print("7. Status distribution")
    statuses = _q(f"""
        SELECT status, COUNT(*) AS cnt,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
        FROM {src}
        GROUP BY status
        ORDER BY cnt DESC
    """)
    for s in statuses:
        print(f"         {s[0] or '(NULL)'}: {s[1]:,} ({s[2]}%)")

    # ── 8. Year-over-year volume anomalies ───────────────────────
    print(f"\n{'─' * 64}")
    print("8. Year-over-year volume (>50% change flagged)")
    yearly = _q(f"""
        SELECT request_year, COUNT(*) AS cnt
        FROM {src}
        WHERE request_year IS NOT NULL
        GROUP BY request_year
        ORDER BY request_year
    """)
    prev = None
    for yr, cnt in yearly:
        if prev is not None:
            change = (cnt - prev) / prev * 100
            flag = " <-- ANOMALY" if abs(change) > 50 else ""
            print(f"         {yr}: {cnt:>10,}  ({change:+.0f}%){flag}")
        else:
            print(f"         {yr}: {cnt:>10,}")
        prev = cnt

    # ── 9. Aggregation file checks ───────────────────────────────
    print(f"\n{'─' * 64}")
    print("9. Aggregation files")
    expected_aggs = [
        "case_origin.parquet",
        "day_hour_patterns.parquet",
        "map_points.parquet",
        "monthly_trends.parquet",
        "resolution_by_district.parquet",
        "response_by_neighborhood.parquet",
        "top_problem_types.parquet",
        "volume_by_service_monthly.parquet",
        "yearly_volume.parquet",
    ]
    for fname in expected_aggs:
        path = AGGREGATED_DIR / fname
        if path.exists():
            cnt = _scalar(f"SELECT COUNT(*) FROM '{path}'")
            size_kb = path.stat().st_size / 1024
            print(f"   PASS  {fname}: {cnt:,} rows ({size_kb:.0f} KB)")
        else:
            issues += 1
            print(f"   FAIL  {fname}: MISSING")

    # ── 10. Map points vs main dataset consistency ───────────────
    map_path = AGGREGATED_DIR / "map_points.parquet"
    if map_path.exists():
        print(f"\n{'─' * 64}")
        print("10. Map points consistency")
        map_count = _scalar(f"SELECT COUNT(*) FROM '{map_path}'")
        main_with_geo = _scalar(f"""
            SELECT COUNT(*) FROM {src}
            WHERE lat IS NOT NULL AND lng IS NOT NULL
              AND lat BETWEEN {SD_LAT_MIN} AND {SD_LAT_MAX}
              AND lng BETWEEN {SD_LNG_MIN} AND {SD_LNG_MAX}
        """)
        if map_count == main_with_geo:
            print(f"   PASS  map_points ({map_count:,}) matches main dataset geo-filtered count")
        else:
            issues += 1
            print(f"   FAIL  map_points ({map_count:,}) != main geo-filtered ({main_with_geo:,})")

    # ── Summary ──────────────────────────────────────────────────
    print(f"\n{'=' * 64}")
    if issues == 0:
        print("  ALL CHECKS PASSED")
    else:
        print(f"  {issues:,} total records flagged across all checks")
    print("=" * 64)

    return issues


if __name__ == "__main__":
    result = validate()
    sys.exit(1 if result > 0 else 0)
