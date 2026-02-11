"""Shared query layer — all SQL lives here.

Every public function returns ``list[dict]`` or ``dict``.
A fresh ``duckdb.connect()`` is created per call (read-only, thread-safe).
"""

from __future__ import annotations

from pathlib import Path

import duckdb

_ROOT = Path(__file__).resolve().parent.parent
_AGG = _ROOT / "data" / "aggregated"


# ── helpers ──────────────────────────────────────────────────────────────

def _q(where: str, condition: str) -> str:
    """Append an AND condition (or start a WHERE clause)."""
    if where:
        return f"{where} AND {condition}"
    return f"WHERE {condition}"


def _where(
    year_min: int | None = None,
    year_max: int | None = None,
    service_name: str | None = None,
    district: int | None = None,
    neighborhood: str | None = None,
    *,
    year_col: str = "request_year",
    has_service_name: bool = True,
    has_district: bool = True,
    has_neighborhood: bool = True,
) -> str:
    """Build a WHERE clause from optional filter params."""
    w = ""
    if year_min is not None:
        w = _q(w, f"{year_col} >= {int(year_min)}")
    if year_max is not None:
        w = _q(w, f"{year_col} <= {int(year_max)}")
    if service_name is not None and has_service_name:
        safe = service_name.replace("'", "''")
        w = _q(w, f"service_name = '{safe}'")
    if district is not None and has_district:
        w = _q(w, f"council_district = {int(district)}")
    if neighborhood is not None and has_neighborhood:
        safe = neighborhood.replace("'", "''")
        w = _q(w, f"comm_plan_name = '{safe}'")
    return w


def _run(sql: str) -> list[dict]:
    """Execute *sql* and return rows as a list of dicts."""
    con = duckdb.connect()
    try:
        df = con.execute(sql).fetchdf()
        return df.to_dict(orient="records")
    finally:
        con.close()


def _pq(name: str) -> str:
    """Return the quoted parquet path for *name*."""
    return f"'{_AGG / name}.parquet'"


# ── query functions ──────────────────────────────────────────────────────

def get_filter_options() -> dict:
    """Return valid values for all filter parameters."""
    con = duckdb.connect()
    try:
        service_names = sorted(
            r[0]
            for r in con.execute(
                f"SELECT DISTINCT service_name FROM {_pq('top_problem_types')} ORDER BY service_name"
            ).fetchall()
        )
        council_districts = sorted(
            r[0]
            for r in con.execute(
                f"SELECT DISTINCT council_district FROM {_pq('resolution_by_district')} ORDER BY council_district"
            ).fetchall()
        )
        neighborhoods = sorted(
            r[0]
            for r in con.execute(
                f"SELECT DISTINCT comm_plan_name FROM {_pq('response_by_neighborhood')} ORDER BY comm_plan_name"
            ).fetchall()
        )
        years = sorted(
            r[0]
            for r in con.execute(
                f"SELECT DISTINCT request_year FROM {_pq('yearly_volume')} ORDER BY request_year"
            ).fetchall()
        )
    finally:
        con.close()
    return {
        "service_names": service_names,
        "council_districts": council_districts,
        "neighborhoods": neighborhoods,
        "years": years,
    }


def get_overview(
    year_min: int | None = None,
    year_max: int | None = None,
) -> dict:
    """High-level KPIs across the entire dataset (or a year range)."""
    w = _where(year_min=year_min, year_max=year_max, has_service_name=False, has_district=False, has_neighborhood=False)
    rows = _run(
        f"SELECT SUM(total_requests) AS total_requests, "
        f"       SUM(closed_requests) AS closed_requests "
        f"FROM {_pq('yearly_volume')} {w}"
    )
    total = int(rows[0]["total_requests"] or 0)
    closed = int(rows[0]["closed_requests"] or 0)
    close_rate = round(closed / total * 100, 1) if total else 0.0

    # Median resolution from monthly_trends
    if year_min is not None or year_max is not None:
        mw = ""
        if year_min is not None:
            mw = _q(mw, f"YEAR(request_month_start) >= {int(year_min)}")
        if year_max is not None:
            mw = _q(mw, f"YEAR(request_month_start) <= {int(year_max)}")
    else:
        mw = ""
    med_rows = _run(
        f"SELECT AVG(median_resolution_days) AS median_resolution_days "
        f"FROM {_pq('monthly_trends')} {mw}"
    )
    median_res = round(float(med_rows[0]["median_resolution_days"] or 0), 1)

    return {
        "total_requests": total,
        "closed_requests": closed,
        "close_rate_pct": close_rate,
        "median_resolution_days": median_res,
    }


def get_top_problem_types(limit: int = 10) -> list[dict]:
    """Top problem types by total request count."""
    return _run(
        f"SELECT service_name, total_requests, closed_requests, "
        f"       median_resolution_days, close_rate_pct "
        f"FROM {_pq('top_problem_types')} "
        f"ORDER BY total_requests DESC LIMIT {int(limit)}"
    )


def get_response_by_neighborhood(
    district: int | None = None,
    limit: int = 20,
) -> list[dict]:
    """Neighborhood-level response metrics, optionally filtered by district."""
    w = _where(district=district, has_service_name=False, has_neighborhood=False)
    return _run(
        f"SELECT comm_plan_name, council_district, total_requests, closed_requests, "
        f"       median_resolution_days, p90_resolution_days, close_rate_pct "
        f"FROM {_pq('response_by_neighborhood')} {w} "
        f"ORDER BY median_resolution_days DESC LIMIT {int(limit)}"
    )


def get_resolution_by_district(
    service_name: str | None = None,
) -> list[dict]:
    """District-level resolution metrics, optionally filtered by service."""
    if service_name is not None:
        w = _where(service_name=service_name, has_district=False, has_neighborhood=False)
        return _run(
            f"SELECT council_district, total_requests, closed_requests, "
            f"       avg_resolution_days, median_resolution_days, close_rate_pct "
            f"FROM {_pq('resolution_by_district')} {w} "
            f"ORDER BY council_district"
        )
    # Aggregate across all services per district
    return _run(
        f"SELECT council_district, "
        f"       SUM(total_requests)::BIGINT AS total_requests, "
        f"       SUM(closed_requests)::BIGINT AS closed_requests, "
        f"       ROUND(SUM(avg_resolution_days * total_requests) / SUM(total_requests), 1) AS avg_resolution_days, "
        f"       ROUND(SUM(median_resolution_days * total_requests) / SUM(total_requests), 1) AS median_resolution_days, "
        f"       ROUND(SUM(closed_requests) / SUM(total_requests) * 100, 1) AS close_rate_pct "
        f"FROM {_pq('resolution_by_district')} "
        f"GROUP BY council_district ORDER BY council_district"
    )


def get_monthly_trends(
    year_min: int | None = None,
    year_max: int | None = None,
) -> list[dict]:
    """Monthly trend data, optionally filtered by year range."""
    w = ""
    if year_min is not None:
        w = _q(w, f"YEAR(request_month_start) >= {int(year_min)}")
    if year_max is not None:
        w = _q(w, f"YEAR(request_month_start) <= {int(year_max)}")
    rows = _run(
        f"SELECT request_month_start, total_requests, closed_requests, "
        f"       avg_resolution_days, median_resolution_days "
        f"FROM {_pq('monthly_trends')} {w} "
        f"ORDER BY request_month_start"
    )
    # Convert date to YYYY-MM-DD string for JSON serialization
    for r in rows:
        r["request_month_start"] = str(r["request_month_start"])[:10]
    return rows


def get_yearly_volume(
    year_min: int | None = None,
    year_max: int | None = None,
) -> list[dict]:
    """Yearly volume data, optionally filtered by year range."""
    w = _where(year_min=year_min, year_max=year_max, has_service_name=False, has_district=False, has_neighborhood=False)
    return _run(
        f"SELECT request_year, total_requests, closed_requests "
        f"FROM {_pq('yearly_volume')} {w} "
        f"ORDER BY request_year"
    )


def get_case_origins() -> list[dict]:
    """Request counts by submission channel."""
    return _run(
        f"SELECT channel, request_count "
        f"FROM {_pq('case_origin')} "
        f"ORDER BY request_count DESC"
    )


def get_day_hour_patterns() -> list[dict]:
    """Request counts by day-of-week and hour (168 rows)."""
    return _run(
        f"SELECT request_dow, request_hour, request_count "
        f"FROM {_pq('day_hour_patterns')} "
        f"ORDER BY request_dow, request_hour"
    )
