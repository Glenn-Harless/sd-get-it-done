"""FastMCP server exposing San Diego 311 data as tools."""

from __future__ import annotations

from fastmcp import FastMCP

from . import queries as q

mcp = FastMCP(
    "San Diego Get It Done 311",
    instructions=(
        "San Diego 311 service request data covering May 2016 to present "
        "(~3 million requests). Includes potholes, graffiti, illegal dumping, "
        "streetlight outages, encampments, and 40+ other problem types. "
        "Call get_filter_options first to see available service names, "
        "council districts, neighborhoods, and years. "
        "Resolution times are in days."
    ),
)


@mcp.tool()
def get_filter_options() -> dict:
    """Return valid filter values: service_names, council_districts, neighborhoods, years.

    Call this first to discover what values you can pass to other tools.
    """
    return q.get_filter_options()


@mcp.tool()
def get_overview(
    year_min: int | None = None,
    year_max: int | None = None,
) -> dict:
    """High-level KPIs for 311 requests.

    Returns: total_requests, closed_requests, close_rate_pct,
    median_resolution_days. Filter by year range.
    """
    return q.get_overview(year_min=year_min, year_max=year_max)


@mcp.tool()
def get_top_problem_types(limit: int = 10) -> list[dict]:
    """Top problem types ranked by total request count.

    Returns: service_name, total_requests, closed_requests,
    median_resolution_days (days), close_rate_pct.
    """
    return q.get_top_problem_types(limit=limit)


@mcp.tool()
def get_response_by_neighborhood(
    district: int | None = None,
    limit: int = 20,
) -> list[dict]:
    """Neighborhood-level response metrics, sorted by slowest median resolution.

    Returns: comm_plan_name, council_district, total_requests,
    closed_requests, median_resolution_days (days),
    p90_resolution_days (days), close_rate_pct.
    Optionally filter by council_district.
    """
    return q.get_response_by_neighborhood(district=district, limit=limit)


@mcp.tool()
def get_resolution_by_district(
    service_name: str | None = None,
) -> list[dict]:
    """District-level resolution metrics.

    Returns: council_district, total_requests, closed_requests,
    avg_resolution_days (days), median_resolution_days (days),
    close_rate_pct. Optionally filter by service_name.
    If no service_name, aggregates across all services per district.
    """
    return q.get_resolution_by_district(service_name=service_name)


@mcp.tool()
def get_monthly_trends(
    year_min: int | None = None,
    year_max: int | None = None,
) -> list[dict]:
    """Monthly trend data for 311 requests.

    Returns: request_month_start (YYYY-MM-DD), total_requests,
    closed_requests, avg_resolution_days, median_resolution_days.
    Filter by year range.
    """
    return q.get_monthly_trends(year_min=year_min, year_max=year_max)


@mcp.tool()
def get_yearly_volume(
    year_min: int | None = None,
    year_max: int | None = None,
) -> list[dict]:
    """Yearly request volume.

    Returns: request_year, total_requests, closed_requests.
    Filter by year range.
    """
    return q.get_yearly_volume(year_min=year_min, year_max=year_max)


@mcp.tool()
def get_case_origins() -> list[dict]:
    """Request counts by submission channel (app, phone, web, other).

    Returns: channel, request_count.
    """
    return q.get_case_origins()


@mcp.tool()
def get_day_hour_patterns() -> list[dict]:
    """Request volume by day-of-week and hour (168 rows).

    Returns: request_dow (0=Sun..6=Sat), request_hour (0-23),
    request_count.
    """
    return q.get_day_hour_patterns()


def main():
    mcp.run()


if __name__ == "__main__":
    main()
