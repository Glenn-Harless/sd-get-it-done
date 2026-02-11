"""FastAPI REST app for San Diego Get It Done 311 data."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Query

from . import queries as q
from .models import (
    CaseOrigin,
    DayHourPattern,
    DistrictResolution,
    FilterOptions,
    MonthlyTrend,
    NeighborhoodResponse,
    OverviewResponse,
    ProblemType,
    YearlyVolume,
)

app = FastAPI(
    title="San Diego Get It Done 311 API",
    description=(
        "REST API for San Diego 311 service request data. "
        "Covers ~3 million requests from May 2016 to present, including "
        "potholes, graffiti, illegal dumping, streetlight outages, "
        "encampments, and 40+ other problem types."
    ),
    version="0.1.0",
)

_AGG = Path(__file__).resolve().parent.parent / "data" / "aggregated"


@app.get("/")
def root():
    """List available endpoints."""
    return {
        "endpoints": [
            {"path": "/health", "description": "Health check — list parquet files"},
            {"path": "/filters", "description": "Valid filter values"},
            {"path": "/overview", "description": "High-level KPIs"},
            {"path": "/problem-types", "description": "Top problem types"},
            {"path": "/neighborhoods", "description": "Response by neighborhood"},
            {"path": "/districts", "description": "Resolution by council district"},
            {"path": "/trends/monthly", "description": "Monthly trends"},
            {"path": "/trends/yearly", "description": "Yearly volume"},
            {"path": "/case-origins", "description": "Requests by submission channel"},
            {"path": "/day-hour-patterns", "description": "Request volume by day/hour"},
        ]
    }


@app.get("/health")
def health():
    """Health check — verify parquet files exist."""
    files = sorted(p.name for p in _AGG.glob("*.parquet"))
    return {"status": "ok", "parquet_files": files, "count": len(files)}


@app.get("/filters", response_model=FilterOptions)
def filters():
    """Return valid values for all filter parameters."""
    return q.get_filter_options()


@app.get("/overview", response_model=OverviewResponse)
def overview(
    year_min: int | None = Query(None, description="Minimum year (inclusive)"),
    year_max: int | None = Query(None, description="Maximum year (inclusive)"),
):
    """High-level KPIs, optionally filtered by year range."""
    return q.get_overview(year_min=year_min, year_max=year_max)


@app.get("/problem-types", response_model=list[ProblemType])
def problem_types(
    limit: int = Query(10, ge=1, le=100, description="Max results"),
):
    """Top problem types by request volume."""
    return q.get_top_problem_types(limit=limit)


@app.get("/neighborhoods", response_model=list[NeighborhoodResponse])
def neighborhoods(
    district: int | None = Query(None, description="Filter by council district"),
    limit: int = Query(20, ge=1, le=300, description="Max results"),
):
    """Neighborhood response metrics, sorted by slowest median resolution."""
    return q.get_response_by_neighborhood(district=district, limit=limit)


@app.get("/districts", response_model=list[DistrictResolution])
def districts(
    service_name: str | None = Query(None, description="Filter by service/problem type"),
):
    """District-level resolution metrics."""
    return q.get_resolution_by_district(service_name=service_name)


@app.get("/trends/monthly", response_model=list[MonthlyTrend])
def trends_monthly(
    year_min: int | None = Query(None, description="Minimum year (inclusive)"),
    year_max: int | None = Query(None, description="Maximum year (inclusive)"),
):
    """Monthly trend data."""
    return q.get_monthly_trends(year_min=year_min, year_max=year_max)


@app.get("/trends/yearly", response_model=list[YearlyVolume])
def trends_yearly(
    year_min: int | None = Query(None, description="Minimum year (inclusive)"),
    year_max: int | None = Query(None, description="Maximum year (inclusive)"),
):
    """Yearly volume data."""
    return q.get_yearly_volume(year_min=year_min, year_max=year_max)


@app.get("/case-origins", response_model=list[CaseOrigin])
def case_origins():
    """Request counts by submission channel."""
    return q.get_case_origins()


@app.get("/day-hour-patterns", response_model=list[DayHourPattern])
def day_hour_patterns():
    """Request volume by day-of-week and hour (168 rows)."""
    return q.get_day_hour_patterns()
