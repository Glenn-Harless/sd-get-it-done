"""Pydantic response models for the 311 API."""

from __future__ import annotations

from pydantic import BaseModel


class FilterOptions(BaseModel):
    service_names: list[str]
    council_districts: list[int]
    neighborhoods: list[str]
    years: list[int]


class OverviewResponse(BaseModel):
    total_requests: int
    closed_requests: int
    close_rate_pct: float
    median_resolution_days: float


class ProblemType(BaseModel):
    service_name: str
    total_requests: int
    closed_requests: int
    median_resolution_days: float
    close_rate_pct: float


class NeighborhoodResponse(BaseModel):
    comm_plan_name: str
    council_district: int | None
    total_requests: int
    closed_requests: int
    median_resolution_days: float
    p90_resolution_days: float
    close_rate_pct: float


class DistrictResolution(BaseModel):
    council_district: int
    total_requests: int
    closed_requests: int
    avg_resolution_days: float
    median_resolution_days: float
    close_rate_pct: float


class MonthlyTrend(BaseModel):
    request_month_start: str
    total_requests: int
    closed_requests: int
    avg_resolution_days: float
    median_resolution_days: float


class YearlyVolume(BaseModel):
    request_year: int
    total_requests: int
    closed_requests: int


class CaseOrigin(BaseModel):
    channel: str
    request_count: int


class DayHourPattern(BaseModel):
    request_dow: int
    request_hour: int
    request_count: int
