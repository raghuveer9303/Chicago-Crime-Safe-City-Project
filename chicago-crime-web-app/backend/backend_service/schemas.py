from __future__ import annotations

from pydantic import BaseModel, Field


class TopCategory(BaseModel):
    label: str
    rank: int = Field(ge=1, le=3)


class AreaRow(BaseModel):
    area_id: int
    name: str | None = None
    safety_score: float
    predicted_count: float
    top_categories: list[TopCategory]
    # 90% Poisson prediction interval for predicted incidents (lower=low, upper=high).
    predicted_count_low: float | None = None
    predicted_count_high: float | None = None


class MetaBlock(BaseModel):
    prediction_date: str | None = None
    target_date: str | None = None
    process_date: str
    model_version: str | None = None
    model_name: str | None = None
    disclaimer: str = (
        "Model-assisted estimates from historical Chicago crime data. "
        "Not official CPD guidance and not a guarantee of safety."
    )


class AreasResponse(BaseModel):
    areas: list[AreaRow]
    meta: MetaBlock


class CitySummaryResponse(BaseModel):
    average_safety: float
    total_areas: int
    process_date: str


class CityCompareResponse(BaseModel):
    """City-wide average safety vs the previous loaded batch (higher = safer)."""

    process_date_current: str
    process_date_previous: str
    average_safety_current: float
    average_safety_previous: float
    delta_average_safety: float = Field(
        ...,
        description="current minus previous; positive means city-wide safety improved",
    )
    direction: str = Field(
        ...,
        description="'up' | 'down' | 'flat' — whether average safety increased (safer)",
    )
    total_areas_current: int
    total_areas_previous: int
