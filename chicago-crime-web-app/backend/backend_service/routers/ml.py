from __future__ import annotations

import json
import os
from functools import lru_cache
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend_service.db import get_session_factory
from backend_service.config import get_settings
from backend_service.schemas import (
    AreaRow,
    AreasResponse,
    CityCompareResponse,
    CitySummaryResponse,
    MetaBlock,
    TopCategory,
)

router = APIRouter(prefix="/api/ml", tags=["ml"])


def get_db() -> Session:
    factory = get_session_factory()
    db = factory()
    try:
        yield db
    finally:
        db.close()


@lru_cache
def _area_id_to_name() -> dict[int, str]:
    """
    Fallback for cases where `studio_area_snapshot.area_name` is NULL.
    The GeoJSON at `GEOJSON_PATH` contains `properties.area_id` + `properties.name`.
    """
    settings = get_settings()
    try:
        with open(settings.geojson_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

    mapping: dict[int, str] = {}
    for feat in (data.get("features") or []):
        props = feat.get("properties") or {}
        area_id = props.get("area_id") or props.get("community_area_id") or props.get("id")
        name = props.get("name") or props.get("area_name") or props.get("community_area_name")
        if area_id is None or name is None:
            continue
        try:
            mapping[int(area_id)] = str(name)
        except Exception:
            continue
    return mapping


@router.get("/community-areas")
def community_areas() -> JSONResponse:
    """
    Alias GeoJSON endpoint under /api/ml for the frontend live map.
    Uses the same normalization logic as /api/geo/community-areas.
    """
    from backend_service.routers.geo import _get_community_geojson

    geojson = _get_community_geojson()
    return JSONResponse(content=geojson, media_type="application/geo+json")


def _resolve_process_date(db: Session, snapshot: str | None, process_date: date | None) -> date:
    if process_date is not None:
        return process_date
    if snapshot and snapshot != "latest":
        raise HTTPException(status_code=400, detail="Only snapshot=latest is supported")
    row = db.execute(text("SELECT MAX(process_date) AS d FROM studio_area_snapshot")).mappings().first()
    if not row or row["d"] is None:
        raise HTTPException(status_code=404, detail="No data in studio_area_snapshot")
    return row["d"]


def _average_safety_batch(
    db: Session, pd: date, prediction_date: date | None
) -> tuple[float, int] | None:
    """Average city safety for one loaded batch; None if no rows."""
    if prediction_date is not None:
        row = db.execute(
            text(
                """
                SELECT AVG(safety_score) AS avg_s, COUNT(*) AS n
                FROM studio_area_snapshot
                WHERE process_date = :pd AND prediction_date = :pdate
                """
            ),
            {"pd": pd, "pdate": prediction_date},
        ).mappings().first()
    else:
        row = db.execute(
            text(
                """
                WITH latest AS (
                    SELECT area_id, MAX(prediction_date) AS pd
                    FROM studio_area_snapshot
                    WHERE process_date = :pd
                    GROUP BY area_id
                )
                SELECT AVG(s.safety_score) AS avg_s, COUNT(*) AS n
                FROM studio_area_snapshot s
                INNER JOIN latest l ON s.area_id = l.area_id AND s.prediction_date = l.pd
                WHERE s.process_date = :pd
                """
            ),
            {"pd": pd},
        ).mappings().first()
    if not row or row["n"] == 0:
        return None
    return float(row["avg_s"] or 0), int(row["n"])


def _resolve_compare_process_dates(db: Session, process_date: date | None) -> tuple[date, date]:
    """
    Return (newer_batch, older_batch) process_date values for day-over-day style comparison.
    """
    if process_date is not None:
        prev = db.execute(
            text(
                """
                SELECT MAX(process_date) AS d
                FROM studio_area_snapshot
                WHERE process_date < :pd
                """
            ),
            {"pd": process_date},
        ).mappings().first()
        if not prev or prev["d"] is None:
            raise HTTPException(
                status_code=404,
                detail=f"No earlier batch found in studio_area_snapshot before process_date={process_date}",
            )
        return process_date, prev["d"]

    pairs = db.execute(
        text(
            """
            SELECT DISTINCT process_date AS d
            FROM studio_area_snapshot
            ORDER BY process_date DESC
            LIMIT 2
            """
        )
    ).fetchall()
    if len(pairs) < 2:
        raise HTTPException(
            status_code=404,
            detail="Need at least two distinct process_date batches in studio_area_snapshot for comparison",
        )
    newer, older = pairs[0][0], pairs[1][0]
    return newer, older


@router.get("/areas", response_model=AreasResponse)
def list_areas(
    snapshot: str | None = Query("latest"),
    process_date: date | None = Query(None, description="ISO date; default latest batch"),
    prediction_date: date | None = Query(
        None,
        description="Which forward prediction_date to show; default is latest prediction_date in that batch per area",
    ),
    db: Session = Depends(get_db),
) -> AreasResponse:
    pd = _resolve_process_date(db, snapshot, process_date)
    area_id_to_name = _area_id_to_name()

    if prediction_date is not None:
        rows = db.execute(
            text(
                """
                SELECT area_type, area_id, area_name, predicted_count, safety_score,
                       top_crime_1, top_crime_2, top_crime_3,
                       predicted_count_low, predicted_count_high,
                       prediction_date, target_date, process_date,
                       model_name, model_version, run_id
                FROM studio_area_snapshot
                WHERE process_date = :pd AND prediction_date = :pdate
                ORDER BY area_id
                """
            ),
            {"pd": pd, "pdate": prediction_date},
        ).mappings().all()
    else:
        rows = db.execute(
            text(
                """
                WITH latest AS (
                    SELECT area_id, MAX(prediction_date) AS pd
                    FROM studio_area_snapshot
                    WHERE process_date = :pd
                    GROUP BY area_id
                )
                SELECT s.area_type, s.area_id, s.area_name, s.predicted_count, s.safety_score,
                       s.top_crime_1, s.top_crime_2, s.top_crime_3,
                       s.predicted_count_low, s.predicted_count_high,
                       s.prediction_date, s.target_date, s.process_date,
                       s.model_name, s.model_version, s.run_id
                FROM studio_area_snapshot s
                INNER JOIN latest l ON s.area_id = l.area_id AND s.prediction_date = l.pd
                WHERE s.process_date = :pd
                ORDER BY s.area_id
                """
            ),
            {"pd": pd},
        ).mappings().all()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No rows for process_date={pd}")

    areas: list[AreaRow] = []
    meta_sample = rows[0]
    for r in rows:
        tops: list[TopCategory] = []
        if r["top_crime_1"]:
            tops.append(TopCategory(label=r["top_crime_1"], rank=1))
        if r["top_crime_2"]:
            tops.append(TopCategory(label=r["top_crime_2"], rank=2))
        if r["top_crime_3"]:
            tops.append(TopCategory(label=r["top_crime_3"], rank=3))

        name = r["area_name"]
        if not name:
            # Some loads write NULL into `area_name`; GeoJSON keeps canonical names.
            name = area_id_to_name.get(int(r["area_id"]))

        areas.append(
            AreaRow(
                area_id=r["area_id"],
                name=name,
                safety_score=float(r["safety_score"]),
                predicted_count=float(r["predicted_count"]),
                top_categories=tops,
                predicted_count_low=r["predicted_count_low"],
                predicted_count_high=r["predicted_count_high"],
            )
        )

    meta = MetaBlock(
        prediction_date=meta_sample["prediction_date"].isoformat() if meta_sample["prediction_date"] else None,
        target_date=meta_sample["target_date"].isoformat() if meta_sample["target_date"] else None,
        process_date=pd.isoformat(),
        model_version=meta_sample["model_version"],
        model_name=meta_sample["model_name"],
    )
    return AreasResponse(areas=areas, meta=meta)


@router.get("/city-summary", response_model=CitySummaryResponse)
def city_summary(
    snapshot: str | None = Query("latest"),
    process_date: date | None = Query(None),
    prediction_date: date | None = Query(
        None,
        description="Matches /areas; default is latest prediction_date per area in the batch",
    ),
    db: Session = Depends(get_db),
) -> CitySummaryResponse:
    pd = _resolve_process_date(db, snapshot, process_date)
    got = _average_safety_batch(db, pd, prediction_date)
    if got is None:
        raise HTTPException(status_code=404, detail=f"No rows for process_date={pd}")
    avg_s, n = got
    return CitySummaryResponse(
        average_safety=avg_s,
        total_areas=n,
        process_date=pd.isoformat(),
    )


@router.get("/city-summary/compare", response_model=CityCompareResponse)
def city_summary_compare(
    process_date: date | None = Query(
        None,
        description="Newer batch to compare; default: latest distinct process_date vs the prior batch",
    ),
    prediction_date: date | None = Query(
        None,
        description="Same as /city-summary — use one horizon for both batches",
    ),
    db: Session = Depends(get_db),
) -> CityCompareResponse:
    pd_new, pd_old = _resolve_compare_process_dates(db, process_date)
    cur = _average_safety_batch(db, pd_new, prediction_date)
    prev = _average_safety_batch(db, pd_old, prediction_date)
    if cur is None:
        raise HTTPException(status_code=404, detail=f"No rows for process_date={pd_new}")
    if prev is None:
        raise HTTPException(status_code=404, detail=f"No rows for process_date={pd_old}")
    avg_c, n_c = cur
    avg_p, n_p = prev
    delta = avg_c - avg_p
    eps = 1e-6
    if delta > eps:
        direction = "up"
    elif delta < -eps:
        direction = "down"
    else:
        direction = "flat"
    return CityCompareResponse(
        process_date_current=pd_new.isoformat(),
        process_date_previous=pd_old.isoformat(),
        average_safety_current=avg_c,
        average_safety_previous=avg_p,
        delta_average_safety=delta,
        direction=direction,
        total_areas_current=n_c,
        total_areas_previous=n_p,
    )
