from __future__ import annotations

import json
import os
import urllib.request
from functools import lru_cache

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from backend_service.config import get_settings

router = APIRouter(prefix="/api/geo", tags=["geo"])


def _read_json_file(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _is_placeholder_geojson(data: dict) -> bool:
    features = data.get("features") or []
    # Our bundled placeholder has only 3 sample polygons.
    return len(features) <= 5


@lru_cache(maxsize=1)
def _get_community_geojson() -> dict:
    """
    Return GeoJSON with normalized feature properties:
      - properties.area_id (int)
      - properties.name (str)
    """
    settings = get_settings()
    path = settings.geojson_path
    if not os.path.isabs(path):
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        path = os.path.join(root, path)

    local_data: dict | None = None
    if os.path.isfile(path):
        try:
            local_data = _read_json_file(path)
        except Exception:
            local_data = None

    if local_data is not None and not _is_placeholder_geojson(local_data):
        return local_data

    # Placeholder detected → fetch authoritative boundaries from City of Chicago.
    default_url = "https://data.cityofchicago.org/resource/vrxf-vc4k.geojson"
    url = os.getenv("COMMUNITY_AREAS_GEOJSON_URL", default_url)
    if "?" in url:
        if "$limit=" not in url:
            url = url + "&$limit=1000"
    else:
        url = url + "?$limit=1000"

    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
        remote_data = json.loads(raw)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Could not load community-area GeoJSON: {exc}",
        ) from exc

    features = remote_data.get("features") or []
    normalized = {
        "type": remote_data.get("type", "FeatureCollection"),
        "name": "community_areas",
        "features": [],
    }

    for feat in features:
        props = feat.get("properties") or {}
        area_id_raw = props.get("area_numbe") or props.get("area_num_1") or props.get("comarea_id")
        name = props.get("community") or props.get("name") or props.get("area")
        if area_id_raw is None:
            continue
        try:
            area_id_int = int(float(area_id_raw))
        except Exception:
            continue
        if name is None:
            name = str(area_id_int)
        props["area_id"] = area_id_int
        props["name"] = str(name)
        feat["properties"] = props
        normalized["features"].append(feat)
    # Persist so future calls (and local dev) don't need the remote fetch again.
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(normalized, f)
    except Exception:
        # Non-fatal: the endpoint can still return the normalized geojson.
        pass

    return normalized


@router.get("/community-areas")
def community_areas() -> JSONResponse:
    geojson = _get_community_geojson()
    return JSONResponse(content=geojson, media_type="application/geo+json")
