from __future__ import annotations

import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, cast

import polars as pl

from cyclehire.routes.common import JsonlRouteCache, fetch_routes, serializable_route_row
from cyclehire.routes.paths import (
    google_bicycle_routes_jsonl_path,
    google_bicycle_routes_parquet_path,
)


ROUTES_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"
FIELD_MASK = "routes.distanceMeters,routes.duration,routes.staticDuration,routes.polyline"


class GoogleBicycleRouteCache(JsonlRouteCache):
    def __init__(self, data_dir: Path) -> None:
        super().__init__(
            data_dir,
            jsonl_path=google_bicycle_routes_jsonl_path(),
            parquet_path=google_bicycle_routes_parquet_path(),
            serialize_row=serializable_route_row,
        )


def fetch_google_bicycle_routes(
    pairs: pl.DataFrame,
    *,
    api_key: str,
    cache: GoogleBicycleRouteCache,
    requests_per_minute: float | None = None,
) -> int:
    return fetch_routes(
        pairs,
        fetch_route=lambda pair: fetch_route(pair, api_key),
        cache=cache,
        requests_per_minute=requests_per_minute,
    )


def fetch_route(pair: dict[str, Any], api_key: str) -> dict[str, Any]:
    request_body = {
        "origin": {
            "location": {
                "latLng": {
                    "latitude": pair["from_lat"],
                    "longitude": pair["from_lon"],
                }
            }
        },
        "destination": {
            "location": {
                "latLng": {
                    "latitude": pair["to_lat"],
                    "longitude": pair["to_lon"],
                }
            }
        },
        "travelMode": "BICYCLE",
        "polylineQuality": "HIGH_QUALITY",
        "polylineEncoding": "GEO_JSON_LINESTRING",
        "languageCode": "en-GB",
        "regionCode": "gb",
    }
    request = urllib.request.Request(
        ROUTES_URL,
        data=json.dumps(request_body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": FIELD_MASK,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = cast(dict[str, Any], json.load(response))
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        payload = {"error": {"status": exc.status, "body": error_body}}

    routes = cast(list[dict[str, Any]], payload.get("routes") or [{}])
    route = routes[0]
    polyline = cast(dict[str, Any], route.get("polyline") or {})
    return {
        "pair_from": pair["pair_from"],
        "pair_to": pair["pair_to"],
        "from_name": pair["from_name"],
        "to_name": pair["to_name"],
        "from_coord": [pair["from_lon"], pair["from_lat"]],
        "to_coord": [pair["to_lon"], pair["to_lat"]],
        "trip_count": pair["trips"],
        "distance_meters": route.get("distanceMeters"),
        "duration": route.get("duration"),
        "static_duration": route.get("staticDuration"),
        "geometry": polyline.get("geoJsonLinestring"),
        "error": payload.get("error"),
    }
