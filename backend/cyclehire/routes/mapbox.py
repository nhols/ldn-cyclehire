from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast
import urllib.error
import urllib.parse
import urllib.request

import polars as pl

from cyclehire.routes.common import JsonlRouteCache, fetch_routes, serializable_route_row
from cyclehire.routes.paths import (
    mapbox_cycling_routes_jsonl_path,
    mapbox_cycling_routes_parquet_path,
)


ROUTES_URL = "https://api.mapbox.com/directions/v5/mapbox/cycling"


class MapboxCyclingRouteCache(JsonlRouteCache):
    def __init__(self, data_dir: Path) -> None:
        super().__init__(
            data_dir,
            jsonl_path=mapbox_cycling_routes_jsonl_path(),
            parquet_path=mapbox_cycling_routes_parquet_path(),
            serialize_row=serializable_route_row,
        )


def fetch_mapbox_cycling_routes(
    pairs: pl.DataFrame,
    *,
    access_token: str,
    cache: MapboxCyclingRouteCache,
    requests_per_minute: float | None = None,
) -> int:
    return fetch_routes(
        pairs,
        fetch_route=lambda pair: fetch_route(pair, access_token),
        cache=cache,
        requests_per_minute=requests_per_minute,
    )


def fetch_route(pair: dict[str, Any], access_token: str) -> dict[str, Any]:
    url = route_url(pair, access_token)
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = cast(dict[str, Any], json.load(response))
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        payload = {"code": "HttpError", "error": {"status": exc.status, "body": error_body}}
    except urllib.error.URLError as exc:
        payload = {"code": "UrlError", "error": {"reason": str(exc.reason)}}

    routes = cast(list[dict[str, Any]], payload.get("routes") or [{}])
    route = routes[0]
    geometry = cast(dict[str, Any], route.get("geometry") or {})
    return {
        "pair_from": pair["pair_from"],
        "pair_to": pair["pair_to"],
        "from_name": pair["from_name"],
        "to_name": pair["to_name"],
        "from_coord": [pair["from_lon"], pair["from_lat"]],
        "to_coord": [pair["to_lon"], pair["to_lat"]],
        "trip_count": pair["trips"],
        "distance_meters": route.get("distance"),
        "duration_seconds": route.get("duration"),
        "weight": route.get("weight"),
        "weight_name": route.get("weight_name"),
        "geometry": geometry if geometry.get("coordinates") else None,
        "code": payload.get("code"),
        "uuid": payload.get("uuid"),
        "error": payload.get("error"),
    }


def route_url(pair: dict[str, Any], access_token: str) -> str:
    coordinates = f"{pair['from_lon']},{pair['from_lat']};{pair['to_lon']},{pair['to_lat']}"
    query = urllib.parse.urlencode(
        {
            "access_token": access_token,
            "alternatives": "false",
            "geometries": "geojson",
            "overview": "full",
            "steps": "false",
        }
    )
    return f"{ROUTES_URL}/{coordinates}?{query}"
