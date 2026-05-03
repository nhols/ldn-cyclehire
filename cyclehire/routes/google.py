from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import polars as pl

from cyclehire.routes.paths import (
    google_bicycle_routes_jsonl_path,
    google_bicycle_routes_parquet_path,
)
from cyclehire.utils import write_parquet_atomic


ROUTES_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"
FIELD_MASK = "routes.distanceMeters,routes.duration,routes.staticDuration,routes.polyline"


@dataclass(frozen=True)
class GoogleBicycleRouteCache:
    data_dir: Path

    @property
    def jsonl_path(self) -> Path:
        return self.data_dir / google_bicycle_routes_jsonl_path()

    @property
    def parquet_path(self) -> Path:
        return self.data_dir / google_bicycle_routes_parquet_path()

    def fetched_keys(self) -> set[str]:
        if not self.jsonl_path.exists():
            return set()
        keys = set()
        with self.jsonl_path.open("r", encoding="utf-8") as input_file:
            for line in input_file:
                if not line.strip():
                    continue
                row = json.loads(line)
                keys.add(route_key(row["pair_from"], row["pair_to"]))
        return keys

    def append_routes(self, routes: Iterable[dict[str, Any]]) -> None:
        self.jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        with self.jsonl_path.open("a", encoding="utf-8") as output:
            for route in routes:
                output.write(json.dumps(route, sort_keys=True) + "\n")
                output.flush()

    def write_parquet(self) -> None:
        if not self.jsonl_path.exists():
            return
        rows = []
        with self.jsonl_path.open("r", encoding="utf-8") as input_file:
            for line in input_file:
                if line.strip():
                    rows.append(json.loads(line))
        if not rows:
            return
        frame = pl.DataFrame([serializable_route_row(row) for row in rows], infer_schema_length=None)
        write_parquet_atomic(frame, self.parquet_path)


def fetch_google_bicycle_routes(
    pairs: pl.DataFrame,
    *,
    api_key: str,
    cache: GoogleBicycleRouteCache,
    sleep_seconds: float = 0.0,
) -> int:
    fetched = 0
    for pair in pairs.iter_rows(named=True):
        route = fetch_route(pair, api_key)
        cache.append_routes([route])
        fetched += 1
        if fetched % 25 == 0:
            print(f"fetched {fetched:,}/{pairs.height:,}", flush=True)
        if sleep_seconds:
            time.sleep(sleep_seconds)
    cache.write_parquet()
    return fetched


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
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        payload = {"error": {"status": exc.status, "body": error_body}}

    route = (payload.get("routes") or [{}])[0]
    polyline = route.get("polyline") or {}
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


def route_key(pair_from: str, pair_to: str) -> str:
    return f"{pair_from}:{pair_to}"


def serializable_route_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "from_coord": json.dumps(row["from_coord"]),
        "to_coord": json.dumps(row["to_coord"]),
        "geometry": json.dumps(row["geometry"]),
        "error": json.dumps(row["error"]),
    }
