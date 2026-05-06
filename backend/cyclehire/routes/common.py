from __future__ import annotations

import json
import logging
from pathlib import Path
import time
from typing import Any, Callable, Iterable, cast

import polars as pl

from cyclehire.utils import write_parquet_atomic


logger = logging.getLogger(__name__)
RouteFetcher = Callable[[dict[str, Any]], dict[str, Any]]
RouteSerializer = Callable[[dict[str, Any]], dict[str, Any]]


class JsonlRouteCache:
    def __init__(
        self,
        data_dir: Path,
        *,
        jsonl_path: Path,
        parquet_path: Path,
        serialize_row: RouteSerializer,
    ) -> None:
        self.data_dir = data_dir
        self._jsonl_path = jsonl_path
        self._parquet_path = parquet_path
        self._serialize_row = serialize_row

    @property
    def jsonl_path(self) -> Path:
        return self.data_dir / self._jsonl_path

    @property
    def parquet_path(self) -> Path:
        return self.data_dir / self._parquet_path

    def fetched_keys(self) -> set[str]:
        if not self.jsonl_path.exists():
            return set()
        keys: set[str] = set()
        with self.jsonl_path.open("r", encoding="utf-8") as input_file:
            for line in input_file:
                if not line.strip():
                    continue
                row = cast(dict[str, Any], json.loads(line))
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
        rows: list[dict[str, Any]] = []
        with self.jsonl_path.open("r", encoding="utf-8") as input_file:
            for line in input_file:
                if line.strip():
                    rows.append(cast(dict[str, Any], json.loads(line)))
        if not rows:
            return
        frame = pl.DataFrame([self._serialize_row(row) for row in rows], infer_schema_length=None)
        write_parquet_atomic(frame, self.parquet_path)


def fetch_routes(
    pairs: pl.DataFrame,
    *,
    fetch_route: RouteFetcher,
    cache: JsonlRouteCache,
    requests_per_minute: float | None = None,
) -> int:
    fetched = 0
    request_interval = request_interval_seconds(requests_per_minute)
    next_request_at = time.monotonic()
    try:
        for pair in pairs.iter_rows(named=True):
            if request_interval:
                wait_seconds = next_request_at - time.monotonic()
                if wait_seconds > 0:
                    time.sleep(wait_seconds)

            next_request_at = time.monotonic() + request_interval
            try:
                route = fetch_route(pair)
            except Exception as exc:
                logger.exception(
                    "route request failed for %s -> %s; recording error and continuing",
                    pair.get("pair_from"),
                    pair.get("pair_to"),
                )
                route = failed_route(pair, exc)
            cache.append_routes([route])
            fetched += 1
            if fetched % 25 == 0:
                logger.info("fetched %s/%s", f"{fetched:,}", f"{pairs.height:,}")
    finally:
        logger.info("writing route parquet snapshot to %s", cache.parquet_path)
        cache.write_parquet()
    return fetched


def request_interval_seconds(requests_per_minute: float | None) -> float:
    if requests_per_minute is None:
        return 0.0
    if requests_per_minute <= 0:
        raise ValueError("requests_per_minute must be greater than zero.")
    return 60 / requests_per_minute


def route_key(pair_from: str, pair_to: str) -> str:
    return f"{pair_from}:{pair_to}"


def failed_route(pair: dict[str, Any], exc: Exception) -> dict[str, Any]:
    return {
        "pair_from": pair["pair_from"],
        "pair_to": pair["pair_to"],
        "from_name": pair["from_name"],
        "to_name": pair["to_name"],
        "from_coord": [pair["from_lon"], pair["from_lat"]],
        "to_coord": [pair["to_lon"], pair["to_lat"]],
        "trip_count": pair["trips"],
        "distance_meters": None,
        "duration": None,
        "duration_seconds": None,
        "static_duration": None,
        "weight": None,
        "weight_name": None,
        "geometry": None,
        "code": "FetchError",
        "uuid": None,
        "error": {
            "type": type(exc).__name__,
            "message": str(exc),
        },
    }


def serializable_route_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "from_coord": json.dumps(row["from_coord"]),
        "to_coord": json.dumps(row["to_coord"]),
        "geometry": json.dumps(row["geometry"]),
        "error": json.dumps(row["error"]),
    }
