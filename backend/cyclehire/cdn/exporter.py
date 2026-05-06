from __future__ import annotations

from datetime import UTC, date, datetime
import gzip
import json
import logging
from pathlib import Path
from typing import Any, cast

import polars as pl

from cyclehire.cdn.config import CdnExportConfig, RouteProvider
from cyclehire.cdn.paths import (
    bikepoints_path,
    compressed_bikepoints_path,
    compressed_day_path,
    compressed_manifest_path,
    compressed_routes_path,
    day_path,
    manifest_path,
    routes_path,
)
from cyclehire.dashboard.models import MatchedTripRow
from cyclehire.dashboard.playback import (
    PlaybackDataStore,
    activity_payload,
    load_route_lookup,
    route_key_for_trip,
    seconds_since_midnight,
    station_payload,
)
from cyclehire.routes.paths import google_bicycle_routes_parquet_path, mapbox_cycling_routes_parquet_path

logger = logging.getLogger(__name__)


def run_cdn_export(config: CdnExportConfig) -> None:
    store = PlaybackDataStore(config.data_dir)
    route_lookup = route_lookup_for_provider(config)
    output_dir = config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    selected_days = select_export_days(store, config.dates, config.limit_days)
    logger.info("Exporting %s playback day(s) to %s", len(selected_days), output_dir)

    routes_payload = route_payload(route_lookup)
    routes_file = write_json(output_dir / routes_path(), routes_payload)
    compressed_routes_file = write_json_gzip(output_dir / compressed_routes_path(), routes_payload)

    bikepoints_payload = bikepoint_payload(store)
    bikepoints_file = write_json(output_dir / bikepoints_path(), bikepoints_payload)
    compressed_bikepoints_file = write_json_gzip(output_dir / compressed_bikepoints_path(), bikepoints_payload)

    day_files = []
    for playback_date in selected_days:
        payload = day_payload(store, playback_date, route_lookup)
        written = write_json(output_dir / day_path(playback_date.isoformat()), payload)
        compressed_written = write_json_gzip(output_dir / compressed_day_path(playback_date.isoformat()), payload)
        day_files.append(
            {
                "date": playback_date.isoformat(),
                "path": str(day_path(playback_date.isoformat())),
                "gzipPath": str(compressed_day_path(playback_date.isoformat())),
                "trips": payload["summary"]["matchedTrips"],
                "routedTrips": payload["summary"]["routedTrips"],
                "bytes": written.stat().st_size,
                "gzipBytes": compressed_written.stat().st_size,
            }
        )
        logger.info(
            "Exported %s with %s trips, %s routed trips",
            playback_date.isoformat(),
            payload["summary"]["matchedTrips"],
            payload["summary"]["routedTrips"],
        )

    manifest = {
        "version": 1,
        "generatedAt": datetime.now(UTC).isoformat(),
        "dateRange": store.date_range(),
        "files": {
            "routes": {
                "path": str(routes_path()),
                "gzipPath": str(compressed_routes_path()),
                "bytes": routes_file.stat().st_size,
                "gzipBytes": compressed_routes_file.stat().st_size,
                "routeCount": len(route_lookup),
            },
            "bikepoints": {
                "path": str(bikepoints_path()),
                "gzipPath": str(compressed_bikepoints_path()),
                "bytes": bikepoints_file.stat().st_size,
                "gzipBytes": compressed_bikepoints_file.stat().st_size,
                "stationCount": store.bikepoints.height,
            },
        },
        "days": day_files,
    }
    write_json(output_dir / manifest_path(), manifest)
    write_json_gzip(output_dir / compressed_manifest_path(), manifest)
    logger.info("Wrote CDN manifest to %s", output_dir / manifest_path())


def select_export_days(
    store: PlaybackDataStore,
    dates: tuple[date, ...],
    limit_days: int | None,
) -> list[date]:
    if dates:
        selected = sorted(set(dates))
    else:
        days = cast(
            pl.DataFrame,
            store._trip_scan()
            .select(pl.col("start_at").dt.date().alias("day"))
            .unique()
            .sort("day")
            .collect(),
        )
        selected = [row[0] for row in days.iter_rows()]

    if limit_days is not None:
        return selected[:limit_days]
    return selected


def day_payload(
    store: PlaybackDataStore,
    playback_date: date,
    route_lookup: dict[tuple[str, str], list[list[float]]],
) -> dict[str, Any]:
    frames = store.playback_frames(playback_date)
    trips = compact_trip_payload(frames.matched, route_lookup)
    return {
        "date": playback_date.isoformat(),
        "stations": station_payload(frames.station_matches),
        "trips": trips,
        "activity": activity_payload(frames.matched),
        "summary": {
            "totalTrips": frames.trips.height,
            "matchedTrips": frames.matched.height,
            "unmatchedTrips": frames.trips.height - frames.matched.height,
            "stationCount": frames.station_matches.filter(pl.col("lat").is_not_null()).height,
            "routedTrips": sum(1 for trip in trips if trip["routeKey"] is not None),
        },
    }


def compact_trip_payload(trips: pl.DataFrame, route_lookup: dict[tuple[str, str], list[list[float]]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for raw_row in trips.sort("start_at").iter_rows(named=True):
        row = cast(MatchedTripRow, raw_row)
        start_seconds = seconds_since_midnight(row["start_at"])
        end_seconds = seconds_since_midnight(row["end_at"])
        if end_seconds < start_seconds:
            end_seconds = start_seconds + int(row["duration_seconds"])

        route_key = route_key_for_trip(row, route_lookup)
        payload.append(
            {
                "id": row["journey_id"],
                "bikeId": row["bike_id"],
                "bikeModel": row["bike_model"],
                "start": start_seconds,
                "end": end_seconds,
                "durationSeconds": row["duration_seconds"],
                "fromStationId": row["start_station_id"],
                "fromStationName": row["start_station_name"],
                "toStationId": row["end_station_id"],
                "toStationName": row["end_station_name"],
                "fromCoord": [row["start_lon"], row["start_lat"]],
                "toCoord": [row["end_lon"], row["end_lat"]],
                "routeKey": route_key[0] if route_key else None,
                "routeReversed": route_key[1] if route_key else False,
            }
        )
    return payload


def route_payload(route_lookup: dict[tuple[str, str], list[list[float]]]) -> dict[str, Any]:
    routes = {
        f"{pair_from}|{pair_to}": coordinates
        for (pair_from, pair_to), coordinates in sorted(route_lookup.items())
    }
    return {
        "version": 1,
        "encoding": "geojson-coordinate-array",
        "routes": routes,
    }


def route_lookup_for_provider(config: CdnExportConfig) -> dict[tuple[str, str], list[list[float]]]:
    if config.route_provider == RouteProvider.google:
        return load_route_lookup(config.data_dir / google_bicycle_routes_parquet_path())
    if config.route_provider == RouteProvider.mapbox:
        return load_route_lookup(config.data_dir / mapbox_cycling_routes_parquet_path())

    lookup = load_route_lookup(config.data_dir / google_bicycle_routes_parquet_path())
    lookup.update(load_route_lookup(config.data_dir / mapbox_cycling_routes_parquet_path()))
    return lookup


def bikepoint_payload(store: PlaybackDataStore) -> dict[str, Any]:
    stations = []
    for row in store.bikepoints.sort("bikepoint_id").iter_rows(named=True):
        stations.append(
            {
                "bikepointId": row["bikepoint_id"],
                "bikepointNumber": row["bikepoint_number"],
                "name": row["common_name"],
                "terminalName": row["terminal_name"],
                "coord": [row["lon"], row["lat"]],
                "installed": row["installed"],
                "locked": row["locked"],
                "temporary": row["temporary"],
                "nbDocks": row["nb_docks"],
            }
        )
    return {
        "version": 1,
        "stations": stations,
    }


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(",", ":"), default=str), encoding="utf-8")
    return path


def write_json_gzip(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
    with gzip.open(path, "wb", compresslevel=9) as file:
        file.write(content)
    return path
