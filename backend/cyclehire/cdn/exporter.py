from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import gzip
import hashlib
import json
import logging
from pathlib import Path
from typing import cast

import polars as pl

from cyclehire.cdn.config import CdnExportConfig, RouteProvider
from cyclehire.cdn.models import (
    BikepointPayload,
    BikepointsPayload,
    DayFileEntry,
    JsonPayload,
    RouteGeometry,
    RouteShardFileEntry,
    RouteShardPayload,
    StaticDayPayload,
    StaticRouteMatch,
    StaticManifestPayload,
    StaticTripPayload,
)
from cyclehire.cdn.paths import (
    bikepoints_path,
    compressed_manifest_path,
    day_path,
    manifest_path,
    route_shard_path,
)
from cyclehire.dashboard.models import MatchedTripRow
from cyclehire.dashboard.playback import (
    PlaybackDataStore,
    activity_payload,
    seconds_since_midnight,
    station_payload,
)
from cyclehire.routes.paths import google_bicycle_routes_parquet_path, mapbox_cycling_routes_parquet_path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RouteRecord:
    pair_from: str
    pair_to: str
    coordinates: list[list[float]]
    trip_count: int
    distance_meters: float | None

    @property
    def key(self) -> str:
        return f"{self.pair_from}|{self.pair_to}"


@dataclass(frozen=True)
class RouteShard:
    shard_id: str
    routes: list[RouteRecord]
    raw_bytes: int


@dataclass(frozen=True)
class WrittenJson:
    path: Path
    gzip_path: Path
    digest: str
    bytes: int
    gzip_bytes: int


def run_cdn_export(config: CdnExportConfig) -> None:
    store = PlaybackDataStore(config.data_dir)
    route_records = route_records_for_provider(config)
    route_lookup = {record.key: record for record in route_records}
    output_dir = config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    selected_days = select_export_days(store, config.dates, config.limit_days)
    logger.info("Exporting %s playback day(s) to %s", len(selected_days), output_dir)

    provider_name = route_provider_name(config.route_provider)
    route_shards = build_route_shards(
        route_records,
        target_gzip_bytes=config.route_shard_target_gzip_bytes,
        compression_ratio=config.route_shard_compression_ratio,
    )
    route_shard_index = {
        record.key: shard.shard_id
        for shard in route_shards
        for record in shard.routes
    }

    bikepoints_payload = bikepoint_payload(store)
    bikepoints_file = write_hashed_json(output_dir, bikepoints_path(), bikepoints_payload)

    day_files: list[DayFileEntry] = []
    for playback_date in selected_days:
        payload = day_payload(store, playback_date, route_lookup, route_shard_index)
        written = write_hashed_json(output_dir, day_path(playback_date.isoformat()), payload)
        day_files.append(
            {
                "date": playback_date.isoformat(),
                "path": str(written.path.relative_to(output_dir)),
                "gzipPath": str(written.gzip_path.relative_to(output_dir)),
                "hash": written.digest,
                "trips": payload["summary"]["matchedTrips"],
                "routedTrips": payload["summary"]["routedTrips"],
                "bytes": written.bytes,
                "gzipBytes": written.gzip_bytes,
            }
        )
        logger.info(
            "Exported %s with %s trips, %s routed trips",
            playback_date.isoformat(),
            payload["summary"]["matchedTrips"],
            payload["summary"]["routedTrips"],
        )

    shard_files = write_route_shards(output_dir, provider_name, route_shards)

    manifest: StaticManifestPayload = {
        "version": 1,
        "generatedAt": datetime.now(UTC).isoformat(),
        "dateRange": store.date_range(),
        "files": {
            "routes": {
                "provider": provider_name,
                "version": f"{provider_name}-cycling-v1",
                "encoding": "geojson-coordinate-array",
                "shardStrategy": "cache-order-byte-pack-v1",
                "shardTemplate": f"routes/{provider_name}-cycling-v1/{{shard}}.json",
                "gzipShardTemplate": f"routes/{provider_name}-cycling-v1/{{shard}}.json.gz",
                "shardTargetGzipBytes": config.route_shard_target_gzip_bytes,
                "estimatedCompressionRatio": config.route_shard_compression_ratio,
                "shardCount": len(route_shards),
                "routeCount": len(route_records),
                "bytes": sum(file["bytes"] for file in shard_files),
                "gzipBytes": sum(file["gzipBytes"] for file in shard_files),
                "shards": shard_files,
            },
            "bikepoints": {
                "path": str(bikepoints_file.path.relative_to(output_dir)),
                "gzipPath": str(bikepoints_file.gzip_path.relative_to(output_dir)),
                "hash": bikepoints_file.digest,
                "bytes": bikepoints_file.bytes,
                "gzipBytes": bikepoints_file.gzip_bytes,
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
    route_lookup: dict[str, RouteRecord],
    route_shard_index: dict[str, str],
) -> StaticDayPayload:
    frames = store.playback_frames(playback_date)
    trips = compact_trip_payload(frames.matched, route_lookup, route_shard_index)
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


def compact_trip_payload(
    trips: pl.DataFrame,
    route_lookup: dict[str, RouteRecord],
    route_shard_index: dict[str, str],
) -> list[StaticTripPayload]:
    payload: list[StaticTripPayload] = []
    for raw_row in trips.sort("start_at").iter_rows(named=True):
        row = cast(MatchedTripRow, raw_row)
        start_seconds = seconds_since_midnight(row["start_at"])
        end_seconds = seconds_since_midnight(row["end_at"])
        if end_seconds < start_seconds:
            end_seconds = start_seconds + int(row["duration_seconds"])

        route_key = route_key_for_static_trip(row, route_lookup, route_shard_index)
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
                "routeKey": route_key["routeKey"] if route_key else None,
                "routeShard": route_key["routeShard"] if route_key else None,
                "routeDistanceMeters": route_key["routeDistanceMeters"] if route_key else None,
                "routeReversed": route_key["routeReversed"] if route_key else False,
            }
        )
    return payload


def route_key_for_static_trip(
    row: MatchedTripRow,
    route_lookup: dict[str, RouteRecord],
    route_shard_index: dict[str, str],
) -> StaticRouteMatch | None:
    start_id = row["start_bikepoint_id"]
    end_id = row["end_bikepoint_id"]
    if not start_id or not end_id or start_id == end_id:
        return None

    pair_from = min(start_id, end_id)
    pair_to = max(start_id, end_id)
    route_key = f"{pair_from}|{pair_to}"
    if route_key not in route_lookup:
        return None
    shard_id = route_shard_index[route_key]
    return {
        "routeKey": route_key,
        "routeReversed": start_id != pair_from,
        "routeShard": shard_id,
        "routeDistanceMeters": route_lookup[route_key].distance_meters,
    }


def route_payload(route_records: list[RouteRecord]) -> RouteShardPayload:
    routes = {
        record.key: record.coordinates
        for record in route_records
    }
    return {
        "version": 1,
        "encoding": "geojson-coordinate-array",
        "routes": routes,
    }


def build_route_shards(
    route_records: list[RouteRecord],
    target_gzip_bytes: int,
    compression_ratio: float,
) -> list[RouteShard]:
    if target_gzip_bytes <= 0:
        raise ValueError("route_shard_target_gzip_bytes must be greater than zero")
    if compression_ratio <= 0:
        raise ValueError("route_shard_compression_ratio must be greater than zero")

    target_raw_bytes = int(target_gzip_bytes * compression_ratio)
    shards: list[RouteShard] = []
    current: list[RouteRecord] = []
    current_bytes = 0

    for record in route_records:
        record_bytes = route_record_estimated_bytes(record)
        if current and current_bytes + record_bytes > target_raw_bytes:
            shard_id = f"shard-{len(shards):04d}"
            shards.append(RouteShard(shard_id=shard_id, routes=current, raw_bytes=current_bytes))
            current = []
            current_bytes = 0
        current.append(record)
        current_bytes += record_bytes

    if current:
        shard_id = f"shard-{len(shards):04d}"
        shards.append(RouteShard(shard_id=shard_id, routes=current, raw_bytes=current_bytes))

    return shards


def route_record_estimated_bytes(record: RouteRecord) -> int:
    return len(json.dumps({record.key: record.coordinates}, separators=(",", ":")))


def write_route_shards(output_dir: Path, provider: str, route_shards: list[RouteShard]) -> list[RouteShardFileEntry]:
    shard_files: list[RouteShardFileEntry] = []
    total_shards = len(route_shards)
    for shard in route_shards:
        payload = route_payload(shard.routes)
        written = write_hashed_json(output_dir, route_shard_path(provider, shard.shard_id), payload)
        logger.info(
            "Exported route shard %s/%s %s with %s routes, %.1f MB raw, %.1f MB gzip",
            len(shard_files) + 1,
            total_shards,
            shard.shard_id,
            len(shard.routes),
            written.bytes / 1_000_000,
            written.gzip_bytes / 1_000_000,
        )
        shard_files.append(
            {
                "id": shard.shard_id,
                "path": str(written.path.relative_to(output_dir)),
                "gzipPath": str(written.gzip_path.relative_to(output_dir)),
                "hash": written.digest,
                "routeCount": len(shard.routes),
                "bytes": written.bytes,
                "gzipBytes": written.gzip_bytes,
            }
        )

    logger.info("Exported %s route shards with %s routes", len(route_shards), sum(len(shard.routes) for shard in route_shards))
    return shard_files


def route_records_for_provider(config: CdnExportConfig) -> list[RouteRecord]:
    if config.route_provider == RouteProvider.google:
        return load_route_records(config.data_dir / google_bicycle_routes_parquet_path())
    if config.route_provider == RouteProvider.mapbox:
        return load_route_records(config.data_dir / mapbox_cycling_routes_parquet_path())

    records = {record.key: record for record in load_route_records(config.data_dir / google_bicycle_routes_parquet_path())}
    records.update({record.key: record for record in load_route_records(config.data_dir / mapbox_cycling_routes_parquet_path())})
    return list(records.values())


def load_route_records(path: Path) -> list[RouteRecord]:
    if not path.exists():
        return []

    routes = pl.read_parquet(path).filter(
        (pl.col("error").is_null() | (pl.col("error") == "null"))
        & pl.col("geometry").is_not_null()
        & (pl.col("geometry") != "null")
    )
    records: list[RouteRecord] = []
    for row in routes.iter_rows(named=True):
        geometry = cast(RouteGeometry, json.loads(cast(str, row["geometry"])))
        coordinates = geometry.get("coordinates")
        if not coordinates:
            continue
        records.append(
            RouteRecord(
                pair_from=cast(str, row["pair_from"]),
                pair_to=cast(str, row["pair_to"]),
                coordinates=coordinates,
                trip_count=int(row.get("trip_count") or 0),
                distance_meters=cast(float | None, row.get("distance_meters")),
            )
        )
    return records


def route_provider_name(route_provider: RouteProvider) -> str:
    return route_provider.value


def bikepoint_payload(store: PlaybackDataStore) -> BikepointsPayload:
    stations: list[BikepointPayload] = []
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


def write_hashed_json(output_dir: Path, relative_path: Path, payload: JsonPayload) -> WrittenJson:
    content = json_content(payload)
    digest = hashlib.sha256(content).hexdigest()[:12]
    path = output_dir / path_with_hash(relative_path, digest)
    gzip_path = path.with_name(f"{path.name}.gz")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    gzip_content = gzip.compress(content, compresslevel=9, mtime=0)
    gzip_path.write_bytes(gzip_content)
    return WrittenJson(
        path=path,
        gzip_path=gzip_path,
        digest=digest,
        bytes=len(content),
        gzip_bytes=len(gzip_content),
    )


def path_with_hash(path: Path, digest: str) -> Path:
    return path.with_name(f"{path.stem}.{digest}{path.suffix}")


def write_json(path: Path, payload: JsonPayload) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(json_content(payload))
    return path


def write_json_gzip(path: Path, payload: JsonPayload) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(json_content(payload), compresslevel=9, mtime=0))
    return path


def json_content(payload: JsonPayload) -> bytes:
    return json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
