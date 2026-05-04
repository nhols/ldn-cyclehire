from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from functools import cached_property
import json
from pathlib import Path
from typing import cast

import polars as pl

from cyclehire.bikepoints.paths import bikepoints_parquet_path
from cyclehire.dashboard.models import (
    ActivityPoint,
    DateRangePayload,
    DateRangeRow,
    MatchedTripRow,
    PlaybackPayload,
    RouteCacheRow,
    RouteGeometry,
    RouteLookup,
    RoutePath,
    StationMatchRow,
    StationPayload,
    StationSourceRow,
    TripPayload,
)
from cyclehire.routes.paths import google_bicycle_routes_parquet_path
from cyclehire.stations import BikePointLookup, make_station_key, station_key, string_or_none


@dataclass(frozen=True)
class PlaybackDataStore:
    data_dir: Path

    @cached_property
    def trip_files(self) -> tuple[Path, ...]:
        files = tuple(sorted((self.data_dir / "validated" / "trips_by_file").glob("*.parquet")))
        if not files:
            raise FileNotFoundError(f"No validated trip Parquet files under {self.data_dir}")
        return files

    @cached_property
    def bikepoints(self) -> pl.DataFrame:
        path = self.data_dir / bikepoints_parquet_path()
        if not path.exists():
            raise FileNotFoundError(f"BikePoint metadata not found at {path}. Run `cyclehire bikepoints` first.")
        return pl.read_parquet(path)

    @cached_property
    def bikepoint_lookup(self) -> BikePointLookup:
        return BikePointLookup.from_frame(self.bikepoints)

    @cached_property
    def route_lookup(self) -> RouteLookup:
        path = self.data_dir / google_bicycle_routes_parquet_path()
        if not path.exists():
            return {}

        routes = pl.read_parquet(path).filter(
            (pl.col("error").is_null() | (pl.col("error") == "null"))
            & pl.col("geometry").is_not_null()
            & (pl.col("geometry") != "null")
        )
        lookup: RouteLookup = {}
        for raw_row in routes.iter_rows(named=True):
            row = cast(RouteCacheRow, raw_row)
            geometry = cast(RouteGeometry, json.loads(row["geometry"]))
            coordinates = geometry.get("coordinates")
            if coordinates:
                lookup[(row["pair_from"], row["pair_to"])] = coordinates
        return lookup

    def date_range(self) -> DateRangePayload:
        summary_frame = cast(
            pl.DataFrame,
            self._trip_scan()
            .select(
                pl.col("start_at").min().alias("min_start"),
                pl.col("start_at").max().alias("max_start"),
                pl.len().alias("trip_count"),
            )
            .collect(),
        )
        summary = cast(DateRangeRow, summary_frame.row(0, named=True))
        min_start = summary["min_start"]
        max_start = summary["max_start"]
        return {
            "minDate": min_start.date().isoformat(),
            "maxDate": max_start.date().isoformat(),
            "tripCount": summary["trip_count"],
        }

    def playback_day(self, day: date) -> PlaybackPayload:
        frames = self.playback_frames(day)
        if frames.trips.is_empty():
            return {
                "date": day.isoformat(),
                "stations": [],
                "trips": [],
                "activity": empty_activity_bins(),
                "summary": {
                    "totalTrips": 0,
                    "matchedTrips": 0,
                    "unmatchedTrips": 0,
                    "stationCount": 0,
                },
            }

        return {
            "date": day.isoformat(),
            "stations": station_payload(frames.station_matches),
            "trips": trip_payload(frames.matched, self.route_lookup),
            "activity": activity_payload(frames.matched),
            "summary": {
                "totalTrips": frames.trips.height,
                "matchedTrips": frames.matched.height,
                "unmatchedTrips": frames.trips.height - frames.matched.height,
                "stationCount": frames.station_matches.filter(pl.col("lat").is_not_null()).height,
                "routedTrips": routed_trip_count(frames.matched, self.route_lookup),
            },
        }

    def playback_frames(self, day: date) -> PlaybackFrames:
        trips = self._trips_for_day(day)
        station_matches = self._station_matches_for_trips(trips) if not trips.is_empty() else empty_station_matches()
        enriched = (
            trips.with_columns(
                start_station_key=station_key("start_station_id", "start_station_name"),
                end_station_key=station_key("end_station_id", "end_station_name"),
            )
            .join(
                station_matches.select(
                    pl.col("station_key").alias("start_station_key"),
                    pl.col("station_id").alias("matched_start_station_id"),
                    pl.col("station_name").alias("matched_start_station_name"),
                    pl.col("bikepoint_id").alias("start_bikepoint_id"),
                    pl.col("lat").alias("start_lat"),
                    pl.col("lon").alias("start_lon"),
                    pl.col("match_method").alias("start_match_method"),
                ),
                on="start_station_key",
                how="left",
            )
            .join(
                station_matches.select(
                    pl.col("station_key").alias("end_station_key"),
                    pl.col("station_id").alias("matched_end_station_id"),
                    pl.col("station_name").alias("matched_end_station_name"),
                    pl.col("bikepoint_id").alias("end_bikepoint_id"),
                    pl.col("lat").alias("end_lat"),
                    pl.col("lon").alias("end_lon"),
                    pl.col("match_method").alias("end_match_method"),
                ),
                on="end_station_key",
                how="left",
            )
        )
        matched = enriched.filter(
            pl.col("start_lat").is_not_null()
            & pl.col("start_lon").is_not_null()
            & pl.col("end_lat").is_not_null()
            & pl.col("end_lon").is_not_null()
        )
        return PlaybackFrames(trips=trips, station_matches=station_matches, matched=matched)

    def _trip_scan(self) -> pl.LazyFrame:
        return pl.scan_parquet([str(path) for path in self.trip_files], extra_columns="ignore")

    def _trips_for_day(self, day: date) -> pl.DataFrame:
        start_at = datetime.combine(day, time.min)
        end_at = datetime.combine(day, time.max)
        return cast(
            pl.DataFrame,
            self._trip_scan()
            .filter((pl.col("start_at") >= start_at) & (pl.col("start_at") <= end_at))
            .select(
                "journey_id",
                "bike_id",
                "bike_model",
                "start_at",
                "end_at",
                "start_station_id",
                "start_station_name",
                "end_station_id",
                "end_station_name",
                "duration_seconds",
            )
            .filter(pl.col("end_station_name").is_not_null())
            .collect(),
        )

    def _station_matches_for_trips(self, trips: pl.DataFrame) -> pl.DataFrame:
        stations = (
            pl.concat(
                [
                    trips.select(
                        pl.col("start_station_id").alias("station_id"),
                        pl.col("start_station_name").alias("station_name"),
                    ),
                    trips.select(
                        pl.col("end_station_id").alias("station_id"),
                        pl.col("end_station_name").alias("station_name"),
                    ),
                ]
            )
            .filter(pl.col("station_name").is_not_null())
            .group_by("station_id", "station_name")
            .agg(pl.len().alias("trip_count"))
            .sort("trip_count", descending=True)
        )

        rows: list[StationMatchRow] = []
        for raw_station in stations.iter_rows(named=True):
            station = cast(StationSourceRow, raw_station)
            station_id = string_or_none(station["station_id"])
            station_name = string_or_none(station["station_name"])
            if station_name is None:
                continue
            match = self.bikepoint_lookup.match(station_id, station_name)
            rows.append(
                {
                    "station_key": make_station_key(station_id, station_name),
                    "station_id": station_id,
                    "station_name": station_name,
                    "trip_count": station["trip_count"],
                    "match_method": match.method if match else None,
                    "bikepoint_id": match.row["bikepoint_id"] if match else None,
                    "lat": match.row["lat"] if match else None,
                    "lon": match.row["lon"] if match else None,
                }
            )
        return pl.DataFrame(rows)


@dataclass(frozen=True)
class PlaybackFrames:
    trips: pl.DataFrame
    station_matches: pl.DataFrame
    matched: pl.DataFrame


def empty_station_matches() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "station_key": pl.String,
            "station_id": pl.String,
            "station_name": pl.String,
            "trip_count": pl.Int64,
            "match_method": pl.String,
            "bikepoint_id": pl.String,
            "lat": pl.Float64,
            "lon": pl.Float64,
        }
    )


def station_payload(stations: pl.DataFrame) -> list[StationPayload]:
    matched = stations.filter(pl.col("lat").is_not_null() & pl.col("lon").is_not_null())
    payload: list[StationPayload] = []
    for raw_row in matched.iter_rows(named=True):
        row = cast(StationMatchRow, raw_row)
        lat = row["lat"]
        lon = row["lon"]
        if lat is None or lon is None:
            continue
        station: StationPayload = {
            "id": row["station_key"],
            "stationId": row["station_id"],
            "name": row["station_name"],
            "bikepointId": row["bikepoint_id"],
            "coord": [lon, lat],
            "tripCount": row["trip_count"],
            "matchMethod": row["match_method"],
        }
        payload.append(station)
    return payload


def trip_payload(
    trips: pl.DataFrame,
    route_lookup: RouteLookup,
) -> list[TripPayload]:
    rows: list[TripPayload] = []
    for raw_row in trips.sort("start_at").iter_rows(named=True):
        row = cast(MatchedTripRow, raw_row)
        start_seconds = seconds_since_midnight(row["start_at"])
        end_seconds = seconds_since_midnight(row["end_at"])
        if end_seconds < start_seconds:
            end_seconds = start_seconds + int(row["duration_seconds"])
        route_path = route_path_for_trip(row, route_lookup)
        rows.append(
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
                "path": route_path,
            }
        )
    return rows


def routed_trip_count(
    trips: pl.DataFrame,
    route_lookup: RouteLookup,
) -> int:
    if not route_lookup or trips.is_empty():
        return 0
    return sum(
        1 for raw_row in trips.iter_rows(named=True) if route_path_for_trip(cast(MatchedTripRow, raw_row), route_lookup)
    )


def route_path_for_trip(
    row: MatchedTripRow,
    route_lookup: RouteLookup,
) -> RoutePath | None:
    start_id = row["start_bikepoint_id"]
    end_id = row["end_bikepoint_id"]
    if not start_id or not end_id or start_id == end_id:
        return None

    pair_from = min(start_id, end_id)
    pair_to = max(start_id, end_id)
    path = route_lookup.get((pair_from, pair_to))
    if not path:
        return None
    if start_id == pair_from:
        return path
    return list(reversed(path))


def route_key_for_trip(
    row: MatchedTripRow,
    route_lookup: RouteLookup,
) -> tuple[str, bool] | None:
    start_id = row["start_bikepoint_id"]
    end_id = row["end_bikepoint_id"]
    if not start_id or not end_id or start_id == end_id:
        return None

    pair_from = min(start_id, end_id)
    pair_to = max(start_id, end_id)
    if (pair_from, pair_to) not in route_lookup:
        return None
    return f"{pair_from}|{pair_to}", start_id != pair_from


def activity_payload(trips: pl.DataFrame, bin_seconds: int = 300) -> list[ActivityPoint]:
    bins = empty_activity_bins(bin_seconds)
    if trips.is_empty():
        return bins

    counts = [0 for _ in bins]
    max_time = 24 * 60 * 60 - 1
    for raw_row in trips.iter_rows(named=True):
        row = cast(MatchedTripRow, raw_row)
        start = seconds_since_midnight(row["start_at"])
        end = min(max_time, seconds_since_midnight(row["end_at"]))
        if end < start:
            end = min(max_time, start + int(row["duration_seconds"]))
        start_index = max(0, start // bin_seconds)
        end_index = min(len(counts) - 1, end // bin_seconds)
        for index in range(start_index, end_index + 1):
            counts[index] += 1

    return [
        {
            "time": bin_item["time"],
            "activeTrips": count,
        }
        for bin_item, count in zip(bins, counts)
    ]


def empty_activity_bins(bin_seconds: int = 300) -> list[ActivityPoint]:
    return [{"time": second, "activeTrips": 0} for second in range(0, 24 * 60 * 60, bin_seconds)]


def seconds_since_midnight(value: datetime) -> int:
    return value.hour * 3600 + value.minute * 60 + value.second
