from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from functools import cached_property
from pathlib import Path
from typing import Any

import polars as pl

from cyclehire.bikepoints.paths import bikepoints_parquet_path
from cyclehire.bikepoints.stage import name_key, numeric_key


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
            raise FileNotFoundError(
                f"BikePoint metadata not found at {path}. Run `cyclehire bikepoints` first."
            )
        return pl.read_parquet(path)

    @cached_property
    def bikepoint_lookup(self) -> BikePointLookup:
        return BikePointLookup.from_frame(self.bikepoints)

    def date_range(self) -> dict[str, Any]:
        summary = (
            self._trip_scan()
            .select(
                pl.col("start_at").min().alias("min_start"),
                pl.col("start_at").max().alias("max_start"),
                pl.len().alias("trip_count"),
            )
            .collect()
            .row(0, named=True)
        )
        min_start = summary["min_start"]
        max_start = summary["max_start"]
        return {
            "minDate": min_start.date().isoformat(),
            "maxDate": max_start.date().isoformat(),
            "tripCount": summary["trip_count"],
        }

    def playback_day(self, day: date) -> dict[str, Any]:
        trips = self._trips_for_day(day)
        if trips.is_empty():
            return {
                "date": day.isoformat(),
                "stations": [],
                "trips": [],
                "summary": {
                    "totalTrips": 0,
                    "matchedTrips": 0,
                    "unmatchedTrips": 0,
                    "stationCount": 0,
                },
            }

        station_matches = self._station_matches_for_trips(trips)
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

        return {
            "date": day.isoformat(),
            "stations": station_payload(station_matches),
            "trips": trip_payload(matched),
            "summary": {
                "totalTrips": trips.height,
                "matchedTrips": matched.height,
                "unmatchedTrips": trips.height - matched.height,
                "stationCount": station_matches.filter(pl.col("lat").is_not_null()).height,
            },
        }

    def _trip_scan(self) -> pl.LazyFrame:
        return pl.scan_parquet([str(path) for path in self.trip_files], extra_columns="ignore")

    def _trips_for_day(self, day: date) -> pl.DataFrame:
        start_at = datetime.combine(day, time.min)
        end_at = datetime.combine(day, time.max)
        return (
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
            .collect()
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

        rows: list[dict[str, Any]] = []
        for station in stations.iter_rows(named=True):
            station_id = string_or_none(station["station_id"])
            station_name = string_or_none(station["station_name"])
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
class BikePointMatch:
    method: str
    row: dict[str, Any]


@dataclass(frozen=True)
class BikePointLookup:
    bikepoint_number: dict[str, dict[str, Any]]
    terminal_name: dict[str, dict[str, Any]]
    common_name: dict[str, dict[str, Any]]

    @classmethod
    def from_frame(cls, frame: pl.DataFrame) -> BikePointLookup:
        return cls(
            bikepoint_number=lookup(frame, "bikepoint_number_key"),
            terminal_name=lookup(frame, "terminal_name_key"),
            common_name=lookup(frame, "common_name_key"),
        )

    def match(self, station_id: str | None, station_name: str | None) -> BikePointMatch | None:
        id_key = numeric_key(station_id)
        station_name_key = name_key(station_name)
        if id_key and id_key in self.bikepoint_number:
            return BikePointMatch("bikepoint_number", self.bikepoint_number[id_key])
        if id_key and id_key in self.terminal_name:
            return BikePointMatch("terminal_name", self.terminal_name[id_key])
        if station_name_key and station_name_key in self.common_name:
            return BikePointMatch("common_name", self.common_name[station_name_key])
        return None


def lookup(frame: pl.DataFrame, key_column: str) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in frame.filter(pl.col(key_column).is_not_null()).iter_rows(named=True):
        key = row[key_column]
        if key and key not in rows:
            rows[key] = row
    return rows


def station_key(id_column: str, name_column: str) -> pl.Expr:
    return (
        pl.col(id_column).fill_null("")
        + pl.lit("\u001f")
        + pl.col(name_column).fill_null("")
    )


def make_station_key(station_id: str | None, station_name: str | None) -> str:
    return f"{station_id or ''}\u001f{station_name or ''}"


def station_payload(stations: pl.DataFrame) -> list[dict[str, Any]]:
    matched = stations.filter(pl.col("lat").is_not_null() & pl.col("lon").is_not_null())
    return [
        {
            "id": row["station_key"],
            "stationId": row["station_id"],
            "name": row["station_name"],
            "bikepointId": row["bikepoint_id"],
            "coord": [row["lon"], row["lat"]],
            "tripCount": row["trip_count"],
            "matchMethod": row["match_method"],
        }
        for row in matched.iter_rows(named=True)
    ]


def trip_payload(trips: pl.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in trips.sort("start_at").iter_rows(named=True):
        start_seconds = seconds_since_midnight(row["start_at"])
        end_seconds = seconds_since_midnight(row["end_at"])
        if end_seconds < start_seconds:
            end_seconds = start_seconds + int(row["duration_seconds"])
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
            }
        )
    return rows


def seconds_since_midnight(value: datetime) -> int:
    return value.hour * 3600 + value.minute * 60 + value.second


def string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    string_value = str(value).strip()
    return string_value or None
