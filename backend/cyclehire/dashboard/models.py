from __future__ import annotations

from datetime import datetime
from typing import NotRequired, TypedDict


Coordinate = list[float]
RoutePath = list[Coordinate]
RouteLookup = dict[tuple[str, str], RoutePath]


class DateRangePayload(TypedDict):
    minDate: str
    maxDate: str
    tripCount: int


class StationPayload(TypedDict):
    id: str
    stationId: str | None
    name: str
    bikepointId: str | None
    coord: Coordinate
    tripCount: int
    matchMethod: str | None


class TripPayload(TypedDict):
    id: str
    bikeId: str
    bikeModel: str | None
    start: int
    end: int
    durationSeconds: int
    fromStationId: str | None
    fromStationName: str
    toStationId: str | None
    toStationName: str
    fromCoord: Coordinate
    toCoord: Coordinate
    path: RoutePath | None


class ActivityPoint(TypedDict):
    time: int
    activeTrips: int


class PlaybackSummary(TypedDict):
    totalTrips: int
    matchedTrips: int
    unmatchedTrips: int
    stationCount: int
    routedTrips: NotRequired[int]


class PlaybackPayload(TypedDict):
    date: str
    stations: list[StationPayload]
    trips: list[TripPayload]
    activity: list[ActivityPoint]
    summary: PlaybackSummary


class RouteGeometry(TypedDict):
    coordinates: RoutePath
    type: str


class RouteCacheRow(TypedDict):
    pair_from: str
    pair_to: str
    geometry: str


class DateRangeRow(TypedDict):
    min_start: datetime
    max_start: datetime
    trip_count: int


class StationMatchRow(TypedDict):
    station_key: str
    station_id: str | None
    station_name: str
    trip_count: int
    match_method: str | None
    bikepoint_id: str | None
    lat: float | None
    lon: float | None


class StationSourceRow(TypedDict):
    station_id: str | None
    station_name: str | None
    trip_count: int


class MatchedTripRow(TypedDict):
    journey_id: str
    bike_id: str
    bike_model: str | None
    start_at: datetime
    end_at: datetime
    start_station_id: str | None
    start_station_name: str
    end_station_id: str | None
    end_station_name: str
    duration_seconds: int
    start_bikepoint_id: str | None
    end_bikepoint_id: str | None
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
