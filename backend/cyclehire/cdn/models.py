from __future__ import annotations

from collections.abc import Mapping
from typing import NotRequired, TypeAlias, TypedDict

from cyclehire.dashboard.models import ActivityPoint, Coordinate, DateRangePayload, StationPayload


JsonPayload: TypeAlias = Mapping[str, object]


class StaticRouteMatch(TypedDict):
    routeKey: str
    routeReversed: bool
    routeShard: str
    routeDistanceMeters: float | None


class StaticTripPayload(TypedDict):
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
    routeKey: str | None
    routeShard: str | None
    routeDistanceMeters: float | None
    routeReversed: bool


class StaticPlaybackSummary(TypedDict):
    totalTrips: int
    matchedTrips: int
    unmatchedTrips: int
    stationCount: int
    routedTrips: int


class StaticDayPayload(TypedDict):
    date: str
    stations: list[StationPayload]
    trips: list[StaticTripPayload]
    activity: list[ActivityPoint]
    summary: StaticPlaybackSummary


class DayFileEntry(TypedDict):
    date: str
    path: str
    gzipPath: str
    trips: int
    routedTrips: int
    bytes: int
    gzipBytes: int


class RouteShardPayload(TypedDict):
    version: int
    encoding: str
    routes: dict[str, list[Coordinate]]


class RouteShardFileEntry(TypedDict):
    id: str
    path: str
    gzipPath: str
    routeCount: int
    bytes: int
    gzipBytes: int


class RoutesManifestEntry(TypedDict):
    provider: str
    version: str
    encoding: str
    shardStrategy: str
    shardTemplate: str
    gzipShardTemplate: str
    shardTargetGzipBytes: int
    estimatedCompressionRatio: float
    shardCount: int
    routeCount: int
    bytes: int
    gzipBytes: int
    shards: list[RouteShardFileEntry]


class BikepointPayload(TypedDict):
    bikepointId: str
    bikepointNumber: int | None
    name: str
    terminalName: str | None
    coord: Coordinate
    installed: bool | None
    locked: bool | None
    temporary: bool | None
    nbDocks: int | None


class BikepointsPayload(TypedDict):
    version: int
    stations: list[BikepointPayload]


class BikepointsManifestEntry(TypedDict):
    path: str
    gzipPath: str
    bytes: int
    gzipBytes: int
    stationCount: int


class ManifestFiles(TypedDict):
    routes: RoutesManifestEntry
    bikepoints: BikepointsManifestEntry


class StaticManifestPayload(TypedDict):
    version: int
    generatedAt: str
    dateRange: DateRangePayload
    files: ManifestFiles
    days: list[DayFileEntry]


class RouteGeometry(TypedDict):
    coordinates: NotRequired[list[Coordinate]]
    type: NotRequired[str]
