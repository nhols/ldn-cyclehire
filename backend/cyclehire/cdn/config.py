from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from pathlib import Path

DEFAULT_ROUTE_SHARD_TARGET_GZIP_BYTES = 1_500_000
DEFAULT_ROUTE_SHARD_COMPRESSION_RATIO = 6.0


class RouteProvider(str, Enum):
    all = "all"
    google = "google"
    mapbox = "mapbox"


@dataclass(frozen=True)
class CdnExportConfig:
    data_dir: Path
    output_dir: Path = Path("data") / "cdn"
    dates: tuple[date, ...] = ()
    limit_days: int | None = None
    route_provider: RouteProvider = RouteProvider.all
    route_shard_target_gzip_bytes: int = DEFAULT_ROUTE_SHARD_TARGET_GZIP_BYTES
    route_shard_compression_ratio: float = DEFAULT_ROUTE_SHARD_COMPRESSION_RATIO
