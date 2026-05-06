from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from pathlib import Path


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
