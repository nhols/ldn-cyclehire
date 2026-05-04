from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class GoogleBicycleRoutesConfig:
    data_dir: Path
    api_key: str | None
    route_date: date | None = None
    limit: int = 10_000
    dry_run: bool = False
    sleep_seconds: float = 0.0
