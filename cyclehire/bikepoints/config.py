from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class BikePointsConfig:
    data_dir: Path
    sample_dates: tuple[date, ...]
