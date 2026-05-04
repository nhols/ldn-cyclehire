from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class CdnExportConfig:
    data_dir: Path
    output_dir: Path = Path("data") / "cdn"
    dates: tuple[date, ...] = ()
    limit_days: int | None = None
