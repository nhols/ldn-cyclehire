from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RawPipelineConfig:
    data_dir: Path
    dry_run: bool = False
    limit: int | None = None
    include_zero_byte: bool = False
