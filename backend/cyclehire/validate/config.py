from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ValidatePipelineConfig:
    data_dir: Path
    dry_run: bool = False
    limit: int | None = None
    retry_failed: bool = False
    force: bool = False
