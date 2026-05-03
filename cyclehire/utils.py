import os
import tempfile
from pathlib import Path

import polars as pl


def write_parquet_atomic(frame: pl.DataFrame, destination: Path) -> None:
    """Avoid leaving a corrupt final Parquet file if writing is interrupted."""
    temp_fd, temp_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
    )
    os.close(temp_fd)
    temp_path = Path(temp_name)
    try:
        frame.write_parquet(temp_path)
        temp_path.replace(destination)
    finally:
        temp_path.unlink(missing_ok=True)
