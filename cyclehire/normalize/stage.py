import logging
import sqlite3
from pathlib import Path

from cyclehire.normalize.config import NormalizePipelineConfig
from cyclehire.normalize.paths import normalized_path_for
from cyclehire.normalize.tracker import mark_normalize_failed, mark_normalized, plan_normalize_work
from cyclehire.normalize.transforms import normalize_csv_frame, read_source_frame
from cyclehire.tracking import FileRecord, connect_tracking_db
from cyclehire.utils import write_parquet_atomic


LOGGER = logging.getLogger(__name__)
SCHEMA_VERSION = "trips_v1"


def run_normalize_pipeline(config: NormalizePipelineConfig) -> None:
    LOGGER.info("Starting normalize stage")
    LOGGER.info("Data directory: %s", config.data_dir)

    with connect_tracking_db(config.data_dir) as connection:
        files = plan_normalize_work(connection, config.retry_failed, config.force, config.limit)
        LOGGER.info("Files requiring normalization: %s", len(files))

        if config.dry_run:
            for item in files:
                LOGGER.info("Would normalize %s", item.source_key)
            LOGGER.info("Dry run complete; no normalized outputs written")
            return

        for index, item in enumerate(files, start=1):
            LOGGER.info(
                "Normalizing %s of %s: %s",
                index,
                len(files),
                item.source_key,
            )
            normalize_file(config.data_dir, connection, item)

    LOGGER.info("Normalize stage complete")


def normalize_file(
    data_dir: Path,
    connection: sqlite3.Connection,
    item: FileRecord,
) -> None:
    source_key = item.source_key
    if item.raw_path is None:
        raise ValueError(f"Tracked file has no raw path: {source_key}")
    raw_path = data_dir / item.raw_path
    normalized_relative_path = normalized_path_for(source_key)
    destination = data_dir / normalized_relative_path
    destination.parent.mkdir(parents=True, exist_ok=True)

    try:
        if not raw_path.exists():
            raise FileNotFoundError(f"Raw file is missing: {raw_path}")
        if raw_path.suffix.lower() not in {".csv", ".xlsx"}:
            raise ValueError(f"Unsupported raw file type for normalization: {raw_path.suffix}")

        raw_frame = read_source_frame(raw_path)
        normalized = normalize_csv_frame(raw_frame, item)
        write_parquet_atomic(normalized, destination)

        mark_normalized(
            connection=connection,
            source_key=source_key,
            normalized_path=normalized_relative_path,
            raw_row_count=raw_frame.height,
            normalized_row_count=normalized.height,
            schema_version=SCHEMA_VERSION,
        )
        LOGGER.info("Normalized %s rows to %s", normalized.height, destination)
    except Exception as exc:
        mark_normalize_failed(connection, source_key, exc)
        LOGGER.exception("Failed to normalize %s", source_key)
