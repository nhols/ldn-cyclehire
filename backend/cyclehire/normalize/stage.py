import logging
import sqlite3
from pathlib import Path

import polars as pl

from cyclehire.normalize.config import NormalizePipelineConfig
from cyclehire.normalize.fingerprints import (
    CsvFingerprint,
    FingerprintedCsv,
    build_flat_csv_fingerprint_index,
)
from cyclehire.normalize.paths import normalized_path_for
from cyclehire.normalize.tracker import (
    list_downloaded_files,
    mark_normalize_failed,
    mark_normalize_skipped,
    mark_normalized,
    plan_normalize_work,
)
from cyclehire.normalize.transforms import normalize_csv_frame, read_source_frame
from cyclehire.normalize.zip_sources import read_unique_zip_csv_members
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

        duplicate_index: dict[CsvFingerprint, list[FingerprintedCsv]] = {}
        if any(item.raw_path is not None and item.raw_path.lower().endswith(".zip") for item in files):
            duplicate_index = build_flat_csv_fingerprint_index(
                config.data_dir,
                list_downloaded_files(connection),
            )

        for index, item in enumerate(files, start=1):
            LOGGER.info(
                "Normalizing %s of %s: %s",
                index,
                len(files),
                item.source_key,
            )
            normalize_file(config.data_dir, connection, item, duplicate_index)

    LOGGER.info("Normalize stage complete")


def normalize_file(
    data_dir: Path,
    connection: sqlite3.Connection,
    item: FileRecord,
    duplicate_index: dict[CsvFingerprint, list[FingerprintedCsv]],
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
        if raw_path.suffix.lower() not in {".csv", ".xlsx", ".zip"}:
            raise ValueError(f"Unsupported raw file type for normalization: {raw_path.suffix}")

        raw_row_count, normalized = normalize_source(raw_path, item, duplicate_index)
        if normalized is None:
            mark_normalize_skipped(
                connection,
                source_key,
                "All ZIP CSV members duplicate already-downloaded flat CSV files",
            )
            LOGGER.info("Skipped %s because all ZIP CSV members are duplicates", source_key)
            return

        write_parquet_atomic(normalized, destination)

        mark_normalized(
            connection=connection,
            source_key=source_key,
            normalized_path=normalized_relative_path,
            raw_row_count=raw_row_count,
            normalized_row_count=normalized.height,
            schema_version=SCHEMA_VERSION,
        )
        LOGGER.info("Normalized %s rows to %s", normalized.height, destination)
    except Exception as exc:
        mark_normalize_failed(connection, source_key, exc)
        LOGGER.exception("Failed to normalize %s", source_key)


def normalize_source(
    raw_path: Path,
    item: FileRecord,
    duplicate_index: dict[CsvFingerprint, list[FingerprintedCsv]],
) -> tuple[int, pl.DataFrame | None]:
    if raw_path.suffix.lower() != ".zip":
        raw_frame = read_source_frame(raw_path)
        return raw_frame.height, normalize_csv_frame(raw_frame, item)

    normalized_members: list[pl.DataFrame] = []
    raw_row_count = 0
    for member_name, raw_frame in read_unique_zip_csv_members(raw_path, duplicate_index):
        raw_row_count += raw_frame.height
        normalized_members.append(normalize_csv_frame(raw_frame, item, source_member=member_name))

    if not normalized_members:
        return 0, None
    return raw_row_count, pl.concat(normalized_members, how="vertical")
