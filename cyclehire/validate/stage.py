import logging
import sqlite3
from pathlib import Path

import polars as pl

from cyclehire.schemas import TripSchema
from cyclehire.tracking import FileRecord, connect_tracking_db
from cyclehire.utils import write_parquet_atomic
from cyclehire.validate.config import ValidatePipelineConfig
from cyclehire.validate.paths import invalid_path_for, validated_path_for
from cyclehire.validate.tracker import mark_validate_failed, mark_validated, plan_validate_work


LOGGER = logging.getLogger(__name__)


def run_validate_pipeline(config: ValidatePipelineConfig) -> None:
    LOGGER.info("Starting validate stage")
    LOGGER.info("Data directory: %s", config.data_dir)

    with connect_tracking_db(config.data_dir) as connection:
        files = plan_validate_work(connection, config.retry_failed, config.force, config.limit)
        LOGGER.info("Files requiring validation: %s", len(files))

        if config.dry_run:
            for item in files:
                LOGGER.info("Would validate %s", item.source_key)
            LOGGER.info("Dry run complete; no validated outputs written")
            return

        for index, item in enumerate(files, start=1):
            LOGGER.info(
                "Validating %s of %s: %s",
                index,
                len(files),
                item.source_key,
            )
            validate_file(config.data_dir, connection, item)

    LOGGER.info("Validate stage complete")


def validate_file(
    data_dir: Path,
    connection: sqlite3.Connection,
    item: FileRecord,
) -> None:
    source_key = item.source_key
    if item.normalized_path is None:
        raise ValueError(f"Tracked file has no normalized path: {source_key}")
    normalized_path = data_dir / item.normalized_path
    validated_relative_path = validated_path_for(source_key)
    invalid_relative_path = invalid_path_for(source_key)
    validated_destination = data_dir / validated_relative_path
    invalid_destination = data_dir / invalid_relative_path
    validated_destination.parent.mkdir(parents=True, exist_ok=True)
    invalid_destination.parent.mkdir(parents=True, exist_ok=True)

    try:
        if not normalized_path.exists():
            raise FileNotFoundError(f"Normalized file is missing: {normalized_path}")

        frame = pl.read_parquet(normalized_path)
        valid, failure = TripSchema.filter(frame, cast=True)
        invalid = failure.details()

        write_parquet_atomic(valid, validated_destination)
        invalid_count = invalid.height
        if invalid_count > 0:
            write_parquet_atomic(invalid, invalid_destination)
            stored_invalid_path = invalid_relative_path
            LOGGER.warning(
                "Validation rejected %s rows for %s: %s",
                invalid_count,
                source_key,
                failure.counts(),
            )
        else:
            invalid_destination.unlink(missing_ok=True)
            stored_invalid_path = None

        status = "validated" if valid.height > 0 else "failed"
        error_message = None
        if valid.height == 0:
            error_message = "Validation produced zero valid rows"

        mark_validated(
            connection=connection,
            source_key=source_key,
            status=status,
            validated_path=validated_relative_path,
            invalid_path=stored_invalid_path,
            valid_row_count=valid.height,
            invalid_row_count=invalid_count,
            error_message=error_message,
        )
        LOGGER.info(
            "Validated %s rows for %s; invalid rows: %s",
            valid.height,
            source_key,
            invalid_count,
        )
    except Exception as exc:
        mark_validate_failed(connection, source_key, exc)
        LOGGER.exception("Failed to validate %s", source_key)
