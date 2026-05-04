import logging
import sqlite3
from pathlib import Path

from cyclehire.tracking import FileRecord


LOGGER = logging.getLogger(__name__)


def plan_validate_work(
    connection: sqlite3.Connection,
    retry_failed: bool,
    force: bool,
    limit: int | None,
) -> list[FileRecord]:
    statuses = ["pending"]
    if retry_failed:
        statuses.append("failed")
    if force:
        statuses.append("validated")

    placeholders = ", ".join("?" for _ in statuses)
    rows = connection.execute(
        f"""
        SELECT *
        FROM files
        WHERE normalized_status = 'normalized'
          AND validated_status IN ({placeholders})
        ORDER BY source_key
        """,
        statuses,
    ).fetchall()

    if limit is not None:
        LOGGER.info("Applying validate work limit: %s", limit)
        rows = rows[:limit]
    return [FileRecord.from_row(row) for row in rows]


def mark_validated(
    connection: sqlite3.Connection,
    source_key: str,
    status: str,
    validated_path: Path,
    invalid_path: Path | None,
    valid_row_count: int,
    invalid_row_count: int,
    error_message: str | None,
) -> None:
    connection.execute(
        """
        UPDATE files
        SET validated_status = ?,
            validated_path = ?,
            invalid_path = ?,
            row_count_validated = ?,
            row_count_invalid = ?,
            error_message = ?,
            processed_at = CURRENT_TIMESTAMP
        WHERE source_key = ?
        """,
        (
            status,
            str(validated_path),
            str(invalid_path) if invalid_path is not None else None,
            valid_row_count,
            invalid_row_count,
            error_message,
            source_key,
        ),
    )
    connection.commit()


def mark_validate_failed(connection: sqlite3.Connection, source_key: str, error: Exception) -> None:
    connection.execute(
        """
        UPDATE files
        SET validated_status = 'failed',
            error_message = ?,
            processed_at = CURRENT_TIMESTAMP
        WHERE source_key = ?
        """,
        (str(error), source_key),
    )
    connection.commit()
