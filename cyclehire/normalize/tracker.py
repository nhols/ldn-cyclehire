import logging
import sqlite3
from pathlib import Path

from cyclehire.tracking import FileRecord


LOGGER = logging.getLogger(__name__)


def plan_normalize_work(
    connection: sqlite3.Connection,
    retry_failed: bool,
    force: bool,
    limit: int | None,
) -> list[FileRecord]:
    statuses = ["pending"]
    if retry_failed:
        statuses.append("failed")
    if force:
        statuses.append("normalized")

    placeholders = ", ".join("?" for _ in statuses)
    rows = connection.execute(
        f"""
        SELECT *
        FROM files
        WHERE raw_status = 'downloaded'
          AND normalized_status IN ({placeholders})
        ORDER BY source_key
        """,
        statuses,
    ).fetchall()

    if limit is not None:
        LOGGER.info("Applying normalize work limit: %s", limit)
        rows = rows[:limit]
    return [FileRecord.from_row(row) for row in rows]


def mark_normalized(
    connection: sqlite3.Connection,
    source_key: str,
    normalized_path: Path,
    raw_row_count: int,
    normalized_row_count: int,
    schema_version: str,
) -> None:
    connection.execute(
        """
        UPDATE files
        SET normalized_status = 'normalized',
            normalized_path = ?,
            row_count_raw = ?,
            row_count_normalized = ?,
            schema_version = ?,
            error_message = NULL,
            processed_at = CURRENT_TIMESTAMP
        WHERE source_key = ?
        """,
        (
            str(normalized_path),
            raw_row_count,
            normalized_row_count,
            schema_version,
            source_key,
        ),
    )
    connection.commit()


def mark_normalize_failed(connection: sqlite3.Connection, source_key: str, error: Exception) -> None:
    connection.execute(
        """
        UPDATE files
        SET normalized_status = 'failed',
            error_message = ?,
            processed_at = CURRENT_TIMESTAMP
        WHERE source_key = ?
        """,
        (str(error), source_key),
    )
    connection.commit()
