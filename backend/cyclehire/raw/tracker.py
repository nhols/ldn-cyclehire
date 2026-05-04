import logging
import sqlite3

from cyclehire.raw.paths import raw_path_for
from cyclehire.raw.sources import SourceObject


LOGGER = logging.getLogger(__name__)


def plan_raw_work(
    connection: sqlite3.Connection,
    source_objects: list[SourceObject],
    limit: int | None,
) -> list[SourceObject]:
    planned: list[SourceObject] = []
    for item in source_objects:
        existing = connection.execute(
            "SELECT etag, size_bytes, last_modified, raw_status FROM files WHERE source_key = ?",
            (item.key,),
        ).fetchone()

        raw_path = str(raw_path_for(item.key))
        if existing is None:
            connection.execute(
                """
                INSERT INTO files (
                    source_key,
                    source_url,
                    etag,
                    size_bytes,
                    last_modified,
                    raw_path,
                    raw_status,
                    normalized_status,
                    validated_status
                )
                VALUES (?, ?, ?, ?, ?, ?, 'pending', 'pending', 'pending')
                """,
                (
                    item.key,
                    item.url,
                    item.etag,
                    item.size_bytes,
                    item.last_modified,
                    raw_path,
                ),
            )
            planned.append(item)
            continue

        changed = (
            existing["etag"] != item.etag
            or existing["size_bytes"] != item.size_bytes
            or existing["last_modified"] != item.last_modified
        )
        if changed:
            LOGGER.warning("Source object changed upstream; resetting downstream statuses: %s", item.key)
            connection.execute(
                """
                UPDATE files
                SET source_url = ?,
                    etag = ?,
                    size_bytes = ?,
                    last_modified = ?,
                    raw_path = ?,
                    raw_status = 'pending',
                    normalized_status = 'pending',
                    validated_status = 'pending',
                    normalized_path = NULL,
                    validated_path = NULL,
                    invalid_path = NULL,
                    row_count_raw = NULL,
                    row_count_normalized = NULL,
                    row_count_validated = NULL,
                    row_count_invalid = NULL,
                    schema_version = NULL,
                    error_message = NULL,
                    raw_downloaded_at = NULL,
                    processed_at = NULL
                WHERE source_key = ?
                """,
                (
                    item.url,
                    item.etag,
                    item.size_bytes,
                    item.last_modified,
                    raw_path,
                    item.key,
                ),
            )
            planned.append(item)
            continue

        if existing["raw_status"] != "downloaded":
            planned.append(item)

    if limit is not None:
        LOGGER.info("Applying raw work limit: %s", limit)
        planned = planned[:limit]
    return planned


def mark_raw_downloaded(connection: sqlite3.Connection, source_key: str) -> None:
    connection.execute(
        """
        UPDATE files
        SET raw_status = 'downloaded',
            raw_downloaded_at = CURRENT_TIMESTAMP,
            error_message = NULL
        WHERE source_key = ?
        """,
        (source_key,),
    )
    connection.commit()


def mark_raw_failed(connection: sqlite3.Connection, source_key: str, error: Exception) -> None:
    connection.execute(
        """
        UPDATE files
        SET raw_status = 'failed',
            error_message = ?
        WHERE source_key = ?
        """,
        (str(error), source_key),
    )
    connection.commit()
