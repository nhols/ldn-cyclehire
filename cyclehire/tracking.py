import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    source_key TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    etag TEXT,
    size_bytes INTEGER NOT NULL,
    last_modified TEXT NOT NULL,
    raw_path TEXT,
    normalized_path TEXT,
    validated_path TEXT,
    invalid_path TEXT,
    raw_status TEXT NOT NULL DEFAULT 'pending',
    normalized_status TEXT NOT NULL DEFAULT 'pending',
    validated_status TEXT NOT NULL DEFAULT 'pending',
    row_count_raw INTEGER,
    row_count_normalized INTEGER,
    row_count_validated INTEGER,
    row_count_invalid INTEGER,
    schema_version TEXT,
    error_message TEXT,
    discovered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    raw_downloaded_at TEXT,
    processed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_files_raw_status ON files(raw_status);
CREATE INDEX IF NOT EXISTS idx_files_normalized_status ON files(normalized_status);
CREATE INDEX IF NOT EXISTS idx_files_validated_status ON files(validated_status);
CREATE INDEX IF NOT EXISTS idx_files_etag ON files(etag);
"""

MIGRATIONS = {
    "normalized_path": "ALTER TABLE files ADD COLUMN normalized_path TEXT",
    "validated_path": "ALTER TABLE files ADD COLUMN validated_path TEXT",
    "invalid_path": "ALTER TABLE files ADD COLUMN invalid_path TEXT",
    "row_count_invalid": "ALTER TABLE files ADD COLUMN row_count_invalid INTEGER",
}


@dataclass(frozen=True)
class FileRecord:
    source_key: str
    source_url: str
    etag: str | None
    size_bytes: int
    last_modified: str
    raw_path: str | None
    normalized_path: str | None
    validated_path: str | None
    invalid_path: str | None
    raw_status: str
    normalized_status: str
    validated_status: str
    row_count_raw: int | None
    row_count_normalized: int | None
    row_count_validated: int | None
    row_count_invalid: int | None
    schema_version: str | None
    error_message: str | None
    discovered_at: str
    raw_downloaded_at: str | None
    processed_at: str | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "FileRecord":
        return cls(
            source_key=row["source_key"],
            source_url=row["source_url"],
            etag=row["etag"],
            size_bytes=row["size_bytes"],
            last_modified=row["last_modified"],
            raw_path=row["raw_path"],
            normalized_path=row["normalized_path"],
            validated_path=row["validated_path"],
            invalid_path=row["invalid_path"],
            raw_status=row["raw_status"],
            normalized_status=row["normalized_status"],
            validated_status=row["validated_status"],
            row_count_raw=row["row_count_raw"],
            row_count_normalized=row["row_count_normalized"],
            row_count_validated=row["row_count_validated"],
            row_count_invalid=row["row_count_invalid"],
            schema_version=row["schema_version"],
            error_message=row["error_message"],
            discovered_at=row["discovered_at"],
            raw_downloaded_at=row["raw_downloaded_at"],
            processed_at=row["processed_at"],
        )


@contextmanager
def connect_tracking_db(data_dir: Path) -> Iterator[sqlite3.Connection]:
    db_path = data_dir / "metadata" / "files.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        connection.executescript(SCHEMA)
        _apply_migrations(connection)
        yield connection
        connection.commit()
    finally:
        connection.close()


def _apply_migrations(connection: sqlite3.Connection) -> None:
    columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(files)").fetchall()
    }
    for column_name, statement in MIGRATIONS.items():
        if column_name not in columns:
            connection.execute(statement)
