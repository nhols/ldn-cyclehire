import logging
import os
import shutil
import sqlite3
import tempfile
import urllib.request
from pathlib import Path

from cyclehire.raw.config import RawPipelineConfig
from cyclehire.raw.paths import raw_path_for
from cyclehire.raw.sources import SourceObject, fetch_source_listing
from cyclehire.raw.tracker import mark_raw_downloaded, mark_raw_failed, plan_raw_work
from cyclehire.tracking import connect_tracking_db


LOGGER = logging.getLogger(__name__)


def run_raw_pipeline(config: RawPipelineConfig) -> None:
    LOGGER.info("Starting raw ingestion stage")
    LOGGER.info("Data directory: %s", config.data_dir)

    source_objects = list(fetch_source_listing())
    if not config.include_zero_byte:
        source_objects = [item for item in source_objects if item.size_bytes > 0]
    LOGGER.info("Discovered %s source objects", len(source_objects))

    with connect_tracking_db(config.data_dir) as connection:
        planned = plan_raw_work(connection, source_objects, config.limit)
        LOGGER.info("Files requiring raw download: %s", len(planned))

        if config.dry_run:
            for item in planned:
                LOGGER.info("Would download %s (%s bytes)", item.key, item.size_bytes)
            LOGGER.info("Dry run complete; tracking table updated for discovered files")
            return

        for index, item in enumerate(planned, start=1):
            LOGGER.info(
                "Downloading %s of %s: %s (%s bytes)",
                index,
                len(planned),
                item.key,
                item.size_bytes,
            )
            download_source_object(config.data_dir, connection, item)

    LOGGER.info("Raw ingestion stage complete")


def download_source_object(
    data_dir: Path,
    connection: sqlite3.Connection,
    item: SourceObject,
) -> None:
    destination = data_dir / raw_path_for(item.key)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_fd, temp_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
    )
    os.close(temp_fd)
    temp_path = Path(temp_name)

    try:
        request = urllib.request.Request(item.url, headers={"User-Agent": "cyclehire-ingest/0.1"})
        with urllib.request.urlopen(request, timeout=300) as response:
            with temp_path.open("wb") as output:
                shutil.copyfileobj(response, output, length=1024 * 1024)

        downloaded_size = temp_path.stat().st_size
        if downloaded_size != item.size_bytes:
            raise ValueError(
                f"Downloaded size mismatch for {item.key}: "
                f"expected {item.size_bytes}, got {downloaded_size}"
            )

        temp_path.replace(destination)
        mark_raw_downloaded(connection, item.key)
        LOGGER.info("Downloaded %s", destination)
    except Exception as exc:
        mark_raw_failed(connection, item.key, exc)
        LOGGER.exception("Failed to download %s", item.key)
        raise
    finally:
        temp_path.unlink(missing_ok=True)
