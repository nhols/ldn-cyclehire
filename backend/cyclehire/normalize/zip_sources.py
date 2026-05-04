import logging
import zipfile
from io import BytesIO
from pathlib import Path

import polars as pl

from cyclehire.normalize.fingerprints import CsvFingerprint, FingerprintedCsv


LOGGER = logging.getLogger(__name__)
CSV_ENCODINGS = ("utf8", "windows-1252")


def read_unique_zip_csv_members(
    zip_path: Path,
    duplicate_index: dict[CsvFingerprint, list[FingerprintedCsv]],
) -> list[tuple[str, pl.DataFrame]]:
    unique_members: list[tuple[str, pl.DataFrame]] = []
    skipped = 0
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.infolist():
            if member.is_dir() or not member.filename.lower().endswith(".csv"):
                continue

            fingerprint = CsvFingerprint(
                file_size=member.file_size,
                crc32=member.CRC,
            )
            duplicates = duplicate_index.get(fingerprint, [])
            if duplicates:
                skipped += 1
                LOGGER.info(
                    "Skipping duplicate ZIP member %s in %s; duplicate of %s",
                    member.filename,
                    zip_path,
                    duplicates[0].source_key,
                )
                continue

            with archive.open(member) as file:
                data = file.read()
            frame = read_csv_bytes(data, member.filename)
            unique_members.append((member.filename, frame))

    LOGGER.info(
        "Read %s unique CSV members from %s; skipped %s duplicate members",
        len(unique_members),
        zip_path,
        skipped,
    )
    return unique_members


def read_csv_bytes(data: bytes, source_name: str) -> pl.DataFrame:
    last_error: Exception | None = None
    for encoding in CSV_ENCODINGS:
        try:
            frame = pl.read_csv(
                BytesIO(data),
                infer_schema=False,
                try_parse_dates=False,
                null_values=[""],
                encoding=encoding,
            )
            if encoding != "utf8":
                LOGGER.info("Read %s with %s encoding", source_name, encoding)
            return frame
        except pl.exceptions.ComputeError as exc:
            last_error = exc

    raise ValueError(f"Could not read CSV member with supported encodings: {source_name}") from last_error
