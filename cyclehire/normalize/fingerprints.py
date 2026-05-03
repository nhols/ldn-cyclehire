import logging
import zlib
from dataclasses import dataclass
from pathlib import Path

from cyclehire.tracking import FileRecord


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CsvFingerprint:
    file_size: int
    crc32: int


@dataclass(frozen=True)
class FingerprintedCsv:
    source_key: str
    path: Path
    fingerprint: CsvFingerprint


def build_flat_csv_fingerprint_index(
    data_dir: Path,
    files: list[FileRecord],
) -> dict[CsvFingerprint, list[FingerprintedCsv]]:
    index: dict[CsvFingerprint, list[FingerprintedCsv]] = {}
    for item in files:
        if item.raw_path is None or not item.raw_path.lower().endswith(".csv"):
            continue

        path = data_dir / item.raw_path
        if not path.exists():
            LOGGER.warning("Cannot fingerprint missing raw CSV: %s", path)
            continue

        fingerprint = CsvFingerprint(
            file_size=path.stat().st_size,
            crc32=crc32_for_file(path),
        )
        index.setdefault(fingerprint, []).append(
            FingerprintedCsv(
                source_key=item.source_key,
                path=path,
                fingerprint=fingerprint,
            )
        )
    LOGGER.info("Indexed %s flat CSV fingerprints for duplicate detection", len(index))
    return index


def crc32_for_file(path: Path) -> int:
    checksum = 0
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            checksum = zlib.crc32(chunk, checksum)
    return checksum & 0xFFFFFFFF
