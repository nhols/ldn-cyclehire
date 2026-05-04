# normalize stage

The normalize stage reads downloaded raw CSV/XLSX files, detects the TfL source
schema, and writes one canonical Parquet file per source file.

It writes normalized files under:

```text
data/cache/normalized/
```

The normalized schema preserves source traceability with:

```text
source_key
source_etag
source_member
source_row_number
```

For normal CSV/XLSX inputs, `source_member` is null. For CSVs read from ZIP
archives, it stores the member filename inside the ZIP.

ZIP member CSVs are deduplicated against downloaded flat CSVs using
`(file_size, crc32)`. ZIPs where every CSV member is already present as a flat
CSV are marked as `normalized_status = skipped`.

Key modules:

- `csv_schemas.py`: typed dataclasses describing known TfL source layouts.
- `fingerprints.py`: computes flat CSV `(file_size, crc32)` fingerprints for
  duplicate detection.
- `transforms.py`: parsing, datetime handling, duration repair, and canonical
  column selection.
- `zip_sources.py`: reads unique ZIP member CSVs and skips exact duplicates.
- `paths.py`: normalized Parquet path helpers.
- `tracker.py`: selects files to normalize and records outcomes.
- `stage.py`: runs per-file normalization and logging.

Examples:

```sh
uv run cyclehire normalize --dry-run
uv run cyclehire normalize --retry-failed
uv run cyclehire normalize --force
```
