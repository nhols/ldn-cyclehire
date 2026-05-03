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
source_row_number
```

Key modules:

- `csv_schemas.py`: typed dataclasses describing known TfL source layouts.
- `transforms.py`: parsing, datetime handling, duration repair, and canonical
  column selection.
- `paths.py`: normalized Parquet path helpers.
- `tracker.py`: selects files to normalize and records outcomes.
- `stage.py`: runs per-file normalization and logging.

Examples:

```sh
uv run cyclehire normalize --dry-run
uv run cyclehire normalize --retry-failed
uv run cyclehire normalize --force
```
