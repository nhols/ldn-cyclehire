Cycle Hire
==========

Tools for ingesting, validating, and visualising TfL cycle hire usage data.

Raw Ingestion
-------------

The raw stage discovers objects in TfL's `usage-stats/` bucket folder, records
each source file in a local tracking table, and downloads only files that are
missing or have changed upstream.

Run a dry check without downloading files:

```sh
uv run cyclehire raw --dry-run
```

Download all pending raw files:

```sh
uv run cyclehire raw
```

Download only a small batch:

```sh
uv run cyclehire raw --limit 5
```

Useful options:

- `--data-dir data`: choose where raw files and metadata are stored.
- `--log-level DEBUG`: increase logging detail.
- `--dry-run`: update the tracking table and show planned downloads without
  downloading file contents.
- `--limit N`: cap the number of pending files handled in this run.

Local layout:

```text
data/
  metadata/
    files.sqlite
  raw/
    usage-stats/
      ...
```

The `files` tracking table marks new files as:

- `raw_status = pending`
- `normalized_status = pending`
- `validated_status = pending`

Later stages should be runnable independently and update their own status
columns without re-downloading raw data.

Normalisation
-------------

The normalize stage reads downloaded raw CSV/XLSX files, detects their TfL
source schema, and writes one canonical Parquet file per source file.

Run a dry check:

```sh
uv run cyclehire normalize --dry-run
```

Normalize all pending downloaded files:

```sh
uv run cyclehire normalize
```

Normalize a small batch:

```sh
uv run cyclehire normalize --limit 5
```

Retry files that previously failed normalization:

```sh
uv run cyclehire normalize --retry-failed
```

Rebuild normalized files after schema/parser changes:

```sh
uv run cyclehire normalize --force
```

Normalized files are written to:

```text
data/cache/normalized/
```

The canonical trip schema is:

```text
journey_id
bike_id
bike_model
start_at
end_at
start_station_id
start_station_name
end_station_id
end_station_name
duration_seconds
duration_ms
source_key
source_etag
source_member
source_row_number
```

ZIP members are deduplicated against downloaded flat CSVs using uncompressed
file size and CRC32. ZIPs where every CSV member is a duplicate are marked as
`normalized_status = skipped`.

Unsupported downloaded file types, unknown source schemas, and parse failures
are marked as `normalized_status = failed` with an `error_message` in the
tracking table. They can be retried with `--retry-failed` after the normalizer
learns how to handle them.

Validation
----------

The validate stage reads normalized Parquet files, applies the `TripSchema`
Dataframely schema with soft validation, and persists both valid rows and
invalid-row diagnostics.

Run a dry check:

```sh
uv run cyclehire validate --dry-run
```

Validate all pending normalized files:

```sh
uv run cyclehire validate
```

Validate a small batch:

```sh
uv run cyclehire validate --limit 5
```

Retry files that previously failed validation:

```sh
uv run cyclehire validate --retry-failed
```

Rebuild validation outputs after rule changes:

```sh
uv run cyclehire validate --force
```

Valid rows are written to:

```text
data/validated/trips_by_file/
```

Invalid row diagnostics are written to:

```text
data/validation/invalid/
```

Invalid diagnostics preserve the normalized row values plus Dataframely rule
statuses. The `source_key` and `source_row_number` columns allow every failed
row to be traced back to the original raw file and row number.

BikePoint station metadata
--------------------------

Fetch the current TfL BikePoint station metadata and compare it with stations
seen on a few sample trip dates:

```sh
uv run cyclehire bikepoints
```

Sample specific dates:

```sh
uv run cyclehire bikepoints --sample-date 2015-06-17 --sample-date 2025-06-18
```

Outputs are written to:

```text
data/reference/bikepoints/
  bikepoints.json
  bikepoints.parquet
  station_match_samples.parquet
```

Dashboard proof of concept
--------------------------

The dashboard is split into a reusable data layer, a thin API layer, and a
separate frontend:

```text
cyclehire/dashboard/  # Parquet/BikePoint playback data loading
cyclehire/api/        # FastAPI endpoints only
frontend/             # Vite + React + MapLibre + deck.gl UI
```

Run the API:

```sh
uv run uvicorn cyclehire.api.app:app --reload
```

Run the frontend:

```sh
cd frontend
npm install
npm run dev
```

The frontend dev server proxies `/api` requests to `http://127.0.0.1:8000`.
