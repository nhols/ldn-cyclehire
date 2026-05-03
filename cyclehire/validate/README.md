# validate stage

The validate stage reads normalized Parquet files, filters rows through the
Dataframely `TripSchema`, and persists both valid rows and invalid diagnostics.

It writes valid rows under:

```text
data/validated/trips_by_file/
```

It writes invalid-row diagnostics under:

```text
data/validation/invalid/
```

Invalid diagnostics keep the row values plus Dataframely field/rule statuses.
`source_key` and `source_row_number` make each rejected row traceable back to
the original source file and row number.

Key modules:

- `paths.py`: validated and invalid-output path helpers.
- `tracker.py`: selects files to validate and records row counts/status.
- `stage.py`: runs schema filtering, writes outputs, and logs failures.

Examples:

```sh
uv run cyclehire validate --dry-run
uv run cyclehire validate --retry-failed
uv run cyclehire validate --force
```
