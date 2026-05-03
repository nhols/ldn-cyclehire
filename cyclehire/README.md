# cyclehire package

This package contains the local data pipeline for TfL cycle hire usage data.
The stages are intentionally runnable independently, with progress and durable
outcomes tracked in `data/metadata/files.sqlite`.

## Pipeline

```text
raw source files -> normalized Parquet -> validated Parquet + invalid diagnostics
```

- [raw](raw/README.md): discover TfL source files, track them, and download raw
  objects that are new or changed.
- [normalize](normalize/README.md): parse downloaded CSV/XLSX files and write a
  canonical per-file Parquet dataset.
- [validate](validate/README.md): filter normalized rows through the Dataframely
  trip schema, preserving valid rows and invalid-row diagnostics.

## CLI examples

```sh
uv run cyclehire raw --dry-run
uv run cyclehire raw --limit 5

uv run cyclehire normalize --dry-run
uv run cyclehire normalize --retry-failed
uv run cyclehire normalize --force

uv run cyclehire validate --dry-run
uv run cyclehire validate --retry-failed
uv run cyclehire validate --force
```

Useful global options:

```sh
uv run cyclehire --data-dir data --log-level DEBUG raw --dry-run
```

## Module layout

Each stage package follows the same rough shape:

- `config.py`: stage configuration dataclass.
- `paths.py`: stage-specific output path helpers.
- `tracker.py`: file tracking table queries and status updates.
- `stage.py`: orchestration and logging for the runnable stage.

Additional modules hold stage-specific logic, such as raw source listing and
normalization transforms.
