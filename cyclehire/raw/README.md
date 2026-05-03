# raw stage

The raw stage discovers TfL `usage-stats/` source objects, records each file in
the local tracking table, and downloads raw files that are missing or have
changed upstream.

It writes source files under:

```text
data/raw/usage-stats/
```

It updates `data/metadata/files.sqlite` with durable raw outcomes:

```text
raw_status = pending | downloaded | failed
```

Key modules:

- `sources.py`: fetches and parses the TfL S3 XML listing.
- `paths.py`: builds public URLs and local raw paths.
- `tracker.py`: inserts discovered files and plans download work.
- `stage.py`: runs discovery, planning, download, and logging.

Examples:

```sh
uv run cyclehire raw --dry-run
uv run cyclehire raw --limit 5
uv run cyclehire raw
```
