from pathlib import Path


def google_bicycle_routes_dir() -> Path:
    return Path("reference") / "routes" / "google_bicycle"


def google_bicycle_routes_jsonl_path() -> Path:
    return google_bicycle_routes_dir() / "routes.jsonl"


def google_bicycle_routes_parquet_path() -> Path:
    return google_bicycle_routes_dir() / "routes.parquet"
