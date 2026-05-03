from pathlib import Path


def validated_path_for(source_key: str) -> Path:
    safe_name = source_key.replace("/", "__")
    return Path("validated") / "trips_by_file" / f"{safe_name}.parquet"


def invalid_path_for(source_key: str) -> Path:
    safe_name = source_key.replace("/", "__")
    return Path("validation") / "invalid" / f"{safe_name}.parquet"
