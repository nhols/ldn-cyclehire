from pathlib import Path


def normalized_path_for(source_key: str) -> Path:
    safe_name = source_key.replace("/", "__")
    return Path("cache") / "normalized" / f"{safe_name}.parquet"
