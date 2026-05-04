from pathlib import Path


def manifest_path() -> Path:
    return Path("manifest.json")


def compressed_manifest_path() -> Path:
    return Path("manifest.json.gz")


def routes_path() -> Path:
    return Path("routes") / "google-bicycle-v1.json"


def compressed_routes_path() -> Path:
    return Path("routes") / "google-bicycle-v1.json.gz"


def bikepoints_path() -> Path:
    return Path("stations") / "bikepoints-v1.json"


def compressed_bikepoints_path() -> Path:
    return Path("stations") / "bikepoints-v1.json.gz"


def day_path(day: str) -> Path:
    return Path("days") / f"{day}.json"


def compressed_day_path(day: str) -> Path:
    return Path("days") / f"{day}.json.gz"
