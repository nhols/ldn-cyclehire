from pathlib import Path


BIKEPOINTS_URL = "https://api.tfl.gov.uk/BikePoint"


def bikepoints_raw_path() -> Path:
    return Path("reference") / "bikepoints" / "bikepoints.json"


def bikepoints_parquet_path() -> Path:
    return Path("reference") / "bikepoints" / "bikepoints.parquet"


def station_match_samples_path() -> Path:
    return Path("reference") / "bikepoints" / "station_match_samples.parquet"
