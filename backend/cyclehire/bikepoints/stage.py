import json
import logging
import os
import re
import tempfile
import urllib.request
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, cast

import polars as pl

from cyclehire.bikepoints.config import BikePointsConfig
from cyclehire.bikepoints.paths import (
    BIKEPOINTS_URL,
    bikepoints_parquet_path,
    bikepoints_raw_path,
    station_match_samples_path,
)
from cyclehire.utils import write_parquet_atomic


LOGGER = logging.getLogger(__name__)


def run_bikepoints_pipeline(config: BikePointsConfig) -> None:
    LOGGER.info("Fetching TfL BikePoint station metadata")
    raw_payload = fetch_bikepoints()

    raw_destination = config.data_dir / bikepoints_raw_path()
    raw_destination.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(raw_payload, raw_destination)
    LOGGER.info("Wrote raw BikePoint JSON: %s", raw_destination)

    bikepoints = flatten_bikepoints(raw_payload)
    parquet_destination = config.data_dir / bikepoints_parquet_path()
    write_parquet_atomic(bikepoints, parquet_destination)
    LOGGER.info("Wrote flattened BikePoint Parquet: %s", parquet_destination)

    samples = sample_station_matches(config.data_dir, bikepoints, config.sample_dates)
    samples_destination = config.data_dir / station_match_samples_path()
    write_parquet_atomic(samples, samples_destination)
    LOGGER.info("Wrote station match samples: %s", samples_destination)

    print_match_summary(samples)


def fetch_bikepoints() -> list[dict[str, Any]]:
    request = urllib.request.Request(
        BIKEPOINTS_URL,
        headers={"User-Agent": "cyclehire-bikepoints/0.1"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        payload = json.load(response)
    if not isinstance(payload, list):
        raise ValueError("Expected BikePoint API to return a JSON list")
    return payload


def write_json_atomic(payload: Any, destination: Path) -> None:
    temp_fd, temp_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
        text=True,
    )
    os.close(temp_fd)
    temp_path = Path(temp_name)
    try:
        with temp_path.open("w", encoding="utf-8") as output:
            json.dump(payload, output, indent=2, sort_keys=True)
            output.write("\n")
        temp_path.replace(destination)
    finally:
        temp_path.unlink(missing_ok=True)


def flatten_bikepoints(payload: list[dict[str, Any]]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in payload:
        properties = {
            prop.get("key"): prop.get("value")
            for prop in item.get("additionalProperties", [])
            if isinstance(prop, dict)
        }
        bikepoint_id = str(item.get("id") or "")
        bikepoint_number = bikepoint_id.removeprefix("BikePoints_")
        terminal_name = _string_or_none(properties.get("TerminalName"))
        rows.append(
            {
                "bikepoint_id": bikepoint_id,
                "bikepoint_number": bikepoint_number,
                "bikepoint_number_key": numeric_key(bikepoint_number),
                "terminal_name": terminal_name,
                "terminal_name_key": numeric_key(terminal_name),
                "common_name": _string_or_none(item.get("commonName")),
                "common_name_key": name_key(_string_or_none(item.get("commonName"))),
                "lat": item.get("lat"),
                "lon": item.get("lon"),
                "installed": parse_bool(properties.get("Installed")),
                "locked": parse_bool(properties.get("Locked")),
                "temporary": parse_bool(properties.get("Temporary")),
                "install_date": parse_tfl_epoch_ms(properties.get("InstallDate")),
                "removal_date": parse_tfl_epoch_ms(properties.get("RemovalDate")),
                "nb_bikes": parse_int(properties.get("NbBikes")),
                "nb_empty_docks": parse_int(properties.get("NbEmptyDocks")),
                "nb_docks": parse_int(properties.get("NbDocks")),
                "nb_standard_bikes": parse_int(properties.get("NbStandardBikes")),
                "nb_ebikes": parse_int(properties.get("NbEBikes")),
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None)


def sample_station_matches(
    data_dir: Path,
    bikepoints: pl.DataFrame,
    sample_dates: tuple[date, ...],
) -> pl.DataFrame:
    trip_files = sorted((data_dir / "validated" / "trips_by_file").glob("*.parquet"))
    if not trip_files:
        raise FileNotFoundError(f"No validated trip Parquet files under {data_dir}")

    bikepoint_number_lookup = _lookup(bikepoints, "bikepoint_number_key")
    terminal_lookup = _lookup(bikepoints, "terminal_name_key")
    name_lookup = _lookup(bikepoints, "common_name_key")

    rows: list[dict[str, Any]] = []
    for sample_date in sample_dates:
        LOGGER.info("Sampling station matches for %s", sample_date.isoformat())
        stations = stations_for_day(trip_files, sample_date)
        for station in stations.iter_rows(named=True):
            station_id = _string_or_none(station["station_id"])
            station_name = _string_or_none(station["station_name"])
            id_key = numeric_key(station_id)
            station_name_key = name_key(station_name)

            match_method = None
            match = None
            if id_key in bikepoint_number_lookup:
                match_method = "bikepoint_number"
                match = bikepoint_number_lookup[id_key]
            elif id_key in terminal_lookup:
                match_method = "terminal_name"
                match = terminal_lookup[id_key]
            elif station_name_key in name_lookup:
                match_method = "common_name"
                match = name_lookup[station_name_key]

            rows.append(
                {
                    "sample_date": sample_date,
                    "station_id": station_id,
                    "station_id_key": id_key,
                    "station_name": station_name,
                    "station_name_key": station_name_key,
                    "trip_count": station["trip_count"],
                    "match_method": match_method,
                    "matched_bikepoint_id": match["bikepoint_id"] if match else None,
                    "matched_common_name": match["common_name"] if match else None,
                    "matched_terminal_name": match["terminal_name"] if match else None,
                    "matched_lat": match["lat"] if match else None,
                    "matched_lon": match["lon"] if match else None,
                }
            )

    return pl.DataFrame(rows)


def stations_for_day(trip_files: list[Path], sample_date: date) -> pl.DataFrame:
    start_at = datetime.combine(sample_date, time.min)
    end_at = datetime.combine(sample_date, time.max)
    trips = pl.scan_parquet([str(path) for path in trip_files], extra_columns="ignore").filter(
        (pl.col("start_at") >= start_at) & (pl.col("start_at") <= end_at)
    )
    starts = trips.select(
        pl.col("start_station_id").alias("station_id"),
        pl.col("start_station_name").alias("station_name"),
    )
    ends = trips.select(
        pl.col("end_station_id").alias("station_id"),
        pl.col("end_station_name").alias("station_name"),
    )
    return cast(
        pl.DataFrame,
        (
            pl.concat([starts, ends])
            .filter(pl.col("station_name").is_not_null())
            .group_by("station_id", "station_name")
            .agg(pl.len().alias("trip_count"))
            .sort("trip_count", descending=True)
            .collect()
        ),
    )


def print_match_summary(samples: pl.DataFrame) -> None:
    summary = (
        samples.with_columns(pl.col("match_method").fill_null("unmatched"))
        .group_by("sample_date", "match_method")
        .agg(pl.len().alias("stations"))
        .sort("sample_date", "match_method")
    )
    print(summary)

    unmatched = (
        samples.filter(pl.col("match_method").is_null())
        .sort("trip_count", descending=True)
        .select("sample_date", "station_id", "station_name", "trip_count")
        .head(25)
    )
    if unmatched.height:
        print("\nTop unmatched sampled stations:")
        print(unmatched)
    else:
        print("\nAll sampled stations matched BikePoint metadata.")


def _lookup(frame: pl.DataFrame, key_column: str) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in frame.filter(pl.col(key_column).is_not_null()).iter_rows(named=True):
        key = row[key_column]
        if key and key not in rows:
            rows[key] = row
    return rows


def numeric_key(value: str | None) -> str | None:
    if not value:
        return None
    digits = re.sub(r"\D+", "", value)
    if not digits:
        return None
    return digits.lstrip("0") or "0"


def name_key(value: str | None) -> str | None:
    if not value:
        return None
    key = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
    return re.sub(r"\s+", " ", key) or None


def parse_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    return str(value).lower() == "true"


def parse_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def parse_tfl_epoch_ms(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    return datetime.fromtimestamp(int(value) / 1000)


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    string_value = str(value).strip()
    return string_value or None
