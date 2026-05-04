from __future__ import annotations

from collections import Counter
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

import polars as pl

from cyclehire.bikepoints.paths import bikepoints_parquet_path
from cyclehire.stations import BikePointLookup, make_station_key, string_or_none


def ranked_route_pairs(data_dir: Path, route_date: date | None = None) -> pl.DataFrame:
    lookup = BikePointLookup.from_frame(pl.read_parquet(data_dir / bikepoints_parquet_path()))
    pair_counts: Counter[tuple[str, str]] = Counter()
    pair_details: dict[tuple[str, str], dict[str, Any]] = {}

    for path in sorted((data_dir / "validated" / "trips_by_file").glob("*.parquet")):
        frame = pl.scan_parquet(str(path), extra_columns="ignore").select(
            "start_at",
            "start_station_id",
            "start_station_name",
            "end_station_id",
            "end_station_name",
        )
        if route_date is not None:
            start_at = datetime.combine(route_date, time.min)
            end_at = datetime.combine(route_date, time.max)
            frame = frame.filter((pl.col("start_at") >= start_at) & (pl.col("start_at") <= end_at))

        trips = frame.collect()
        if trips.is_empty():
            continue

        station_map = station_map_for(trips, lookup)
        grouped = (
            trips.with_columns(
                start_key=station_key_expr("start_station_id", "start_station_name"),
                end_key=station_key_expr("end_station_id", "end_station_name"),
            )
            .join(
                station_map.rename(
                    {
                        "station_key": "start_key",
                        "bikepoint_id": "start_bikepoint_id",
                        "name": "start_bikepoint_name",
                        "lat": "start_lat",
                        "lon": "start_lon",
                    }
                ),
                on="start_key",
                how="left",
            )
            .join(
                station_map.rename(
                    {
                        "station_key": "end_key",
                        "bikepoint_id": "end_bikepoint_id",
                        "name": "end_bikepoint_name",
                        "lat": "end_lat",
                        "lon": "end_lon",
                    }
                ),
                on="end_key",
                how="left",
            )
            .filter(
                pl.col("start_bikepoint_id").is_not_null()
                & pl.col("end_bikepoint_id").is_not_null()
                & (pl.col("start_bikepoint_id") != pl.col("end_bikepoint_id"))
            )
            .with_columns(
                pair_from=pl.min_horizontal("start_bikepoint_id", "end_bikepoint_id"),
                pair_to=pl.max_horizontal("start_bikepoint_id", "end_bikepoint_id"),
            )
            .with_columns(
                from_lat=pl.when(pl.col("start_bikepoint_id") == pl.col("pair_from"))
                .then(pl.col("start_lat"))
                .otherwise(pl.col("end_lat")),
                from_lon=pl.when(pl.col("start_bikepoint_id") == pl.col("pair_from"))
                .then(pl.col("start_lon"))
                .otherwise(pl.col("end_lon")),
                from_name=pl.when(pl.col("start_bikepoint_id") == pl.col("pair_from"))
                .then(pl.col("start_bikepoint_name"))
                .otherwise(pl.col("end_bikepoint_name")),
                to_lat=pl.when(pl.col("start_bikepoint_id") == pl.col("pair_from"))
                .then(pl.col("end_lat"))
                .otherwise(pl.col("start_lat")),
                to_lon=pl.when(pl.col("start_bikepoint_id") == pl.col("pair_from"))
                .then(pl.col("end_lon"))
                .otherwise(pl.col("start_lon")),
                to_name=pl.when(pl.col("start_bikepoint_id") == pl.col("pair_from"))
                .then(pl.col("end_bikepoint_name"))
                .otherwise(pl.col("start_bikepoint_name")),
            )
            .group_by("pair_from", "pair_to")
            .agg(
                pl.len().alias("trips"),
                pl.first("from_name").alias("from_name"),
                pl.first("from_lat").alias("from_lat"),
                pl.first("from_lon").alias("from_lon"),
                pl.first("to_name").alias("to_name"),
                pl.first("to_lat").alias("to_lat"),
                pl.first("to_lon").alias("to_lon"),
            )
        )
        for row in grouped.iter_rows(named=True):
            key = (row["pair_from"], row["pair_to"])
            pair_counts[key] += int(row["trips"])
            pair_details.setdefault(key, {k: row[k] for k in row if k != "trips"})

    rows = []
    for (pair_from, pair_to), trips in pair_counts.most_common():
        rows.append({"pair_from": pair_from, "pair_to": pair_to, "trips": trips, **pair_details[(pair_from, pair_to)]})
    return pl.DataFrame(rows, infer_schema_length=None)


def station_map_for(trips: pl.DataFrame, lookup: BikePointLookup) -> pl.DataFrame:
    stations = pl.concat(
        [
            trips.select(
                pl.col("start_station_id").alias("station_id"),
                pl.col("start_station_name").alias("station_name"),
            ),
            trips.select(
                pl.col("end_station_id").alias("station_id"),
                pl.col("end_station_name").alias("station_name"),
            ),
        ]
    ).unique()
    rows = []
    for station in stations.iter_rows(named=True):
        station_id = string_or_none(station["station_id"])
        station_name = string_or_none(station["station_name"])
        match = lookup.match(station_id, station_name)
        rows.append(
            {
                "station_key": make_station_key(station_id, station_name),
                "bikepoint_id": match.row["bikepoint_id"] if match else None,
                "name": match.row["common_name"] if match else station_name,
                "lat": match.row["lat"] if match else None,
                "lon": match.row["lon"] if match else None,
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None)


def station_key_expr(id_column: str, name_column: str) -> pl.Expr:
    return pl.concat_str([pl.col(id_column).fill_null(""), pl.lit("\u001f"), pl.col(name_column).fill_null("")])
