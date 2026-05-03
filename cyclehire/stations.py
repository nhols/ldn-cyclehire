from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl

from cyclehire.bikepoints.stage import name_key, numeric_key


@dataclass(frozen=True)
class BikePointMatch:
    method: str
    row: dict[str, Any]


@dataclass(frozen=True)
class BikePointLookup:
    bikepoint_number: dict[str, dict[str, Any]]
    terminal_name: dict[str, dict[str, Any]]
    common_name: dict[str, dict[str, Any]]

    @classmethod
    def from_frame(cls, frame: pl.DataFrame) -> BikePointLookup:
        return cls(
            bikepoint_number=lookup(frame, "bikepoint_number_key"),
            terminal_name=lookup(frame, "terminal_name_key"),
            common_name=lookup(frame, "common_name_key"),
        )

    def match(self, station_id: str | None, station_name: str | None) -> BikePointMatch | None:
        id_key = numeric_key(station_id)
        station_name_key = name_key(station_name)
        if id_key and id_key in self.bikepoint_number:
            return BikePointMatch("bikepoint_number", self.bikepoint_number[id_key])
        if id_key and id_key in self.terminal_name:
            return BikePointMatch("terminal_name", self.terminal_name[id_key])
        if station_name_key and station_name_key in self.common_name:
            return BikePointMatch("common_name", self.common_name[station_name_key])
        return None


def lookup(frame: pl.DataFrame, key_column: str) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in frame.filter(pl.col(key_column).is_not_null()).iter_rows(named=True):
        key = row[key_column]
        if key and key not in rows:
            rows[key] = row
    return rows


def station_key(id_column: str, name_column: str) -> pl.Expr:
    return (
        pl.col(id_column).fill_null("")
        + pl.lit("\u001f")
        + pl.col(name_column).fill_null("")
    )


def make_station_key(station_id: str | None, station_name: str | None) -> str:
    return f"{station_id or ''}\u001f{station_name or ''}"


def string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    string_value = str(value).strip()
    return string_value or None
