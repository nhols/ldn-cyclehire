import logging
from pathlib import Path

import polars as pl

from cyclehire.normalize.csv_schemas import CSV_SCHEMAS, CsvSchema
from cyclehire.tracking import FileRecord


LOGGER = logging.getLogger(__name__)

CANONICAL_COLUMNS = [
    "journey_id",
    "bike_id",
    "bike_model",
    "start_at",
    "end_at",
    "start_station_id",
    "start_station_name",
    "end_station_id",
    "end_station_name",
    "duration_seconds",
    "duration_ms",
    "source_key",
    "source_etag",
    "source_row_number",
]


def read_source_frame(raw_path: Path) -> pl.DataFrame:
    if raw_path.suffix.lower() == ".xlsx":
        return pl.read_excel(raw_path)

    return pl.read_csv(
        raw_path,
        infer_schema=False,
        try_parse_dates=False,
        null_values=[""],
    )


def normalize_csv_frame(frame: pl.DataFrame, item: FileRecord) -> pl.DataFrame:
    columns = set(frame.columns)
    for schema in CSV_SCHEMAS:
        source_columns = schema.columns.required_source_columns()
        if source_columns.issubset(columns):
            LOGGER.debug(
                "Detected %s TfL CSV schema for %s",
                schema.name,
                item.source_key,
            )
            return normalize_mapped_schema(frame, item, schema)

    raise ValueError("Unknown CSV schema. Columns: " + ", ".join(frame.columns))


def normalize_mapped_schema(
    frame: pl.DataFrame,
    item: FileRecord,
    schema: CsvSchema,
) -> pl.DataFrame:
    columns = schema.columns
    start_at = parse_datetime(columns.start_at, list(schema.date_formats))
    end_at = parse_datetime(columns.end_at, list(schema.date_formats))
    computed_duration_ms = (end_at - start_at).dt.total_milliseconds()
    source_duration = pl.col(columns.duration).cast(pl.Int64, strict=False)

    if schema.duration_unit == "milliseconds":
        duration_ms = pl.coalesce(source_duration, computed_duration_ms)
        duration_seconds = duration_ms // 1000
    else:
        duration_seconds = pl.coalesce(source_duration, computed_duration_ms // 1000)
        duration_ms = pl.coalesce(source_duration * 1000, computed_duration_ms)

    return (
        frame.with_row_index("source_row_number", offset=1)
        .select(
            clean_string(columns.journey_id).alias("journey_id"),
            clean_string(columns.bike_id).alias("bike_id"),
            clean_string(columns.bike_model).alias("bike_model"),
            start_at.alias("start_at"),
            end_at.alias("end_at"),
            clean_string(columns.start_station_id).alias("start_station_id"),
            clean_string(columns.start_station_name).alias("start_station_name"),
            clean_string(columns.end_station_id).alias("end_station_id"),
            clean_string(columns.end_station_name).alias("end_station_name"),
            duration_seconds.alias("duration_seconds"),
            duration_ms.alias("duration_ms"),
            pl.lit(item.source_key).alias("source_key"),
            pl.lit(item.etag).alias("source_etag"),
            pl.col("source_row_number").cast(pl.Int64),
        )
        .select(CANONICAL_COLUMNS)
    )


def clean_string(column_name: str | None) -> pl.Expr:
    if column_name is None:
        return pl.lit(None, dtype=pl.String)
    return pl.col(column_name).cast(pl.String).str.strip_chars()


def parse_datetime(column_name: str, fmt: str | list[str]) -> pl.Expr:
    column = pl.col(column_name)
    formats = [fmt] if isinstance(fmt, str) else fmt
    parsed_strings = [
        column.cast(pl.String).str.strip_chars().str.strptime(pl.Datetime, format=date_format, strict=False)
        for date_format in formats
    ]
    return pl.coalesce(
        column.cast(pl.Datetime, strict=False),
        *parsed_strings,
    )
