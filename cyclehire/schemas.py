import dataframely as dy
import polars as pl


class TripSchema(dy.Schema):
    journey_id = dy.String(nullable=False, primary_key=True, min_length=1)
    bike_id = dy.String(nullable=False, min_length=1)
    bike_model = dy.String(nullable=True, min_length=1)
    start_at = dy.Datetime(nullable=False)
    end_at = dy.Datetime(nullable=False)
    start_station_id = dy.String(nullable=False, min_length=1)
    start_station_name = dy.String(nullable=False, min_length=1)
    end_station_id = dy.String(nullable=True, min_length=1)
    end_station_name = dy.String(nullable=False, min_length=1)
    duration_seconds = dy.Int64(nullable=False, min=0)
    duration_ms = dy.Int64(nullable=False, min=0)
    source_key = dy.String(nullable=False, min_length=1)
    source_etag = dy.String(nullable=False, min_length=1)
    source_row_number = dy.Int64(nullable=False, min=1)

    @dy.rule()
    def end_not_before_start(schema) -> pl.Expr:
        return pl.col("end_at") >= pl.col("start_at")

    @dy.rule()
    def duration_units_agree(schema) -> pl.Expr:
        lower_bound = pl.col("duration_seconds") * 1000
        upper_bound = (pl.col("duration_seconds") + 1) * 1000
        return (pl.col("duration_ms") >= lower_bound) & (pl.col("duration_ms") < upper_bound)

    @dy.rule()
    def duration_matches_timestamps(schema) -> pl.Expr:
        timestamp_duration_ms = (
            pl.col("end_at") - pl.col("start_at")
        ).dt.total_milliseconds()
        return (pl.col("duration_ms") - timestamp_duration_ms).abs() <= 120_000

    @dy.rule()
    def plausible_duration(schema) -> pl.Expr:
        return pl.col("duration_ms") <= 14 * 24 * 60 * 60 * 1000
