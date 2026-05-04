from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class CsvColumns:
    journey_id: str
    bike_id: str
    start_at: str
    end_at: str
    start_station_id: str
    start_station_name: str
    end_station_id: str | None
    end_station_name: str
    duration: str
    bike_model: str | None = None

    def required_source_columns(self) -> set[str]:
        return {
            column
            for column in (
                self.journey_id,
                self.bike_id,
                self.start_at,
                self.end_at,
                self.start_station_id,
                self.start_station_name,
                self.end_station_id,
                self.end_station_name,
                self.duration,
                self.bike_model,
            )
            if column is not None
        }


@dataclass(frozen=True)
class CsvSchema:
    name: str
    date_formats: tuple[str, ...]
    duration_unit: Literal["seconds", "milliseconds"]
    columns: CsvColumns


CSV_SCHEMAS = [
    CsvSchema(
        name="old",
        date_formats=("%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S"),
        duration_unit="seconds",
        columns=CsvColumns(
            journey_id="Rental Id",
            bike_id="Bike Id",
            start_at="Start Date",
            end_at="End Date",
            start_station_id="StartStation Id",
            start_station_name="StartStation Name",
            end_station_id="EndStation Id",
            end_station_name="EndStation Name",
            duration="Duration",
        ),
    ),
    CsvSchema(
        name="old_logical_terminal",
        date_formats=("%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S"),
        duration_unit="seconds",
        columns=CsvColumns(
            journey_id="Rental Id",
            bike_id="Bike Id",
            start_at="Start Date",
            end_at="End Date",
            start_station_id="StartStation Logical Terminal",
            start_station_name="StartStation Name",
            end_station_id="EndStation Logical Terminal",
            end_station_name="EndStation Name",
            duration="Duration",
        ),
    ),
    CsvSchema(
        name="old_spaced_station_columns",
        date_formats=("%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S"),
        duration_unit="seconds",
        columns=CsvColumns(
            journey_id="Rental Id",
            bike_id="Bike Id",
            start_at="Start Date",
            end_at="End Date",
            start_station_id="Start Station Id",
            start_station_name="Start Station Name",
            end_station_id="End Station Id",
            end_station_name="End Station Name",
            duration="Duration_Seconds",
        ),
    ),
    CsvSchema(
        name="old_missing_end_station_id",
        date_formats=("%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S"),
        duration_unit="seconds",
        columns=CsvColumns(
            journey_id="Rental Id",
            bike_id="Bike Id",
            start_at="Start Date",
            end_at="End Date",
            start_station_id="StartStation Id",
            start_station_name="StartStation Name",
            end_station_id=None,
            end_station_name="EndStation Name",
            duration="Duration",
        ),
    ),
    CsvSchema(
        name="new",
        date_formats=("%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M"),
        duration_unit="milliseconds",
        columns=CsvColumns(
            journey_id="Number",
            bike_id="Bike number",
            bike_model="Bike model",
            start_at="Start date",
            end_at="End date",
            start_station_id="Start station number",
            start_station_name="Start station",
            end_station_id="End station number",
            end_station_name="End station",
            duration="Total duration (ms)",
        ),
    ),
]
