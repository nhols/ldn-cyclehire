import logging
from dataclasses import dataclass
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Annotated

import typer

from cyclehire.bikepoints import BikePointsConfig, run_bikepoints_pipeline
from cyclehire.normalize import NormalizePipelineConfig, run_normalize_pipeline
from cyclehire.raw import RawPipelineConfig, run_raw_pipeline
from cyclehire.validate import ValidatePipelineConfig, run_validate_pipeline


class LogLevel(str, Enum):
    debug = "DEBUG"
    info = "INFO"
    warning = "WARNING"
    error = "ERROR"
    critical = "CRITICAL"


@dataclass(frozen=True)
class CliContext:
    data_dir: Path
    log_level: LogLevel


app = typer.Typer(
    help="Ingest and validate TfL cycle hire usage data.",
    no_args_is_help=True,
)


@app.callback()
def configure(
    context: typer.Context,
    data_dir: Annotated[
        Path,
        typer.Option(
            "--data-dir",
            help="Root directory for local data files and metadata.",
        ),
    ] = Path("data"),
    log_level: Annotated[
        LogLevel,
        typer.Option(
            "--log-level",
            help="Logging verbosity.",
        ),
    ] = LogLevel.info,
) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.value),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    context.obj = CliContext(data_dir=data_dir, log_level=log_level)


@app.command()
def raw(
    context: typer.Context,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="List and track planned work without downloading files."),
    ] = False,
    limit: Annotated[
        int | None,
        typer.Option("--limit", help="Maximum number of files to download or plan in this run."),
    ] = None,
    include_zero_byte: Annotated[
        bool,
        typer.Option("--include-zero-byte", help="Include zero-byte objects from the source listing."),
    ] = False,
) -> None:
    cli_context = _cli_context(context)
    run_raw_pipeline(
        RawPipelineConfig(
            data_dir=cli_context.data_dir,
            dry_run=dry_run,
            limit=limit,
            include_zero_byte=include_zero_byte,
        )
    )


@app.command()
def normalize(
    context: typer.Context,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="List files that would be normalized without writing outputs."),
    ] = False,
    limit: Annotated[
        int | None,
        typer.Option("--limit", help="Maximum number of files to normalize or plan in this run."),
    ] = None,
    retry_failed: Annotated[
        bool,
        typer.Option("--retry-failed", help="Retry files with normalized_status = failed."),
    ] = False,
    force: Annotated[
        bool,
        typer.Option("--force", help="Re-normalize files even if already normalized."),
    ] = False,
) -> None:
    cli_context = _cli_context(context)
    run_normalize_pipeline(
        NormalizePipelineConfig(
            data_dir=cli_context.data_dir,
            dry_run=dry_run,
            limit=limit,
            retry_failed=retry_failed,
            force=force,
        )
    )


@app.command()
def validate(
    context: typer.Context,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="List files that would be validated without writing outputs."),
    ] = False,
    limit: Annotated[
        int | None,
        typer.Option("--limit", help="Maximum number of files to validate or plan in this run."),
    ] = None,
    retry_failed: Annotated[
        bool,
        typer.Option("--retry-failed", help="Retry files with validated_status = failed."),
    ] = False,
    force: Annotated[
        bool,
        typer.Option("--force", help="Re-validate files even if already validated."),
    ] = False,
) -> None:
    cli_context = _cli_context(context)
    run_validate_pipeline(
        ValidatePipelineConfig(
            data_dir=cli_context.data_dir,
            dry_run=dry_run,
            limit=limit,
            retry_failed=retry_failed,
            force=force,
        )
    )


@app.command()
def bikepoints(
    context: typer.Context,
    sample_date: Annotated[
        list[str] | None,
        typer.Option(
            "--sample-date",
            metavar="YYYY-MM-DD",
            help="Trip date to sample for station matching. Can be passed multiple times.",
        ),
    ] = None,
) -> None:
    cli_context = _cli_context(context)
    run_bikepoints_pipeline(
        BikePointsConfig(
            data_dir=cli_context.data_dir,
            sample_dates=parse_sample_dates(sample_date),
        )
    )


def parse_sample_dates(values: list[str] | None) -> tuple[date, ...]:
    if not values:
        return default_bikepoint_sample_dates()
    return tuple(date.fromisoformat(value) for value in values)


def default_bikepoint_sample_dates() -> tuple[date, ...]:
    return (
        date(2015, 6, 17),
        date(2018, 6, 13),
        date(2021, 6, 16),
        date(2024, 6, 19),
        date(2025, 6, 18),
    )


def _cli_context(context: typer.Context) -> CliContext:
    if not isinstance(context.obj, CliContext):
        raise typer.BadParameter("CLI context was not configured")
    return context.obj


def main() -> None:
    app()
