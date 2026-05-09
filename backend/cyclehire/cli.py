import logging
import os
from dataclasses import dataclass
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Annotated

import typer

from cyclehire.bikepoints import BikePointsConfig, run_bikepoints_pipeline
from cyclehire.cdn import (
    DEFAULT_ROUTE_SHARD_COMPRESSION_RATIO,
    DEFAULT_ROUTE_SHARD_TARGET_GZIP_BYTES,
    CdnExportConfig,
    RouteProvider,
    run_cdn_export,
)
from cyclehire.normalize import NormalizePipelineConfig, run_normalize_pipeline
from cyclehire.raw import RawPipelineConfig, run_raw_pipeline
from cyclehire.routes import (
    GoogleBicycleRoutesConfig,
    MapboxCyclingRoutesConfig,
    run_google_bicycle_routes,
    run_mapbox_cycling_routes,
)
from cyclehire.sql import run_sql_shell
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


@app.command()
def google_routes(
    context: typer.Context,
    route_date: Annotated[
        str | None,
        typer.Option(
            "--date",
            metavar="YYYY-MM-DD",
            help="Optional trip date for ranking route candidates. Omit to rank all dates.",
        ),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", help="Maximum candidate pairs to fetch."),
    ] = 10_000,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Print pending pairs without fetching."),
    ] = False,
    requests_per_minute: Annotated[
        float | None,
        typer.Option("--rpm", help="Optional request start-rate limit per minute."),
    ] = None,
) -> None:
    cli_context = _cli_context(context)
    run_google_bicycle_routes(
        GoogleBicycleRoutesConfig(
            data_dir=cli_context.data_dir,
            api_key=os.environ.get("GOOGLE_MAPS_API_KEY"),
            route_date=date.fromisoformat(route_date) if route_date else None,
            limit=limit,
            dry_run=dry_run,
            requests_per_minute=requests_per_minute,
        )
    )


@app.command()
def mapbox_routes(
    context: typer.Context,
    route_date: Annotated[
        str | None,
        typer.Option(
            "--date",
            metavar="YYYY-MM-DD",
            help="Optional trip date for ranking route candidates. Omit to rank all dates.",
        ),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", help="Maximum candidate pairs to fetch."),
    ] = 100_000,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Print pending pairs without fetching."),
    ] = False,
    requests_per_minute: Annotated[
        float,
        typer.Option("--rpm", help="Maximum request starts per minute."),
    ] = 275,
) -> None:
    cli_context = _cli_context(context)
    run_mapbox_cycling_routes(
        MapboxCyclingRoutesConfig(
            data_dir=cli_context.data_dir,
            access_token=os.environ.get("MAPBOX_ACCESS_TOKEN"),
            route_date=date.fromisoformat(route_date) if route_date else None,
            limit=limit,
            dry_run=dry_run,
            requests_per_minute=requests_per_minute,
        )
    )


@app.command()
def export_static(
    context: typer.Context,
    export_date: Annotated[
        list[str] | None,
        typer.Option(
            "--date",
            metavar="YYYY-MM-DD",
            help="Playback date to export. Can be passed multiple times. Omit to export all dates.",
        ),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Directory for CDN-ready static data files."),
    ] = Path("data") / "cdn",
    limit_days: Annotated[
        int | None,
        typer.Option("--limit-days", help="Maximum number of days to export, useful for smoke tests."),
    ] = None,
    route_provider: Annotated[
        RouteProvider,
        typer.Option(
            "--route-provider",
            help="Route cache to include in exported playback data.",
        ),
    ] = RouteProvider.all,
    route_shard_target_gzip_mb: Annotated[
        float,
        typer.Option(
            "--route-shard-target-gzip-mb",
            help="Target compressed route shard size in MB. Shards are packed using the estimated compression ratio.",
        ),
    ] = DEFAULT_ROUTE_SHARD_TARGET_GZIP_BYTES / 1_000_000,
    route_shard_compression_ratio: Annotated[
        float,
        typer.Option(
            "--route-shard-compression-ratio",
            help="Estimated raw JSON to gzip compression ratio used when packing route shards.",
        ),
    ] = DEFAULT_ROUTE_SHARD_COMPRESSION_RATIO,
) -> None:
    cli_context = _cli_context(context)
    run_cdn_export(
        CdnExportConfig(
            data_dir=cli_context.data_dir,
            output_dir=output_dir,
            dates=tuple(date.fromisoformat(value) for value in export_date or ()),
            limit_days=limit_days,
            route_provider=route_provider,
            route_shard_target_gzip_bytes=int(route_shard_target_gzip_mb * 1_000_000),
            route_shard_compression_ratio=route_shard_compression_ratio,
        )
    )


@app.command()
def sql(
    context: typer.Context,
    query: Annotated[
        str | None,
        typer.Option("--query", "-q", help="Run one SQL query and exit instead of opening the REPL."),
    ] = None,
    display_limit: Annotated[
        int,
        typer.Option("--limit", "-n", min=1, help="Maximum rows to display for each query result."),
    ] = 100,
) -> None:
    cli_context = _cli_context(context)
    run_sql_shell(cli_context.data_dir, query=query, display_limit=display_limit)


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
