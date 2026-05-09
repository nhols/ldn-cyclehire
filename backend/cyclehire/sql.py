from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
import os
from pathlib import Path
import sys
from time import perf_counter

import polars as pl

from cyclehire.bikepoints.paths import bikepoints_parquet_path, station_match_samples_path
from cyclehire.routes.paths import google_bicycle_routes_parquet_path, mapbox_cycling_routes_parquet_path


@dataclass(frozen=True)
class SqlTable:
    name: str
    frame: pl.LazyFrame
    source: str
    description: str


def run_sql_shell(
    data_dir: Path,
    query: str | None = None,
    display_limit: int = 100,
) -> None:
    tables = discover_tables(data_dir)
    if not tables:
        raise FileNotFoundError(f"No queryable Parquet tables found under {data_dir}")
    colors = AnsiColors.enabled_for_terminal()

    context = pl.SQLContext()
    for table in tables:
        context.register(table.name, table.frame)

    if query:
        execute_query(context, query, display_limit)
        return

    print(colors.bold("Cyclehire SQL"))
    table_names = ", ".join(colors.cyan(table.name) for table in tables)
    print(f"Registered tables: {table_names}")
    print(f"Type {colors.cyan('.help')} for commands, {colors.cyan('.quit')} to exit. End SQL queries with ;")
    with configure_readline(tables, data_dir / "metadata" / "sql_history"):
        repl(context, tables, display_limit, colors)


def discover_tables(data_dir: Path) -> list[SqlTable]:
    tables: list[SqlTable] = []

    validated_trip_files = sorted((data_dir / "validated" / "trips_by_file").glob("*.parquet"))
    if validated_trip_files:
        tables.append(
            SqlTable(
                name="trips",
                frame=scan_many_parquet(validated_trip_files),
                source="validated/trips_by_file/*.parquet",
                description="Validated TfL cycle hire journeys",
            )
        )

    normalized_trip_files = sorted((data_dir / "cache" / "normalized").glob("*.parquet"))
    if normalized_trip_files:
        tables.append(
            SqlTable(
                name="normalized_trips",
                frame=scan_many_parquet(normalized_trip_files),
                source="cache/normalized/*.parquet",
                description="Canonical trip rows before validation filtering",
            )
        )

    maybe_add_parquet_table(
        tables,
        data_dir / bikepoints_parquet_path(),
        name="bikepoints",
        source=str(bikepoints_parquet_path()),
        description="Flattened TfL BikePoint station metadata",
    )
    maybe_add_parquet_table(
        tables,
        data_dir / station_match_samples_path(),
        name="station_match_samples",
        source=str(station_match_samples_path()),
        description="Sampled station-name to BikePoint matching diagnostics",
    )
    maybe_add_parquet_table(
        tables,
        data_dir / google_bicycle_routes_parquet_path(),
        name="google_routes",
        source=str(google_bicycle_routes_parquet_path()),
        description="Cached Google bicycle route responses",
    )
    maybe_add_parquet_table(
        tables,
        data_dir / mapbox_cycling_routes_parquet_path(),
        name="mapbox_routes",
        source=str(mapbox_cycling_routes_parquet_path()),
        description="Cached Mapbox cycling route responses",
    )
    maybe_add_parquet_table(
        tables,
        data_dir / "reference" / "routes" / "pair_coverage_cdf.parquet",
        name="route_pair_coverage",
        source="reference/routes/pair_coverage_cdf.parquet",
        description="Ranked station-pair trip coverage",
    )

    return tables


def scan_many_parquet(paths: list[Path]) -> pl.LazyFrame:
    return pl.scan_parquet(
        [str(path) for path in paths],
        missing_columns="insert",
        extra_columns="ignore",
    )


def maybe_add_parquet_table(
    tables: list[SqlTable],
    path: Path,
    *,
    name: str,
    source: str,
    description: str,
) -> None:
    if path.exists():
        tables.append(
            SqlTable(
                name=name,
                frame=pl.scan_parquet(str(path)),
                source=source,
                description=description,
            )
        )


def repl(context: pl.SQLContext, tables: list[SqlTable], display_limit: int, colors: AnsiColors) -> None:
    buffer: list[str] = []
    while True:
        prompt = colors.prompt("sql> " if not buffer else "...> ")
        try:
            line = input(prompt)
        except EOFError:
            print()
            return
        except KeyboardInterrupt:
            print()
            buffer.clear()
            continue

        stripped = line.strip()
        if not buffer and not stripped:
            continue
        if not buffer and stripped.startswith("."):
            if handle_command(stripped, tables, display_limit, colors):
                return
            continue

        buffer.append(line)
        if stripped.endswith(";"):
            query = "\n".join(buffer).strip()
            buffer.clear()
            execute_query(context, query, display_limit, colors)


def handle_command(command: str, tables: list[SqlTable], display_limit: int, colors: AnsiColors) -> bool:
    parts = command.split()
    name = parts[0].lower()

    if name in {".quit", ".exit", ".q"}:
        return True
    if name == ".help":
        print_help(display_limit, colors)
        return False
    if name == ".tables":
        print_tables(tables)
        return False
    if name == ".schema":
        print_schema(tables, parts[1] if len(parts) > 1 else None, colors)
        return False

    print(colors.red(f"Unknown command: {parts[0]}.") + f" Type {colors.cyan('.help')} for commands.")
    return False


def print_help(display_limit: int, colors: AnsiColors) -> None:
    print(
        f"""{colors.bold("Commands:")}
  .tables          List registered tables
  .schema [table]  Show column names and types
  .help            Show this help
  .quit            Exit

{colors.bold("SQL:")}
  Finish queries with a semicolon.
  Results show at most {display_limit} rows; add LIMIT in SQL for tighter output.

{colors.bold("Example:")}
  SELECT CAST(start_at AS DATE) AS day, COUNT(*) AS trips
  FROM trips
  WHERE start_at >= '2025-06-01' AND start_at < '2025-07-01'
  GROUP BY day
  ORDER BY trips DESC
  LIMIT 10;
"""
    )


def print_tables(tables: list[SqlTable]) -> None:
    frame = pl.DataFrame(
        {
            "table": [table.name for table in tables],
            "source": [table.source for table in tables],
            "description": [table.description for table in tables],
        }
    )
    print_frame(frame, display_limit=len(tables))


def print_schema(tables: list[SqlTable], table_name: str | None, colors: AnsiColors) -> None:
    selected = [table for table in tables if table.name == table_name] if table_name else tables
    if not selected:
        print(colors.red(f"Unknown table: {table_name}.") + f" Type {colors.cyan('.tables')} to see registered tables.")
        return

    for index, table in enumerate(selected):
        if index:
            print()
        schema = table.frame.collect_schema()
        frame = pl.DataFrame(
            {
                "column": schema.names(),
                "type": [str(dtype) for dtype in schema.dtypes()],
            }
        )
        print(f"{colors.cyan(table.name)} {colors.dim(f'({table.source})')}")
        print_frame(frame, display_limit=len(frame))


def execute_query(context: pl.SQLContext, query: str, display_limit: int, colors: AnsiColors | None = None) -> None:
    colors = colors or AnsiColors.enabled_for_terminal()
    started = perf_counter()
    try:
        result = context.execute(query.rstrip(";"), eager=False)
        frame = result.limit(display_limit + 1).collect()
    except KeyboardInterrupt:
        print(colors.yellow("Cancelled"))
        return
    except Exception as exc:
        print(colors.red(f"Error: {exc}"))
        return

    elapsed_ms = (perf_counter() - started) * 1000
    has_more = frame.height > display_limit
    if has_more:
        frame = frame.head(display_limit)

    print_frame(frame, display_limit)
    suffix = colors.yellow(" (truncated)") if has_more else ""
    print(colors.dim(f"{frame.height:,} row{'s' if frame.height != 1 else ''} shown in {elapsed_ms:,.0f} ms") + suffix)


def print_frame(frame: pl.DataFrame, display_limit: int) -> None:
    with pl.Config(
        tbl_rows=max(display_limit, 1),
        tbl_cols=-1,
        tbl_width_chars=160,
        fmt_str_lengths=80,
    ):
        print(frame)


@dataclass(frozen=True)
class AnsiColors:
    enabled: bool

    @classmethod
    def enabled_for_terminal(cls) -> AnsiColors:
        enabled = (
            sys.stdout.isatty()
            and os.environ.get("NO_COLOR") is None
            and os.environ.get("TERM") != "dumb"
        )
        return cls(enabled)

    def prompt(self, text: str) -> str:
        if not self.enabled:
            return text
        return f"\x01\033[1;32m\x02{text}\x01\033[0m\x02"

    def bold(self, text: str) -> str:
        return self.wrap(text, "1")

    def dim(self, text: str) -> str:
        return self.wrap(text, "2")

    def red(self, text: str) -> str:
        return self.wrap(text, "31")

    def yellow(self, text: str) -> str:
        return self.wrap(text, "33")

    def cyan(self, text: str) -> str:
        return self.wrap(text, "36")

    def wrap(self, text: str, code: str) -> str:
        if not self.enabled:
            return text
        return f"\033[{code}m{text}\033[0m"


class configure_readline:
    def __init__(self, tables: list[SqlTable], history_path: Path) -> None:
        self.tables = tables
        self.history_path = history_path
        self.readline = None
        self.previous_completer: Callable[[str, int], str | None] | None = None

    def __enter__(self) -> None:
        with suppress(ImportError):
            import readline

            self.readline = readline
            self.previous_completer = readline.get_completer()
            readline.set_completer(SqlCompleter(self.tables))
            readline.set_completer_delims(" \t\n`!@#$%^&*()-=+[{]}\\|;:'\",<>/?")
            if readline.__doc__ and "libedit" in readline.__doc__:
                readline.parse_and_bind("bind ^I rl_complete")
            else:
                readline.parse_and_bind("tab: complete")

            self.history_path.parent.mkdir(parents=True, exist_ok=True)
            with suppress(OSError):
                readline.read_history_file(str(self.history_path))

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self.readline is None:
            return
        with suppress(OSError):
            self.readline.write_history_file(str(self.history_path))
        self.readline.set_completer(self.previous_completer)


class SqlCompleter:
    commands = (".help", ".quit", ".exit", ".q", ".tables", ".schema")
    table_context_keywords = {"from", "join", "table", "describe", "into", "update"}

    def __init__(self, tables: list[SqlTable]) -> None:
        self.table_names = tuple(table.name for table in tables)
        self.matches: list[str] = []

    def __call__(self, text: str, state: int) -> str | None:
        if state == 0:
            self.matches = self.build_matches(text)
        if state >= len(self.matches):
            return None
        return self.matches[state]

    def build_matches(self, text: str) -> list[str]:
        import readline

        line = readline.get_line_buffer()
        stripped = line.lstrip()
        lower_text = text.lower()

        if stripped.startswith("."):
            words = stripped.split()
            if stripped.startswith(".schema ") or words[:1] == [".schema"]:
                return self.match_values(self.table_names, lower_text)
            return self.match_values(self.commands, lower_text)

        words_before_cursor = line[: readline.get_begidx()].split()
        previous = words_before_cursor[-1].rstrip(",").lower() if words_before_cursor else ""
        if previous in self.table_context_keywords or text:
            return self.match_values(self.table_names, lower_text)
        return []

    @staticmethod
    def match_values(values: tuple[str, ...], lower_text: str) -> list[str]:
        return [value for value in values if value.lower().startswith(lower_text)]
