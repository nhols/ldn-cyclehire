from datetime import date
from pathlib import Path

import polars as pl

from cyclehire.routes.common import JsonlRouteCache
from cyclehire.routes.config import GoogleBicycleRoutesConfig, MapboxCyclingRoutesConfig
from cyclehire.routes.google import GoogleBicycleRouteCache, fetch_google_bicycle_routes
from cyclehire.routes.mapbox import MapboxCyclingRouteCache, fetch_mapbox_cycling_routes
from cyclehire.routes.pairs import ranked_route_pairs


def run_google_bicycle_routes(config: GoogleBicycleRoutesConfig) -> None:
    if not config.api_key and not config.dry_run:
        raise ValueError("Set GOOGLE_MAPS_API_KEY before fetching routes.")

    cache = GoogleBicycleRouteCache(config.data_dir)
    pending = pending_route_pairs(
        config.data_dir,
        config.route_date,
        config.limit,
        cache,
        cache_label="global cache",
    )

    if config.dry_run:
        print_pending(pending)
        return

    fetch_google_bicycle_routes(
        pending,
        api_key=config.api_key or "",
        cache=cache,
        requests_per_minute=config.requests_per_minute,
    )
    print(f"wrote {cache.jsonl_path}")
    print(f"wrote {cache.parquet_path}")


def run_mapbox_cycling_routes(config: MapboxCyclingRoutesConfig) -> None:
    if not config.access_token and not config.dry_run:
        raise ValueError("Set MAPBOX_ACCESS_TOKEN before fetching routes.")

    cache = MapboxCyclingRouteCache(config.data_dir)
    pending = pending_route_pairs(
        config.data_dir,
        config.route_date,
        config.limit,
        cache,
        cache_label="mapbox cache",
    )

    if config.dry_run:
        print_pending(pending)
        return

    fetch_mapbox_cycling_routes(
        pending,
        access_token=config.access_token or "",
        cache=cache,
        requests_per_minute=config.requests_per_minute,
    )
    print(f"wrote {cache.jsonl_path}")
    print(f"wrote {cache.parquet_path}")


def pending_route_pairs(
    data_dir: Path,
    route_date: date | None,
    limit: int,
    cache: JsonlRouteCache,
    *,
    cache_label: str,
) -> pl.DataFrame:
    pairs = ranked_route_pairs(data_dir, route_date).head(limit)
    fetched = cache.fetched_keys()
    pending = pairs.filter(~pl.concat_str("pair_from", pl.lit(":"), "pair_to").is_in(fetched))

    scope = route_date.isoformat() if route_date else "all dates"
    print(f"scope: {scope}")
    print(f"candidate undirected pairs: {pairs.height:,}")
    print(f"already fetched in {cache_label}: {len(fetched):,}")
    print(f"pending: {pending.height:,}")
    print(f"cache jsonl: {cache.jsonl_path}")
    print(f"cache parquet: {cache.parquet_path}")
    return pending


def print_pending(pending: pl.DataFrame) -> None:
    print(pending.select("pair_from", "pair_to", "trips").head(20))
