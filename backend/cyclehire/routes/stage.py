import polars as pl

from cyclehire.routes.config import GoogleBicycleRoutesConfig
from cyclehire.routes.google import GoogleBicycleRouteCache, fetch_google_bicycle_routes
from cyclehire.routes.pairs import ranked_route_pairs


def run_google_bicycle_routes(config: GoogleBicycleRoutesConfig) -> None:
    if not config.api_key and not config.dry_run:
        raise ValueError("Set GOOGLE_MAPS_API_KEY before fetching routes.")

    cache = GoogleBicycleRouteCache(config.data_dir)
    pairs = ranked_route_pairs(config.data_dir, config.route_date).head(config.limit)
    fetched = cache.fetched_keys()
    pending = pairs.filter(~pl.concat_str("pair_from", pl.lit(":"), "pair_to").is_in(fetched))

    scope = config.route_date.isoformat() if config.route_date else "all dates"
    print(f"scope: {scope}")
    print(f"candidate undirected pairs: {pairs.height:,}")
    print(f"already fetched in global cache: {len(fetched):,}")
    print(f"pending: {pending.height:,}")
    print(f"cache jsonl: {cache.jsonl_path}")
    print(f"cache parquet: {cache.parquet_path}")

    if config.dry_run:
        print(pending.select("pair_from", "pair_to", "trips").head(20))
        return

    fetch_google_bicycle_routes(
        pending,
        api_key=config.api_key or "",
        cache=cache,
        sleep_seconds=config.sleep_seconds,
    )
    print(f"wrote {cache.jsonl_path}")
    print(f"wrote {cache.parquet_path}")
