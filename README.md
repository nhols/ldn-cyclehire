Cycle Hire
==========

Tools for ingesting, validating, and visualising TfL cycle hire usage data.
The Python app prepares static playback data, and the React frontend replays
cycle hire journeys over a map.

Prepare data
------------

Run the pipeline stages in order:

```sh
uv run cyclehire raw
uv run cyclehire normalize
uv run cyclehire validate
uv run cyclehire bikepoints
```

Optional: fetch cached routes so trips can render along routed paths instead of fallback arcs:

```sh
GOOGLE_MAPS_API_KEY="..." uv run cyclehire google-routes --date 2025-06-18 --limit 10000
MAPBOX_ACCESS_TOKEN="..." uv run cyclehire mapbox-routes --limit 100000 --rpm 275
```

Export one day for local development:

```sh
uv run cyclehire export-static --date 2025-06-18 --output-dir frontend/public/data --route-provider mapbox
```

Export all days for CDN upload:

```sh
make export-static-full
```

Upload compressed static data to Cloudflare R2:

```sh
make upload-static-r2
```

Run the app
-----------

Install dependencies once:

```sh
make install
```

Export local static data and start the frontend:

```sh
make dev
```

The frontend runs on `http://localhost:5173`.
