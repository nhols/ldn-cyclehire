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

Optional: fetch cached Google bicycle routes for smoother map playback:

```sh
GOOGLE_MAPS_API_KEY="..." uv run cyclehire google-routes --date 2025-06-18 --limit 10000
```

Optional: export CDN-ready static playback data:

```sh
uv run cyclehire export-static --date 2025-06-18
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
