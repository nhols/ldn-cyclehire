Cycle Hire
==========

Tools for ingesting, validating, and visualising TfL cycle hire usage data.

The app includes a Python backend for the data pipeline/API and a React map
frontend for replaying cycle hire journeys over time.

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

Run the app
-----------

Install dependencies once:

```sh
make install
```

Start both backend and frontend dev servers:

```sh
make dev
```

The frontend runs on `http://localhost:5173` and proxies API requests to the
FastAPI backend on `http://127.0.0.1:8000`.
