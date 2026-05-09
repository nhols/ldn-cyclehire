London Cycle Hire
=================

Replay and explore TfL cycle hire journeys for a selected day, with trips moving
across London as the clock rolls forward.

https://ldn-cyclehire.pages.dev

https://github.com/user-attachments/assets/6159ed4a-8c69-4f7b-ab19-873966fd91d7

Methodology
-----------

The project starts with TfL's cycle hire journey records and BikePoint station
data, then prepares them so a single day can be replayed smoothly in the browser.

Where possible, trips are matched to cycling directions from
[Mapbox Directions](https://docs.mapbox.com/api/navigation/directions/) or
[Google Routes](https://developers.google.com/maps/documentation/routes). When a
route has not been fetched yet, the trip is still shown as a simple arc between
the two stations.

During playback, the clock moves through the selected day at the chosen speed.
Each trip appears as a trace on the map when it departs, travels for its real
recorded duration, and finishes at its arrival station. Routed and unrouted trips
use different colours, so it is easy to see which journeys are following a
fetched route.

The stations are live too: they flash when journeys depart or arrive, grow or
shrink as the station gains or loses bikes over the day, and can be selected to
focus the map on journeys touching particular places.

Running the app
---------------

Install dependencies:

```sh
make install
```

Prepare TfL journey and station data:

```sh
make prep-tfl-data
```

Fetch optional station-to-station routes. Use either provider:

```sh
MAPBOX_ACCESS_TOKEN="..." uv run cyclehire mapbox-routes --limit 100000 --rpm 275
GOOGLE_MAPS_API_KEY="..." uv run cyclehire google-routes --limit 10000
```

Export a few demo days for local playback:

```sh
uv run cyclehire export-static \
  --date 2025-06-18 \
  --date 2025-05-05 \
  --date 2022-05-15 \
  --output-dir frontend/public/data \
  --route-provider mapbox
```

Run the frontend:

```sh
make frontend
```

Open `http://localhost:5173`.

Querying the data
-----------------

Open an interactive SQL shell over the local Parquet datasets:

```sh
make sql
```

Useful shell commands:

```text
.tables
.schema trips
.quit
```

Tab completes shell commands and registered table names, arrow keys recall
history, and `Ctrl+C` cancels the current input or running query.

Example query:

```sql
SELECT CAST(start_at AS DATE) AS day, COUNT(*) AS trips
FROM trips
WHERE start_at >= '2025-06-01' AND start_at < '2025-07-01'
GROUP BY day
ORDER BY trips DESC
LIMIT 10;
```

For one-off queries:

```sh
uv run cyclehire sql --query "SELECT COUNT(*) AS trips FROM trips"
```

Exporting to R2
---------------

Export all prepared days and route shards:

```sh
make export-static-full
```

Upload the compressed static files to Cloudflare R2:

```sh
make upload-static-r2
```

The uploader prepares a temporary upload directory from `data/cdn` and pushes it
through R2's S3-compatible API. Configure credentials in `.env`:

```sh
R2_ACCESS_KEY_ID="..."
R2_SECRET_ACCESS_KEY="..."
R2_ACCOUNT_ID="..."
```

The frontend reads static data from `VITE_STATIC_DATA_BASE_URL`; set that value
in Cloudflare Pages to the public R2 bucket URL.

Acknowledgements
----------------

Powered by TfL Open Data.

This project uses TfL transport data under the
[Transport Data Service terms](https://tfl.gov.uk/corporate/terms-and-conditions/transport-data-service).
TfL requires the attribution statement above and notes that the information
contains Ordnance Survey derived data: Contains OS data © Crown copyright and
database rights 2016 and Geomni UK Map data © and database rights 2019.

This project is not endorsed by, certified by, or affiliated with Transport for
London.
