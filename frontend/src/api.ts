import type {
  DateRangeResponse,
  PlaybackResponse,
  StaticManifest,
  StaticPlaybackResponse,
  StaticRoutesResponse
} from "./types";

const DATA_BASE_URL = (import.meta.env.VITE_STATIC_DATA_BASE_URL ?? "/data").replace(/\/$/, "");

let manifestPromise: Promise<StaticManifest> | null = null;
let routesPromise: Promise<StaticRoutesResponse> | null = null;

export async function fetchDateRange(): Promise<DateRangeResponse> {
  const manifest = await fetchManifest();
  return manifest.dateRange;
}

export async function fetchPlayback(date: string): Promise<PlaybackResponse> {
  const manifest = await fetchManifest();
  const day = manifest.days.find((item) => item.date === date);
  if (!day) {
    throw new Error(`No static playback data exported for ${date}`);
  }

  const [playback, routes] = await Promise.all([
    fetchJson<StaticPlaybackResponse>(day.path),
    fetchRoutes()
  ]);

  return {
    ...playback,
    trips: playback.trips.map((trip) => {
      const route = trip.routeKey ? routes.routes[trip.routeKey] : null;
      const path = route ? (trip.routeReversed ? [...route].reverse() : route) : null;
      return {
        ...trip,
        path
      };
    })
  };
}

function fetchManifest(): Promise<StaticManifest> {
  manifestPromise ??= fetchJson<StaticManifest>("manifest.json");
  return manifestPromise;
}

function fetchRoutes(): Promise<StaticRoutesResponse> {
  routesPromise ??= fetchManifest().then((manifest) => fetchJson<StaticRoutesResponse>(manifest.files.routes.path));
  return routesPromise;
}

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`${DATA_BASE_URL}/${path.replace(/^\//, "")}`);
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return response.json();
}
