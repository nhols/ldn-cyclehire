import type {
  DateRangeResponse,
  DaySummary,
  PlaybackResponse,
  Coord,
  FlatPath,
  StaticManifest,
  StaticPlaybackResponse,
  StaticRoutesResponse
} from "./types";

const DATA_BASE_URL = (import.meta.env.VITE_STATIC_DATA_BASE_URL ?? "/data").replace(/\/$/, "");

let manifestPromise: Promise<StaticManifest> | null = null;
const inFlightRouteShardPromises = new Map<string, Promise<StaticRoutesResponse>>();

export async function fetchDateRange(): Promise<DateRangeResponse> {
  const manifest = await fetchManifest();
  return manifest.dateRange;
}

export async function fetchDaySummaries(): Promise<DaySummary[]> {
  const manifest = await fetchManifest();
  return manifest.days;
}

export async function fetchPlayback(date: string): Promise<PlaybackResponse> {
  const manifest = await fetchManifest();
  const day = manifest.days.find((item) => item.date === date);
  if (!day) {
    throw new Error(`No static playback data exported for ${date}`);
  }

  const playback = await fetchJson<StaticPlaybackResponse>(day.path);

  return {
    ...playback,
    trips: playback.trips.map((trip) => {
      return {
        ...trip,
        path: null
      };
    })
  };
}

export async function fetchRouteShardRoutes(
  shardId: string,
  routeKeys: Set<string>
): Promise<globalThis.Map<string, FlatPath>> {
  const manifest = await fetchManifest();
  const routeShards = new Map((manifest.files.routes.shards ?? []).map((shard) => [shard.id, shard.path]));
  const path = routeShards.get(shardId) ?? manifest.files.routes.shardTemplate?.replace("{shard}", shardId);
  if (!path) {
    throw new Error(`No route shard path for ${shardId}`);
  }
  const payload = await fetchRouteShardPath(shardId, path);
  const routes = new globalThis.Map<string, FlatPath>();
  for (const [routeKey, coordinates] of Object.entries(payload.routes)) {
    if (routeKeys.has(routeKey)) {
      routes.set(routeKey, flattenCoordinates(coordinates));
    }
  }
  return routes;
}

function flattenCoordinates(coordinates: Coord[]): FlatPath {
  const path = new Float32Array(coordinates.length * 2);
  for (let index = 0; index < coordinates.length; index += 1) {
    path[index * 2] = coordinates[index][0];
    path[index * 2 + 1] = coordinates[index][1];
  }
  return path;
}

function fetchManifest(): Promise<StaticManifest> {
  manifestPromise ??= fetchJson<StaticManifest>("manifest.json");
  return manifestPromise;
}

function fetchRouteShardPath(shardId: string, path: string): Promise<StaticRoutesResponse> {
  const key = `${shardId}:${path}`;
  const cached = inFlightRouteShardPromises.get(key);
  if (cached) {
    return cached;
  }
  const promise = fetchJson<StaticRoutesResponse>(path).finally(() => {
    inFlightRouteShardPromises.delete(key);
  });
  inFlightRouteShardPromises.set(key, promise);
  return promise;
}

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`${DATA_BASE_URL}/${path.replace(/^\//, "")}`);
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return response.json();
}
