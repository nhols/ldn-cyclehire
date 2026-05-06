import type {
  DateRangeResponse,
  PlaybackResponse,
  StaticManifest,
  StaticPlaybackResponse,
  StaticRoutesResponse
} from "./types";

const DATA_BASE_URL = (import.meta.env.VITE_STATIC_DATA_BASE_URL ?? "/data").replace(/\/$/, "");

let manifestPromise: Promise<StaticManifest> | null = null;
const routeShardPromises = new Map<string, Promise<StaticRoutesResponse>>();

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

export async function fetchRouteShard(shardId: string): Promise<StaticRoutesResponse> {
  const manifest = await fetchManifest();
  const routeShards = new Map((manifest.files.routes.shards ?? []).map((shard) => [shard.id, shard.path]));
  const path = routeShards.get(shardId) ?? manifest.files.routes.shardTemplate?.replace("{shard}", shardId);
  if (!path) {
    throw new Error(`No route shard path for ${shardId}`);
  }
  return fetchRouteShardPath(shardId, path);
}

function fetchManifest(): Promise<StaticManifest> {
  manifestPromise ??= fetchJson<StaticManifest>("manifest.json");
  return manifestPromise;
}

function fetchRouteShardPath(shardId: string, path: string): Promise<StaticRoutesResponse> {
  const key = `${shardId}:${path}`;
  const cached = routeShardPromises.get(key);
  if (cached) {
    return cached;
  }
  const promise = fetchJson<StaticRoutesResponse>(path);
  routeShardPromises.set(key, promise);
  return promise;
}

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`${DATA_BASE_URL}/${path.replace(/^\//, "")}`);
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return response.json();
}
