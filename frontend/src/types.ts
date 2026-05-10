export type Coord = [number, number];
export type FlatPath = Float32Array;

export type PlaybackStation = {
  id: string;
  stationId: string | null;
  name: string;
  bikepointId: string | null;
  coord: Coord;
  tripCount: number;
  departureCount: number;
  arrivalCount: number;
  matchMethod: string | null;
};

export type PlaybackTrip = {
  id: string;
  bikeId: string;
  bikeModel: string | null;
  start: number;
  end: number;
  durationSeconds: number;
  fromStationId: string | null;
  fromStationName: string;
  toStationId: string | null;
  toStationName: string;
  fromCoord: Coord;
  toCoord: Coord;
  routeKey: string | null;
  routeShard: string | null;
  routeDistanceMeters: number | null;
  routeReversed: boolean;
  path: Coord[] | null;
};

export type StaticPlaybackTrip = Omit<PlaybackTrip, "path">;

export type PlaybackSummary = {
  totalTrips: number;
  matchedTrips: number;
  unmatchedTrips: number;
  stationCount: number;
  routedTrips?: number;
};

export type ActivityPoint = {
  time: number;
  activeTrips: number;
};

export type PlaybackResponse = {
  date: string;
  stations: PlaybackStation[];
  trips: PlaybackTrip[];
  activity: ActivityPoint[];
  summary: PlaybackSummary;
};

export type DateRangeResponse = {
  minDate: string;
  maxDate: string;
  tripCount: number;
};

export type DaySummary = {
  date: string;
  path: string;
  gzipPath?: string;
  trips: number;
  routedTrips: number;
  bytes?: number;
  gzipBytes: number;
};

export type StaticPlaybackResponse = Omit<PlaybackResponse, "trips"> & {
  trips: StaticPlaybackTrip[];
};

export type StaticManifest = {
  version: number;
  generatedAt: string;
  dateRange: DateRangeResponse;
  files: {
    routes: {
      path?: string;
      gzipPath?: string;
      provider?: string;
      version?: string;
      encoding?: string;
      shardStrategy?: string;
      shardTemplate?: string;
      gzipShardTemplate?: string;
      shardTargetGzipBytes?: number;
      estimatedCompressionRatio?: number;
      shardCount?: number;
      bytes?: number;
      gzipBytes: number;
      routeCount: number;
      shards?: Array<{
        id: string;
        path: string;
        gzipPath?: string;
        routeCount: number;
        bytes?: number;
        gzipBytes: number;
      }>;
    };
    bikepoints: {
      path: string;
      gzipPath?: string;
      bytes?: number;
      gzipBytes: number;
      stationCount: number;
    };
  };
  days: DaySummary[];
};

export type StaticRoutesResponse = {
  version: number;
  encoding: string;
  routes: Record<string, Coord[]>;
};
