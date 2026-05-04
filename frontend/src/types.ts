export type Coord = [number, number];

export type PlaybackStation = {
  id: string;
  stationId: string | null;
  name: string;
  bikepointId: string | null;
  coord: Coord;
  tripCount: number;
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
  path: Coord[] | null;
};

export type StaticPlaybackTrip = Omit<PlaybackTrip, "path"> & {
  routeKey: string | null;
  routeReversed: boolean;
};

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

export type StaticPlaybackResponse = Omit<PlaybackResponse, "trips"> & {
  trips: StaticPlaybackTrip[];
};

export type StaticManifest = {
  version: number;
  generatedAt: string;
  dateRange: DateRangeResponse;
  files: {
    routes: {
      path: string;
      gzipPath?: string;
      bytes?: number;
      gzipBytes: number;
      routeCount: number;
    };
    bikepoints: {
      path: string;
      gzipPath?: string;
      bytes?: number;
      gzipBytes: number;
      stationCount: number;
    };
  };
  days: Array<{
    date: string;
    path: string;
    gzipPath?: string;
    trips: number;
    routedTrips: number;
    bytes?: number;
    gzipBytes: number;
  }>;
};

export type StaticRoutesResponse = {
  version: number;
  encoding: string;
  routes: Record<string, Coord[]>;
};
