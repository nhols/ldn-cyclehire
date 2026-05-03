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
};

export type PlaybackSummary = {
  totalTrips: number;
  matchedTrips: number;
  unmatchedTrips: number;
  stationCount: number;
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
