import { useEffect, useMemo, useRef, useState } from "react";
import DeckGL from "@deck.gl/react";
import { PathLayer, ScatterplotLayer } from "@deck.gl/layers";
import { Map } from "react-map-gl/maplibre";
import { Eye, EyeOff, Pause, Play, RotateCcw } from "lucide-react";
import { ActivityScrubber } from "./ActivityScrubber";
import { fetchDateRange, fetchPlayback } from "./api";
import { curvedPath, formatClock, slicePathWindow } from "./paths";
import type { PlaybackResponse, PlaybackStation, PlaybackTrip } from "./types";

const LONDON_VIEW = {
  longitude: -0.12,
  latitude: 51.51,
  zoom: 11.4,
  pitch: 36,
  bearing: -8
};

const MAP_STYLE = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";
const SPEEDS = [1, 10, 60, 180, 600];
const ARRIVAL_FLASH_SECONDS = 90;

type ActivePath = {
  id: string;
  path: [number, number][];
  progress: number;
  routed: boolean;
};

type ArrivalFlash = {
  id: string;
  coord: [number, number];
  age: number;
};

type HoverInfo = {
  x: number;
  y: number;
  station: PlaybackStation;
};

export function App() {
  const [selectedDate, setSelectedDate] = useState("2025-06-18");
  const [minDate, setMinDate] = useState("");
  const [maxDate, setMaxDate] = useState("");
  const [playback, setPlayback] = useState<PlaybackResponse | null>(null);
  const [currentTime, setCurrentTime] = useState(7 * 3600);
  const [speed, setSpeed] = useState(180);
  const [tailLength, setTailLength] = useState(18);
  const [showUnrouted, setShowUnrouted] = useState(true);
  const [playing, setPlaying] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null);
  const frameRef = useRef<number | null>(null);
  const lastTickRef = useRef<number | null>(null);

  useEffect(() => {
    fetchDateRange()
      .then((range) => {
        setMinDate(range.minDate);
        setMaxDate(range.maxDate);
        if (!selectedDate) {
          setSelectedDate(range.maxDate);
        }
      })
      .catch((reason) => setError(String(reason)));
  }, []);

  useEffect(() => {
    setLoading(true);
    setError(null);
    setPlaying(false);
    fetchPlayback(selectedDate)
      .then((data) => {
        setPlayback(data);
        const firstTrip = data.trips[0];
        setCurrentTime(firstTrip ? Math.max(0, firstTrip.start - 600) : 7 * 3600);
      })
      .catch((reason) => setError(String(reason)))
      .finally(() => setLoading(false));
  }, [selectedDate]);

  useEffect(() => {
    if (!playing) {
      lastTickRef.current = null;
      if (frameRef.current !== null) {
        cancelAnimationFrame(frameRef.current);
      }
      return;
    }

    const tick = (timestamp: number) => {
      if (lastTickRef.current !== null) {
        const elapsed = (timestamp - lastTickRef.current) / 1000;
        setCurrentTime((value) => Math.min(86399, value + elapsed * speed));
      }
      lastTickRef.current = timestamp;
      frameRef.current = requestAnimationFrame(tick);
    };

    frameRef.current = requestAnimationFrame(tick);
    return () => {
      if (frameRef.current !== null) {
        cancelAnimationFrame(frameRef.current);
      }
    };
  }, [playing, speed]);

  const activePaths = useMemo(() => {
    if (!playback) return [];
    return playback.trips
      .filter((trip) => trip.start <= currentTime && trip.end >= currentTime)
      .filter((trip) => showUnrouted || trip.path)
      .map((trip): ActivePath => {
        const progress = (currentTime - trip.start) / Math.max(1, trip.end - trip.start);
        const routed = trip.path !== null;
        const path = trip.path ?? curvedPath(trip.fromCoord, trip.toCoord);
        return {
          id: trip.id,
          progress,
          routed,
          path: slicePathWindow(path, progress, tailLength / 100)
        };
      });
  }, [playback, currentTime, tailLength, showUnrouted]);

  const arrivalFlashes = useMemo(() => {
    if (!playback) return [];
    return playback.trips
      .filter((trip) => trip.end <= currentTime && trip.end >= currentTime - ARRIVAL_FLASH_SECONDS)
      .map(
        (trip): ArrivalFlash => ({
          id: trip.id,
          coord: trip.toCoord,
          age: currentTime - trip.end
        })
      );
  }, [playback, currentTime]);

  const layers = useMemo(
    () => [
      new ScatterplotLayer<PlaybackStation>({
        id: "stations",
        data: playback?.stations ?? [],
        getPosition: (station) => station.coord,
        getRadius: (station) => Math.max(18, Math.sqrt(station.tripCount) * 4),
        getFillColor: [32, 130, 120, 150],
        getLineColor: [234, 241, 237, 220],
        lineWidthMinPixels: 1,
        stroked: true,
        pickable: true,
        onHover: (info) => {
          setHoverInfo(
            info.object
              ? {
                  x: info.x,
                  y: info.y,
                  station: info.object as PlaybackStation
                }
              : null
          );
        },
        radiusUnits: "meters"
      }),
      new PathLayer<ActivePath>({
        id: "active-trips",
        data: activePaths,
        getPath: (item) => item.path,
        getColor: (item) =>
          item.routed
            ? [250, 188, 72, 100 + Math.round(item.progress * 140)]
            : [82, 180, 210, 55 + Math.round(item.progress * 110)],
        getWidth: (item) => (item.routed ? 3.5 : 2.2),
        widthMinPixels: 2,
        jointRounded: true,
        capRounded: true
      }),
      new ScatterplotLayer<ArrivalFlash>({
        id: "arrival-flashes",
        data: arrivalFlashes,
        getPosition: (item) => item.coord,
        getRadius: (item) => 34 + (item.age / ARRIVAL_FLASH_SECONDS) * 130,
        getFillColor: (item) => [250, 188, 72, Math.max(0, 90 - item.age)],
        getLineColor: (item) => [255, 238, 184, Math.max(0, 220 - item.age * 2)],
        lineWidthMinPixels: 2,
        radiusUnits: "meters",
        stroked: true,
        filled: true
      })
    ],
    [activePaths, arrivalFlashes, playback]
  );

  const activeTrips = activePaths.length;
  const activeRoutedTrips = activePaths.filter((path) => path.routed).length;
  const activeUnroutedTrips = activeTrips - activeRoutedTrips;
  const summary = playback?.summary;

  return (
    <main className="app-shell">
      <section className="toolbar" aria-label="Playback controls">
        <div className="brand-block">
          <h1>Cycle Hire Playback</h1>
          <span>{formatClock(currentTime)}</span>
        </div>

        <label className="field">
          <span>Date</span>
          <input
            type="date"
            min={minDate}
            max={maxDate}
            value={selectedDate}
            onChange={(event) => setSelectedDate(event.target.value)}
          />
        </label>

        <button
          className="icon-button primary"
          type="button"
          aria-label={playing ? "Pause playback" : "Play playback"}
          title={playing ? "Pause playback" : "Play playback"}
          onClick={() => setPlaying((value) => !value)}
        >
          {playing ? <Pause size={18} /> : <Play size={18} />}
        </button>

        <button
          className="icon-button"
          type="button"
          aria-label="Reset time"
          title="Reset time"
          onClick={() => setCurrentTime(0)}
        >
          <RotateCcw size={18} />
        </button>

        <label className="field speed-field">
          <span>Speed</span>
          <select value={speed} onChange={(event) => setSpeed(Number(event.target.value))}>
            {SPEEDS.map((value) => (
              <option key={value} value={value}>
                {value}x
              </option>
            ))}
          </select>
        </label>

        <label className="scrubber">
          <span>Time</span>
          <ActivityScrubber
            activity={playback?.activity ?? []}
            currentTime={currentTime}
            onChange={setCurrentTime}
          />
        </label>

        <label className="scrubber tail-scrubber">
          <span>Tail {tailLength}%</span>
          <input
            type="range"
            min={4}
            max={40}
            step={1}
            value={tailLength}
            onChange={(event) => setTailLength(Number(event.target.value))}
          />
        </label>

        <button
          className={`toggle-button ${showUnrouted ? "enabled" : ""}`}
          type="button"
          aria-pressed={showUnrouted}
          aria-label={showUnrouted ? "Hide unrouted journeys" : "Show unrouted journeys"}
          title={showUnrouted ? "Hide unrouted journeys" : "Show unrouted journeys"}
          onClick={() => setShowUnrouted((value) => !value)}
        >
          {showUnrouted ? <Eye size={17} /> : <EyeOff size={17} />}
          <span>Unrouted</span>
        </button>
      </section>

      <section className="map-stage" aria-label="Cycle hire map playback">
        <DeckGL initialViewState={LONDON_VIEW} controller layers={layers}>
          <Map mapStyle={MAP_STYLE} reuseMaps />
        </DeckGL>
        <aside className="stats-panel">
          <Metric label="Journeys" value={summary?.matchedTrips ?? 0} />
          <Metric label="Active" value={activeTrips} />
          <Metric label="Active routed" value={activeRoutedTrips} />
          <Metric label="Active fallback" value={activeUnroutedTrips} />
          <Metric label="Routed total" value={summary?.routedTrips ?? 0} />
          <Metric label="Stations" value={summary?.stationCount ?? 0} />
          <Metric label="Unmatched" value={summary?.unmatchedTrips ?? 0} />
        </aside>
        {(loading || error) && (
          <div className="status-panel">
            {loading && <span>Loading {selectedDate}</span>}
            {error && <span>{error}</span>}
          </div>
        )}
        {hoverInfo && (
          <div
            className="map-tooltip"
            style={{
              transform: `translate(${hoverInfo.x + 12}px, ${hoverInfo.y + 12}px)`
            }}
          >
            <strong>{hoverInfo.station.name}</strong>
            <span>{hoverInfo.station.tripCount.toLocaleString()} journeys</span>
          </div>
        )}
      </section>
    </main>
  );
}

function Metric({ label, value }: { label: string; value: number }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>{value.toLocaleString()}</strong>
    </div>
  );
}
