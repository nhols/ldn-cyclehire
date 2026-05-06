import { useEffect, useMemo, useRef, useState } from "react";
import DeckGL from "@deck.gl/react";
import { PathLayer, ScatterplotLayer } from "@deck.gl/layers";
import { Map } from "react-map-gl/maplibre";
import { Monitor, Moon, Settings2, Pause, Play, RotateCcw, Sun } from "lucide-react";
import { ActivityScrubber } from "./ActivityScrubber";
import { fetchDateRange, fetchPlayback } from "./api";
import { curvedPath, formatClock, slicePathWindow } from "./paths";
import type { PlaybackResponse, PlaybackStation, PlaybackTrip } from "./types";

const LONDON_VIEW = {
  longitude: -0.11,
  latitude: 51.495,
  zoom: 11.68,
  pitch: 0,
  bearing: -8
};

const MAP_STYLES = {
  dark: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
  light: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
} satisfies Record<ResolvedTheme, string>;
const SPEEDS = [1, 10, 60, 180, 600, 1200, 2400];
const ARRIVAL_FLASH_SECONDS = 90;
const DEFAULT_ROUTED_COLOR = "#467cfb";
const DEFAULT_UNROUTED_COLOR = "#ed074c";
const DEFAULT_ORIGIN_FLASH_COLOR = "#52b4d2";
const DEFAULT_DESTINATION_FLASH_COLOR = "#fabc48";
const THEME_STORAGE_KEY = "cyclehire-theme";

type ThemePreference = "system" | "dark" | "light";
type ResolvedTheme = "dark" | "light";

type ActivePath = {
  id: string;
  path: [number, number][];
  progress: number;
  routed: boolean;
};

type StationFlash = {
  id: string;
  coord: [number, number];
  age: number;
  kind: "origin" | "destination";
};

type HoverInfo = {
  x: number;
  y: number;
  station: PlaybackStation;
};

export function App() {
  const [themePreference, setThemePreference] = useState<ThemePreference>(readStoredTheme);
  const [systemTheme, setSystemTheme] = useState<ResolvedTheme>(getSystemTheme);
  const [selectedDate, setSelectedDate] = useState("2025-06-18");
  const [minDate, setMinDate] = useState("");
  const [maxDate, setMaxDate] = useState("");
  const [playback, setPlayback] = useState<PlaybackResponse | null>(null);
  const [currentTime, setCurrentTime] = useState(7 * 3600);
  const [speed, setSpeed] = useState(180);
  const [tailLength, setTailLength] = useState(18);
  const [showUnrouted, setShowUnrouted] = useState(true);
  const [showOriginFlashes, setShowOriginFlashes] = useState(true);
  const [showDestinationFlashes, setShowDestinationFlashes] = useState(true);
  const [routedColor, setRoutedColor] = useState(DEFAULT_ROUTED_COLOR);
  const [unroutedColor, setUnroutedColor] = useState(DEFAULT_UNROUTED_COLOR);
  const [originFlashColor, setOriginFlashColor] = useState(DEFAULT_ORIGIN_FLASH_COLOR);
  const [destinationFlashColor, setDestinationFlashColor] = useState(DEFAULT_DESTINATION_FLASH_COLOR);
  const [traceMenuOpen, setTraceMenuOpen] = useState(false);
  const [playing, setPlaying] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null);
  const frameRef = useRef<number | null>(null);
  const lastTickRef = useRef<number | null>(null);
  const resolvedTheme = themePreference === "system" ? systemTheme : themePreference;

  useEffect(() => {
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const updateSystemTheme = () => setSystemTheme(media.matches ? "dark" : "light");

    updateSystemTheme();
    media.addEventListener("change", updateSystemTheme);
    return () => media.removeEventListener("change", updateSystemTheme);
  }, []);

  useEffect(() => {
    document.documentElement.dataset.theme = resolvedTheme;
    document.documentElement.style.colorScheme = resolvedTheme;
  }, [resolvedTheme]);

  useEffect(() => {
    window.localStorage.setItem(THEME_STORAGE_KEY, themePreference);
  }, [themePreference]);

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

  const routedDistances = useMemo(() => {
    const distances = new globalThis.Map<string, number>();
    if (!playback) return distances;
    for (const trip of playback.trips) {
      if (trip.path) {
        distances.set(trip.id, pathDistanceMetres(trip.path));
      }
    }
    return distances;
  }, [playback]);

  const stationFlashes = useMemo(() => {
    if (!playback) return [];
    const flashes: StationFlash[] = [];
    for (const trip of playback.trips) {
      if (showOriginFlashes && trip.start <= currentTime && trip.start >= currentTime - ARRIVAL_FLASH_SECONDS) {
        flashes.push({
          id: `${trip.id}-origin`,
          coord: trip.fromCoord,
          age: currentTime - trip.start,
          kind: "origin"
        });
      }

      if (showDestinationFlashes && trip.end <= currentTime && trip.end >= currentTime - ARRIVAL_FLASH_SECONDS) {
        flashes.push({
          id: `${trip.id}-destination`,
          coord: trip.toCoord,
          age: currentTime - trip.end,
          kind: "destination"
        });
      }
    }
    return flashes;
  }, [playback, currentTime, showOriginFlashes, showDestinationFlashes]);

  const runningTotals = useMemo(() => {
    if (!playback) {
      return { journeys: 0, routedKilometres: 0 };
    }

    let journeys = 0;
    let routedMetres = 0;
    for (const trip of playback.trips) {
      if (trip.start <= currentTime) {
        journeys += 1;
      }

      const routeDistance = routedDistances.get(trip.id);
      if (routeDistance !== undefined && trip.start <= currentTime) {
        const progress = Math.min(1, Math.max(0, (currentTime - trip.start) / Math.max(1, trip.end - trip.start)));
        routedMetres += routeDistance * progress;
      }
    }

    return {
      journeys,
      routedKilometres: routedMetres / 1000
    };
  }, [playback, currentTime, routedDistances]);

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
            ? withAlpha(hexToRgb(routedColor), 100 + Math.round(item.progress * 140))
            : withAlpha(hexToRgb(unroutedColor), 55 + Math.round(item.progress * 110)),
        getWidth: (item) => (item.routed ? 3.5 : 2.2),
        widthMinPixels: 2,
        jointRounded: true,
        capRounded: true
      }),
      new ScatterplotLayer<StationFlash>({
        id: "station-flashes",
        data: stationFlashes,
        getPosition: (item) => item.coord,
        getRadius: (item) => 34 + (item.age / ARRIVAL_FLASH_SECONDS) * 130,
        getFillColor: (item) =>
          withAlpha(
            hexToRgb(item.kind === "origin" ? originFlashColor : destinationFlashColor),
            Math.max(0, 90 - item.age)
          ),
        getLineColor: (item) =>
          withAlpha(
            hexToRgb(item.kind === "origin" ? originFlashColor : destinationFlashColor),
            Math.max(0, 220 - item.age * 2)
          ),
        lineWidthMinPixels: 2,
        radiusUnits: "meters",
        stroked: true,
        filled: true
      })
    ],
    [activePaths, stationFlashes, playback, routedColor, unroutedColor, originFlashColor, destinationFlashColor]
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

        <div className="trace-settings">
          <button
            className={`icon-button ${traceMenuOpen ? "active" : ""}`}
            type="button"
            aria-expanded={traceMenuOpen}
            aria-label="Trace settings"
            title="Trace settings"
            onClick={() => setTraceMenuOpen((value) => !value)}
          >
            <Settings2 size={18} />
          </button>
          {traceMenuOpen && (
            <div className="trace-popover" role="dialog" aria-label="Trace settings">
              <div className="popover-group">
                <span>Theme</span>
                <div className="theme-toggle" role="group" aria-label="Theme preference">
                  <button
                    className={themePreference === "light" ? "active" : ""}
                    type="button"
                    aria-label="Use light theme"
                    title="Light"
                    onClick={() => setThemePreference("light")}
                  >
                    <Sun size={16} />
                  </button>
                  <button
                    className={themePreference === "system" ? "active" : ""}
                    type="button"
                    aria-label="Use system theme"
                    title={`System (${resolvedTheme})`}
                    onClick={() => setThemePreference("system")}
                  >
                    <Monitor size={16} />
                  </button>
                  <button
                    className={themePreference === "dark" ? "active" : ""}
                    type="button"
                    aria-label="Use dark theme"
                    title="Dark"
                    onClick={() => setThemePreference("dark")}
                  >
                    <Moon size={16} />
                  </button>
                </div>
              </div>

              <label className="popover-slider">
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

              <label className="color-field">
                <span>Routed</span>
                <input
                  type="color"
                  value={routedColor}
                  onChange={(event) => setRoutedColor(event.target.value)}
                />
              </label>

              <label className="color-field">
                <span>Unrouted</span>
                <input
                  type="color"
                  value={unroutedColor}
                  onChange={(event) => setUnroutedColor(event.target.value)}
                />
              </label>

              <label className="checkbox-field">
                <input
                  type="checkbox"
                  checked={showUnrouted}
                  onChange={(event) => setShowUnrouted(event.target.checked)}
                />
                <span>Show unrouted</span>
              </label>

              <label className="color-field">
                <span>Origin flash</span>
                <input
                  type="color"
                  value={originFlashColor}
                  onChange={(event) => setOriginFlashColor(event.target.value)}
                />
              </label>

              <label className="checkbox-field">
                <input
                  type="checkbox"
                  checked={showOriginFlashes}
                  onChange={(event) => setShowOriginFlashes(event.target.checked)}
                />
                <span>Show origin flash</span>
              </label>

              <label className="color-field">
                <span>Destination flash</span>
                <input
                  type="color"
                  value={destinationFlashColor}
                  onChange={(event) => setDestinationFlashColor(event.target.value)}
                />
              </label>

              <label className="checkbox-field">
                <input
                  type="checkbox"
                  checked={showDestinationFlashes}
                  onChange={(event) => setShowDestinationFlashes(event.target.checked)}
                />
                <span>Show destination flash</span>
              </label>
            </div>
          )}
        </div>
      </section>

      <section className="map-stage" aria-label="Cycle hire map playback">
        <DeckGL initialViewState={LONDON_VIEW} controller layers={layers}>
          <Map mapStyle={MAP_STYLES[resolvedTheme]} reuseMaps />
        </DeckGL>
        <aside className="stats-panel">
          <Metric label="Journeys so far" value={runningTotals.journeys} />
          <Metric label="Routed km so far" value={runningTotals.routedKilometres} decimals={1} />
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

function Metric({ label, value, decimals = 0 }: { label: string; value: number; decimals?: number }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>
        {value.toLocaleString(undefined, {
          maximumFractionDigits: decimals,
          minimumFractionDigits: decimals
        })}
      </strong>
    </div>
  );
}

function hexToRgb(hex: string): [number, number, number] {
  const value = hex.replace("#", "");
  return [
    Number.parseInt(value.slice(0, 2), 16),
    Number.parseInt(value.slice(2, 4), 16),
    Number.parseInt(value.slice(4, 6), 16)
  ];
}

function withAlpha(rgb: [number, number, number], alpha: number): [number, number, number, number] {
  return [rgb[0], rgb[1], rgb[2], alpha];
}

function pathDistanceMetres(path: [number, number][]): number {
  let total = 0;
  for (let index = 1; index < path.length; index += 1) {
    total += distanceMetres(path[index - 1], path[index]);
  }
  return total;
}

function distanceMetres(from: [number, number], to: [number, number]): number {
  const earthRadiusMetres = 6_371_000;
  const fromLat = toRadians(from[1]);
  const toLat = toRadians(to[1]);
  const latDelta = toRadians(to[1] - from[1]);
  const lonDelta = toRadians(to[0] - from[0]);
  const haversine =
    Math.sin(latDelta / 2) ** 2 + Math.cos(fromLat) * Math.cos(toLat) * Math.sin(lonDelta / 2) ** 2;
  return earthRadiusMetres * 2 * Math.atan2(Math.sqrt(haversine), Math.sqrt(1 - haversine));
}

function toRadians(value: number): number {
  return (value * Math.PI) / 180;
}

function readStoredTheme(): ThemePreference {
  const value = window.localStorage.getItem(THEME_STORAGE_KEY);
  return isThemePreference(value) ? value : "system";
}

function isThemePreference(value: string | null): value is ThemePreference {
  return value === "system" || value === "dark" || value === "light";
}

function getSystemTheme(): ResolvedTheme {
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}
