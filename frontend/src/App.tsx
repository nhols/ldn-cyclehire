import { useEffect, useMemo, useRef, useState } from "react";
import DeckGL from "@deck.gl/react";
import { PathLayer, ScatterplotLayer } from "@deck.gl/layers";
import { Map } from "react-map-gl/maplibre";
import { CalendarDays, Filter, Github, Monitor, Moon, Settings2, Pause, Play, RotateCcw, Sun, X } from "lucide-react";
import { ActivityScrubber } from "./ActivityScrubber";
import { DateHistogram } from "./DateHistogram";
import { fetchDateRange, fetchDaySummaries, fetchPlayback, fetchRouteShard } from "./api";
import { curvedPath, formatClock, slicePathWindow } from "./paths";
import type { Coord, DaySummary, PlaybackResponse, PlaybackStation, PlaybackTrip } from "./types";

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
const STATION_BASE_RADIUS_METERS = 44;
const STATION_MIN_RADIUS_METERS = 14;
const STATION_MAX_RADIUS_METERS = 130;
const STATION_BALANCE_RADIUS_SCALE = 4;
const DEFAULT_ROUTED_COLOR = "#467cfb";
const DEFAULT_UNROUTED_COLOR = "#ed074c";
const DEFAULT_ORIGIN_FLASH_COLOR = "#52b4d2";
const DEFAULT_DESTINATION_FLASH_COLOR = "#fabc48";
const SELECTED_STATION_FILL: [number, number, number, number] = [237, 7, 76, 215];
const SELECTED_STATION_LINE: [number, number, number, number] = [255, 214, 226, 245];
const DEFAULT_STATION_FILL: [number, number, number, number] = [32, 130, 120, 150];
const DEFAULT_STATION_LINE: [number, number, number, number] = [234, 241, 237, 220];
const THEME_STORAGE_KEY = "cyclehire-theme";
const DESKTOP_ROUTE_SHARD_FETCH_CONCURRENCY = 6;
const MOBILE_ROUTE_SHARD_FETCH_CONCURRENCY = 1;

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
  kind: "departure" | "arrival";
};

type HoverInfo = {
  x: number;
  y: number;
  stationId: string;
};

type StationBalance = {
  arrivals: number;
  departures: number;
};

type BalancedStation = PlaybackStation & {
  liveArrivals: number;
  liveDepartures: number;
  liveNet: number;
  liveRadius: number;
};

export function App() {
  const [themePreference, setThemePreference] = useState<ThemePreference>(readStoredTheme);
  const [systemTheme, setSystemTheme] = useState<ResolvedTheme>(getSystemTheme);
  const [selectedDate, setSelectedDate] = useState("2025-06-18");
  const [minDate, setMinDate] = useState("");
  const [maxDate, setMaxDate] = useState("");
  const [daySummaries, setDaySummaries] = useState<DaySummary[]>([]);
  const [playback, setPlayback] = useState<PlaybackResponse | null>(null);
  const [routeCache, setRouteCache] = useState<globalThis.Map<string, Coord[]>>(() => new globalThis.Map());
  const [requiredRouteShards, setRequiredRouteShards] = useState<string[]>([]);
  const [loadedRouteShardCount, setLoadedRouteShardCount] = useState(0);
  const [currentTime, setCurrentTime] = useState(7 * 3600);
  const [speed, setSpeed] = useState(180);
  const [tailLength, setTailLength] = useState(18);
  const [showUnrouted, setShowUnrouted] = useState(false);
  const [showOriginFlashes, setShowOriginFlashes] = useState(true);
  const [showDestinationFlashes, setShowDestinationFlashes] = useState(true);
  const [routedColor, setRoutedColor] = useState(DEFAULT_ROUTED_COLOR);
  const [unroutedColor, setUnroutedColor] = useState(DEFAULT_UNROUTED_COLOR);
  const [originFlashColor, setOriginFlashColor] = useState(DEFAULT_ORIGIN_FLASH_COLOR);
  const [destinationFlashColor, setDestinationFlashColor] = useState(DEFAULT_DESTINATION_FLASH_COLOR);
  const [dateExplorerOpen, setDateExplorerOpen] = useState(false);
  const [traceMenuOpen, setTraceMenuOpen] = useState(false);
  const [stationFilterMode, setStationFilterMode] = useState(false);
  const [filteredStationIds, setFilteredStationIds] = useState<string[]>([]);
  const [playing, setPlaying] = useState(false);
  const [pendingPlay, setPendingPlay] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null);
  const frameRef = useRef<number | null>(null);
  const lastTickRef = useRef<number | null>(null);
  const resolvedTheme = themePreference === "system" ? systemTheme : themePreference;
  const routeShardFetchConcurrency = isLowMemoryRouteMode()
    ? MOBILE_ROUTE_SHARD_FETCH_CONCURRENCY
    : DESKTOP_ROUTE_SHARD_FETCH_CONCURRENCY;

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
    Promise.all([fetchDateRange(), fetchDaySummaries()])
      .then(([range, days]) => {
        setDaySummaries(days);
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
        const routeShardIds = routeShardIdsForTrips(data.trips);
        setPlayback(data);
        setRouteCache(new globalThis.Map());
        setRequiredRouteShards(routeShardIds);
        setLoadedRouteShardCount(0);
        setPendingPlay(false);
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

  const routesReady =
    playback !== null && requiredRouteShards.length === loadedRouteShardCount;
  const routeLoading =
    playback !== null && requiredRouteShards.length > loadedRouteShardCount;

  useEffect(() => {
    if (pendingPlay && routesReady) {
      setPlaying(true);
      setPendingPlay(false);
    }
  }, [pendingPlay, routesReady]);

  useEffect(() => {
    if (!playback || !requiredRouteShards.length) return;

    const routeShardIds = requiredRouteShards;
    const routeKeysForDay = routeKeysForTrips(playback.trips);
    let cancelled = false;

    async function loadRouteShards() {
      for (let index = 0; index < routeShardIds.length; index += routeShardFetchConcurrency) {
        const batch = routeShardIds.slice(index, index + routeShardFetchConcurrency);
        const payloads = await Promise.all(batch.map((shardId) => fetchRouteShard(shardId)));
        if (cancelled) return;
        setRouteCache((value) => {
          const next = new globalThis.Map(value);
          for (const payload of payloads) {
            for (const [routeKey, coordinates] of Object.entries(payload.routes)) {
              if (routeKeysForDay.has(routeKey)) {
                next.set(routeKey, coordinates);
              }
            }
          }
          return next;
        });
        setLoadedRouteShardCount((value) => value + batch.length);
      }
    }

    loadRouteShards().catch((reason) => setError(String(reason)));

    return () => {
      cancelled = true;
    };
  }, [playback, requiredRouteShards, routeShardFetchConcurrency]);

  const displayTrips = useMemo(() => {
    if (!playback) return [];
    if (filteredStationIds.length === 0) return playback.trips;
    const filteredStations = new Set(filteredStationIds);
    return playback.trips.filter((trip) => tripTouchesStationFilter(trip, filteredStations));
  }, [filteredStationIds, playback]);

  const activePaths = useMemo(() => {
    return displayTrips
      .filter((trip) => trip.start <= currentTime && trip.end >= currentTime)
      .filter((trip) => showUnrouted || trip.routeKey)
      .map((trip): ActivePath => {
        const progress = (currentTime - trip.start) / Math.max(1, trip.end - trip.start);
        const routed = trip.routeKey !== null;
        const routePath = routePathForTrip(trip, routeCache);
        const path = routePath ?? curvedPath(trip.fromCoord, trip.toCoord);
        return {
          id: trip.id,
          progress,
          routed,
          path: slicePathWindow(path, progress, tailLength / 100)
        };
      });
  }, [displayTrips, currentTime, routeCache, tailLength, showUnrouted]);

  const routedDistances = useMemo(() => {
    const distances = new globalThis.Map<string, number>();
    if (!playback) return distances;
    for (const trip of playback.trips) {
      if (trip.routeDistanceMeters !== null) {
        distances.set(trip.id, trip.routeDistanceMeters);
      }
    }
    return distances;
  }, [playback]);

  const stationFlashes = useMemo(() => {
    const flashes: StationFlash[] = [];
    for (const trip of displayTrips) {
      if (showOriginFlashes && trip.start <= currentTime && trip.start >= currentTime - ARRIVAL_FLASH_SECONDS) {
        flashes.push({
          id: `${trip.id}-departure`,
          coord: trip.fromCoord,
          age: currentTime - trip.start,
          kind: "departure"
        });
      }

      if (showDestinationFlashes && trip.end <= currentTime && trip.end >= currentTime - ARRIVAL_FLASH_SECONDS) {
        flashes.push({
          id: `${trip.id}-arrival`,
          coord: trip.toCoord,
          age: currentTime - trip.end,
          kind: "arrival"
        });
      }
    }
    return flashes;
  }, [displayTrips, currentTime, showOriginFlashes, showDestinationFlashes]);

  const runningTotals = useMemo(() => {
    if (!playback) {
      return { journeys: 0, routedJourneys: 0, unroutedJourneys: 0, routedKilometres: 0 };
    }

    let journeys = 0;
    let routedJourneys = 0;
    let routedMetres = 0;
    for (const trip of playback.trips) {
      if (trip.start <= currentTime) {
        journeys += 1;
        if (trip.routeKey !== null) {
          routedJourneys += 1;
        }
      }

      const routeDistance = routedDistances.get(trip.id);
      if (routeDistance !== undefined && trip.start <= currentTime) {
        const progress = Math.min(1, Math.max(0, (currentTime - trip.start) / Math.max(1, trip.end - trip.start)));
        routedMetres += routeDistance * progress;
      }
    }

    return {
      journeys,
      routedJourneys,
      unroutedJourneys: journeys - routedJourneys,
      routedKilometres: routedMetres / 1000
    };
  }, [playback, currentTime, routedDistances]);

  const stationBalance = useMemo(() => {
    const balances = new globalThis.Map<string, StationBalance>();
    if (!playback) return balances;

    for (const trip of playback.trips) {
      if (trip.start <= currentTime) {
        const departureKey = stationKey(trip.fromStationId, trip.fromStationName);
        const balance = balances.get(departureKey) ?? { arrivals: 0, departures: 0 };
        balances.set(departureKey, {
          ...balance,
          departures: balance.departures + 1
        });
      }

      if (trip.end <= currentTime) {
        const arrivalKey = stationKey(trip.toStationId, trip.toStationName);
        const balance = balances.get(arrivalKey) ?? { arrivals: 0, departures: 0 };
        balances.set(arrivalKey, {
          ...balance,
          arrivals: balance.arrivals + 1
        });
      }
    }

    return balances;
  }, [playback, currentTime]);

  const stationsWithBalance = useMemo((): BalancedStation[] => {
    if (!playback) return [];
    return playback.stations.map((station) => {
      const balance = stationBalance.get(station.id);
      const liveArrivals = balance?.arrivals ?? 0;
      const liveDepartures = balance?.departures ?? 0;
      const liveNet = liveArrivals - liveDepartures;
      return {
        ...station,
        liveArrivals,
        liveDepartures,
        liveNet,
        liveRadius: stationRadiusForBalance(liveNet)
      };
    });
  }, [playback, stationBalance]);

  const stationById = useMemo(() => {
    return new globalThis.Map(stationsWithBalance.map((station) => [station.id, station]));
  }, [stationsWithBalance]);

  const selectedStationSet = useMemo(() => new Set(filteredStationIds), [filteredStationIds]);
  const layers = useMemo(
    () => [
      new ScatterplotLayer<BalancedStation>({
        id: "stations",
        data: stationsWithBalance,
        getPosition: (station) => station.coord,
        getRadius: (station) => station.liveRadius,
        getFillColor: (station) => selectedStationSet.has(station.id) ? SELECTED_STATION_FILL : DEFAULT_STATION_FILL,
        getLineColor: (station) => selectedStationSet.has(station.id) ? SELECTED_STATION_LINE : DEFAULT_STATION_LINE,
        updateTriggers: {
          getFillColor: [filteredStationIds],
          getLineColor: [filteredStationIds],
          getLineWidth: [filteredStationIds]
        },
        lineWidthMinPixels: 1,
        getLineWidth: (station) => selectedStationSet.has(station.id) ? 5 : 1,
        stroked: true,
        pickable: true,
        onHover: (info) => {
          setHoverInfo(
            info.object
              ? {
                  x: info.x,
                  y: info.y,
                  stationId: (info.object as BalancedStation).id
                }
              : null
          );
        },
        onClick: (info) => {
          if (!info.object) return;
          const stationId = (info.object as BalancedStation).id;
          if (!stationFilterMode) return;
          setFilteredStationIds((value) =>
            value.includes(stationId)
              ? value.filter((selectedId) => selectedId !== stationId)
              : [...value, stationId]
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
            hexToRgb(item.kind === "departure" ? originFlashColor : destinationFlashColor),
            Math.max(0, 90 - item.age)
          ),
        getLineColor: (item) =>
          withAlpha(
            hexToRgb(item.kind === "departure" ? originFlashColor : destinationFlashColor),
            Math.max(0, 220 - item.age * 2)
          ),
        lineWidthMinPixels: 2,
        radiusUnits: "meters",
        stroked: true,
        filled: true
      })
    ],
    [
      activePaths,
      selectedStationSet,
      stationFlashes,
      stationsWithBalance,
      routedColor,
      unroutedColor,
      originFlashColor,
      destinationFlashColor,
      stationFilterMode
    ]
  );

  const activeTrips = activePaths.length;
  const activeRoutedTrips = activePaths.filter((path) => path.routed).length;
  const activeUnroutedTrips = activeTrips - activeRoutedTrips;
  const summary = playback?.summary;
  const totalRoutedTrips = summary?.routedTrips ?? 0;
  const totalTrips = summary?.matchedTrips ?? 0;
  const totalUnroutedTrips = Math.max(0, totalTrips - totalRoutedTrips);

  return (
    <main className="app-shell">
      <section className="toolbar" aria-label="Playback controls">
        <div className="brand-block">
          <div className="brand-copy">
            <h1>London Cycle Hire</h1>
            <p>Powered by TfL Open Data</p>
          </div>
          <span>{formatClock(currentTime)}</span>
          <a
            className="github-link"
            href="https://github.com/nhols/ldn-cyclehire"
            target="_blank"
            rel="noreferrer"
            aria-label="View source on GitHub"
            title="View source on GitHub"
          >
            <Github size={18} />
          </a>
        </div>

        <div className="date-discovery">
          <label className="field date-field">
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
            className={`icon-button date-explorer-button ${dateExplorerOpen ? "active" : ""}`}
            type="button"
            aria-label="Open date explorer"
            title="Date explorer"
            aria-expanded={dateExplorerOpen}
            onClick={() => setDateExplorerOpen((value) => !value)}
          >
            <CalendarDays size={18} />
          </button>
          {dateExplorerOpen && (
            <div className="date-explorer-popover" role="dialog" aria-label="Date explorer">
              <div className="date-explorer-header">
                <span>Date explorer</span>
                <strong>{selectedDate}</strong>
              </div>
              <DateHistogram days={daySummaries} selectedDate={selectedDate} onChange={setSelectedDate} />
            </div>
          )}
        </div>

        <button
          className="icon-button primary play-button"
          type="button"
          aria-label={playing ? "Pause playback" : routeLoading ? "Play when routes are loaded" : "Play playback"}
          title={playing ? "Pause playback" : routeLoading ? "Play when routes are loaded" : "Play playback"}
          onClick={() => {
            if (playing) {
              setPlaying(false);
              setPendingPlay(false);
            } else if (routesReady) {
              setPlaying(true);
            } else {
              setPendingPlay(true);
            }
          }}
        >
          {playing ? <Pause size={18} /> : <Play size={18} />}
        </button>

        <button
          className="icon-button reset-button"
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
          <div className="station-filter-controls">
            <button
              className={`icon-button station-filter-mode-button ${stationFilterMode ? "active" : ""}`}
              type="button"
              aria-pressed={stationFilterMode}
              aria-label="Station filter mode"
              title="Station filter mode"
              onClick={() => setStationFilterMode((value) => !value)}
            >
              <Filter size={18} />
              {filteredStationIds.length > 0 && <span>{filteredStationIds.length}</span>}
            </button>
            <button
              className="icon-button station-filter-clear-button"
              type="button"
              aria-label="Clear station filters"
              title="Clear station filters"
              disabled={filteredStationIds.length === 0}
              onClick={() => setFilteredStationIds([])}
            >
              <X size={16} />
            </button>
          </div>
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
                <span>Departure flash</span>
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
                <span>Show departure flash</span>
              </label>

              <label className="color-field">
                <span>Arrival flash</span>
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
                <span>Show arrival flash</span>
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
          <div className="metric-key" aria-label="Metric format">
            <span>Key</span>
            <strong>Metric</strong>
            <em>routed / unrouted</em>
          </div>
          <Metric
            label="Journeys"
            value={runningTotals.journeys}
            split={{
              routed: runningTotals.routedJourneys,
              unrouted: runningTotals.unroutedJourneys
            }}
          />
          <Metric
            label="Active"
            value={activeTrips}
            split={{
              routed: activeRoutedTrips,
              unrouted: activeUnroutedTrips
            }}
          />
          <Metric label="Routed km" value={runningTotals.routedKilometres} decimals={1} />
          <Metric
            className="static-metric-start"
            label="Journeys"
            value={totalTrips}
            split={{
              routed: totalRoutedTrips,
              unrouted: totalUnroutedTrips
            }}
          />
          <Metric label="Stations" value={summary?.stationCount ?? 0} />
          <Metric label="Unmatched" value={summary?.unmatchedTrips ?? 0} />
        </aside>
        {(loading || routeLoading || error) && (
          <div className="status-panel">
            {loading && <span>Loading {selectedDate}</span>}
            {routeLoading && (
              <span>
                Loading routes {loadedRouteShardCount.toLocaleString()} / {requiredRouteShards.length.toLocaleString()}
              </span>
            )}
            {error && <span>{error}</span>}
          </div>
        )}
        {hoverInfo && stationById.has(hoverInfo.stationId) && (
          <div
            className="map-tooltip"
            style={{
              transform: `translate(${hoverInfo.x + 12}px, ${hoverInfo.y + 12}px)`
            }}
          >
            {(() => {
              const station = stationById.get(hoverInfo.stationId);
              if (!station) return null;
              const arrivals = station.liveArrivals;
              const departures = station.liveDepartures;
              const net = station.liveNet;
              const totalDepartures = station.departureCount ?? 0;
              const totalArrivals = station.arrivalCount ?? 0;
              const totalNet = totalArrivals - totalDepartures;
              return (
                <>
                  <strong>{station.name}</strong>
                  <span>departures / arrivals / net</span>
                  <span>live: {formatBalanceCounts(departures, arrivals, net)}</span>
                  <span>total: {formatBalanceCounts(totalDepartures, totalArrivals, totalNet)}</span>
                </>
              );
            })()}
          </div>
        )}
      </section>
    </main>
  );
}

function Metric({
  className,
  label,
  value,
  decimals = 0,
  split
}: {
  className?: string;
  label: string;
  value: number;
  decimals?: number;
  split?: {
    routed: number;
    unrouted: number;
  };
}) {
  return (
    <div className={`metric ${className ?? ""}`}>
      <span>{label}</span>
      <strong>
        {value.toLocaleString(undefined, {
          maximumFractionDigits: decimals,
          minimumFractionDigits: decimals
        })}
      </strong>
      {split && (
        <div className="metric-split">
          <span>
            {split.routed.toLocaleString()} / {split.unrouted.toLocaleString()}
          </span>
        </div>
      )}
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

function routePathForTrip(trip: PlaybackTrip, routeCache: globalThis.Map<string, Coord[]>): Coord[] | null {
  if (!trip.routeKey) {
    return null;
  }
  const route = routeCache.get(trip.routeKey);
  if (!route) {
    return null;
  }
  return trip.routeReversed ? [...route].reverse() : route;
}

function routeShardIdsForTrips(trips: PlaybackTrip[]): string[] {
  return [
    ...new Set(
      trips
        .map((trip) => trip.routeShard)
        .filter((shardId): shardId is string => shardId !== null)
    )
  ];
}

function routeKeysForTrips(trips: PlaybackTrip[]): Set<string> {
  return new Set(
    trips
      .map((trip) => trip.routeKey)
      .filter((routeKey): routeKey is string => routeKey !== null)
  );
}

function tripTouchesStationFilter(trip: PlaybackTrip, stationIds: Set<string>): boolean {
  return (
    stationIds.has(stationKey(trip.fromStationId, trip.fromStationName)) ||
    stationIds.has(stationKey(trip.toStationId, trip.toStationName))
  );
}

function stationRadiusForBalance(balance: number): number {
  const signedDelta = Math.sign(balance) * Math.sqrt(Math.abs(balance)) * STATION_BALANCE_RADIUS_SCALE;
  return clamp(
    STATION_BASE_RADIUS_METERS + signedDelta,
    STATION_MIN_RADIUS_METERS,
    STATION_MAX_RADIUS_METERS
  );
}

function stationKey(stationId: string | null, stationName: string): string {
  return `${stationId ?? ""}\u001f${stationName}`;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function formatBalanceCounts(departures: number, arrivals: number, net: number): string {
  return `${departures.toLocaleString()} / ${arrivals.toLocaleString()} / ${formatSignedCount(net)}`;
}

function formatSignedCount(value: number): string {
  return `${value > 0 ? "+" : ""}${value.toLocaleString()}`;
}

function isLowMemoryRouteMode(): boolean {
  const coarsePointer = window.matchMedia("(pointer: coarse)").matches;
  const narrowViewport = window.matchMedia("(max-width: 760px)").matches;
  const deviceMemory =
    "deviceMemory" in navigator
      ? (navigator as Navigator & { deviceMemory?: number }).deviceMemory ?? 8
      : 8;
  return coarsePointer || narrowViewport || deviceMemory <= 4;
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
