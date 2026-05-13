import { useEffect, useMemo, useRef, useState } from "react";
import DeckGL from "@deck.gl/react";
import { PathLayer, ScatterplotLayer } from "@deck.gl/layers";
import { Map } from "react-map-gl/maplibre";
import { BarChart3, Filter, Github, Info, Monitor, Moon, Settings2, Pause, Play, RotateCcw, Search, Sun, X } from "lucide-react";
import { ActivityScrubber } from "./ActivityScrubber";
import { DateHistogram } from "./DateHistogram";
import { fetchDateRange, fetchDaySummaries, fetchPlayback, fetchRouteShardRoutes } from "./api";
import { curvedFlatPath, formatClock, sliceFlatPathWindow } from "./paths";
import type { DaySummary, FlatPath, PlaybackResponse, PlaybackStation, PlaybackTrip } from "./types";

const LONDON_VIEW = {
  longitude: -0.11,
  latitude: 51.505,
  zoom: 11.8,
  pitch: 0,
  bearing: -8
};

const MAP_STYLES = {
  dark: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
  light: "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json"
} satisfies Record<ResolvedTheme, string>;
const MIN_PLAYBACK_SPEED = 1;
const MAX_PLAYBACK_SPEED = 2400;
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
  path: FlatPath;
  progress: number;
  routed: boolean;
};

type TraceHead = ActivePath & {
  position: [number, number];
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
  const routeCacheRef = useRef<globalThis.Map<string, FlatPath>>(new globalThis.Map());
  const [routeCacheVersion, setRouteCacheVersion] = useState(0);
  const [requiredRouteShards, setRequiredRouteShards] = useState<string[]>([]);
  const [loadedRouteShardCount, setLoadedRouteShardCount] = useState(0);
  const [currentTime, setCurrentTime] = useState(7 * 3600);
  const [speed, setSpeed] = useState(180);
  const [tailLength, setTailLength] = useState(18);
  const [showUnrouted, setShowUnrouted] = useState(false);
  const [showTraceHeads, setShowTraceHeads] = useState(true);
  const [showOriginFlashes, setShowOriginFlashes] = useState(true);
  const [showDestinationFlashes, setShowDestinationFlashes] = useState(true);
  const [routedColor, setRoutedColor] = useState(DEFAULT_ROUTED_COLOR);
  const [unroutedColor, setUnroutedColor] = useState(DEFAULT_UNROUTED_COLOR);
  const [originFlashColor, setOriginFlashColor] = useState(DEFAULT_ORIGIN_FLASH_COLOR);
  const [destinationFlashColor, setDestinationFlashColor] = useState(DEFAULT_DESTINATION_FLASH_COLOR);
  const [dateExplorerOpen, setDateExplorerOpen] = useState(false);
  const [traceMenuOpen, setTraceMenuOpen] = useState(false);
  const [filterMenuOpen, setFilterMenuOpen] = useState(false);
  const [metricsOpen, setMetricsOpen] = useState(false);
  const [aboutOpen, setAboutOpen] = useState(false);
  const [stationFilterMode, setStationFilterMode] = useState(false);
  const [filteredStationIds, setFilteredStationIds] = useState<string[]>([]);
  const [filterQuery, setFilterQuery] = useState("");
  const [playing, setPlaying] = useState(false);
  const [pendingPlay, setPendingPlay] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null);
  const frameRef = useRef<number | null>(null);
  const lastTickRef = useRef<number | null>(null);
  const dateDiscoveryRef = useRef<HTMLDivElement | null>(null);
  const traceSettingsRef = useRef<HTMLDivElement | null>(null);
  const filterMenuRef = useRef<HTMLDivElement | null>(null);
  const aboutMenuRef = useRef<HTMLDivElement | null>(null);
  const filterSearchRef = useRef<HTMLInputElement | null>(null);
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
    if (!dateExplorerOpen && !traceMenuOpen && !filterMenuOpen && !aboutOpen) return;

    const handlePointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (dateExplorerOpen && !dateDiscoveryRef.current?.contains(target)) {
        setDateExplorerOpen(false);
      }
      if (traceMenuOpen && !traceSettingsRef.current?.contains(target)) {
        setTraceMenuOpen(false);
      }
      if (filterMenuOpen && !filterMenuRef.current?.contains(target)) {
        setFilterMenuOpen(false);
      }
      if (aboutOpen && !aboutMenuRef.current?.contains(target)) {
        setAboutOpen(false);
      }
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setDateExplorerOpen(false);
      setTraceMenuOpen(false);
      setFilterMenuOpen(false);
      setAboutOpen(false);
    };

    document.addEventListener("pointerdown", handlePointerDown);
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("pointerdown", handlePointerDown);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [dateExplorerOpen, traceMenuOpen, filterMenuOpen, aboutOpen]);

  useEffect(() => {
    if (filterMenuOpen) {
      filterSearchRef.current?.focus();
    }
  }, [filterMenuOpen]);

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
    let cancelled = false;
    setLoading(true);
    setError(null);
    setPlaying(false);
    fetchPlayback(selectedDate)
      .then((data) => {
        if (cancelled) return;
        const routeShardIds = routeShardIdsForTrips(data.trips);
        setPlayback(data);
        routeCacheRef.current = new globalThis.Map();
        setRouteCacheVersion((value) => value + 1);
        setRequiredRouteShards(routeShardIds);
        setLoadedRouteShardCount(0);
        setPendingPlay(true);
        const firstTrip = data.trips[0];
        setCurrentTime(firstTrip ? Math.max(0, firstTrip.start - 600) : 7 * 3600);
      })
      .catch((reason) => {
        if (!cancelled) setError(String(reason));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
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
        const routeBatches = await Promise.all(
          batch.map((shardId) => fetchRouteShardRoutes(shardId, routeKeysForDay))
        );
        if (cancelled) return;
        for (const routes of routeBatches) {
          for (const [routeKey, coordinates] of routes) {
            routeCacheRef.current.set(routeKey, coordinates);
          }
        }
        setRouteCacheVersion((value) => value + 1);
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
        const routePath = routePathForTrip(trip, routeCacheRef.current);
        const path = routePath ?? curvedFlatPath(trip.fromCoord, trip.toCoord);
        return {
          id: trip.id,
          progress,
          routed,
          path: sliceFlatPathWindow(path, progress, tailLength / 100, routePath !== null && trip.routeReversed)
        };
      });
  }, [displayTrips, currentTime, routeCacheVersion, tailLength, showUnrouted]);

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

  const filteredStations = useMemo(() => {
    return filteredStationIds
      .map((stationId) => stationById.get(stationId))
      .filter((station): station is BalancedStation => station !== undefined);
  }, [filteredStationIds, stationById]);

  const filterSearchResults = useMemo(() => {
    const query = filterQuery.trim().toLocaleLowerCase();
    if (!query) return [];
    return stationsWithBalance
      .filter((station) => station.name.toLocaleLowerCase().includes(query))
      .sort((a, b) => b.tripCount - a.tripCount)
      .slice(0, 8);
  }, [filterQuery, stationsWithBalance]);

  const selectedStationSet = useMemo(() => new Set(filteredStationIds), [filteredStationIds]);
  const traceHeads = useMemo(
    () => showTraceHeads ? activePaths.map(traceHeadForPath).filter(isTraceHead) : [],
    [activePaths, showTraceHeads]
  );
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
        _pathType: "open",
        positionFormat: "XY",
        getPath: (item) => item.path,
        getColor: (item) => traceColor(item, routedColor, unroutedColor),
        getWidth: (item) => (item.routed ? 3.5 : 2.2),
        widthMinPixels: 2,
        jointRounded: true,
        capRounded: true
      }),
      new ScatterplotLayer<TraceHead>({
        id: "active-trip-heads",
        data: traceHeads,
        getPosition: (item) => item.position,
        getFillColor: (item) => traceHeadFillColor(item, routedColor, unroutedColor),
        getLineColor: [8, 12, 10, 120],
        getRadius: (item) => (item.routed ? 34 : 26),
        lineWidthMinPixels: 1,
        radiusUnits: "meters",
        stroked: true,
        filled: true
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
      traceHeads,
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
  const dataLoading = loading || routeLoading;
  const routeLoadingProgress =
    requiredRouteShards.length === 0
      ? 0
      : (loadedRouteShardCount / requiredRouteShards.length) * 100;
  const loadingProgressLabel = routeLoading
    ? `Loading routes ${loadedRouteShardCount.toLocaleString()} of ${requiredRouteShards.length.toLocaleString()}`
    : `Loading ${selectedDate}`;
  const selectedDateLabel = formatPanelDate(selectedDate);
  const currentTimeLabel = formatPanelClock(currentTime);
  const settingsShortcutLabel = isApplePlatform() ? "⌘K" : "Ctrl K";
  const speedSliderValue = speedToSliderValue(speed);
  const speedScaleLabel = formatSpeedScale(speed);

  function togglePlayback() {
    if (playing) {
      setPlaying(false);
      setPendingPlay(false);
    } else if (routesReady) {
      setPlaying(true);
    } else {
      setPendingPlay(true);
    }
  }

  function restartPlayback() {
    if (routesReady) {
      setPlaying(true);
      setPendingPlay(false);
    } else {
      setPlaying(false);
      setPendingPlay(true);
    }
    setCurrentTime(0);
  }

  function toggleStationFilter(stationId: string) {
    setFilteredStationIds((value) =>
      value.includes(stationId)
        ? value.filter((selectedId) => selectedId !== stationId)
        : [...value, stationId]
    );
  }

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setTraceMenuOpen((value) => !value);
        setDateExplorerOpen(false);
        setFilterMenuOpen(false);
        setAboutOpen(false);
        return;
      }

      if (!event.metaKey && !event.ctrlKey && !event.altKey && !isTextEntryTarget(event.target)) {
        if (event.key.toLowerCase() === "f") {
          event.preventDefault();
          setFilterMenuOpen((value) => !value);
          setDateExplorerOpen(false);
          setTraceMenuOpen(false);
          setAboutOpen(false);
          return;
        }

        if (event.key.toLowerCase() === "m") {
          event.preventDefault();
          setMetricsOpen((value) => !value);
          return;
        }

        if (event.key.toLowerCase() === "r") {
          event.preventDefault();
          restartPlayback();
          return;
        }

        if (event.key.toLowerCase() === "i") {
          event.preventDefault();
          setAboutOpen((value) => !value);
          setDateExplorerOpen(false);
          setTraceMenuOpen(false);
          setFilterMenuOpen(false);
          return;
        }
      }

      if (event.code === "Space" && !event.metaKey && !event.ctrlKey && !event.altKey && !isTextEntryTarget(event.target)) {
        event.preventDefault();
        togglePlayback();
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  });

  return (
    <main className="app-shell">
      <section className="map-stage" aria-label="Cycle hire map playback">
        <DeckGL initialViewState={LONDON_VIEW} controller layers={layers}>
          <Map mapStyle={MAP_STYLES[resolvedTheme]} reuseMaps />
        </DeckGL>

        <div className="map-controls-left" aria-label="Playback controls">
          <button
            className="floating-button play-control"
            type="button"
            aria-label={playing ? "Pause playback" : routeLoading ? "Play when routes are loaded" : "Play playback"}
            title={playing ? "Pause playback" : routeLoading ? "Play when routes are loaded" : "Play playback"}
            onClick={togglePlayback}
          >
            {playing ? <Pause size={18} /> : <Play size={18} />}
            <span className="mobile-button-label">{playing ? "Pause" : "Play"}</span>
            <kbd>Space</kbd>
          </button>

          <button
            className="floating-button compact"
            type="button"
            aria-label="Reset time"
            title="Reset time"
            onClick={restartPlayback}
          >
            <RotateCcw size={17} />
            <span className="mobile-button-label">Restart</span>
            <kbd>R</kbd>
          </button>

          <div className="filter-menu-anchor" ref={filterMenuRef}>
            <button
              className={`floating-button compact ${filterMenuOpen ? "active" : ""}`}
              type="button"
              aria-expanded={filterMenuOpen}
              aria-label="Open station filter"
              title="Station filter"
              onClick={() => {
                setFilterMenuOpen((value) => !value);
                setDateExplorerOpen(false);
                setTraceMenuOpen(false);
                setAboutOpen(false);
              }}
            >
              <Filter size={17} />
              <span className="mobile-button-label">Filter</span>
              {filteredStationIds.length > 0 && <em>{filteredStationIds.length}</em>}
              <kbd>F</kbd>
            </button>
            {filterMenuOpen && (
              <div className="filter-popover glass-panel centered-popover" role="dialog" aria-label="Station filter">
                <div className="settings-header">
                  <div>
                    <strong>Station Filter</strong>
                    <span>{filteredStationIds.length.toLocaleString()} selected</span>
                  </div>
                  <button
                    className="icon-button"
                    type="button"
                    aria-label="Close station filter"
                    title="Close station filter"
                    onClick={() => setFilterMenuOpen(false)}
                  >
                    <X size={16} />
                  </button>
                </div>

                <label className="switch-field">
                  <span>Select filter stations on a map</span>
                  <input
                    type="checkbox"
                    checked={stationFilterMode}
                    onChange={(event) => setStationFilterMode(event.target.checked)}
                  />
                </label>

                <label className="search-field">
                  <Search size={16} />
                  <input
                    ref={filterSearchRef}
                    type="search"
                    placeholder="Type station name"
                    value={filterQuery}
                    onChange={(event) => setFilterQuery(event.target.value)}
                  />
                </label>

                {filterSearchResults.length > 0 && (
                  <div className="station-results" role="listbox" aria-label="Station search results">
                    {filterSearchResults.map((station) => (
                      <button
                        key={station.id}
                        className={selectedStationSet.has(station.id) ? "selected" : ""}
                        type="button"
                        onClick={() => toggleStationFilter(station.id)}
                      >
                        <span>{station.name}</span>
                        <em>{station.tripCount.toLocaleString()}</em>
                      </button>
                    ))}
                  </div>
                )}

                <div className="filter-pills-header">
                  <span>Current filters</span>
                  <button
                    type="button"
                    disabled={filteredStationIds.length === 0}
                    onClick={() => setFilteredStationIds([])}
                  >
                    Clear all
                  </button>
                </div>

                <div className="filter-pills">
                  {filteredStations.length === 0 ? (
                    <span>No station filters</span>
                  ) : (
                    filteredStations.map((station) => (
                      <button key={station.id} type="button" onClick={() => toggleStationFilter(station.id)}>
                        {station.name}
                        <X size={13} />
                      </button>
                    ))
                  )}
                </div>
              </div>
            )}
          </div>

          <button
            className={`floating-button compact ${metricsOpen ? "active" : ""}`}
            type="button"
            aria-pressed={metricsOpen}
            aria-label="Toggle detailed metrics"
            title="Detailed metrics"
            onClick={() => setMetricsOpen((value) => !value)}
          >
            <BarChart3 size={17} />
            <span className="mobile-button-label">Metrics</span>
            <kbd>M</kbd>
          </button>

          <div className="about-menu-anchor" ref={aboutMenuRef}>
            <button
              className={`floating-button compact ${aboutOpen ? "active" : ""}`}
              type="button"
              aria-expanded={aboutOpen}
              aria-label="Open about"
              title="About"
              onClick={() => {
                setAboutOpen((value) => !value);
                setDateExplorerOpen(false);
                setTraceMenuOpen(false);
                setFilterMenuOpen(false);
              }}
            >
              <Info size={17} />
              <span className="mobile-button-label">About</span>
              <kbd>I</kbd>
            </button>
            {aboutOpen && (
              <div className="about-popover glass-panel centered-popover" role="dialog" aria-label="About this visualization">
                <div className="settings-header">
                  <div>
                    <strong>London Cycle Hire</strong>
                    <span>Powered by TfL Open Data.</span>
                  </div>
                  <button
                    className="icon-button"
                    type="button"
                    aria-label="Close about"
                    title="Close about"
                    onClick={() => setAboutOpen(false)}
                  >
                    <X size={16} />
                  </button>
                </div>
                <div className="about-copy">
                  <p>
                    TfL Open Data gives each hire trip a start time, end time, start station, and end station. For each
                    pair of stations, this app finds a likely cycling route and then replays the day at the selected
                    speed, so trips leave and arrive at their recorded times and places.
                  </p>
                  <p>
                    Stations grow or shrink as their balance changes through the day: a station with more arrivals than
                    departures gets larger, while one with a bike deficit gets smaller.
                  </p>
                  <p>
                    The activity histogram shows the rhythm of a day. Weekdays often have a sharp morning commute peak
                    around 9am and a more spread-out evening peak as people make their way home. Disruptions and events
                    show up too: Tube strike days such as 2015-07-09 have unusually high volume.
                  </p>
                  <p>
                    Big football nights are visible as well. On 2021-07-11, the England v Italy Euro final shows traffic
                    building before the 8pm kick-off as people head out, a drop during the match, then a sharp spike
                    after Italy win on penalties.
                  </p>
                </div>
                <a
                  className="source-link"
                  href="https://github.com/nhols/ldn-cyclehire"
                  target="_blank"
                  rel="noreferrer"
                >
                  <Github size={16} />
                  <span>GitHub source</span>
                </a>
              </div>
            )}
          </div>

          <div className="trace-settings command-menu-anchor" ref={traceSettingsRef}>
            <button
              className={`floating-button compact ${traceMenuOpen ? "active" : ""}`}
              type="button"
              aria-expanded={traceMenuOpen}
              aria-label="Open settings"
              title="Settings"
              onClick={() => {
                setTraceMenuOpen((value) => !value);
                setDateExplorerOpen(false);
                setFilterMenuOpen(false);
                setAboutOpen(false);
              }}
            >
              <Settings2 size={18} />
              <span className="mobile-button-label">Settings</span>
              <kbd>{settingsShortcutLabel}</kbd>
            </button>
            {traceMenuOpen && (
              <div className="trace-popover settings-command-menu centered-popover" role="dialog" aria-label="Settings">
                <div className="settings-header">
                  <div>
                    <strong>Settings</strong>
                    <span>Playback, theme, traces</span>
                  </div>
                  <button
                    className="icon-button"
                    type="button"
                    aria-label="Close settings"
                    title="Close settings"
                    onClick={() => setTraceMenuOpen(false)}
                  >
                    <X size={16} />
                  </button>
                </div>

                <div className="popover-group">
                  <span>Playback</span>
                  <label className="popover-slider speed-slider">
                    <span>Speed</span>
                    <strong>{speedScaleLabel}</strong>
                    <input
                      type="range"
                      min={0}
                      max={100}
                      step={1}
                      value={speedSliderValue}
                      onChange={(event) => setSpeed(sliderValueToSpeed(Number(event.target.value)))}
                    />
                  </label>
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
                </div>

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

                <div className="popover-group two-column-controls">
                  <span>Traces</span>
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
                  <label className="checkbox-field">
                    <input
                      type="checkbox"
                      checked={showTraceHeads}
                      onChange={(event) => setShowTraceHeads(event.target.checked)}
                    />
                    <span>Show trace heads</span>
                  </label>
                </div>

                <div className="popover-group two-column-controls">
                  <span>Station flashes</span>
                  <label className="color-field">
                    <span>Departure</span>
                    <input
                      type="color"
                      value={originFlashColor}
                      onChange={(event) => setOriginFlashColor(event.target.value)}
                    />
                  </label>
                  <label className="color-field">
                    <span>Arrival</span>
                    <input
                      type="color"
                      value={destinationFlashColor}
                      onChange={(event) => setDestinationFlashColor(event.target.value)}
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
                  <label className="checkbox-field">
                    <input
                      type="checkbox"
                      checked={showDestinationFlashes}
                      onChange={(event) => setShowDestinationFlashes(event.target.checked)}
                    />
                    <span>Show arrival flash</span>
                  </label>
                </div>

              </div>
            )}
          </div>
        </div>

        <div className="date-discovery map-date-control" ref={dateDiscoveryRef}>
          <button
            className={`date-chip ${dateExplorerOpen ? "active" : ""}`}
            type="button"
            aria-label="Open date explorer"
            title="Date explorer"
            aria-expanded={dateExplorerOpen}
            onClick={() => {
              setDateExplorerOpen((value) => !value);
              setTraceMenuOpen(false);
              setFilterMenuOpen(false);
              setAboutOpen(false);
            }}
          >
            <span>{selectedDateLabel}</span>
            <strong>{currentTimeLabel}</strong>
          </button>
          {dateExplorerOpen && (
            <div className="date-explorer-popover" role="dialog" aria-label="Date explorer">
              <div className="date-explorer-header">
                <span>Date explorer</span>
                <strong>{selectedDateLabel}</strong>
              </div>
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
              <DateHistogram days={daySummaries} selectedDate={selectedDate} onChange={setSelectedDate} />
            </div>
          )}
        </div>

        <aside className="activity-panel" aria-label="Daily activity">
          <div className="activity-panel-summary">
            <strong>{activeTrips.toLocaleString()}</strong>
            <span>active</span>
            <em>{totalTrips.toLocaleString()} total</em>
          </div>
          <ActivityScrubber
            activity={playback?.activity ?? []}
            currentTime={currentTime}
            onChange={setCurrentTime}
          />
          <div className="activity-axis" aria-hidden="true">
            <span>06:00</span>
            <span>12:00</span>
            <span>18:00</span>
          </div>
        </aside>
        {metricsOpen && (
          <aside className="metrics-panel glass-panel" aria-label="Detailed metrics">
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
              label="Total trips"
              value={totalTrips}
              split={{
                routed: totalRoutedTrips,
                unrouted: totalUnroutedTrips
              }}
            />
            <Metric label="Stations" value={summary?.stationCount ?? 0} />
            <Metric label="Unmatched" value={summary?.unmatchedTrips ?? 0} />
          </aside>
        )}
        {dataLoading && (
          <div
            className="loading-progress"
            role="progressbar"
            aria-label={loadingProgressLabel}
            aria-valuemin={0}
            aria-valuemax={100}
            aria-valuenow={routeLoading ? Math.round(routeLoadingProgress) : undefined}
          >
            <div className="loading-progress-track">
              <div
                className={`loading-progress-fill ${routeLoading ? "" : "indeterminate"}`}
                style={routeLoading ? { width: `${routeLoadingProgress}%` } : undefined}
              />
            </div>
          </div>
        )}
        {error && (
          <div className="status-panel">
            <span>{error}</span>
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

function traceColor(
  item: Pick<ActivePath, "progress" | "routed">,
  routedColor: string,
  unroutedColor: string
): [number, number, number, number] {
  return item.routed
    ? withAlpha(hexToRgb(routedColor), 100 + Math.round(item.progress * 140))
    : withAlpha(hexToRgb(unroutedColor), 55 + Math.round(item.progress * 110));
}

function traceHeadFillColor(
  item: Pick<ActivePath, "progress" | "routed">,
  routedColor: string,
  unroutedColor: string
): [number, number, number, number] {
  const base = item.routed ? hexToRgb(routedColor) : hexToRgb(unroutedColor);
  return withAlpha(mixRgb(base, [255, 255, 255], 0.34), 120 + Math.round(item.progress * 45));
}

function mixRgb(
  from: [number, number, number],
  to: [number, number, number],
  amount: number
): [number, number, number] {
  return [
    Math.round(from[0] + (to[0] - from[0]) * amount),
    Math.round(from[1] + (to[1] - from[1]) * amount),
    Math.round(from[2] + (to[2] - from[2]) * amount)
  ];
}

function traceHeadForPath(path: ActivePath): TraceHead | null {
  const pointCount = path.path.length / 2;
  if (pointCount < 1) return null;

  const endIndex = pointCount - 1;
  const end: [number, number] = [path.path[endIndex * 2], path.path[endIndex * 2 + 1]];

  return {
    ...path,
    position: end
  };
}

function isTraceHead(path: TraceHead | null): path is TraceHead {
  return path !== null;
}

function routePathForTrip(trip: PlaybackTrip, routeCache: globalThis.Map<string, FlatPath>): FlatPath | null {
  if (!trip.routeKey) {
    return null;
  }
  const route = routeCache.get(trip.routeKey);
  if (!route) {
    return null;
  }
  return route;
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

function isApplePlatform(): boolean {
  return /Mac|iPhone|iPad|iPod/.test(navigator.platform);
}

function speedToSliderValue(speed: number): number {
  const min = Math.log(MIN_PLAYBACK_SPEED);
  const max = Math.log(MAX_PLAYBACK_SPEED);
  return Math.round(((Math.log(clamp(speed, MIN_PLAYBACK_SPEED, MAX_PLAYBACK_SPEED)) - min) / (max - min)) * 100);
}

function sliderValueToSpeed(value: number): number {
  const min = Math.log(MIN_PLAYBACK_SPEED);
  const max = Math.log(MAX_PLAYBACK_SPEED);
  const speed = Math.exp(min + (clamp(value, 0, 100) / 100) * (max - min));
  return Math.max(MIN_PLAYBACK_SPEED, Math.round(speed));
}

function formatSpeedScale(speed: number): string {
  return `1 minute on screen = ${formatApproxDuration(speed * 60)} in the day`;
}

function formatApproxDuration(seconds: number): string {
  const roundedMinutes = Math.max(1, Math.round(seconds / 60));
  const hours = Math.floor(roundedMinutes / 60);
  const minutes = roundedMinutes % 60;

  if (hours === 0) {
    return `${roundedMinutes} ${roundedMinutes === 1 ? "minute" : "mins"}`;
  }

  if (minutes === 0) {
    return `${hours} ${hours === 1 ? "hour" : "hours"}`;
  }

  return `${hours} ${hours === 1 ? "hour" : "hours"} ${minutes} mins`;
}

function formatPanelDate(date: string): string {
  return new Date(`${date}T00:00:00Z`).toLocaleDateString("en-GB", {
    weekday: "short",
    day: "numeric",
    month: "short",
    year: "numeric",
    timeZone: "UTC"
  });
}

function formatPanelClock(seconds: number): string {
  const bounded = Math.max(0, Math.min(86399, Math.floor(seconds)));
  const hours = Math.floor(bounded / 3600);
  const minutes = Math.floor((bounded % 3600) / 60);
  const suffix = hours >= 12 ? "PM" : "AM";
  const displayHours = hours % 12 || 12;
  return `${displayHours}:${minutes.toString().padStart(2, "0")} ${suffix}`;
}

function isTextEntryTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  return target.matches("input, textarea, select") || target.isContentEditable;
}
