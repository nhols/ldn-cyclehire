import { useEffect, useMemo, useRef, useState } from "react";
import type { PointerEvent } from "react";
import type { DaySummary } from "./types";

type DateHistogramProps = {
  days: DaySummary[];
  selectedDate: string;
  onChange: (date: string) => void;
};

type ZoomWidth = "week" | "month" | "year";

type Boundary = {
  date: string;
  label: string;
  x: number;
};

const DAY_WIDTH_BY_ZOOM: Record<ZoomWidth, number> = {
  week: 34,
  month: 9,
  year: 2.4
};
const SVG_HEIGHT = 88;
const PLOT_TOP = 16;
const PLOT_HEIGHT = 64;
const DRAG_THRESHOLD_PX = 8;
const INERTIA_FRICTION_PER_FRAME = 0.92;
const INERTIA_MIN_VELOCITY = 0.02;

type DragState = {
  pointerId: number;
  startClientX: number;
  startClientY: number;
  startScrollLeft: number;
  lastScrollLeft: number;
  lastTimestamp: number;
  velocity: number;
  dragged: boolean;
};

export function DateHistogram({ days, selectedDate, onChange }: DateHistogramProps) {
  const viewportRef = useRef<HTMLDivElement | null>(null);
  const dragRef = useRef<DragState | null>(null);
  const inertiaFrameRef = useRef<number | null>(null);
  const autoScrollRef = useRef<{ initialized: boolean; dayWidth: number | null }>({
    initialized: false,
    dayWidth: null
  });
  const [hoveredDate, setHoveredDate] = useState<string | null>(null);
  const [zoomWidth, setZoomWidth] = useState<ZoomWidth>("month");
  const sortedDays = useMemo(() => [...days].sort((a, b) => a.date.localeCompare(b.date)), [days]);
  const dayWidth = DAY_WIDTH_BY_ZOOM[zoomWidth];
  const maxTrips = Math.max(1, ...sortedDays.map((day) => day.trips));
  const selectedIndex = sortedDays.findIndex((day) => day.date === selectedDate);
  const selectedDay = selectedIndex >= 0 ? sortedDays[selectedIndex] : undefined;
  const hoveredDay = hoveredDate ? sortedDays.find((day) => day.date === hoveredDate) : null;
  const labelDay = hoveredDay ?? selectedDay;
  const width = Math.max(1, sortedDays.length * dayWidth);

  const bars = useMemo(() => {
    return sortedDays.map((day, index) => {
      const height = Math.max(1, (day.trips / maxTrips) * PLOT_HEIGHT);
      return {
        date: day.date,
        x: index * dayWidth,
        y: PLOT_TOP + PLOT_HEIGHT - height,
        width: Math.max(1, dayWidth * 0.72),
        height
      };
    });
  }, [dayWidth, maxTrips, sortedDays]);

  const boundaries = useMemo(
    () => boundariesForDays(sortedDays, zoomWidth, dayWidth),
    [dayWidth, sortedDays, zoomWidth]
  );

  useEffect(() => {
    const viewport = viewportRef.current;
    if (!viewport || selectedIndex < 0) return;
    const shouldAutoScroll = !autoScrollRef.current.initialized || autoScrollRef.current.dayWidth !== dayWidth;
    if (!shouldAutoScroll) return;

    autoScrollRef.current = {
      initialized: true,
      dayWidth
    };
    const selectedX = selectedIndex * dayWidth;
    viewport.scrollTo({
      left: Math.max(0, selectedX - viewport.clientWidth / 2),
      behavior: "smooth"
    });
  }, [dayWidth, selectedIndex]);

  useEffect(() => {
    return () => stopInertia();
  }, []);

  function pickDate(clientX: number) {
    const day = dayForClientX(clientX);
    if (day) onChange(day.date);
  }

  function hoverDate(clientX: number) {
    setHoveredDate(dayForClientX(clientX)?.date ?? null);
  }

  function handlePointerDown(event: PointerEvent<HTMLDivElement>) {
    const viewport = event.currentTarget;
    stopInertia();
    if (event.pointerType !== "touch") {
      viewport.setPointerCapture(event.pointerId);
    }
    dragRef.current = {
      pointerId: event.pointerId,
      startClientX: event.clientX,
      startClientY: event.clientY,
      startScrollLeft: viewport.scrollLeft,
      lastScrollLeft: viewport.scrollLeft,
      lastTimestamp: event.timeStamp,
      velocity: 0,
      dragged: false
    };
  }

  function handlePointerMove(event: PointerEvent<HTMLDivElement>) {
    const drag = dragRef.current;
    if (!drag || drag.pointerId !== event.pointerId) {
      if (event.pointerType !== "touch") {
        hoverDate(event.clientX);
      }
      return;
    }

    const deltaX = event.clientX - drag.startClientX;
    const deltaY = event.clientY - drag.startClientY;
    if (!drag.dragged && Math.hypot(deltaX, deltaY) > DRAG_THRESHOLD_PX) {
      drag.dragged = true;
    }

    if (drag.dragged) {
      setHoveredDate(null);
      if (event.pointerType !== "touch") {
        const nextScrollLeft = drag.startScrollLeft - deltaX;
        const elapsed = Math.max(1, event.timeStamp - drag.lastTimestamp);
        event.currentTarget.scrollLeft = nextScrollLeft;
        drag.velocity = (event.currentTarget.scrollLeft - drag.lastScrollLeft) / elapsed;
        drag.lastScrollLeft = event.currentTarget.scrollLeft;
        drag.lastTimestamp = event.timeStamp;
        event.preventDefault();
      }
      return;
    }

    if (event.pointerType !== "touch") {
      hoverDate(event.clientX);
    }
  }

  function handlePointerUp(event: PointerEvent<HTMLDivElement>) {
    const drag = dragRef.current;
    if (!drag || drag.pointerId !== event.pointerId) return;
    dragRef.current = null;
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }

    if (!drag.dragged) {
      pickDate(event.clientX);
    } else if (event.pointerType !== "touch") {
      startInertia(event.currentTarget, drag.velocity);
    }
  }

  function handlePointerCancel(event: PointerEvent<HTMLDivElement>) {
    if (dragRef.current?.pointerId === event.pointerId) {
      dragRef.current = null;
    }
    setHoveredDate(null);
  }

  function startInertia(viewport: HTMLDivElement, initialVelocity: number) {
    let velocity = initialVelocity;
    let lastTimestamp: number | null = null;

    const step = (timestamp: number) => {
      if (lastTimestamp === null) {
        lastTimestamp = timestamp;
      }
      const elapsed = timestamp - lastTimestamp;
      lastTimestamp = timestamp;

      const maxScrollLeft = viewport.scrollWidth - viewport.clientWidth;
      const nextScrollLeft = Math.max(0, Math.min(maxScrollLeft, viewport.scrollLeft + velocity * elapsed));
      const hitBoundary = nextScrollLeft === 0 || nextScrollLeft === maxScrollLeft;
      viewport.scrollLeft = nextScrollLeft;
      velocity *= Math.pow(INERTIA_FRICTION_PER_FRAME, elapsed / 16.67);

      if (Math.abs(velocity) > INERTIA_MIN_VELOCITY && !hitBoundary) {
        inertiaFrameRef.current = requestAnimationFrame(step);
      } else {
        inertiaFrameRef.current = null;
      }
    };

    if (Math.abs(velocity) > INERTIA_MIN_VELOCITY) {
      inertiaFrameRef.current = requestAnimationFrame(step);
    }
  }

  function stopInertia() {
    if (inertiaFrameRef.current !== null) {
      cancelAnimationFrame(inertiaFrameRef.current);
      inertiaFrameRef.current = null;
    }
  }

  function dayForClientX(clientX: number): DaySummary | null {
    const rect = viewportRef.current?.getBoundingClientRect();
    const viewport = viewportRef.current;
    if (!rect || !viewport || !sortedDays.length) return null;
    const x = clientX - rect.left + viewport.scrollLeft;
    const index = Math.min(sortedDays.length - 1, Math.max(0, Math.floor(x / dayWidth)));
    return sortedDays[index];
  }

  return (
    <div className="date-histogram">
      <div className="date-histogram-toolbar" role="group" aria-label="Histogram zoom width">
        {(["week", "month", "year"] as const).map((value) => (
          <button
            key={value}
            className={zoomWidth === value ? "active" : ""}
            type="button"
            onClick={() => setZoomWidth(value)}
          >
            {value}ly
          </button>
        ))}
      </div>

      <div
        ref={viewportRef}
        className="date-histogram-chart"
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerCancel={handlePointerCancel}
        onPointerLeave={() => {
          if (!dragRef.current) {
            setHoveredDate(null);
          }
        }}
      >
        <svg
          width={width}
          height={SVG_HEIGHT}
          viewBox={`0 0 ${width} ${SVG_HEIGHT}`}
          preserveAspectRatio="none"
          aria-label="Daily journey volume"
        >
          {boundaries.map((boundary) => (
            <g key={boundary.date}>
              <line className="date-boundary" x1={boundary.x} x2={boundary.x} y1={PLOT_TOP} y2={SVG_HEIGHT} />
              <text className="date-boundary-label" x={boundary.x + 3} y={10}>
                {boundary.label}
              </text>
            </g>
          ))}
          {bars.map((bar) => (
            <rect
              key={bar.date}
              className={bar.date === selectedDate ? "date-bar selected" : "date-bar"}
              x={bar.x}
              y={bar.y}
              width={bar.width}
              height={bar.height}
            />
          ))}
          {selectedDay && selectedIndex >= 0 && (
            <line
              className="date-cursor"
              x1={selectedIndex * dayWidth}
              x2={selectedIndex * dayWidth}
              y1={PLOT_TOP}
              y2={SVG_HEIGHT}
            />
          )}
        </svg>
      </div>
      <span className="date-histogram-label">
        {labelDay
          ? `${labelDay.date} · ${weekdayLabel(labelDay.date)} · ${labelDay.trips.toLocaleString()} journeys`
          : "No day data"}
      </span>
    </div>
  );
}

function boundariesForDays(days: DaySummary[], zoomWidth: ZoomWidth, dayWidth: number): Boundary[] {
  const boundaries: Boundary[] = [];
  for (let index = 0; index < days.length; index += 1) {
    const date = days[index].date;
    if (!isBoundary(date, zoomWidth)) continue;
    boundaries.push({
      date,
      label: boundaryLabel(date, zoomWidth),
      x: index * dayWidth
    });
  }
  return boundaries;
}

function isBoundary(date: string, zoomWidth: ZoomWidth): boolean {
  if (zoomWidth === "year") return date.endsWith("-01-01");
  if (zoomWidth === "month") return date.endsWith("-01");
  return isMonday(date);
}

function boundaryLabel(date: string, zoomWidth: ZoomWidth): string {
  const value = new Date(`${date}T00:00:00Z`);
  if (zoomWidth === "year") return String(value.getUTCFullYear());
  const month = value.toLocaleString("en-GB", { month: "short", timeZone: "UTC" });
  if (zoomWidth === "month") return `${month} ${value.getUTCFullYear()}`;
  return `w/c ${value.getUTCDate()} ${month}`;
}

function isMonday(date: string): boolean {
  return new Date(`${date}T00:00:00Z`).getUTCDay() === 1;
}

function weekdayLabel(date: string): string {
  return new Date(`${date}T00:00:00Z`).toLocaleString("en-GB", {
    weekday: "long",
    timeZone: "UTC"
  });
}
