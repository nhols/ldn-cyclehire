import { useMemo, useRef } from "react";
import type { ActivityPoint } from "./types";

type ActivityScrubberProps = {
  activity: ActivityPoint[];
  currentTime: number;
  onChange: (time: number) => void;
};

const WIDTH = 720;
const HEIGHT = 76;
const DAY_SECONDS = 24 * 60 * 60 - 1;
const ACTIVITY_THRESHOLD_RATIO = 0.02;

export function ActivityScrubber({ activity, currentTime, onChange }: ActivityScrubberProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const maxActive = Math.max(1, ...activity.map((point) => point.activeTrips));
  const [domainStart, domainEnd] = useMemo(() => activityDomain(activity, maxActive), [activity, maxActive]);
  const domainSpan = Math.max(1, domainEnd - domainStart);
  const cursorRatio = (currentTime - domainStart) / domainSpan;
  const cursorX = Math.max(0, Math.min(WIDTH, cursorRatio * WIDTH));

  const { areaPath, linePath } = useMemo(() => {
    if (!activity.length) {
      return { areaPath: "", linePath: "" };
    }

    const visibleActivity = activity.filter(
      (point) => point.time >= domainStart && point.time <= domainEnd
    );
    const points = visibleActivity.map((point) => {
      const x = ((point.time - domainStart) / domainSpan) * WIDTH;
      const y = HEIGHT - (point.activeTrips / maxActive) * HEIGHT;
      return [x, y] as const;
    });

    const line = points
      .map(([x, y], index) => `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`)
      .join(" ");
    const baseline = HEIGHT;
    const area = `${line} L ${points[points.length - 1][0].toFixed(2)} ${baseline} L ${points[0][0].toFixed(
      2
    )} ${baseline} Z`;

    return { areaPath: area, linePath: line };
  }, [activity, domainEnd, domainSpan, domainStart, maxActive]);

  function scrub(clientX: number) {
    const rect = containerRef.current?.getBoundingClientRect();
    if (!rect) return;
    const ratio = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
    onChange(Math.round(domainStart + ratio * domainSpan));
  }

  return (
    <div
      ref={containerRef}
      className="activity-scrubber"
      onPointerDown={(event) => {
        event.currentTarget.setPointerCapture(event.pointerId);
        scrub(event.clientX);
      }}
      onPointerMove={(event) => {
        if (event.buttons === 1) {
          scrub(event.clientX);
        }
      }}
    >
      <svg
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        preserveAspectRatio="none"
        role="img"
        aria-label="Active trips through the day"
      >
        <path className="activity-area" d={areaPath} />
        <path className="activity-line" d={linePath} />
        <line className="activity-cursor" x1={cursorX} x2={cursorX} y1={0} y2={HEIGHT} />
      </svg>
    </div>
  );
}

function activityDomain(activity: ActivityPoint[], maxActive: number): [number, number] {
  if (!activity.length) {
    return [0, DAY_SECONDS];
  }

  const threshold = Math.max(1, Math.floor(maxActive * ACTIVITY_THRESHOLD_RATIO));
  const active = activity.filter((point) => point.activeTrips >= threshold);
  if (!active.length) {
    return [0, DAY_SECONDS];
  }

  const start = active[0].time;
  const end = active[active.length - 1].time;
  return end > start ? [start, end] : [0, DAY_SECONDS];
}
