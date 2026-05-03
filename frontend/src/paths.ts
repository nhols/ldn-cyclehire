import type { Coord } from "./types";

export function curvedPath(from: Coord, to: Coord, steps = 24): Coord[] {
  const [fromLon, fromLat] = from;
  const [toLon, toLat] = to;
  const dx = toLon - fromLon;
  const dy = toLat - fromLat;
  const distance = Math.hypot(dx, dy);
  const bend = Math.min(0.012, distance * 0.22);
  const normalX = distance === 0 ? 0 : -dy / distance;
  const normalY = distance === 0 ? 0 : dx / distance;
  const path: Coord[] = [];

  for (let index = 0; index <= steps; index += 1) {
    const t = index / steps;
    const curve = Math.sin(Math.PI * t) * bend;
    path.push([
      fromLon + dx * t + normalX * curve,
      fromLat + dy * t + normalY * curve
    ]);
  }

  return path;
}

export function slicePath(path: Coord[], progress: number): Coord[] {
  return slicePathWindow(path, Math.max(0, Math.min(1, progress)), 0);
}

export function slicePathWindow(path: Coord[], endProgress: number, windowSize: number): Coord[] {
  const end = pointAtProgress(path, endProgress);
  const startProgress = Math.max(0, endProgress - windowSize);
  const start = pointAtProgress(path, startProgress);
  const startIndex = Math.floor(startProgress * (path.length - 1));
  const endIndex = Math.floor(endProgress * (path.length - 1));
  const middle = path.slice(startIndex + 1, endIndex + 1);

  if (endProgress <= 0) {
    return [path[0]];
  }

  return [start, ...middle, end];
}

function pointAtProgress(path: Coord[], progress: number): Coord {
  if (progress <= 0) return path[0];
  if (progress >= 1) return path[path.length - 1];

  const scaled = progress * (path.length - 1);
  const index = Math.floor(scaled);
  const partial = scaled - index;
  const current = path[index];
  const next = path[index + 1] ?? current;

  return [
    current[0] + (next[0] - current[0]) * partial,
    current[1] + (next[1] - current[1]) * partial
  ];
}

export function formatClock(seconds: number): string {
  const bounded = Math.max(0, Math.min(86399, Math.floor(seconds)));
  const hours = Math.floor(bounded / 3600);
  const minutes = Math.floor((bounded % 3600) / 60);
  const secs = bounded % 60;
  return [hours, minutes, secs].map((value) => value.toString().padStart(2, "0")).join(":");
}
