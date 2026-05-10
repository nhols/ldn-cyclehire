import type { Coord, FlatPath } from "./types";

export function curvedFlatPath(from: Coord, to: Coord, steps = 24): FlatPath {
  const [fromLon, fromLat] = from;
  const [toLon, toLat] = to;
  const dx = toLon - fromLon;
  const dy = toLat - fromLat;
  const distance = Math.hypot(dx, dy);
  const bend = Math.min(0.012, distance * 0.22);
  const normalX = distance === 0 ? 0 : -dy / distance;
  const normalY = distance === 0 ? 0 : dx / distance;
  const path = new Float32Array((steps + 1) * 2);

  for (let index = 0; index <= steps; index += 1) {
    const t = index / steps;
    const curve = Math.sin(Math.PI * t) * bend;
    path[index * 2] = fromLon + dx * t + normalX * curve;
    path[index * 2 + 1] = fromLat + dy * t + normalY * curve;
  }

  return path;
}

export function sliceFlatPathWindow(
  path: FlatPath,
  endProgress: number,
  windowSize: number,
  reversed = false
): FlatPath {
  const pointCount = path.length / 2;
  if (pointCount <= 1 || endProgress <= 0) {
    const point = pointAtFlatProgress(path, 0, reversed);
    return new Float32Array(point);
  }

  const boundedEnd = Math.max(0, Math.min(1, endProgress));
  const startProgress = Math.max(0, boundedEnd - windowSize);
  const start = pointAtFlatProgress(path, startProgress, reversed);
  const end = pointAtFlatProgress(path, boundedEnd, reversed);
  const startIndex = Math.floor(startProgress * (pointCount - 1));
  const endIndex = Math.floor(boundedEnd * (pointCount - 1));
  const middlePointCount = Math.max(0, endIndex - startIndex);
  const sliced = new Float32Array((middlePointCount + 2) * 2);
  let offset = 0;

  sliced[offset++] = start[0];
  sliced[offset++] = start[1];
  for (let index = startIndex + 1; index <= endIndex; index += 1) {
    const sourceIndex = reversed ? pointCount - 1 - index : index;
    sliced[offset++] = path[sourceIndex * 2];
    sliced[offset++] = path[sourceIndex * 2 + 1];
  }
  sliced[offset++] = end[0];
  sliced[offset] = end[1];

  return sliced;
}

function pointAtFlatProgress(path: FlatPath, progress: number, reversed: boolean): Coord {
  const pointCount = path.length / 2;
  if (pointCount === 0) return [0, 0];

  const readPoint = (index: number): Coord => {
    const sourceIndex = reversed ? pointCount - 1 - index : index;
    return [path[sourceIndex * 2], path[sourceIndex * 2 + 1]];
  };

  if (progress <= 0) return readPoint(0);
  if (progress >= 1) return readPoint(pointCount - 1);

  const scaled = progress * (pointCount - 1);
  const index = Math.floor(scaled);
  const partial = scaled - index;
  const current = readPoint(index);
  const next = readPoint(index + 1);

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
