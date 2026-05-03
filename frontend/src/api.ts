import type { DateRangeResponse, PlaybackResponse } from "./types";

export async function fetchDateRange(): Promise<DateRangeResponse> {
  const response = await fetch("/api/playback/date-range");
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return response.json();
}

export async function fetchPlayback(date: string): Promise<PlaybackResponse> {
  const response = await fetch(`/api/playback/${date}`);
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return response.json();
}
