from __future__ import annotations

from datetime import date
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from cyclehire.dashboard import PlaybackDataStore


def create_app(data_dir: Path = Path("data")) -> FastAPI:
    api = FastAPI(title="Cycle Hire Dashboard API")
    api.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://127.0.0.1:5173",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @lru_cache(maxsize=1)
    def store() -> PlaybackDataStore:
        return PlaybackDataStore(data_dir=data_dir)

    @api.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @api.get("/api/playback/date-range")
    def playback_date_range() -> dict[str, object]:
        try:
            return store().date_range()
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @api.get("/api/playback/{playback_date}")
    def playback_day(playback_date: date) -> dict[str, object]:
        try:
            return store().playback_day(playback_date)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    return api


app = create_app()
