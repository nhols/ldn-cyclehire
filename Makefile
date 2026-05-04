.PHONY: install dev export-static export-static-all frontend

install:
	uv sync
	cd frontend && npm install

dev: export-static frontend

export-static:
	uv run cyclehire export-cdn --date 2025-06-18 --output-dir frontend/public/data

export-static-all:
	uv run cyclehire export-cdn --output-dir frontend/public/data

frontend:
	cd frontend && npm run dev
