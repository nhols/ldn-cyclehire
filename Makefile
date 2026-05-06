R2_BUCKET ?= ldn-cyclehire-data
CDN_DIR ?= data/cdn
ROUTE_PROVIDER ?= mapbox

.PHONY: install dev export-static export-static-full upload-static-r2 frontend

install:
	uv sync
	cd frontend && npm install

dev: export-static frontend

export-static:
	uv run cyclehire export-static --date 2025-06-18 --output-dir frontend/public/data --route-provider $(ROUTE_PROVIDER)

export-static-full:
	uv run cyclehire export-static --output-dir $(CDN_DIR) --route-provider $(ROUTE_PROVIDER)

upload-static-r2:
	CDN_DIR=$(CDN_DIR) R2_BUCKET=$(R2_BUCKET) bash scripts/upload-static-r2.sh

frontend:
	cd frontend && npm run dev
