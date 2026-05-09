R2_BUCKET ?= ldn-cyclehire-data
R2_ACCOUNT_ID ?=
R2_UPLOAD_DIR ?= /tmp/cyclehire-r2-upload
CDN_DIR ?= data/cdn
ROUTE_PROVIDER ?= mapbox

.PHONY: install dev prep-tfl-data sql export-static export-static-full prepare-r2-upload upload-static-r2 upload-static-r2-bulk upload-static-r2-wrangler frontend

install:
	uv sync
	cd frontend && npm install

dev: export-static frontend

prep-tfl-data:
	uv run cyclehire raw
	uv run cyclehire normalize
	uv run cyclehire validate
	uv run cyclehire bikepoints

sql:
	uv run cyclehire sql

export-static:
	uv run cyclehire export-static --date 2025-06-18 --output-dir frontend/public/data --route-provider $(ROUTE_PROVIDER)

export-static-full:
	uv run cyclehire export-static --output-dir $(CDN_DIR) --route-provider $(ROUTE_PROVIDER)

prepare-r2-upload:
	CDN_DIR=$(CDN_DIR) R2_UPLOAD_DIR=$(R2_UPLOAD_DIR) bash scripts/prepare-r2-upload-dir.sh

upload-static-r2: upload-static-r2-bulk

upload-static-r2-bulk: prepare-r2-upload
	R2_UPLOAD_DIR=$(R2_UPLOAD_DIR) R2_BUCKET=$(R2_BUCKET) R2_ACCOUNT_ID=$(R2_ACCOUNT_ID) bash scripts/upload-static-r2-bulk.sh

upload-static-r2-wrangler:
	CDN_DIR=$(CDN_DIR) R2_BUCKET=$(R2_BUCKET) bash scripts/upload-static-r2.sh

frontend:
	cd frontend && npm run dev
