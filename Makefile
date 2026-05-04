.PHONY: install dev backend frontend

install:
	uv sync
	cd frontend && npm install

dev:
	$(MAKE) -j2 backend frontend

backend:
	uv run uvicorn cyclehire.api.app:app --reload

frontend:
	cd frontend && npm run dev
