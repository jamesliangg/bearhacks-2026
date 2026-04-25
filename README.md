# VIA Delay Oracle — MVP

Predicts VIA Rail delays and overlays them on the booking page.

## Components
- `services/common` — shared storage/schemas/features (Snowflake or SQLite fallback)
- `services/ingestion` — FastAPI, TransitDocs + VIA live scrapers, APScheduler
- `services/training` — FastAPI, feature pipeline, GradientBoosting model, registry
- `services/prediction` — FastAPI `/predict` consumed by the extension
- `extension` — WXT (Chrome/Firefox) content script + popup
- `infra/snowflake` — SQL DDL for RAW / STAGING / MART
- `infra/docker-compose.yml` — run all three services locally

## Quick start (local, no Snowflake)

```bash
cd infra
docker compose up --build

# Ingest one day of train 67
curl -X POST http://localhost:8001/scrape/historical/67/2026-04-25

# Train a model (uses scraped data; falls back to synthetic if < 20 rows)
curl -X POST http://localhost:8002/train

# Predict
curl -X POST http://localhost:8003/predict \
  -H 'Content-Type: application/json' \
  -d '{"items":[{"train_number":"67","service_date":"2026-04-25"}]}'
```

## Extension

```bash
cd extension
pnpm install   # or npm/yarn
pnpm dev       # loads unpacked into Chromium via WXT
```

Open the booking results page on reservia.viarail.ca. Delay badges appear next
to each trip row. Use the popup to point the extension at a different backend
URL; it defaults to `http://localhost:8003`.

## Snowflake mode

1. Apply `infra/snowflake/001_raw.sql`, `002_staging.sql`, `003_mart.sql`.
2. Fill `infra/.env.example` values and set `USE_SNOWFLAKE=true`.
3. Restart the three services.
