# Lambdas — CLAUDE.md

## Structure

Each Lambda service is a directory under `lambdas/` with a `main.py` entry point.

### API Services (exposed via API Gateway)

| Service | Route | Docker | Memory | Description |
|---------|-------|--------|--------|-------------|
| `endpoints` | /endpoints | No | 256MB | Schema Registry CRUD |
| `serverless_ingestion` | /ingest | No | 256MB | Data ingestion via Firehose |
| `query_api` | /consumption | Yes | 5GB | DuckDB query engine |
| `transform_jobs` | /transform | Yes | 512MB | Gold layer job management |

### Background Services (event-driven, no API)

| Service | Trigger | Docker | Memory | Description |
|---------|---------|--------|--------|-------------|
| `serverless_processing` | S3 events | Yes | 5GB | Bronze processing |
| `serverless_processing_iceberg` | S3 events | Yes | 5GB | Bronze→Silver (Iceberg) |
| `serverless_analytics` | EventBridge | Yes | 5GB | Analytics jobs |
| `serverless_xtable` | Async invoke | Yes | 5GB | Delta→Iceberg (disabled) |

## Pattern

All Lambdas follow the same pattern:

```python
from fastapi import FastAPI
from mangum import Mangum

app = FastAPI(title="Service Name")
# ... routes ...
handler = Mangum(app)
```

## Adding a New Lambda

1. Create `lambdas/<name>/main.py` with FastAPI app + Mangum handler.
2. If Docker: add `Dockerfile` + `requirements.txt` in the same directory.
3. If Layer-based: reference existing layers from `layers/`.
4. Register in `stack/serverless_data_lake_stack.py`:
   - `API_SERVICES` dict for API-exposed services
   - `BACKGROUND_SERVICES` dict for event-driven services

## Shared Code

Non-Docker Lambdas import from Lambda Layers:
- `from shared.models import EndpointSchema, ...` (layers/shared)
- `from shared.schema_registry import SchemaRegistry` (layers/shared)
- `from shared.infrastructure import ...` (layers/shared)

Docker Lambdas bundle their own dependencies via Dockerfile.
