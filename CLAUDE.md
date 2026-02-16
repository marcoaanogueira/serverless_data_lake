# CLAUDE.md

## Project Overview

Serverless Data Lake on AWS using the **medallion architecture** (Bronze → Silver → Gold).
Built with AWS CDK (Python), FastAPI Lambdas, React frontend, and DuckDB for queries.

## Architecture

```
Ingestion API → Kinesis Firehose → S3 Bronze → Processing Lambda → S3 Silver (Iceberg) → Transform (dbt/ECS) → S3 Gold
```

- **Bronze**: Raw data in S3, partitioned by domain/table
- **Silver**: Cleaned data in Iceberg tables (Glue Catalog)
- **Gold**: Transformed data via dbt jobs on ECS Fargate
- **Schema Registry**: YAML schemas in S3, managed via Endpoints API

## Key Patterns

### Declarative API Services
Services are defined in `stack/serverless_data_lake_stack.py` via `API_SERVICES` and `BACKGROUND_SERVICES` dicts.
The `ApiServiceConfig` (Pydantic model in `stack/constructs/api_service.py`) handles Lambda creation, API Gateway routing, and IAM permissions automatically.

### Lambda Structure
Every Lambda uses FastAPI + Mangum. Entry point is always `main.py` with `handler = Mangum(app)`.
- **Non-Docker Lambdas** (endpoints, ingestion): use Lambda Layers from `layers/`
- **Docker Lambdas** (query_api, processing, analytics): have their own `Dockerfile` + `requirements.txt`

### Shared Code
- `layers/shared/python/shared/models.py` — Pydantic models (EndpointSchema, ColumnDefinition, DataType, etc.)
- `layers/shared/python/shared/schema_registry.py` — S3-based schema CRUD
- `layers/shared/python/shared/infrastructure.py` — AWS service wrappers
- `layers/utils/` — General utilities

## Commands

```bash
# Tests (Python backend)
pytest tests/

# Lint
ruff check .

# CDK deploy
cdk deploy

# CDK synth (validate without deploying)
cdk synth

# Frontend
cd frontend && npm run dev      # dev server
cd frontend && npm run build    # production build
cd frontend && npm run test:run # tests (vitest)
cd frontend && npm run lint     # eslint
```

## Conventions

- Python naming: snake_case everywhere. Table names, domains, columns are all snake_case (validated by Pydantic).
- Each Lambda service lives in `lambdas/<service_name>/main.py`.
- FastAPI apps use `CORSMiddleware` with `allow_origins=["*"]`.
- Environment variables: `TENANT`, `TZ`, `API_GATEWAY_ENDPOINT` are injected by CDK.
- Config files: `artifacts/tables.yaml` defines tenant tables loaded at deploy time.
- Linter: ruff (configured in requirements-dev.txt).
- Tests: pytest, files in `tests/test_*.py`.

## Don't

- Don't modify `cdk.json` context flags without explicit review.
- Don't change IAM permissions (`grant_*` fields in ApiServiceConfig) without understanding blast radius.
- Don't add Lambda Layers to Docker-based Lambdas (mutually exclusive — validated by Pydantic).
- Don't hardcode AWS account/region — use `os.getenv("CDK_DEFAULT_ACCOUNT")`.
- Don't add dependencies to `requirements.txt` without checking if they're available in Lambda runtime or Docker image.
