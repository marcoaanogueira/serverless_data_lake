# Architecture

## Data flow

```
REST API / Push
     │
     ▼
POST /ingest/{domain}/{table}
     │
     │  (Pydantic validation, optional)
     ▼
Kinesis Data Firehose
     │
     │  (buffered delivery)
     ▼
S3 Bronze  ──────────────────── raw JSONL, partitioned by domain/table
     │
     │  (S3 event trigger)
     ▼
Lambda: serverless_processing_iceberg
     │
     │  (dedup by PK, schema from registry)
     ▼
S3 Silver (Apache Iceberg)  ──── Glue Catalog, domain_silver namespace
     │
     │  (Step Functions + ECS Fargate)
     ▼
dbt transform jobs
     │
     ▼
S3 Gold (Apache Iceberg)  ─────  Glue Catalog, domain_gold namespace
     │
     ▼
DuckDB / Lambda  ◄──────────────  GET /consumption/query
```

---

## Layers

### Bronze

- **Storage:** S3
- **Format:** JSONL (one JSON object per line)
- **Path:** `s3://{bronze_bucket}/firehose-data/{domain}/{table}/`
- **Metadata injected:** `_insert_date`, `_domain`, `_endpoint`
- **Retention:** raw, unmodified — source of truth

### Silver

- **Storage:** S3
- **Format:** Apache Iceberg (via Glue Catalog)
- **Namespace:** `{domain}_silver`
- **Table:** `{endpoint_name}`
- **Dedup:** automatic upsert if primary keys are defined, append-only otherwise
- **Catalog:** AWS Glue (queryable from Athena, Spark, DuckDB)

### Gold

- **Storage:** S3
- **Format:** Apache Iceberg
- **Namespace:** `{domain}_gold`
- **Produced by:** dbt models running on ECS Fargate
- **Scheduling:** cron-based or dependency-driven via Step Functions

---

## Lambda services

| Service | Type | Memory | Timeout | Trigger |
|---|---|---|---|---|
| `auth` | Non-Docker | 128MB | 30s | API Gateway |
| `authorizer` | Non-Docker | 128MB | 10s | API Gateway (authorizer) |
| `endpoints` | Non-Docker (Layers) | 256MB | 30s | API Gateway |
| `serverless_ingestion` | Non-Docker (Layers) | 256MB | 30s | API Gateway |
| `query_api` | Docker | 5GB | 900s | API Gateway |
| `transform_jobs` | Docker | 512MB | 30s | API Gateway |
| `ingestion_plans` | Docker | 512MB | 30s | API Gateway |
| `ingestion_agent` | Docker | 1GB | 900s | API Gateway |
| `transformation_agent` | Docker | 512MB | 900s | API Gateway |
| `chat_api` | Docker | 512MB | 120s | API Gateway |
| `serverless_processing_iceberg` | Docker | 5GB | 900s | S3 event |
| `serverless_analytics` | Docker | 5GB | 900s | EventBridge |

**Non-Docker Lambdas** use shared Lambda Layers (`layers/shared/`, `layers/utils/`).
**Docker Lambdas** have their own `Dockerfile` and `requirements.txt`. Layers are not compatible with Docker Lambdas.

---

## Why ECS Fargate for transforms?

Lambda has a hard 15-minute timeout. dbt jobs on large datasets routinely exceed this. ECS Fargate Spot is used instead — still serverless (no persistent instances), but with no timeout constraint.

The transform job flow:
1. `POST /transform/jobs/{domain}/{job}/run` → Lambda starts a Step Functions execution
2. Step Functions invokes ECS Fargate task
3. Fargate runs dbt with the job config fetched from S3
4. Results written to S3 Gold as Iceberg tables
5. Execution status queryable via `GET /transform/executions/{id}`

---

## Schema registry

All schema metadata is stored in S3 (not a database). This keeps the system fully serverless and makes schemas version-controlled by default.

```
s3://{schema_bucket}/
├── schemas/
│   └── {domain}/
│       ├── bronze/
│       │   └── {table_name}/
│       │       ├── v1.yaml
│       │       ├── v2.yaml
│       │       └── latest.yaml       ← always points to current
│       ├── silver/
│       │   └── {table_name}/
│       │       └── latest.yaml       ← registered after first processing run
│       └── gold/
│           └── {job_name}/
│               └── config.yaml
├── {tenant}/
│   └── ingestion_plans/
│       └── {plan_name}/
│           └── config.yaml
└── jobs/
    ├── ingestion/
    │   └── {job_id}.json             ← async job status
    └── transformation/
        └── {job_id}.json
```

---

## Authentication

### Request flow

```
Client
  │
  ├─► POST /auth/login  ──► Secrets Manager (credentials check)
  │        │
  │        └── returns API key
  │
  └─► Any other request
        │  x-api-key: <token>
        ▼
      Lambda Authorizer
        │  validates against Secrets Manager
        ▼
      Actual Lambda handler
```

### Secrets Manager secrets

| Secret | Content |
|---|---|
| `auth-credentials` | `{email, password_hash, salt}` — PBKDF2-HMAC-SHA256 (260k iterations) |
| `api-key` | The API key string |
| `/data-lake/ingestion/{plan_name}/oauth2` | OAuth2 credentials per ingestion plan |

### OIDC readiness

The auth layer is abstracted to support OIDC/SSO in the future (e.g., Supabase, Cognito). The `authorizer` Lambda validates `x-api-key` today but the interface is designed to swap in JWT validation without changing the API Gateway config.

---

## CDK stack patterns

Services are declared as dictionaries in `stack/serverless_data_lake_stack.py`:

```python
API_SERVICES = {
  "endpoints": ApiServiceConfig(
    name="endpoints",
    route_key="ANY /endpoints/{proxy+}",
    memory_size=256,
    timeout=30,
    layers=[shared_layer, utils_layer],
    grant_read=[schema_bucket],
    grant_write=[schema_bucket],
  ),
  ...
}
```

`ApiServiceConfig` (Pydantic model in `stack/constructs/api_service.py`) handles:
- Lambda function creation
- API Gateway route attachment
- IAM permission grants
- Layer vs Docker mutual exclusion validation
- Environment variable injection

---

## S3 bucket layout

| Bucket | Purpose |
|---|---|
| `bronze_bucket` | Raw ingested data from Firehose |
| `silver_bucket` | Iceberg tables for Silver layer |
| `schema_bucket` | Schemas, job configs, ingestion plans, agent job status |

---

## AI integration

All AI calls go through **Amazon Bedrock** (Claude) via the **Strands** agent framework.

- **Ingestion Agent:** multi-phase chain — plan generation → endpoint sampling → PK detection → description enrichment
- **Transform Agent:** reads Silver schema metadata, generates dbt YAML models
- **Chat Agent:** text-to-SQL with session memory, table catalog context, and query execution loop

Long-running agent jobs (ingestion and transformation) use **async self-invocation**: the Lambda invokes itself asynchronously and writes progress to S3, which the client polls via a job ID.
