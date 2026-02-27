# Tadpole — Serverless Data Lake

Tadpole is a serverless data lake platform built on AWS using the **medallion architecture** (Bronze → Silver → Gold). It combines AI agents for automated ingestion and transformation with a fully serverless infrastructure stack.

---

## What it does

| Layer | What happens |
|---|---|
| **Bronze** | Raw data lands in S3, partitioned by domain and table, exactly as received |
| **Silver** | Data is cleaned, deduplicated, and written as Apache Iceberg tables in Glue Catalog |
| **Gold** | Business-ready tables produced by dbt models, running on ECS Fargate |

---

## Core modules

### AI Agents

**Ingestion Agent**
Reads an OpenAPI or Swagger spec, filters endpoints that match your areas of interest (with semantic matching — it can map "customers" to "persons"), samples live data to detect primary keys, and enriches every field with AI-generated descriptions. Produces an ingestion plan ready to run.

**Transform Agent**
Takes the metadata produced by the Ingestion Agent (descriptions, PKs, domain context) and auto-generates dbt YAML model definitions for the Gold layer. No SQL required.

**Analyze Agent**
ChatBI-style text-to-SQL agent. Send a natural language question, get back valid SQL executed against your Bronze/Silver/Gold tables via DuckDB.

---

### Ingestion

Two modes:

- **Active ingestion** — DLT-powered pipelines running in Lambda. Pull from any REST API on a configurable schedule (hourly, daily, on-demand). Auto-upsert into Silver.
- **Passive ingestion** — Push data to a REST endpoint. Pydantic validates the payload against the registered schema. Primary key auto-detection handles deduplication automatically.

After any ingestion, metadata is written to S3 and the Silver layer is updated.

---

### Transformation

dbt runs on ECS Fargate (not Lambda) to avoid the 15-minute timeout limit. Transform jobs can be scheduled (hourly/daily/monthly) or dependency-driven (only runs when upstream tables are ready). YAML model definitions are generated dynamically and stored in S3.

---

### Query

A SQL editor organized by Bronze / Silver / Gold. Powered by DuckDB on Lambda — click any table to see its schema catalog. Results can be piped directly into the Analyze Agent.

---

## Tech stack

| Concern | Technology |
|---|---|
| Infrastructure | AWS CDK (Python) |
| APIs | FastAPI + Mangum + API Gateway |
| Compute | AWS Lambda + ECS Fargate |
| Storage | S3 (data, schemas, configs) |
| Table format | Apache Iceberg (Silver) |
| Transformations | dbt |
| Query engine | DuckDB |
| AI | Amazon Bedrock (Claude) via Strands |
| Schema catalog | AWS Glue |
| Streaming | Kinesis Data Firehose |
| Auth | Secrets Manager + OIDC-ready |
| Frontend | React + Vite + Tailwind CSS |

---

## Repository structure

```
serverless_data_lake/
├── stack/                   # CDK stack definition
│   └── constructs/          # ApiServiceConfig and helpers
├── lambdas/                 # One directory per Lambda service
│   ├── auth/
│   ├── authorizer/
│   ├── endpoints/
│   ├── serverless_ingestion/
│   ├── query_api/
│   ├── transform_jobs/
│   ├── ingestion_plans/
│   ├── ingestion_agent/
│   ├── transformation_agent/
│   ├── chat_api/
│   ├── serverless_processing_iceberg/
│   └── serverless_analytics/
├── layers/
│   └── shared/python/shared/ # Shared Pydantic models + schema registry
├── frontend/                # React app
├── artifacts/               # tables.yaml tenant config
└── docs/                    # This documentation
```

---

## Further reading

- [Getting Started](./getting-started.md)
- [API Reference](./api-reference.md)
- [Architecture](./architecture.md)
