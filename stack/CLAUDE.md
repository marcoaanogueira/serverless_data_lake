# Stack (CDK) — CLAUDE.md

## Structure

```
stack/
├── serverless_data_lake_stack.py   # Main stack (~730 lines)
└── constructs/
    ├── api_gateway.py              # API Gateway wrapper
    ├── api_service.py              # Lambda service abstraction (ApiServiceConfig + ApiService)
    └── static_website.py           # Frontend S3 + CloudFront hosting
```

## How Services Are Defined

Services are registered as dicts at the top of `serverless_data_lake_stack.py`:

```python
API_SERVICES: Dict[str, ApiServiceConfig] = {
    "endpoints": ApiServiceConfig(
        code_path="lambdas/endpoints",
        route="/endpoints",
        use_docker=False,
        layers=["Shared", "Utils"],
        ...
    ),
}

BACKGROUND_SERVICES: Dict[str, ApiServiceConfig] = { ... }
```

The `ApiServiceConfig` (Pydantic model) validates config at synth time. Key rules:
- `use_docker=True` and `layers` are **mutually exclusive** (validator enforces this).
- `enable_api=False` skips API Gateway route registration (for background services).
- Permissions are granted via boolean flags: `grant_s3_access`, `grant_firehose_access`, `grant_glue_access`, `grant_lambda_invoke`.

## Adding a New Service

1. Add entry to `API_SERVICES` or `BACKGROUND_SERVICES` dict.
2. The framework auto-creates: Lambda function, API Gateway route (if applicable), IAM permissions, CloudWatch logs.
3. For S3 triggers or EventBridge schedules, configure them in the stack's `__init__` method after service creation.

## Key Resources Created

- **S3 Buckets**: bronze, silver, gold, artifacts (per tenant)
- **API Gateway**: Single shared gateway, routes from all API services
- **Kinesis Firehose**: One stream per ingestion endpoint (created dynamically)
- **DynamoDB**: Metadata tables
- **Step Functions + ECS Fargate**: dbt transformation pipeline
- **Lambda Layers**: Shared, Ingestion, Utils, DuckDB
