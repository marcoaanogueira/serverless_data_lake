# API Reference

All endpoints require the `x-api-key` header except `POST /auth/login`.

```
Base URL: https://<api-gateway-id>.execute-api.<region>.amazonaws.com
```

---

## Authentication

### `POST /auth/login`

Authenticate with email and password. Returns an API key to use in subsequent requests.

**Request**
```json
{
  "email": "admin@mycompany.com",
  "password": "your_password"
}
```

**Response `200`**
```json
{
  "token": "abc123..."
}
```

Use the returned `token` as the `x-api-key` header on all other requests.

---

## Endpoints (Schema Registry)

Endpoints define the schema for a data source. Creating an endpoint automatically provisions a Kinesis Firehose stream.

### `GET /endpoints`

List all registered endpoints.

**Query params**
| Param | Type | Description |
|---|---|---|
| `domain` | string | Filter by domain |

**Response `200`**
```json
[
  {
    "id": "ecommerce/orders",
    "name": "orders",
    "domain": "ecommerce",
    "version": 1,
    "mode": "MANUAL",
    "endpoint_url": "https://.../ingest/ecommerce/orders",
    "schema_url": "s3://bucket/schemas/ecommerce/bronze/orders/latest.yaml",
    "status": "active",
    "created_at": "2024-01-15T10:00:00",
    "updated_at": "2024-01-15T10:00:00"
  }
]
```

---

### `POST /endpoints`

Create a new endpoint and provision its Firehose stream.

**Request**
```json
{
  "name": "orders",
  "domain": "ecommerce",
  "description": "Order records from the e-commerce platform",
  "mode": "MANUAL",
  "columns": [
    {"name": "id",          "type": "INTEGER",   "required": true,  "primary_key": true},
    {"name": "customer_id", "type": "INTEGER",   "required": true,  "primary_key": false},
    {"name": "total",       "type": "FLOAT",     "required": false, "primary_key": false},
    {"name": "created_at",  "type": "TIMESTAMP", "required": false, "primary_key": false}
  ]
}
```

**Supported types:** `STRING`, `VARCHAR`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `TIMESTAMP`, `DATE`, `JSON`, `ARRAY`, `DECIMAL`

**Modes:** `MANUAL` (explicit columns), `AUTO_INFERENCE` (infer from payload), `SINGLE_COLUMN` (one raw JSON column)

**Response `201`**
```json
{
  "id": "ecommerce/orders",
  "name": "orders",
  "domain": "ecommerce",
  "version": 1,
  "endpoint_url": "https://.../ingest/ecommerce/orders",
  ...
}
```

---

### `GET /endpoints/{domain}/{name}`

Get a specific endpoint schema.

---

### `PUT /endpoints/{domain}/{name}`

Update an endpoint (creates a new version, previous versions are kept).

**Request** — same shape as `POST /endpoints`

---

### `DELETE /endpoints/{domain}/{name}`

Delete an endpoint and all its versions. Also removes the Firehose stream.

---

### `GET /endpoints/{domain}/{name}/versions`

List all versions of an endpoint.

**Response `200`**
```json
[1, 2, 3]
```

---

### `GET /endpoints/{domain}/{name}/yaml`

Get the raw YAML schema for the latest version.

---

### `GET /endpoints/{domain}/{name}/download`

Get a presigned S3 URL to download the schema YAML.

---

### `POST /endpoints/infer`

Infer a schema from a JSON payload. Useful for quickly creating endpoints from a sample record.

**Request**
```json
{
  "payload": {
    "id": 1,
    "name": "John",
    "active": true,
    "score": 9.5,
    "created_at": "2024-01-15T10:00:00"
  }
}
```

**Response `200`**
```json
{
  "columns": [
    {"name": "id",         "type": "INTEGER",   "inferred_from": 1},
    {"name": "name",       "type": "STRING",    "inferred_from": "John"},
    {"name": "active",     "type": "BOOLEAN",   "inferred_from": true},
    {"name": "score",      "type": "FLOAT",     "inferred_from": 9.5},
    {"name": "created_at", "type": "TIMESTAMP", "inferred_from": "2024-01-15T10:00:00"}
  ],
  "payload_keys": ["id", "name", "active", "score", "created_at"]
}
```

---

## Ingestion

### `POST /ingest/{domain}/{endpoint_name}`

Ingest a single record. The record is validated against the registered schema (if `validate=true`) and forwarded to Kinesis Firehose.

**Query params**
| Param | Type | Default | Description |
|---|---|---|---|
| `validate` | bool | false | Validate against schema before sending |

**Request**
```json
{
  "data": {
    "id": 1,
    "customer_id": 42,
    "total": 99.90,
    "created_at": "2024-01-15T10:00:00"
  }
}
```

**Response `200`**
```json
{
  "status": "ok",
  "endpoint": "ecommerce/orders",
  "records_sent": 1,
  "validated": false
}
```

---

### `POST /ingest/{domain}/{endpoint_name}/batch`

Ingest multiple records in one call.

**Query params**
| Param | Type | Default | Description |
|---|---|---|---|
| `validate` | bool | false | Validate each record |
| `strict` | bool | false | Reject entire batch if any record fails validation |

**Request**
```json
{
  "records": [
    {"id": 1, "customer_id": 42, "total": 99.90},
    {"id": 2, "customer_id": 43, "total": 150.00}
  ]
}
```

**Response `200`**
```json
{
  "status": "ok",
  "endpoint": "ecommerce/orders",
  "total_records": 2,
  "validated_count": 2,
  "sent_count": 2,
  "failed_count": 0
}
```

---

## Ingestion Plans

Ingestion plans are AI-generated configurations that describe which API endpoints to pull and how to ingest them.

### `GET /ingestion/plans`

List all ingestion plans.

### `GET /ingestion/plans/{plan_name}`

Get a specific plan. OAuth2 credentials are redacted in the response.

### `POST /ingestion/plans`

Create or update a plan.

**Request**
```json
{
  "plan_name": "ecommerce_sync",
  "domain": "ecommerce",
  "tags": ["hourly", "production"],
  "plan": {
    "endpoints": [
      {"path": "/orders", "method": "GET", "params": {"page_size": 100}}
    ]
  },
  "oauth2": {
    "token_url": "https://api.myservice.com/oauth/token",
    "client_id": "client_id",
    "client_secret": "client_secret",
    "username": "user",
    "password": "pass"
  }
}
```

OAuth2 credentials are stored in Secrets Manager, never in S3.

### `DELETE /ingestion/plans/{plan_name}`

Delete a plan and its associated OAuth2 secret.

### `POST /ingestion/plans/{plan_name}/run`

Trigger an immediate execution of the plan via Step Functions.

**Response `200`**
```json
{
  "execution_id": "ecommerce_sync-20240115-100000",
  "status": "RUNNING"
}
```

---

## Transform Jobs

### `GET /transform/jobs`

List all transform jobs.

**Query params**
| Param | Type | Description |
|---|---|---|
| `domain` | string | Filter by domain |

### `POST /transform/jobs`

Create a new transform job.

**Request**
```json
{
  "domain": "ecommerce",
  "job_name": "daily_revenue",
  "query": "SELECT DATE(created_at) as day, SUM(total) as revenue FROM ecommerce.silver.orders GROUP BY 1",
  "write_mode": "overwrite",
  "unique_key": "day",
  "schedule_type": "cron",
  "cron_schedule": "0 6 * * *",
  "status": "active"
}
```

**`schedule_type` options:**
- `cron` — runs on the specified cron expression. Requires `cron_schedule`.
- `dependency` — runs when upstream tables are updated. Requires `dependencies` (list of `domain.layer.table` strings).

**`write_mode` options:** `overwrite`, `append`

### `GET /transform/jobs/{domain}/{job_name}`

Get a specific job config.

### `PUT /transform/jobs/{domain}/{job_name}`

Update a job.

### `DELETE /transform/jobs/{domain}/{job_name}`

Delete a job and its S3 config.

### `POST /transform/jobs/{domain}/{job_name}/run`

Trigger an immediate execution via Step Functions.

### `GET /transform/executions/{execution_id}`

Poll execution status.

**Response `200`**
```json
{
  "execution_id": "daily_revenue-20240115-060000",
  "job_name": "daily_revenue",
  "domain": "ecommerce",
  "status": "SUCCEEDED",
  "started_at": "2024-01-15T06:00:00"
}
```

---

## Query

### `GET /consumption/query`

Execute a SQL query against Bronze, Silver, or Gold tables.

**Query params**
| Param | Type | Description |
|---|---|---|
| `sql` | string | SQL SELECT statement |

**Table naming conventions:**
| Reference | Maps to |
|---|---|
| `domain.bronze.table` | Raw JSONL files in S3 |
| `domain.silver.table` | Iceberg table in Glue Catalog |
| `domain.gold.table` | Iceberg table in Glue Catalog |

**Example**
```
GET /consumption/query?sql=SELECT * FROM ecommerce.silver.orders LIMIT 10
```

**Response `200`**
```json
{
  "data": [
    {"id": 1, "customer_id": 42, "total": 99.90, "created_at": "2024-01-15T10:00:00"}
  ],
  "row_count": 1,
  "truncated": false
}
```

**Security:** Only `SELECT` and `WITH` statements are allowed. Results are capped at 10,000 rows.

---

### `GET /consumption/tables`

List all available tables across all layers.

**Response `200`**
```json
{
  "tables": [
    {"domain": "ecommerce", "layer": "silver", "name": "orders", "location": "s3://..."},
    {"domain": "ecommerce", "layer": "gold",   "name": "daily_revenue", "location": "s3://..."}
  ],
  "count": 2
}
```

---

## AI Agents

### Ingestion Agent

#### `POST /agent/ingestion/plan`

Generate an ingestion plan synchronously from an OpenAPI spec. Does not save or execute.

**Request**
```json
{
  "openapi_url": "https://api.myservice.com/openapi.json",
  "interests": ["orders", "customers", "products"],
  "token": "Bearer eyJ...",
  "base_url": "https://api.myservice.com"
}
```

`interests` is a list of business concepts. The agent semantically matches them to API endpoints — it will find `GET /pessoas` even if you listed `customers`.

**Response `200`** — the generated plan object.

---

#### `POST /agent/ingestion/run`

Generate a plan, save it to S3, and optionally trigger execution. Runs asynchronously.

**Request** — same as `/plan`, plus:
```json
{
  "domain": "ecommerce",
  "plan_name": "ecommerce_sync",
  "tags": ["hourly"]
}
```

**Response `202`**
```json
{
  "job_id": "abc123",
  "status": "running",
  "poll_url": "/agent/ingestion/jobs/abc123"
}
```

---

#### `GET /agent/ingestion/jobs/{job_id}`

Poll an async ingestion agent job.

**Response `200`**
```json
{
  "job_id": "abc123",
  "status": "completed",
  "result": { ... }
}
```

`status` values: `running`, `completed`, `failed`

---

### Transform Agent

#### `POST /agent/transformation/plan`

Generate dbt model definitions for the Gold layer from Silver table metadata.

**Request**
```json
{
  "domain": "ecommerce",
  "tables": ["orders", "customers"]
}
```

**Response `200`** — list of generated job configurations.

---

#### `POST /agent/transformation/run`

Generate jobs, save them, and optionally trigger execution immediately.

**Request** — same as `/plan`, plus:
```json
{
  "trigger_execution": true
}
```

**Response `202`**
```json
{
  "job_id": "xyz789",
  "status": "running",
  "poll_url": "/agent/transformation/jobs/xyz789"
}
```

---

#### `GET /agent/transformation/jobs/{job_id}`

Poll an async transformation agent job.

---

### Chat (Analyze Agent)

#### `POST /chat/message`

Send a natural language message. The agent translates it to SQL, executes it, and returns the result.

**Request**
```json
{
  "session_id": "optional-existing-session-id",
  "message": "What was the total revenue per day last week?"
}
```

**Response `200`**
```json
{
  "session_id": "sess_abc123",
  "message_id": "msg_xyz",
  "content": [
    {
      "type": "text",
      "text": "Here are the daily revenues for last week:"
    },
    {
      "type": "tool_result",
      "data": [
        {"day": "2024-01-08", "revenue": 12450.00},
        {"day": "2024-01-09", "revenue": 9800.00}
      ]
    }
  ]
}
```

---

#### `GET /chat/sessions`

List all chat sessions.

#### `GET /chat/sessions/{session_id}`

Get a session with its full message history.

#### `DELETE /chat/sessions/{session_id}`

Delete a session and all its messages.

---

## Error responses

All endpoints return standard error shapes:

```json
{
  "detail": "Endpoint ecommerce/orders not found"
}
```

| Status | Meaning |
|---|---|
| `400` | Bad request — missing or invalid fields |
| `401` | Unauthorized — missing or invalid API key |
| `404` | Resource not found |
| `422` | Validation error — Pydantic rejected the payload |
| `500` | Internal server error |
