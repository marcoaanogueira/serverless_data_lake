<p align="center">
  <img src="./assets/images/logo.png" alt="Logo" width="200"/>
</p>

# Serverless Lakehouse with DuckDB, Polars, and Delta-rs on AWS

This Serverless Lakehouse architecture leverages DuckDB, Polars, Delta-rs, and FastAPI to process data economically and efficiently, without the need for tools like Spark or complex EMR clusters. Using a simple ingestion pipeline through FastAPI and Kinesis Firehose, data is stored in the **bronze layer**, processed in a Lambda function that utilizes DuckDB, Polars, and Delta-rs to perform merges, schema evolution and save it at **silver layers**, and finally transformed into the **gold layer** using EventBridge. The goal of this solution is to build a Lakehouse that eliminates unnecessary costs and complexity, offering a robust and scalable approach for most data processing scenarios.

## Architecture

This solution follows the medallion architecture with three main layers:

* **Bronze**: Raw data is ingested and stored here after being sent through an API built with FastAPI and buffered by Kinesis Firehose. This layer holds the unprocessed data in its original form.
* **Silver**: The data is processed in Lambda using DuckDB, with Polars providing smooth integration between libraries, and Delta-rs handling merges and schema evolution based on primary keys. The processed data is stored in Delta format.
* **Gold**: The final transformation occurs in this layer using EventBridge and DuckDB, providing optimized and cleaned data ready for consumption by analytics tools.

## How to execute this project

Create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To deploy the infrastructure to AWS, run:
```
$ cdk deploy
```

## How the Architecture Works

<p align="center">
  <img src="./assets/images/architecture.png" alt="Logo"/>
</p>

* **Ingestion**: Data is ingested via the FastAPI and buffered through Kinesis Firehose.
* **Processing**: Data is processed in AWS Lambda using DuckDB and Polars for efficient data manipulation.
* **Storage**: Delta-rs is used for data merging and schema evolution, ensuring that data remains up-to-date in the silver and gold layers.
* **Transformation**: EventBridge triggers final transformations, storing the refined data in the gold layer for consumption.
* **Consumer API**: Then a API is build, so we can consume this data

## API Reference

All APIs are served through a single API Gateway and use FastAPI with Mangum for Lambda integration. CORS is enabled on all endpoints.

### 1. Endpoints API (`/endpoints`) — Schema Registry

Manages ingestion endpoint schemas. When an endpoint is created, a corresponding Kinesis Firehose delivery stream is provisioned automatically.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Health check |
| POST | `/endpoints` | Create a new ingestion endpoint |
| GET | `/endpoints` | List all endpoints (query params: `domain`, `order_by`) |
| GET | `/endpoints/{domain}/{name}` | Get a specific endpoint (query param: `version`) |
| PUT | `/endpoints/{domain}/{name}` | Update endpoint schema (creates new version) |
| DELETE | `/endpoints/{domain}/{name}` | Delete endpoint and all its versions |
| GET | `/endpoints/{domain}/{name}/versions` | List all schema versions |
| GET | `/endpoints/{domain}/{name}/yaml` | Get raw YAML schema definition |
| GET | `/endpoints/{domain}/{name}/download` | Get presigned URL to download YAML schema |
| POST | `/endpoints/infer` | Infer schema from a sample JSON payload |

**Request — `POST /endpoints`**
```json
{
  "name": "my_table",
  "domain": "sales",
  "mode": "MANUAL",
  "columns": [
    {
      "name": "order_id",
      "type": "integer",
      "required": true,
      "primary_key": true,
      "description": "Unique order identifier"
    }
  ],
  "description": "Sales orders endpoint"
}
```

**Response**
```json
{
  "id": "sales/my_table",
  "name": "my_table",
  "domain": "sales",
  "version": 1,
  "mode": "MANUAL",
  "endpoint_url": "https://<api>/ingest/sales/my_table",
  "schema_url": "https://<api>/endpoints/sales/my_table/download",
  "status": "active",
  "created_at": "2025-01-01T00:00:00Z",
  "updated_at": "2025-01-01T00:00:00Z"
}
```

**Schema modes:** `MANUAL` (user defines columns), `AUTO_INFERENCE` (inferred from first payload), `SINGLE_COLUMN` (raw payload stored as-is).

**Column types:** `string`, `integer`, `float`, `boolean`, `timestamp`, `date`, `json`, `array`, `decimal`.

---

### 2. Ingestion API (`/ingest`) — Data Ingestion

Receives data records, validates them against the endpoint schema, and sends them to Kinesis Firehose for landing in the bronze layer.

| Method | Path | Description |
|--------|------|-------------|
| POST | `/ingest/{domain}/{endpoint_name}` | Ingest a single record |
| POST | `/ingest/{domain}/{endpoint_name}/batch` | Ingest multiple records |

**Query parameters:**
- `validate` (bool, default: `true`) — validate payload against schema
- `strict` (bool, default: `false`) — reject the request if validation fails

**Request — single record**
```json
{
  "data": {
    "order_id": 123,
    "product": "Widget",
    "amount": 49.90
  }
}
```

**Request — batch**
```json
[
  {"order_id": 123, "product": "Widget", "amount": 49.90},
  {"order_id": 124, "product": "Gadget", "amount": 29.90}
]
```

**Response — single**
```json
{
  "status": "sent",
  "endpoint": "sales/my_table",
  "records_sent": 1,
  "validated": true
}
```

**Response — batch**
```json
{
  "status": "completed",
  "endpoint": "sales/my_table",
  "total_records": 2,
  "validated_count": 2,
  "sent_count": 2
}
```

Metadata fields `_insert_date`, `_domain`, and `_endpoint` are automatically added to each record.

---

### 3. Query API (`/consumption`) — Data Consumption

Executes SQL queries against the data lake using DuckDB, with transparent table reference rewriting.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/consumption/query` | Execute a SQL query |
| GET | `/consumption/tables` | List all available silver tables |

**Query parameters (for `/consumption/query`):**
- `sql` (string, required) — SQL query to execute

**Table reference syntax in queries:**

| Reference | Resolves to |
|-----------|-------------|
| `domain.bronze.table` | `read_json_auto('s3://bucket/firehose-data/domain/table/**')` |
| `domain.silver.table` | Glue Iceberg catalog `domain_silver.table` |
| `domain.gold.table` | Glue Iceberg catalog `domain_gold.table` |

**Example**
```
GET /consumption/query?sql=SELECT * FROM sales.silver.my_table LIMIT 10
```

**Response**
```json
{
  "data": [
    {"order_id": 123, "product": "Widget", "amount": 49.90}
  ],
  "row_count": 1
}
```

---

### 4. Transform Jobs API (`/transform`) — Gold Layer Transforms

CRUD for transform job definitions and triggering executions via Step Functions + ECS Fargate (dbt runner).

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Health check |
| POST | `/transform/jobs` | Create a new transform job |
| GET | `/transform/jobs` | List all jobs (query params: `domain`, `order_by`) |
| GET | `/transform/jobs/{domain}/{job_name}` | Get a specific job |
| PUT | `/transform/jobs/{domain}/{job_name}` | Update a transform job |
| DELETE | `/transform/jobs/{domain}/{job_name}` | Delete a transform job |
| POST | `/transform/jobs/{domain}/{job_name}/run` | Trigger job execution |
| GET | `/transform/executions/{execution_id}` | Get execution status |

**Request — `POST /transform/jobs`**
```json
{
  "domain": "sales",
  "job_name": "daily_revenue",
  "query": "SELECT date, SUM(amount) as revenue FROM sales.silver.orders GROUP BY date",
  "write_mode": "overwrite",
  "unique_key": "date",
  "schedule_type": "cron",
  "cron_schedule": "0 2 * * ? *"
}
```

**Response**
```json
{
  "id": "sales/daily_revenue",
  "domain": "sales",
  "job_name": "daily_revenue",
  "query": "SELECT date, SUM(amount) as revenue FROM sales.silver.orders GROUP BY date",
  "write_mode": "overwrite",
  "unique_key": "date",
  "schedule_type": "cron",
  "cron_schedule": "0 2 * * ? *",
  "status": "active",
  "created_at": "2025-01-01T00:00:00Z",
  "updated_at": "2025-01-01T00:00:00Z"
}
```

**Trigger execution — `POST /transform/jobs/{domain}/{job_name}/run`**
```json
{
  "execution_id": "arn:aws:states:...:execution:...",
  "job_name": "daily_revenue",
  "domain": "sales",
  "status": "RUNNING",
  "started_at": "2025-01-01T02:00:00Z"
}
```

**Write modes:** `overwrite`, `append`.
**Schedule types:** `cron` (time-based), `dependency` (triggered by upstream jobs).

---

### Background Services (event-driven, no API routes)

These services are not exposed through the API Gateway but are triggered by events:

| Service | Trigger | Description |
|---------|---------|-------------|
| **Processing (Iceberg)** | S3 event (object created in bronze bucket) | Reads raw data from bronze, applies schema, performs merge/upsert, and writes to silver layer as Iceberg tables via Glue catalog |
| **Analytics** | EventBridge scheduled rules | Runs pre-configured SQL analytics jobs on a schedule and writes results to the gold layer |

### Transform Pipeline (Step Functions + ECS Fargate)

Gold layer transformations run on ECS Fargate via Step Functions with a dbt runner container:

- **Single mode** — triggered via `POST /transform/jobs/{domain}/{job_name}/run`, executes one job
- **Scheduled mode** — triggered by EventBridge on preset schedules (`hourly`, `daily` at 2 AM UTC, `monthly` at 3 AM UTC on the 1st), runs all jobs matching the schedule tag