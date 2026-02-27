# Getting Started

## Prerequisites

- Python 3.11+
- Node.js 18+
- AWS CLI configured (`aws configure`)
- AWS CDK CLI (`npm install -g aws-cdk`)
- Docker (for building Docker-based Lambdas)

---

## 1. Clone and install dependencies

```bash
git clone https://github.com/marcoaanogueira/serverless_data_lake
cd serverless_data_lake

# Python dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Frontend dependencies
cd frontend && npm install && cd ..
```

---

## 2. Configure your tenant

Set the tenant name in `cdk.json` under the `context` key:

```json
{
  "context": {
    "tenant": "my_company"
  }
}
```

This value becomes the `TENANT` environment variable injected into all Lambdas and is used to name S3 buckets and other AWS resources (`my_company-bronze`, `my_company-silver`, etc.).

Tables and schemas are managed at runtime through the Schema Registry API (see step 7).

---

## 3. Bootstrap CDK (first time only)

```bash
cdk bootstrap
```

---

## 4. Deploy

```bash
cdk synth   # validate, no deployment
cdk deploy  # deploy all stacks
```

The deploy outputs the API Gateway endpoint URL. Copy it — you'll need it for the frontend.

---

## 5. Create the first user

After deploy, create credentials in Secrets Manager. The auth service expects a secret with the following structure:

```json
{
  "email": "admin@mycompany.com",
  "password_hash": "<PBKDF2-HMAC-SHA256 hash>",
  "salt": "<hex salt>"
}
```

You can generate a hash with:

```python
import hashlib, secrets, binascii

salt = secrets.token_hex(32)
password = "your_password"
dk = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 260000)
print("salt:", salt)
print("hash:", binascii.hexlify(dk).decode())
```

---

## 6. Run the frontend

```bash
cd frontend
npm run dev   # starts at http://localhost:3000
```

The app will show the landing page. Click **View on GitHub** to go to the repo, or configure the API key in `src/api/dataLakeClient.js` to connect to your deployed stack.

---

## 7. Register your first endpoint (schema)

Using the UI or directly via API:

```bash
curl -X POST https://<api-gateway>/endpoints \
  -H "x-api-key: <your-api-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders",
    "domain": "ecommerce",
    "columns": [
      {"name": "id", "type": "INTEGER", "required": true, "primary_key": true},
      {"name": "customer_id", "type": "INTEGER", "required": true, "primary_key": false},
      {"name": "total", "type": "FLOAT", "required": false, "primary_key": false},
      {"name": "created_at", "type": "TIMESTAMP", "required": false, "primary_key": false}
    ],
    "mode": "MANUAL"
  }'
```

This automatically provisions a Kinesis Firehose stream and generates the ingestion endpoint URL.

---

## 8. Ingest data

```bash
# Single record
curl -X POST https://<api-gateway>/ingest/ecommerce/orders \
  -H "x-api-key: <your-api-key>" \
  -H "Content-Type: application/json" \
  -d '{"data": {"id": 1, "customer_id": 42, "total": 99.90, "created_at": "2024-01-15T10:00:00"}}'

# Batch
curl -X POST https://<api-gateway>/ingest/ecommerce/orders/batch \
  -H "x-api-key: <your-api-key>" \
  -H "Content-Type: application/json" \
  -d '{"records": [{"id": 1, ...}, {"id": 2, ...}]}'
```

Records go to Kinesis Firehose → S3 Bronze. The processing Lambda then picks them up and writes to Silver (Iceberg).

---

## 9. Run the AI Ingestion Agent

Point it at an OpenAPI spec and describe what you need:

```bash
curl -X POST https://<api-gateway>/agent/ingestion/plan \
  -H "x-api-key: <your-api-key>" \
  -H "Content-Type: application/json" \
  -d '{
    "openapi_url": "https://api.myservice.com/openapi.json",
    "interests": ["orders", "customers", "products"],
    "domain": "ecommerce"
  }'
```

The agent reads the spec, filters relevant endpoints semantically, samples data, and returns a ready-to-use ingestion plan. Use `/agent/ingestion/run` to save and execute it immediately.

---

## Running tests and linting

```bash
# Python tests
pytest tests/

# Python linting
ruff check .

# Frontend tests
cd frontend && npm run test:run

# Frontend linting
cd frontend && npm run lint
```

---

## Environment variables (injected by CDK)

| Variable | Description |
|---|---|
| `TENANT` | Tenant name from `cdk.json` context |
| `TZ` | Timezone |
| `API_GATEWAY_ENDPOINT` | API Gateway base URL (for inter-service calls) |
| `SCHEMA_BUCKET` | S3 bucket for schemas, plans, and job configs |
| `BRONZE_BUCKET` | S3 bucket for raw ingested data |
| `SILVER_BUCKET` | S3 bucket for Iceberg Silver tables |
| `STATE_MACHINE_ARN` | Step Functions ARN for transform jobs |
| `INGESTION_STATE_MACHINE_ARN` | Step Functions ARN for ingestion plans |
| `API_KEY_SECRET_ARN` | Secrets Manager ARN for the API key |
| `AUTH_CREDENTIALS_SECRET_ARN` | Secrets Manager ARN for user credentials |
