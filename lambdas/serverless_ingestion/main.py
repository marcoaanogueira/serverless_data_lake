"""
Data Ingestion API

Receives data payloads and sends them to Kinesis Firehose for storage in the data lake.
Validates payloads against schemas defined in the endpoint registry.
"""

import os
import sys
import json
from datetime import datetime
from typing import Any

import boto3
from fastapi import FastAPI, HTTPException, Query
from mangum import Mangum
from pydantic import BaseModel

# Add endpoints module to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "endpoints"))

try:
    from schema_registry import SchemaRegistry
except ImportError:
    from lambdas.endpoints.schema_registry import SchemaRegistry


app = FastAPI(
    title="Data Lake Ingestion API",
    description="Ingest data into the data lake with schema validation",
    version="2.0.0",
)

# AWS Clients
firehose_client = boto3.client("firehose")

# Configuration
TENANT = os.environ.get("TENANT", "default")

# Schema registry instance
registry = SchemaRegistry()


class RawDataModel(BaseModel):
    """Request model for data ingestion"""
    data: dict[str, Any]


class IngestionResponse(BaseModel):
    """Response model for successful ingestion"""
    status: str
    endpoint: str
    records_sent: int
    validated: bool


def send_to_firehose(endpoint_name: str, data: dict[str, Any] | list[dict[str, Any]]):
    """Send data to Kinesis Firehose."""
    if isinstance(data, dict):
        data = [data]

    firehose_name = f"{TENANT.capitalize()}{endpoint_name.title().replace('_', '')}Firehose"

    for record in data:
        firehose_client.put_record(
            DeliveryStreamName=firehose_name,
            Record={"Data": json.dumps(record).encode("utf-8")},
        )


@app.post("/ingest/{domain}/{endpoint_name}", response_model=IngestionResponse)
async def ingest_data(
    domain: str,
    endpoint_name: str,
    payload: RawDataModel,
    validate: bool = Query(True, description="Whether to validate against schema"),
    strict: bool = Query(False, description="Reject payload if validation fails"),
):
    """
    Ingest data into the data lake.

    The data is validated against the endpoint schema (if validation is enabled)
    and then sent to Kinesis Firehose for storage.
    """
    # Get schema from registry
    schema = registry.get(domain, endpoint_name)
    if not schema:
        raise HTTPException(
            status_code=404,
            detail=f"Endpoint '{domain}/{endpoint_name}' not found. Create it first using POST /endpoints.",
        )

    data = payload.data
    validated = False

    if validate:
        validated_data, errors = schema.validate_payload(payload.data)

        if errors:
            if strict:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "error": "Payload validation failed",
                        "endpoint": f"{domain}/{endpoint_name}",
                        "validation_errors": errors,
                    },
                )
            # Non-strict mode: continue with original data
        else:
            data = validated_data
            validated = True

    # Add metadata
    data["_insert_date"] = datetime.now().isoformat()
    data["_domain"] = domain
    data["_endpoint"] = endpoint_name

    send_to_firehose(endpoint_name, data)

    return IngestionResponse(
        status="success",
        endpoint=f"{domain}/{endpoint_name}",
        records_sent=1,
        validated=validated,
    )


@app.post("/ingest/{domain}/{endpoint_name}/batch")
async def ingest_batch(
    domain: str,
    endpoint_name: str,
    records: list[dict[str, Any]],
    validate: bool = Query(True, description="Whether to validate against schema"),
    strict: bool = Query(False, description="Reject payload if validation fails"),
):
    """Ingest multiple records in a single request."""
    schema = registry.get(domain, endpoint_name)
    if not schema:
        raise HTTPException(
            status_code=404,
            detail=f"Endpoint '{domain}/{endpoint_name}' not found.",
        )

    validated_count = 0
    failed_count = 0
    errors = []

    for i, record in enumerate(records):
        record_data = record
        record_validated = False

        if validate:
            validated_data, validation_errors = schema.validate_payload(record)

            if validation_errors:
                if strict:
                    errors.append({"record_index": i, "errors": validation_errors})
                    failed_count += 1
                    continue
            else:
                record_data = validated_data
                record_validated = True
                validated_count += 1

        # Add metadata
        record_data["_insert_date"] = datetime.now().isoformat()
        record_data["_domain"] = domain
        record_data["_endpoint"] = endpoint_name

        send_to_firehose(endpoint_name, record_data)

    response = {
        "status": "completed",
        "endpoint": f"{domain}/{endpoint_name}",
        "total_records": len(records),
        "validated_count": validated_count,
        "sent_count": len(records) - failed_count,
    }

    if strict and failed_count > 0:
        response["failed_count"] = failed_count
        response["errors"] = errors

    return response


handler = Mangum(app, lifespan="off")
