"""
Data Ingestion API

Receives data payloads and sends them to Kinesis Firehose for storage in the data lake.
Validates payloads against schemas defined in the endpoint registry.
"""

import os
import json
from datetime import datetime
from typing import Any

import boto3
from fastapi import FastAPI, HTTPException, Query
from mangum import Mangum
from pydantic import BaseModel

try:
    from schema_validator import SchemaValidator, SchemaNotFoundError, SchemaValidationError
except ImportError:
    from lambdas.serverless_ingestion.schema_validator import SchemaValidator, SchemaNotFoundError, SchemaValidationError

# Re-export for testing
__all__ = ["app", "handler", "SchemaValidator", "SchemaNotFoundError", "SchemaValidationError"]


app = FastAPI(
    title="Data Lake Ingestion API",
    description="Ingest data into the data lake with schema validation",
    version="2.0.0",
)

# AWS Clients
firehose_client = boto3.client("firehose")
s3_client = boto3.client("s3")

# Configuration
TENANT = os.environ.get("TENANT", "default")

# Schema validator instance
schema_validator = SchemaValidator()


class RawDataModel(BaseModel):
    """Request model for data ingestion"""
    data: dict[str, Any]


class IngestionResponse(BaseModel):
    """Response model for successful ingestion"""
    status: str
    endpoint: str
    records_sent: int
    validated: bool


class ValidationErrorResponse(BaseModel):
    """Response model for validation errors"""
    error: str
    endpoint: str
    validation_errors: list[dict]


def send_to_firehose(endpoint_name: str, data: dict[str, Any] | list[dict[str, Any]]):
    """
    Send data to Kinesis Firehose.

    Args:
        endpoint_name: Name of the endpoint (used to derive Firehose name)
        data: Single record or list of records to send
    """
    if isinstance(data, dict):
        data = [data]

    firehose_name = f"{TENANT.capitalize()}{endpoint_name.title().replace('_', '')}Firehose"

    for record in data:
        firehose_client.put_record(
            DeliveryStreamName=firehose_name,
            Record={"Data": json.dumps(record).encode("utf-8")},
        )


@app.get("/endpoints")
async def list_endpoints(domain: str | None = Query(None, description="Filter by domain")):
    """
    List all available endpoints.

    Returns a list of endpoints that can receive data.
    """
    endpoints = schema_validator.list_endpoints(domain)
    return {
        "endpoints": endpoints,
        "count": len(endpoints),
    }


@app.get("/endpoints/{domain}/{endpoint_name}")
async def get_endpoint_schema(domain: str, endpoint_name: str):
    """
    Get the schema for a specific endpoint.

    Useful for clients to understand what data format is expected.
    """
    try:
        schema = schema_validator.get_schema(domain, endpoint_name)
        return schema
    except SchemaNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post(
    "/ingest/{domain}/{endpoint_name}",
    response_model=IngestionResponse,
    responses={
        400: {"model": ValidationErrorResponse},
        404: {"description": "Endpoint not found"},
    },
)
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

    Args:
        domain: Business domain (e.g., sales, finance, ads)
        endpoint_name: Name of the endpoint/table
        payload: Data to ingest (wrapped in {"data": {...}})
        validate: Whether to validate the payload against the schema
        strict: If True, reject payloads that fail validation

    Returns:
        IngestionResponse with status and metadata
    """
    # Check if endpoint exists
    if not schema_validator.endpoint_exists(domain, endpoint_name):
        raise HTTPException(
            status_code=404,
            detail=f"Endpoint '{domain}/{endpoint_name}' not found. "
            f"Create it first using POST /endpoints.",
        )

    data = payload.data
    validated = False

    if validate:
        try:
            # Validate and potentially coerce the data
            data = schema_validator.validate(domain, endpoint_name, payload.data)
            validated = True
        except SchemaValidationError as e:
            if strict:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "error": "Payload validation failed",
                        "endpoint": f"{domain}/{endpoint_name}",
                        "validation_errors": e.errors,
                    },
                )
            # In non-strict mode, log warning but continue with original data
            data = payload.data
            validated = False

    # Add metadata
    data["_insert_date"] = datetime.now().isoformat()
    data["_domain"] = domain
    data["_endpoint"] = endpoint_name

    # Send to Firehose
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
    """
    Ingest multiple records in a single request.

    Args:
        domain: Business domain
        endpoint_name: Name of the endpoint/table
        records: List of records to ingest
        validate: Whether to validate payloads against the schema
        strict: If True, reject payloads that fail validation

    Returns:
        Summary of ingestion results
    """
    # Check if endpoint exists
    if not schema_validator.endpoint_exists(domain, endpoint_name):
        raise HTTPException(
            status_code=404,
            detail=f"Endpoint '{domain}/{endpoint_name}' not found.",
        )

    validated_count = 0
    failed_count = 0
    errors = []

    for i, record in enumerate(records):
        try:
            if validate:
                record = schema_validator.validate(domain, endpoint_name, record)
                validated_count += 1

            # Add metadata
            record["_insert_date"] = datetime.now().isoformat()
            record["_domain"] = domain
            record["_endpoint"] = endpoint_name

            send_to_firehose(endpoint_name, record)

        except SchemaValidationError as e:
            if strict:
                errors.append({
                    "record_index": i,
                    "errors": e.errors,
                })
                failed_count += 1
            else:
                # Send without validation in non-strict mode
                record["_insert_date"] = datetime.now().isoformat()
                record["_domain"] = domain
                record["_endpoint"] = endpoint_name
                send_to_firehose(endpoint_name, record)

    response = {
        "status": "completed",
        "endpoint": f"{domain}/{endpoint_name}",
        "total_records": len(records),
        "validated_count": validated_count,
    }

    if strict and failed_count > 0:
        response["failed_count"] = failed_count
        response["errors"] = errors

    return response


# Legacy endpoint for backward compatibility
@app.post("/send_data_bronze/{tenant}/{data_model_name}")
async def process_data_legacy(tenant: str, data_model_name: str, data_model: RawDataModel):
    """
    Legacy endpoint for backward compatibility.

    Deprecated: Use POST /ingest/{domain}/{endpoint_name} instead.
    """
    # For legacy support, use tenant as domain
    # Check if new-style endpoint exists first
    if schema_validator.endpoint_exists(tenant, data_model_name):
        return await ingest_data(
            domain=tenant,
            endpoint_name=data_model_name,
            payload=data_model,
            validate=True,
            strict=False,
        )

    # Fallback to old behavior - just send to Firehose without validation
    data = data_model.data
    data["insert_date"] = datetime.now().isoformat()

    firehose_name = f"{tenant.capitalize()}{data_model_name.title().replace('_', '')}Firehose"

    firehose_client.put_record(
        DeliveryStreamName=firehose_name,
        Record={"Data": json.dumps(data).encode("utf-8")},
    )

    return {"status": "success", "message": "Record sent to Firehose (legacy mode)"}


handler = Mangum(app, lifespan="off")
