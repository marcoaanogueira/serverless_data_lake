"""
Data Ingestion API

Receives data payloads and sends them to Kinesis Firehose for storage in the data lake.
Validates payloads against schemas defined in the endpoint registry.
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Query
from mangum import Mangum
from pydantic import BaseModel

from shared.schema_registry import SchemaRegistry

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Data Lake Ingestion API",
    description="Ingest data into the data lake with schema validation",
    version="2.0.0",
)

# AWS Clients
firehose_client = boto3.client("firehose")

# Configuration
TENANT = os.environ.get("TENANT", "default")
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", f"{TENANT}-bronze")
FIREHOSE_ROLE_ARN = os.environ.get("FIREHOSE_ROLE_ARN", "")

# Schema registry instance
registry = SchemaRegistry()

# In-memory cache of streams known to be active (reset per Lambda cold start)
_active_streams: set[str] = set()


class RawDataModel(BaseModel):
    """Request model for data ingestion"""
    data: dict[str, Any]


class IngestionResponse(BaseModel):
    """Response model for successful ingestion"""
    status: str
    endpoint: str
    records_sent: int
    validated: bool


def get_firehose_name(domain: str, endpoint_name: str) -> str:
    """Generate Firehose delivery stream name from domain and endpoint."""
    tenant_part = TENANT.capitalize()
    domain_part = domain.title().replace("_", "")
    name_part = endpoint_name.title().replace("_", "")
    return f"{tenant_part}{domain_part}{name_part}Firehose"


def _create_firehose_stream(stream_name: str, domain: str, endpoint_name: str) -> None:
    """Create a Firehose delivery stream with retry and backoff."""
    s3_prefix = f"firehose-data/{domain}/{endpoint_name}/"
    s3_error_prefix = f"firehose-errors/{domain}/{endpoint_name}/"

    max_retries = 3
    for attempt in range(max_retries):
        try:
            firehose_client.create_delivery_stream(
                DeliveryStreamName=stream_name,
                DeliveryStreamType="DirectPut",
                ExtendedS3DestinationConfiguration={
                    "BucketARN": f"arn:aws:s3:::{BRONZE_BUCKET}",
                    "RoleARN": FIREHOSE_ROLE_ARN,
                    "Prefix": s3_prefix,
                    "ErrorOutputPrefix": s3_error_prefix,
                    "BufferingHints": {
                        "SizeInMBs": 5,
                        "IntervalInSeconds": 60,
                    },
                    "CompressionFormat": "UNCOMPRESSED",
                    "CloudWatchLoggingOptions": {"Enabled": False},
                },
            )
            logger.info("Created Firehose stream %s", stream_name)
            return
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "ResourceInUseException":
                # Another request already created it (race between concurrent invocations)
                logger.info("Firehose %s already being created by another request.", stream_name)
                return
            if attempt < max_retries - 1:
                delay = 2 ** (attempt + 1)
                logger.warning(
                    "Failed to create Firehose %s (attempt %d/%d): %s. Retrying in %ds...",
                    stream_name, attempt + 1, max_retries, e, delay,
                )
                time.sleep(delay)
            else:
                raise RuntimeError(f"Failed to create Firehose {stream_name} after {max_retries} attempts: {e}")


def ensure_firehose(domain: str, endpoint_name: str) -> str:
    """Ensure a Firehose delivery stream exists and is ACTIVE before sending data."""
    firehose_name = get_firehose_name(domain, endpoint_name)

    if firehose_name in _active_streams:
        return firehose_name

    max_polls = 20  # 20 * 6s = 120s max wait
    for attempt in range(max_polls):
        try:
            resp = firehose_client.describe_delivery_stream(DeliveryStreamName=firehose_name)
            status = resp["DeliveryStreamDescription"]["DeliveryStreamStatus"]

            if status == "ACTIVE":
                _active_streams.add(firehose_name)
                logger.info("Firehose %s is ACTIVE.", firehose_name)
                return firehose_name

            if status == "CREATING":
                delay = min(2 ** attempt, 6)
                logger.info(
                    "Firehose %s is CREATING, waiting %ds... (poll %d/%d)",
                    firehose_name, delay, attempt + 1, max_polls,
                )
                time.sleep(delay)
                continue

            raise RuntimeError(f"Firehose {firehose_name} in unexpected state: {status}")

        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                logger.info("Firehose %s not found, creating...", firehose_name)
                _create_firehose_stream(firehose_name, domain, endpoint_name)
                time.sleep(2)
                continue
            raise

    raise RuntimeError(f"Firehose {firehose_name} did not become ACTIVE within timeout.")


def send_to_firehose(domain: str, endpoint_name: str, data: dict[str, Any] | list[dict[str, Any]]):
    """Send data to Kinesis Firehose, creating the stream on-demand if needed."""
    if isinstance(data, dict):
        data = [data]

    firehose_name = ensure_firehose(domain, endpoint_name)

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

    send_to_firehose(domain, endpoint_name, data)

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

        send_to_firehose(domain, endpoint_name, record_data)

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
