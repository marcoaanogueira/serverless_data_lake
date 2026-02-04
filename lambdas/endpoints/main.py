"""
Endpoints API - FastAPI Lambda

CRUD operations for managing ingestion endpoint schemas.
Schemas are stored in S3 with automatic versioning.
"""

import os
import re
from datetime import datetime
from typing import Optional, Any
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pydantic import BaseModel, Field

from shared.models import (
    CreateEndpointRequest,
    EndpointResponse,
    EndpointSchema,
    SchemaMode,
    DataType,
    ColumnDefinition,
)
from shared.schema_registry import SchemaRegistry


# =============================================================================
# Schema Inference Logic
# =============================================================================

def to_snake_case(name: str) -> str:
    """Convert camelCase or PascalCase to snake_case"""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def infer_type_from_value(value: Any) -> DataType:
    """Infer DataType from a Python value"""
    if value is None:
        return DataType.STRING

    if isinstance(value, bool):
        return DataType.BOOLEAN

    if isinstance(value, int):
        return DataType.INTEGER

    if isinstance(value, float):
        return DataType.FLOAT

    if isinstance(value, list):
        return DataType.ARRAY

    if isinstance(value, dict):
        return DataType.JSON

    if isinstance(value, str):
        # Try to detect timestamps and dates
        iso_patterns = [
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO timestamp
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # Common timestamp
        ]
        for pattern in iso_patterns:
            if re.match(pattern, value):
                return DataType.TIMESTAMP

        # Date only
        if re.match(r'^\d{4}-\d{2}-\d{2}$', value):
            return DataType.DATE

        return DataType.STRING

    return DataType.STRING


def infer_columns_from_payload(payload: dict) -> list[dict]:
    """
    Infer column definitions from a sample JSON payload.

    Returns a list of column definitions with inferred types.
    """
    columns = []

    for key, value in payload.items():
        # Convert key to snake_case
        column_name = to_snake_case(key)

        # Ensure it starts with a letter
        if not column_name[0].isalpha():
            column_name = f"col_{column_name}"

        # Replace invalid characters
        column_name = re.sub(r'[^a-z0-9_]', '_', column_name)

        inferred_type = infer_type_from_value(value)

        columns.append({
            "name": column_name,
            "type": inferred_type.value,
            "required": value is not None,
            "primary_key": column_name in ("id", "uuid", "key"),
            "sample_value": str(value)[:100] if value is not None else None,
        })

    return columns


class InferSchemaRequest(BaseModel):
    """Request model for schema inference"""
    payload: dict = Field(..., description="Sample JSON payload to infer schema from")


class InferredColumn(BaseModel):
    """Inferred column with sample value"""
    name: str
    type: str
    required: bool
    primary_key: bool
    sample_value: Optional[str] = None


class InferSchemaResponse(BaseModel):
    """Response model for schema inference"""
    columns: list[InferredColumn]
    payload_keys: list[str] = Field(description="Original keys from payload")

app = FastAPI(
    title="Data Lake Endpoints API",
    description="Manage ingestion endpoint schemas",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize schema registry
registry = SchemaRegistry()

# Get API Gateway endpoint from environment
API_GATEWAY_ENDPOINT = os.environ.get("API_GATEWAY_ENDPOINT", "")


def schema_to_response(schema: EndpointSchema) -> EndpointResponse:
    """Convert EndpointSchema to API response"""
    endpoint_id = f"{schema.domain}/{schema.name}"
    return EndpointResponse(
        id=endpoint_id,
        name=schema.name,
        domain=schema.domain,
        version=schema.version,
        mode=schema.mode,
        endpoint_url=f"{API_GATEWAY_ENDPOINT}/ingestion/{schema.domain}/{schema.name}",
        schema_url=registry.get_schema_url(schema.domain, schema.name),
        status="active",
        created_at=schema.created_at,
        updated_at=schema.updated_at,
    )


@app.get("/")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "endpoints"}


@app.post("/endpoints", response_model=EndpointResponse)
def create_endpoint(request: CreateEndpointRequest):
    """
    Create a new ingestion endpoint.

    This creates a schema definition in S3 that can be used to validate
    incoming data during ingestion.
    """
    try:
        # Convert columns to dict format
        columns = [
            {
                "name": col.name,
                "type": col.type.value,
                "required": col.required,
                "primary_key": col.primary_key,
                "description": col.description,
            }
            for col in request.columns
        ]

        schema = registry.create(
            name=request.name,
            domain=request.domain,
            columns=columns,
            mode=request.mode,
            description=request.description,
        )

        return schema_to_response(schema)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/endpoints", response_model=list[EndpointResponse])
def list_endpoints(
    domain: Optional[str] = Query(None, description="Filter by domain"),
    order_by: Optional[str] = Query(None, description="Order by field"),
):
    """List all endpoints, optionally filtered by domain"""
    schemas = registry.list_all(domain=domain)

    responses = [schema_to_response(s) for s in schemas]

    # Sort if order_by specified
    if order_by:
        reverse = order_by.startswith("-")
        field = order_by.lstrip("-")
        if hasattr(EndpointResponse, field):
            responses.sort(key=lambda x: getattr(x, field), reverse=reverse)

    return responses


@app.get("/endpoints/{domain}/{name}", response_model=EndpointResponse)
def get_endpoint(
    domain: str,
    name: str,
    version: Optional[int] = Query(None, description="Specific version"),
):
    """Get a specific endpoint by domain and name"""
    schema = registry.get(domain, name, version)
    if not schema:
        raise HTTPException(status_code=404, detail=f"Endpoint {domain}/{name} not found")

    return schema_to_response(schema)


@app.put("/endpoints/{domain}/{name}", response_model=EndpointResponse)
def update_endpoint(
    domain: str,
    name: str,
    request: CreateEndpointRequest,
):
    """
    Update an endpoint schema (creates new version).

    The previous versions are preserved and can be accessed by version number.
    """
    try:
        columns = [
            {
                "name": col.name,
                "type": col.type.value,
                "required": col.required,
                "primary_key": col.primary_key,
                "description": col.description,
            }
            for col in request.columns
        ]

        schema = registry.update(
            domain=domain,
            name=name,
            columns=columns,
            description=request.description,
        )

        return schema_to_response(schema)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/endpoints/{domain}/{name}")
def delete_endpoint(domain: str, name: str):
    """Delete an endpoint and all its versions"""
    success = registry.delete(domain, name)
    if not success:
        raise HTTPException(status_code=404, detail=f"Endpoint {domain}/{name} not found")

    return {"message": f"Endpoint {domain}/{name} deleted"}


@app.get("/endpoints/{domain}/{name}/versions")
def list_endpoint_versions(domain: str, name: str):
    """List all versions of an endpoint schema"""
    versions = registry.list_versions(domain, name)
    if not versions:
        raise HTTPException(status_code=404, detail=f"Endpoint {domain}/{name} not found")

    return {
        "domain": domain,
        "name": name,
        "versions": versions,
        "latest": max(versions),
    }


@app.get("/endpoints/{domain}/{name}/yaml")
def get_endpoint_yaml(
    domain: str,
    name: str,
    version: Optional[int] = Query(None, description="Specific version"),
):
    """Get the raw YAML schema definition"""
    schema = registry.get(domain, name, version)
    if not schema:
        raise HTTPException(status_code=404, detail=f"Endpoint {domain}/{name} not found")

    return schema.to_yaml_dict()


@app.get("/endpoints/{domain}/{name}/download")
def download_endpoint_yaml(
    domain: str,
    name: str,
    version: Optional[int] = Query(None, description="Specific version"),
):
    """Get a presigned URL to download the YAML file"""
    schema = registry.get(domain, name, version)
    if not schema:
        raise HTTPException(status_code=404, detail=f"Endpoint {domain}/{name} not found")

    url = registry.generate_presigned_url(domain, name, version)
    return {"download_url": url, "expires_in": 3600}


@app.post("/endpoints/infer", response_model=InferSchemaResponse)
def infer_schema(request: InferSchemaRequest):
    """
    Infer schema from a sample JSON payload.

    Takes a sample payload and returns the inferred column definitions
    with detected types. The user can then review and adjust before
    creating the endpoint.

    Example:
        POST /endpoints/infer
        {
            "payload": {
                "orderId": "abc123",
                "totalAmount": 99.90,
                "quantity": 5,
                "isPaid": true,
                "createdAt": "2024-01-15T10:30:00Z"
            }
        }

    Returns:
        {
            "columns": [
                {"name": "order_id", "type": "string", "required": true, ...},
                {"name": "total_amount", "type": "float", "required": true, ...},
                ...
            ],
            "payload_keys": ["orderId", "totalAmount", ...]
        }
    """
    if not request.payload:
        raise HTTPException(status_code=400, detail="Payload cannot be empty")

    if not isinstance(request.payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object")

    columns = infer_columns_from_payload(request.payload)

    return InferSchemaResponse(
        columns=[InferredColumn(**col) for col in columns],
        payload_keys=list(request.payload.keys()),
    )


# Lambda handler
handler = Mangum(app)
