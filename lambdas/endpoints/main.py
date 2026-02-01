"""
Endpoints API - FastAPI Lambda

CRUD operations for managing ingestion endpoint schemas.
Schemas are stored in S3 with automatic versioning.
"""

import os
from typing import Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum

from models import (
    CreateEndpointRequest,
    EndpointResponse,
    EndpointSchema,
    SchemaMode,
)
from schema_registry import SchemaRegistry

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


# Lambda handler
handler = Mangum(app)
