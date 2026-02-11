"""
Pydantic models for the Lakehouse Ingestion Agent.

Defines the IngestionPlan output structure that maps OpenAPI endpoints
to data lake resources, ready for consumption by dlt-init-openapi pipelines.
"""

from __future__ import annotations

import re
from pydantic import BaseModel, Field, field_validator


class EndpointSpec(BaseModel):
    """A single API endpoint mapped to a data lake table."""

    path: str = Field(
        ...,
        description="Full API path (e.g., /api/v1/orders)",
    )
    method: str = Field(
        default="GET",
        description="HTTP method for data retrieval",
    )
    resource_name: str = Field(
        ...,
        description="snake_case name for the target table (e.g., orders, customer_invoices)",
    )
    primary_key: str | None = Field(
        default=None,
        description="Primary key field extracted from the OpenAPI schema or description (e.g., id, order_id)",
    )
    description: str = Field(
        default="",
        description="Human-readable description of the resource",
    )
    params: dict[str, str] = Field(
        default_factory=dict,
        description="Default query parameters for the endpoint (e.g., pagination, filters)",
    )
    data_path: str = Field(
        default="",
        description="JSON path to the data array in the response (e.g., results, data.items)",
    )
    is_collection: bool = Field(
        default=True,
        description="Whether this endpoint returns a list/collection of resources",
    )

    @field_validator("resource_name")
    @classmethod
    def validate_resource_name(cls, v: str) -> str:
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError(
                "resource_name must be snake_case (lowercase, start with letter, "
                "only letters, numbers, and underscores)"
            )
        return v


class IngestionPlan(BaseModel):
    """
    Structured ingestion plan generated from an OpenAPI spec.

    This is the primary output of the Ingestion Agent. It contains all
    the information needed to initialize a dlt pipeline for ingesting
    data from the described API into the data lake.
    """

    base_url: str = Field(
        ...,
        description="Base URL of the API (e.g., https://api.example.com/v1)",
    )
    api_name: str = Field(
        ...,
        description="snake_case name for the API source (e.g., stripe_api, github_api)",
    )
    auth_type: str = Field(
        default="bearer",
        description="Authentication type detected from the spec (bearer, api_key, basic, oauth2)",
    )
    auth_header: str = Field(
        default="Authorization",
        description="Header name used for authentication",
    )
    pagination_style: str = Field(
        default="unknown",
        description="Pagination pattern detected (offset, cursor, page_number, link_header, unknown)",
    )
    endpoints: list[EndpointSpec] = Field(
        default_factory=list,
        description="List of endpoints selected for ingestion",
    )

    @field_validator("api_name")
    @classmethod
    def validate_api_name(cls, v: str) -> str:
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError(
                "api_name must be snake_case (lowercase, start with letter, "
                "only letters, numbers, and underscores)"
            )
        return v

    @property
    def table_names(self) -> list[str]:
        """Get list of all resource/table names in the plan."""
        return [ep.resource_name for ep in self.endpoints]

    @property
    def collection_endpoints(self) -> list[EndpointSpec]:
        """Get only collection endpoints (lists of resources)."""
        return [ep for ep in self.endpoints if ep.is_collection]

    @property
    def get_endpoints(self) -> list[EndpointSpec]:
        """Get only GET endpoints (safe for data extraction)."""
        return [ep for ep in self.endpoints if ep.method.upper() == "GET"]

    def get_only(self) -> IngestionPlan:
        """Return a new plan with only GET endpoints."""
        return self.model_copy(update={"endpoints": self.get_endpoints})

    def to_dlt_config(self) -> dict:
        """
        Convert to a dlt rest_api source configuration dictionary.

        This output is compatible with the dlt rest_api verified source
        and can be used to bootstrap a dlt pipeline.
        """
        resources = []
        for ep in self.endpoints:
            resource: dict = {
                "name": ep.resource_name,
                "endpoint": {
                    "path": ep.path,
                    "method": ep.method,
                },
            }
            if ep.primary_key:
                resource["primary_key"] = ep.primary_key
            if ep.params:
                resource["endpoint"]["params"] = ep.params
            if ep.data_path:
                resource["endpoint"]["data_selector"] = ep.data_path
            resources.append(resource)

        config = {
            "client": {
                "base_url": self.base_url,
                "auth": {
                    "type": self.auth_type,
                },
            },
            "resources": resources,
        }

        if self.pagination_style != "unknown":
            config["client"]["paginator"] = self.pagination_style

        return config
