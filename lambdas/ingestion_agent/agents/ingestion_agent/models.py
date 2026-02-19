"""
Pydantic models for the Lakehouse Ingestion Agent.

Defines the IngestionPlan output structure that maps OpenAPI endpoints
to data lake resources, ready for consumption by dlt-init-openapi pipelines.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)


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
    field_descriptions: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Mapping of field names to their descriptions, extracted from "
            "the OpenAPI response schema or generated from sample data"
        ),
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


class PaginationConfig(BaseModel):
    """Pagination configuration for dlt rest_api source."""

    type: str = Field(
        default="auto",
        description=(
            "Paginator type: 'json_link' (follow next URL in response body — PREFERRED "
            "when the API returns a next-page URL), 'page_number', 'offset', 'cursor', "
            "'header_link', 'auto', 'single_page'"
        ),
    )
    next_url_path: str | None = Field(
        default=None,
        description=(
            "For json_link: dot-separated path to the next page URL in the response JSON "
            "(e.g., 'info.next', 'next', 'paging.next', 'links.next')"
        ),
    )
    total_path: str | None = Field(
        default=None,
        description=(
            "For page_number/offset: dot-separated path to total pages or total items "
            "in the response JSON (e.g., 'info.pages', 'meta.total_pages', 'total')"
        ),
    )
    page_param: str | None = Field(
        default=None,
        description="For page_number: query parameter name for the page number (default: 'page')",
    )
    limit: int | None = Field(
        default=None,
        description="For offset: number of items per page",
    )
    offset_param: str | None = Field(
        default=None,
        description="For offset: query parameter name for offset (default: 'offset')",
    )
    limit_param: str | None = Field(
        default=None,
        description="For offset: query parameter name for limit (default: 'limit')",
    )
    cursor_path: str | None = Field(
        default=None,
        description="For cursor: dot-separated path to cursor value in response (e.g., 'cursors.next')",
    )
    cursor_param: str | None = Field(
        default=None,
        description="For cursor: query parameter name for cursor (default: 'cursor')",
    )

    # Fields accepted by each dlt paginator type (avoids DictValidationException)
    _DLT_VALID_FIELDS: dict[str, set[str]] = {
        "page_number": {"total_path"},
        "offset": {"limit", "offset_param", "limit_param", "total_path"},
        "json_link": {"next_url_path"},
        "json_response": {"cursor_path", "cursor_param"},
        "cursor": {"cursor_path", "cursor_param"},
        "header_link": set(),
        "header_cursor": {"cursor_path", "cursor_param"},
    }

    def to_dlt_paginator(self) -> dict | str:
        """Convert to a dlt-compatible paginator config (dict or string).

        Only includes fields that the specific dlt paginator type accepts,
        preventing DictValidationException from unexpected fields.
        """
        if self.type in ("auto", "single_page"):
            return self.type

        valid = self._DLT_VALID_FIELDS.get(self.type, set())
        config: dict = {"type": self.type}
        for name in valid:
            value = getattr(self, name, None)
            if value is not None:
                config[name] = value
        return config


class OAuth2Config(BaseModel):
    """OAuth2 Resource Owner Password Credentials (ROPC) configuration.

    Used to obtain a Bearer token from APIs that require OAuth2 ROPC flow,
    such as ProjurisADV/SAJ ADV.

    The token endpoint is called with HTTP Basic auth (client_id:client_secret)
    and form body: grant_type=password&username=...&password=...
    """

    token_url: str = Field(
        ...,
        description=(
            "Token endpoint URL "
            "(e.g., https://login.projurisadv.com.br/adv-bouncer-authorization-server/oauth/token)"
        ),
    )
    client_id: str = Field(..., description="OAuth2 client ID")
    client_secret: str = Field(..., description="OAuth2 client secret")
    username: str = Field(
        ...,
        description=(
            "Resource owner username. Some APIs require a domain suffix, "
            "e.g., 'user$$tenant' for ProjurisADV."
        ),
    )
    password: str = Field(..., description="Resource owner password")


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
    pagination: PaginationConfig = Field(
        default_factory=PaginationConfig,
        description="Pagination configuration detected from the API spec",
    )
    endpoints: list[EndpointSpec] = Field(
        default_factory=list,
        description="List of endpoints selected for ingestion",
    )

    @model_validator(mode="before")
    @classmethod
    def _migrate_pagination_style(cls, data: Any) -> Any:
        """Backward compat: convert old ``pagination_style`` string to ``PaginationConfig``."""
        if isinstance(data, dict) and "pagination_style" in data and "pagination" not in data:
            style = data.pop("pagination_style")
            if style and style != "unknown":
                data["pagination"] = {"type": style}
            else:
                data["pagination"] = {"type": "auto"}
        return data

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

    def drop_non_collection_post(self) -> IngestionPlan:
        """
        Remove non-GET endpoints where ``is_collection=False``.

        These are unambiguously mutation endpoints (POST that creates a single
        resource, PUT, PATCH, DELETE) that should never appear in an ingestion
        plan.  This acts as a safety net after the LLM prompt fails to exclude
        them.

        POST endpoints with ``is_collection=True`` (search/query endpoints that
        return a list) are left untouched here — they are handled by
        ``prefer_get_endpoints`` and ultimately by ``get_only``.
        """
        filtered = [
            ep for ep in self.endpoints
            if ep.method.upper() == "GET" or ep.is_collection
        ]
        if len(filtered) < len(self.endpoints):
            dropped = [
                f"{ep.method} {ep.path}"
                for ep in self.endpoints
                if ep not in filtered
            ]
            logger.info(
                "Dropped mutation endpoint(s) (non-GET, is_collection=False): %s",
                dropped,
            )
        return self.model_copy(update={"endpoints": filtered})

    def prefer_get_endpoints(self) -> IngestionPlan:
        """
        When GET and non-GET endpoints share the same path, drop the non-GET.

        Some APIs (like Projuris ADV) expose both ``GET /resource`` (simple
        query via URL params) and ``POST /resource`` (advanced search via JSON
        body) for the same path.  For data lake extraction, GET is always
        preferred because it:
          - Requires no request body (simpler)
          - Works natively with dlt's rest_api source
          - Is unambiguously read-only

        Endpoints whose path has no GET alternative are left untouched.
        """
        get_paths = {ep.path for ep in self.endpoints if ep.method.upper() == "GET"}
        filtered = [
            ep for ep in self.endpoints
            if ep.method.upper() == "GET" or ep.path not in get_paths
        ]
        if len(filtered) < len(self.endpoints):
            dropped = [ep.resource_name for ep in self.endpoints if ep not in filtered]
            logger.info(
                "Preferred GET over non-GET for shared path(s) — dropped POST: %s",
                dropped,
            )
        return self.model_copy(update={"endpoints": filtered})

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

        paginator = self.pagination.to_dlt_paginator()
        config["client"]["paginator"] = paginator

        return config
