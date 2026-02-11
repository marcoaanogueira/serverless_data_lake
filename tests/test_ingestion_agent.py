"""
Tests for the Lakehouse Ingestion Agent.

Tests cover:
- Pydantic model validation (IngestionPlan, EndpointSpec)
- OpenAPI spec summary builder
- dlt config generation
- $ref resolution logic
"""

import json
import pytest

from agents.ingestion_agent.models import EndpointSpec, IngestionPlan
from agents.ingestion_agent.spec_parser import (
    build_spec_summary,
    resolve_ref,
    simplify_schema,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

PETSTORE_SPEC = {
    "openapi": "3.0.0",
    "info": {
        "title": "Petstore API",
        "version": "1.0.0",
        "description": "A sample Petstore API for testing",
    },
    "servers": [{"url": "https://petstore.example.com/v1"}],
    "paths": {
        "/pets": {
            "get": {
                "summary": "List all pets",
                "operationId": "listPets",
                "parameters": [
                    {"name": "limit", "in": "query", "required": False},
                    {"name": "offset", "in": "query", "required": False},
                ],
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "results": {
                                            "type": "array",
                                            "items": {"$ref": "#/components/schemas/Pet"},
                                        }
                                    },
                                }
                            }
                        }
                    }
                },
            }
        },
        "/pets/{petId}": {
            "get": {
                "summary": "Get a pet by ID",
                "operationId": "getPet",
                "parameters": [
                    {"name": "petId", "in": "path", "required": True},
                ],
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/Pet"}
                            }
                        }
                    }
                },
            }
        },
        "/store/inventory": {
            "get": {
                "summary": "Returns pet inventories by status",
                "operationId": "getInventory",
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "additionalProperties": {"type": "integer"},
                                }
                            }
                        }
                    }
                },
            }
        },
        "/orders": {
            "get": {
                "summary": "List orders",
                "operationId": "listOrders",
                "parameters": [
                    {"name": "page", "in": "query", "required": False},
                ],
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/Order"},
                                }
                            }
                        }
                    }
                },
            }
        },
    },
    "components": {
        "schemas": {
            "Pet": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                    "status": {"type": "string"},
                },
                "required": ["id", "name"],
            },
            "Order": {
                "type": "object",
                "properties": {
                    "order_id": {"type": "integer"},
                    "pet_id": {"type": "integer"},
                    "quantity": {"type": "integer"},
                    "status": {"type": "string"},
                },
                "required": ["order_id"],
            },
        },
        "securitySchemes": {
            "bearerAuth": {
                "type": "http",
                "scheme": "bearer",
            }
        },
    },
}


SWAGGER_2_SPEC = {
    "swagger": "2.0",
    "info": {"title": "Legacy API", "version": "2.0"},
    "host": "legacy.example.com",
    "basePath": "/api",
    "schemes": ["https"],
    "paths": {
        "/users": {
            "get": {
                "summary": "List users",
                "parameters": [],
                "responses": {
                    "200": {
                        "schema": {
                            "type": "array",
                            "items": {"$ref": "#/definitions/User"},
                        }
                    }
                },
            }
        }
    },
    "definitions": {
        "User": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "email": {"type": "string"},
            },
        }
    },
    "securityDefinitions": {
        "apiKey": {"type": "apiKey", "name": "X-API-Key", "in": "header"}
    },
}


# ---------------------------------------------------------------------------
# EndpointSpec Tests
# ---------------------------------------------------------------------------


class TestEndpointSpec:
    def test_valid_endpoint(self):
        ep = EndpointSpec(
            path="/api/v1/orders",
            resource_name="orders",
            primary_key="order_id",
            description="List all orders",
        )
        assert ep.path == "/api/v1/orders"
        assert ep.resource_name == "orders"
        assert ep.is_collection is True

    def test_snake_case_validation(self):
        with pytest.raises(ValueError, match="snake_case"):
            EndpointSpec(
                path="/api/v1/orders",
                resource_name="MyOrders",  # Invalid: not snake_case
            )

    def test_default_method_is_get(self):
        ep = EndpointSpec(path="/pets", resource_name="pets")
        assert ep.method == "GET"

    def test_params_and_data_path(self):
        ep = EndpointSpec(
            path="/api/orders",
            resource_name="orders",
            params={"status": "active", "limit": "100"},
            data_path="data.orders",
        )
        assert ep.params == {"status": "active", "limit": "100"}
        assert ep.data_path == "data.orders"


# ---------------------------------------------------------------------------
# IngestionPlan Tests
# ---------------------------------------------------------------------------


class TestIngestionPlan:
    def test_valid_plan(self):
        plan = IngestionPlan(
            base_url="https://api.example.com/v1",
            api_name="example_api",
            endpoints=[
                EndpointSpec(
                    path="/orders",
                    resource_name="orders",
                    primary_key="id",
                ),
                EndpointSpec(
                    path="/customers",
                    resource_name="customers",
                    primary_key="customer_id",
                ),
            ],
        )
        assert len(plan.endpoints) == 2
        assert plan.table_names == ["orders", "customers"]

    def test_api_name_validation(self):
        with pytest.raises(ValueError, match="snake_case"):
            IngestionPlan(
                base_url="https://api.example.com",
                api_name="ExampleAPI",  # Invalid
                endpoints=[],
            )

    def test_collection_endpoints_filter(self):
        plan = IngestionPlan(
            base_url="https://api.example.com",
            api_name="test_api",
            endpoints=[
                EndpointSpec(
                    path="/orders",
                    resource_name="orders",
                    is_collection=True,
                ),
                EndpointSpec(
                    path="/orders/{id}",
                    resource_name="order_detail",
                    is_collection=False,
                ),
            ],
        )
        collections = plan.collection_endpoints
        assert len(collections) == 1
        assert collections[0].resource_name == "orders"

    def test_to_dlt_config(self):
        plan = IngestionPlan(
            base_url="https://api.example.com/v1",
            api_name="test_api",
            auth_type="bearer",
            pagination_style="offset",
            endpoints=[
                EndpointSpec(
                    path="/orders",
                    resource_name="orders",
                    primary_key="order_id",
                    data_path="results",
                    params={"limit": "100"},
                ),
            ],
        )
        config = plan.to_dlt_config()

        assert config["client"]["base_url"] == "https://api.example.com/v1"
        assert config["client"]["auth"]["type"] == "bearer"
        assert config["client"]["paginator"] == "offset"
        assert len(config["resources"]) == 1
        assert config["resources"][0]["name"] == "orders"
        assert config["resources"][0]["primary_key"] == "order_id"
        assert config["resources"][0]["endpoint"]["data_selector"] == "results"
        assert config["resources"][0]["endpoint"]["params"] == {"limit": "100"}

    def test_to_dlt_config_unknown_pagination(self):
        plan = IngestionPlan(
            base_url="https://api.example.com",
            api_name="test_api",
            pagination_style="unknown",
            endpoints=[],
        )
        config = plan.to_dlt_config()
        assert "paginator" not in config["client"]

    def test_model_serialization_roundtrip(self):
        plan = IngestionPlan(
            base_url="https://api.example.com/v1",
            api_name="test_api",
            auth_type="api_key",
            auth_header="X-API-Key",
            pagination_style="cursor",
            endpoints=[
                EndpointSpec(
                    path="/items",
                    resource_name="items",
                    primary_key="id",
                    description="All items",
                    data_path="data",
                    params={"cursor": ""},
                ),
            ],
        )
        dumped = plan.model_dump()
        restored = IngestionPlan.model_validate(dumped)
        assert restored == plan


# ---------------------------------------------------------------------------
# OpenAPI Spec Summary Builder Tests
# ---------------------------------------------------------------------------


class TestBuildSpecSummary:
    def test_openapi3_summary(self):
        summary = build_spec_summary(PETSTORE_SPEC)
        assert "Petstore API" in summary
        assert "petstore.example.com" in summary
        assert "/pets" in summary
        assert "/orders" in summary
        assert "bearerAuth" in summary

    def test_swagger2_summary(self):
        summary = build_spec_summary(SWAGGER_2_SPEC)
        assert "Legacy API" in summary
        assert "legacy.example.com" in summary
        assert "/users" in summary
        assert "apiKey" in summary

    def test_empty_spec(self):
        summary = build_spec_summary({})
        assert "Unknown" in summary


# ---------------------------------------------------------------------------
# $ref Resolution Tests
# ---------------------------------------------------------------------------


class TestResolveRef:
    def test_resolve_component_schema(self):
        result = resolve_ref("#/components/schemas/Pet", PETSTORE_SPEC)
        assert result is not None
        assert result["type"] == "object"
        assert "id" in result["properties"]

    def test_resolve_definition(self):
        result = resolve_ref("#/definitions/User", SWAGGER_2_SPEC)
        assert result is not None
        assert "email" in result["properties"]

    def test_resolve_invalid_ref(self):
        result = resolve_ref("#/components/schemas/NonExistent", PETSTORE_SPEC)
        assert result is None

    def test_resolve_external_ref(self):
        result = resolve_ref("http://example.com/schema.json", PETSTORE_SPEC)
        assert result is None


# ---------------------------------------------------------------------------
# Schema Simplification Tests
# ---------------------------------------------------------------------------


class TestSimplifySchema:
    def test_simplify_with_ref(self):
        schema = {"$ref": "#/components/schemas/Pet"}
        result = simplify_schema(schema, PETSTORE_SPEC)
        assert result["type"] == "object"
        assert "id" in result["properties"]

    def test_simplify_array(self):
        schema = {
            "type": "array",
            "items": {"$ref": "#/components/schemas/Order"},
        }
        result = simplify_schema(schema, PETSTORE_SPEC)
        assert result["type"] == "array"
        assert "order_id" in result["items"]["properties"]

    def test_depth_limit(self):
        # Create a deeply nested schema
        schema = {"$ref": "#/components/schemas/Pet"}
        result = simplify_schema(schema, PETSTORE_SPEC, depth=4)
        assert result.get("note") == "truncated"

    def test_plain_schema(self):
        schema = {"type": "string"}
        result = simplify_schema(schema, PETSTORE_SPEC)
        assert result == {"type": "string"}
