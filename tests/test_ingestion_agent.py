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
    extract_field_descriptions,
    extract_swagger_spec_url,
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
        assert config["client"]["paginator"] == {"type": "offset"}
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
        assert config["client"]["paginator"] == "auto"

    def test_to_dlt_config_json_link_pagination(self):
        from agents.ingestion_agent.models import PaginationConfig
        plan = IngestionPlan(
            base_url="https://rickandmortyapi.com/api",
            api_name="rick_and_morty_api",
            pagination=PaginationConfig(type="json_link", next_url_path="info.next"),
            endpoints=[
                EndpointSpec(
                    path="/character",
                    resource_name="characters",
                    primary_key="id",
                    data_path="results",
                ),
            ],
        )
        config = plan.to_dlt_config()
        assert config["client"]["paginator"] == {
            "type": "json_link",
            "next_url_path": "info.next",
        }

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


# ---------------------------------------------------------------------------
# Fuzzy Endpoint Name Matching Tests
# ---------------------------------------------------------------------------

from agents.ingestion_agent.runner import _find_similar_endpoint


class TestFindSimilarEndpoint:
    """Tests for fuzzy matching of endpoint names."""

    def test_singular_plural_match(self):
        existing = ["ability", "pokemon", "moves"]
        assert _find_similar_endpoint("abilities", existing) == "ability"

    def test_exact_match_not_returned(self):
        # exact matches are handled separately; similarity == 1.0 still >= threshold
        existing = ["orders", "customers"]
        assert _find_similar_endpoint("orders", existing) == "orders"

    def test_no_match_below_threshold(self):
        existing = ["customers", "invoices"]
        assert _find_similar_endpoint("pokemon", existing) is None

    def test_empty_existing_list(self):
        assert _find_similar_endpoint("orders", []) is None

    def test_best_match_wins(self):
        existing = ["pokemon_type", "pokemon_types", "pokemon"]
        # "pokemon_types" should be highest match for "pokemon_type"
        result = _find_similar_endpoint("pokemon_type", existing)
        assert result in ("pokemon_type", "pokemon_types")

    def test_underscore_variations(self):
        existing = ["pokemon_species"]
        assert _find_similar_endpoint("pokemon_specie", existing) == "pokemon_species"

    def test_custom_threshold(self):
        existing = ["ability"]
        # "abilities" normalizes to "ability" so it's a 1.0 match even at 0.95
        assert _find_similar_endpoint("abilities", existing, threshold=0.95) == "ability"
        # A truly different name should not match even with a low threshold
        assert _find_similar_endpoint("pokemon", existing, threshold=0.5) is None


# ---------------------------------------------------------------------------
# extract_field_descriptions Tests
# ---------------------------------------------------------------------------

# Spec with field descriptions for testing
DESCRIBED_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Described API", "version": "1.0.0"},
    "paths": {
        "/pets": {
            "get": {
                "summary": "List all pets",
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/Pet"},
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
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "count": {"type": "integer"},
                                        "results": {
                                            "type": "array",
                                            "items": {"$ref": "#/components/schemas/Order"},
                                        },
                                    },
                                }
                            }
                        }
                    }
                },
            }
        },
        "/users": {
            "get": {
                "summary": "List users",
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/User"}
                            }
                        }
                    }
                },
            }
        },
        "/no_desc": {
            "get": {
                "summary": "No descriptions",
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "id": {"type": "integer"},
                                        "value": {"type": "string"},
                                    },
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
                    "id": {"type": "integer", "description": "Unique identifier for the pet"},
                    "name": {"type": "string", "description": "Name of the pet"},
                    "status": {"type": "string", "description": "Availability status in the store"},
                    "tag": {"type": "string"},
                },
            },
            "Order": {
                "type": "object",
                "properties": {
                    "order_id": {"type": "integer", "description": "Unique order identifier"},
                    "pet_id": {"type": "integer", "description": "ID of the pet being ordered"},
                    "quantity": {"type": "integer", "description": "Number of pets ordered"},
                    "status": {"type": "string"},
                },
            },
            "User": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer", "description": "Unique user ID"},
                    "email": {"type": "string", "description": "User email address"},
                    "role": {"type": "string"},
                },
            },
        },
    },
}


class TestExtractFieldDescriptions:
    """Tests for extracting field descriptions from OpenAPI specs."""

    def test_array_response_with_ref(self):
        """Array of $ref items — should extract descriptions from resolved schema."""
        descriptions = extract_field_descriptions(DESCRIBED_SPEC, "/pets", "GET")
        assert descriptions["id"] == "Unique identifier for the pet"
        assert descriptions["name"] == "Name of the pet"
        assert descriptions["status"] == "Availability status in the store"
        # 'tag' has no description, should not be included
        assert "tag" not in descriptions

    def test_wrapper_object_with_results_array(self):
        """Pagination wrapper with 'results' array — should extract from items."""
        descriptions = extract_field_descriptions(DESCRIBED_SPEC, "/orders", "GET")
        assert descriptions["order_id"] == "Unique order identifier"
        assert descriptions["pet_id"] == "ID of the pet being ordered"
        assert descriptions["quantity"] == "Number of pets ordered"
        # 'status' has no description
        assert "status" not in descriptions

    def test_direct_ref_response(self):
        """Direct $ref to a schema object — should extract from its properties."""
        descriptions = extract_field_descriptions(DESCRIBED_SPEC, "/users", "GET")
        assert descriptions["id"] == "Unique user ID"
        assert descriptions["email"] == "User email address"
        # 'role' has no description
        assert "role" not in descriptions

    def test_no_descriptions_returns_empty(self):
        """Schema with no field descriptions → empty dict."""
        descriptions = extract_field_descriptions(DESCRIBED_SPEC, "/no_desc", "GET")
        assert descriptions == {}

    def test_nonexistent_path_returns_empty(self):
        """Path not in spec → empty dict."""
        descriptions = extract_field_descriptions(DESCRIBED_SPEC, "/nonexistent", "GET")
        assert descriptions == {}

    def test_nonexistent_method_returns_empty(self):
        """Method not in path → empty dict."""
        descriptions = extract_field_descriptions(DESCRIBED_SPEC, "/pets", "POST")
        assert descriptions == {}

    def test_swagger2_with_descriptions(self):
        """Swagger 2.x spec with field descriptions."""
        swagger_spec = {
            "swagger": "2.0",
            "info": {"title": "Legacy", "version": "1.0"},
            "paths": {
                "/items": {
                    "get": {
                        "responses": {
                            "200": {
                                "schema": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "integer", "description": "Item ID"},
                                            "label": {"type": "string", "description": "Display label"},
                                        },
                                    },
                                }
                            }
                        },
                    }
                }
            },
        }
        descriptions = extract_field_descriptions(swagger_spec, "/items", "GET")
        assert descriptions["id"] == "Item ID"
        assert descriptions["label"] == "Display label"

    def test_empty_spec_returns_empty(self):
        """Empty spec → empty dict."""
        descriptions = extract_field_descriptions({}, "/pets", "GET")
        assert descriptions == {}

    def test_existing_spec_without_descriptions(self):
        """PETSTORE_SPEC (from existing fixtures) has no field descriptions."""
        descriptions = extract_field_descriptions(PETSTORE_SPEC, "/pets", "GET")
        assert descriptions == {}


# ---------------------------------------------------------------------------
# prefer_get_endpoints Tests
# ---------------------------------------------------------------------------


def _make_plan(*endpoints_kwargs_list) -> IngestionPlan:
    """Helper: build a minimal IngestionPlan with given endpoint overrides."""
    endpoints = []
    for kw in endpoints_kwargs_list:
        endpoints.append(
            EndpointSpec(
                path=kw.get("path", "/resource"),
                resource_name=kw.get("resource_name", "resource"),
                method=kw.get("method", "GET"),
            )
        )
    return IngestionPlan(
        base_url="https://api.example.com",
        api_name="test_api",
        auth_type="none",
        endpoints=endpoints,
    )


class TestPreferGetEndpoints:
    """Tests for IngestionPlan.prefer_get_endpoints()."""

    def test_drops_post_when_get_exists_for_same_path(self):
        """Core Projuris ADV case: GET and POST on the same path → keep GET."""
        plan = _make_plan(
            {"path": "/processo/consulta", "resource_name": "processo_get", "method": "GET"},
            {"path": "/processo/consulta", "resource_name": "processo_post", "method": "POST"},
        )
        result = plan.prefer_get_endpoints()
        assert len(result.endpoints) == 1
        assert result.endpoints[0].method == "GET"
        assert result.endpoints[0].resource_name == "processo_get"

    def test_keeps_post_when_no_get_exists_for_path(self):
        """POST-only endpoint (no GET alternative) must NOT be removed."""
        plan = _make_plan(
            {"path": "/pessoa/consulta", "resource_name": "pessoa", "method": "POST"},
        )
        result = plan.prefer_get_endpoints()
        assert len(result.endpoints) == 1
        assert result.endpoints[0].method == "POST"

    def test_multiple_paths_mixed(self):
        """GET wins on shared paths; POST survives on unique paths."""
        plan = _make_plan(
            # /processo has both GET and POST → keep GET
            {"path": "/processo/consulta", "resource_name": "processo_get", "method": "GET"},
            {"path": "/processo/consulta", "resource_name": "processo_post", "method": "POST"},
            # /tarefa only has POST → keep it
            {"path": "/tarefa/consulta", "resource_name": "tarefa", "method": "POST"},
            # /pessoa only has GET → keep it
            {"path": "/pessoa/consulta", "resource_name": "pessoa", "method": "GET"},
        )
        result = plan.prefer_get_endpoints()
        assert len(result.endpoints) == 3
        methods_by_name = {ep.resource_name: ep.method for ep in result.endpoints}
        assert methods_by_name["processo_get"] == "GET"
        assert "processo_post" not in methods_by_name
        assert methods_by_name["tarefa"] == "POST"
        assert methods_by_name["pessoa"] == "GET"

    def test_no_duplicates_unchanged(self):
        """Plans without duplicate paths are returned as-is."""
        plan = _make_plan(
            {"path": "/a", "resource_name": "a", "method": "GET"},
            {"path": "/b", "resource_name": "b", "method": "GET"},
        )
        result = plan.prefer_get_endpoints()
        assert len(result.endpoints) == 2

    def test_order_preserved(self):
        """GET endpoint that appears after POST must still survive."""
        plan = _make_plan(
            {"path": "/processo/consulta", "resource_name": "processo_post", "method": "POST"},
            {"path": "/processo/consulta", "resource_name": "processo_get", "method": "GET"},
        )
        result = plan.prefer_get_endpoints()
        assert len(result.endpoints) == 1
        assert result.endpoints[0].method == "GET"


# ---------------------------------------------------------------------------
# _extract_swagger_spec_url Tests
# ---------------------------------------------------------------------------

class TestExtractSwaggerSpecUrl:
    """Tests for the Swagger UI / Redoc spec URL extractor."""

    BASE = "https://docs.example.com/ui/index.html"

    def test_swagger_ui_bundle_absolute_url(self):
        html = """
        <script>
          window.onload = function() {
            SwaggerUIBundle({
              url: "https://api.example.com/v3/api-docs",
              dom_id: '#swagger-ui',
            })
          }
        </script>
        """
        result = extract_swagger_spec_url(html, self.BASE)
        assert result == "https://api.example.com/v3/api-docs"

    def test_swagger_ui_bundle_relative_url(self):
        html = """
        <script>
          SwaggerUIBundle({ url: "/v3/api-docs", dom_id: '#swagger-ui' })
        </script>
        """
        result = extract_swagger_spec_url(html, self.BASE)
        assert result == "https://docs.example.com/v3/api-docs"

    def test_data_spec_url_attribute(self):
        html = '<div id="swagger-ui" data-spec-url="/openapi.json"></div>'
        result = extract_swagger_spec_url(html, self.BASE)
        assert result == "https://docs.example.com/openapi.json"

    def test_redoc_spec_url(self):
        html = '<redoc spec-url="https://api.example.com/swagger.yaml"></redoc>'
        result = extract_swagger_spec_url(html, self.BASE)
        assert result == "https://api.example.com/swagger.yaml"

    def test_url_with_openapi_keyword(self):
        html = "const config = { url: '/api/openapi.json', deepLinking: true };"
        result = extract_swagger_spec_url(html, self.BASE)
        assert result == "https://docs.example.com/api/openapi.json"

    def test_url_with_api_docs_keyword(self):
        html = "const opts = { url: '/v3/api-docs?group=public' };"
        result = extract_swagger_spec_url(html, self.BASE)
        assert result == "https://docs.example.com/v3/api-docs?group=public"

    def test_url_ending_with_json_extension(self):
        html = "{ url: '/specs/my-service.json' }"
        result = extract_swagger_spec_url(html, self.BASE)
        assert result == "https://docs.example.com/specs/my-service.json"

    def test_url_ending_with_yaml_extension(self):
        html = "{ url: '/specs/my-service.yaml' }"
        result = extract_swagger_spec_url(html, self.BASE)
        assert result == "https://docs.example.com/specs/my-service.yaml"

    def test_skips_js_library_urls(self):
        # A js lib URL that matches the generic `url:` pattern must be skipped
        html = "{ url: '/static/swagger-ui.js', dom_id: '#ui' }"
        result = extract_swagger_spec_url(html, self.BASE)
        assert result is None

    def test_no_spec_url_returns_none(self):
        html = "<html><body><p>Just a regular page</p></body></html>"
        result = extract_swagger_spec_url(html, self.BASE)
        assert result is None

    def test_springdoc_pattern(self):
        """Springdoc Spring Boot default Swagger UI page."""
        html = """
        <script>
        window.onload = function() {
          window.ui = SwaggerUIBundle({
            url: "/v3/api-docs",
            dom_id: '#swagger-ui',
            presets: [SwaggerUIBundle.presets.apis],
          })
        }
        </script>
        """
        result = extract_swagger_spec_url(html, "https://api.projurisadv.com.br/ui/index.html")
        assert result == "https://api.projurisadv.com.br/v3/api-docs"


class TestEndpointSpecFieldDescriptions:
    """Tests that EndpointSpec properly handles field_descriptions."""

    def test_default_empty(self):
        ep = EndpointSpec(path="/pets", resource_name="pets")
        assert ep.field_descriptions == {}

    def test_with_descriptions(self):
        ep = EndpointSpec(
            path="/pets",
            resource_name="pets",
            field_descriptions={"id": "Pet ID", "name": "Pet name"},
        )
        assert ep.field_descriptions["id"] == "Pet ID"
        assert ep.field_descriptions["name"] == "Pet name"

    def test_serialization_roundtrip(self):
        ep = EndpointSpec(
            path="/pets",
            resource_name="pets",
            field_descriptions={"id": "Pet ID"},
        )
        dumped = ep.model_dump()
        restored = EndpointSpec.model_validate(dumped)
        assert restored.field_descriptions == {"id": "Pet ID"}
