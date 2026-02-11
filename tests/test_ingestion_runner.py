"""
Tests for the Ingestion Runner.

Tests cover:
- GET-only endpoint filtering (models.get_only)
- Data extraction with data_path
- Auto-creation of endpoints (fetch_sample + infer + create)
- dlt config building (build_dlt_config)
- Custom dlt destination (make_destination)
- Full orchestrator (setup_endpoints, run)
"""

import json
import pytest
import httpx
import respx

from agents.ingestion_agent.models import EndpointSpec, IngestionPlan
from agents.ingestion_agent.runner import (
    extract_data,
    fetch_sample,
    infer_and_create_endpoint,
    setup_endpoints,
    build_dlt_config,
    make_destination,
    EndpointResult,
    RunResult,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _plan(endpoints: list[EndpointSpec] | None = None) -> IngestionPlan:
    """Build a minimal IngestionPlan for testing."""
    return IngestionPlan(
        base_url="https://petstore.example.com/v3",
        api_name="petstore_api",
        auth_type="bearer",
        pagination_style="unknown",
        endpoints=endpoints or [],
    )


PET_RECORDS = [
    {"id": 1, "name": "Rex", "status": "available"},
    {"id": 2, "name": "Luna", "status": "sold"},
]

INFERRED_SCHEMA = {
    "columns": [
        {"name": "id", "type": "integer", "required": True, "primary_key": True, "sample_value": "1"},
        {"name": "name", "type": "string", "required": True, "primary_key": False, "sample_value": "Rex"},
        {"name": "status", "type": "string", "required": True, "primary_key": False, "sample_value": "available"},
    ],
    "payload_keys": ["id", "name", "status"],
}

ENDPOINT_RESPONSE = {
    "id": "petstore/pets",
    "name": "pets",
    "domain": "petstore",
    "version": 1,
    "mode": "manual",
    "endpoint_url": "https://api.example.com/ingest/petstore/pets",
    "schema_url": "s3://bucket/schemas/petstore/bronze/pets/latest.yaml",
    "status": "active",
    "created_at": "2026-01-01T00:00:00",
    "updated_at": "2026-01-01T00:00:00",
}


# ---------------------------------------------------------------------------
# Model: get_only filter
# ---------------------------------------------------------------------------

class TestGetOnly:
    def test_filters_post_endpoints(self):
        plan = _plan([
            EndpointSpec(path="/pets", method="GET", resource_name="pets"),
            EndpointSpec(path="/store/order", method="POST", resource_name="orders"),
        ])
        safe = plan.get_only()
        assert len(safe.endpoints) == 1
        assert safe.endpoints[0].resource_name == "pets"

    def test_filters_put_and_delete(self):
        plan = _plan([
            EndpointSpec(path="/pets", method="GET", resource_name="pets"),
            EndpointSpec(path="/pets/1", method="PUT", resource_name="pet_update"),
            EndpointSpec(path="/pets/1", method="DELETE", resource_name="pet_delete"),
        ])
        safe = plan.get_only()
        assert len(safe.endpoints) == 1

    def test_keeps_all_get_endpoints(self):
        plan = _plan([
            EndpointSpec(path="/pets", method="GET", resource_name="pets"),
            EndpointSpec(path="/store/inventory", method="GET", resource_name="inventory"),
        ])
        safe = plan.get_only()
        assert len(safe.endpoints) == 2

    def test_empty_when_no_get(self):
        plan = _plan([
            EndpointSpec(path="/store/order", method="POST", resource_name="orders"),
        ])
        safe = plan.get_only()
        assert len(safe.endpoints) == 0

    def test_get_endpoints_property(self):
        plan = _plan([
            EndpointSpec(path="/pets", method="GET", resource_name="pets"),
            EndpointSpec(path="/order", method="POST", resource_name="orders"),
        ])
        assert len(plan.get_endpoints) == 1
        assert plan.get_endpoints[0].method == "GET"

    def test_case_insensitive(self):
        plan = _plan([
            EndpointSpec(path="/pets", method="get", resource_name="pets"),
        ])
        assert len(plan.get_only().endpoints) == 1


# ---------------------------------------------------------------------------
# extract_data
# ---------------------------------------------------------------------------

class TestExtractData:
    def test_empty_path_list_response(self):
        records = extract_data(PET_RECORDS, "")
        assert records == PET_RECORDS

    def test_empty_path_dict_response(self):
        records = extract_data({"id": 1, "name": "Rex"}, "")
        assert records == [{"id": 1, "name": "Rex"}]

    def test_single_key_path(self):
        response = {"results": PET_RECORDS}
        records = extract_data(response, "results")
        assert records == PET_RECORDS

    def test_nested_path(self):
        response = {"data": {"items": PET_RECORDS}}
        records = extract_data(response, "data.items")
        assert records == PET_RECORDS

    def test_missing_key_returns_empty(self):
        records = extract_data({"other": []}, "results")
        assert records == []

    def test_non_dict_at_path_returns_empty(self):
        records = extract_data("not a dict", "results")
        assert records == []

    def test_scalar_at_path_returns_empty(self):
        records = extract_data({"count": 42}, "count")
        assert records == []


# ---------------------------------------------------------------------------
# fetch_sample (mocked HTTP)
# ---------------------------------------------------------------------------

class TestFetchSample:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_first_record(self):
        ep = EndpointSpec(path="/pet/findByStatus", method="GET", resource_name="pets", params={"status": "available"})
        respx.get("https://petstore.example.com/v3/pet/findByStatus").mock(
            return_value=httpx.Response(200, json=PET_RECORDS)
        )
        async with httpx.AsyncClient() as client:
            sample = await fetch_sample(client, "https://petstore.example.com/v3", ep)
        assert sample == PET_RECORDS[0]

    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_none_for_empty(self):
        ep = EndpointSpec(path="/pets", method="GET", resource_name="pets")
        respx.get("https://petstore.example.com/v3/pets").mock(
            return_value=httpx.Response(200, json=[])
        )
        async with httpx.AsyncClient() as client:
            sample = await fetch_sample(client, "https://petstore.example.com/v3", ep)
        assert sample is None

    @pytest.mark.asyncio
    @respx.mock
    async def test_with_data_path(self):
        ep = EndpointSpec(path="/pets", method="GET", resource_name="pets", data_path="results")
        respx.get("https://petstore.example.com/v3/pets").mock(
            return_value=httpx.Response(200, json={"results": PET_RECORDS})
        )
        async with httpx.AsyncClient() as client:
            sample = await fetch_sample(client, "https://petstore.example.com/v3", ep)
        assert sample == PET_RECORDS[0]

    @pytest.mark.asyncio
    @respx.mock
    async def test_with_token(self):
        ep = EndpointSpec(path="/pets", method="GET", resource_name="pets")
        route = respx.get("https://petstore.example.com/v3/pets").mock(
            return_value=httpx.Response(200, json=[{"id": 1}])
        )
        async with httpx.AsyncClient() as client:
            await fetch_sample(client, "https://petstore.example.com/v3", ep, token="my-token")
        assert route.calls[0].request.headers["Authorization"] == "Bearer my-token"

    @pytest.mark.asyncio
    @respx.mock
    async def test_no_token_no_auth_header(self):
        ep = EndpointSpec(path="/pets", method="GET", resource_name="pets")
        route = respx.get("https://petstore.example.com/v3/pets").mock(
            return_value=httpx.Response(200, json=[])
        )
        async with httpx.AsyncClient() as client:
            await fetch_sample(client, "https://petstore.example.com/v3", ep, token="")
        assert "Authorization" not in route.calls[0].request.headers


# ---------------------------------------------------------------------------
# infer_and_create_endpoint (mocked HTTP)
# ---------------------------------------------------------------------------

class TestInferAndCreateEndpoint:
    @pytest.mark.asyncio
    @respx.mock
    async def test_infer_and_create(self):
        ep = EndpointSpec(path="/pets", method="GET", resource_name="pets", primary_key="id", description="All pets")

        infer_route = respx.post("https://api.example.com/endpoints/infer").mock(
            return_value=httpx.Response(200, json=INFERRED_SCHEMA)
        )
        create_route = respx.post("https://api.example.com/endpoints").mock(
            return_value=httpx.Response(200, json=ENDPOINT_RESPONSE)
        )

        async with httpx.AsyncClient() as client:
            ok = await infer_and_create_endpoint(
                client, "https://api.example.com", "petstore", ep, PET_RECORDS[0]
            )

        assert ok is True

        # Check infer was called with sample payload
        infer_body = json.loads(infer_route.calls[0].request.content)
        assert infer_body["payload"] == PET_RECORDS[0]

        # Check create was called with correct domain/name/columns
        create_body = json.loads(create_route.calls[0].request.content)
        assert create_body["name"] == "pets"
        assert create_body["domain"] == "petstore"
        assert create_body["description"] == "All pets"
        assert len(create_body["columns"]) == 3

        # Check primary_key was set from agent's plan
        id_col = next(c for c in create_body["columns"] if c["name"] == "id")
        assert id_col["primary_key"] is True
        assert id_col["required"] is True

    @pytest.mark.asyncio
    @respx.mock
    async def test_primary_key_override(self):
        """Agent's primary_key overrides inferred PK."""
        ep = EndpointSpec(path="/pets", method="GET", resource_name="pets", primary_key="name")

        inferred_no_pk = {
            "columns": [
                {"name": "id", "type": "integer", "required": True, "primary_key": False},
                {"name": "name", "type": "string", "required": False, "primary_key": False},
            ],
            "payload_keys": ["id", "name"],
        }

        respx.post("https://api.example.com/endpoints/infer").mock(
            return_value=httpx.Response(200, json=inferred_no_pk)
        )
        create_route = respx.post("https://api.example.com/endpoints").mock(
            return_value=httpx.Response(200, json=ENDPOINT_RESPONSE)
        )

        async with httpx.AsyncClient() as client:
            await infer_and_create_endpoint(
                client, "https://api.example.com", "petstore", ep, {"id": 1, "name": "Rex"}
            )

        create_body = json.loads(create_route.calls[0].request.content)
        name_col = next(c for c in create_body["columns"] if c["name"] == "name")
        assert name_col["primary_key"] is True
        assert name_col["required"] is True


# ---------------------------------------------------------------------------
# setup_endpoints (mocked HTTP)
# ---------------------------------------------------------------------------

class TestSetupEndpoints:
    @pytest.mark.asyncio
    @respx.mock
    async def test_creates_new_endpoint(self):
        plan = _plan([
            EndpointSpec(path="/pet/findByStatus", method="GET", resource_name="pets", params={"status": "available"}, primary_key="id"),
        ])

        # Source API
        respx.get("https://petstore.example.com/v3/pet/findByStatus").mock(
            return_value=httpx.Response(200, json=PET_RECORDS)
        )
        # Check existence → 404 (not found)
        respx.get("https://api.example.com/endpoints/petstore/pets").mock(
            return_value=httpx.Response(404, json={"detail": "Not found"})
        )
        # Infer + Create
        respx.post("https://api.example.com/endpoints/infer").mock(
            return_value=httpx.Response(200, json=INFERRED_SCHEMA)
        )
        respx.post("https://api.example.com/endpoints").mock(
            return_value=httpx.Response(200, json=ENDPOINT_RESPONSE)
        )

        created, skipped, errors = await setup_endpoints(
            plan, "petstore", "https://api.example.com"
        )

        assert created == ["pets"]
        assert skipped == []
        assert errors == []

    @pytest.mark.asyncio
    @respx.mock
    async def test_skips_existing_endpoint(self):
        plan = _plan([
            EndpointSpec(path="/pets", method="GET", resource_name="pets"),
        ])

        # Endpoint already exists
        respx.get("https://api.example.com/endpoints/petstore/pets").mock(
            return_value=httpx.Response(200, json=ENDPOINT_RESPONSE)
        )

        created, skipped, errors = await setup_endpoints(
            plan, "petstore", "https://api.example.com"
        )

        assert created == []
        assert skipped == ["pets"]
        assert errors == []

    @pytest.mark.asyncio
    @respx.mock
    async def test_handles_source_api_error(self):
        plan = _plan([
            EndpointSpec(path="/pets", method="GET", resource_name="pets"),
        ])

        respx.get("https://api.example.com/endpoints/petstore/pets").mock(
            return_value=httpx.Response(404)
        )
        respx.get("https://petstore.example.com/v3/pets").mock(
            return_value=httpx.Response(500, text="Server Error")
        )

        created, skipped, errors = await setup_endpoints(
            plan, "petstore", "https://api.example.com"
        )

        assert created == []
        assert len(errors) == 1
        assert "Failed to fetch sample" in errors[0]

    @pytest.mark.asyncio
    @respx.mock
    async def test_handles_empty_response(self):
        plan = _plan([
            EndpointSpec(path="/pets", method="GET", resource_name="pets"),
        ])

        respx.get("https://api.example.com/endpoints/petstore/pets").mock(
            return_value=httpx.Response(404)
        )
        respx.get("https://petstore.example.com/v3/pets").mock(
            return_value=httpx.Response(200, json=[])
        )

        created, skipped, errors = await setup_endpoints(
            plan, "petstore", "https://api.example.com"
        )

        assert created == []
        assert len(errors) == 1
        assert "No sample data" in errors[0]


# ---------------------------------------------------------------------------
# build_dlt_config
# ---------------------------------------------------------------------------

class TestBuildDltConfig:
    def test_with_token(self):
        plan = _plan([
            EndpointSpec(path="/pets", method="GET", resource_name="pets"),
        ])
        config = build_dlt_config(plan, token="my-secret")

        assert config["client"]["auth"]["type"] == "bearer"
        assert config["client"]["auth"]["token"] == "my-secret"

    def test_without_token(self):
        plan = _plan([
            EndpointSpec(path="/pets", method="GET", resource_name="pets"),
        ])
        config = build_dlt_config(plan, token="")

        assert "auth" not in config["client"]

    def test_filters_non_get(self):
        plan = _plan([
            EndpointSpec(path="/pets", method="GET", resource_name="pets"),
            EndpointSpec(path="/order", method="POST", resource_name="orders"),
        ])
        config = build_dlt_config(plan)

        assert len(config["resources"]) == 1
        assert config["resources"][0]["name"] == "pets"

    def test_preserves_endpoint_config(self):
        plan = _plan([
            EndpointSpec(
                path="/pets",
                method="GET",
                resource_name="pets",
                primary_key="id",
                data_path="results",
                params={"limit": "50"},
            ),
        ])
        config = build_dlt_config(plan)

        resource = config["resources"][0]
        assert resource["primary_key"] == "id"
        assert resource["endpoint"]["data_selector"] == "results"
        assert resource["endpoint"]["params"] == {"limit": "50"}


# ---------------------------------------------------------------------------
# make_destination
# ---------------------------------------------------------------------------

class TestMakeDestination:
    def test_creates_callable(self):
        dest = make_destination("https://api.example.com", "petstore", batch_size=10)
        assert callable(dest)

    @respx.mock
    def test_destination_posts_to_ingestion(self):
        route = respx.post("https://api.example.com/ingest/petstore/pets/batch").mock(
            return_value=httpx.Response(200, json={"sent_count": 2, "status": "completed"})
        )

        dest = make_destination("https://api.example.com", "petstore")

        # The dlt decorator wraps the function; the original is in __wrapped__
        dest_fn = dest.__wrapped__
        dest_fn(
            items=[{"id": 1, "name": "Rex"}, {"id": 2, "name": "Luna"}],
            table={"name": "pets"},
        )

        assert route.called
        body = json.loads(route.calls[0].request.content)
        assert len(body) == 2
        assert body[0]["id"] == 1


# ---------------------------------------------------------------------------
# RunResult
# ---------------------------------------------------------------------------

class TestRunResult:
    def test_ok_when_completed(self):
        result = RunResult(
            endpoints_created=["pets"],
            pipeline_completed=True,
            records_loaded={"pets": 10},
        )
        assert result.ok
        assert result.total_loaded == 10

    def test_not_ok_when_pipeline_failed(self):
        result = RunResult(pipeline_completed=False)
        assert not result.ok

    def test_not_ok_with_errors(self):
        result = RunResult(
            pipeline_completed=True,
            errors=["something failed"],
        )
        assert not result.ok

    def test_summary(self):
        result = RunResult(
            endpoints_created=["pets"],
            endpoints_skipped=["orders"],
            pipeline_completed=True,
            records_loaded={"pets": 10},
        )
        summary = result.summary()
        assert summary["ok"] is True
        assert summary["endpoints_created"] == ["pets"]
        assert summary["endpoints_skipped"] == ["orders"]
        assert summary["total_loaded"] == 10
