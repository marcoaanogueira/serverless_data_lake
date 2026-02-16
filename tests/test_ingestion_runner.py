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
from unittest.mock import AsyncMock, patch

import pytest
import httpx
import respx

from agents.ingestion_agent.models import EndpointSpec, IngestionPlan
from agents.ingestion_agent.runner import (
    detect_data_path,
    detect_primary_key,
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
# detect_data_path
# ---------------------------------------------------------------------------

class TestDetectDataPath:
    """Tests for auto-detecting the data array path in API responses."""

    def test_plain_list_response(self):
        """API returns a bare list of dicts → data_path is empty."""
        path, records = detect_data_path(PET_RECORDS)
        assert path == ""
        assert records == PET_RECORDS

    def test_swapi_pagination_wrapper(self):
        """
        The exact SWAPI response structure that caused the original bug:
        count/next/previous are scalars, 'results' is the real data.
        """
        swapi_response = {
            "count": 82,
            "next": "https://swapi.dev/api/people/?page=2",
            "previous": None,
            "results": [
                {"name": "Luke Skywalker", "height": "172", "mass": "77"},
                {"name": "C-3PO", "height": "167", "mass": "75"},
            ],
        }
        path, records = detect_data_path(swapi_response)
        assert path == "results"
        assert len(records) == 2
        assert records[0]["name"] == "Luke Skywalker"

    def test_rick_and_morty_wrapper(self):
        """Rick & Morty API: info.next for pagination, 'results' for data."""
        rm_response = {
            "info": {"count": 826, "pages": 42, "next": "https://...", "prev": None},
            "results": [
                {"id": 1, "name": "Rick Sanchez", "status": "Alive"},
            ],
        }
        path, records = detect_data_path(rm_response)
        assert path == "results"
        assert records[0]["name"] == "Rick Sanchez"

    def test_nested_data_path(self):
        """Data array nested one level deep: response.data.items."""
        response = {
            "status": "ok",
            "data": {
                "items": [
                    {"id": 1, "name": "Item A"},
                    {"id": 2, "name": "Item B"},
                ],
            },
        }
        path, records = detect_data_path(response)
        assert path == "data.items"
        assert len(records) == 2

    def test_prefers_known_data_key(self):
        """When multiple arrays exist, prefer well-known names like 'results'."""
        response = {
            "results": [{"id": 1}],
            "errors": [{"code": "E001"}],
        }
        path, records = detect_data_path(response)
        assert path == "results"

    def test_multiple_arrays_picks_largest_if_no_preferred(self):
        """When no preferred name matches, pick the longest array."""
        response = {
            "things": [{"id": 1}, {"id": 2}, {"id": 3}],
            "metadata": [{"key": "value"}],
        }
        path, records = detect_data_path(response)
        assert path == "things"
        assert len(records) == 3

    def test_single_dict_response(self):
        """API returns a single dict (no array) → treat as single record."""
        response = {"id": 1, "name": "Rex", "status": "available"}
        path, records = detect_data_path(response)
        assert path == ""
        assert records == [response]

    def test_empty_dict(self):
        """Empty dict response."""
        path, records = detect_data_path({})
        assert path == ""
        assert records == []

    def test_non_dict_non_list(self):
        """Scalar response."""
        path, records = detect_data_path("just a string")
        assert path == ""
        assert records == []

    def test_empty_list(self):
        """Empty list response."""
        path, records = detect_data_path([])
        assert path == ""
        assert records == []

    def test_array_of_scalars_not_dicts(self):
        """List of non-dicts should still return the list."""
        path, records = detect_data_path([1, 2, 3])
        assert path == ""
        assert records == [1, 2, 3]

    def test_data_key_preferred_over_unknown(self):
        """'data' is preferred over unknown key names."""
        response = {
            "data": [{"id": 1}],
            "extras": [{"id": 2}],
        }
        path, records = detect_data_path(response)
        assert path == "data"


# ---------------------------------------------------------------------------
# detect_primary_key
# ---------------------------------------------------------------------------

class TestDetectPrimaryKey:
    """Tests for auto-detecting primary key from sample records."""

    def test_id_field(self):
        """'id' field is the top priority."""
        sample = {"id": 1, "name": "Luke", "height": "172"}
        assert detect_primary_key(sample) == "id"

    def test_resource_id_field(self):
        """'{singular_resource}_id' is second priority."""
        sample = {"person_id": 1, "name": "Luke"}
        assert detect_primary_key(sample, "people") == "person_id"

    def test_single_id_suffix_field(self):
        """Exactly one '_id' field is third priority."""
        sample = {"character_id": 1, "name": "Luke", "height": "172"}
        assert detect_primary_key(sample) == "character_id"

    def test_multiple_id_fields_skipped(self):
        """Multiple '_id' fields → skip to next heuristic."""
        sample = {"user_id": 1, "org_id": 2, "name": "Luke"}
        assert detect_primary_key(sample) == "name"

    def test_name_field(self):
        """'name' as natural key (SWAPI people, planets, etc.)."""
        sample = {"name": "Luke Skywalker", "height": "172", "mass": "77"}
        assert detect_primary_key(sample) == "name"

    def test_url_field_as_fallback(self):
        """'url' as last resort (SWAPI uses url as unique identifier)."""
        sample = {"url": "https://swapi.dev/api/people/1/", "height": "172"}
        assert detect_primary_key(sample) == "url"

    def test_no_candidate(self):
        """No recognizable PK field → returns None."""
        sample = {"height": "172", "mass": "77", "hair_color": "blond"}
        assert detect_primary_key(sample) is None

    def test_id_wins_over_name(self):
        """'id' takes priority over 'name'."""
        sample = {"id": 1, "name": "Luke"}
        assert detect_primary_key(sample) == "id"

    def test_swapi_people_sample(self):
        """Real SWAPI people record → should detect 'name'."""
        sample = {
            "name": "Luke Skywalker",
            "height": "172",
            "mass": "77",
            "hair_color": "blond",
            "skin_color": "fair",
            "eye_color": "blue",
            "birth_year": "19BBY",
            "gender": "male",
            "homeworld": "https://swapi.dev/api/planets/1/",
            "films": [],
            "species": [],
            "vehicles": [],
            "starships": [],
            "created": "2014-12-09T13:50:51.644000Z",
            "edited": "2014-12-20T21:17:56.891000Z",
            "url": "https://swapi.dev/api/people/1/",
        }
        assert detect_primary_key(sample, "people") == "name"


# ---------------------------------------------------------------------------
# fetch_sample (mocked HTTP)
# ---------------------------------------------------------------------------

class TestFetchSample:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_first_record_and_empty_path(self):
        """Bare list response → sample is first record, path is empty."""
        ep = EndpointSpec(path="/pet/findByStatus", method="GET", resource_name="pets", params={"status": "available"})
        respx.get("https://petstore.example.com/v3/pet/findByStatus").mock(
            return_value=httpx.Response(200, json=PET_RECORDS)
        )
        async with httpx.AsyncClient() as client:
            sample, path = await fetch_sample(client, "https://petstore.example.com/v3", ep)
        assert sample == PET_RECORDS[0]
        assert path == ""

    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_none_for_empty(self):
        ep = EndpointSpec(path="/pets", method="GET", resource_name="pets")
        respx.get("https://petstore.example.com/v3/pets").mock(
            return_value=httpx.Response(200, json=[])
        )
        async with httpx.AsyncClient() as client:
            sample, path = await fetch_sample(client, "https://petstore.example.com/v3", ep)
        assert sample is None

    @pytest.mark.asyncio
    @respx.mock
    async def test_auto_detects_data_path(self):
        """Agent has wrong/empty data_path but code auto-detects 'results'."""
        ep = EndpointSpec(path="/pets", method="GET", resource_name="pets", data_path="")
        respx.get("https://petstore.example.com/v3/pets").mock(
            return_value=httpx.Response(200, json={
                "count": 2,
                "next": None,
                "results": PET_RECORDS,
            })
        )
        async with httpx.AsyncClient() as client:
            sample, path = await fetch_sample(client, "https://petstore.example.com/v3", ep)
        assert sample == PET_RECORDS[0]
        assert path == "results"

    @pytest.mark.asyncio
    @respx.mock
    async def test_auto_detect_overrides_wrong_agent_path(self):
        """Agent says data_path='data' but real response has 'results'."""
        ep = EndpointSpec(path="/pets", method="GET", resource_name="pets", data_path="data")
        respx.get("https://petstore.example.com/v3/pets").mock(
            return_value=httpx.Response(200, json={
                "count": 2,
                "results": PET_RECORDS,
            })
        )
        async with httpx.AsyncClient() as client:
            sample, path = await fetch_sample(client, "https://petstore.example.com/v3", ep)
        assert sample == PET_RECORDS[0]
        assert path == "results"

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

    @pytest.mark.asyncio
    @respx.mock
    @patch("agents.ingestion_agent.runner.identify_primary_key", new_callable=AsyncMock)
    async def test_pk_agent_called_when_no_pk(self, mock_pk_agent):
        """
        When openapi_analyzer sets primary_key=None, the PK agent is called
        with the sample to identify the PK.
        """
        mock_pk_agent.return_value = "name"

        ep = EndpointSpec(
            path="/people/",
            method="GET",
            resource_name="people",
            primary_key=None,
            description="Star Wars characters",
        )
        swapi_sample = {
            "name": "Luke Skywalker",
            "height": "172",
            "mass": "77",
        }
        inferred = {
            "columns": [
                {"name": "name", "type": "string", "required": True, "primary_key": False},
                {"name": "height", "type": "string", "required": True, "primary_key": False},
                {"name": "mass", "type": "string", "required": True, "primary_key": False},
            ],
            "payload_keys": ["name", "height", "mass"],
        }

        respx.post("https://api.example.com/endpoints/infer").mock(
            return_value=httpx.Response(200, json=inferred)
        )
        create_route = respx.post("https://api.example.com/endpoints").mock(
            return_value=httpx.Response(200, json=ENDPOINT_RESPONSE)
        )

        async with httpx.AsyncClient() as client:
            await infer_and_create_endpoint(
                client, "https://api.example.com", "starwars", ep, swapi_sample
            )

        # PK agent was called with sample and resource_name
        mock_pk_agent.assert_awaited_once_with(swapi_sample, "people")

        create_body = json.loads(create_route.calls[0].request.content)
        name_col = next(c for c in create_body["columns"] if c["name"] == "name")
        assert name_col["primary_key"] is True
        assert name_col["required"] is True

    @pytest.mark.asyncio
    @respx.mock
    @patch("agents.ingestion_agent.runner.identify_primary_key", new_callable=AsyncMock)
    async def test_pk_agent_returns_id(self, mock_pk_agent):
        """PK agent identifies 'id' for a resource with an id field."""
        mock_pk_agent.return_value = "id"

        ep = EndpointSpec(
            path="/characters",
            method="GET",
            resource_name="characters",
            primary_key=None,
        )
        sample = {"id": 1, "name": "Rick Sanchez", "status": "Alive"}
        inferred = {
            "columns": [
                {"name": "id", "type": "integer", "required": True, "primary_key": False},
                {"name": "name", "type": "string", "required": True, "primary_key": False},
                {"name": "status", "type": "string", "required": True, "primary_key": False},
            ],
            "payload_keys": ["id", "name", "status"],
        }

        respx.post("https://api.example.com/endpoints/infer").mock(
            return_value=httpx.Response(200, json=inferred)
        )
        create_route = respx.post("https://api.example.com/endpoints").mock(
            return_value=httpx.Response(200, json=ENDPOINT_RESPONSE)
        )

        async with httpx.AsyncClient() as client:
            await infer_and_create_endpoint(
                client, "https://api.example.com", "rickandmorty", ep, sample
            )

        create_body = json.loads(create_route.calls[0].request.content)
        id_col = next(c for c in create_body["columns"] if c["name"] == "id")
        assert id_col["primary_key"] is True
        assert id_col["required"] is True

    @pytest.mark.asyncio
    @respx.mock
    @patch("agents.ingestion_agent.runner.identify_primary_key", new_callable=AsyncMock)
    async def test_pk_agent_not_called_when_pk_already_set(self, mock_pk_agent):
        """When openapi_analyzer already set primary_key, PK agent is NOT called."""
        ep = EndpointSpec(path="/pets", method="GET", resource_name="pets", primary_key="id", description="All pets")

        respx.post("https://api.example.com/endpoints/infer").mock(
            return_value=httpx.Response(200, json=INFERRED_SCHEMA)
        )
        respx.post("https://api.example.com/endpoints").mock(
            return_value=httpx.Response(200, json=ENDPOINT_RESPONSE)
        )

        async with httpx.AsyncClient() as client:
            await infer_and_create_endpoint(
                client, "https://api.example.com", "petstore", ep, PET_RECORDS[0]
            )

        mock_pk_agent.assert_not_awaited()

    @pytest.mark.asyncio
    @respx.mock
    @patch("agents.ingestion_agent.runner.generate_field_descriptions", new_callable=AsyncMock)
    async def test_applies_spec_descriptions_to_columns(self, mock_desc_agent):
        """
        When field_descriptions are set from the OpenAPI spec, they should
        be applied to columns. The description agent is called only for
        fields WITHOUT spec descriptions.
        """
        mock_desc_agent.return_value = {"status": "Availability status of the pet"}

        ep = EndpointSpec(
            path="/pets",
            method="GET",
            resource_name="pets",
            primary_key="id",
            description="All pets",
            field_descriptions={
                "id": "Unique identifier for the pet",
                "name": "Name of the pet",
            },
        )

        respx.post("https://api.example.com/endpoints/infer").mock(
            return_value=httpx.Response(200, json=INFERRED_SCHEMA)
        )
        create_route = respx.post("https://api.example.com/endpoints").mock(
            return_value=httpx.Response(200, json=ENDPOINT_RESPONSE)
        )

        async with httpx.AsyncClient() as client:
            await infer_and_create_endpoint(
                client, "https://api.example.com", "petstore", ep, PET_RECORDS[0]
            )

        create_body = json.loads(create_route.calls[0].request.content)

        # Spec descriptions applied
        id_col = next(c for c in create_body["columns"] if c["name"] == "id")
        assert id_col["description"] == "Unique identifier for the pet"

        name_col = next(c for c in create_body["columns"] if c["name"] == "name")
        assert name_col["description"] == "Name of the pet"

        # Agent-generated description for remaining field
        status_col = next(c for c in create_body["columns"] if c["name"] == "status")
        assert status_col["description"] == "Availability status of the pet"

        # Description agent was called for the field without spec description
        mock_desc_agent.assert_awaited_once_with(
            PET_RECORDS[0], "pets", ["status"]
        )

    @pytest.mark.asyncio
    @respx.mock
    @patch("agents.ingestion_agent.runner.generate_field_descriptions", new_callable=AsyncMock)
    async def test_all_descriptions_from_spec_skips_agent(self, mock_desc_agent):
        """When all fields have spec descriptions, the agent is not called."""
        ep = EndpointSpec(
            path="/pets",
            method="GET",
            resource_name="pets",
            primary_key="id",
            field_descriptions={
                "id": "Pet ID",
                "name": "Pet name",
                "status": "Pet status",
            },
        )

        respx.post("https://api.example.com/endpoints/infer").mock(
            return_value=httpx.Response(200, json=INFERRED_SCHEMA)
        )
        create_route = respx.post("https://api.example.com/endpoints").mock(
            return_value=httpx.Response(200, json=ENDPOINT_RESPONSE)
        )

        async with httpx.AsyncClient() as client:
            await infer_and_create_endpoint(
                client, "https://api.example.com", "petstore", ep, PET_RECORDS[0]
            )

        # Agent NOT called because all fields had spec descriptions
        mock_desc_agent.assert_not_awaited()

        create_body = json.loads(create_route.calls[0].request.content)
        for col in create_body["columns"]:
            assert "description" in col

    @pytest.mark.asyncio
    @respx.mock
    @patch("agents.ingestion_agent.runner.generate_field_descriptions", new_callable=AsyncMock)
    async def test_description_agent_failure_does_not_block(self, mock_desc_agent):
        """When the description agent fails, endpoint creation still succeeds."""
        mock_desc_agent.side_effect = RuntimeError("Model unavailable")

        ep = EndpointSpec(
            path="/pets",
            method="GET",
            resource_name="pets",
            primary_key="id",
        )

        respx.post("https://api.example.com/endpoints/infer").mock(
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
        # Endpoint was still created, just without generated descriptions
        assert create_route.called

    @pytest.mark.asyncio
    @respx.mock
    @patch("agents.ingestion_agent.runner.identify_primary_key", new_callable=AsyncMock)
    async def test_falls_back_to_heuristic_when_pk_agent_fails(self, mock_pk_agent):
        """When PK agent raises an exception, falls back to detect_primary_key heuristic."""
        mock_pk_agent.side_effect = RuntimeError("Model unavailable")

        ep = EndpointSpec(
            path="/people/",
            method="GET",
            resource_name="people",
            primary_key=None,
        )
        swapi_sample = {
            "name": "Luke Skywalker",
            "height": "172",
            "mass": "77",
        }
        inferred = {
            "columns": [
                {"name": "name", "type": "string", "required": True, "primary_key": False},
                {"name": "height", "type": "string", "required": True, "primary_key": False},
                {"name": "mass", "type": "string", "required": True, "primary_key": False},
            ],
            "payload_keys": ["name", "height", "mass"],
        }

        respx.post("https://api.example.com/endpoints/infer").mock(
            return_value=httpx.Response(200, json=inferred)
        )
        create_route = respx.post("https://api.example.com/endpoints").mock(
            return_value=httpx.Response(200, json=ENDPOINT_RESPONSE)
        )

        async with httpx.AsyncClient() as client:
            await infer_and_create_endpoint(
                client, "https://api.example.com", "starwars", ep, swapi_sample
            )

        # Heuristic fallback detected "name" as PK
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

        # List existing endpoints → empty (no existing endpoints in domain)
        respx.get("https://api.example.com/endpoints").mock(
            return_value=httpx.Response(200, json=[])
        )
        # Source API
        respx.get("https://petstore.example.com/v3/pet/findByStatus").mock(
            return_value=httpx.Response(200, json=PET_RECORDS)
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

        # List existing endpoints → "pets" already exists
        respx.get("https://api.example.com/endpoints").mock(
            return_value=httpx.Response(200, json=[{"name": "pets"}])
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

        # List existing endpoints → empty
        respx.get("https://api.example.com/endpoints").mock(
            return_value=httpx.Response(200, json=[])
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

        # List existing endpoints → empty
        respx.get("https://api.example.com/endpoints").mock(
            return_value=httpx.Response(200, json=[])
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

    @pytest.mark.asyncio
    @respx.mock
    async def test_auto_detects_data_path_and_propagates(self):
        """
        Regression test for the original bug: agent sets wrong/empty data_path,
        code auto-detects 'results' from the SWAPI-style paginated response
        and propagates it back to the endpoint for dlt.
        """
        swapi_people = [
            {"name": "Luke Skywalker", "height": "172", "mass": "77"},
        ]
        plan = _plan([
            EndpointSpec(
                path="/people/",
                method="GET",
                resource_name="people",
                primary_key="name",
                data_path="",  # Agent didn't set data_path
            ),
        ])

        # List existing endpoints → empty
        respx.get("https://api.example.com/endpoints").mock(
            return_value=httpx.Response(200, json=[])
        )
        # Source API returns SWAPI-style paginated response
        respx.get("https://petstore.example.com/v3/people/").mock(
            return_value=httpx.Response(200, json={
                "count": 82,
                "next": "https://swapi.dev/api/people/?page=2",
                "previous": None,
                "results": swapi_people,
            })
        )
        # Infer (called with the REAL sample, not the wrapper)
        infer_route = respx.post("https://api.example.com/endpoints/infer").mock(
            return_value=httpx.Response(200, json={
                "columns": [
                    {"name": "name", "type": "string", "required": True, "primary_key": False},
                    {"name": "height", "type": "string", "required": True, "primary_key": False},
                    {"name": "mass", "type": "string", "required": True, "primary_key": False},
                ],
                "payload_keys": ["name", "height", "mass"],
            })
        )
        respx.post("https://api.example.com/endpoints").mock(
            return_value=httpx.Response(200, json=ENDPOINT_RESPONSE)
        )

        created, skipped, errors = await setup_endpoints(
            plan, "petstore", "https://api.example.com"
        )

        assert created == ["people"]
        assert errors == []

        # Verify infer was called with the REAL sample (Luke Skywalker),
        # NOT the pagination wrapper (count, next, previous, results)
        infer_body = json.loads(infer_route.calls[0].request.content)
        assert infer_body["payload"] == swapi_people[0]
        assert "count" not in infer_body["payload"]
        assert "next" not in infer_body["payload"]

        # Verify data_path was auto-detected and propagated back
        assert plan.endpoints[0].data_path == "results"


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
