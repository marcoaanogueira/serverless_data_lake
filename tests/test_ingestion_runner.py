"""
Tests for the Ingestion Runner.

Tests cover:
- GET-only endpoint filtering (models.get_only)
- Data extraction with data_path
- fetch_endpoint (mocked HTTP)
- send_to_ingestion (mocked HTTP)
- Full run() integration (mocked HTTP both sides)
- POST endpoints are skipped
"""

import json
import pytest
import httpx
import respx

from agents.ingestion_agent.models import EndpointSpec, IngestionPlan
from agents.ingestion_agent.runner import (
    extract_data,
    fetch_endpoint,
    send_to_ingestion,
    run,
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

INVENTORY_DATA = {"available": 10, "sold": 5}


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
# fetch_endpoint (mocked HTTP)
# ---------------------------------------------------------------------------

class TestFetchEndpoint:
    @pytest.mark.asyncio
    @respx.mock
    async def test_fetch_list(self):
        ep = EndpointSpec(path="/pet/findByStatus", method="GET", resource_name="pets", params={"status": "available"})
        respx.get("https://petstore.example.com/v3/pet/findByStatus").mock(
            return_value=httpx.Response(200, json=PET_RECORDS)
        )
        async with httpx.AsyncClient() as client:
            records = await fetch_endpoint(client, "https://petstore.example.com/v3", ep)
        assert len(records) == 2

    @pytest.mark.asyncio
    @respx.mock
    async def test_fetch_with_data_path(self):
        ep = EndpointSpec(path="/pets", method="GET", resource_name="pets", data_path="results")
        respx.get("https://petstore.example.com/v3/pets").mock(
            return_value=httpx.Response(200, json={"results": PET_RECORDS})
        )
        async with httpx.AsyncClient() as client:
            records = await fetch_endpoint(client, "https://petstore.example.com/v3", ep)
        assert len(records) == 2

    @pytest.mark.asyncio
    @respx.mock
    async def test_fetch_with_token(self):
        ep = EndpointSpec(path="/pets", method="GET", resource_name="pets")
        route = respx.get("https://petstore.example.com/v3/pets").mock(
            return_value=httpx.Response(200, json=[])
        )
        async with httpx.AsyncClient() as client:
            await fetch_endpoint(client, "https://petstore.example.com/v3", ep, token="my-token")
        assert route.calls[0].request.headers["Authorization"] == "Bearer my-token"

    @pytest.mark.asyncio
    @respx.mock
    async def test_fetch_no_token_no_auth_header(self):
        ep = EndpointSpec(path="/pets", method="GET", resource_name="pets")
        route = respx.get("https://petstore.example.com/v3/pets").mock(
            return_value=httpx.Response(200, json=[])
        )
        async with httpx.AsyncClient() as client:
            await fetch_endpoint(client, "https://petstore.example.com/v3", ep, token="")
        assert "Authorization" not in route.calls[0].request.headers

    @pytest.mark.asyncio
    @respx.mock
    async def test_fetch_http_error_raises(self):
        ep = EndpointSpec(path="/pets", method="GET", resource_name="pets")
        respx.get("https://petstore.example.com/v3/pets").mock(
            return_value=httpx.Response(500, text="Internal Server Error")
        )
        async with httpx.AsyncClient() as client:
            with pytest.raises(httpx.HTTPStatusError):
                await fetch_endpoint(client, "https://petstore.example.com/v3", ep)


# ---------------------------------------------------------------------------
# send_to_ingestion (mocked HTTP)
# ---------------------------------------------------------------------------

class TestSendToIngestion:
    @pytest.mark.asyncio
    @respx.mock
    async def test_send_batch(self):
        respx.post("https://ingestion.example.com/ingest/petstore/pets/batch").mock(
            return_value=httpx.Response(200, json={"sent_count": 2, "status": "completed"})
        )
        async with httpx.AsyncClient() as client:
            sent = await send_to_ingestion(
                client,
                "https://ingestion.example.com",
                "petstore",
                "pets",
                PET_RECORDS,
            )
        assert sent == 2

    @pytest.mark.asyncio
    @respx.mock
    async def test_send_respects_batch_size(self):
        route = respx.post("https://ingestion.example.com/ingest/petstore/pets/batch").mock(
            return_value=httpx.Response(200, json={"sent_count": 1, "status": "completed"})
        )
        records = [{"id": i} for i in range(5)]
        async with httpx.AsyncClient() as client:
            sent = await send_to_ingestion(
                client,
                "https://ingestion.example.com",
                "petstore",
                "pets",
                records,
                batch_size=2,
            )
        # 5 records / batch_size 2 = 3 requests (2+2+1)
        assert len(route.calls) == 3
        assert sent == 3  # 1 per batch response


# ---------------------------------------------------------------------------
# Full run() integration
# ---------------------------------------------------------------------------

class TestRun:
    @pytest.mark.asyncio
    @respx.mock
    async def test_run_get_only(self):
        """POST endpoints are skipped, GET endpoints are fetched and sent."""
        plan = _plan([
            EndpointSpec(
                path="/pet/findByStatus",
                method="GET",
                resource_name="pets",
                params={"status": "available"},
            ),
            EndpointSpec(
                path="/store/order",
                method="POST",
                resource_name="orders",
            ),
        ])

        # Mock source API
        respx.get("https://petstore.example.com/v3/pet/findByStatus").mock(
            return_value=httpx.Response(200, json=PET_RECORDS)
        )
        # Mock ingestion API
        respx.post("https://ingestion.example.com/ingest/petstore/pets/batch").mock(
            return_value=httpx.Response(200, json={"sent_count": 2, "status": "completed"})
        )

        result = await run(
            plan=plan,
            domain="petstore",
            ingestion_url="https://ingestion.example.com",
        )

        assert result.ok
        assert result.total_fetched == 2
        assert result.total_sent == 2
        # Only 1 endpoint processed (GET), POST skipped
        assert len(result.endpoints) == 1
        assert result.endpoints[0].resource_name == "pets"

    @pytest.mark.asyncio
    @respx.mock
    async def test_run_source_error_continues(self):
        """If one endpoint fails to fetch, others still run."""
        plan = _plan([
            EndpointSpec(path="/pets", method="GET", resource_name="pets"),
            EndpointSpec(path="/inventory", method="GET", resource_name="inventory"),
        ])

        respx.get("https://petstore.example.com/v3/pets").mock(
            return_value=httpx.Response(500, text="error")
        )
        respx.get("https://petstore.example.com/v3/inventory").mock(
            return_value=httpx.Response(200, json=INVENTORY_DATA)
        )
        respx.post("https://ingestion.example.com/ingest/petstore/inventory/batch").mock(
            return_value=httpx.Response(200, json={"sent_count": 1, "status": "completed"})
        )

        result = await run(
            plan=plan,
            domain="petstore",
            ingestion_url="https://ingestion.example.com",
        )

        assert not result.ok  # one endpoint errored
        assert len(result.endpoints) == 2
        assert result.endpoints[0].errors  # pets failed
        assert result.endpoints[1].records_sent == 1  # inventory succeeded

    @pytest.mark.asyncio
    async def test_run_empty_plan(self):
        """No endpoints → empty result, no errors."""
        plan = _plan([])
        result = await run(
            plan=plan,
            domain="petstore",
            ingestion_url="https://ingestion.example.com",
        )
        assert result.ok
        assert result.total_fetched == 0
        assert result.total_sent == 0

    @pytest.mark.asyncio
    async def test_run_only_post_endpoints(self):
        """Plan with only POST endpoints → nothing runs."""
        plan = _plan([
            EndpointSpec(path="/store/order", method="POST", resource_name="orders"),
        ])
        result = await run(
            plan=plan,
            domain="petstore",
            ingestion_url="https://ingestion.example.com",
        )
        assert result.ok
        assert len(result.endpoints) == 0


# ---------------------------------------------------------------------------
# RunResult / EndpointResult
# ---------------------------------------------------------------------------

class TestResultModels:
    def test_run_result_summary(self):
        result = RunResult(endpoints=[
            EndpointResult(resource_name="pets", records_fetched=10, records_sent=10),
            EndpointResult(resource_name="orders", records_fetched=5, records_sent=5),
        ])
        summary = result.summary()
        assert summary["total_endpoints"] == 2
        assert summary["total_fetched"] == 15
        assert summary["total_sent"] == 15
        assert summary["ok"] is True

    def test_run_result_with_errors(self):
        result = RunResult(endpoints=[
            EndpointResult(resource_name="pets", records_fetched=0, errors=["HTTP 500"]),
        ])
        assert not result.ok
        summary = result.summary()
        assert summary["ok"] is False
        assert summary["endpoints"][0]["errors"] == ["HTTP 500"]
