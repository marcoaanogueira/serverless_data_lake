"""
Regression tests for example plan files.

Ensures all example plans in examples/*_plan.json:
  - Can be loaded and validated as IngestionPlan
  - Produce valid dlt rest_api configs
  - Have proper pagination configuration
  - Have valid endpoint specs (GET-only filtering, data_path, etc.)

These tests act as a regression suite: when you fix the pipeline for a
new API, add its plan to examples/ and it will be automatically tested.
"""

import json
from pathlib import Path

import pytest

from agents.ingestion_agent.models import IngestionPlan, PaginationConfig


EXAMPLES_DIR = Path(__file__).parent.parent / "examples"
PLAN_FILES = sorted(EXAMPLES_DIR.glob("*_plan.json"))


def _load_plan(path: Path) -> IngestionPlan:
    """Load and validate a plan file."""
    with open(path) as f:
        data = json.load(f)
    return IngestionPlan.model_validate(data)


# ---------------------------------------------------------------------------
# Parametrized: every plan file gets tested automatically
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "plan_path",
    PLAN_FILES,
    ids=[p.stem for p in PLAN_FILES],
)
class TestExamplePlans:
    """Suite that runs against every *_plan.json in examples/."""

    def test_loads_and_validates(self, plan_path: Path):
        """Plan JSON parses into a valid IngestionPlan."""
        plan = _load_plan(plan_path)
        assert plan.base_url
        assert plan.api_name

    def test_has_endpoints(self, plan_path: Path):
        """Plan has at least one endpoint."""
        plan = _load_plan(plan_path)
        assert len(plan.endpoints) > 0, f"{plan_path.name} has no endpoints"

    def test_all_endpoints_are_get(self, plan_path: Path):
        """All endpoints in example plans should be GET."""
        plan = _load_plan(plan_path)
        for ep in plan.endpoints:
            assert ep.method.upper() == "GET", (
                f"{plan_path.name}: endpoint {ep.resource_name} has method {ep.method}"
            )

    def test_resource_names_are_snake_case(self, plan_path: Path):
        """All resource names must be valid snake_case."""
        plan = _load_plan(plan_path)
        for ep in plan.endpoints:
            # The model validator already checks this, but let's be explicit
            assert ep.resource_name.islower(), (
                f"{plan_path.name}: {ep.resource_name} is not lowercase"
            )

    def test_produces_valid_dlt_config(self, plan_path: Path):
        """Plan converts to a valid dlt rest_api config dict."""
        plan = _load_plan(plan_path)
        config = plan.to_dlt_config()

        assert "client" in config
        assert "resources" in config
        assert config["client"]["base_url"] == plan.base_url
        assert "paginator" in config["client"]

        # Each endpoint becomes a dlt resource
        assert len(config["resources"]) == len(plan.endpoints)

    def test_pagination_is_configured(self, plan_path: Path):
        """Plan has explicit pagination config (not left as default)."""
        plan = _load_plan(plan_path)
        paginator = plan.pagination.to_dlt_paginator()

        # Should be either a dict with type or the string "auto"
        if isinstance(paginator, dict):
            assert "type" in paginator
        else:
            assert paginator in ("auto", "single_page")

    def test_dlt_config_resources_have_required_fields(self, plan_path: Path):
        """Each dlt resource has name and endpoint path."""
        plan = _load_plan(plan_path)
        config = plan.to_dlt_config()

        for resource in config["resources"]:
            assert "name" in resource
            assert "endpoint" in resource
            assert "path" in resource["endpoint"]

    def test_serialization_roundtrip(self, plan_path: Path):
        """Plan survives dump → re-validate without data loss."""
        plan = _load_plan(plan_path)
        dumped = plan.model_dump()
        restored = IngestionPlan.model_validate(dumped)
        assert restored.base_url == plan.base_url
        assert restored.api_name == plan.api_name
        assert len(restored.endpoints) == len(plan.endpoints)
        for orig, rest in zip(plan.endpoints, restored.endpoints):
            assert orig.resource_name == rest.resource_name
            assert orig.path == rest.path


# ---------------------------------------------------------------------------
# Specific plan validations (known expected values)
# ---------------------------------------------------------------------------

class TestRickAndMortyPlan:
    def test_pagination(self):
        plan = _load_plan(EXAMPLES_DIR / "rickandmorty_plan.json")
        assert plan.pagination.type == "json_link"
        assert plan.pagination.next_url_path == "info.next"

    def test_data_path(self):
        plan = _load_plan(EXAMPLES_DIR / "rickandmorty_plan.json")
        for ep in plan.endpoints:
            assert ep.data_path == "results"


class TestPokeAPIPlan:
    def test_pagination(self):
        plan = _load_plan(EXAMPLES_DIR / "pokeapi_plan.json")
        assert plan.pagination.type == "json_link"
        assert plan.pagination.next_url_path == "next"

    def test_data_path(self):
        plan = _load_plan(EXAMPLES_DIR / "pokeapi_plan.json")
        for ep in plan.endpoints:
            assert ep.data_path == "results"

    def test_has_limit_param(self):
        plan = _load_plan(EXAMPLES_DIR / "pokeapi_plan.json")
        for ep in plan.endpoints:
            assert "limit" in ep.params


class TestSWAPIPlan:
    def test_pagination(self):
        plan = _load_plan(EXAMPLES_DIR / "swapi_plan.json")
        assert plan.pagination.type == "json_link"
        assert plan.pagination.next_url_path == "next"

    def test_data_path(self):
        plan = _load_plan(EXAMPLES_DIR / "swapi_plan.json")
        for ep in plan.endpoints:
            assert ep.data_path == "results"

    def test_films_primary_key(self):
        plan = _load_plan(EXAMPLES_DIR / "swapi_plan.json")
        films = next(ep for ep in plan.endpoints if ep.resource_name == "films")
        assert films.primary_key == "title"


class TestPetstorePlan:
    def test_no_pagination_needed(self):
        plan = _load_plan(EXAMPLES_DIR / "petstore_plan.json")
        assert plan.pagination.type == "single_page"


class TestJSONPlaceholderPlan:
    def test_no_pagination_needed(self):
        plan = _load_plan(EXAMPLES_DIR / "jsonplaceholder_plan.json")
        assert plan.pagination.type == "single_page"
