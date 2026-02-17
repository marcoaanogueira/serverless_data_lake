"""
Tests for the Lakehouse Transformation Agent.

Tests cover:
- Pydantic model validation (TransformJob, TransformationPlan, TableMetadata)
- Runner result tracking (RunResult)
- Metadata building helpers
- Plan serialization roundtrip
"""

import json
import pytest

from agents.transformation_agent.models import (
    TableMetadata,
    TransformationPlan,
    TransformJob,
)
from agents.transformation_agent.runner import RunResult, JobResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_COLUMNS_PEOPLE = [
    {"name": "name", "type": "string", "required": True, "primary_key": False,
     "description": "The name of this person"},
    {"name": "height", "type": "string", "required": False, "primary_key": False,
     "description": "The height of the person in centimeters"},
    {"name": "mass", "type": "string", "required": False, "primary_key": False,
     "description": "The mass of the person in kilograms"},
    {"name": "birth_year", "type": "string", "required": False, "primary_key": False,
     "description": "Birth year using BBY/ABY format"},
    {"name": "homeworld", "type": "string", "required": False, "primary_key": False,
     "description": "URL of the homeworld planet"},
    {"name": "url", "type": "string", "required": True, "primary_key": True,
     "description": "The URL of this resource"},
]

SAMPLE_COLUMNS_PLANETS = [
    {"name": "name", "type": "string", "required": True, "primary_key": False,
     "description": "The name of this planet"},
    {"name": "climate", "type": "string", "required": False, "primary_key": False,
     "description": "The climate of this planet"},
    {"name": "population", "type": "string", "required": False, "primary_key": False,
     "description": "The population of this planet"},
    {"name": "url", "type": "string", "required": True, "primary_key": True,
     "description": "The URL of this resource"},
]

SAMPLE_DATA_PEOPLE = [
    {"name": "Luke Skywalker", "height": "172", "mass": "77",
     "birth_year": "19BBY", "homeworld": "https://swapi.dev/api/planets/1/",
     "url": "https://swapi.dev/api/people/1/"},
    {"name": "Darth Vader", "height": "202", "mass": "136",
     "birth_year": "41.9BBY", "homeworld": "https://swapi.dev/api/planets/1/",
     "url": "https://swapi.dev/api/people/4/"},
]

SAMPLE_DATA_PLANETS = [
    {"name": "Tatooine", "climate": "arid", "population": "200000",
     "url": "https://swapi.dev/api/planets/1/"},
    {"name": "Alderaan", "climate": "temperate", "population": "2000000000",
     "url": "https://swapi.dev/api/planets/2/"},
]


# ---------------------------------------------------------------------------
# TransformJob Tests
# ---------------------------------------------------------------------------


class TestTransformJob:
    def test_valid_job(self):
        job = TransformJob(
            domain="starwars",
            job_name="daily_people_count",
            query="SELECT COUNT(*) as cnt FROM starwars.silver.people",
            write_mode="overwrite",
            cron_schedule="day",
            description="Daily count of people in the Star Wars universe",
        )
        assert job.domain == "starwars"
        assert job.job_name == "daily_people_count"
        assert job.write_mode == "overwrite"

    def test_job_name_validation(self):
        with pytest.raises(ValueError, match="snake_case"):
            TransformJob(
                domain="starwars",
                job_name="DailyPeopleCount",  # Invalid: not snake_case
                query="SELECT 1",
            )

    def test_domain_validation(self):
        with pytest.raises(ValueError, match="snake_case"):
            TransformJob(
                domain="Star Wars",  # Invalid
                job_name="test",
                query="SELECT 1",
            )

    def test_job_with_dependencies(self):
        job = TransformJob(
            domain="sales",
            job_name="revenue_report",
            query="SELECT * FROM sales.gold.daily_revenue",
            schedule_type="dependency",
            dependencies=["daily_revenue"],
        )
        assert job.schedule_type == "dependency"
        assert job.dependencies == ["daily_revenue"]

    def test_job_defaults(self):
        job = TransformJob(
            domain="test",
            job_name="test_job",
            query="SELECT 1",
        )
        assert job.write_mode == "append"
        assert job.unique_key is None
        assert job.schedule_type == "cron"
        assert job.cron_schedule == "day"
        assert job.dependencies == []


# ---------------------------------------------------------------------------
# TableMetadata Tests
# ---------------------------------------------------------------------------


class TestTableMetadata:
    def test_valid_metadata(self):
        meta = TableMetadata(
            name="people",
            domain="starwars",
            layer="silver",
            columns=SAMPLE_COLUMNS_PEOPLE,
            sample_data=SAMPLE_DATA_PEOPLE,
            row_count=82,
        )
        assert meta.name == "people"
        assert len(meta.columns) == 6
        assert len(meta.sample_data) == 2
        assert meta.row_count == 82

    def test_metadata_defaults(self):
        meta = TableMetadata(name="test", domain="test")
        assert meta.layer == "silver"
        assert meta.columns == []
        assert meta.sample_data == []
        assert meta.row_count is None


# ---------------------------------------------------------------------------
# TransformationPlan Tests
# ---------------------------------------------------------------------------


class TestTransformationPlan:
    def test_valid_plan(self):
        plan = TransformationPlan(
            domain="starwars",
            source_tables=["people", "planets"],
            jobs=[
                TransformJob(
                    domain="starwars",
                    job_name="people_per_planet",
                    query=(
                        "SELECT p.name as planet, COUNT(pp.name) as people_count "
                        "FROM starwars.silver.people pp "
                        "JOIN starwars.silver.planets p ON pp.homeworld = p.url "
                        "GROUP BY p.name"
                    ),
                    description="Count of people per planet",
                ),
                TransformJob(
                    domain="starwars",
                    job_name="people_summary",
                    query="SELECT COUNT(*) as total FROM starwars.silver.people",
                    description="Overall people summary",
                ),
            ],
            rationale="Generated cross-table join using homeworld FK and basic aggregations",
        )
        assert len(plan.jobs) == 2
        assert plan.source_tables == ["people", "planets"]
        assert "homeworld" in plan.rationale

    def test_domain_validation(self):
        with pytest.raises(ValueError, match="snake_case"):
            TransformationPlan(
                domain="Star Wars",  # Invalid
                jobs=[],
            )

    def test_empty_plan(self):
        plan = TransformationPlan(domain="test")
        assert plan.jobs == []
        assert plan.source_tables == []
        assert plan.rationale == ""

    def test_serialization_roundtrip(self):
        plan = TransformationPlan(
            domain="starwars",
            source_tables=["people", "planets", "films"],
            jobs=[
                TransformJob(
                    domain="starwars",
                    job_name="people_per_planet",
                    query="SELECT * FROM starwars.silver.people",
                    write_mode="overwrite",
                    cron_schedule="day",
                    description="People per planet",
                ),
                TransformJob(
                    domain="starwars",
                    job_name="film_character_count",
                    query="SELECT * FROM starwars.silver.films",
                    write_mode="append",
                    unique_key="film_id",
                    description="Character count per film",
                ),
            ],
            rationale="Test roundtrip",
        )
        dumped = plan.model_dump()
        restored = TransformationPlan.model_validate(dumped)
        assert restored == plan

    def test_json_serialization(self):
        plan = TransformationPlan(
            domain="sales",
            source_tables=["orders"],
            jobs=[
                TransformJob(
                    domain="sales",
                    job_name="daily_revenue",
                    query="SELECT date, SUM(amount) FROM sales.silver.orders GROUP BY date",
                ),
            ],
        )
        json_str = json.dumps(plan.model_dump(), indent=2)
        restored = TransformationPlan.model_validate(json.loads(json_str))
        assert restored.domain == "sales"
        assert len(restored.jobs) == 1


# ---------------------------------------------------------------------------
# Runner Result Tests
# ---------------------------------------------------------------------------


class TestJobResult:
    def test_successful_result(self):
        result = JobResult(
            job_name="daily_count",
            domain="starwars",
            created=True,
        )
        assert result.ok is True

    def test_failed_result(self):
        result = JobResult(
            job_name="daily_count",
            domain="starwars",
            created=False,
            error="HTTP 500",
        )
        assert result.ok is False

    def test_created_but_error(self):
        result = JobResult(
            job_name="daily_count",
            domain="starwars",
            created=True,
            error="Trigger failed",
        )
        assert result.ok is False


class TestRunResult:
    def test_successful_run(self):
        result = RunResult(
            jobs_created=["job_a", "job_b"],
            jobs_triggered=["job_a"],
        )
        assert result.ok is True
        summary = result.summary()
        assert summary["total_created"] == 2
        assert summary["total_triggered"] == 1

    def test_failed_run_no_jobs(self):
        result = RunResult()
        assert result.ok is False

    def test_failed_run_with_errors(self):
        result = RunResult(
            jobs_created=["job_a"],
            errors=["[job_b] HTTP 500"],
        )
        assert result.ok is False

    def test_summary_format(self):
        result = RunResult(
            jobs_created=["a", "b", "c"],
            jobs_skipped=["d"],
            jobs_triggered=["a", "b"],
            errors=["[e] failed"],
        )
        summary = result.summary()
        assert summary["ok"] is False
        assert summary["total_created"] == 3
        assert summary["total_triggered"] == 2
        assert len(summary["errors"]) == 1


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


class TestInferType:
    def test_infer_types(self):
        from agents.transformation_agent.main import _infer_type

        assert _infer_type("hello") == "string"
        assert _infer_type(42) == "integer"
        assert _infer_type(3.14) == "float"
        assert _infer_type(True) == "boolean"
        assert _infer_type([1, 2, 3]) == "array"
        assert _infer_type({"key": "val"}) == "json"
        assert _infer_type(None) == "string"


# ---------------------------------------------------------------------------
# Ingestion result parsing tests (stdin piping)
# ---------------------------------------------------------------------------


class TestExtractTablesFromIngestionResult:
    def test_full_result(self):
        from agents.transformation_agent.main import extract_tables_from_ingestion_result

        result = {
            "ok": True,
            "endpoints_created": ["people", "planets", "films"],
            "endpoints_skipped": [],
            "pipeline_completed": True,
            "records_loaded": {"people": 82, "planets": 60, "films": 7},
            "total_loaded": 149,
            "errors": [],
        }
        tables = extract_tables_from_ingestion_result(result)
        assert tables == ["people", "planets", "films"]

    def test_with_skipped(self):
        from agents.transformation_agent.main import extract_tables_from_ingestion_result

        result = {
            "ok": True,
            "endpoints_created": ["people", "films"],
            "endpoints_skipped": ["planets"],
            "records_loaded": {"people": 82, "planets": 60, "films": 7},
        }
        tables = extract_tables_from_ingestion_result(result)
        assert set(tables) == {"people", "films", "planets"}

    def test_no_duplicates(self):
        from agents.transformation_agent.main import extract_tables_from_ingestion_result

        result = {
            "endpoints_created": ["people"],
            "endpoints_skipped": ["people"],
            "records_loaded": {"people": 82},
        }
        tables = extract_tables_from_ingestion_result(result)
        assert tables == ["people"]

    def test_records_loaded_fallback(self):
        from agents.transformation_agent.main import extract_tables_from_ingestion_result

        result = {
            "ok": True,
            "endpoints_created": [],
            "endpoints_skipped": [],
            "records_loaded": {"people": 82, "planets": 60},
        }
        tables = extract_tables_from_ingestion_result(result)
        assert set(tables) == {"people", "planets"}

    def test_empty_result(self):
        from agents.transformation_agent.main import extract_tables_from_ingestion_result

        result = {
            "ok": False,
            "endpoints_created": [],
            "endpoints_skipped": [],
            "records_loaded": {},
            "errors": ["something failed"],
        }
        tables = extract_tables_from_ingestion_result(result)
        assert tables == []

    def test_extra_tables_from_records(self):
        """Tables in records_loaded but not in endpoints lists (edge case)."""
        from agents.transformation_agent.main import extract_tables_from_ingestion_result

        result = {
            "endpoints_created": ["people"],
            "endpoints_skipped": [],
            "records_loaded": {"people": 82, "species": 37},
        }
        tables = extract_tables_from_ingestion_result(result)
        assert tables == ["people", "species"]


class TestIsIngestionResult:
    def test_valid_ingestion_result(self):
        from agents.transformation_agent.main import _is_ingestion_result

        assert _is_ingestion_result({
            "ok": True,
            "endpoints_created": ["people"],
            "pipeline_completed": True,
        }) is True

    def test_minimal_result(self):
        from agents.transformation_agent.main import _is_ingestion_result

        assert _is_ingestion_result({"records_loaded": {"a": 1}}) is True

    def test_not_ingestion_result(self):
        from agents.transformation_agent.main import _is_ingestion_result

        assert _is_ingestion_result({"domain": "starwars", "jobs": []}) is False


# ---------------------------------------------------------------------------
# Analyzer prompt formatting tests
# ---------------------------------------------------------------------------


class TestAnalyzerFormatting:
    def test_format_table_metadata(self):
        from agents.transformation_agent.analyzer import _format_table_metadata

        meta = TableMetadata(
            name="people",
            domain="starwars",
            layer="silver",
            columns=SAMPLE_COLUMNS_PEOPLE,
            sample_data=SAMPLE_DATA_PEOPLE,
            row_count=82,
        )
        formatted = _format_table_metadata(meta)
        assert "starwars.silver.people" in formatted
        assert "Row count: ~82" in formatted
        assert "name: string" in formatted
        assert "Luke Skywalker" in formatted
        assert "[PK]" in formatted
        assert "[required]" in formatted

    def test_format_table_no_samples(self):
        from agents.transformation_agent.analyzer import _format_table_metadata

        meta = TableMetadata(
            name="empty_table",
            domain="test",
            columns=[{"name": "id", "type": "integer"}],
        )
        formatted = _format_table_metadata(meta)
        assert "test.silver.empty_table" in formatted
        assert "id: integer" in formatted
        assert "Sample data" not in formatted
