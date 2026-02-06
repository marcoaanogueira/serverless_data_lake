"""
Tests for Transform Jobs API

Tests covering:
- Job CRUD operations (create, list, get, update, delete)
- Job execution trigger
- Request validation
"""

import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'layers', 'shared', 'python'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas', 'transform_jobs'))

# Mock infrastructure before importing
sys.modules['shared.infrastructure'] = MagicMock()

from lambdas.transform_jobs.main import app


# =============================================================================
# Test Client Setup
# =============================================================================

@pytest.fixture
def mock_registry():
    with patch('lambdas.transform_jobs.main.registry') as mock_reg:
        yield mock_reg


@pytest.fixture
def client(mock_registry):
    return TestClient(app)


SAMPLE_JOB = {
    "domain": "sales",
    "job_name": "all_vendas",
    "query": "SELECT * FROM silver.vendas",
    "partition_column": "created_at",
    "schedule_type": "cron",
    "cron_schedule": "hour",
    "status": "active",
    "created_at": "2026-01-01T00:00:00",
    "updated_at": "2026-01-01T00:00:00",
}


# =============================================================================
# Health Check
# =============================================================================

class TestHealthCheck:
    def test_health_check(self, client):
        response = client.get("/")
        assert response.status_code == 200
        assert response.json()["service"] == "transform_jobs"


# =============================================================================
# Create Job
# =============================================================================

class TestCreateJob:
    def test_create_job_success(self, client, mock_registry):
        mock_registry.get_gold_job.return_value = None
        mock_registry.save_gold_job.return_value = SAMPLE_JOB

        response = client.post("/transform/jobs", json={
            "domain": "sales",
            "job_name": "all_vendas",
            "query": "SELECT * FROM silver.vendas",
            "partition_column": "created_at",
            "schedule_type": "cron",
            "cron_schedule": "hour",
        })

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "sales/all_vendas"
        assert data["job_name"] == "all_vendas"
        assert data["domain"] == "sales"
        assert data["query"] == "SELECT * FROM silver.vendas"

    def test_create_job_already_exists(self, client, mock_registry):
        mock_registry.get_gold_job.return_value = SAMPLE_JOB

        response = client.post("/transform/jobs", json={
            "domain": "sales",
            "job_name": "all_vendas",
            "query": "SELECT 1",
            "partition_column": "created_at",
        })

        assert response.status_code == 400
        assert "already exists" in response.json()["detail"]

    def test_create_job_invalid_name(self, client):
        response = client.post("/transform/jobs", json={
            "domain": "sales",
            "job_name": "Invalid-Name",
            "query": "SELECT 1",
            "partition_column": "created_at",
        })

        assert response.status_code == 422

    def test_create_job_invalid_domain(self, client):
        response = client.post("/transform/jobs", json={
            "domain": "Sales Domain",
            "job_name": "test_job",
            "query": "SELECT 1",
            "partition_column": "created_at",
        })

        assert response.status_code == 422

    def test_create_dependency_job(self, client, mock_registry):
        dep_job = {**SAMPLE_JOB, "schedule_type": "dependency", "dependencies": ["job_a", "job_b"]}
        mock_registry.get_gold_job.return_value = None
        mock_registry.save_gold_job.return_value = dep_job

        response = client.post("/transform/jobs", json={
            "domain": "sales",
            "job_name": "all_vendas",
            "query": "SELECT * FROM silver.vendas",
            "partition_column": "created_at",
            "schedule_type": "dependency",
            "dependencies": ["job_a", "job_b"],
        })

        assert response.status_code == 200
        data = response.json()
        assert data["schedule_type"] == "dependency"
        assert data["dependencies"] == ["job_a", "job_b"]


# =============================================================================
# List Jobs
# =============================================================================

class TestListJobs:
    def test_list_jobs_empty(self, client, mock_registry):
        mock_registry.list_gold_jobs.return_value = []

        response = client.get("/transform/jobs")

        assert response.status_code == 200
        assert response.json() == []

    def test_list_jobs_with_results(self, client, mock_registry):
        job2 = {**SAMPLE_JOB, "job_name": "revenue_report", "domain": "finance"}
        mock_registry.list_gold_jobs.return_value = [SAMPLE_JOB, job2]

        response = client.get("/transform/jobs")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2

    def test_list_jobs_with_domain_filter(self, client, mock_registry):
        mock_registry.list_gold_jobs.return_value = [SAMPLE_JOB]

        response = client.get("/transform/jobs?domain=sales")

        assert response.status_code == 200
        mock_registry.list_gold_jobs.assert_called_with(domain="sales")


# =============================================================================
# Get Job
# =============================================================================

class TestGetJob:
    def test_get_job_success(self, client, mock_registry):
        mock_registry.get_gold_job.return_value = SAMPLE_JOB

        response = client.get("/transform/jobs/sales/all_vendas")

        assert response.status_code == 200
        data = response.json()
        assert data["job_name"] == "all_vendas"

    def test_get_job_not_found(self, client, mock_registry):
        mock_registry.get_gold_job.return_value = None

        response = client.get("/transform/jobs/sales/nonexistent")

        assert response.status_code == 404


# =============================================================================
# Update Job
# =============================================================================

class TestUpdateJob:
    def test_update_job_success(self, client, mock_registry):
        updated = {**SAMPLE_JOB, "query": "SELECT id, total FROM silver.vendas"}
        mock_registry.get_gold_job.return_value = SAMPLE_JOB.copy()
        mock_registry.save_gold_job.return_value = updated

        response = client.put("/transform/jobs/sales/all_vendas", json={
            "query": "SELECT id, total FROM silver.vendas",
        })

        assert response.status_code == 200

    def test_update_job_not_found(self, client, mock_registry):
        mock_registry.get_gold_job.return_value = None

        response = client.put("/transform/jobs/sales/nonexistent", json={
            "query": "SELECT 1",
        })

        assert response.status_code == 404


# =============================================================================
# Delete Job
# =============================================================================

class TestDeleteJob:
    def test_delete_job_success(self, client, mock_registry):
        mock_registry.delete_gold_job.return_value = True

        response = client.delete("/transform/jobs/sales/all_vendas")

        assert response.status_code == 200
        assert "deleted" in response.json()["message"]

    def test_delete_job_not_found(self, client, mock_registry):
        mock_registry.delete_gold_job.return_value = False

        response = client.delete("/transform/jobs/sales/nonexistent")

        assert response.status_code == 404


# =============================================================================
# Run Job (Step Functions)
# =============================================================================

class TestRunJob:
    def test_run_job_no_state_machine(self, client, mock_registry):
        """Should return 503 if Step Functions not configured"""
        mock_registry.get_gold_job.return_value = SAMPLE_JOB

        with patch('lambdas.transform_jobs.main.STATE_MACHINE_ARN', ''):
            response = client.post("/transform/jobs/sales/all_vendas/run")

        assert response.status_code == 503
        assert "not configured" in response.json()["detail"]

    def test_run_job_not_found(self, client, mock_registry):
        mock_registry.get_gold_job.return_value = None

        response = client.post("/transform/jobs/sales/nonexistent/run")

        assert response.status_code == 404
