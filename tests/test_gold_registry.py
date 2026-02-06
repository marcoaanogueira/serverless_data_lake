"""
Tests for Schema Registry - Gold Layer Methods

Tests covering:
- save_gold_job: saves job config to S3
- get_gold_job: retrieves job config from S3
- list_gold_jobs: lists all gold jobs
- delete_gold_job: deletes a gold job config
"""

import pytest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'layers', 'shared', 'python'))
sys.modules['shared.infrastructure'] = MagicMock()

from shared.schema_registry import SchemaRegistry


@pytest.fixture
def mock_s3():
    with patch('shared.schema_registry.boto3') as mock_boto:
        mock_client = MagicMock()
        mock_boto.client.return_value = mock_client
        yield mock_client


@pytest.fixture
def registry(mock_s3):
    reg = SchemaRegistry(bucket_name="test-artifacts", provision_infrastructure=False)
    return reg


# =============================================================================
# save_gold_job
# =============================================================================

class TestSaveGoldJob:
    def test_save_new_job(self, registry, mock_s3):
        """Should save a new gold job config to S3"""
        # Mock get_gold_job to return None (new job)
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "GetObject"
        )

        config = {
            "query": "SELECT * FROM silver.vendas",
            "partition_column": "created_at",
            "schedule_type": "cron",
            "cron_schedule": "hour",
        }

        result = registry.save_gold_job("sales", "all_vendas", config)

        assert result["domain"] == "sales"
        assert result["job_name"] == "all_vendas"
        assert "created_at" in result
        assert "updated_at" in result

        # Verify S3 put was called
        mock_s3.put_object.assert_called_once()
        call_args = mock_s3.put_object.call_args
        assert call_args[1]["Key"] == "schemas/sales/gold/all_vendas/config.yaml"

    def test_save_updates_existing(self, registry, mock_s3):
        """Should preserve created_at when updating existing job"""
        import yaml

        existing = {
            "domain": "sales",
            "job_name": "all_vendas",
            "created_at": "2025-01-01T00:00:00",
            "updated_at": "2025-01-01T00:00:00",
        }

        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: yaml.dump(existing).encode("utf-8"))
        }

        config = {"query": "SELECT 1"}
        result = registry.save_gold_job("sales", "all_vendas", config)

        assert result["created_at"] == "2025-01-01T00:00:00"
        assert result["updated_at"] != "2025-01-01T00:00:00"


# =============================================================================
# get_gold_job
# =============================================================================

class TestGetGoldJob:
    def test_get_existing_job(self, registry, mock_s3):
        """Should return job config when it exists"""
        import yaml

        job_data = {
            "domain": "sales",
            "job_name": "all_vendas",
            "query": "SELECT * FROM silver.vendas",
        }

        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: yaml.dump(job_data).encode("utf-8"))
        }

        result = registry.get_gold_job("sales", "all_vendas")

        assert result is not None
        assert result["domain"] == "sales"
        assert result["job_name"] == "all_vendas"

    def test_get_nonexistent_job(self, registry, mock_s3):
        """Should return None when job doesn't exist"""
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "GetObject"
        )

        result = registry.get_gold_job("sales", "nonexistent")

        assert result is None


# =============================================================================
# list_gold_jobs
# =============================================================================

class TestListGoldJobs:
    def test_list_all_jobs(self, registry, mock_s3):
        """Should list all gold jobs across domains"""
        import yaml

        job1 = {"domain": "sales", "job_name": "all_vendas"}
        job2 = {"domain": "finance", "job_name": "revenue"}

        paginator = MagicMock()
        mock_s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "schemas/sales/gold/all_vendas/config.yaml"},
                    {"Key": "schemas/finance/gold/revenue/config.yaml"},
                    {"Key": "schemas/sales/bronze/vendas/latest.yaml"},  # Should be skipped
                ]
            }
        ]

        call_count = [0]
        def mock_get_object(**kwargs):
            key = kwargs["Key"]
            if "all_vendas" in key:
                return {"Body": MagicMock(read=lambda: yaml.dump(job1).encode("utf-8"))}
            elif "revenue" in key:
                return {"Body": MagicMock(read=lambda: yaml.dump(job2).encode("utf-8"))}
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

        mock_s3.get_object.side_effect = mock_get_object

        result = registry.list_gold_jobs()

        assert len(result) == 2
        names = [j["job_name"] for j in result]
        assert "all_vendas" in names
        assert "revenue" in names

    def test_list_jobs_by_domain(self, registry, mock_s3):
        """Should filter jobs by domain"""
        import yaml

        job1 = {"domain": "sales", "job_name": "all_vendas"}

        paginator = MagicMock()
        mock_s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "schemas/sales/gold/all_vendas/config.yaml"},
                ]
            }
        ]

        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: yaml.dump(job1).encode("utf-8"))
        }

        result = registry.list_gold_jobs(domain="sales")

        assert len(result) == 1
        # Verify the prefix used for filtering
        paginator.paginate.assert_called_with(
            Bucket="test-artifacts",
            Prefix="schemas/sales/gold/"
        )

    def test_list_jobs_empty(self, registry, mock_s3):
        """Should return empty list when no jobs exist"""
        paginator = MagicMock()
        mock_s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [{"Contents": []}]

        result = registry.list_gold_jobs()

        assert result == []


# =============================================================================
# delete_gold_job
# =============================================================================

class TestDeleteGoldJob:
    def test_delete_existing_job(self, registry, mock_s3):
        """Should delete job and return True"""
        mock_s3.head_object.return_value = {}

        result = registry.delete_gold_job("sales", "all_vendas")

        assert result is True
        mock_s3.delete_object.assert_called_once()

    def test_delete_nonexistent_job(self, registry, mock_s3):
        """Should return False when job doesn't exist"""
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "HeadObject"
        )

        result = registry.delete_gold_job("sales", "nonexistent")

        assert result is False


# =============================================================================
# S3 Path Structure
# =============================================================================

class TestGoldPaths:
    def test_gold_job_path(self, registry, mock_s3):
        """Gold jobs should use the correct S3 path pattern"""
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "GetObject"
        )

        registry.save_gold_job("analytics", "daily_summary", {"query": "SELECT 1"})

        call_args = mock_s3.put_object.call_args
        assert call_args[1]["Key"] == "schemas/analytics/gold/daily_summary/config.yaml"
        assert call_args[1]["Bucket"] == "test-artifacts"
