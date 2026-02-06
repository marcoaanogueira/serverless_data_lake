"""
Tests for Schema Registry - Bronze/Silver Layer Paths

Tests covering:
- Layer-based S3 paths (bronze/silver)
- register_silver_table (create-once behavior)
- list_silver_tables
- Backward compatibility of existing methods with bronze layer
"""

import pytest
from unittest.mock import MagicMock, patch, call
from botocore.exceptions import ClientError
from datetime import datetime

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'layers', 'shared', 'python'))

# Mock infrastructure module before importing schema_registry
sys.modules['shared.infrastructure'] = MagicMock()

from shared.schema_registry import SchemaRegistry


@pytest.fixture
def mock_s3():
    with patch('shared.schema_registry.boto3') as mock_boto3:
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        yield mock_client


@pytest.fixture
def registry(mock_s3):
    reg = SchemaRegistry(bucket_name="test-artifacts", provision_infrastructure=False)
    return reg


# =============================================================================
# Path Generation Tests
# =============================================================================

class TestGetSchemaPath:
    """Tests for _get_schema_path with layer parameter"""

    def test_default_layer_is_bronze(self, registry):
        """Default layer should be bronze"""
        path = registry._get_schema_path("sales", "orders")
        assert path == "schemas/sales/bronze/orders/latest.yaml"

    def test_explicit_bronze_layer(self, registry):
        """Explicit bronze layer should work"""
        path = registry._get_schema_path("sales", "orders", layer="bronze")
        assert path == "schemas/sales/bronze/orders/latest.yaml"

    def test_silver_layer(self, registry):
        """Silver layer should use silver in path"""
        path = registry._get_schema_path("sales", "orders", layer="silver")
        assert path == "schemas/sales/silver/orders/latest.yaml"

    def test_versioned_bronze_path(self, registry):
        """Versioned path should include bronze"""
        path = registry._get_schema_path("sales", "orders", version=1)
        assert path == "schemas/sales/bronze/orders/v1.yaml"

    def test_versioned_silver_path(self, registry):
        """Versioned path with silver layer"""
        path = registry._get_schema_path("sales", "orders", version=3, layer="silver")
        assert path == "schemas/sales/silver/orders/v3.yaml"

    def test_different_domains(self, registry):
        """Different domains should produce correct paths"""
        assert registry._get_schema_path("ads", "campaigns") == "schemas/ads/bronze/campaigns/latest.yaml"
        assert registry._get_schema_path("finance", "transactions", layer="silver") == "schemas/finance/silver/transactions/latest.yaml"


# =============================================================================
# Register Silver Table Tests
# =============================================================================

class TestRegisterSilverTable:
    """Tests for register_silver_table method"""

    def test_register_new_silver_table(self, registry, mock_s3):
        """Should create YAML when silver table doesn't exist yet"""
        # Simulate table not found (head_object raises ClientError)
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )

        result = registry.register_silver_table("sales", "orders", "s3://bucket/silver/sales/orders")

        assert result is True
        mock_s3.put_object.assert_called_once()

        put_call = mock_s3.put_object.call_args
        assert put_call.kwargs["Bucket"] == "test-artifacts"
        assert put_call.kwargs["Key"] == "schemas/sales/silver/orders/latest.yaml"
        assert put_call.kwargs["ContentType"] == "application/x-yaml"

        # Verify YAML content
        body = put_call.kwargs["Body"].decode("utf-8")
        assert "name: orders" in body
        assert "domain: sales" in body
        assert "location: s3://bucket/silver/sales/orders" in body
        assert "created_at:" in body

    def test_skip_if_already_exists(self, registry, mock_s3):
        """Should return False and not write if table already registered"""
        # Simulate table already exists (head_object succeeds)
        mock_s3.head_object.return_value = {"ContentLength": 100}

        result = registry.register_silver_table("sales", "orders", "s3://bucket/silver/sales/orders")

        assert result is False
        mock_s3.put_object.assert_not_called()

    def test_create_once_semantics(self, registry, mock_s3):
        """Calling register twice should only create once"""
        # First call: not found
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )
        result1 = registry.register_silver_table("sales", "orders", "s3://bucket/silver/sales/orders")
        assert result1 is True

        # Second call: now exists
        mock_s3.head_object.side_effect = None
        mock_s3.head_object.return_value = {"ContentLength": 100}
        result2 = registry.register_silver_table("sales", "orders", "s3://bucket/silver/sales/orders")
        assert result2 is False

        # put_object should have been called only once
        assert mock_s3.put_object.call_count == 1

    def test_no_updated_at_field(self, registry, mock_s3):
        """Silver YAML should NOT contain updated_at field"""
        mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )

        registry.register_silver_table("sales", "orders", "s3://bucket/silver/sales/orders")

        body = mock_s3.put_object.call_args.kwargs["Body"].decode("utf-8")
        assert "updated_at" not in body


# =============================================================================
# List Silver Tables Tests
# =============================================================================

class TestListSilverTables:
    """Tests for list_silver_tables method"""

    def test_list_silver_tables_returns_data(self, registry, mock_s3):
        """Should return silver table data from S3"""
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "schemas/sales/silver/orders/latest.yaml"},
                    {"Key": "schemas/sales/silver/products/latest.yaml"},
                ]
            }
        ]

        yaml_data = {
            "schemas/sales/silver/orders/latest.yaml": b"name: orders\ndomain: sales\nlocation: s3://bucket/silver/sales/orders\ncreated_at: '2026-01-01'\n",
            "schemas/sales/silver/products/latest.yaml": b"name: products\ndomain: sales\nlocation: s3://bucket/silver/sales/products\ncreated_at: '2026-01-02'\n",
        }

        def mock_get_object(Bucket, Key):
            body = MagicMock()
            body.read.return_value = yaml_data[Key]
            return {"Body": body}

        mock_s3.get_object.side_effect = mock_get_object

        tables = registry.list_silver_tables()

        assert len(tables) == 2
        assert tables[0]["name"] == "orders"
        assert tables[0]["domain"] == "sales"
        assert tables[1]["name"] == "products"

    def test_list_silver_tables_with_domain_filter(self, registry, mock_s3):
        """Should filter by domain prefix"""
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": []}]

        registry.list_silver_tables(domain="sales")

        # Verify prefix used in pagination
        paginate_call = mock_paginator.paginate.call_args
        assert paginate_call.kwargs["Prefix"] == "schemas/sales/silver/"

    def test_list_silver_tables_without_domain(self, registry, mock_s3):
        """Without domain filter should use broad prefix"""
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": []}]

        registry.list_silver_tables()

        paginate_call = mock_paginator.paginate.call_args
        assert paginate_call.kwargs["Prefix"] == "schemas/"

    def test_list_silver_tables_ignores_bronze(self, registry, mock_s3):
        """Should skip bronze paths even when listed"""
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "schemas/sales/bronze/orders/latest.yaml"},
                    {"Key": "schemas/sales/silver/orders/latest.yaml"},
                ]
            }
        ]

        body = MagicMock()
        body.read.return_value = b"name: orders\ndomain: sales\nlocation: s3://test\ncreated_at: '2026-01-01'\n"
        mock_s3.get_object.return_value = {"Body": body}

        tables = registry.list_silver_tables()

        # Should only have the silver one
        assert len(tables) == 1
        mock_s3.get_object.assert_called_once()

    def test_list_silver_tables_ignores_versioned_files(self, registry, mock_s3):
        """Should only pick latest.yaml, not versioned files"""
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "schemas/sales/silver/orders/v1.yaml"},
                    {"Key": "schemas/sales/silver/orders/latest.yaml"},
                ]
            }
        ]

        body = MagicMock()
        body.read.return_value = b"name: orders\ndomain: sales\nlocation: s3://test\ncreated_at: '2026-01-01'\n"
        mock_s3.get_object.return_value = {"Body": body}

        tables = registry.list_silver_tables()

        assert len(tables) == 1

    def test_list_silver_tables_empty(self, registry, mock_s3):
        """Should return empty list when no silver tables exist"""
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": []}]

        tables = registry.list_silver_tables()
        assert tables == []

    def test_list_silver_tables_handles_client_error(self, registry, mock_s3):
        """Should return empty list on S3 errors"""
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "Bucket not found"}},
            "ListObjectsV2"
        )

        tables = registry.list_silver_tables()
        assert tables == []


# =============================================================================
# List All (Bronze) Tests
# =============================================================================

class TestListAllBronze:
    """Tests for list_all method with bronze layer paths"""

    def test_list_all_uses_bronze_path(self, registry, mock_s3):
        """list_all should only return bronze schemas"""
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "schemas/sales/bronze/orders/latest.yaml"},
                    {"Key": "schemas/sales/silver/orders/latest.yaml"},
                ]
            }
        ]

        # Mock get to return None (we just want to test filtering)
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}}, "GetObject"
        )

        registry.list_all()

        # get_object should only be called for the bronze path
        calls = mock_s3.get_object.call_args_list
        keys = [c.kwargs["Key"] for c in calls]
        assert all("/bronze/" in k for k in keys)
        assert not any("/silver/" in k for k in keys)

    def test_list_all_with_domain_filter(self, registry, mock_s3):
        """list_all with domain filter should use bronze prefix"""
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": []}]

        registry.list_all(domain="sales")

        paginate_call = mock_paginator.paginate.call_args
        assert paginate_call.kwargs["Prefix"] == "schemas/sales/bronze/"
