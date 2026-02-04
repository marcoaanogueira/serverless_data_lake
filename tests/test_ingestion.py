"""
Tests for the Data Ingestion API

Tests cover:
- Schema validation
- Endpoint existence checks
- Data ingestion with validation
- Batch ingestion
- Legacy endpoint compatibility
"""

import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from botocore.exceptions import ClientError

# Add lambdas directory to path and mock boto3 before importing
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas', 'serverless_ingestion'))
sys.modules['boto3'] = MagicMock()

from lambdas.serverless_ingestion.main import app
from lambdas.serverless_ingestion.schema_validator import (
    SchemaValidator,
    SchemaNotFoundError,
    SchemaValidationError,
)


client = TestClient(app)


# Sample schema YAML content
SAMPLE_SCHEMA_YAML = """
name: orders
domain: sales
version: 1
mode: manual
description: Customer orders
created_at: "2024-01-15T10:30:00.000000"
updated_at: "2024-01-15T10:30:00.000000"
schema:
  columns:
    - name: order_id
      type: integer
      required: true
      primary_key: true
    - name: customer_id
      type: integer
      required: true
    - name: total_amount
      type: float
      required: true
    - name: status
      type: string
      required: false
    - name: created_at
      type: timestamp
      required: false
"""

SINGLE_COLUMN_SCHEMA_YAML = """
name: raw_events
domain: tracking
version: 1
mode: single_column
description: Raw events without schema validation
created_at: "2024-01-15T10:30:00.000000"
updated_at: "2024-01-15T10:30:00.000000"
schema:
  columns: []
"""


class TestSchemaValidator:
    """Tests for SchemaValidator class"""

    def test_endpoint_exists_true(self):
        """Test endpoint_exists returns True when schema exists"""
        with patch.object(SchemaValidator, '__init__', lambda x, y=None: None):
            validator = SchemaValidator()
            validator.s3 = MagicMock()
            validator.bucket = "test-bucket"
            validator.prefix = "schemas"
            validator._cache = {}

            # head_object succeeds = endpoint exists
            validator.s3.head_object.return_value = {}

            assert validator.endpoint_exists("sales", "orders") is True
            validator.s3.head_object.assert_called_once_with(
                Bucket="test-bucket",
                Key="schemas/sales/orders/latest.yaml"
            )

    def test_endpoint_exists_false(self):
        """Test endpoint_exists returns False when schema doesn't exist"""
        with patch.object(SchemaValidator, '__init__', lambda x, y=None: None):
            validator = SchemaValidator()
            validator.s3 = MagicMock()
            validator.bucket = "test-bucket"
            validator.prefix = "schemas"
            validator._cache = {}

            # head_object raises 404 = endpoint doesn't exist
            error_response = {"Error": {"Code": "404"}}
            validator.s3.head_object.side_effect = ClientError(error_response, "HeadObject")

            assert validator.endpoint_exists("sales", "nonexistent") is False

    def test_get_schema_success(self):
        """Test get_schema returns schema dict when exists"""
        with patch.object(SchemaValidator, '__init__', lambda x, y=None: None):
            validator = SchemaValidator()
            validator.s3 = MagicMock()
            validator.bucket = "test-bucket"
            validator.prefix = "schemas"
            validator._cache = {}

            # Mock S3 response
            mock_body = MagicMock()
            mock_body.read.return_value = SAMPLE_SCHEMA_YAML.encode("utf-8")
            validator.s3.get_object.return_value = {"Body": mock_body}

            schema = validator.get_schema("sales", "orders")

            assert schema["name"] == "orders"
            assert schema["domain"] == "sales"
            assert len(schema["schema"]["columns"]) == 5

    def test_get_schema_not_found(self):
        """Test get_schema raises SchemaNotFoundError when doesn't exist"""
        with patch.object(SchemaValidator, '__init__', lambda x, y=None: None):
            validator = SchemaValidator()
            validator.s3 = MagicMock()
            validator.bucket = "test-bucket"
            validator.prefix = "schemas"
            validator._cache = {}

            error_response = {"Error": {"Code": "NoSuchKey"}}
            validator.s3.get_object.side_effect = ClientError(error_response, "GetObject")

            with pytest.raises(SchemaNotFoundError) as exc_info:
                validator.get_schema("sales", "nonexistent")

            assert "not found" in str(exc_info.value)

    def test_get_schema_uses_cache(self):
        """Test get_schema uses cache on second call"""
        with patch.object(SchemaValidator, '__init__', lambda x, y=None: None):
            validator = SchemaValidator()
            validator.s3 = MagicMock()
            validator.bucket = "test-bucket"
            validator.prefix = "schemas"
            validator._cache = {}

            mock_body = MagicMock()
            mock_body.read.return_value = SAMPLE_SCHEMA_YAML.encode("utf-8")
            validator.s3.get_object.return_value = {"Body": mock_body}

            # First call - hits S3
            validator.get_schema("sales", "orders")
            # Second call - should use cache
            validator.get_schema("sales", "orders")

            # S3 should only be called once
            assert validator.s3.get_object.call_count == 1

    def test_validate_success(self):
        """Test validate succeeds with valid payload"""
        with patch.object(SchemaValidator, '__init__', lambda x, y=None: None):
            validator = SchemaValidator()
            validator.s3 = MagicMock()
            validator.bucket = "test-bucket"
            validator.prefix = "schemas"
            validator._cache = {}

            mock_body = MagicMock()
            mock_body.read.return_value = SAMPLE_SCHEMA_YAML.encode("utf-8")
            validator.s3.get_object.return_value = {"Body": mock_body}

            payload = {
                "order_id": 123,
                "customer_id": 456,
                "total_amount": 99.99,
            }

            result = validator.validate("sales", "orders", payload)

            assert result["order_id"] == 123
            assert result["customer_id"] == 456
            assert result["total_amount"] == 99.99

    def test_validate_missing_required_field(self):
        """Test validate raises error when required field is missing"""
        with patch.object(SchemaValidator, '__init__', lambda x, y=None: None):
            validator = SchemaValidator()
            validator.s3 = MagicMock()
            validator.bucket = "test-bucket"
            validator.prefix = "schemas"
            validator._cache = {}

            mock_body = MagicMock()
            mock_body.read.return_value = SAMPLE_SCHEMA_YAML.encode("utf-8")
            validator.s3.get_object.return_value = {"Body": mock_body}

            # Missing required fields: order_id, customer_id, total_amount
            payload = {"status": "pending"}

            with pytest.raises(SchemaValidationError) as exc_info:
                validator.validate("sales", "orders", payload)

            assert len(exc_info.value.errors) > 0

    def test_validate_wrong_type(self):
        """Test validate raises error when field has wrong type"""
        with patch.object(SchemaValidator, '__init__', lambda x, y=None: None):
            validator = SchemaValidator()
            validator.s3 = MagicMock()
            validator.bucket = "test-bucket"
            validator.prefix = "schemas"
            validator._cache = {}

            mock_body = MagicMock()
            mock_body.read.return_value = SAMPLE_SCHEMA_YAML.encode("utf-8")
            validator.s3.get_object.return_value = {"Body": mock_body}

            payload = {
                "order_id": "not_an_integer",  # Should be integer
                "customer_id": 456,
                "total_amount": 99.99,
            }

            with pytest.raises(SchemaValidationError) as exc_info:
                validator.validate("sales", "orders", payload)

            errors = exc_info.value.errors
            assert any("order_id" in e["field"] for e in errors)

    def test_validate_single_column_mode_accepts_anything(self):
        """Test validate in single_column mode accepts any payload"""
        with patch.object(SchemaValidator, '__init__', lambda x, y=None: None):
            validator = SchemaValidator()
            validator.s3 = MagicMock()
            validator.bucket = "test-bucket"
            validator.prefix = "schemas"
            validator._cache = {}

            mock_body = MagicMock()
            mock_body.read.return_value = SINGLE_COLUMN_SCHEMA_YAML.encode("utf-8")
            validator.s3.get_object.return_value = {"Body": mock_body}

            # Any payload should be accepted
            payload = {"anything": "goes", "nested": {"data": [1, 2, 3]}}

            result = validator.validate("tracking", "raw_events", payload)
            assert result == payload

    def test_list_endpoints(self):
        """Test list_endpoints returns available endpoints"""
        with patch.object(SchemaValidator, '__init__', lambda x, y=None: None):
            validator = SchemaValidator()
            validator.s3 = MagicMock()
            validator.bucket = "test-bucket"
            validator.prefix = "schemas"
            validator._cache = {}

            # Mock paginator response
            mock_paginator = MagicMock()
            mock_paginator.paginate.return_value = [
                {
                    "Contents": [
                        {"Key": "schemas/sales/orders/latest.yaml"},
                        {"Key": "schemas/sales/customers/latest.yaml"},
                        {"Key": "schemas/finance/invoices/latest.yaml"},
                    ]
                }
            ]
            validator.s3.get_paginator.return_value = mock_paginator

            endpoints = validator.list_endpoints()

            assert len(endpoints) == 3
            assert {"domain": "sales", "name": "orders"} in endpoints
            assert {"domain": "sales", "name": "customers"} in endpoints
            assert {"domain": "finance", "name": "invoices"} in endpoints

    def test_list_endpoints_filtered_by_domain(self):
        """Test list_endpoints filters by domain"""
        with patch.object(SchemaValidator, '__init__', lambda x, y=None: None):
            validator = SchemaValidator()
            validator.s3 = MagicMock()
            validator.bucket = "test-bucket"
            validator.prefix = "schemas"
            validator._cache = {}

            mock_paginator = MagicMock()
            mock_paginator.paginate.return_value = [
                {
                    "Contents": [
                        {"Key": "schemas/sales/orders/latest.yaml"},
                        {"Key": "schemas/sales/customers/latest.yaml"},
                    ]
                }
            ]
            validator.s3.get_paginator.return_value = mock_paginator

            endpoints = validator.list_endpoints(domain="sales")

            # Verify prefix was set correctly for filtering
            mock_paginator.paginate.assert_called_once()
            call_args = mock_paginator.paginate.call_args
            assert call_args[1]["Prefix"] == "schemas/sales/"


class TestIngestionAPI:
    """Tests for the Ingestion API endpoints"""

    @patch("lambdas.serverless_ingestion.main.schema_validator")
    @patch("lambdas.serverless_ingestion.main.firehose_client")
    def test_ingest_valid_payload(self, mock_firehose, mock_validator):
        """Test successful ingestion with valid payload"""
        mock_validator.endpoint_exists.return_value = True
        mock_validator.validate.return_value = {
            "order_id": 123,
            "customer_id": 456,
            "total_amount": 99.99,
        }

        response = client.post(
            "/ingest/sales/orders",
            json={"data": {"order_id": 123, "customer_id": 456, "total_amount": 99.99}},
        )

        assert response.status_code == 200
        result = response.json()
        assert result["status"] == "success"
        assert result["endpoint"] == "sales/orders"
        assert result["validated"] is True
        mock_firehose.put_record.assert_called_once()

    @patch("lambdas.serverless_ingestion.main.schema_validator")
    def test_ingest_endpoint_not_found(self, mock_validator):
        """Test ingestion fails when endpoint doesn't exist"""
        mock_validator.endpoint_exists.return_value = False

        response = client.post(
            "/ingest/sales/nonexistent",
            json={"data": {"some": "data"}},
        )

        assert response.status_code == 404
        assert "not found" in response.json()["detail"]

    @patch("lambdas.serverless_ingestion.main.schema_validator")
    @patch("lambdas.serverless_ingestion.main.firehose_client")
    def test_ingest_validation_error_non_strict(self, mock_firehose, mock_validator):
        """Test ingestion continues with invalid payload in non-strict mode"""
        # Import the exception from the same place the main module uses
        from lambdas.serverless_ingestion.main import SchemaValidationError as MainSchemaValidationError

        mock_validator.endpoint_exists.return_value = True
        mock_validator.validate.side_effect = MainSchemaValidationError(
            "Validation failed",
            errors=[{"field": "order_id", "message": "required", "type": "missing"}],
        )

        response = client.post(
            "/ingest/sales/orders?strict=false",
            json={"data": {"invalid": "payload"}},
        )

        assert response.status_code == 200
        result = response.json()
        assert result["validated"] is False
        # Data should still be sent to Firehose
        mock_firehose.put_record.assert_called_once()

    @patch("lambdas.serverless_ingestion.main.schema_validator")
    def test_ingest_validation_error_strict(self, mock_validator):
        """Test ingestion fails with invalid payload in strict mode"""
        # Import the exception from the same place the main module uses
        from lambdas.serverless_ingestion.main import SchemaValidationError as MainSchemaValidationError

        mock_validator.endpoint_exists.return_value = True
        mock_validator.validate.side_effect = MainSchemaValidationError(
            "Validation failed",
            errors=[{"field": "order_id", "message": "required", "type": "missing"}],
        )

        response = client.post(
            "/ingest/sales/orders?strict=true",
            json={"data": {"invalid": "payload"}},
        )

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["error"] == "Payload validation failed"
        assert len(detail["validation_errors"]) > 0

    @patch("lambdas.serverless_ingestion.main.schema_validator")
    @patch("lambdas.serverless_ingestion.main.firehose_client")
    def test_ingest_skip_validation(self, mock_firehose, mock_validator):
        """Test ingestion with validation disabled"""
        mock_validator.endpoint_exists.return_value = True

        response = client.post(
            "/ingest/sales/orders?validate=false",
            json={"data": {"any": "data"}},
        )

        assert response.status_code == 200
        result = response.json()
        assert result["validated"] is False
        # validate should not be called
        mock_validator.validate.assert_not_called()

    @patch("lambdas.serverless_ingestion.main.schema_validator")
    @patch("lambdas.serverless_ingestion.main.firehose_client")
    def test_ingest_batch_success(self, mock_firehose, mock_validator):
        """Test batch ingestion with multiple records"""
        mock_validator.endpoint_exists.return_value = True
        mock_validator.validate.side_effect = lambda d, n, p: p

        records = [
            {"order_id": 1, "customer_id": 100, "total_amount": 10.0},
            {"order_id": 2, "customer_id": 200, "total_amount": 20.0},
            {"order_id": 3, "customer_id": 300, "total_amount": 30.0},
        ]

        response = client.post(
            "/ingest/sales/orders/batch",
            json=records,
        )

        assert response.status_code == 200
        result = response.json()
        assert result["total_records"] == 3
        assert result["validated_count"] == 3
        assert mock_firehose.put_record.call_count == 3

    @patch("lambdas.serverless_ingestion.main.schema_validator")
    def test_list_endpoints(self, mock_validator):
        """Test listing available endpoints"""
        mock_validator.list_endpoints.return_value = [
            {"domain": "sales", "name": "orders"},
            {"domain": "sales", "name": "customers"},
        ]

        response = client.get("/endpoints")

        assert response.status_code == 200
        result = response.json()
        assert result["count"] == 2
        assert len(result["endpoints"]) == 2

    @patch("lambdas.serverless_ingestion.main.schema_validator")
    def test_get_endpoint_schema(self, mock_validator):
        """Test getting endpoint schema"""
        mock_validator.get_schema.return_value = {
            "name": "orders",
            "domain": "sales",
            "schema": {"columns": []},
        }

        response = client.get("/endpoints/sales/orders")

        assert response.status_code == 200
        result = response.json()
        assert result["name"] == "orders"
        assert result["domain"] == "sales"

    @patch("lambdas.serverless_ingestion.main.schema_validator")
    def test_get_endpoint_schema_not_found(self, mock_validator):
        """Test getting schema for non-existent endpoint"""
        # Import the exception from the same place the main module uses
        from lambdas.serverless_ingestion.main import SchemaNotFoundError as MainSchemaNotFoundError

        mock_validator.get_schema.side_effect = MainSchemaNotFoundError(
            "Endpoint 'sales/nonexistent' not found"
        )

        response = client.get("/endpoints/sales/nonexistent")

        assert response.status_code == 404


class TestLegacyEndpoint:
    """Tests for the legacy endpoint compatibility"""

    @patch("lambdas.serverless_ingestion.main.schema_validator")
    @patch("lambdas.serverless_ingestion.main.firehose_client")
    def test_legacy_endpoint_with_new_schema(self, mock_firehose, mock_validator):
        """Test legacy endpoint uses new validation when schema exists"""
        mock_validator.endpoint_exists.return_value = True
        mock_validator.validate.return_value = {"some_key": "some_value"}

        response = client.post(
            "/send_data_bronze/decolares/vendas",
            json={"data": {"some_key": "some_value"}},
        )

        assert response.status_code == 200
        mock_validator.endpoint_exists.assert_called_with("decolares", "vendas")

    @patch("lambdas.serverless_ingestion.main.schema_validator")
    @patch("lambdas.serverless_ingestion.main.firehose_client")
    def test_legacy_endpoint_fallback(self, mock_firehose, mock_validator):
        """Test legacy endpoint falls back to old behavior when no schema"""
        mock_validator.endpoint_exists.return_value = False

        response = client.post(
            "/send_data_bronze/decolares/vendas",
            json={"data": {"some_key": "some_value"}},
        )

        assert response.status_code == 200
        result = response.json()
        assert "legacy mode" in result["message"]
        mock_firehose.put_record.assert_called_once()


class TestValidationTypes:
    """Tests for type validation logic"""

    def test_type_mapping_coverage(self):
        """Ensure all DataType enum values are mapped"""
        from lambdas.serverless_ingestion.schema_validator import TYPE_MAPPING

        expected_types = [
            "string", "integer", "float", "boolean",
            "timestamp", "date", "json", "array", "decimal"
        ]

        for t in expected_types:
            assert t in TYPE_MAPPING, f"Missing type mapping for: {t}"

    def test_validate_types_returns_issues(self):
        """Test validate_types returns list of issues"""
        with patch.object(SchemaValidator, '__init__', lambda x, y=None: None):
            validator = SchemaValidator()
            validator.s3 = MagicMock()
            validator.bucket = "test-bucket"
            validator.prefix = "schemas"
            validator._cache = {}

            mock_body = MagicMock()
            mock_body.read.return_value = SAMPLE_SCHEMA_YAML.encode("utf-8")
            validator.s3.get_object.return_value = {"Body": mock_body}

            # Valid payload - no issues
            payload = {
                "order_id": 123,
                "customer_id": 456,
                "total_amount": 99.99,
            }

            issues = validator.validate_types("sales", "orders", payload)
            # Should have no error issues (warnings for extra fields are ok)
            error_issues = [i for i in issues if i.get("severity") == "error"]
            assert len(error_issues) == 0

    def test_validate_types_detects_missing_required(self):
        """Test validate_types detects missing required fields"""
        with patch.object(SchemaValidator, '__init__', lambda x, y=None: None):
            validator = SchemaValidator()
            validator.s3 = MagicMock()
            validator.bucket = "test-bucket"
            validator.prefix = "schemas"
            validator._cache = {}

            mock_body = MagicMock()
            mock_body.read.return_value = SAMPLE_SCHEMA_YAML.encode("utf-8")
            validator.s3.get_object.return_value = {"Body": mock_body}

            # Missing required order_id
            payload = {
                "customer_id": 456,
                "total_amount": 99.99,
            }

            issues = validator.validate_types("sales", "orders", payload)

            error_issues = [i for i in issues if i.get("severity") == "error"]
            assert len(error_issues) > 0
            assert any("order_id" in i["field"] for i in error_issues)

    def test_validate_types_warns_extra_fields(self):
        """Test validate_types warns about extra fields"""
        with patch.object(SchemaValidator, '__init__', lambda x, y=None: None):
            validator = SchemaValidator()
            validator.s3 = MagicMock()
            validator.bucket = "test-bucket"
            validator.prefix = "schemas"
            validator._cache = {}

            mock_body = MagicMock()
            mock_body.read.return_value = SAMPLE_SCHEMA_YAML.encode("utf-8")
            validator.s3.get_object.return_value = {"Body": mock_body}

            payload = {
                "order_id": 123,
                "customer_id": 456,
                "total_amount": 99.99,
                "unknown_field": "should warn",
            }

            issues = validator.validate_types("sales", "orders", payload)

            warning_issues = [i for i in issues if i.get("severity") == "warning"]
            assert len(warning_issues) > 0
            assert any("unknown_field" in i["field"] for i in warning_issues)
