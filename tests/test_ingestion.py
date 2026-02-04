"""
Tests for the Data Ingestion API

Tests cover:
- Schema validation via EndpointSchema.validate_payload
- Data ingestion with validation
- Batch ingestion
"""

import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

# Mock boto3 before importing modules
import sys
sys.modules['boto3'] = MagicMock()

# Add paths for imports
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas', 'endpoints'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas', 'serverless_ingestion'))

from lambdas.serverless_ingestion.main import app
from lambdas.endpoints.models import (
    EndpointSchema,
    SchemaDefinition,
    ColumnDefinition,
    DataType,
    SchemaMode,
)

client = TestClient(app)


def create_test_schema(
    name: str = "orders",
    domain: str = "sales",
    mode: SchemaMode = SchemaMode.MANUAL,
    columns: list[ColumnDefinition] | None = None,
) -> EndpointSchema:
    """Helper to create test schemas"""
    if columns is None:
        columns = [
            ColumnDefinition(name="order_id", type=DataType.INTEGER, required=True, primary_key=True),
            ColumnDefinition(name="customer_id", type=DataType.INTEGER, required=True),
            ColumnDefinition(name="total_amount", type=DataType.FLOAT, required=True),
            ColumnDefinition(name="status", type=DataType.STRING, required=False),
        ]

    return EndpointSchema(
        name=name,
        domain=domain,
        version=1,
        mode=mode,
        schema=SchemaDefinition(columns=columns),
    )


class TestEndpointSchemaValidation:
    """Tests for EndpointSchema.validate_payload method"""

    def test_validate_valid_payload(self):
        """Valid payload should pass validation"""
        schema = create_test_schema()
        payload = {"order_id": 123, "customer_id": 456, "total_amount": 99.99}

        validated, errors = schema.validate_payload(payload)

        assert errors == []
        assert validated["order_id"] == 123
        assert validated["customer_id"] == 456

    def test_validate_missing_required_field(self):
        """Missing required field should return errors"""
        schema = create_test_schema()
        payload = {"customer_id": 456, "total_amount": 99.99}  # missing order_id

        validated, errors = schema.validate_payload(payload)

        assert len(errors) > 0
        assert any("order_id" in e["field"] for e in errors)

    def test_validate_wrong_type(self):
        """Wrong type should return errors"""
        schema = create_test_schema()
        payload = {"order_id": "not_an_int", "customer_id": 456, "total_amount": 99.99}

        validated, errors = schema.validate_payload(payload)

        assert len(errors) > 0
        assert any("order_id" in e["field"] for e in errors)

    def test_validate_single_column_mode_accepts_anything(self):
        """Single column mode should accept any payload"""
        schema = create_test_schema(mode=SchemaMode.SINGLE_COLUMN, columns=[])
        payload = {"anything": "goes", "nested": {"data": [1, 2, 3]}}

        validated, errors = schema.validate_payload(payload)

        assert errors == []
        assert validated == payload

    def test_validate_optional_field_can_be_missing(self):
        """Optional fields can be omitted"""
        schema = create_test_schema()
        payload = {"order_id": 123, "customer_id": 456, "total_amount": 99.99}
        # status is optional, not included

        validated, errors = schema.validate_payload(payload)

        assert errors == []
        assert "status" not in validated  # optional field excluded


class TestIngestionAPI:
    """Tests for the Ingestion API endpoints"""

    @patch("lambdas.serverless_ingestion.main.registry")
    @patch("lambdas.serverless_ingestion.main.firehose_client")
    def test_ingest_valid_payload(self, mock_firehose, mock_registry):
        """Successful ingestion with valid payload"""
        mock_registry.get.return_value = create_test_schema()

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

    @patch("lambdas.serverless_ingestion.main.registry")
    def test_ingest_endpoint_not_found(self, mock_registry):
        """Ingestion fails when endpoint doesn't exist"""
        mock_registry.get.return_value = None

        response = client.post(
            "/ingest/sales/nonexistent",
            json={"data": {"some": "data"}},
        )

        assert response.status_code == 404
        assert "not found" in response.json()["detail"]

    @patch("lambdas.serverless_ingestion.main.registry")
    @patch("lambdas.serverless_ingestion.main.firehose_client")
    def test_ingest_validation_error_non_strict(self, mock_firehose, mock_registry):
        """Non-strict mode continues with invalid payload"""
        mock_registry.get.return_value = create_test_schema()

        response = client.post(
            "/ingest/sales/orders?strict=false",
            json={"data": {"invalid": "payload"}},  # missing required fields
        )

        assert response.status_code == 200
        result = response.json()
        assert result["validated"] is False
        mock_firehose.put_record.assert_called_once()

    @patch("lambdas.serverless_ingestion.main.registry")
    def test_ingest_validation_error_strict(self, mock_registry):
        """Strict mode rejects invalid payload"""
        mock_registry.get.return_value = create_test_schema()

        response = client.post(
            "/ingest/sales/orders?strict=true",
            json={"data": {"invalid": "payload"}},
        )

        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["error"] == "Payload validation failed"
        assert len(detail["validation_errors"]) > 0

    @patch("lambdas.serverless_ingestion.main.registry")
    @patch("lambdas.serverless_ingestion.main.firehose_client")
    def test_ingest_skip_validation(self, mock_firehose, mock_registry):
        """Validation can be skipped"""
        mock_registry.get.return_value = create_test_schema()

        response = client.post(
            "/ingest/sales/orders?validate=false",
            json={"data": {"any": "data"}},
        )

        assert response.status_code == 200
        result = response.json()
        assert result["validated"] is False

    @patch("lambdas.serverless_ingestion.main.registry")
    @patch("lambdas.serverless_ingestion.main.firehose_client")
    def test_ingest_batch_success(self, mock_firehose, mock_registry):
        """Batch ingestion with multiple records"""
        mock_registry.get.return_value = create_test_schema()

        records = [
            {"order_id": 1, "customer_id": 100, "total_amount": 10.0},
            {"order_id": 2, "customer_id": 200, "total_amount": 20.0},
            {"order_id": 3, "customer_id": 300, "total_amount": 30.0},
        ]

        response = client.post("/ingest/sales/orders/batch", json=records)

        assert response.status_code == 200
        result = response.json()
        assert result["total_records"] == 3
        assert result["validated_count"] == 3
        assert result["sent_count"] == 3
        assert mock_firehose.put_record.call_count == 3

    @patch("lambdas.serverless_ingestion.main.registry")
    @patch("lambdas.serverless_ingestion.main.firehose_client")
    def test_ingest_batch_partial_failure_strict(self, mock_firehose, mock_registry):
        """Batch strict mode rejects invalid records"""
        mock_registry.get.return_value = create_test_schema()

        records = [
            {"order_id": 1, "customer_id": 100, "total_amount": 10.0},  # valid
            {"invalid": "record"},  # invalid
            {"order_id": 3, "customer_id": 300, "total_amount": 30.0},  # valid
        ]

        response = client.post("/ingest/sales/orders/batch?strict=true", json=records)

        assert response.status_code == 200
        result = response.json()
        assert result["total_records"] == 3
        assert result["validated_count"] == 2
        assert result["failed_count"] == 1
        assert result["sent_count"] == 2
        assert mock_firehose.put_record.call_count == 2


class TestDataTypes:
    """Tests for different data type validations"""

    def test_integer_type(self):
        """Integer type validation"""
        schema = create_test_schema(columns=[
            ColumnDefinition(name="count", type=DataType.INTEGER, required=True),
        ])

        validated, errors = schema.validate_payload({"count": 42})
        assert errors == []
        assert validated["count"] == 42

        validated, errors = schema.validate_payload({"count": "not_int"})
        assert len(errors) > 0

    def test_float_type(self):
        """Float type validation"""
        schema = create_test_schema(columns=[
            ColumnDefinition(name="amount", type=DataType.FLOAT, required=True),
        ])

        validated, errors = schema.validate_payload({"amount": 99.99})
        assert errors == []

        # Integer should coerce to float
        validated, errors = schema.validate_payload({"amount": 100})
        assert errors == []

    def test_boolean_type(self):
        """Boolean type validation"""
        schema = create_test_schema(columns=[
            ColumnDefinition(name="active", type=DataType.BOOLEAN, required=True),
        ])

        validated, errors = schema.validate_payload({"active": True})
        assert errors == []

        validated, errors = schema.validate_payload({"active": "not_bool"})
        assert len(errors) > 0

    def test_array_type(self):
        """Array type validation"""
        schema = create_test_schema(columns=[
            ColumnDefinition(name="items", type=DataType.ARRAY, required=True),
        ])

        validated, errors = schema.validate_payload({"items": [1, 2, 3]})
        assert errors == []

        validated, errors = schema.validate_payload({"items": "not_array"})
        assert len(errors) > 0

    def test_json_type(self):
        """JSON/dict type validation"""
        schema = create_test_schema(columns=[
            ColumnDefinition(name="metadata", type=DataType.JSON, required=True),
        ])

        validated, errors = schema.validate_payload({"metadata": {"key": "value"}})
        assert errors == []

        validated, errors = schema.validate_payload({"metadata": "not_dict"})
        assert len(errors) > 0
