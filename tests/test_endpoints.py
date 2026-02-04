"""
Tests for Endpoints API - Endpoint Creation Module

Tests covering:
- Models validation (ColumnDefinition, CreateEndpointRequest, EndpointSchema)
- Schema inference logic (type inference, snake_case conversion)
- API endpoints (CRUD operations, schema inference)
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from pydantic import ValidationError

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'layers', 'shared', 'python'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas', 'endpoints'))

from shared.models import (
    DataType,
    SchemaMode,
    ColumnDefinition,
    SchemaDefinition,
    EndpointSchema,
    CreateEndpointRequest,
    EndpointResponse,
)
from lambdas.endpoints.main import (
    app,
    to_snake_case,
    infer_type_from_value,
    infer_columns_from_payload,
)


# =============================================================================
# Test Client Setup
# =============================================================================

@pytest.fixture
def client():
    """Create test client with mocked registry"""
    with patch('lambdas.endpoints.main.registry') as mock_registry:
        mock_registry.create.return_value = EndpointSchema(
            name="test_table",
            domain="sales",
            version=1,
            mode=SchemaMode.MANUAL,
            schema=SchemaDefinition(columns=[
                ColumnDefinition(name="id", type=DataType.INTEGER, required=True, primary_key=True)
            ]),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        mock_registry.list_all.return_value = []
        mock_registry.get.return_value = None
        mock_registry.delete.return_value = False  # Simulate not found
        mock_registry.list_versions.return_value = []  # Simulate not found
        mock_registry.get_schema_url.return_value = "https://s3.amazonaws.com/bucket/schema.yaml"

        yield TestClient(app)


# =============================================================================
# Model Validation Tests
# =============================================================================

class TestDataType:
    """Tests for DataType enum"""

    def test_all_data_types_exist(self):
        """All expected data types should be defined"""
        assert DataType.STRING == "string"
        assert DataType.INTEGER == "integer"
        assert DataType.FLOAT == "float"
        assert DataType.BOOLEAN == "boolean"
        assert DataType.TIMESTAMP == "timestamp"
        assert DataType.DATE == "date"
        assert DataType.JSON == "json"
        assert DataType.ARRAY == "array"
        assert DataType.DECIMAL == "decimal"


class TestSchemaMode:
    """Tests for SchemaMode enum"""

    def test_all_schema_modes_exist(self):
        """All expected schema modes should be defined"""
        assert SchemaMode.MANUAL == "manual"
        assert SchemaMode.AUTO_INFERENCE == "auto_inference"
        assert SchemaMode.SINGLE_COLUMN == "single_column"


class TestColumnDefinition:
    """Tests for ColumnDefinition model"""

    def test_valid_column_definition(self):
        """Valid column definition should be created successfully"""
        col = ColumnDefinition(
            name="user_id",
            type=DataType.INTEGER,
            required=True,
            primary_key=True,
            description="User identifier"
        )
        assert col.name == "user_id"
        assert col.type == DataType.INTEGER
        assert col.required is True
        assert col.primary_key is True
        assert col.description == "User identifier"

    def test_column_with_defaults(self):
        """Column with default values should work"""
        col = ColumnDefinition(name="status")
        assert col.name == "status"
        assert col.type == DataType.STRING
        assert col.required is False
        assert col.primary_key is False
        assert col.description is None

    def test_invalid_column_name_uppercase(self):
        """Column name with uppercase should fail validation"""
        with pytest.raises(ValidationError) as exc_info:
            ColumnDefinition(name="UserName")
        assert "snake_case" in str(exc_info.value).lower()

    def test_invalid_column_name_starts_with_number(self):
        """Column name starting with number should fail validation"""
        with pytest.raises(ValidationError) as exc_info:
            ColumnDefinition(name="123column")
        assert "snake_case" in str(exc_info.value).lower()

    def test_invalid_column_name_special_chars(self):
        """Column name with special characters should fail validation"""
        with pytest.raises(ValidationError) as exc_info:
            ColumnDefinition(name="column-name")
        assert "snake_case" in str(exc_info.value).lower()

    def test_valid_snake_case_names(self):
        """Various valid snake_case names should pass"""
        valid_names = ["id", "user_id", "order_total_amount", "col1", "data_2024"]
        for name in valid_names:
            col = ColumnDefinition(name=name)
            assert col.name == name


class TestSchemaDefinition:
    """Tests for SchemaDefinition model"""

    def test_empty_schema(self):
        """Empty schema should be valid"""
        schema = SchemaDefinition()
        assert schema.columns == []
        assert schema.primary_keys == []
        assert schema.required_columns == []

    def test_schema_with_columns(self):
        """Schema with columns should work"""
        schema = SchemaDefinition(columns=[
            ColumnDefinition(name="id", type=DataType.INTEGER, required=True, primary_key=True),
            ColumnDefinition(name="name", type=DataType.STRING, required=True),
            ColumnDefinition(name="email", type=DataType.STRING, required=False),
        ])
        assert len(schema.columns) == 3
        assert schema.primary_keys == ["id"]
        assert schema.required_columns == ["id", "name"]

    def test_multiple_primary_keys(self):
        """Schema with composite primary key"""
        schema = SchemaDefinition(columns=[
            ColumnDefinition(name="tenant_id", primary_key=True),
            ColumnDefinition(name="user_id", primary_key=True),
            ColumnDefinition(name="name"),
        ])
        assert schema.primary_keys == ["tenant_id", "user_id"]


class TestEndpointSchema:
    """Tests for EndpointSchema model"""

    def test_valid_endpoint_schema(self):
        """Valid endpoint schema should be created"""
        schema = EndpointSchema(
            name="orders",
            domain="sales",
            version=1,
            mode=SchemaMode.MANUAL,
            description="Customer orders"
        )
        assert schema.name == "orders"
        assert schema.domain == "sales"
        assert schema.version == 1

    def test_invalid_table_name(self):
        """Invalid table name should fail"""
        with pytest.raises(ValidationError):
            EndpointSchema(name="InvalidName", domain="sales")

    def test_invalid_domain_name(self):
        """Invalid domain name should fail"""
        with pytest.raises(ValidationError):
            EndpointSchema(name="orders", domain="Sales-Domain")

    def test_to_yaml_dict(self):
        """to_yaml_dict should return proper structure"""
        schema = EndpointSchema(
            name="orders",
            domain="sales",
            version=1,
            mode=SchemaMode.MANUAL,
            schema=SchemaDefinition(columns=[
                ColumnDefinition(name="id", type=DataType.INTEGER, required=True, primary_key=True)
            ])
        )
        yaml_dict = schema.to_yaml_dict()

        assert yaml_dict["name"] == "orders"
        assert yaml_dict["domain"] == "sales"
        assert yaml_dict["version"] == 1
        assert yaml_dict["mode"] == "manual"
        assert len(yaml_dict["schema"]["columns"]) == 1
        assert yaml_dict["schema"]["columns"][0]["name"] == "id"

    def test_from_yaml_dict(self):
        """from_yaml_dict should create instance correctly"""
        data = {
            "name": "products",
            "domain": "catalog",
            "version": 2,
            "mode": "auto_inference",
            "created_at": "2024-01-15T10:30:00",
            "updated_at": "2024-01-15T10:30:00",
            "schema": {
                "columns": [
                    {"name": "product_id", "type": "integer", "required": True, "primary_key": True},
                    {"name": "title", "type": "string", "required": True},
                ]
            }
        }
        schema = EndpointSchema.from_yaml_dict(data)

        assert schema.name == "products"
        assert schema.domain == "catalog"
        assert schema.version == 2
        assert schema.mode == SchemaMode.AUTO_INFERENCE
        assert len(schema.schema_def.columns) == 2


class TestCreateEndpointRequest:
    """Tests for CreateEndpointRequest model"""

    def test_valid_request(self):
        """Valid create request should work"""
        request = CreateEndpointRequest(
            name="events",
            domain="analytics",
            mode=SchemaMode.MANUAL,
            columns=[
                ColumnDefinition(name="event_id", type=DataType.STRING, required=True, primary_key=True)
            ]
        )
        assert request.name == "events"
        assert request.domain == "analytics"

    def test_request_with_invalid_name(self):
        """Request with invalid name should fail"""
        with pytest.raises(ValidationError):
            CreateEndpointRequest(name="Invalid-Name", domain="analytics")

    def test_request_with_invalid_domain(self):
        """Request with invalid domain should fail"""
        with pytest.raises(ValidationError):
            CreateEndpointRequest(name="events", domain="My Domain")


# =============================================================================
# Schema Inference Tests
# =============================================================================

class TestToSnakeCase:
    """Tests for to_snake_case function"""

    def test_camel_case(self):
        """camelCase should convert to snake_case"""
        assert to_snake_case("userName") == "user_name"
        assert to_snake_case("firstName") == "first_name"
        assert to_snake_case("orderTotalAmount") == "order_total_amount"

    def test_pascal_case(self):
        """PascalCase should convert to snake_case"""
        assert to_snake_case("UserName") == "user_name"
        assert to_snake_case("FirstName") == "first_name"
        assert to_snake_case("OrderTotalAmount") == "order_total_amount"

    def test_already_snake_case(self):
        """Already snake_case should remain unchanged"""
        assert to_snake_case("user_name") == "user_name"
        assert to_snake_case("first_name") == "first_name"

    def test_single_word(self):
        """Single word should remain lowercase"""
        assert to_snake_case("name") == "name"
        assert to_snake_case("Name") == "name"

    def test_acronyms(self):
        """Acronyms should be handled"""
        assert to_snake_case("userID") == "user_id"
        assert to_snake_case("HTTPResponse") == "http_response"


class TestInferTypeFromValue:
    """Tests for infer_type_from_value function"""

    def test_infer_string(self):
        """String values should infer STRING type"""
        assert infer_type_from_value("hello") == DataType.STRING
        assert infer_type_from_value("") == DataType.STRING

    def test_infer_integer(self):
        """Integer values should infer INTEGER type"""
        assert infer_type_from_value(42) == DataType.INTEGER
        assert infer_type_from_value(0) == DataType.INTEGER
        assert infer_type_from_value(-100) == DataType.INTEGER

    def test_infer_float(self):
        """Float values should infer FLOAT type"""
        assert infer_type_from_value(3.14) == DataType.FLOAT
        assert infer_type_from_value(0.0) == DataType.FLOAT
        assert infer_type_from_value(-99.99) == DataType.FLOAT

    def test_infer_boolean(self):
        """Boolean values should infer BOOLEAN type"""
        assert infer_type_from_value(True) == DataType.BOOLEAN
        assert infer_type_from_value(False) == DataType.BOOLEAN

    def test_infer_list(self):
        """List values should infer ARRAY type"""
        assert infer_type_from_value([1, 2, 3]) == DataType.ARRAY
        assert infer_type_from_value([]) == DataType.ARRAY

    def test_infer_dict(self):
        """Dict values should infer JSON type"""
        assert infer_type_from_value({"key": "value"}) == DataType.JSON
        assert infer_type_from_value({}) == DataType.JSON

    def test_infer_none(self):
        """None values should default to STRING type"""
        assert infer_type_from_value(None) == DataType.STRING

    def test_infer_timestamp_iso(self):
        """ISO timestamp strings should infer TIMESTAMP type"""
        assert infer_type_from_value("2024-01-15T10:30:00") == DataType.TIMESTAMP
        assert infer_type_from_value("2024-01-15T10:30:00Z") == DataType.TIMESTAMP
        assert infer_type_from_value("2024-01-15 10:30:00") == DataType.TIMESTAMP

    def test_infer_date(self):
        """Date strings should infer DATE type"""
        assert infer_type_from_value("2024-01-15") == DataType.DATE


class TestInferColumnsFromPayload:
    """Tests for infer_columns_from_payload function"""

    def test_simple_payload(self):
        """Simple payload should infer columns correctly"""
        payload = {
            "id": 1,
            "name": "John",
            "active": True,
        }
        columns = infer_columns_from_payload(payload)

        assert len(columns) == 3

        id_col = next(c for c in columns if c["name"] == "id")
        assert id_col["type"] == "integer"
        assert id_col["primary_key"] is True  # 'id' is detected as primary key

        name_col = next(c for c in columns if c["name"] == "name")
        assert name_col["type"] == "string"

        active_col = next(c for c in columns if c["name"] == "active")
        assert active_col["type"] == "boolean"

    def test_camel_case_keys(self):
        """camelCase keys should be converted to snake_case"""
        payload = {
            "userId": 1,
            "firstName": "John",
            "lastName": "Doe",
        }
        columns = infer_columns_from_payload(payload)

        column_names = [c["name"] for c in columns]
        assert "user_id" in column_names
        assert "first_name" in column_names
        assert "last_name" in column_names

    def test_complex_payload(self):
        """Complex payload with various types"""
        payload = {
            "orderId": "abc123",
            "totalAmount": 99.90,
            "quantity": 5,
            "isPaid": True,
            "createdAt": "2024-01-15T10:30:00Z",
            "items": [{"sku": "A1", "qty": 2}],
            "metadata": {"source": "web"},
        }
        columns = infer_columns_from_payload(payload)

        assert len(columns) == 7

        # Check specific type inferences
        order_id = next(c for c in columns if c["name"] == "order_id")
        assert order_id["type"] == "string"

        total = next(c for c in columns if c["name"] == "total_amount")
        assert total["type"] == "float"

        qty = next(c for c in columns if c["name"] == "quantity")
        assert qty["type"] == "integer"

        items = next(c for c in columns if c["name"] == "items")
        assert items["type"] == "array"

        metadata = next(c for c in columns if c["name"] == "metadata")
        assert metadata["type"] == "json"

        created = next(c for c in columns if c["name"] == "created_at")
        assert created["type"] == "timestamp"

    def test_sample_values_included(self):
        """Sample values should be included in output"""
        payload = {"status": "active"}
        columns = infer_columns_from_payload(payload)

        assert columns[0]["sample_value"] == "active"

    def test_none_values(self):
        """None values should be handled"""
        payload = {"optional_field": None}
        columns = infer_columns_from_payload(payload)

        assert columns[0]["type"] == "string"
        assert columns[0]["required"] is False
        assert columns[0]["sample_value"] is None


# =============================================================================
# API Endpoint Tests
# =============================================================================

class TestHealthCheck:
    """Tests for health check endpoint"""

    def test_health_check(self, client):
        """Health check should return healthy status"""
        response = client.get("/")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
        assert response.json()["service"] == "endpoints"


class TestCreateEndpoint:
    """Tests for POST /endpoints"""

    def test_create_endpoint_success(self, client):
        """Creating valid endpoint should succeed"""
        response = client.post("/endpoints", json={
            "name": "orders",
            "domain": "sales",
            "mode": "manual",
            "columns": [
                {"name": "id", "type": "integer", "required": True, "primary_key": True}
            ]
        })
        assert response.status_code == 200
        data = response.json()
        assert "id" in data
        assert "endpoint_url" in data

    def test_create_endpoint_invalid_name(self, client):
        """Creating endpoint with invalid name should fail"""
        response = client.post("/endpoints", json={
            "name": "Invalid-Name",
            "domain": "sales",
            "columns": []
        })
        assert response.status_code == 422  # Validation error

    def test_create_endpoint_invalid_domain(self, client):
        """Creating endpoint with invalid domain should fail"""
        response = client.post("/endpoints", json={
            "name": "orders",
            "domain": "Sales Domain",
            "columns": []
        })
        assert response.status_code == 422


class TestListEndpoints:
    """Tests for GET /endpoints"""

    def test_list_endpoints_empty(self, client):
        """Listing endpoints when empty should return empty list"""
        response = client.get("/endpoints")
        assert response.status_code == 200
        assert response.json() == []

    def test_list_endpoints_with_domain_filter(self, client):
        """Listing endpoints with domain filter should work"""
        response = client.get("/endpoints?domain=sales")
        assert response.status_code == 200


class TestInferSchema:
    """Tests for POST /endpoints/infer"""

    def test_infer_schema_success(self, client):
        """Inferring schema from valid payload should succeed"""
        response = client.post("/endpoints/infer", json={
            "payload": {
                "userId": 123,
                "userName": "John",
                "isActive": True,
                "balance": 99.50
            }
        })
        assert response.status_code == 200
        data = response.json()

        assert "columns" in data
        assert "payload_keys" in data
        assert len(data["columns"]) == 4
        assert data["payload_keys"] == ["userId", "userName", "isActive", "balance"]

        # Check column names are snake_case
        column_names = [c["name"] for c in data["columns"]]
        assert "user_id" in column_names
        assert "user_name" in column_names
        assert "is_active" in column_names
        assert "balance" in column_names

    def test_infer_schema_empty_payload(self, client):
        """Inferring schema from empty payload should fail"""
        response = client.post("/endpoints/infer", json={
            "payload": {}
        })
        assert response.status_code == 400
        assert "empty" in response.json()["detail"].lower()

    def test_infer_schema_complex_types(self, client):
        """Inferring schema should handle complex types"""
        response = client.post("/endpoints/infer", json={
            "payload": {
                "items": [1, 2, 3],
                "metadata": {"key": "value"},
                "createdAt": "2024-01-15T10:30:00Z"
            }
        })
        assert response.status_code == 200
        data = response.json()

        items_col = next(c for c in data["columns"] if c["name"] == "items")
        assert items_col["type"] == "array"

        metadata_col = next(c for c in data["columns"] if c["name"] == "metadata")
        assert metadata_col["type"] == "json"

        created_col = next(c for c in data["columns"] if c["name"] == "created_at")
        assert created_col["type"] == "timestamp"


class TestGetEndpoint:
    """Tests for GET /endpoints/{domain}/{name}"""

    def test_get_endpoint_not_found(self, client):
        """Getting non-existent endpoint should return 404"""
        response = client.get("/endpoints/sales/nonexistent")
        assert response.status_code == 404


class TestDeleteEndpoint:
    """Tests for DELETE /endpoints/{domain}/{name}"""

    def test_delete_endpoint_not_found(self):
        """Deleting non-existent endpoint should return 404"""
        with patch('lambdas.endpoints.main.registry') as mock_registry:
            mock_registry.delete.return_value = False
            test_client = TestClient(app)
            response = test_client.delete("/endpoints/sales/nonexistent")
            assert response.status_code == 404


class TestEndpointVersions:
    """Tests for GET /endpoints/{domain}/{name}/versions"""

    def test_list_versions_not_found(self):
        """Listing versions for non-existent endpoint should return 404"""
        with patch('lambdas.endpoints.main.registry') as mock_registry:
            mock_registry.list_versions.return_value = []
            test_client = TestClient(app)
            response = test_client.get("/endpoints/sales/nonexistent/versions")
            assert response.status_code == 404
