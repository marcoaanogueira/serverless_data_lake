"""
Tests for Query API (consumption module)

Tests covering:
- /consumption/tables endpoint (silver tables from schema registry)
- /consumption/query endpoint (DuckDB SQL execution)
"""

import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'layers', 'shared', 'python'))

# Mock heavy dependencies before importing app
mock_duckdb_module = MagicMock()
sys.modules['duckdb'] = mock_duckdb_module
sys.modules['shared.infrastructure'] = MagicMock()

from lambdas.query_api.main import app, rewrite_query


# =============================================================================
# Test Client Setup
# =============================================================================

@pytest.fixture
def mock_registry():
    with patch('lambdas.query_api.main.registry') as mock_reg:
        yield mock_reg


@pytest.fixture
def mock_configure_duckdb():
    with patch('lambdas.query_api.main.configure_duckdb') as mock_config:
        yield mock_config


@pytest.fixture
def client(mock_registry, mock_configure_duckdb):
    return TestClient(app)


# =============================================================================
# Query Rewriting
# =============================================================================

class TestRewriteQuery:
    def test_rewrites_silver(self):
        assert rewrite_query("SELECT * FROM sales.silver.teste") == "SELECT * FROM tadpole.sales_silver.teste"

    def test_rewrites_gold(self):
        assert rewrite_query("SELECT * FROM sales.gold.report") == "SELECT * FROM tadpole.sales_gold.report"

    def test_no_rewrite_other(self):
        sql = "SELECT 1"
        assert rewrite_query(sql) == sql


# =============================================================================
# /consumption/tables Tests
# =============================================================================

class TestListTables:
    """Tests for GET /consumption/tables"""

    def test_list_tables_returns_silver_tables(self, client, mock_registry):
        """Should return silver tables from schema registry"""
        mock_registry.list_silver_tables.return_value = [
            {"name": "orders", "domain": "sales", "location": "s3://bucket/silver/sales/orders", "created_at": "2026-01-01"},
            {"name": "products", "domain": "sales", "location": "s3://bucket/silver/sales/products", "created_at": "2026-01-02"},
        ]

        # Mock bronze schema with columns
        from shared.models import EndpointSchema, SchemaDefinition, ColumnDefinition, DataType, SchemaMode

        orders_schema = EndpointSchema(
            name="orders", domain="sales", version=1, mode=SchemaMode.MANUAL,
            schema=SchemaDefinition(columns=[
                ColumnDefinition(name="id", type=DataType.INTEGER, required=True, primary_key=True),
                ColumnDefinition(name="total", type=DataType.DECIMAL),
            ])
        )
        products_schema = EndpointSchema(
            name="products", domain="sales", version=1, mode=SchemaMode.MANUAL,
            schema=SchemaDefinition(columns=[
                ColumnDefinition(name="product_id", type=DataType.INTEGER, required=True, primary_key=True),
                ColumnDefinition(name="title", type=DataType.STRING),
            ])
        )

        def mock_get(domain, name):
            if name == "orders":
                return orders_schema
            if name == "products":
                return products_schema
            return None

        mock_registry.get.side_effect = mock_get

        response = client.get("/consumption/tables")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert len(data["tables"]) == 2

        # Verify first table
        orders_table = next(t for t in data["tables"] if t["name"] == "orders")
        assert orders_table["domain"] == "sales"
        assert orders_table["location"] == "s3://bucket/silver/sales/orders"
        assert len(orders_table["columns"]) == 2
        assert orders_table["columns"][0]["name"] == "id"
        assert orders_table["columns"][0]["type"] == "integer"

    def test_list_tables_empty(self, client, mock_registry):
        """Should return empty list when no silver tables exist"""
        mock_registry.list_silver_tables.return_value = []

        response = client.get("/consumption/tables")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert data["tables"] == []

    def test_list_tables_without_bronze_schema(self, client, mock_registry):
        """Should still list silver table even if bronze schema is missing"""
        mock_registry.list_silver_tables.return_value = [
            {"name": "orphan_table", "domain": "sales", "location": "s3://bucket/silver/sales/orphan_table", "created_at": "2026-01-01"},
        ]
        mock_registry.get.return_value = None  # No bronze schema found

        response = client.get("/consumption/tables")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["tables"][0]["name"] == "orphan_table"
        assert data["tables"][0]["columns"] == []  # No columns since no bronze schema


# =============================================================================
# /consumption/query Tests
# =============================================================================

class TestExecuteQuery:
    """Tests for GET /consumption/query"""

    def test_execute_query_success(self, client, mock_configure_duckdb):
        """Should execute SQL and return results"""
        mock_con = MagicMock()
        mock_configure_duckdb.return_value = mock_con

        mock_result = MagicMock()
        mock_result.description = [("id",), ("name",)]
        mock_result.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        mock_con.execute.return_value = mock_result

        response = client.get("/consumption/query?sql=SELECT * FROM test")

        assert response.status_code == 200
        data = response.json()
        assert data["row_count"] == 2
        assert data["data"][0] == {"id": 1, "name": "Alice"}
        assert data["data"][1] == {"id": 2, "name": "Bob"}

    def test_execute_query_rewrites_table_refs(self, client, mock_configure_duckdb):
        """Should rewrite sales.silver.x → tadpole.sales_silver.x before executing"""
        mock_con = MagicMock()
        mock_configure_duckdb.return_value = mock_con

        mock_result = MagicMock()
        mock_result.description = [("id",)]
        mock_result.fetchall.return_value = [(1,)]
        mock_con.execute.return_value = mock_result

        response = client.get("/consumption/query?sql=SELECT * FROM sales.silver.teste")

        assert response.status_code == 200
        executed_sql = mock_con.execute.call_args[0][0]
        assert "tadpole.sales_silver.teste" in executed_sql
        assert "sales.silver.teste" not in executed_sql

    def test_execute_query_returns_duckdb_error(self, client, mock_configure_duckdb):
        """Should return actual DuckDB error message on query failure"""
        mock_con = MagicMock()
        mock_configure_duckdb.return_value = mock_con
        mock_con.execute.side_effect = Exception(
            "Catalog Error: Table with name 'nonexistent' does not exist!"
        )

        response = client.get("/consumption/query?sql=SELECT * FROM nonexistent")

        assert response.status_code == 400
        data = response.json()
        assert "nonexistent" in data["detail"]
        assert "Catalog Error" in data["detail"]

    def test_execute_query_returns_syntax_error(self, client, mock_configure_duckdb):
        """Should return DuckDB syntax error details"""
        mock_con = MagicMock()
        mock_configure_duckdb.return_value = mock_con
        mock_con.execute.side_effect = Exception(
            "Parser Error: syntax error at or near 'SELEC'"
        )

        response = client.get("/consumption/query?sql=SELEC * FROM test")

        assert response.status_code == 400
        data = response.json()
        assert "Parser Error" in data["detail"]

    def test_execute_query_bronze_table_not_found(self, client, mock_configure_duckdb):
        """Should return friendly message when bronze S3 path has no files"""
        mock_con = MagicMock()
        mock_configure_duckdb.return_value = mock_con
        mock_con.execute.side_effect = Exception(
            'IO Error: No files found that match the pattern '
            '"s3://my-bronze-bucket/firehose-data/ecommerce/order_item/**"'
        )

        response = client.get("/consumption/query?sql=SELECT * FROM ecommerce.bronze.order_item")

        assert response.status_code == 400
        data = response.json()
        assert "ecommerce.bronze.order_item" in data["detail"]
        assert "does not exist" in data["detail"]
        assert "s3://" not in data["detail"]

    def test_execute_query_config_error(self, client, mock_configure_duckdb):
        """Should return 500 when DuckDB configuration fails"""
        mock_configure_duckdb.side_effect = Exception("Failed to load extension 'iceberg'")

        response = client.get("/consumption/query?sql=SELECT 1")

        assert response.status_code == 500
        data = response.json()
        assert "Failed to initialize query engine" in data["detail"]
        assert "iceberg" in data["detail"]

    def test_execute_query_requires_sql(self, client):
        """Should fail if sql parameter is missing"""
        response = client.get("/consumption/query")
        assert response.status_code == 422  # Validation error
