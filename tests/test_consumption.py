"""
Tests for Query API (consumption module)

Tests covering:
- /consumption/tables endpoint (silver + gold tables from Glue catalog)
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
        mock_reg.list_gold_jobs.return_value = []
        yield mock_reg


@pytest.fixture
def mock_configure_duckdb():
    with patch('lambdas.query_api.main.configure_duckdb') as mock_config:
        yield mock_config


@pytest.fixture
def mock_glue_columns():
    with patch('lambdas.query_api.main._get_glue_columns', return_value=[]) as mock_glue:
        yield mock_glue


@pytest.fixture
def client(mock_registry, mock_configure_duckdb, mock_glue_columns):
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

    def test_list_tables_returns_silver_tables(self, client, mock_registry, mock_glue_columns):
        """Should return silver tables with Glue columns"""
        mock_registry.list_silver_tables.return_value = [
            {"name": "orders", "domain": "sales", "location": "s3://bucket/silver/sales/orders", "created_at": "2026-01-01"},
        ]
        mock_glue_columns.return_value = [
            {"name": "id", "type": "int"},
            {"name": "total", "type": "decimal(10,2)"},
        ]

        response = client.get("/consumption/tables")

        assert response.status_code == 200
        data = response.json()
        orders_table = next(t for t in data["tables"] if t["name"] == "orders")
        assert orders_table["layer"] == "silver"
        assert orders_table["domain"] == "sales"
        assert len(orders_table["columns"]) == 2
        assert orders_table["columns"][0]["name"] == "id"
        mock_glue_columns.assert_any_call("sales_silver", "orders")

    def test_list_tables_silver_fallback_to_bronze_schema(self, client, mock_registry, mock_glue_columns):
        """Should fallback to bronze schema columns when Glue returns empty"""
        mock_registry.list_silver_tables.return_value = [
            {"name": "orders", "domain": "sales", "location": "s3://bucket/silver/sales/orders", "created_at": "2026-01-01"},
        ]
        mock_glue_columns.return_value = []

        from shared.models import EndpointSchema, SchemaDefinition, ColumnDefinition, DataType, SchemaMode
        mock_registry.get.return_value = EndpointSchema(
            name="orders", domain="sales", version=1, mode=SchemaMode.MANUAL,
            schema=SchemaDefinition(columns=[
                ColumnDefinition(name="id", type=DataType.INTEGER, required=True, primary_key=True),
                ColumnDefinition(name="total", type=DataType.DECIMAL),
            ])
        )

        response = client.get("/consumption/tables")

        assert response.status_code == 200
        data = response.json()
        orders_table = next(t for t in data["tables"] if t["name"] == "orders")
        assert len(orders_table["columns"]) == 2
        assert orders_table["columns"][0]["name"] == "id"
        assert orders_table["columns"][0]["type"] == "integer"

    def test_list_tables_returns_gold_tables(self, client, mock_registry, mock_glue_columns):
        """Should return gold tables with Glue columns"""
        mock_registry.list_silver_tables.return_value = []
        mock_registry.list_gold_jobs.return_value = [
            {"domain": "sales", "job_name": "daily_report"},
        ]
        mock_glue_columns.return_value = [
            {"name": "date", "type": "date"},
            {"name": "revenue", "type": "double"},
        ]

        response = client.get("/consumption/tables")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        gold_table = data["tables"][0]
        assert gold_table["name"] == "daily_report"
        assert gold_table["domain"] == "sales"
        assert gold_table["layer"] == "gold"
        assert len(gold_table["columns"]) == 2
        mock_glue_columns.assert_any_call("sales_gold", "daily_report")

    def test_list_tables_empty(self, client, mock_registry):
        """Should return empty list when no tables exist"""
        mock_registry.list_silver_tables.return_value = []
        mock_registry.list_gold_jobs.return_value = []

        response = client.get("/consumption/tables")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert data["tables"] == []

    def test_list_tables_without_columns(self, client, mock_registry, mock_glue_columns):
        """Should still list table even if no columns are available"""
        mock_registry.list_silver_tables.return_value = [
            {"name": "orphan_table", "domain": "sales", "location": "s3://bucket/silver/sales/orphan_table", "created_at": "2026-01-01"},
        ]
        mock_glue_columns.return_value = []
        mock_registry.get.return_value = None

        response = client.get("/consumption/tables")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["tables"][0]["name"] == "orphan_table"
        assert data["tables"][0]["columns"] == []


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
        mock_result.fetchmany.return_value = [(1, "Alice"), (2, "Bob")]
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
        """Non-SELECT queries are now blocked by validation before reaching DuckDB"""
        response = client.get("/consumption/query?sql=SELEC * FROM test")

        assert response.status_code == 400
        data = response.json()
        assert "Only SELECT queries are allowed" in data["detail"]

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
        """Should return 500 with generic message when DuckDB configuration fails (no internal details leaked)"""
        mock_configure_duckdb.side_effect = Exception("Failed to load extension 'iceberg'")

        response = client.get("/consumption/query?sql=SELECT 1")

        assert response.status_code == 500
        data = response.json()
        assert "Failed to initialize query engine" in data["detail"]
        # Internal error details should NOT be leaked
        assert "iceberg" not in data["detail"]

    def test_execute_query_requires_sql(self, client):
        """Should fail if sql parameter is missing"""
        response = client.get("/consumption/query")
        assert response.status_code == 422  # Validation error


# =============================================================================
# SQL Security Validation Tests
# =============================================================================

class TestQuerySecurityValidation:
    """Tests for SQL injection prevention and query validation."""

    def test_blocks_drop_table(self, client):
        response = client.get("/consumption/query?sql=DROP TABLE users")
        assert response.status_code == 400
        assert "Only SELECT" in response.json()["detail"]

    def test_blocks_delete(self, client):
        response = client.get("/consumption/query?sql=DELETE FROM users")
        assert response.status_code == 400

    def test_blocks_insert(self, client):
        response = client.get("/consumption/query?sql=INSERT INTO users VALUES (1)")
        assert response.status_code == 400

    def test_blocks_update(self, client):
        response = client.get("/consumption/query?sql=UPDATE users SET name='x'")
        assert response.status_code == 400

    def test_blocks_create_table(self, client):
        response = client.get("/consumption/query?sql=CREATE TABLE evil (id int)")
        assert response.status_code == 400

    def test_blocks_attach(self, client):
        response = client.get("/consumption/query?sql=ATTACH 'db.duckdb' AS stolen")
        assert response.status_code == 400

    def test_blocks_copy(self, client):
        response = client.get("/consumption/query?sql=COPY users TO '/tmp/data.csv'")
        assert response.status_code == 400

    def test_blocks_install_extension(self, client):
        response = client.get("/consumption/query?sql=INSTALL httpfs")
        assert response.status_code == 400

    def test_blocks_read_csv_auto(self, client):
        response = client.get("/consumption/query?sql=SELECT * FROM read_csv_auto('s3://secret-bucket/data.csv')")
        assert response.status_code == 400
        assert "file access" in response.json()["detail"].lower()

    def test_blocks_read_parquet(self, client):
        response = client.get("/consumption/query?sql=SELECT * FROM read_parquet('s3://bucket/file.parquet')")
        assert response.status_code == 400

    def test_blocks_read_json_auto_direct(self, client):
        response = client.get("/consumption/query?sql=SELECT * FROM read_json_auto('s3://bucket/data.json')")
        assert response.status_code == 400

    def test_blocks_select_with_drop_injection(self, client):
        """Block SELECT that contains DDL via subquery or semicolon tricks"""
        response = client.get("/consumption/query?sql=SELECT 1; DROP TABLE users")
        assert response.status_code == 400

    def test_blocks_select_with_insert_subquery(self, client):
        response = client.get("/consumption/query?sql=SELECT * FROM (INSERT INTO x VALUES (1))")
        assert response.status_code == 400

    def test_allows_valid_select(self, client, mock_configure_duckdb):
        mock_con = MagicMock()
        mock_configure_duckdb.return_value = mock_con
        mock_result = MagicMock()
        mock_result.description = [("c",)]
        mock_result.fetchmany.return_value = [(1,)]
        mock_con.execute.return_value = mock_result

        response = client.get("/consumption/query?sql=SELECT 1")
        assert response.status_code == 200

    def test_allows_with_cte(self, client, mock_configure_duckdb):
        mock_con = MagicMock()
        mock_configure_duckdb.return_value = mock_con
        mock_result = MagicMock()
        mock_result.description = [("c",)]
        mock_result.fetchmany.return_value = [(1,)]
        mock_con.execute.return_value = mock_result

        response = client.get("/consumption/query?sql=WITH cte AS (SELECT 1) SELECT * FROM cte")
        assert response.status_code == 200

    def test_blocks_empty_query(self, client):
        response = client.get("/consumption/query?sql=  ")
        assert response.status_code == 400
        assert "Empty" in response.json()["detail"]

    def test_blocks_oversized_query(self, client):
        huge_sql = "SELECT " + "a" * 11000
        response = client.get(f"/consumption/query?sql={huge_sql}")
        assert response.status_code == 400
        assert "maximum length" in response.json()["detail"]

    def test_error_sanitizes_s3_paths(self, client, mock_configure_duckdb):
        """Error messages should not leak S3 bucket paths"""
        mock_con = MagicMock()
        mock_configure_duckdb.return_value = mock_con
        mock_con.execute.side_effect = Exception(
            "IO Error: Could not read file s3://internal-bucket-name/secret/path.parquet"
        )

        response = client.get("/consumption/query?sql=SELECT * FROM sales.silver.orders")
        assert response.status_code == 400
        assert "s3://" not in response.json()["detail"]
        assert "internal-bucket" not in response.json()["detail"]

    def test_error_sanitizes_filesystem_paths(self, client, mock_configure_duckdb):
        """Error messages should not leak internal filesystem paths"""
        mock_con = MagicMock()
        mock_configure_duckdb.return_value = mock_con
        mock_con.execute.side_effect = Exception(
            "IO Error: Could not open file /tmp/duckdb/.duckdb/extensions/iceberg.so"
        )

        response = client.get("/consumption/query?sql=SELECT * FROM sales.silver.orders")
        assert response.status_code == 400
        assert "/tmp/" not in response.json()["detail"]
