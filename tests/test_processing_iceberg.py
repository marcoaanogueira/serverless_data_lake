"""
Tests for Processing Iceberg Lambda

Tests covering:
- Silver table registration after writing data
- S3 path parsing (domain/endpoint extraction)
- Handler event processing
"""

import pytest
from unittest.mock import MagicMock, patch, call
import importlib.util

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'layers', 'shared', 'python'))

# Mock heavy dependencies before importing
sys.modules['duckdb'] = MagicMock()
sys.modules['polars'] = MagicMock()
sys.modules['pyiceberg'] = MagicMock()
sys.modules['pyiceberg.catalog'] = MagicMock()
sys.modules['shared.infrastructure'] = MagicMock()

# Load the processing_iceberg main module explicitly by file path
# to avoid conflicts with other main.py modules on sys.path
_module_path = os.path.join(os.path.dirname(__file__), '..', 'lambdas', 'serverless_processing_iceberg', 'main.py')
_spec = importlib.util.spec_from_file_location("processing_iceberg_main", _module_path)
pi_main = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pi_main)


# =============================================================================
# S3 Path Parsing Tests
# =============================================================================

class TestParseS3Path:
    """Tests for parse_s3_path function"""

    def test_parse_domain_endpoint(self):
        """Should extract domain and endpoint from firehose path"""
        domain, endpoint = pi_main.parse_s3_path("firehose-data/sales/orders/2026/01/01/data.json")
        assert domain == "sales"
        assert endpoint == "orders"

    def test_parse_two_segments(self):
        """Should extract domain and endpoint from two-segment paths"""
        domain, endpoint = pi_main.parse_s3_path("firehose-data/sales/products/2026/data.json")
        assert domain == "sales"
        assert endpoint == "products"

    def test_parse_invalid_path(self):
        """Should raise ValueError for unparseable path"""
        with pytest.raises(ValueError):
            pi_main.parse_s3_path("invalid/path/data.json")


# =============================================================================
# Handler Tests
# =============================================================================

class TestHandler:
    """Tests for Lambda handler"""

    def test_handler_extracts_s3_event(self):
        """Handler should extract bucket and key from S3 event"""
        with patch.object(pi_main, 'process_data') as mock_process:
            mock_process.return_value = "Data written to sales_silver.orders"

            event = {
                "Records": [{
                    "s3": {
                        "bucket": {"name": "decolares-bronze"},
                        "object": {"key": "firehose-data/sales/orders/data.json"},
                    }
                }]
            }

            result = pi_main.handler(event, None)

            mock_process.assert_called_once_with("decolares-bronze", "firehose-data/sales/orders/data.json")
            assert result == "Data written to sales_silver.orders"


# =============================================================================
# Register Silver Table Integration Tests
# =============================================================================

class TestRegisterSilverTableIntegration:
    """Tests for silver table registration in process_data flow"""

    def test_process_data_calls_register_silver_table(self):
        """process_data should call register_silver_table after writing"""
        with patch.object(pi_main, 'catalog') as mock_catalog, \
             patch.object(pi_main, 'registry') as mock_registry, \
             patch.object(pi_main, 'configure_duckdb') as mock_duckdb, \
             patch.object(pi_main, 'get_schema_info') as mock_schema:

            # Setup mocks
            mock_schema.return_value = {"primary_keys": None, "columns": []}
            mock_catalog.list_namespaces.return_value = [("sales_silver",)]
            mock_catalog.list_tables.return_value = [("sales_silver", "orders")]

            # Mock DuckDB query result
            mock_con = MagicMock()
            mock_duckdb.return_value = mock_con
            mock_df = MagicMock()
            mock_df.columns = ["id", "value"]
            mock_df.drop.return_value = mock_df
            mock_con.query.return_value.pl.return_value = mock_df

            # Mock Iceberg table
            mock_table = MagicMock()
            mock_catalog.load_table.return_value = mock_table

            result = pi_main.process_data("decolares-bronze", "firehose-data/sales/orders/data.json")

            # Verify register_silver_table was called
            mock_registry.register_silver_table.assert_called_once_with(
                "sales", "orders", location="s3://decolares-silver/sales/orders"
            )

            assert "Data written to" in result
