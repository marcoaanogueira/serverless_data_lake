"""
Tests for dbt Runner entrypoint and Glue Iceberg plugin

Tests covering:
- dbt project generation (project files, model SQL, profiles)
- Glue Iceberg plugin initialization and connection configuration
- Execution flow
"""

import pytest
import os
import shutil
import yaml
from unittest.mock import MagicMock, patch, call

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'containers', 'dbt_runner'))

from containers.dbt_runner.entrypoint import generate_dbt_project, DBT_PROJECT_DIR
from containers.dbt_runner.glue_iceberg_plugin import Plugin as GlueIcebergPlugin


# =============================================================================
# dbt Project Generation
# =============================================================================

class TestGenerateDbtProject:
    def setup_method(self):
        """Clean up project dir before each test"""
        if os.path.exists(DBT_PROJECT_DIR):
            shutil.rmtree(DBT_PROJECT_DIR)

    def teardown_method(self):
        """Clean up project dir after each test"""
        if os.path.exists(DBT_PROJECT_DIR):
            shutil.rmtree(DBT_PROJECT_DIR)

    def test_generates_project_yml(self):
        """Should create dbt_project.yml"""
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        project_path = f"{DBT_PROJECT_DIR}/dbt_project.yml"
        assert os.path.exists(project_path)

        with open(project_path) as f:
            config = yaml.safe_load(f)

        assert config["name"] == "data_lake_gold"
        assert config["version"] == "1.0.0"
        assert config["profile"] == "data_lake"

    def test_generates_profiles_yml(self):
        """Should create profiles.yml with duckdb config"""
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        profiles_path = f"{DBT_PROJECT_DIR}/profiles.yml"
        assert os.path.exists(profiles_path)

        with open(profiles_path) as f:
            config = yaml.safe_load(f)

        assert "data_lake" in config
        prod = config["data_lake"]["outputs"]["prod"]
        assert prod["type"] == "duckdb"
        assert prod["path"] == ":memory:"

    def test_profiles_has_extensions(self):
        """Should include httpfs, aws, iceberg extensions"""
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        with open(f"{DBT_PROJECT_DIR}/profiles.yml") as f:
            config = yaml.safe_load(f)

        extensions = config["data_lake"]["outputs"]["prod"]["extensions"]
        assert "httpfs" in extensions
        assert "aws" in extensions
        assert "iceberg" in extensions

    def test_profiles_has_s3_secret(self):
        """Should include S3 credential chain secret"""
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        with open(f"{DBT_PROJECT_DIR}/profiles.yml") as f:
            config = yaml.safe_load(f)

        secrets = config["data_lake"]["outputs"]["prod"]["secrets"]
        assert len(secrets) >= 1
        s3_secret = secrets[0]
        assert s3_secret["type"] == "s3"
        assert s3_secret["provider"] == "credential_chain"

    def test_profiles_has_glue_plugin(self):
        """Should include glue_iceberg plugin config"""
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        with open(f"{DBT_PROJECT_DIR}/profiles.yml") as f:
            config = yaml.safe_load(f)

        plugins = config["data_lake"]["outputs"]["prod"]["plugins"]
        assert len(plugins) >= 1
        plugin = plugins[0]
        assert plugin["module"] == "glue_iceberg_plugin"
        assert plugin["alias"] == "glue_iceberg"
        assert "config" in plugin

    def test_generates_model_sql(self):
        """Should create model SQL file with the query"""
        query = "SELECT id, name FROM silver.customers WHERE active = true"
        generate_dbt_project("active_customers", query, "silver-bucket", "gold-bucket")

        model_path = f"{DBT_PROJECT_DIR}/models/active_customers.sql"
        assert os.path.exists(model_path)

        with open(model_path) as f:
            content = f.read()

        assert query in content
        assert "config(materialized='table')" in content

    def test_creates_directories(self):
        """Should create models/ and macros/ directories"""
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        assert os.path.isdir(f"{DBT_PROJECT_DIR}/models")
        assert os.path.isdir(f"{DBT_PROJECT_DIR}/macros")

    def test_cleans_previous_run(self):
        """Should remove existing project directory before generating"""
        # Create a stale file
        os.makedirs(f"{DBT_PROJECT_DIR}/models", exist_ok=True)
        with open(f"{DBT_PROJECT_DIR}/models/old_model.sql", "w") as f:
            f.write("-- old")

        generate_dbt_project("new_job", "SELECT 1", "silver-bucket", "gold-bucket")

        assert not os.path.exists(f"{DBT_PROJECT_DIR}/models/old_model.sql")
        assert os.path.exists(f"{DBT_PROJECT_DIR}/models/new_job.sql")


# =============================================================================
# Glue Iceberg Plugin
# =============================================================================

class TestGlueIcebergPlugin:
    def _make_plugin(self, config):
        """Helper to create plugin with config (constructor calls initialize)."""
        return GlueIcebergPlugin("glue_iceberg", config)

    def test_initialize_from_config(self):
        """Should read catalog params from plugin config dict"""
        plugin = self._make_plugin({
            "catalog_name": "my_catalog",
            "aws_region": "eu-west-1",
            "aws_account_id": "111222333444",
        })

        assert plugin.catalog_name == "my_catalog"
        assert plugin.aws_region == "eu-west-1"
        assert plugin.aws_account_id == "111222333444"

    def test_initialize_defaults_from_env(self):
        """Should fall back to environment variables"""
        with patch.dict(os.environ, {
            "GLUE_CATALOG_NAME": "env_catalog",
            "AWS_REGION": "sa-east-1",
            "AWS_ACCOUNT_ID": "999888777666",
        }):
            plugin = self._make_plugin({})

        assert plugin.catalog_name == "env_catalog"
        assert plugin.aws_region == "sa-east-1"
        assert plugin.aws_account_id == "999888777666"

    def test_configure_connection_sets_directories(self):
        """Should SET home_directory and extension_directory on connection"""
        plugin = self._make_plugin({
            "catalog_name": "tadpole",
            "aws_region": "us-east-1",
            "aws_account_id": "123456789012",
            "home_directory": "/tmp/test_duckdb",
            "extension_directory": "/tmp/test_duckdb/ext",
        })

        mock_conn = MagicMock()
        plugin.configure_connection(mock_conn)

        calls = mock_conn.execute.call_args_list
        set_home = [c for c in calls if "home_directory" in str(c)]
        set_ext = [c for c in calls if "extension_directory" in str(c)]
        assert len(set_home) == 1
        assert len(set_ext) == 1

    def test_configure_connection_attaches_catalog(self):
        """Should ATTACH Glue catalog with iceberg type and sigv4 auth"""
        plugin = self._make_plugin({
            "catalog_name": "tadpole",
            "aws_region": "us-east-1",
            "aws_account_id": "123456789012",
        })

        mock_conn = MagicMock()
        plugin.configure_connection(mock_conn)

        calls = [str(c) for c in mock_conn.execute.call_args_list]
        attach_calls = [c for c in calls if "ATTACH" in c]
        assert len(attach_calls) == 1
        attach_sql = attach_calls[0]
        assert "123456789012" in attach_sql
        assert "AS tadpole" in attach_sql
        assert "TYPE iceberg" in attach_sql
        assert "glue.us-east-1.amazonaws.com/iceberg" in attach_sql
        assert "sigv4" in attach_sql

    def test_configure_connection_skips_attach_without_account_id(self):
        """Should skip ATTACH if AWS_ACCOUNT_ID is not set"""
        plugin = self._make_plugin({
            "catalog_name": "tadpole",
            "aws_region": "us-east-1",
            "aws_account_id": "",
        })

        mock_conn = MagicMock()
        plugin.configure_connection(mock_conn)

        calls = [str(c) for c in mock_conn.execute.call_args_list]
        attach_calls = [c for c in calls if "ATTACH" in c]
        assert len(attach_calls) == 0

    def test_config_takes_precedence_over_env(self):
        """Plugin config should override env vars"""
        with patch.dict(os.environ, {
            "GLUE_CATALOG_NAME": "env_catalog",
            "AWS_REGION": "eu-west-1",
            "AWS_ACCOUNT_ID": "111111111111",
        }):
            plugin = self._make_plugin({
                "catalog_name": "config_catalog",
                "aws_region": "us-west-2",
                "aws_account_id": "222222222222",
            })

        assert plugin.catalog_name == "config_catalog"
        assert plugin.aws_region == "us-west-2"
        assert plugin.aws_account_id == "222222222222"
