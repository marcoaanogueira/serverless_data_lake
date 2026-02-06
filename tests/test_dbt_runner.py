"""
Tests for dbt Runner entrypoint

Tests covering:
- dbt project generation (project files, model SQL, profiles)
- Execution flow
"""

import pytest
import os
import shutil
import yaml
from unittest.mock import MagicMock, patch

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'containers', 'dbt_runner'))

from containers.dbt_runner.entrypoint import generate_dbt_project, DBT_PROJECT_DIR


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
        assert config["data_lake"]["outputs"]["prod"]["type"] == "duckdb"

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
