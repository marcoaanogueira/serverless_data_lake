"""
Tests for dbt Runner entrypoint and Glue Iceberg plugin

Tests covering:
- Query rewriting (domain.layer.table -> catalog.domain_layer.table)
- Query processing for dbt (ref() substitution for dependency jobs)
- Tag computation (cron schedule mapping + dependency inheritance)
- dbt project generation (single mode + multi-model mode)
- on-run-start macro for Glue Iceberg catalog attach
- PyIceberg write_to_iceberg function
- Glue Iceberg plugin initialization and connection configuration
"""

import pytest
import os
import shutil
import yaml
from unittest.mock import MagicMock, patch, call

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'containers', 'dbt_runner'))

from containers.dbt_runner.entrypoint import (
    generate_dbt_project, generate_multi_model_project,
    rewrite_query, process_query_for_dbt,
    compute_effective_tags, SCHEDULE_TO_TAG, FREQUENCY_ORDER,
    DBT_PROJECT_DIR, OUTPUT_DIR, OUTPUT_PARQUET,
)
from containers.dbt_runner.glue_iceberg_plugin import Plugin as GlueIcebergPlugin


# =============================================================================
# Query Rewriting
# =============================================================================

class TestRewriteQuery:
    """Test domain.layer.table -> catalog.domain_layer.table rewriting."""

    def test_silver_rewrite(self):
        sql = "SELECT * FROM sales.silver.teste"
        assert rewrite_query(sql, "tadpole") == "SELECT * FROM tadpole.sales_silver.teste"

    def test_gold_rewrite(self):
        sql = "SELECT * FROM sales.gold.report"
        assert rewrite_query(sql, "tadpole") == "SELECT * FROM tadpole.sales_gold.report"

    def test_multiple_tables(self):
        sql = "SELECT a.id, b.name FROM sales.silver.orders a JOIN sales.silver.customers b ON a.cust_id = b.id"
        result = rewrite_query(sql, "tadpole")
        assert "tadpole.sales_silver.orders" in result
        assert "tadpole.sales_silver.customers" in result

    def test_mixed_layers(self):
        sql = "SELECT * FROM sales.silver.raw_data UNION SELECT * FROM sales.gold.agg_data"
        result = rewrite_query(sql, "tadpole")
        assert "tadpole.sales_silver.raw_data" in result
        assert "tadpole.sales_gold.agg_data" in result

    def test_different_domains(self):
        sql = "SELECT * FROM marketing.silver.campaigns JOIN sales.silver.orders ON 1=1"
        result = rewrite_query(sql, "tadpole")
        assert "tadpole.marketing_silver.campaigns" in result
        assert "tadpole.sales_silver.orders" in result

    def test_no_rewrite_for_other_patterns(self):
        sql = "SELECT * FROM some_table WHERE x = 1"
        assert rewrite_query(sql, "tadpole") == sql

    def test_no_rewrite_for_already_correct(self):
        sql = "SELECT * FROM tadpole.sales_silver.teste"
        assert rewrite_query(sql, "tadpole") == sql

    def test_uses_default_catalog(self):
        sql = "SELECT * FROM sales.silver.teste"
        result = rewrite_query(sql)
        assert "sales_silver.teste" in result

    def test_model_sql_has_rewritten_query(self):
        """generate_dbt_project should rewrite the query in the model SQL"""
        generate_dbt_project("test_job", "SELECT * FROM sales.silver.teste", "s", "g")
        with open(f"{DBT_PROJECT_DIR}/models/test_job.sql") as f:
            content = f.read()
        assert "sales.silver.teste" not in content
        assert "sales_silver.teste" in content


# =============================================================================
# Query Processing for dbt (ref() substitution)
# =============================================================================

class TestProcessQueryForDbt:
    """Test process_query_for_dbt: ref() for deps, rewrite for cron."""

    def _make_job(self, name, query, schedule_type="cron", deps=None):
        return {
            "job_name": name,
            "domain": "sales",
            "query": query,
            "schedule_type": schedule_type,
            "dependencies": deps or [],
        }

    def test_cron_job_rewrites_silver(self):
        job = self._make_job("report", "SELECT * FROM sales.silver.vendas")
        all_jobs = [job]
        result = process_query_for_dbt(job, all_jobs, "tadpole")
        assert "tadpole.sales_silver.vendas" in result
        assert "ref(" not in result

    def test_cron_job_does_not_substitute_ref(self):
        """Cron jobs never get ref() — even for gold table refs"""
        other = self._make_job("base_table", "SELECT 1")
        job = self._make_job("report", "SELECT * FROM sales.gold.base_table")
        all_jobs = [other, job]
        result = process_query_for_dbt(job, all_jobs, "tadpole")
        assert "ref(" not in result
        assert "tadpole.sales_gold.base_table" in result

    def test_dependency_job_gets_ref(self):
        """Dependency jobs get ref() for known gold jobs"""
        base = self._make_job("vendas_agg", "SELECT * FROM sales.silver.vendas")
        dep = self._make_job("report", "SELECT * FROM sales.gold.vendas_agg", "dependency", ["vendas_agg"])
        all_jobs = [base, dep]
        result = process_query_for_dbt(dep, all_jobs, "tadpole")
        assert "{{ ref('vendas_agg') }}" in result
        assert "sales.gold.vendas_agg" not in result

    def test_dependency_preserves_silver_refs(self):
        """Dependency jobs still rewrite silver refs normally"""
        base = self._make_job("vendas_agg", "SELECT 1")
        dep = self._make_job("report",
            "SELECT a.*, b.name FROM sales.gold.vendas_agg a JOIN sales.silver.customers b ON a.id = b.id",
            "dependency", ["vendas_agg"])
        all_jobs = [base, dep]
        result = process_query_for_dbt(dep, all_jobs, "tadpole")
        assert "{{ ref('vendas_agg') }}" in result
        assert "tadpole.sales_silver.customers" in result

    def test_dependency_unknown_gold_ref_gets_rewritten(self):
        """Gold refs to non-existent jobs get normal catalog rewrite"""
        dep = self._make_job("report", "SELECT * FROM sales.gold.unknown_table", "dependency")
        all_jobs = [dep]
        result = process_query_for_dbt(dep, all_jobs, "tadpole")
        assert "ref(" not in result
        assert "tadpole.sales_gold.unknown_table" in result

    def test_dependency_multiple_refs(self):
        """Multiple gold refs in one dependency query"""
        base1 = self._make_job("vendas_agg", "SELECT 1")
        base2 = self._make_job("customers_active", "SELECT 1")
        dep = self._make_job("final_report",
            "SELECT * FROM sales.gold.vendas_agg JOIN sales.gold.customers_active ON 1=1",
            "dependency", ["vendas_agg", "customers_active"])
        all_jobs = [base1, base2, dep]
        result = process_query_for_dbt(dep, all_jobs, "tadpole")
        assert "{{ ref('vendas_agg') }}" in result
        assert "{{ ref('customers_active') }}" in result


# =============================================================================
# Tag Computation
# =============================================================================

class TestComputeEffectiveTags:
    """Test compute_effective_tags: cron mapping + dependency inheritance."""

    def _make_job(self, name, schedule_type="cron", cron_schedule="day", deps=None):
        return {
            "job_name": name,
            "domain": "sales",
            "schedule_type": schedule_type,
            "cron_schedule": cron_schedule,
            "dependencies": deps or [],
        }

    def test_cron_hour(self):
        jobs = [self._make_job("j1", cron_schedule="hour")]
        tags = compute_effective_tags(jobs)
        assert tags["j1"] == "hourly"

    def test_cron_day(self):
        jobs = [self._make_job("j1", cron_schedule="day")]
        tags = compute_effective_tags(jobs)
        assert tags["j1"] == "daily"

    def test_cron_month(self):
        jobs = [self._make_job("j1", cron_schedule="month")]
        tags = compute_effective_tags(jobs)
        assert tags["j1"] == "monthly"

    def test_cron_unknown_defaults_daily(self):
        jobs = [self._make_job("j1", cron_schedule="unknown")]
        tags = compute_effective_tags(jobs)
        assert tags["j1"] == "daily"

    def test_dependency_inherits_from_consumer(self):
        """Dependency job inherits tag from its consumer"""
        dep = self._make_job("base", "dependency")
        consumer = self._make_job("report", "cron", "hour", deps=["base"])
        tags = compute_effective_tags([dep, consumer])
        assert tags["report"] == "hourly"
        assert tags["base"] == "hourly"

    def test_dependency_picks_highest_frequency(self):
        """Dependency consumed by hourly and monthly -> gets hourly"""
        dep = self._make_job("base", "dependency")
        hourly = self._make_job("fast_report", "cron", "hour", deps=["base"])
        monthly = self._make_job("slow_report", "cron", "month", deps=["base"])
        tags = compute_effective_tags([dep, hourly, monthly])
        assert tags["base"] == "hourly"

    def test_chain_dependency(self):
        """A -> B -> C (hourly): both A and B get hourly"""
        a = self._make_job("a", "dependency")
        b = self._make_job("b", "dependency", deps=["a"])
        c = self._make_job("c", "cron", "hour", deps=["b"])
        tags = compute_effective_tags([a, b, c])
        assert tags["a"] == "hourly"
        assert tags["b"] == "hourly"
        assert tags["c"] == "hourly"

    def test_orphan_dependency_defaults_daily(self):
        """Dependency with no consumers defaults to daily"""
        dep = self._make_job("orphan", "dependency")
        tags = compute_effective_tags([dep])
        assert tags["orphan"] == "daily"

    def test_all_cron_jobs(self):
        """Multiple cron jobs get their own tags independently"""
        jobs = [
            self._make_job("j1", cron_schedule="hour"),
            self._make_job("j2", cron_schedule="day"),
            self._make_job("j3", cron_schedule="month"),
        ]
        tags = compute_effective_tags(jobs)
        assert tags == {"j1": "hourly", "j2": "daily", "j3": "monthly"}

    def test_mixed_domains(self):
        """Tag computation works across domains"""
        dep = {"job_name": "shared_base", "domain": "common", "schedule_type": "dependency", "dependencies": []}
        consumer = {"job_name": "sales_report", "domain": "sales", "schedule_type": "cron", "cron_schedule": "hour", "dependencies": ["shared_base"]}
        tags = compute_effective_tags([dep, consumer])
        assert tags["shared_base"] == "hourly"

    def test_schedule_to_tag_mapping(self):
        assert SCHEDULE_TO_TAG == {"hour": "hourly", "day": "daily", "month": "monthly"}

    def test_frequency_order(self):
        assert FREQUENCY_ORDER["hourly"] < FREQUENCY_ORDER["daily"] < FREQUENCY_ORDER["monthly"]


# =============================================================================
# dbt Project Generation (single mode)
# =============================================================================

class TestGenerateDbtProject:
    def setup_method(self):
        if os.path.exists(DBT_PROJECT_DIR):
            shutil.rmtree(DBT_PROJECT_DIR)

    def teardown_method(self):
        if os.path.exists(DBT_PROJECT_DIR):
            shutil.rmtree(DBT_PROJECT_DIR)

    def test_generates_project_yml(self):
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        project_path = f"{DBT_PROJECT_DIR}/dbt_project.yml"
        assert os.path.exists(project_path)

        with open(project_path) as f:
            config = yaml.safe_load(f)

        assert config["name"] == "data_lake_gold"
        assert config["version"] == "1.0.0"
        assert config["profile"] == "data_lake"

    def test_project_has_on_run_start(self):
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        with open(f"{DBT_PROJECT_DIR}/dbt_project.yml") as f:
            config = yaml.safe_load(f)

        assert "on-run-start" in config
        hooks = config["on-run-start"]
        assert len(hooks) >= 1
        assert "attach_glue_catalog" in hooks[0]

    def test_generates_attach_macro(self):
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        macro_path = f"{DBT_PROJECT_DIR}/macros/attach_glue_catalog.sql"
        assert os.path.exists(macro_path)

        with open(macro_path) as f:
            content = f.read()

        assert "macro attach_glue_catalog" in content
        assert "ATTACH" in content
        assert "TYPE iceberg" in content
        assert "ENDPOINT" in content
        assert "sigv4" in content
        assert "env_var('AWS_ACCOUNT_ID'" in content
        assert "env_var('GLUE_CATALOG_NAME'" in content
        assert "env_var('AWS_REGION'" in content

    def test_profiles_uses_in_memory_duckdb(self):
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        with open(f"{DBT_PROJECT_DIR}/profiles.yml") as f:
            config = yaml.safe_load(f)

        prod = config["data_lake"]["outputs"]["prod"]
        assert prod["type"] == "duckdb"
        assert prod["path"] == ":memory:"

    def test_profiles_has_extensions(self):
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        with open(f"{DBT_PROJECT_DIR}/profiles.yml") as f:
            config = yaml.safe_load(f)

        extensions = config["data_lake"]["outputs"]["prod"]["extensions"]
        assert "httpfs" in extensions
        assert "aws" in extensions
        assert "iceberg" in extensions

    def test_profiles_has_s3_secret(self):
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        with open(f"{DBT_PROJECT_DIR}/profiles.yml") as f:
            config = yaml.safe_load(f)

        secrets = config["data_lake"]["outputs"]["prod"]["secrets"]
        assert len(secrets) >= 1
        s3_secret = secrets[0]
        assert s3_secret["type"] == "s3"
        assert s3_secret["provider"] == "credential_chain"

    def test_profiles_has_no_plugins(self):
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        with open(f"{DBT_PROJECT_DIR}/profiles.yml") as f:
            config = yaml.safe_load(f)

        prod = config["data_lake"]["outputs"]["prod"]
        assert "plugins" not in prod

    def test_generates_model_sql_with_parquet_export(self):
        query = "SELECT id, name FROM silver.customers WHERE active = true"
        generate_dbt_project("active_customers", query, "silver-bucket", "gold-bucket", write_mode="overwrite")

        model_path = f"{DBT_PROJECT_DIR}/models/active_customers.sql"
        assert os.path.exists(model_path)

        with open(model_path) as f:
            content = f.read()

        assert query in content
        assert "materialized='table'" in content
        assert "COPY" in content
        assert "FORMAT PARQUET" in content
        assert OUTPUT_PARQUET in content

    def test_model_sql_same_for_all_write_modes(self):
        for mode in ["overwrite", "append"]:
            generate_dbt_project("test", "SELECT 1", "s", "g", write_mode=mode)
            with open(f"{DBT_PROJECT_DIR}/models/test.sql") as f:
                content = f.read()
            assert "materialized='table'" in content

    def test_creates_directories(self):
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        assert os.path.isdir(f"{DBT_PROJECT_DIR}/models")
        assert os.path.isdir(f"{DBT_PROJECT_DIR}/macros")
        assert os.path.isdir(OUTPUT_DIR)

    def test_cleans_previous_run(self):
        os.makedirs(f"{DBT_PROJECT_DIR}/models", exist_ok=True)
        with open(f"{DBT_PROJECT_DIR}/models/old_model.sql", "w") as f:
            f.write("-- old")

        generate_dbt_project("new_job", "SELECT 1", "silver-bucket", "gold-bucket")

        assert not os.path.exists(f"{DBT_PROJECT_DIR}/models/old_model.sql")
        assert os.path.exists(f"{DBT_PROJECT_DIR}/models/new_job.sql")


# =============================================================================
# Multi-Model dbt Project Generation (scheduled mode)
# =============================================================================

class TestGenerateMultiModelProject:
    def setup_method(self):
        if os.path.exists(DBT_PROJECT_DIR):
            shutil.rmtree(DBT_PROJECT_DIR)

    def teardown_method(self):
        if os.path.exists(DBT_PROJECT_DIR):
            shutil.rmtree(DBT_PROJECT_DIR)

    def _make_job(self, name, query="SELECT 1", schedule_type="cron", cron="day", deps=None):
        return {
            "job_name": name,
            "domain": "sales",
            "query": query,
            "schedule_type": schedule_type,
            "cron_schedule": cron,
            "dependencies": deps or [],
        }

    def test_generates_model_per_job(self):
        jobs = [self._make_job("j1"), self._make_job("j2"), self._make_job("j3")]
        tags = {"j1": "daily", "j2": "daily", "j3": "daily"}
        generate_multi_model_project(jobs, tags)

        assert os.path.exists(f"{DBT_PROJECT_DIR}/models/j1.sql")
        assert os.path.exists(f"{DBT_PROJECT_DIR}/models/j2.sql")
        assert os.path.exists(f"{DBT_PROJECT_DIR}/models/j3.sql")

    def test_model_has_correct_tag(self):
        jobs = [self._make_job("hourly_job", cron="hour")]
        tags = {"hourly_job": "hourly"}
        generate_multi_model_project(jobs, tags)

        with open(f"{DBT_PROJECT_DIR}/models/hourly_job.sql") as f:
            content = f.read()

        assert "tags=['hourly']" in content

    def test_model_has_per_job_parquet_output(self):
        jobs = [self._make_job("my_job")]
        tags = {"my_job": "daily"}
        generate_multi_model_project(jobs, tags)

        with open(f"{DBT_PROJECT_DIR}/models/my_job.sql") as f:
            content = f.read()

        assert f"{OUTPUT_DIR}/my_job.parquet" in content

    def test_dependency_model_has_ref(self):
        base = self._make_job("vendas_agg", "SELECT * FROM sales.silver.vendas")
        dep = self._make_job("report", "SELECT * FROM sales.gold.vendas_agg", "dependency", deps=["vendas_agg"])
        jobs = [base, dep]
        tags = {"vendas_agg": "daily", "report": "daily"}
        generate_multi_model_project(jobs, tags)

        with open(f"{DBT_PROJECT_DIR}/models/report.sql") as f:
            content = f.read()

        assert "{{ ref('vendas_agg') }}" in content
        assert "sales.gold.vendas_agg" not in content

    def test_creates_outputs_dir(self):
        jobs = [self._make_job("j1")]
        tags = {"j1": "daily"}
        generate_multi_model_project(jobs, tags)

        assert os.path.isdir(OUTPUT_DIR)

    def test_project_skeleton_is_valid(self):
        jobs = [self._make_job("j1")]
        tags = {"j1": "daily"}
        generate_multi_model_project(jobs, tags)

        with open(f"{DBT_PROJECT_DIR}/dbt_project.yml") as f:
            config = yaml.safe_load(f)
        assert config["name"] == "data_lake_gold"

        assert os.path.exists(f"{DBT_PROJECT_DIR}/profiles.yml")
        assert os.path.exists(f"{DBT_PROJECT_DIR}/macros/attach_glue_catalog.sql")


# =============================================================================
# Fetch Job Configs (mocked S3)
# =============================================================================

class TestFetchAllJobConfigs:
    @patch("containers.dbt_runner.entrypoint.boto3")
    def test_fetches_gold_configs(self, mock_boto3):
        mock_s3 = MagicMock()
        mock_boto3.client.return_value = mock_s3

        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "schemas/sales/gold/vendas_agg/config.yaml"},
                    {"Key": "schemas/sales/gold/report/config.yaml"},
                    {"Key": "schemas/sales/silver/vendas/config.yaml"},  # should be skipped
                ]
            }
        ]

        config_yaml = yaml.dump({"query": "SELECT 1", "schedule_type": "cron", "cron_schedule": "day"})
        mock_body = MagicMock()
        mock_body.read.return_value = config_yaml.encode()
        mock_s3.get_object.return_value = {"Body": mock_body}

        from containers.dbt_runner.entrypoint import fetch_all_job_configs
        jobs = fetch_all_job_configs("my-bucket")

        assert len(jobs) == 2
        assert jobs[0]["domain"] == "sales"
        assert jobs[0]["job_name"] == "vendas_agg"
        assert jobs[1]["job_name"] == "report"

    @patch("containers.dbt_runner.entrypoint.boto3")
    def test_empty_bucket(self, mock_boto3):
        mock_s3 = MagicMock()
        mock_boto3.client.return_value = mock_s3

        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": []}]

        from containers.dbt_runner.entrypoint import fetch_all_job_configs
        jobs = fetch_all_job_configs("empty-bucket")
        assert jobs == []


# =============================================================================
# PyIceberg Write Layer
# =============================================================================

class TestWriteToIceberg:
    """Test write_to_iceberg function with mocked DuckDB and PyIceberg."""

    @patch("containers.dbt_runner.entrypoint.load_catalog")
    @patch("containers.dbt_runner.entrypoint.os.path.exists", return_value=True)
    @patch("pyarrow.parquet.read_table")
    def test_overwrite_mode(self, mock_read_table, mock_exists, mock_load_catalog):
        mock_arrow = MagicMock()
        mock_arrow.num_rows = 10
        mock_arrow.schema = MagicMock()
        mock_read_table.return_value = mock_arrow

        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        from containers.dbt_runner.entrypoint import write_to_iceberg
        write_to_iceberg("test_job", "sales", "overwrite", "", "gold-bucket")

        mock_table.overwrite.assert_called_once_with(mock_arrow)

    @patch("containers.dbt_runner.entrypoint.load_catalog")
    @patch("containers.dbt_runner.entrypoint.os.path.exists", return_value=True)
    @patch("pyarrow.parquet.read_table")
    def test_append_mode(self, mock_read_table, mock_exists, mock_load_catalog):
        mock_arrow = MagicMock()
        mock_arrow.num_rows = 5
        mock_arrow.schema = MagicMock()
        mock_read_table.return_value = mock_arrow

        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        from containers.dbt_runner.entrypoint import write_to_iceberg
        write_to_iceberg("test_job", "sales", "append", "", "gold-bucket")

        mock_table.append.assert_called_once_with(mock_arrow)

    @patch("containers.dbt_runner.entrypoint.load_catalog")
    @patch("containers.dbt_runner.entrypoint.os.path.exists", return_value=True)
    @patch("pyarrow.parquet.read_table")
    def test_upsert_mode(self, mock_read_table, mock_exists, mock_load_catalog):
        mock_arrow = MagicMock()
        mock_arrow.num_rows = 3
        mock_arrow.schema = MagicMock()
        mock_read_table.return_value = mock_arrow

        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        from containers.dbt_runner.entrypoint import write_to_iceberg
        write_to_iceberg("test_job", "sales", "append", "id", "gold-bucket")

        mock_table.overwrite.assert_called_once_with(mock_arrow)

    @patch("containers.dbt_runner.entrypoint.load_catalog")
    @patch("containers.dbt_runner.entrypoint.os.path.exists", return_value=True)
    @patch("pyarrow.parquet.read_table")
    def test_skips_write_on_zero_rows(self, mock_read_table, mock_exists, mock_load_catalog):
        mock_arrow = MagicMock()
        mock_arrow.num_rows = 0
        mock_read_table.return_value = mock_arrow

        from containers.dbt_runner.entrypoint import write_to_iceberg
        write_to_iceberg("test_job", "sales", "overwrite", "", "gold-bucket")

        mock_load_catalog.assert_not_called()

    @patch("containers.dbt_runner.entrypoint.load_catalog")
    @patch("containers.dbt_runner.entrypoint.os.path.exists", return_value=True)
    @patch("pyarrow.parquet.read_table")
    def test_creates_namespace_if_missing(self, mock_read_table, mock_exists, mock_load_catalog):
        from pyiceberg.exceptions import NoSuchNamespaceError

        mock_arrow = MagicMock()
        mock_arrow.num_rows = 1
        mock_arrow.schema = MagicMock()
        mock_read_table.return_value = mock_arrow

        mock_catalog = MagicMock()
        mock_catalog.load_namespace_properties.side_effect = NoSuchNamespaceError("sales_gold")
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        from containers.dbt_runner.entrypoint import write_to_iceberg
        write_to_iceberg("test_job", "sales", "overwrite", "", "gold-bucket")

        mock_catalog.create_namespace.assert_called_once()
        call_args = mock_catalog.create_namespace.call_args
        assert call_args[0][0] == "sales_gold"

    @patch("containers.dbt_runner.entrypoint.load_catalog")
    @patch("containers.dbt_runner.entrypoint.os.path.exists", return_value=True)
    @patch("pyarrow.parquet.read_table")
    def test_creates_table_if_not_exists(self, mock_read_table, mock_exists, mock_load_catalog):
        from pyiceberg.exceptions import NoSuchTableError

        mock_arrow = MagicMock()
        mock_arrow.num_rows = 5
        mock_arrow.schema = MagicMock()
        mock_read_table.return_value = mock_arrow

        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = NoSuchTableError("sales_gold.test_job")
        mock_new_table = MagicMock()
        mock_catalog.create_table.return_value = mock_new_table
        mock_load_catalog.return_value = mock_catalog

        from containers.dbt_runner.entrypoint import write_to_iceberg
        write_to_iceberg("test_job", "sales", "overwrite", "", "gold-bucket")

        mock_catalog.create_table.assert_called_once()
        call_args = mock_catalog.create_table.call_args
        assert call_args[0][0] == "sales_gold.test_job"
        assert "s3://gold-bucket/sales_gold/test_job/" in str(call_args)

    @patch("containers.dbt_runner.entrypoint.load_catalog")
    @patch("containers.dbt_runner.entrypoint.os.path.exists", return_value=True)
    @patch("pyarrow.parquet.read_table")
    def test_custom_parquet_path(self, mock_read_table, mock_exists, mock_load_catalog):
        """Should use custom parquet_path when provided"""
        mock_arrow = MagicMock()
        mock_arrow.num_rows = 1
        mock_arrow.schema = MagicMock()
        mock_read_table.return_value = mock_arrow

        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        from containers.dbt_runner.entrypoint import write_to_iceberg
        write_to_iceberg("j1", "sales", "overwrite", "", "gold-bucket",
                        parquet_path="/tmp/custom/j1.parquet")

        mock_read_table.assert_called_once_with("/tmp/custom/j1.parquet")


# =============================================================================
# Glue Iceberg Plugin (kept as fallback/alternative)
# =============================================================================

class TestGlueIcebergPlugin:
    def _make_plugin(self, config):
        return GlueIcebergPlugin("glue_iceberg", config)

    def test_initialize_from_config(self):
        plugin = self._make_plugin({
            "catalog_name": "my_catalog",
            "aws_region": "eu-west-1",
            "aws_account_id": "111222333444",
        })

        assert plugin.catalog_name == "my_catalog"
        assert plugin.aws_region == "eu-west-1"
        assert plugin.aws_account_id == "111222333444"

    def test_initialize_defaults_from_env(self):
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
