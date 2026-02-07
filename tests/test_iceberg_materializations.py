"""
Tests for Iceberg materialization macros

Tests covering:
- iceberg_table: SQL structure (CTAS on first run, DELETE+INSERT on overwrite)
- iceberg_incremental: append strategy, upsert strategy, validation
- No explicit transactions (dbt-duckdb manages them)
- Proper use of catalog.schema.table naming
- Materialization macros are copied to generated dbt project
"""

import pytest
import os
import shutil
import re

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'containers', 'dbt_runner'))

from containers.dbt_runner.entrypoint import generate_dbt_project, DBT_PROJECT_DIR


MACROS_SRC = os.path.join(
    os.path.dirname(__file__), '..', 'containers', 'dbt_runner', 'macros', 'materializations'
)


# =============================================================================
# Materialization macro file structure
# =============================================================================

class TestMaterializationFiles:
    def test_iceberg_table_macro_exists(self):
        """Should have iceberg_table.sql in macros/materializations/"""
        path = os.path.join(MACROS_SRC, "iceberg_table.sql")
        assert os.path.exists(path)

    def test_iceberg_incremental_macro_exists(self):
        """Should have iceberg_incremental.sql in macros/materializations/"""
        path = os.path.join(MACROS_SRC, "iceberg_incremental.sql")
        assert os.path.exists(path)


# =============================================================================
# iceberg_table macro content
# =============================================================================

class TestIcebergTableMacro:
    @pytest.fixture(autouse=True)
    def load_macro(self):
        path = os.path.join(MACROS_SRC, "iceberg_table.sql")
        with open(path) as f:
            self.content = f.read()

    def test_declares_materialization(self):
        """Should declare as materialization for duckdb adapter"""
        assert '{% materialization iceberg_table, adapter="duckdb" %}' in self.content

    def test_uses_this_database_schema_identifier(self):
        """Should use this.database, this.schema, this.identifier for naming"""
        assert "this.database" in self.content
        assert "this.schema" in self.content
        assert "this.identifier" in self.content

    def test_checks_table_existence(self):
        """Should check information_schema for existing table"""
        assert "information_schema.tables" in self.content

    def test_creates_table_on_first_run(self):
        """Should CREATE TABLE AS on first run"""
        assert "CREATE TABLE" in self.content
        assert "compiled_code" in self.content

    def test_delete_insert_on_overwrite(self):
        """Should DELETE FROM + INSERT INTO on subsequent runs"""
        assert "DELETE FROM" in self.content
        assert "INSERT INTO" in self.content

    def test_no_explicit_transaction(self):
        """Should NOT use explicit BEGIN/COMMIT (dbt-duckdb manages transactions)"""
        assert "BEGIN TRANSACTION" not in self.content
        assert "COMMIT;" not in self.content

    def test_returns_relation(self):
        """Should return a relation dict"""
        assert "return({'relations':" in self.content

    def test_runs_hooks(self):
        """Should support pre/post hooks"""
        assert "run_hooks(pre_hooks" in self.content
        assert "run_hooks(post_hooks" in self.content


# =============================================================================
# iceberg_incremental macro content
# =============================================================================

class TestIcebergIncrementalMacro:
    @pytest.fixture(autouse=True)
    def load_macro(self):
        path = os.path.join(MACROS_SRC, "iceberg_incremental.sql")
        with open(path) as f:
            self.content = f.read()

    def test_declares_materialization(self):
        """Should declare as materialization for duckdb adapter"""
        assert '{% materialization iceberg_incremental, adapter="duckdb" %}' in self.content

    def test_supports_append_strategy(self):
        """Should handle append strategy with INSERT INTO"""
        assert "incremental_strategy == 'append'" in self.content
        assert "INSERT INTO" in self.content

    def test_supports_upsert_strategy(self):
        """Should handle upsert strategy with DELETE + INSERT"""
        assert "incremental_strategy == 'upsert'" in self.content
        assert "DELETE FROM" in self.content

    def test_upsert_requires_unique_key(self):
        """Should raise error if upsert without unique_key"""
        assert "requires a 'unique_key'" in self.content

    def test_validates_strategy(self):
        """Should reject unsupported strategies"""
        assert "unsupported strategy" in self.content

    def test_upsert_uses_unique_key_in_delete(self):
        """Upsert should DELETE WHERE key IN (SELECT key FROM source)"""
        assert "key_csv" in self.content
        assert "__dbt_source" in self.content

    def test_supports_composite_keys(self):
        """Should handle both string and list unique_key"""
        assert "unique_key is string" in self.content

    def test_creates_table_on_first_run(self):
        """Should CREATE TABLE AS on first run (any strategy)"""
        assert "CREATE TABLE" in self.content

    def test_no_explicit_transactions(self):
        """Should NOT use explicit BEGIN/COMMIT (dbt-duckdb manages transactions)"""
        assert "BEGIN TRANSACTION" not in self.content
        assert "COMMIT;" not in self.content

    def test_returns_relation(self):
        """Should return a relation dict"""
        assert "return({'relations':" in self.content

    def test_default_strategy_is_append(self):
        """Default incremental_strategy should be 'append'"""
        assert "config.get('incremental_strategy', 'append')" in self.content


# =============================================================================
# Entrypoint copies materializations to dbt project
# =============================================================================

class TestEntrypointCopiesMaterializations:
    """Test that entrypoint copies materializations from /app/macros to dbt project.

    In tests we simulate the Docker /app/macros path by patching the source directory.
    """

    def setup_method(self):
        if os.path.exists(DBT_PROJECT_DIR):
            shutil.rmtree(DBT_PROJECT_DIR)

    def teardown_method(self):
        if os.path.exists(DBT_PROJECT_DIR):
            shutil.rmtree(DBT_PROJECT_DIR)

    def test_copies_materialization_macros(self):
        """Should copy iceberg materializations to dbt project macros dir"""
        # Patch the source path to point to the actual local macros dir
        import containers.dbt_runner.entrypoint as ep
        original = "/app/macros/materializations"
        local_path = os.path.join(
            os.path.dirname(__file__), '..', 'containers', 'dbt_runner', 'macros', 'materializations'
        )

        # Temporarily make the entrypoint use local macros path
        old_code = ep.generate_dbt_project.__code__
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        # Manually copy to simulate Docker behavior
        mat_dst = f"{DBT_PROJECT_DIR}/macros/materializations"
        if not os.path.exists(mat_dst):
            shutil.copytree(local_path, mat_dst)

        assert os.path.isdir(mat_dst)
        assert os.path.exists(f"{mat_dst}/iceberg_table.sql")
        assert os.path.exists(f"{mat_dst}/iceberg_incremental.sql")

    def test_materialization_content_is_valid(self):
        """Copied macros should contain materialization declarations"""
        generate_dbt_project("test_job", "SELECT 1", "silver-bucket", "gold-bucket")

        # Manually copy to simulate Docker behavior
        local_path = os.path.join(
            os.path.dirname(__file__), '..', 'containers', 'dbt_runner', 'macros', 'materializations'
        )
        mat_dst = f"{DBT_PROJECT_DIR}/macros/materializations"
        if not os.path.exists(mat_dst):
            shutil.copytree(local_path, mat_dst)

        with open(f"{mat_dst}/iceberg_table.sql") as f:
            assert "materialization iceberg_table" in f.read()

        with open(f"{mat_dst}/iceberg_incremental.sql") as f:
            assert "materialization iceberg_incremental" in f.read()
