"""
dbt-duckdb Plugin: Glue Iceberg Catalog

Bootstraps a DuckDB connection with the AWS Glue Iceberg REST catalog
so that dbt models can reference tables as <catalog>.<schema>.<table>.

Configuration via profiles.yml plugin config:
    plugins:
      - module: glue_iceberg_plugin
        alias: glue_iceberg
        config:
          catalog_name: tadpole
          aws_region: us-east-1
          aws_account_id: "123456789012"
          home_directory: /tmp/duckdb
          extension_directory: /tmp/duckdb/.duckdb/extensions

All config values can also be set via environment variables:
    GLUE_CATALOG_NAME, AWS_REGION, AWS_ACCOUNT_ID,
    DUCKDB_HOME_DIR, DUCKDB_EXTENSION_DIR
"""

import os
import logging

from dbt.adapters.duckdb.plugins import BasePlugin

logger = logging.getLogger(__name__)


class Plugin(BasePlugin):
    """Attach AWS Glue Iceberg catalog to DuckDB connection."""

    def initialize(self, plugin_config: dict):
        self.catalog_name = plugin_config.get(
            "catalog_name", os.environ.get("GLUE_CATALOG_NAME", "tadpole")
        )
        self.aws_region = plugin_config.get(
            "aws_region", os.environ.get("AWS_REGION", "us-east-1")
        )
        self.aws_account_id = plugin_config.get(
            "aws_account_id", os.environ.get("AWS_ACCOUNT_ID", "")
        )
        self.home_directory = plugin_config.get(
            "home_directory", os.environ.get("DUCKDB_HOME_DIR", "/tmp/duckdb")
        )
        self.extension_directory = plugin_config.get(
            "extension_directory",
            os.environ.get("DUCKDB_EXTENSION_DIR", "/tmp/duckdb/.duckdb/extensions"),
        )

    def configure_connection(self, conn):
        """Called once per connection — bootstrap DuckDB for Glue Iceberg."""
        # Ensure directories exist
        os.makedirs(self.extension_directory, exist_ok=True)

        # Set home and extension directories
        conn.execute(f"SET home_directory = '{self.home_directory}';")
        conn.execute(f"SET extension_directory = '{self.extension_directory}';")
        logger.info("Set home_directory=%s, extension_directory=%s", self.home_directory, self.extension_directory)

        # Attach Glue Catalog as Iceberg catalog
        if not self.aws_account_id:
            logger.warning("AWS_ACCOUNT_ID not set — skipping Glue catalog attach")
            return

        attach_sql = (
            f"ATTACH '{self.aws_account_id}' AS {self.catalog_name} ("
            f"TYPE iceberg, "
            f"ENDPOINT 'glue.{self.aws_region}.amazonaws.com/iceberg', "
            f"AUTHORIZATION_TYPE 'sigv4'"
            f");"
        )
        logger.info("Attaching Glue catalog: %s", attach_sql)
        conn.execute(attach_sql)
        logger.info("Glue Iceberg catalog '%s' attached successfully", self.catalog_name)
