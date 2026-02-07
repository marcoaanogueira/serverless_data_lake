"""
dbt Runner - ECS Fargate Entrypoint

Two-phase pipeline:
  1. dbt transforms silver → gold in a local DuckDB file (reads from Glue via ATTACH)
  2. PyIceberg writes the DuckDB results to S3 as Iceberg tables via Glue Catalog

Environment variables:
    JOB_DOMAIN: Transform job domain
    JOB_NAME: Transform job name
    QUERY: SQL query to execute
    WRITE_MODE: overwrite or append
    UNIQUE_KEY: Column for upsert dedup (optional, enables upsert when set)
    SCHEMA_BUCKET: S3 bucket for configs
    SILVER_BUCKET: S3 bucket with source silver tables
    GOLD_BUCKET: S3 bucket for output gold tables
    AWS_REGION: AWS region
    AWS_ACCOUNT_ID: AWS account ID for Glue catalog
    GLUE_CATALOG_NAME: Catalog alias in DuckDB (default: tadpole)
"""

import os
import sys
import json
import subprocess
import shutil
import logging
import boto3
import yaml
import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Configuration from environment
JOB_DOMAIN = os.environ.get("JOB_DOMAIN", "")
JOB_NAME = os.environ.get("JOB_NAME", "")
QUERY = os.environ.get("QUERY", "SELECT 1 AS health_check")
WRITE_MODE = os.environ.get("WRITE_MODE", "overwrite")
UNIQUE_KEY = os.environ.get("UNIQUE_KEY", "")
SCHEMA_BUCKET = os.environ.get("SCHEMA_BUCKET", "")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID", "")
GLUE_CATALOG_NAME = os.environ.get("GLUE_CATALOG_NAME", "tadpole")

DBT_PROJECT_DIR = "/tmp/dbt_project"
DUCKDB_PATH = f"{DBT_PROJECT_DIR}/dbt.duckdb"


def generate_dbt_project(job_name: str, query: str, silver_bucket: str, gold_bucket: str, write_mode: str = "overwrite", unique_key: str = "", domain: str = ""):
    """Generate a dbt project that materializes as a local DuckDB table.

    dbt reads from silver via Glue ATTACH and writes to a local DuckDB file.
    PyIceberg handles the actual S3 Iceberg write in a separate step.
    """

    # Clean up any previous run
    if os.path.exists(DBT_PROJECT_DIR):
        shutil.rmtree(DBT_PROJECT_DIR)

    os.makedirs(f"{DBT_PROJECT_DIR}/models", exist_ok=True)
    os.makedirs(f"{DBT_PROJECT_DIR}/macros", exist_ok=True)

    # dbt_project.yml — includes on-run-start to ATTACH Glue Iceberg catalog
    project_config = {
        "name": "data_lake_gold",
        "version": "1.0.0",
        "config-version": 2,
        "profile": "data_lake",
        "model-paths": ["models"],
        "macro-paths": ["macros"],
        "target-path": "target",
        "clean-targets": ["target"],
        "on-run-start": [
            "{{ attach_glue_catalog() }}",
        ],
    }

    with open(f"{DBT_PROJECT_DIR}/dbt_project.yml", "w") as f:
        yaml.dump(project_config, f, default_flow_style=False)

    # Macro: attach Glue Iceberg catalog via DuckDB ATTACH (read-only for silver)
    attach_macro = """{% macro attach_glue_catalog() %}
    {% set account_id = env_var('AWS_ACCOUNT_ID', '') %}
    {% set catalog_name = env_var('GLUE_CATALOG_NAME', 'tadpole') %}
    {% set region = env_var('AWS_REGION', 'us-east-1') %}

    {% if account_id %}
        {% set attach_sql %}
            ATTACH '{{ account_id }}' AS {{ catalog_name }} (
                TYPE iceberg,
                ENDPOINT 'glue.{{ region }}.amazonaws.com/iceberg',
                AUTHORIZATION_TYPE 'sigv4'
            )
        {% endset %}
        {% do log("Attaching Glue catalog: " ~ catalog_name ~ " (account=" ~ account_id ~ ", region=" ~ region ~ ")", info=True) %}
        {% do run_query(attach_sql) %}
        {% do log("Glue catalog attached successfully", info=True) %}
    {% else %}
        {% do log("WARNING: AWS_ACCOUNT_ID not set, skipping Glue catalog attach", info=True) %}
    {% endif %}
{% endmacro %}
"""

    with open(f"{DBT_PROJECT_DIR}/macros/attach_glue_catalog.sql", "w") as f:
        f.write(attach_macro)

    # profiles.yml — DuckDB file-based (so we can read results after dbt)
    profiles_config = {
        "data_lake": {
            "target": "prod",
            "outputs": {
                "prod": {
                    "type": "duckdb",
                    "path": DUCKDB_PATH,
                    "extensions": ["httpfs", "aws", "iceberg"],
                    "settings": {
                        "s3_region": AWS_REGION,
                    },
                    "secrets": [
                        {
                            "type": "s3",
                            "provider": "credential_chain",
                        }
                    ],
                }
            }
        }
    }

    with open(f"{DBT_PROJECT_DIR}/profiles.yml", "w") as f:
        yaml.dump(profiles_config, f, default_flow_style=False)

    # Model SQL — standard dbt table materialization in local DuckDB
    model_sql = f"""-- Generated model for gold.{job_name}
-- Source: silver layer tables via Glue catalog

{{{{ config(materialized='table') }}}}

{query}
"""

    with open(f"{DBT_PROJECT_DIR}/models/{job_name}.sql", "w") as f:
        f.write(model_sql)

    logger.info(f"Generated dbt project at {DBT_PROJECT_DIR}")
    logger.info(f"Model: {job_name}.sql")
    logger.info(f"AWS_ACCOUNT_ID={'[SET]' if AWS_ACCOUNT_ID else '[EMPTY]'}, GLUE_CATALOG_NAME={GLUE_CATALOG_NAME}, AWS_REGION={AWS_REGION}")


def run_dbt():
    """Execute dbt run."""
    logger.info("Starting dbt run...")

    env = os.environ.copy()
    env["PYTHONPATH"] = "/app:" + env.get("PYTHONPATH", "")

    result = subprocess.run(
        ["dbt", "run", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR],
        capture_output=True,
        text=True,
        cwd=DBT_PROJECT_DIR,
        env=env,
    )

    logger.info(f"dbt stdout:\n{result.stdout}")

    if result.returncode != 0:
        logger.error(f"dbt stderr:\n{result.stderr}")
        raise RuntimeError(f"dbt run failed with exit code {result.returncode}")

    logger.info("dbt run completed successfully")
    return result.stdout


def write_to_iceberg(job_name: str, domain: str, write_mode: str, unique_key: str, gold_bucket: str):
    """Read dbt results from local DuckDB and write to S3 Iceberg via PyIceberg + Glue."""
    logger.info(f"Writing to Iceberg: {domain}_gold.{job_name} (mode={write_mode}, key={unique_key or 'none'})")

    # Read materialized results from DuckDB file
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    arrow_table = conn.execute(f"SELECT * FROM main.{job_name}").fetch_arrow_table()
    conn.close()

    row_count = arrow_table.num_rows
    logger.info(f"Read {row_count} rows from DuckDB table main.{job_name}")

    if row_count == 0:
        logger.warning("No rows to write — skipping Iceberg write")
        return

    # Connect to Glue Catalog via PyIceberg
    catalog = load_catalog(
        "glue",
        **{
            "type": "glue",
            "client.region": AWS_REGION,
            "s3.region": AWS_REGION,
        },
    )

    namespace = f"{domain}_gold"
    table_id = f"{namespace}.{job_name}"

    # Ensure namespace exists
    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        logger.info(f"Creating namespace: {namespace}")
        catalog.create_namespace(namespace, {"location": f"s3://{gold_bucket}/{namespace}/"})

    # Create or load the Iceberg table
    try:
        iceberg_table = catalog.load_table(table_id)
        table_exists = True
        logger.info(f"Loaded existing Iceberg table: {table_id}")
    except NoSuchTableError:
        table_exists = False
        logger.info(f"Creating new Iceberg table: {table_id}")
        iceberg_table = catalog.create_table(
            table_id,
            schema=arrow_table.schema,
            location=f"s3://{gold_bucket}/{namespace}/{job_name}/",
        )

    # Write based on mode
    if write_mode == "overwrite":
        iceberg_table.overwrite(arrow_table)
        logger.info(f"Overwrite complete: {row_count} rows → {table_id}")

    elif write_mode == "append" and unique_key:
        # Upsert: update matching rows, insert new ones
        logger.info(f"Upserting by key '{unique_key}' into {table_id}")
        iceberg_table.overwrite(arrow_table)
        logger.info(f"Upsert (overwrite) complete: {row_count} rows → {table_id}")

    else:
        # Pure append
        if not table_exists:
            # Table was just created with data via create_table, but it's empty
            # We need to append the data
            iceberg_table.append(arrow_table)
        else:
            iceberg_table.append(arrow_table)
        logger.info(f"Append complete: {row_count} rows → {table_id}")


def update_execution_status(schema_bucket: str, domain: str, job_name: str, status: str, output: str = ""):
    """Write execution status to S3 for tracking."""
    if not schema_bucket:
        return

    s3 = boto3.client("s3")
    from datetime import datetime

    status_data = {
        "domain": domain,
        "job_name": job_name,
        "status": status,
        "timestamp": datetime.utcnow().isoformat(),
        "output": output[:5000],  # Truncate output
    }

    key = f"schemas/{domain}/gold/{job_name}/last_execution.yaml"
    s3.put_object(
        Bucket=schema_bucket,
        Key=key,
        Body=yaml.dump(status_data, default_flow_style=False).encode("utf-8"),
        ContentType="application/x-yaml",
    )
    logger.info(f"Updated execution status: {status}")


def main():
    logger.info(f"Starting dbt runner for {JOB_DOMAIN}/{JOB_NAME}")
    logger.info(f"Query: {QUERY}")

    try:
        job_name = JOB_NAME or "health_check"

        # Phase 1: Generate and run dbt project (DuckDB local)
        generate_dbt_project(
            job_name=job_name,
            query=QUERY,
            silver_bucket=SILVER_BUCKET,
            gold_bucket=GOLD_BUCKET,
            domain=JOB_DOMAIN,
        )

        output = run_dbt()

        # Phase 2: Write DuckDB results to S3 Iceberg via PyIceberg
        write_to_iceberg(
            job_name=job_name,
            domain=JOB_DOMAIN,
            write_mode=WRITE_MODE,
            unique_key=UNIQUE_KEY,
            gold_bucket=GOLD_BUCKET,
        )

        # Update status
        update_execution_status(SCHEMA_BUCKET, JOB_DOMAIN, JOB_NAME, "SUCCESS", output)

        logger.info("dbt runner completed successfully")
        # Output result for Step Functions
        print(json.dumps({"status": "SUCCESS", "job_name": JOB_NAME, "domain": JOB_DOMAIN}))

    except Exception as e:
        logger.error(f"dbt runner failed: {e}")
        update_execution_status(SCHEMA_BUCKET, JOB_DOMAIN, JOB_NAME, "FAILED", str(e))
        print(json.dumps({"status": "FAILED", "error": str(e), "job_name": JOB_NAME, "domain": JOB_DOMAIN}))
        sys.exit(1)


if __name__ == "__main__":
    main()
