"""
dbt Runner - ECS Fargate Entrypoint

Reads job config from S3, generates a dbt project dynamically,
runs dbt, and writes results as Iceberg tables to S3.

Environment variables:
    JOB_DOMAIN: Transform job domain
    JOB_NAME: Transform job name
    QUERY: SQL query to execute
    PARTITION_COLUMN: Column used for partitioning
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Configuration from environment
JOB_DOMAIN = os.environ.get("JOB_DOMAIN", "")
JOB_NAME = os.environ.get("JOB_NAME", "")
QUERY = os.environ.get("QUERY", "SELECT 1 AS health_check")
PARTITION_COLUMN = os.environ.get("PARTITION_COLUMN", "")
SCHEMA_BUCKET = os.environ.get("SCHEMA_BUCKET", "")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID", "")
GLUE_CATALOG_NAME = os.environ.get("GLUE_CATALOG_NAME", "tadpole")

DBT_PROJECT_DIR = "/tmp/dbt_project"


def generate_dbt_project(job_name: str, query: str, silver_bucket: str, gold_bucket: str):
    """Generate a dbt project dynamically from job config."""

    # Clean up any previous run
    if os.path.exists(DBT_PROJECT_DIR):
        shutil.rmtree(DBT_PROJECT_DIR)

    os.makedirs(f"{DBT_PROJECT_DIR}/models", exist_ok=True)
    os.makedirs(f"{DBT_PROJECT_DIR}/macros", exist_ok=True)

    # dbt_project.yml
    project_config = {
        "name": "data_lake_gold",
        "version": "1.0.0",
        "config-version": 2,
        "profile": "data_lake",
        "model-paths": ["models"],
        "macro-paths": ["macros"],
        "target-path": "target",
        "clean-targets": ["target"],
    }

    with open(f"{DBT_PROJECT_DIR}/dbt_project.yml", "w") as f:
        yaml.dump(project_config, f, default_flow_style=False)

    # profiles.yml - DuckDB with Glue Iceberg catalog
    profiles_config = {
        "data_lake": {
            "target": "prod",
            "outputs": {
                "prod": {
                    "type": "duckdb",
                    "path": ":memory:",
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
                    "plugins": [
                        {
                            "module": "glue_iceberg_plugin",
                            "alias": "glue_iceberg",
                            "config": {
                                "catalog_name": GLUE_CATALOG_NAME,
                                "aws_region": AWS_REGION,
                                "aws_account_id": AWS_ACCOUNT_ID,
                            },
                        }
                    ],
                }
            }
        }
    }

    with open(f"{DBT_PROJECT_DIR}/profiles.yml", "w") as f:
        yaml.dump(profiles_config, f, default_flow_style=False)

    # Model SQL file
    model_sql = f"""-- Generated model for gold.{job_name}
-- Source: silver layer tables via S3

{{{{ config(materialized='table') }}}}

{query}
"""

    with open(f"{DBT_PROJECT_DIR}/models/{job_name}.sql", "w") as f:
        f.write(model_sql)

    logger.info(f"Generated dbt project at {DBT_PROJECT_DIR}")
    logger.info(f"Model: {job_name}.sql")


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
        # Generate dbt project
        generate_dbt_project(
            job_name=JOB_NAME or "health_check",
            query=QUERY,
            silver_bucket=SILVER_BUCKET,
            gold_bucket=GOLD_BUCKET,
        )

        # Run dbt
        output = run_dbt()

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
