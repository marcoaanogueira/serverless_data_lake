"""
dbt Runner - ECS Fargate Entrypoint

Supports two run modes:
  - single: Run a single job (backward-compatible, triggered via API)
  - scheduled: Fetch ALL gold jobs, generate multi-model dbt project, run by tag

Two-phase pipeline:
  1. dbt transforms silver -> gold in-memory DuckDB (reads from Glue via ATTACH, exports to Parquet)
  2. PyIceberg reads the Parquet and writes to S3 as Iceberg tables via Glue Catalog

Environment variables:
    RUN_MODE: "single" (default) or "scheduled"
    TAG_FILTER: For scheduled mode — "hourly", "daily", or "monthly"
    JOB_DOMAIN: Transform job domain (single mode)
    JOB_NAME: Transform job name (single mode)
    QUERY: SQL query to execute (single mode)
    WRITE_MODE: overwrite or append (single mode)
    UNIQUE_KEY: Column for upsert dedup (single mode)
    SCHEMA_BUCKET: S3 bucket for configs
    SILVER_BUCKET: S3 bucket with source silver tables
    GOLD_BUCKET: S3 bucket for output gold tables
    AWS_REGION: AWS region
    AWS_ACCOUNT_ID: AWS account ID for Glue catalog
    GLUE_CATALOG_NAME: Catalog alias in DuckDB (default: tadpole)
"""

import os
import sys
import re
import json
import subprocess
import shutil
import logging
import boto3
import yaml
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Configuration from environment
RUN_MODE = os.environ.get("RUN_MODE", "single")
TAG_FILTER = os.environ.get("TAG_FILTER", "daily")
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
OUTPUT_DIR = f"{DBT_PROJECT_DIR}/outputs"
OUTPUT_PARQUET = f"{DBT_PROJECT_DIR}/output.parquet"  # single-mode compat

# Tag computation
SCHEDULE_TO_TAG = {"hour": "hourly", "day": "daily", "month": "monthly"}
FREQUENCY_ORDER = {"hourly": 0, "daily": 1, "monthly": 2}


# =============================================================================
# Query Processing
# =============================================================================

def rewrite_query(sql: str, catalog_name: str = "") -> str:
    """Rewrite user-friendly table references to DuckDB/Glue format.

    Transforms: domain.layer.table  ->  catalog.domain_layer.table
    Example:    sales.silver.teste  ->  tadpole.sales_silver.teste
    """
    catalog = catalog_name or GLUE_CATALOG_NAME
    return re.sub(
        r'\b(\w+)\.(silver|gold)\.(\w+)\b',
        rf'{catalog}.\1_\2.\3',
        sql,
    )


def process_query_for_dbt(job: dict, all_jobs: list, catalog_name: str = "") -> str:
    """Process a job's query for dbt model generation.

    Cron jobs: rewrite domain.layer.table references.
    Dependency jobs: also substitute gold job refs with {{ ref('job_name') }}.
    """
    query = job.get("query", "SELECT 1")

    if job.get("schedule_type") == "dependency":
        # Build set of known gold job names
        gold_job_names = {j["job_name"] for j in all_jobs}
        # Replace domain.gold.job_name with {{ ref('job_name') }} for known jobs
        for name in gold_job_names:
            pattern = rf'\b(\w+)\.gold\.{re.escape(name)}\b'
            query = re.sub(pattern, "{{ ref('" + name + "') }}", query)

    # Rewrite remaining silver (and non-ref gold) references
    query = rewrite_query(query, catalog_name)
    return query


# =============================================================================
# Tag Computation
# =============================================================================

def compute_effective_tags(all_jobs: list) -> dict:
    """Compute effective dbt tag for every job.

    Cron jobs: tag = mapped from cron_schedule (hour->hourly, day->daily, month->monthly).
    Dependency jobs: tag = highest frequency among all downstream consumers.
    If a dependency has no consumers, defaults to "daily".

    Returns dict of job_name -> tag.
    """
    tags = {}
    jobs_by_name = {j["job_name"]: j for j in all_jobs}

    # Build reverse map: job_name -> list of consumer job_names (who depends on this job)
    consumers = {j["job_name"]: [] for j in all_jobs}
    for job in all_jobs:
        for dep in job.get("dependencies") or []:
            if dep in consumers:
                consumers[dep].append(job["job_name"])

    # First pass: assign tags to cron jobs
    for job in all_jobs:
        if job.get("schedule_type") != "dependency":
            cron_val = job.get("cron_schedule", "day")
            tags[job["job_name"]] = SCHEDULE_TO_TAG.get(cron_val, "daily")

    # Iterative resolution for dependency jobs
    changed = True
    max_iterations = len(all_jobs) + 1
    iteration = 0
    while changed and iteration < max_iterations:
        changed = False
        iteration += 1
        for job in all_jobs:
            if job.get("schedule_type") != "dependency":
                continue
            job_name = job["job_name"]
            consumer_tags = [
                tags[c] for c in consumers.get(job_name, []) if c in tags
            ]
            if not consumer_tags:
                if job_name not in tags:
                    tags[job_name] = "daily"
                    changed = True
                continue
            best = min(consumer_tags, key=lambda t: FREQUENCY_ORDER.get(t, 99))
            if tags.get(job_name) != best:
                tags[job_name] = best
                changed = True

    return tags


# =============================================================================
# Fetch Job Configs from S3
# =============================================================================

def fetch_all_job_configs(schema_bucket: str) -> list:
    """Fetch all gold job configs from S3 schema registry.

    Scans schemas/*/gold/*/config.yaml and returns list of job config dicts.
    """
    s3 = boto3.client("s3")
    jobs = []

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=schema_bucket, Prefix="schemas/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Match schemas/{domain}/gold/{job_name}/config.yaml
            match = re.match(r'^schemas/([^/]+)/gold/([^/]+)/config\.yaml$', key)
            if not match:
                continue
            domain, job_name = match.groups()
            try:
                resp = s3.get_object(Bucket=schema_bucket, Key=key)
                config = yaml.safe_load(resp["Body"].read())
                config.setdefault("domain", domain)
                config.setdefault("job_name", job_name)
                jobs.append(config)
            except Exception as e:
                logger.warning(f"Failed to load {key}: {e}")

    logger.info(f"Fetched {len(jobs)} gold job configs from s3://{schema_bucket}")
    return jobs


# =============================================================================
# dbt Project Generation
# =============================================================================

def _write_project_skeleton():
    """Create shared dbt project files (project.yml, profiles.yml, macros)."""
    # Clean up any previous run
    if os.path.exists(DBT_PROJECT_DIR):
        shutil.rmtree(DBT_PROJECT_DIR)

    os.makedirs(f"{DBT_PROJECT_DIR}/models", exist_ok=True)
    os.makedirs(f"{DBT_PROJECT_DIR}/macros", exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

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
        "on-run-start": [
            "{{ attach_glue_catalog() }}",
        ],
    }

    with open(f"{DBT_PROJECT_DIR}/dbt_project.yml", "w") as f:
        yaml.dump(project_config, f, default_flow_style=False)

    # Macro: attach Glue Iceberg catalog
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

    # profiles.yml — in-memory DuckDB (single connection keeps ATTACH alive)
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
                }
            }
        }
    }

    with open(f"{DBT_PROJECT_DIR}/profiles.yml", "w") as f:
        yaml.dump(profiles_config, f, default_flow_style=False)


def generate_dbt_project(job_name: str, query: str, silver_bucket: str, gold_bucket: str, write_mode: str = "overwrite", unique_key: str = "", domain: str = ""):
    """Generate a single-model dbt project (single mode, backward-compatible)."""
    _write_project_skeleton()

    # Rewrite user-friendly refs
    query = rewrite_query(query)

    parquet_path = OUTPUT_PARQUET
    model_sql = f"""-- Generated model for gold.{job_name}
-- Source: silver layer tables via Glue catalog

{{{{ config(
    materialized='table',
    post_hook="COPY (SELECT * FROM {{{{ this }}}}) TO '{parquet_path}' (FORMAT PARQUET)"
) }}}}

{query}
"""

    with open(f"{DBT_PROJECT_DIR}/models/{job_name}.sql", "w") as f:
        f.write(model_sql)

    logger.info(f"Generated dbt project at {DBT_PROJECT_DIR}")
    logger.info(f"Model: {job_name}.sql")
    logger.info(f"AWS_ACCOUNT_ID={'[SET]' if AWS_ACCOUNT_ID else '[EMPTY]'}, GLUE_CATALOG_NAME={GLUE_CATALOG_NAME}, AWS_REGION={AWS_REGION}")


def generate_multi_model_project(jobs: list, tags: dict):
    """Generate a multi-model dbt project with tags for scheduled runs.

    Each job becomes a model file with its effective tag.
    Dependency jobs get ref() substitutions for gold table references.
    """
    _write_project_skeleton()

    for job in jobs:
        job_name = job["job_name"]
        tag = tags.get(job_name, "daily")
        parquet_path = f"{OUTPUT_DIR}/{job_name}.parquet"

        # Process query: ref() for deps, rewrite for silver
        processed_query = process_query_for_dbt(job, jobs)

        model_sql = f"""-- Generated model for gold.{job_name}
-- Tag: {tag} | Schedule type: {job.get('schedule_type', 'cron')}

{{{{ config(
    materialized='table',
    tags=['{tag}'],
    post_hook="COPY (SELECT * FROM {{{{ this }}}}) TO '{parquet_path}' (FORMAT PARQUET)"
) }}}}

{processed_query}
"""

        with open(f"{DBT_PROJECT_DIR}/models/{job_name}.sql", "w") as f:
            f.write(model_sql)

    logger.info(f"Generated multi-model dbt project: {len(jobs)} models")
    logger.info(f"Tags: {tags}")


# =============================================================================
# dbt Execution
# =============================================================================

def run_dbt(select: str = None):
    """Execute dbt run, optionally with --select."""
    cmd = ["dbt", "run", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR]
    if select:
        cmd.extend(["--select", select])

    logger.info(f"Running: {' '.join(cmd)}")

    env = os.environ.copy()
    env["PYTHONPATH"] = "/app:" + env.get("PYTHONPATH", "")

    result = subprocess.run(
        cmd,
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


# =============================================================================
# Iceberg Write Layer
# =============================================================================

def write_to_iceberg(job_name: str, domain: str, write_mode: str, unique_key: str, gold_bucket: str, parquet_path: str = ""):
    """Read dbt results from Parquet and write to S3 Iceberg via PyIceberg + Glue."""
    import pyarrow.parquet as pq

    parquet_path = parquet_path or OUTPUT_PARQUET

    logger.info(f"Writing to Iceberg: {domain}_gold.{job_name} (mode={write_mode}, key={unique_key or 'none'})")

    if not os.path.exists(parquet_path):
        raise RuntimeError(f"Output parquet not found at {parquet_path} -- dbt post-hook may have failed")

    arrow_table = pq.read_table(parquet_path)
    row_count = arrow_table.num_rows
    logger.info(f"Read {row_count} rows from {parquet_path}")

    if row_count == 0:
        logger.warning("No rows to write -- skipping Iceberg write")
        return

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

    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        logger.info(f"Creating namespace: {namespace}")
        catalog.create_namespace(namespace, {"location": f"s3://{gold_bucket}/{namespace}/"})

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

    if write_mode == "overwrite":
        iceberg_table.overwrite(arrow_table)
        logger.info(f"Overwrite complete: {row_count} rows -> {table_id}")
    elif write_mode == "append" and unique_key:
        logger.info(f"Upserting by key '{unique_key}' into {table_id}")
        iceberg_table.overwrite(arrow_table)
        logger.info(f"Upsert (overwrite) complete: {row_count} rows -> {table_id}")
    else:
        iceberg_table.append(arrow_table)
        logger.info(f"Append complete: {row_count} rows -> {table_id}")


def write_all_to_iceberg(jobs: list, gold_bucket: str):
    """Write all dbt model outputs to Iceberg tables."""
    for job in jobs:
        job_name = job["job_name"]
        parquet_path = f"{OUTPUT_DIR}/{job_name}.parquet"
        if not os.path.exists(parquet_path):
            logger.info(f"No output for {job_name} -- not selected by tag filter")
            continue
        write_to_iceberg(
            job_name=job_name,
            domain=job.get("domain", ""),
            write_mode=job.get("write_mode", "overwrite"),
            unique_key=job.get("unique_key", ""),
            gold_bucket=gold_bucket,
            parquet_path=parquet_path,
        )


# =============================================================================
# Status Tracking
# =============================================================================

def update_execution_status(schema_bucket: str, domain: str, job_name: str, status: str, output: str = ""):
    """Write execution status to S3 for tracking."""
    if not schema_bucket:
        return

    s3 = boto3.client("s3")
    from datetime import datetime, timezone

    status_data = {
        "domain": domain,
        "job_name": job_name,
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "output": output[:5000],
    }

    key = f"schemas/{domain}/gold/{job_name}/last_execution.yaml"
    s3.put_object(
        Bucket=schema_bucket,
        Key=key,
        Body=yaml.dump(status_data, default_flow_style=False).encode("utf-8"),
        ContentType="application/x-yaml",
    )
    logger.info(f"Updated execution status: {status}")


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    run_mode = os.environ.get("RUN_MODE", "single")

    if run_mode == "scheduled":
        run_scheduled()
    else:
        run_single()


def run_single():
    """Run a single job (backward-compatible, triggered via API)."""
    logger.info(f"Starting dbt runner for {JOB_DOMAIN}/{JOB_NAME}")
    logger.info(f"Query: {QUERY}")

    try:
        job_name = JOB_NAME or "health_check"

        generate_dbt_project(
            job_name=job_name,
            query=QUERY,
            silver_bucket=SILVER_BUCKET,
            gold_bucket=GOLD_BUCKET,
            domain=JOB_DOMAIN,
        )

        output = run_dbt()

        write_to_iceberg(
            job_name=job_name,
            domain=JOB_DOMAIN,
            write_mode=WRITE_MODE,
            unique_key=UNIQUE_KEY,
            gold_bucket=GOLD_BUCKET,
        )

        update_execution_status(SCHEMA_BUCKET, JOB_DOMAIN, JOB_NAME, "SUCCESS", output)

        logger.info("dbt runner completed successfully")
        print(json.dumps({"status": "SUCCESS", "job_name": JOB_NAME, "domain": JOB_DOMAIN}))

    except Exception as e:
        logger.error(f"dbt runner failed: {e}")
        update_execution_status(SCHEMA_BUCKET, JOB_DOMAIN, JOB_NAME, "FAILED", str(e))
        print(json.dumps({"status": "FAILED", "error": str(e), "job_name": JOB_NAME, "domain": JOB_DOMAIN}))
        sys.exit(1)


def run_scheduled():
    """Run all jobs matching a tag filter (triggered via EventBridge)."""
    tag_filter = os.environ.get("TAG_FILTER", "daily")
    logger.info(f"Scheduled run: tag={tag_filter}")

    try:
        # Fetch all job configs from S3
        jobs = fetch_all_job_configs(SCHEMA_BUCKET)
        if not jobs:
            logger.warning("No gold jobs found -- nothing to run")
            print(json.dumps({"status": "SUCCESS", "run_mode": "scheduled", "tag": tag_filter, "jobs_run": 0}))
            return

        # Compute effective tags
        tags = compute_effective_tags(jobs)
        logger.info(f"Computed tags: {tags}")

        # Generate multi-model dbt project
        generate_multi_model_project(jobs, tags)

        # Run dbt with tag selection
        output = run_dbt(select=f"tag:{tag_filter}")

        # Write outputs to Iceberg
        write_all_to_iceberg(jobs, GOLD_BUCKET)

        # Update status for each executed job
        for job in jobs:
            if tags.get(job["job_name"]) == tag_filter:
                update_execution_status(SCHEMA_BUCKET, job["domain"], job["job_name"], "SUCCESS", output)

        jobs_run = sum(1 for j in jobs if tags.get(j["job_name"]) == tag_filter)
        logger.info(f"Scheduled run completed: {jobs_run} jobs with tag={tag_filter}")
        print(json.dumps({"status": "SUCCESS", "run_mode": "scheduled", "tag": tag_filter, "jobs_run": jobs_run}))

    except Exception as e:
        logger.error(f"Scheduled run failed: {e}")
        print(json.dumps({"status": "FAILED", "run_mode": "scheduled", "tag": tag_filter, "error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
