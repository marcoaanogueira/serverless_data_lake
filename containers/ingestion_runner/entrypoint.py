"""
Ingestion Runner — ECS Fargate container that executes dlt ingestion pipelines.

Reads IngestionPlan configs from S3, fetches OAuth2 credentials from Secrets Manager
when needed, and runs the dlt pipeline via the ingestion API.

Environment variables:
    RUN_MODE              "single" | "scheduled"
    PLAN_NAME             Plan name to run (single mode only)
    TAG_FILTER            "hourly" | "daily" | "monthly" (scheduled mode only)
    SCHEMA_BUCKET         S3 bucket containing ingestion plan configs
    TENANT                Tenant name (case-insensitive)
    API_GATEWAY_ENDPOINT  Data lake API base URL
"""

import json
import logging
import os
import sys

import boto3
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("ingestion_runner")

SCHEMA_BUCKET = os.environ["SCHEMA_BUCKET"]
TENANT = os.environ["TENANT"].lower()
API_URL = os.environ["API_GATEWAY_ENDPOINT"]
RUN_MODE = os.environ.get("RUN_MODE", "single")


s3 = boto3.client("s3")
sm = boto3.client("secretsmanager")


# =============================================================================
# S3 helpers
# =============================================================================


def _plan_key(plan_name: str) -> str:
    return f"{TENANT}/ingestion_plans/{plan_name}/config.yaml"


def _load_plan_config(plan_name: str) -> dict:
    key = _plan_key(plan_name)
    logger.info("Loading plan from s3://%s/%s", SCHEMA_BUCKET, key)
    obj = s3.get_object(Bucket=SCHEMA_BUCKET, Key=key)
    return yaml.safe_load(obj["Body"].read())


def _list_plan_configs(tag_filter: str) -> list[dict]:
    prefix = f"{TENANT}/ingestion_plans/"
    paginator = s3.get_paginator("list_objects_v2")
    configs = []
    for page in paginator.paginate(Bucket=SCHEMA_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith("config.yaml"):
                continue
            data = s3.get_object(Bucket=SCHEMA_BUCKET, Key=obj["Key"])
            cfg = yaml.safe_load(data["Body"].read())
            if tag_filter in cfg.get("tags", []):
                configs.append(cfg)
    return configs


# =============================================================================
# Secrets Manager helpers
# =============================================================================


def _fetch_oauth2(secret_name: str):
    from agents.ingestion_agent.models import OAuth2Config

    logger.info("Fetching OAuth2 credentials from Secrets Manager: %s", secret_name)
    response = sm.get_secret_value(SecretId=secret_name)
    data = json.loads(response["SecretString"])
    return OAuth2Config(**data)


# =============================================================================
# Pipeline execution
# =============================================================================


def _run_plan(cfg: dict) -> dict:
    import asyncio

    from agents.ingestion_agent.models import IngestionPlan
    from agents.ingestion_agent.runner import run as run_ingestion

    plan_name = cfg["plan_name"]
    domain = cfg["domain"]
    plan = IngestionPlan.model_validate(cfg["plan"])

    oauth2 = None
    if secret_name := cfg.get("oauth2_secret_name"):
        oauth2 = _fetch_oauth2(secret_name)

    logger.info("[%s] Starting dlt pipeline (domain=%s)", plan_name, domain)
    result = asyncio.run(
        run_ingestion(
            plan=plan,
            domain=domain,
            api_url=API_URL,
            oauth2=oauth2,
        )
    )
    logger.info("[%s] Completed: %s", plan_name, result.summary())
    return {
        "plan_name": plan_name,
        "status": "ok",
        "records": result.records_loaded,
    }


# =============================================================================
# Entry point
# =============================================================================


def main() -> None:
    if RUN_MODE == "single":
        plan_name = os.environ["PLAN_NAME"]
        cfg = _load_plan_config(plan_name)
        result = _run_plan(cfg)
        print(json.dumps(result))

    elif RUN_MODE == "scheduled":
        tag_filter = os.environ.get("TAG_FILTER", "hourly")
        configs = _list_plan_configs(tag_filter)

        if not configs:
            logger.info("No ingestion plans found with tag '%s'", tag_filter)
            return

        logger.info("Found %d plan(s) with tag '%s'", len(configs), tag_filter)
        results = {}
        for cfg in configs:
            plan_name = cfg["plan_name"]
            try:
                results[plan_name] = _run_plan(cfg)
            except Exception as exc:
                logger.exception("[%s] Pipeline failed", plan_name)
                results[plan_name] = {
                    "plan_name": plan_name,
                    "status": "error",
                    "error": str(exc),
                }

        print(json.dumps(results))

    else:
        logger.error("Unknown RUN_MODE: '%s'. Expected 'single' or 'scheduled'.", RUN_MODE)
        sys.exit(1)


if __name__ == "__main__":
    main()
