"""
Ingestion Runner — ECS Fargate container that executes dlt ingestion pipelines.

The agent (Lambda + Bedrock) is responsible for:
  - Generating the IngestionPlan (LLM)
  - Running setup_endpoints: sample fetching + PK/description LLM agents
    + creating endpoints in the schema registry
  - Saving the final plan to S3

This container is responsible only for the pure dlt pipeline execution:
  - Read IngestionPlan from S3
  - Fetch OAuth2 token from Secrets Manager if needed
  - Call run_pipeline() — extract from source API → POST to /ingest

No LLM dependencies required here.

Environment variables:
    RUN_MODE              "single" | "scheduled"
    PLAN_NAME             Plan name to run (single mode only)
    TAG_FILTER            "hourly" | "daily" | "monthly" (scheduled mode only)
    SCHEMA_BUCKET         S3 bucket containing ingestion plan configs
    TENANT                Tenant name (case-insensitive)
    API_GATEWAY_ENDPOINT  Data lake API base URL
"""

import asyncio
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
API_KEY_SECRET_ARN = os.environ.get("API_KEY_SECRET_ARN", "")
RUN_MODE = os.environ.get("RUN_MODE", "single")

s3 = boto3.client("s3")
sm = boto3.client("secretsmanager")

_cached_api_key: str | None = None


def _get_api_key() -> str:
    """Fetch the internal x-api-key from Secrets Manager (cached)."""
    global _cached_api_key
    if _cached_api_key is not None:
        return _cached_api_key
    if not API_KEY_SECRET_ARN:
        logger.warning("API_KEY_SECRET_ARN not set — ingestion API calls will not be authenticated")
        _cached_api_key = ""
        return ""
    resp = sm.get_secret_value(SecretId=API_KEY_SECRET_ARN)
    _cached_api_key = resp["SecretString"]
    return _cached_api_key


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
# Secrets Manager helper
# =============================================================================


def _load_oauth2(secret_name: str) -> dict:
    logger.info("Fetching OAuth2 credentials from Secrets Manager: %s", secret_name)
    response = sm.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


# =============================================================================
# Pipeline execution — dlt only, no LLM
# =============================================================================


async def _run_plan_async(cfg: dict) -> dict:
    from agents.ingestion_agent.models import IngestionPlan, OAuth2Config
    from agents.ingestion_agent.runner import fetch_oauth2_token, run_pipeline

    plan_name = cfg["plan_name"]
    domain = cfg["domain"]
    plan = IngestionPlan.model_validate(cfg["plan"])

    # Resolve bearer token from Secrets Manager if OAuth2 is configured
    token = ""
    if secret_name := cfg.get("oauth2_secret_name"):
        oauth2_data = _load_oauth2(secret_name)
        oauth2 = OAuth2Config(**oauth2_data)
        token = await fetch_oauth2_token(oauth2)

    api_key = _get_api_key()
    logger.info(
        "[%s] Starting dlt pipeline (domain=%s, endpoints=%s, base_url=%s)",
        plan_name,
        domain,
        [ep.resource_name for ep in plan.get_only().endpoints],
        plan.base_url,
    )
    loaded = run_pipeline(plan, domain, API_URL, token, api_key=api_key)

    total = sum(loaded.values()) if loaded else 0
    if total == 0:
        logger.warning(
            "[%s] Pipeline completed with 0 records loaded. "
            "Possible causes: API returned empty data, wrong data_path, "
            "authentication failure, or pagination issue. "
            "Check the [dlt] resource=... log lines above for the exact URLs being fetched.",
            plan_name,
        )
    else:
        logger.info("[%s] Completed: %d total records across tables: %s", plan_name, total, loaded)

    return {"plan_name": plan_name, "status": "ok", "records": loaded}


def _run_plan(cfg: dict) -> dict:
    return asyncio.run(_run_plan_async(cfg))


# =============================================================================
# Entry point
# =============================================================================


def main() -> None:
    # ------------------------------------------------------------------
    # Startup banner — first thing logged so it's always visible in
    # CloudWatch even if the pipeline crashes immediately after.
    # ------------------------------------------------------------------
    logger.info(
        "=== ingestion-runner start | RUN_MODE=%s | TENANT=%s | API_URL=%s | "
        "SCHEMA_BUCKET=%s | API_KEY_SECRET_ARN=%s ===",
        RUN_MODE,
        TENANT,
        API_URL,
        SCHEMA_BUCKET,
        "set" if API_KEY_SECRET_ARN else "NOT SET",
    )

    if RUN_MODE == "single":
        plan_name = os.environ.get("PLAN_NAME", "")
        if not plan_name:
            logger.error("PLAN_NAME env var is required in single mode but was not set.")
            sys.exit(1)

        logger.info("[%s] Loading plan config from S3...", plan_name)
        try:
            cfg = _load_plan_config(plan_name)
        except Exception:
            logger.exception("[%s] Failed to load plan config from S3.", plan_name)
            sys.exit(1)

        logger.info("[%s] Plan loaded. Starting pipeline...", plan_name)
        try:
            result = _run_plan(cfg)
        except Exception:
            logger.exception(
                "[%s] Pipeline failed with unhandled exception. "
                "Check the traceback above — common causes: "
                "OAuth2 token error, source API unreachable, wrong data_path, "
                "ingestion API rejected batch (check x-api-key and endpoint schema).",
                plan_name,
            )
            sys.exit(1)

        print(json.dumps(result))
        logger.info(
            "[%s] Done. status=%s records=%s",
            plan_name, result.get("status"), result.get("records"),
        )

    elif RUN_MODE == "scheduled":
        tag_filter = os.environ.get("TAG_FILTER", "hourly")
        logger.info("Listing plans with tag '%s'...", tag_filter)
        configs = _list_plan_configs(tag_filter)

        if not configs:
            logger.info("No ingestion plans found with tag '%s'. Nothing to do.", tag_filter)
            return

        logger.info("Found %d plan(s) with tag '%s': %s", len(configs), tag_filter,
                    [c.get("plan_name") for c in configs])
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

        failed = [name for name, r in results.items() if r.get("status") == "error"]
        ok = [name for name, r in results.items() if r.get("status") == "ok"]
        logger.info("Scheduled run complete. OK: %s | Failed: %s", ok, failed)
        print(json.dumps(results))

        if failed:
            # Exit 1 so Step Functions marks this execution as failed
            sys.exit(1)

    else:
        logger.error("Unknown RUN_MODE: '%s'. Expected 'single' or 'scheduled'.", RUN_MODE)
        sys.exit(1)


if __name__ == "__main__":
    main()
