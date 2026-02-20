"""
Transformation Agent Lambda — FastAPI service that generates and submits
gold-layer transformation plans using AI agents.

Endpoints:
    POST /agent/transformation/plan          — Generate a TransformationPlan (sync)
    POST /agent/transformation/run           — Kick off async pipeline, returns job_id
    GET  /agent/transformation/jobs/{job_id} — Poll job status / result
"""

import asyncio
import json
import logging
import os
import time as _time
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Transformation Agent API",
    description="AI-powered transformation plan generation and execution",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# AWS clients for async job management
s3_client = boto3.client("s3")
lambda_client = boto3.client("lambda")
sm_client = boto3.client("secretsmanager")

SCHEMA_BUCKET = os.environ.get("SCHEMA_BUCKET", "")
JOBS_PREFIX = "jobs/transformation"
SCHEMAS_PREFIX = "schemas"


# =============================================================================
# Bronze schema validation
# =============================================================================


def _check_bronze_schemas(domain: str, tables: list[str]) -> list[str]:
    """
    Verify that bronze schemas exist in the artifacts bucket for the given tables.

    Returns the subset of tables that have a registered bronze schema.
    Tables without schemas are skipped with a warning — they may indicate
    that ingestion failed or has not yet completed for those endpoints.

    Raises RuntimeError if SCHEMA_BUCKET is not configured.
    """
    if not SCHEMA_BUCKET:
        raise RuntimeError("SCHEMA_BUCKET environment variable is not set")

    available: list[str] = []
    for table_name in tables:
        key = f"{SCHEMAS_PREFIX}/{domain}/bronze/{table_name}/latest.yaml"
        try:
            s3_client.head_object(Bucket=SCHEMA_BUCKET, Key=key)
            available.append(table_name)
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("NoSuchKey", "404"):
                logger.warning(
                    "No bronze schema found for %s/%s (key: %s) — "
                    "ingestion may not have completed for this table.",
                    domain,
                    table_name,
                    key,
                )
            else:
                raise

    return available


async def _wait_for_silver_tables(
    domain: str,
    tables: list[str],
    max_wait_seconds: int = 420,
    poll_interval_seconds: int = 15,
) -> list[str]:
    """
    Poll S3 until silver schemas are registered for all expected tables, or timeout.

    Silver schemas are written by the processing_iceberg Lambda after it converts
    bronze S3 files to Iceberg format. Blocks until every table in ``tables`` has
    a ``schemas/{domain}/silver/{name}/latest.yaml`` entry in SCHEMA_BUCKET, so
    the transformation agent only runs when there is actual silver data to sample.

    Returns the subset of tables that are ready (may be fewer than requested if
    timeout is reached with only partial results).
    """
    if not SCHEMA_BUCKET:
        raise RuntimeError("SCHEMA_BUCKET environment variable is not set")

    start = _time.monotonic()
    pending = list(tables)
    ready: list[str] = []
    interval = poll_interval_seconds

    logger.info(
        "Waiting for silver schemas: domain=%s tables=%s (max_wait=%ds)",
        domain, tables, max_wait_seconds,
    )

    while pending:
        still_pending: list[str] = []
        for table_name in pending:
            key = f"{SCHEMAS_PREFIX}/{domain}/silver/{table_name}/latest.yaml"
            try:
                s3_client.head_object(Bucket=SCHEMA_BUCKET, Key=key)
                ready.append(table_name)
                logger.info(
                    "[%s/%s] Silver schema registered — ready for transformation.",
                    domain, table_name,
                )
            except ClientError as exc:
                code = exc.response["Error"]["Code"]
                if code in ("NoSuchKey", "404"):
                    still_pending.append(table_name)
                else:
                    raise

        pending = still_pending
        if not pending:
            elapsed = _time.monotonic() - start
            logger.info(
                "All %d silver schema(s) ready after %.0fs.",
                len(ready), elapsed,
            )
            return ready

        elapsed = _time.monotonic() - start
        if elapsed + interval >= max_wait_seconds:
            logger.warning(
                "Timed out waiting for silver schemas after %.0fs. "
                "Still pending: %s. Proceeding with ready tables: %s.",
                elapsed, pending, ready,
            )
            return ready

        logger.info(
            "Silver schemas not yet available for %s — "
            "retrying in %ds (elapsed: %.0fs)...",
            pending, interval, elapsed,
        )
        await asyncio.sleep(interval)
        interval = min(interval * 2, 60)  # cap at 60s between polls

    return ready


# ---------------------------------------------------------------------------
# Internal API key — cached in memory to avoid repeated Secrets Manager calls
# ---------------------------------------------------------------------------

_cached_internal_api_key: str | None = None


def _get_internal_api_key() -> str:
    """Return the x-api-key for calling our own API Gateway endpoints.

    Reads API_KEY_SECRET_ARN from the environment, fetches the secret value
    on first call, then caches it for the lifetime of the Lambda container.
    Returns an empty string if the env var is not set (local dev / tests).
    """
    global _cached_internal_api_key
    if _cached_internal_api_key is not None:
        return _cached_internal_api_key
    secret_arn = os.environ.get("API_KEY_SECRET_ARN", "")
    if not secret_arn:
        logger.warning("API_KEY_SECRET_ARN not set — internal API calls will not be authenticated")
        _cached_internal_api_key = ""
        return ""
    resp = sm_client.get_secret_value(SecretId=secret_arn)
    _cached_internal_api_key = resp["SecretString"]
    return _cached_internal_api_key


# =============================================================================
# Job persistence helpers (S3)
# =============================================================================


def _save_job(job_id: str, data: dict):
    s3_client.put_object(
        Bucket=SCHEMA_BUCKET,
        Key=f"{JOBS_PREFIX}/{job_id}.json",
        Body=json.dumps(data, default=str),
        ContentType="application/json",
    )


def _load_job(job_id: str) -> dict | None:
    try:
        resp = s3_client.get_object(
            Bucket=SCHEMA_BUCKET,
            Key=f"{JOBS_PREFIX}/{job_id}.json",
        )
        return json.loads(resp["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise


# =============================================================================
# Request / Response Models
# =============================================================================


class PlanRequest(BaseModel):
    """Request to generate a transformation plan."""

    domain: str = Field(..., description="Business domain (e.g., 'starwars', 'sales')")
    tables: list[str] = Field(
        ..., description="Ingested table names (e.g., ['people', 'planets', 'films'])"
    )


class RunRequest(BaseModel):
    """Request to generate a plan and submit jobs."""

    domain: str = Field(..., description="Business domain")
    tables: list[str] = Field(..., description="Ingested table names")
    trigger_execution: bool = Field(
        default=False,
        description="Whether to trigger job execution after creation",
    )


# =============================================================================
# Endpoints
# =============================================================================


@app.get("/")
def health_check():
    return {"status": "healthy", "service": "transformation_agent"}


@app.post("/agent/transformation/plan")
async def generate_plan(request: PlanRequest):
    """Generate a TransformationPlan from table metadata."""
    from agents.transformation_agent.main import generate_plan as gen_plan

    api_url = os.environ.get("API_GATEWAY_ENDPOINT", "")
    if not api_url:
        raise HTTPException(
            status_code=503,
            detail="API_GATEWAY_ENDPOINT not configured",
        )

    # Require at least one bronze schema to exist before generating a plan.
    # Missing schemas indicate ingestion has not completed (or failed) for those tables.
    available_tables = _check_bronze_schemas(request.domain, request.tables)
    if not available_tables:
        raise HTTPException(
            status_code=422,
            detail=(
                f"No bronze schemas found in the artifacts bucket for domain "
                f"'{request.domain}' and tables {request.tables}. "
                "Ingestion must complete successfully before running the transformation agent."
            ),
        )
    if len(available_tables) < len(request.tables):
        missing = sorted(set(request.tables) - set(available_tables))
        logger.warning(
            "Skipping %d table(s) without bronze schemas: %s",
            len(missing),
            missing,
        )

    try:
        plan = await gen_plan(
            domain=request.domain,
            tables=available_tables,
            api_url=api_url,
        )
        return plan
    except Exception as exc:
        logger.exception("Failed to generate transformation plan")
        raise HTTPException(
            status_code=500,
            detail=f"Plan generation failed: {exc}",
        )


@app.post("/agent/transformation/run")
async def run_pipeline(request: RunRequest):
    """Kick off the transformation pipeline asynchronously. Returns a job_id for polling."""
    api_url = os.environ.get("API_GATEWAY_ENDPOINT", "")
    if not api_url:
        raise HTTPException(status_code=503, detail="API_GATEWAY_ENDPOINT not configured")

    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    _save_job(job_id, {
        "job_id": job_id,
        "status": "waiting_for_silver",
        "agent": "transformation",
        "created_at": now,
        "request": request.model_dump(),
    })

    lambda_client.invoke(
        FunctionName=os.environ.get("AWS_LAMBDA_FUNCTION_NAME", ""),
        InvocationType="Event",
        Payload=json.dumps({
            "_async_job": {
                "job_id": job_id,
                "agent": "transformation",
                "request": request.model_dump(),
            }
        }),
    )

    return {
        "job_id": job_id,
        "status": "running",
        "poll_url": f"/agent/transformation/jobs/{job_id}",
    }


@app.get("/agent/transformation/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Poll the status of an async transformation job."""
    job = _load_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return job


# =============================================================================
# Async job execution (invoked directly by Lambda, not via API Gateway)
# =============================================================================


async def _execute_transformation_job(payload: dict):
    """Run the full transformation pipeline and save results to S3."""
    from agents.transformation_agent.main import generate_plan as gen_plan
    from agents.transformation_agent.models import TransformationPlan
    from agents.transformation_agent.runner import run as run_transform

    job_id = payload["job_id"]
    req = payload["request"]
    api_url = os.environ.get("API_GATEWAY_ENDPOINT", "")

    try:
        # Wait for silver schemas to be registered before generating a plan.
        # Silver schemas are written by the processing_iceberg Lambda after it
        # converts bronze S3 files to Iceberg format.  Polling here ensures the
        # transformation agent only runs when there is actual silver data to sample,
        # even if it was invoked immediately after (or during) ingestion.
        available_tables = await _wait_for_silver_tables(req["domain"], req["tables"])
        if not available_tables:
            raise RuntimeError(
                f"Timed out waiting for silver schemas for domain '{req['domain']}' "
                f"and tables {req['tables']}. "
                "Ensure ingestion completed and the processing Lambda converted "
                "bronze data to silver before retrying."
            )
        if len(available_tables) < len(req["tables"]):
            missing = sorted(set(req["tables"]) - set(available_tables))
            logger.warning(
                "Tables without silver schemas after timeout (skipping): %s",
                missing,
            )

        _save_job(job_id, {
            "job_id": job_id,
            "status": "running",
            "agent": "transformation",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "request": req,
            "available_tables": available_tables,
        })

        plan_dict = await gen_plan(
            domain=req["domain"],
            tables=available_tables,
            api_url=api_url,
        )

        plan = TransformationPlan.model_validate(plan_dict)
        result = await run_transform(
            plan=plan,
            api_url=api_url,
            trigger_execution=req.get("trigger_execution", False),
            api_key=_get_internal_api_key(),
        )

        _save_job(job_id, {
            "job_id": job_id,
            "status": "completed",
            "agent": "transformation",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "plan": plan_dict,
            "result": result.summary(),
        })
    except Exception as exc:
        logger.exception("Async transformation job %s failed", job_id)
        _save_job(job_id, {
            "job_id": job_id,
            "status": "failed",
            "agent": "transformation",
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
        })


# =============================================================================
# Lambda handler
# =============================================================================

_mangum = Mangum(app, lifespan="off")


def handler(event, context):
    if "_async_job" in event:
        asyncio.run(_execute_transformation_job(event["_async_job"]))
        return {"statusCode": 200}
    # Python 3.12+ no longer auto-creates an event loop — ensure one exists for Mangum
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    return _mangum(event, context)
