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

SCHEMA_BUCKET = os.environ.get("SCHEMA_BUCKET", "")
JOBS_PREFIX = "jobs/transformation"


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

    try:
        plan = await gen_plan(
            domain=request.domain,
            tables=request.tables,
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
        "status": "running",
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
        plan_dict = await gen_plan(
            domain=req["domain"],
            tables=req["tables"],
            api_url=api_url,
        )

        plan = TransformationPlan.model_validate(plan_dict)
        result = await run_transform(
            plan=plan,
            api_url=api_url,
            trigger_execution=req.get("trigger_execution", False),
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
