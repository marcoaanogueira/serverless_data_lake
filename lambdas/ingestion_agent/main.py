"""
Ingestion Agent Lambda — FastAPI service that generates and executes
ingestion plans from OpenAPI specs using AI agents.

Endpoints:
    POST /agent/ingestion/plan          — Generate an IngestionPlan (sync)
    POST /agent/ingestion/run           — Kick off async pipeline, returns job_id
    GET  /agent/ingestion/jobs/{job_id} — Poll job status / result
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
    title="Ingestion Agent API",
    description="AI-powered ingestion plan generation and execution",
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
JOBS_PREFIX = "jobs/ingestion"


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
    """Request to generate an ingestion plan."""

    openapi_url: str = Field(..., description="URL to the OpenAPI/Swagger spec (JSON or YAML)")
    token: str = Field(default="", description="Bearer token for API authentication")
    interests: list[str] = Field(..., description="Subjects of interest (e.g., ['pets', 'store'])")
    docs_url: str | None = Field(
        default=None,
        description="Optional URL to the API docs page (HTML) for extra LLM context",
    )


class RunRequest(BaseModel):
    """Request to generate a plan and run the full ingestion pipeline."""

    openapi_url: str = Field(..., description="URL to the OpenAPI/Swagger spec")
    token: str = Field(default="", description="Bearer token for API authentication")
    interests: list[str] = Field(..., description="Subjects of interest")
    domain: str = Field(..., description="Business domain in the data lake (e.g., 'petstore')")
    docs_url: str | None = Field(default=None, description="Optional API docs URL")
    batch_size: int = Field(default=25, description="Records per batch POST to ingestion")


# =============================================================================
# Endpoints
# =============================================================================


@app.get("/")
def health_check():
    return {"status": "healthy", "service": "ingestion_agent"}


@app.post("/agent/ingestion/plan")
async def generate_plan(request: PlanRequest):
    """Generate an IngestionPlan from an OpenAPI spec URL."""
    from agents.ingestion_agent.agent import run_ingestion_agent

    try:
        plan = await run_ingestion_agent(
            openapi_url=request.openapi_url,
            token=request.token,
            interests=request.interests,
            docs_url=request.docs_url,
        )
        return plan.model_dump()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Failed to generate ingestion plan")
        raise HTTPException(status_code=500, detail=f"Plan generation failed: {exc}")


@app.post("/agent/ingestion/run")
async def run_pipeline(request: RunRequest):
    """Kick off the ingestion pipeline asynchronously. Returns a job_id for polling."""
    api_url = os.environ.get("API_GATEWAY_ENDPOINT", "")
    if not api_url:
        raise HTTPException(status_code=503, detail="API_GATEWAY_ENDPOINT not configured")

    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    _save_job(job_id, {
        "job_id": job_id,
        "status": "running",
        "agent": "ingestion",
        "created_at": now,
        "request": request.model_dump(),
    })

    lambda_client.invoke(
        FunctionName=os.environ.get("AWS_LAMBDA_FUNCTION_NAME", ""),
        InvocationType="Event",
        Payload=json.dumps({
            "_async_job": {
                "job_id": job_id,
                "agent": "ingestion",
                "request": request.model_dump(),
            }
        }),
    )

    return {
        "job_id": job_id,
        "status": "running",
        "poll_url": f"/agent/ingestion/jobs/{job_id}",
    }


@app.get("/agent/ingestion/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Poll the status of an async ingestion job."""
    job = _load_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return job


# =============================================================================
# Async job execution (invoked directly by Lambda, not via API Gateway)
# =============================================================================


async def _execute_ingestion_job(payload: dict):
    """Run the full ingestion pipeline and save results to S3."""
    from agents.ingestion_agent.agent import run_ingestion_agent
    from agents.ingestion_agent.runner import run as run_ingestion

    job_id = payload["job_id"]
    req = payload["request"]
    api_url = os.environ.get("API_GATEWAY_ENDPOINT", "")

    try:
        plan = await run_ingestion_agent(
            openapi_url=req["openapi_url"],
            token=req.get("token", ""),
            interests=req["interests"],
            docs_url=req.get("docs_url"),
        )

        result = await run_ingestion(
            plan=plan,
            domain=req["domain"],
            api_url=api_url,
            token=req.get("token", ""),
            batch_size=req.get("batch_size", 25),
        )

        _save_job(job_id, {
            "job_id": job_id,
            "status": "completed",
            "agent": "ingestion",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "plan": plan.model_dump(),
            "result": result.summary(),
        })
    except Exception as exc:
        logger.exception("Async ingestion job %s failed", job_id)
        _save_job(job_id, {
            "job_id": job_id,
            "status": "failed",
            "agent": "ingestion",
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
        })


# =============================================================================
# Lambda handler
# =============================================================================

_mangum = Mangum(app)


def handler(event, context):
    if "_async_job" in event:
        asyncio.run(_execute_ingestion_job(event["_async_job"]))
        return {"statusCode": 200}
    return _mangum(event, context)
