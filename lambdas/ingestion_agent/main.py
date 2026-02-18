"""
Ingestion Agent Lambda — FastAPI service that generates ingestion plans from
OpenAPI specs using AI agents and schedules execution via Step Functions + ECS.

Endpoints:
    POST /agent/ingestion/plan          — Generate an IngestionPlan (sync)
    POST /agent/ingestion/run           — Generate plan + save + trigger SFN (async, returns job_id)
    GET  /agent/ingestion/jobs/{job_id} — Poll job status / result
"""

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
import yaml
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Ingestion Agent API",
    description="AI-powered ingestion plan generation and scheduling",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

s3_client = boto3.client("s3")
lambda_client = boto3.client("lambda")
sfn_client = boto3.client("stepfunctions")
sm_client = boto3.client("secretsmanager")

SCHEMA_BUCKET = os.environ.get("SCHEMA_BUCKET", "")
TENANT = os.environ.get("TENANT", "").lower()
JOBS_PREFIX = "jobs/ingestion"
INGESTION_STATE_MACHINE_ARN = os.environ.get("INGESTION_STATE_MACHINE_ARN", "")


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
# Ingestion plan persistence helpers (S3 + Secrets Manager)
# =============================================================================


def _plan_key(plan_name: str) -> str:
    return f"{TENANT}/ingestion_plans/{plan_name}/config.yaml"


def _save_plan_to_s3(plan_name: str, cfg: dict) -> None:
    s3_client.put_object(
        Bucket=SCHEMA_BUCKET,
        Key=_plan_key(plan_name),
        Body=yaml.dump(cfg, allow_unicode=True),
        ContentType="application/x-yaml",
    )


def _save_oauth2_to_secrets_manager(plan_name: str, oauth2: dict) -> str:
    secret_name = f"/data-lake/ingestion/{plan_name}/oauth2"
    try:
        sm_client.put_secret_value(SecretId=secret_name, SecretString=json.dumps(oauth2))
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            sm_client.create_secret(
                Name=secret_name,
                SecretString=json.dumps(oauth2),
                Description=f"OAuth2 credentials for ingestion plan: {plan_name}",
            )
        else:
            raise
    return secret_name


# =============================================================================
# Request / Response Models
# =============================================================================


class OAuth2Credentials(BaseModel):
    """OAuth2 Resource Owner Password Credentials for APIs that require it."""

    token_url: str = Field(..., description="OAuth2 token endpoint URL")
    client_id: str = Field(..., description="OAuth2 client ID")
    client_secret: str = Field(..., description="OAuth2 client secret")
    username: str = Field(
        ...,
        description="Resource owner username (include $$tenant suffix if required by the API)",
    )
    password: str = Field(..., description="Resource owner password")


class PlanRequest(BaseModel):
    """Request to generate an ingestion plan."""

    openapi_url: str = Field(..., description="URL to the OpenAPI/Swagger spec (JSON or YAML)")
    token: str = Field(default="", description="Bearer token for API authentication")
    oauth2: OAuth2Credentials | None = Field(
        default=None,
        description="OAuth2 ROPC credentials. Use instead of 'token' for OAuth2 APIs.",
    )
    interests: list[str] = Field(..., description="Subjects of interest (e.g., ['pets', 'store'])")
    docs_url: str | None = Field(
        default=None,
        description="Optional URL to the API docs page (HTML) for extra LLM context",
    )


class RunRequest(BaseModel):
    """Request to generate a plan and schedule it for execution via Step Functions."""

    openapi_url: str = Field(..., description="URL to the OpenAPI/Swagger spec")
    token: str = Field(default="", description="Bearer token for API authentication")
    oauth2: OAuth2Credentials | None = Field(
        default=None,
        description="OAuth2 ROPC credentials. Use instead of 'token' for OAuth2 APIs.",
    )
    interests: list[str] = Field(..., description="Subjects of interest")
    domain: str = Field(..., description="Business domain in the data lake (e.g., 'juridico')")
    docs_url: str | None = Field(default=None, description="Optional API docs URL")
    plan_name: str | None = Field(
        default=None,
        description="Plan identifier (snake_case). Auto-generated from api_name + domain if omitted.",
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Schedule tags for recurring runs: hourly, daily, monthly",
    )


# =============================================================================
# Endpoints
# =============================================================================


@app.get("/")
def health_check():
    return {"status": "healthy", "service": "ingestion_agent"}


def _build_oauth2_config(oauth2: OAuth2Credentials | None):
    """Convert OAuth2Credentials request model to OAuth2Config domain model."""
    if not oauth2:
        return None
    from agents.ingestion_agent.models import OAuth2Config
    return OAuth2Config(
        token_url=oauth2.token_url,
        client_id=oauth2.client_id,
        client_secret=oauth2.client_secret,
        username=oauth2.username,
        password=oauth2.password,
    )


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
            oauth2=_build_oauth2_config(request.oauth2),
        )
        return plan.model_dump()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Failed to generate ingestion plan")
        raise HTTPException(status_code=500, detail=f"Plan generation failed: {exc}")


@app.post("/agent/ingestion/run")
async def run_pipeline(request: RunRequest):
    """
    Generate an IngestionPlan, save it to S3, store OAuth2 in Secrets Manager,
    and trigger Step Functions for immediate execution.

    Returns a job_id to poll the plan generation status. The actual dlt pipeline
    runs asynchronously in ECS Fargate via the Step Functions state machine.
    """
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
# Async job execution — generate plan, persist, trigger SFN
# =============================================================================


async def _execute_ingestion_job(payload: dict):
    """
    Generate an IngestionPlan via the AI agent, persist it to S3 (with OAuth2
    credentials in Secrets Manager), and trigger the Step Functions state machine
    to run the dlt pipeline in ECS Fargate.
    """
    from agents.ingestion_agent.agent import run_ingestion_agent

    job_id = payload["job_id"]
    req = payload["request"]

    try:
        oauth2_raw = req.get("oauth2")
        oauth2 = _build_oauth2_config(
            OAuth2Credentials(**oauth2_raw) if oauth2_raw else None
        )

        # Phase 1: Generate IngestionPlan via AI agent
        plan = await run_ingestion_agent(
            openapi_url=req["openapi_url"],
            token=req.get("token", ""),
            interests=req["interests"],
            docs_url=req.get("docs_url"),
            oauth2=oauth2,
        )

        # Phase 2: Persist plan config to S3
        plan_name = req.get("plan_name") or f"{plan.api_name}_{req['domain']}"
        plan_cfg: dict = {
            "plan_name": plan_name,
            "domain": req["domain"],
            "tags": req.get("tags", []),
            "plan": plan.model_dump(),
        }

        # Phase 3: Store OAuth2 credentials in Secrets Manager (never in S3)
        if oauth2_raw:
            secret_name = _save_oauth2_to_secrets_manager(plan_name, oauth2_raw)
            plan_cfg["oauth2_secret_name"] = secret_name

        _save_plan_to_s3(plan_name, plan_cfg)
        logger.info("[%s] Ingestion plan saved to S3", plan_name)

        # Phase 4: Trigger Step Functions for immediate dlt execution
        execution_arn = None
        if INGESTION_STATE_MACHINE_ARN:
            resp = sfn_client.start_execution(
                stateMachineArn=INGESTION_STATE_MACHINE_ARN,
                input=json.dumps({"run_mode": "single", "plan_name": plan_name}),
            )
            execution_arn = resp["executionArn"]
            logger.info("[%s] SFN execution started: %s", plan_name, execution_arn)

        _save_job(job_id, {
            "job_id": job_id,
            "status": "completed",
            "agent": "ingestion",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "plan_name": plan_name,
            "plan": plan.model_dump(),
            "execution_arn": execution_arn,
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

_mangum = Mangum(app, lifespan="off")


def handler(event, context):
    if "_async_job" in event:
        asyncio.run(_execute_ingestion_job(event["_async_job"]))
        return {"statusCode": 200}
    # Python 3.12+ no longer auto-creates an event loop — ensure one exists for Mangum
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    return _mangum(event, context)
