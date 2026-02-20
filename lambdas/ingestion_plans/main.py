"""
Ingestion Plans API — CRUD for ingestion plan configs + Step Functions trigger.

Plans are stored in S3 as YAML:
    s3://{SCHEMA_BUCKET}/{tenant}/ingestion_plans/{plan_name}/config.yaml

OAuth2 credentials are stored in AWS Secrets Manager (never in S3):
    /data-lake/ingestion/{plan_name}/oauth2

Endpoints:
    POST   /ingestion/plans                      Create or update a plan
    GET    /ingestion/plans                       List all plans
    GET    /ingestion/plans/{plan_name}           Get a plan (without secrets)
    DELETE /ingestion/plans/{plan_name}           Delete plan + its secret
    POST   /ingestion/plans/{plan_name}/run       Trigger SFN (single run)
"""

import json
import logging
import os

import boto3
import yaml
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

app = FastAPI(title="Ingestion Plans API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

s3 = boto3.client("s3")
sm = boto3.client("secretsmanager")
sfn = boto3.client("stepfunctions")

SCHEMA_BUCKET = os.environ.get("SCHEMA_BUCKET", "")
TENANT = os.environ.get("TENANT", "").lower()
INGESTION_STATE_MACHINE_ARN = os.environ.get("INGESTION_STATE_MACHINE_ARN", "")


# =============================================================================
# S3 helpers
# =============================================================================


def _plan_key(plan_name: str) -> str:
    return f"{TENANT}/ingestion_plans/{plan_name}/config.yaml"


def _save_plan(plan_name: str, cfg: dict) -> None:
    s3.put_object(
        Bucket=SCHEMA_BUCKET,
        Key=_plan_key(plan_name),
        Body=yaml.dump(cfg, allow_unicode=True),
        ContentType="application/x-yaml",
    )


def _load_plan(plan_name: str) -> dict | None:
    try:
        obj = s3.get_object(Bucket=SCHEMA_BUCKET, Key=_plan_key(plan_name))
        return yaml.safe_load(obj["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise


def _delete_plan(plan_name: str) -> None:
    s3.delete_object(Bucket=SCHEMA_BUCKET, Key=_plan_key(plan_name))


def _list_plans() -> list[dict]:
    prefix = f"{TENANT}/ingestion_plans/"
    paginator = s3.get_paginator("list_objects_v2")
    plans = []
    for page in paginator.paginate(Bucket=SCHEMA_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith("config.yaml"):
                continue
            data = s3.get_object(Bucket=SCHEMA_BUCKET, Key=obj["Key"])
            cfg = yaml.safe_load(data["Body"].read())
            cfg.pop("oauth2_secret_name", None)  # never expose secret references
            plans.append(cfg)
    return plans


# =============================================================================
# Secrets Manager helpers
# =============================================================================


def _secret_name(plan_name: str) -> str:
    return f"/data-lake/ingestion/{plan_name}/oauth2"


def _save_oauth2_secret(plan_name: str, oauth2: dict) -> str:
    name = _secret_name(plan_name)
    try:
        sm.put_secret_value(SecretId=name, SecretString=json.dumps(oauth2))
        logger.info("Updated OAuth2 secret: %s", name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            sm.create_secret(
                Name=name,
                SecretString=json.dumps(oauth2),
                Description=f"OAuth2 credentials for ingestion plan: {plan_name}",
            )
            logger.info("Created OAuth2 secret: %s", name)
        else:
            raise
    return name


def _delete_oauth2_secret(plan_name: str) -> None:
    try:
        sm.delete_secret(
            SecretId=_secret_name(plan_name),
            ForceDeleteWithoutRecovery=True,
        )
        logger.info("Deleted OAuth2 secret for plan: %s", plan_name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise


# =============================================================================
# Request / Response models
# =============================================================================


class OAuth2Credentials(BaseModel):
    """OAuth2 ROPC credentials — stored in Secrets Manager, never in S3."""

    token_url: str
    client_id: str
    client_secret: str
    username: str
    password: str


class CreatePlanRequest(BaseModel):
    plan_name: str = Field(..., description="Unique plan identifier (snake_case)")
    domain: str = Field(..., description="Business domain in the data lake (e.g., juridico)")
    tags: list[str] = Field(
        default_factory=list,
        description="Schedule tags: hourly, daily, monthly",
    )
    plan: dict = Field(
        ...,
        description="IngestionPlan as a dict (output of POST /agent/ingestion/plan)",
    )
    oauth2: OAuth2Credentials | None = Field(
        default=None,
        description="OAuth2 credentials — stored in Secrets Manager if provided",
    )


# =============================================================================
# Endpoints
# =============================================================================


@app.get("/")
def health_check():
    return {"status": "healthy", "service": "ingestion_plans"}


@app.post("/ingestion/plans", status_code=201)
def create_plan(request: CreatePlanRequest):
    """Create or update an ingestion plan. OAuth2 credentials go to Secrets Manager."""
    cfg: dict = {
        "plan_name": request.plan_name,
        "domain": request.domain,
        "tags": request.tags,
        "plan": request.plan,
    }

    if request.oauth2:
        secret_name = _save_oauth2_secret(request.plan_name, request.oauth2.model_dump())
        cfg["oauth2_secret_name"] = secret_name

    _save_plan(request.plan_name, cfg)
    logger.info("Saved ingestion plan: %s", request.plan_name)
    return {"plan_name": request.plan_name, "status": "created"}


@app.get("/ingestion/plans")
def list_plans():
    """List all ingestion plans for this tenant."""
    return {"plans": _list_plans()}


@app.get("/ingestion/plans/{plan_name}")
def get_plan(plan_name: str):
    """Get an ingestion plan by name (OAuth2 secret reference is omitted)."""
    cfg = _load_plan(plan_name)
    if not cfg:
        raise HTTPException(status_code=404, detail=f"Plan '{plan_name}' not found")
    cfg.pop("oauth2_secret_name", None)
    return cfg


@app.delete("/ingestion/plans/{plan_name}", status_code=204)
def delete_plan(plan_name: str):
    """Delete an ingestion plan and its associated OAuth2 secret."""
    if not _load_plan(plan_name):
        raise HTTPException(status_code=404, detail=f"Plan '{plan_name}' not found")
    _delete_plan(plan_name)
    _delete_oauth2_secret(plan_name)


@app.post("/ingestion/plans/{plan_name}/run")
def run_plan(plan_name: str):
    """Trigger an immediate single-run execution via Step Functions."""
    if not INGESTION_STATE_MACHINE_ARN:
        raise HTTPException(status_code=503, detail="INGESTION_STATE_MACHINE_ARN not configured")

    if not _load_plan(plan_name):
        raise HTTPException(status_code=404, detail=f"Plan '{plan_name}' not found")

    response = sfn.start_execution(
        stateMachineArn=INGESTION_STATE_MACHINE_ARN,
        input=json.dumps({"run_mode": "single", "plan_name": plan_name}),
    )
    logger.info("Started SFN execution for plan '%s': %s", plan_name, response["executionArn"])
    return {
        "plan_name": plan_name,
        "execution_arn": response["executionArn"],
        "status": "started",
    }


# =============================================================================
# Lambda handler
# =============================================================================

handler = Mangum(app, lifespan="off")
