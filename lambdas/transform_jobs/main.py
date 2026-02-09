"""
Transform Jobs API - FastAPI Lambda

CRUD operations for managing gold layer transform jobs.
Job configs are stored as YAML in S3 via the Schema Registry.
Executions are triggered via Step Functions.
"""

import os
import json
import logging
from typing import Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pydantic import BaseModel, Field, field_validator
import re
import boto3

from shared.schema_registry import SchemaRegistry

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Transform Jobs API",
    description="Manage gold layer transformation jobs",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

registry = SchemaRegistry()

STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN", "")


# =============================================================================
# Request/Response Models
# =============================================================================

class CreateJobRequest(BaseModel):
    """Request model for creating a transform job"""
    domain: str
    job_name: str
    query: str
    write_mode: str = Field(default="overwrite", pattern="^(overwrite|append)$")
    unique_key: Optional[str] = None
    schedule_type: str = Field(default="cron", pattern="^(cron|dependency)$")
    cron_schedule: Optional[str] = None
    dependencies: Optional[list[str]] = None
    status: str = Field(default="active")

    @field_validator("domain", "job_name")
    @classmethod
    def validate_snake_case(cls, v: str) -> str:
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError("Must be snake_case")
        return v


class UpdateJobRequest(BaseModel):
    """Request model for updating a transform job"""
    query: Optional[str] = None
    write_mode: Optional[str] = None
    unique_key: Optional[str] = None
    schedule_type: Optional[str] = None
    cron_schedule: Optional[str] = None
    dependencies: Optional[list[str]] = None
    status: Optional[str] = None


class JobResponse(BaseModel):
    """Response model for a transform job"""
    id: str
    domain: str
    job_name: str
    query: str
    write_mode: str
    unique_key: Optional[str] = None
    schedule_type: str
    cron_schedule: Optional[str] = None
    dependencies: Optional[list[str]] = None
    status: str
    created_at: str
    updated_at: str


class ExecutionResponse(BaseModel):
    """Response model for a job execution"""
    execution_id: str
    job_name: str
    domain: str
    status: str
    started_at: str


def job_to_response(job: dict) -> JobResponse:
    """Convert a job config dict to API response"""
    return JobResponse(
        id=f"{job['domain']}/{job['job_name']}",
        domain=job["domain"],
        job_name=job["job_name"],
        query=job.get("query", ""),
        write_mode=job.get("write_mode", "overwrite"),
        unique_key=job.get("unique_key"),
        schedule_type=job.get("schedule_type", "cron"),
        cron_schedule=job.get("cron_schedule"),
        dependencies=job.get("dependencies"),
        status=job.get("status", "active"),
        created_at=job.get("created_at", ""),
        updated_at=job.get("updated_at", ""),
    )


# =============================================================================
# API Endpoints
# =============================================================================

@app.get("/")
def health_check():
    return {"status": "healthy", "service": "transform_jobs"}


@app.post("/transform/jobs", response_model=JobResponse)
def create_job(request: CreateJobRequest):
    """Create a new transform job"""
    existing = registry.get_gold_job(request.domain, request.job_name)
    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"Job {request.domain}/{request.job_name} already exists"
        )

    config = request.model_dump(exclude_none=True)
    saved = registry.save_gold_job(request.domain, request.job_name, config)
    return job_to_response(saved)


@app.get("/transform/jobs", response_model=list[JobResponse])
def list_jobs(
    domain: Optional[str] = Query(None, description="Filter by domain"),
    order_by: Optional[str] = Query(None, description="Order by field"),
):
    """List all transform jobs"""
    jobs = registry.list_gold_jobs(domain=domain)
    responses = [job_to_response(j) for j in jobs]

    if order_by:
        reverse = order_by.startswith("-")
        field = order_by.lstrip("-")
        responses.sort(key=lambda x: getattr(x, field, ""), reverse=reverse)

    return responses


@app.get("/transform/jobs/{domain}/{job_name}", response_model=JobResponse)
def get_job(domain: str, job_name: str):
    """Get a specific transform job"""
    job = registry.get_gold_job(domain, job_name)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {domain}/{job_name} not found")
    return job_to_response(job)


@app.put("/transform/jobs/{domain}/{job_name}", response_model=JobResponse)
def update_job(domain: str, job_name: str, request: UpdateJobRequest):
    """Update an existing transform job"""
    existing = registry.get_gold_job(domain, job_name)
    if not existing:
        raise HTTPException(status_code=404, detail=f"Job {domain}/{job_name} not found")

    updates = request.model_dump(exclude_none=True)
    existing.update(updates)
    saved = registry.save_gold_job(domain, job_name, existing)
    return job_to_response(saved)


@app.delete("/transform/jobs/{domain}/{job_name}")
def delete_job(domain: str, job_name: str):
    """Delete a transform job"""
    success = registry.delete_gold_job(domain, job_name)
    if not success:
        raise HTTPException(status_code=404, detail=f"Job {domain}/{job_name} not found")
    return {"message": f"Job {domain}/{job_name} deleted"}


@app.post("/transform/jobs/{domain}/{job_name}/run", response_model=ExecutionResponse)
def run_job(domain: str, job_name: str):
    """Trigger execution of a transform job via Step Functions"""
    job = registry.get_gold_job(domain, job_name)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {domain}/{job_name} not found")

    if not STATE_MACHINE_ARN:
        raise HTTPException(status_code=503, detail="Step Functions not configured")

    sfn = boto3.client("stepfunctions")
    execution_input = json.dumps({
        "run_mode": "single",
        "domain": domain,
        "job_name": job_name,
        "query": job["query"],
        "write_mode": job.get("write_mode", "overwrite"),
        "unique_key": job.get("unique_key", ""),
        "schema_bucket": registry.bucket,
    })

    try:
        response = sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=f"{domain}-{job_name}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            input=execution_input,
        )
        execution_arn = response["executionArn"]
        execution_id = execution_arn.split(":")[-1]

        return ExecutionResponse(
            execution_id=execution_id,
            job_name=job_name,
            domain=domain,
            status="RUNNING",
            started_at=response["startDate"].isoformat(),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start execution: {str(e)}")


@app.get("/transform/executions/{execution_id}")
def get_execution(execution_id: str):
    """Get the status of a job execution"""
    if not STATE_MACHINE_ARN:
        raise HTTPException(status_code=503, detail="Step Functions not configured")

    sfn = boto3.client("stepfunctions")

    # List recent executions to find the one matching the ID
    try:
        response = sfn.list_executions(
            stateMachineArn=STATE_MACHINE_ARN,
            maxResults=100,
        )

        for execution in response.get("executions", []):
            arn = execution["executionArn"]
            if arn.endswith(execution_id):
                detail = sfn.describe_execution(executionArn=arn)
                return {
                    "execution_id": execution_id,
                    "status": detail["status"],
                    "started_at": detail["startDate"].isoformat(),
                    "stopped_at": detail.get("stopDate", {}).isoformat() if detail.get("stopDate") else None,
                    "input": json.loads(detail.get("input", "{}")),
                    "output": json.loads(detail.get("output", "null")),
                }

        raise HTTPException(status_code=404, detail=f"Execution {execution_id} not found")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get execution: {str(e)}")


# Lambda handler
handler = Mangum(app)
