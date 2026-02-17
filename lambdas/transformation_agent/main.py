"""
Transformation Agent Lambda — FastAPI service that generates and submits
gold-layer transformation plans using AI agents.

Endpoints:
    POST /agent/transformation/plan  — Generate a TransformationPlan from table metadata
    POST /agent/transformation/run   — Generate plan + submit jobs to transform API
"""

import logging
import os

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
    """Generate a transformation plan and submit jobs to the transform API."""
    from agents.transformation_agent.main import generate_plan as gen_plan
    from agents.transformation_agent.models import TransformationPlan
    from agents.transformation_agent.runner import run as run_transform

    api_url = os.environ.get("API_GATEWAY_ENDPOINT", "")
    if not api_url:
        raise HTTPException(
            status_code=503,
            detail="API_GATEWAY_ENDPOINT not configured",
        )

    # Step 1: Generate plan
    try:
        plan_dict = await gen_plan(
            domain=request.domain,
            tables=request.tables,
            api_url=api_url,
        )
    except Exception as exc:
        logger.exception("Failed to generate transformation plan")
        raise HTTPException(
            status_code=500,
            detail=f"Plan generation failed: {exc}",
        )

    # Step 2: Submit jobs
    try:
        plan = TransformationPlan.model_validate(plan_dict)
        result = await run_transform(
            plan=plan,
            api_url=api_url,
            trigger_execution=request.trigger_execution,
        )
        return {
            "plan": plan_dict,
            "result": result.summary(),
        }
    except Exception as exc:
        logger.exception("Transformation pipeline failed")
        raise HTTPException(
            status_code=500,
            detail=f"Pipeline execution failed: {exc}",
        )


# Lambda handler
handler = Mangum(app)
