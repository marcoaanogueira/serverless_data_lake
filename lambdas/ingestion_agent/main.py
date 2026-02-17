"""
Ingestion Agent Lambda — FastAPI service that generates and executes
ingestion plans from OpenAPI specs using AI agents.

Endpoints:
    POST /agent/ingestion/plan  — Generate an IngestionPlan from an OpenAPI URL
    POST /agent/ingestion/run   — Generate plan + execute full ingestion pipeline
"""

import logging
import os

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
    """Generate an ingestion plan and execute the full pipeline."""
    from agents.ingestion_agent.agent import run_ingestion_agent
    from agents.ingestion_agent.runner import run as run_ingestion

    api_url = os.environ.get("API_GATEWAY_ENDPOINT", "")
    if not api_url:
        raise HTTPException(
            status_code=503,
            detail="API_GATEWAY_ENDPOINT not configured",
        )

    # Step 1: Generate plan
    try:
        plan = await run_ingestion_agent(
            openapi_url=request.openapi_url,
            token=request.token,
            interests=request.interests,
            docs_url=request.docs_url,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Failed to generate ingestion plan")
        raise HTTPException(status_code=500, detail=f"Plan generation failed: {exc}")

    # Step 2: Execute pipeline
    try:
        result = await run_ingestion(
            plan=plan,
            domain=request.domain,
            api_url=api_url,
            token=request.token,
            batch_size=request.batch_size,
        )
        return {
            "plan": plan.model_dump(),
            "result": result.summary(),
        }
    except Exception as exc:
        logger.exception("Ingestion pipeline failed")
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {exc}")


# Lambda handler
handler = Mangum(app)
