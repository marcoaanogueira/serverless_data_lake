"""
Lakehouse Ingestion Agent.

Analyzes OpenAPI specs and generates structured ingestion plans
for dlt pipelines into the Serverless Data Lake.
"""

from agents.ingestion_agent.models import EndpointSpec, IngestionPlan

__all__ = ["EndpointSpec", "IngestionPlan"]


def run_ingestion_agent(*args, **kwargs):
    """Lazy import to avoid requiring strands/pydantic_ai at module load."""
    from agents.ingestion_agent.agent import run_ingestion_agent as _run

    return _run(*args, **kwargs)
