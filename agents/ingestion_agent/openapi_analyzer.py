"""
PydanticAI-based OpenAPI spec analyzer.

This module contains a PydanticAI Agent that receives an OpenAPI spec JSON
and a list of user interests (in natural language), performs semantic mapping
of interests to API endpoints, and returns a structured IngestionPlan.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from pydantic_ai import Agent

from agents.ingestion_agent.models import IngestionPlan
from agents.ingestion_agent.spec_parser import build_spec_summary

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "bedrock:us.anthropic.claude-3-5-sonnet-20241022-v2:0"

ANALYZER_SYSTEM_PROMPT = """\
You are an expert API data engineer. Your job is to analyze OpenAPI/Swagger \
specifications and map user interests (in natural language) to concrete API \
endpoints for data ingestion into a data lake.

Rules:
1. PRIORITIZE collection/list endpoints (GET that returns arrays) over single-resource endpoints.
2. For each selected endpoint, extract or infer the primary_key from the response \
   schema (look for fields named "id", "{resource}_id", or fields marked as unique/primary).
3. Convert resource names to snake_case for table naming (e.g., "CustomerInvoices" -> "customer_invoices").
4. Detect the authentication type from the securitySchemes in the spec.
5. Detect pagination patterns from endpoint parameters (look for page, offset, limit, cursor, after, before).
6. Extract the base_url from the servers array or host+basePath fields.
7. Identify the data_path (JSON path to the array of results) from the response schema \
   (look for common patterns: "results", "data", "items", "records", or the root array).
8. Map user interests SEMANTICALLY:
   - "vendas" or "sales" -> /orders, /transactions, /invoices
   - "clientes" or "customers" -> /customers, /users, /contacts
   - "produtos" or "products" -> /products, /items, /catalog
   - "financeiro" or "finance" -> /payments, /invoices, /billing
   - Use your judgment for other natural language terms.
9. Only include endpoints that are relevant to the user's stated interests.
10. Generate the api_name from the API title in the spec, converted to snake_case.
11. Return ONLY the structured IngestionPlan object. No explanations.
"""


@dataclass
class AnalyzerDeps:
    """Dependencies injected into the PydanticAI analyzer agent."""

    openapi_spec: dict
    interests: list[str]


def create_openapi_analyzer() -> Agent[AnalyzerDeps, IngestionPlan]:
    """
    Create the PydanticAI agent that analyzes OpenAPI specs.

    Model is configurable via INGESTION_AGENT_MODEL env var.
    Examples:
        INGESTION_AGENT_MODEL=anthropic:claude-sonnet-4-5-20250929
        INGESTION_AGENT_MODEL=bedrock:us.anthropic.claude-3-5-sonnet-20241022-v2:0
        INGESTION_AGENT_MODEL=openai:gpt-4o

    Returns a configured Agent instance with IngestionPlan as
    structured output type.
    """
    model = os.environ.get("INGESTION_AGENT_MODEL", DEFAULT_MODEL)
    logger.info("Using model: %s", model)

    agent = Agent(
        model,
        deps_type=AnalyzerDeps,
        output_type=IngestionPlan,
        system_prompt=ANALYZER_SYSTEM_PROMPT,
        retries=2,
    )

    @agent.system_prompt
    async def inject_spec_context(ctx) -> str:
        """Inject the OpenAPI spec summary and user interests into the prompt."""
        spec_summary = build_spec_summary(ctx.deps.openapi_spec)
        interests_str = ", ".join(ctx.deps.interests)
        return (
            f"\n\n--- OpenAPI Spec Summary ---\n{spec_summary}\n"
            f"\n--- User Interests ---\n{interests_str}\n"
            f"\nAnalyze the spec above and return an IngestionPlan that maps "
            f"the user interests to the most relevant collection endpoints."
        )

    return agent


async def analyze_openapi_spec(
    openapi_spec: dict,
    interests: list[str],
) -> IngestionPlan:
    """
    Analyze an OpenAPI spec and return a structured IngestionPlan.

    Args:
        openapi_spec: Parsed OpenAPI/Swagger JSON spec.
        interests: List of user interests in natural language.

    Returns:
        IngestionPlan with mapped endpoints ready for dlt pipeline init.
    """
    analyzer = create_openapi_analyzer()
    deps = AnalyzerDeps(openapi_spec=openapi_spec, interests=interests)

    result = await analyzer.run(
        "Analyze this OpenAPI spec and generate the IngestionPlan for the given interests.",
        deps=deps,
    )

    logger.info(
        "IngestionPlan generated: %d endpoints for interests %s",
        len(result.output.endpoints),
        interests,
    )

    return result.output
