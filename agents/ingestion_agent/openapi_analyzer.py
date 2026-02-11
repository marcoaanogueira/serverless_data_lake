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

DEFAULT_MODEL = "bedrock:anthropic.claude-haiku-4-5-20251001-v1:0"

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
12. CRITICAL — base_url MUST be a REAL, routable URL derived from the spec's \
    servers array, host field, or the Source URL provided. \
    NEVER use placeholder domains like example.com, api.example.com, \
    localhost, or any made-up hostname. If you cannot determine the real \
    base URL, use the origin (scheme + host) of the Source URL.
"""


@dataclass
class AnalyzerDeps:
    """Dependencies injected into the PydanticAI analyzer agent."""

    openapi_spec: dict
    interests: list[str]
    source_url: str | None = None


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
        spec_summary = build_spec_summary(
            ctx.deps.openapi_spec, source_url=ctx.deps.source_url
        )
        interests_str = ", ".join(ctx.deps.interests)
        return (
            f"\n\n--- OpenAPI Spec Summary ---\n{spec_summary}\n"
            f"\n--- User Interests ---\n{interests_str}\n"
            f"\nAnalyze the spec above and return an IngestionPlan that maps "
            f"the user interests to the most relevant collection endpoints."
        )

    return agent


_PLACEHOLDER_HOSTS = {"example.com", "api.example.com", "localhost", "127.0.0.1"}


def _derive_base_url(source_url: str) -> str:
    """Extract scheme + host (+ optional path prefix) from a URL."""
    from urllib.parse import urlparse

    parsed = urlparse(source_url)
    # Use scheme://host as base; strip trailing path components that look
    # like file names (e.g. /openapi.json).
    base = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path.rstrip("/")
    if path and not path.endswith((".json", ".yaml", ".yml")):
        base += path
    return base


def _validate_base_url(plan: IngestionPlan, source_url: str | None) -> IngestionPlan:
    """Replace hallucinated base_url with a real one derived from source_url."""
    from urllib.parse import urlparse

    parsed = urlparse(plan.base_url)
    host = parsed.hostname or ""
    if host in _PLACEHOLDER_HOSTS or not host:
        if source_url:
            real_base = _derive_base_url(source_url)
            logger.warning(
                "Detected placeholder base_url '%s', replacing with '%s'",
                plan.base_url,
                real_base,
            )
            plan = plan.model_copy(update={"base_url": real_base})
        else:
            logger.warning(
                "Detected placeholder base_url '%s' but no source_url to fix it",
                plan.base_url,
            )
    return plan


async def analyze_openapi_spec(
    openapi_spec: dict,
    interests: list[str],
    source_url: str | None = None,
) -> IngestionPlan:
    """
    Analyze an OpenAPI spec and return a structured IngestionPlan.

    Args:
        openapi_spec: Parsed OpenAPI/Swagger JSON spec.
        interests: List of user interests in natural language.
        source_url: Original URL the spec was fetched from (used as
            fallback for base_url derivation).

    Returns:
        IngestionPlan with mapped endpoints ready for dlt pipeline init.
    """
    analyzer = create_openapi_analyzer()
    deps = AnalyzerDeps(
        openapi_spec=openapi_spec, interests=interests, source_url=source_url
    )

    result = await analyzer.run(
        "Analyze this OpenAPI spec and generate the IngestionPlan for the given interests.",
        deps=deps,
    )

    plan = _validate_base_url(result.output, source_url)

    logger.info(
        "IngestionPlan generated: %d endpoints for interests %s (base_url=%s)",
        len(plan.endpoints),
        interests,
        plan.base_url,
    )

    return plan
