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
from agents.ingestion_agent.spec_parser import build_spec_summary, extract_field_descriptions

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "bedrock:us.amazon.nova-2-lite-v1:0"

ANALYZER_SYSTEM_PROMPT = """\
You are an expert API data engineer. Your job is to analyze OpenAPI/Swagger \
specifications and map user interests (in natural language) to concrete API \
endpoints for data ingestion into a data lake.

YOUR RESPONSIBILITIES (what the LLM must do):
  - Map user interests to the most relevant collection/list API endpoints
  - Detect auth type and pagination patterns from the spec

NOT YOUR RESPONSIBILITY (handled automatically by code):
  - primary_key detection: A separate PK agent will identify the primary \
    key after fetching a real sample record from the API. You may set \
    primary_key if you are confident from the response schema, but it \
    will be overridden if empty. When in doubt, leave primary_key as null.
  - data_path detection: Code will fetch the actual API response and \
    auto-detect where the data array lives. You may set data_path if \
    you are confident, but code will override it if the real response \
    structure differs. When in doubt, leave data_path as empty string "".
  - Schema/column inference: Code fetches a real sample record and \
    infers the schema from it. You do NOT need to worry about column \
    definitions.

Rules:
1. PRIORITIZE collection/list endpoints (GET that returns arrays) over single-resource endpoints.
2. If the response schema clearly shows a primary key field (e.g., "id", \
   "{resource}_id"), you MAY set primary_key. Otherwise set primary_key to null — \
   a separate PK agent will determine it from the actual data.
3. Convert resource names to snake_case for table naming (e.g., "CustomerInvoices" -> "customer_invoices").
4. Detect the authentication type from the securitySchemes in the spec.
5. Detect pagination patterns and configure the `pagination` object properly:
   - PREFER 'json_link' when the response schema includes a next-page URL field \
     (e.g., "next", "info.next", "paging.next", "links.next"). Set `next_url_path` \
     to the dot-separated path to that field.
   - Use 'page_number' only when the response body EXPLICITLY contains a total-pages \
     or total-items field (e.g., "info.pages", "meta.total_pages", "total"). \
     Set `total_path` to the dot-separated path to that field. \
     NEVER use 'page_number' if the API returns a plain array with no pagination \
     metadata — use 'auto' instead.
   - Use 'offset' when the API uses offset/limit params AND the response contains \
     a total count. Set `limit` to the page size.
   - Use 'cursor' when the API uses cursor-based pagination. Set `cursor_path` and `cursor_param`.
   - Use 'auto' when you cannot determine the pagination pattern, or when the API \
     returns a plain array/list without any pagination metadata (no total, no next \
     URL, no cursor). PREFER 'auto' over guessing — it is safer.
6. Extract the base_url from the servers array or host+basePath fields.
7. Map user interests SEMANTICALLY:
   - "vendas" or "sales" -> /orders, /transactions, /invoices
   - "clientes" or "customers" -> /customers, /users, /contacts
   - "produtos" or "products" -> /products, /items, /catalog
   - "financeiro" or "finance" -> /payments, /invoices, /billing
   - Use your judgment for other natural language terms.
8. Only include endpoints that are relevant to the user's stated interests.
9. Generate the api_name from the API title in the spec, converted to snake_case.
10. Return ONLY the structured IngestionPlan object. No explanations.
    CRITICAL: Each endpoint MUST have a UNIQUE resource_name. Do NOT generate \
    multiple endpoints with the same resource_name. Pick only the main \
    collection/list endpoint per resource. Skip search, random, autocomplete, \
    and metadata endpoints — only include the primary list endpoint.
11. CRITICAL — base_url MUST be a REAL, routable URL derived from the spec's \
    servers array, host field, or the Source URL provided. \
    NEVER use placeholder domains like example.com, api.example.com, \
    localhost, or any made-up hostname. If you cannot determine the real \
    base URL, use the origin (scheme + host) of the Source URL.
12. API INDEX HANDLING — When the input is NOT a formal OpenAPI spec but an \
    API index (a JSON object mapping resource names to URLs), follow these rules: \
    a) Extract the endpoint PATH from the URL value, NOT from the key name. \
       Example: key="characters", url="https://host/api/character" → path="/character" (NOT "/characters"). \
    b) Derive base_url from the common prefix of all endpoint URLs. \
    c) Use 'auto' pagination unless you can infer the pattern from the response structure. \
    d) Set primary_key to null — the PK agent will determine it from the actual data.
"""


@dataclass
class AnalyzerDeps:
    """Dependencies injected into the PydanticAI analyzer agent."""

    openapi_spec: dict
    interests: list[str]
    source_url: str | None = None
    docs_text: str | None = None


def create_openapi_analyzer() -> Agent[AnalyzerDeps, IngestionPlan]:
    """
    Create the PydanticAI agent that analyzes OpenAPI specs.

    Model is configurable via INGESTION_AGENT_MODEL env var.
    Examples:
        INGESTION_AGENT_MODEL=anthropic:claude-sonnet-4-5-20250929
        INGESTION_AGENT_MODEL=bedrock:us.anthropic.claude-haiku-4-5-20251001-v1:0
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
        retries=4,
    )

    @agent.system_prompt
    async def inject_spec_context(ctx) -> str:
        """Inject the OpenAPI spec summary and user interests into the prompt."""
        spec_summary = build_spec_summary(
            ctx.deps.openapi_spec, source_url=ctx.deps.source_url
        )
        interests_str = ", ".join(ctx.deps.interests)

        parts = [
            f"\n\n--- OpenAPI Spec Summary ---\n{spec_summary}\n",
        ]

        if ctx.deps.docs_text:
            parts.append(
                f"\n--- API Documentation (from docs page) ---\n"
                f"{ctx.deps.docs_text}\n"
                f"\nIMPORTANT: Use the documentation above to determine the "
                f"correct base_url (including version prefixes like /v1) and "
                f"exact endpoint paths. The docs page is authoritative — prefer "
                f"it over guesses from the spec summary.\n"
            )

        parts.append(
            f"\n--- User Interests ---\n{interests_str}\n"
            f"\nAnalyze the spec above and return an IngestionPlan that maps "
            f"the user interests to the most relevant collection endpoints."
        )

        return "".join(parts)

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
    docs_text: str | None = None,
) -> IngestionPlan:
    """
    Analyze an OpenAPI spec and return a structured IngestionPlan.

    Args:
        openapi_spec: Parsed OpenAPI/Swagger JSON spec.
        interests: List of user interests in natural language.
        source_url: Original URL the spec was fetched from (used as
            fallback for base_url derivation).
        docs_text: Optional plain-text extracted from the API documentation
            page. Provides extra context (versioned paths, field names, etc.).

    Returns:
        IngestionPlan with mapped endpoints ready for dlt pipeline init.
    """
    analyzer = create_openapi_analyzer()
    deps = AnalyzerDeps(
        openapi_spec=openapi_spec,
        interests=interests,
        source_url=source_url,
        docs_text=docs_text,
    )

    result = await analyzer.run(
        "Analyze this OpenAPI spec and generate the IngestionPlan for the given interests.",
        deps=deps,
    )

    plan = _validate_base_url(result.output, source_url)

    # Enrich endpoints with field descriptions from the OpenAPI spec.
    # These are extracted programmatically (more reliable than asking the LLM).
    for endpoint in plan.endpoints:
        if not endpoint.field_descriptions:
            descriptions = extract_field_descriptions(
                openapi_spec, endpoint.path, endpoint.method,
            )
            if descriptions:
                endpoint.field_descriptions = descriptions
                logger.info(
                    "[%s] Extracted %d field description(s) from OpenAPI spec.",
                    endpoint.resource_name,
                    len(descriptions),
                )

    logger.info(
        "IngestionPlan generated: %d endpoints for interests %s (base_url=%s)",
        len(plan.endpoints),
        interests,
        plan.base_url,
    )

    return plan
