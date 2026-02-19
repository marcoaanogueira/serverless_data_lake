"""
Strands-based Lakehouse Ingestion Agent.

Orchestrates the OpenAPI analysis pipeline using a Strands Agent with
BedrockModel (Claude 3.5 Sonnet). Exposes a single tool that delegates
the technical extraction to the PydanticAI analyzer.
"""

from __future__ import annotations

import asyncio
import json
import logging

import httpx
from strands import Agent, tool
from strands.models import BedrockModel

from agents.ingestion_agent.models import IngestionPlan, OAuth2Config
from agents.ingestion_agent.openapi_analyzer import analyze_openapi_spec
from agents.ingestion_agent.spec_parser import extract_swagger_spec_url

logger = logging.getLogger(__name__)


async def _fetch_docs_page(url: str, max_chars: int = 8000) -> str:
    """
    Fetch an HTML documentation page and extract its text content.

    Returns a plain-text version of the page, truncated to *max_chars*
    to avoid overwhelming the LLM context window.
    """
    from html.parser import HTMLParser

    class _HTMLToText(HTMLParser):
        _SKIP_TAGS = frozenset(("script", "style", "noscript", "svg", "head"))
        _BLOCK_TAGS = frozenset((
            "p", "br", "div", "h1", "h2", "h3", "h4", "h5", "h6",
            "li", "tr", "dt", "dd", "section", "article", "pre",
        ))

        def __init__(self) -> None:
            super().__init__()
            self._parts: list[str] = []
            self._skip_depth = 0

        def handle_starttag(self, tag: str, attrs: list) -> None:
            if tag in self._SKIP_TAGS:
                self._skip_depth += 1

        def handle_endtag(self, tag: str) -> None:
            if tag in self._SKIP_TAGS:
                self._skip_depth = max(0, self._skip_depth - 1)
            if tag in self._BLOCK_TAGS:
                self._parts.append("\n")

        def handle_data(self, data: str) -> None:
            if self._skip_depth == 0:
                self._parts.append(data)

        def get_text(self) -> str:
            import re
            text = "".join(self._parts)
            text = re.sub(r"\n{3,}", "\n\n", text)
            return text.strip()

    async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
        resp = await client.get(url, headers={"Accept": "text/html, */*"})
        resp.raise_for_status()

    parser = _HTMLToText()
    parser.feed(resp.text)
    text = parser.get_text()

    if len(text) > max_chars:
        text = text[:max_chars] + "\n... [truncated]"

    logger.info("Fetched docs from %s (%d chars)", url, len(text))
    return text


async def _fetch_openapi_spec(url: str, token: str | None = None) -> dict:
    """
    Fetch and parse an OpenAPI spec from a URL.

    Handles both JSON and YAML specs, following redirects.
    When the URL returns a Swagger UI or Redoc HTML page, the function
    automatically detects and follows the embedded spec URL instead of
    raising an error.
    Raises ValueError with a clear message if the URL does not return
    a valid OpenAPI/Swagger spec and no spec URL can be detected.
    """
    headers = {"Accept": "application/json, application/yaml, */*"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "")
        body = response.text

        # When the response is HTML, check if it's a Swagger UI / Redoc page
        # and auto-follow the embedded spec URL before giving up.
        if "html" in content_type or body.lstrip().startswith(("<!DOCTYPE", "<html", "<HTML")):
            spec_url = extract_swagger_spec_url(body, url)
            if spec_url:
                return await _fetch_openapi_spec(spec_url, token)
            raise ValueError(
                f"The URL '{url}' returned an HTML page, not an OpenAPI/Swagger spec. "
                f"Please provide a direct URL to the JSON or YAML spec file "
                f"(e.g., https://petstore3.swagger.io/api/v3/openapi.json). "
                f"If the API has no spec, use --plan with a pre-built plan JSON file."
            )

        if "yaml" in content_type or url.endswith((".yaml", ".yml")):
            import yaml

            return yaml.safe_load(body)

        try:
            spec = response.json()
        except Exception as exc:
            raise ValueError(
                f"The URL '{url}' did not return valid JSON or YAML. "
                f"Content-Type: {content_type}. "
                f"Please provide a direct URL to the OpenAPI/Swagger spec."
            ) from exc

        # Warn (but don't fail) if it doesn't look like a formal OpenAPI spec.
        # Many APIs (e.g., rickandmortyapi.com/api) return a JSON index that
        # the LLM can still analyze to produce a valid ingestion plan.
        if not isinstance(spec, dict):
            raise ValueError(
                f"The URL '{url}' returned a JSON value that is not a dict. "
                f"Expected an OpenAPI spec or API index object."
            )
        if not (spec.get("paths") or spec.get("openapi") or spec.get("swagger")):
            logger.warning(
                "URL '%s' returned JSON but it doesn't look like a formal "
                "OpenAPI/Swagger spec (missing 'paths', 'openapi', or 'swagger' "
                "keys). Will attempt analysis anyway — the LLM may still be "
                "able to generate a plan from this API index.",
                url,
            )

        return spec


@tool
def analyze_api_for_ingestion(
    openapi_url: str,
    token: str,
    interests: list,
) -> dict:
    """Analyze an OpenAPI spec and generate a structured ingestion plan.

    Fetches the OpenAPI/Swagger spec from the given URL, analyzes it against
    the user's interests, and returns a structured IngestionPlan with endpoints
    mapped to data lake tables.

    Args:
        openapi_url: URL pointing to the OpenAPI/Swagger JSON or YAML spec.
        token: Bearer token for authenticating against the API.
        interests: List of subjects of interest in natural language (e.g., ["vendas", "clientes"]).

    Returns:
        A dictionary containing the structured IngestionPlan with base_url,
        endpoints, primary keys, and dlt-compatible configuration.
    """
    loop = asyncio.new_event_loop()
    try:
        plan = loop.run_until_complete(
            _run_analysis(openapi_url, token, interests)
        )
        return plan.model_dump()
    finally:
        loop.close()


async def _run_analysis(
    openapi_url: str,
    token: str | None,
    interests: list[str],
    docs_url: str | None = None,
) -> IngestionPlan:
    """Internal async pipeline: fetch spec -> analyze -> return plan."""
    logger.info("Fetching OpenAPI spec from %s", openapi_url)
    spec = await _fetch_openapi_spec(openapi_url, token)

    logger.info(
        "Spec fetched: %s v%s — %d paths",
        spec.get("info", {}).get("title", "Unknown"),
        spec.get("info", {}).get("version", "?"),
        len(spec.get("paths", {})),
    )

    docs_text: str | None = None
    if docs_url:
        try:
            docs_text = await _fetch_docs_page(docs_url)
        except Exception as exc:
            logger.warning("Failed to fetch docs from %s: %s", docs_url, exc)

    plan = await analyze_openapi_spec(
        spec, interests, source_url=openapi_url, docs_text=docs_text,
    )

    # Safety net: drop mutation endpoints the LLM may have slipped in.
    # 1. prefer_get_endpoints: when GET + POST share a path, keep only GET.
    # 2. drop_non_collection_post: remove non-GET endpoints where
    #    is_collection=False (clear mutations: POST create, PUT, PATCH, DELETE).
    plan = plan.prefer_get_endpoints().drop_non_collection_post()

    logger.info(
        "Plan generated: %s with %d endpoints",
        plan.api_name,
        len(plan.endpoints),
    )

    return plan


def create_ingestion_agent() -> Agent:
    """
    Create the Strands orchestration agent.

    Uses BedrockModel with Claude 3.5 Sonnet and exposes the
    analyze_api_for_ingestion tool.
    """
    bedrock_model = BedrockModel(
        model_id="us.anthropic.claude-3-5-sonnet-20241022-v2:0",
        streaming=True,
    )

    agent = Agent(
        model=bedrock_model,
        tools=[analyze_api_for_ingestion],
        system_prompt=(
            "You are a Lakehouse Ingestion Agent. Your ONLY job is to receive "
            "an OpenAPI spec URL, an authentication token, and a list of subjects "
            "of interest, then call the analyze_api_for_ingestion tool to generate "
            "a structured ingestion plan.\n\n"
            "Rules:\n"
            "1. ALWAYS call the analyze_api_for_ingestion tool with the provided parameters.\n"
            "2. Return ONLY the raw JSON output from the tool. No explanations, "
            "no greetings, no commentary.\n"
            "3. Do NOT modify or summarize the tool output.\n"
            "4. If the tool returns an error, return the error message as JSON."
        ),
    )

    return agent


async def run_ingestion_agent(
    openapi_url: str,
    token: str = "",
    interests: list[str] | None = None,
    docs_url: str | None = None,
    oauth2: OAuth2Config | None = None,
) -> IngestionPlan:
    """
    High-level async entry point for the ingestion agent.

    This is the recommended way to use the agent programmatically.
    It bypasses the Strands conversational layer and calls the
    PydanticAI analyzer directly for maximum reliability.

    Args:
        openapi_url: URL to the OpenAPI/Swagger spec.
        token: Bearer token for API auth. If empty and oauth2 is provided,
            a token is fetched automatically before fetching the spec.
        interests: Natural language list of subjects of interest.
        docs_url: Optional URL to the API documentation page (HTML).
        oauth2: Optional OAuth2 ROPC credentials. When provided (and token
            is empty), an access token is fetched from the token endpoint
            and reused for all subsequent API calls.

    Returns:
        Validated IngestionPlan object.
    """
    if interests is None:
        interests = []

    # Resolve OAuth2 token once so it can be reused for spec + sample fetches
    if oauth2 and not token:
        from agents.ingestion_agent.runner import fetch_oauth2_token
        token = await fetch_oauth2_token(oauth2)

    plan = await _run_analysis(openapi_url, token, interests, docs_url=docs_url)
    return plan


def run_ingestion_agent_via_strands(
    openapi_url: str,
    token: str,
    interests: list[str],
) -> dict:
    """
    Run the ingestion agent through the Strands orchestration layer.

    Uses the full Strands Agent with BedrockModel. The agent will
    call the analyze_api_for_ingestion tool and return the result.

    Args:
        openapi_url: URL to the OpenAPI/Swagger spec.
        token: Bearer token for API auth.
        interests: Natural language list of subjects of interest.

    Returns:
        Raw dictionary with the ingestion plan from the Strands agent.
    """
    agent = create_ingestion_agent()

    prompt = (
        f"Generate an ingestion plan for this API.\n"
        f"OpenAPI URL: {openapi_url}\n"
        f"Token: {token}\n"
        f"Interests: {json.dumps(interests)}"
    )

    result = agent(prompt)

    # Extract the structured output from the agent response
    response_text = str(result)

    try:
        # Try to parse the JSON from the agent's response
        plan_data = json.loads(response_text)
        # Validate through Pydantic
        plan = IngestionPlan.model_validate(plan_data)
        return plan.model_dump()
    except (json.JSONDecodeError, Exception):
        # If the agent returned the data inside the tool result,
        # look for it in the tool results
        logger.warning(
            "Could not parse agent text response as JSON, returning raw text"
        )
        return {"raw_response": response_text}
