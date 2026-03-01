"""OpenAPI URL discovery via web search + LLM relevance filtering.

Given a free-text API name (e.g. "Projuris", "Stripe"), this module:
1. Searches DuckDuckGo for candidate URLs.
2. Asks an LLM to identify which results are actually about the queried API.
3. Probes only the LLM-selected candidates for a valid OpenAPI spec.
"""

from __future__ import annotations

import asyncio
import logging
import os
from urllib.parse import urlparse

import httpx
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# Common OpenAPI spec paths to probe on a candidate host
_SPEC_PATHS = [
    "/openapi.json",
    "/openapi.yaml",
    "/swagger.json",
    "/swagger.yaml",
    "/api-docs",
    "/v3/api-docs",
    "/v2/api-docs",
    "/api/openapi.json",
    "/api/swagger.json",
    "/docs/openapi.json",
    "/api/v1/openapi.json",
    "/api/v2/openapi.json",
    "/api/v3/openapi.json",
    "/swagger/v1/swagger.json",
    "/swagger/v2/swagger.json",
    # API index roots (e.g. SWAPI, Rick & Morty) — no formal OpenAPI spec
    "/api/",
    "/api",
    "/api/v1/",
    "/api/v2/",
    "/api/v3/",
]

_HTTP_TIMEOUT = 6.0  # seconds per probe


def _is_openapi_spec(data: object) -> bool:
    return (
        isinstance(data, dict)
        and ("openapi" in data or "swagger" in data)
        and "paths" in data
    )


def _is_api_index(data: object) -> bool:
    """True for simple API index dicts (e.g. SWAPI, Rick & Morty root)."""
    return (
        isinstance(data, dict)
        and bool(data)
        and all(isinstance(v, str) and v.startswith("http") for v in data.values())
    )


async def _probe_url(client: httpx.AsyncClient, url: str) -> dict | None:
    """
    Fetch *url* and return ``{"url": str, "data": dict}`` if it is a valid
    OpenAPI spec or API index. Returns ``None`` otherwise.
    """
    from agents.ingestion_agent.spec_parser import extract_swagger_spec_url

    try:
        resp = await client.get(url, timeout=_HTTP_TIMEOUT, follow_redirects=True)
        if resp.status_code != 200:
            return None

        content_type = resp.headers.get("content-type", "")
        final_url = str(resp.url)

        # JSON
        if "json" in content_type or url.lower().endswith((".json",)):
            try:
                data = resp.json()
                if _is_openapi_spec(data) or _is_api_index(data):
                    return {"url": final_url, "data": data}
            except Exception:
                pass

        # YAML
        if "yaml" in content_type or url.lower().endswith((".yaml", ".yml")):
            try:
                import yaml

                data = yaml.safe_load(resp.text)
                if _is_openapi_spec(data) or _is_api_index(data):
                    return {"url": final_url, "data": data}
            except Exception:
                pass

        # HTML — look for embedded Swagger UI / Redoc spec URL (one level deep)
        if "html" in content_type:
            spec_url = extract_swagger_spec_url(resp.text, url)
            if spec_url:
                inner = await _probe_url(client, spec_url)
                if inner:
                    return inner

    except Exception as exc:
        logger.debug("Probe failed for %s: %s", url, exc)

    return None


def _search_ddg(query: str) -> list[dict]:
    """Synchronous DuckDuckGo search — runs in a thread executor."""
    from ddgs import DDGS

    # Append "API" if the user didn't mention it, so generic terms like
    # "starwars" or "weather" target API docs instead of unrelated content.
    q = query if any(w in query.lower() for w in ("api", "swagger", "openapi")) else f"{query} API"

    all_results: list[dict] = []
    ddgs = DDGS()
    search_queries = [
        f"{q} openapi swagger json spec",
        f"{q} swagger.json OR openapi.json",
    ]
    for sq in search_queries:
        try:
            results = ddgs.text(sq, max_results=8)
            all_results.extend(results or [])
        except Exception as exc:
            logger.warning("DDG search failed for '%s': %s", sq, exc)
    return all_results


class _CandidateSelection(BaseModel):
    """LLM output: ordered list of candidate URLs for the queried API."""

    candidate_urls: list[str] = Field(
        default_factory=list,
        description=(
            "Ordered list of URLs most likely to be the OpenAPI spec or official API "
            "documentation for the queried API. Best candidate first. "
            "Empty list if none of the results match the queried API."
        ),
    )


async def _llm_select_candidates(query: str, results: list[dict]) -> list[str]:
    """
    Ask an LLM to pick the most relevant URLs from DuckDuckGo search results.

    Returns an ordered list of candidate URLs (best first), or an empty list
    if the LLM finds no results genuinely related to the queried API.
    """
    from pydantic_ai import Agent

    model = os.environ.get("INGESTION_AGENT_MODEL", "bedrock:us.amazon.nova-2-lite-v1:0")

    agent = Agent(
        model,
        output_type=_CandidateSelection,
        system_prompt=(
            "You are an API documentation locator. Given a search query for a specific "
            "API and a list of web search results, identify which URLs are most likely to be:\n"
            "  1. The OpenAPI/Swagger spec file for that specific API\n"
            "  2. The official API documentation or developer portal for that specific API\n\n"
            "Rules:\n"
            "- ONLY return URLs genuinely related to the queried API — not to generic tools, "
            "tutorials, or completely unrelated APIs that happened to appear in search results.\n"
            "- If none of the results are about the queried API, return an empty list.\n"
            "- Prefer direct spec file URLs (.json, .yaml) over docs pages.\n"
            "- Return at most 3 candidates, ordered by confidence (best first).\n"
            "- Be conservative: a wrong API is worse than returning nothing."
        ),
    )

    results_text = "\n".join(
        f"[{i + 1}] Title: {r.get('title', '')}\n"
        f"     URL: {r.get('href', '')}\n"
        f"     Snippet: {r.get('body', '')[:300]}"
        for i, r in enumerate(results)
    )

    try:
        result = await agent.run(
            f"Query: '{query}'\n\n"
            f"Search results:\n{results_text}\n\n"
            f"Which URLs are the OpenAPI spec or official docs for '{query}'?"
        )
        urls = result.output.candidate_urls
        logger.info("LLM selected %d candidate(s) for '%s': %s", len(urls), query, urls)
        return urls
    except Exception as exc:
        logger.warning("LLM candidate selection failed: %s", exc)
        return []


def _extract_title(spec: dict, fallback: str) -> str:
    info = spec.get("info", {})
    title = info.get("title", "")
    version = info.get("version", "")
    if title:
        return f"{title} {version}".strip()
    return fallback


async def discover_openapi_url(query: str) -> dict | None:
    """
    Discover the OpenAPI/Swagger spec URL for a free-text *query*.

    Strategy:
    1. Search DuckDuckGo for candidates.
    2. Ask an LLM to filter results to only those relevant to the queried API.
    3. Probe LLM-selected URLs directly, then common spec paths on their hosts.

    Returns ``{"url": str, "title": str}`` on success, ``None`` otherwise.
    """
    all_results = await asyncio.to_thread(_search_ddg, query)
    if not all_results:
        logger.info("No DDG results for query: %s", query)
        return None

    # LLM filters search results to only relevant candidates
    candidate_urls = await _llm_select_candidates(query, all_results)
    if not candidate_urls:
        logger.info("No relevant candidates found for: %s", query)
        return None

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; DataLakeDiscoveryBot/1.0)"},
        follow_redirects=True,
    ) as client:
        for url in candidate_urls:
            # Try the URL directly (may be a spec file or a Swagger UI page)
            result = await _probe_url(client, url)
            if result:
                logger.info("Discovered spec at %s (direct probe)", result["url"])
                return {"url": result["url"], "title": _extract_title(result["data"], query)}

            # URL may be a docs/developer portal page — probe common spec paths on its host
            parsed = urlparse(url)
            if not (parsed.scheme and parsed.netloc):
                continue
            host = f"{parsed.scheme}://{parsed.netloc}"
            for path in _SPEC_PATHS:
                result = await _probe_url(client, host + path)
                if result:
                    logger.info("Discovered spec at %s (path probe on %s)", result["url"], host)
                    return {"url": result["url"], "title": _extract_title(result["data"], query)}

    logger.info("No valid OpenAPI spec found for query: %s", query)
    return None
