"""OpenAPI URL discovery via web search + agentic browsing.

Given a free-text API name (e.g. "Projuris", "Stripe"), this module:
1. Searches DuckDuckGo for candidate URLs.
2. Runs a Strands agent WITH a fetch_page tool so it can actively browse
   candidate pages (docs portals, Swagger UIs, API indexes) to find the
   actual OpenAPI spec URL — not just guess from snippets.
3. Falls back to probing common spec paths on the selected host if the
   agent cannot find the URL directly.
"""

from __future__ import annotations

import asyncio
import logging
import os
from urllib.parse import urlparse

import httpx
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# Common OpenAPI spec paths to probe on a candidate host (fallback only)
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

_HTTP_TIMEOUT = 8.0  # seconds per probe


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
        logger.warning("[discovery] Probe failed for %s: %s", url, exc)

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


def _fetch_page_content(url: str, max_chars: int = 4000) -> str:
    """
    Synchronous HTTP fetch that returns page content as plain text.

    Used as a Strands @tool so the discovery agent can actively browse
    candidate pages (docs portals, Swagger UIs, API root indexes) to find
    the embedded OpenAPI spec URL rather than guessing from DDG snippets.

    Returns a short, LLM-readable representation:
    - JSON responses: raw JSON (truncated)
    - HTML responses: extracted text with script/style tags stripped
    - Other: first max_chars chars of body
    """
    import re as _re

    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=_HTTP_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0 (compatible; DataLakeDiscoveryBot/1.0)"},
        ) as client:
            resp = client.get(url)

        if resp.status_code != 200:
            return f"HTTP {resp.status_code} for {url}"

        content_type = resp.headers.get("content-type", "")
        final_url = str(resp.url)
        prefix = f"[fetched: {final_url}]\n"

        if "json" in content_type or url.lower().endswith((".json", ".yaml", ".yml")):
            return prefix + resp.text[:max_chars]

        if "html" in content_type or resp.text.lstrip().startswith(("<!DOCTYPE", "<html")):
            html = resp.text
            # Strip <script> and <style> blocks — keep JS inline that configures Swagger
            # but remove large library bundles. We keep short inline scripts.
            html = _re.sub(r"<script\b[^>]*\bsrc\b[^>]*>.*?</script>", "", html,
                           flags=_re.DOTALL | _re.IGNORECASE)
            html = _re.sub(r"<style[^>]*>.*?</style>", "", html,
                           flags=_re.DOTALL | _re.IGNORECASE)
            # Keep inline <script> blocks (contain SwaggerUIBundle config)
            # but strip all other HTML tags
            text = _re.sub(r"<(?!script|/script)[^>]+>", " ", html)
            text = _re.sub(r"\s+", " ", text).strip()
            return prefix + text[:max_chars]

        return prefix + resp.text[:max_chars]

    except Exception as exc:
        return f"Error fetching {url}: {exc}"


class _DiscoveryResult(BaseModel):
    """LLM output from the agentic discovery loop."""

    spec_url: str | None = Field(
        default=None,
        description=(
            "The direct URL of the OpenAPI/Swagger spec file or API index root. "
            "Must be a URL that returns JSON or YAML when fetched directly. "
            "Null if no spec could be found."
        ),
    )
    title: str | None = Field(
        default=None,
        description="Human-readable name of the API, extracted from spec info.title or page title.",
    )
    candidate_host: str | None = Field(
        default=None,
        description=(
            "If spec_url is null, the base host (scheme://host) of the most "
            "likely API so the caller can probe common spec paths. "
            "E.g. 'https://api.example.com'. Null if nothing was found."
        ),
    )


def _run_discovery_agent_sync(query: str, results: list[dict]) -> _DiscoveryResult:
    """
    Strands agent with a fetch_page tool that actively browses candidate pages
    to discover the OpenAPI spec URL.

    Unlike the old approach (LLM picks from snippets → code probes),
    this agent can READ the actual page content:
    - Swagger UI HTML → finds SwaggerUIBundle URL parameter
    - API index JSON (SWAPI, Rick & Morty) → returns that URL directly
    - Developer portal → navigates to the spec link

    Runs synchronously (called via asyncio.to_thread from async context).
    """
    import json
    import re

    from strands import Agent, tool
    from strands.models import BedrockModel

    model_id = os.environ.get("INGESTION_AGENT_MODEL_ID", "us.amazon.nova-2-lite-v1:0")

    @tool
    def fetch_page(url: str) -> str:
        """Fetch a URL and return its content as text. Use this to verify if
        a page is an OpenAPI spec, API index, or Swagger UI that embeds a spec URL.

        Args:
            url: The URL to fetch (any HTTP/HTTPS URL).

        Returns:
            Page content as plain text (JSON kept as-is, HTML stripped of tags
            but inline <script> blocks preserved to expose SwaggerUIBundle config).
        """
        logger.info("[discovery-agent] Fetching: %s", url)
        return _fetch_page_content(url)

    agent = Agent(
        model=BedrockModel(model_id=model_id),
        tools=[fetch_page],
        system_prompt=(
            "You are an OpenAPI spec locator. Your job is to find the actual "
            "OpenAPI/Swagger spec URL for a given API.\n\n"
            "You have a fetch_page tool — use it actively:\n"
            "1. Scan search results for direct spec file URLs (.json/.yaml) or "
            "developer portal / docs URLs.\n"
            "2. For promising pages, call fetch_page(url) to read the content:\n"
            "   - If it returns JSON with 'openapi'/'swagger'/'paths' keys → that IS the spec.\n"
            "   - If it returns JSON where all values are URLs → that is an API index root "
            "(treat it as the spec URL).\n"
            "   - If it returns HTML containing 'SwaggerUIBundle' or 'spec-url' or 'Redoc' "
            "→ look for the url: '...' parameter inside the script and fetch THAT URL.\n"
            "3. Try at most 4 fetch_page calls to avoid timeouts.\n\n"
            "When you find the spec URL, respond with ONLY this JSON (no markdown):\n"
            '{"spec_url": "https://...", "title": "API Name", "candidate_host": null}\n\n'
            "If you find a likely API host but not the exact spec URL:\n"
            '{"spec_url": null, "title": null, "candidate_host": "https://api.example.com"}\n\n'
            "If nothing is found:\n"
            '{"spec_url": null, "title": null, "candidate_host": null}'
        ),
    )

    results_text = "\n".join(
        f"[{i + 1}] Title: {r.get('title', '')}\n"
        f"     URL: {r.get('href', '')}\n"
        f"     Snippet: {r.get('body', '')[:200]}"
        for i, r in enumerate(results)
    )

    result = agent(
        f"Find the OpenAPI spec for: '{query}'\n\n"
        f"Web search results:\n{results_text}\n\n"
        f"Use fetch_page to verify and locate the spec. "
        f"Return JSON with spec_url, title, and candidate_host."
    )

    try:
        match = re.search(r'\{[^{}]*"spec_url"[^{}]*\}', str(result), re.DOTALL)
        if match:
            data = json.loads(match.group())
            discovery = _DiscoveryResult.model_validate(data)
            logger.info(
                "[discovery-agent] Result for '%s': spec_url=%s candidate_host=%s",
                query, discovery.spec_url, discovery.candidate_host,
            )
            return discovery
    except Exception as exc:
        logger.warning(
            "Failed to parse discovery agent result: %s | raw: %.300s", exc, str(result)
        )

    return _DiscoveryResult()


async def _run_discovery_agent(query: str, results: list[dict]) -> _DiscoveryResult:
    """Async wrapper — runs the synchronous Strands agent in a thread pool."""
    try:
        return await asyncio.to_thread(_run_discovery_agent_sync, query, results)
    except Exception as exc:
        logger.warning("Discovery agent failed: %s", exc)
        return _DiscoveryResult()


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
    2. Run a Strands agent with a fetch_page tool — the agent actively browses
       candidate pages (Swagger UI HTML, API index JSON, docs portals) to find
       the spec URL instead of guessing from snippets.
    3. If the agent found a spec_url directly → probe it to verify + extract title.
    4. If the agent found only a candidate_host → probe common spec paths on it.
    5. Keyword fallback if the agent found nothing.

    Returns ``{"url": str, "title": str}`` on success, ``None`` otherwise.
    """
    all_results = await asyncio.to_thread(_search_ddg, query)
    if not all_results:
        logger.info("[discovery] No DDG results for query: %s", query)
        return None

    logger.info(
        "[discovery] DDG returned %d results for '%s': %s",
        len(all_results),
        query,
        [r.get("href", "") for r in all_results],
    )

    # Agentic discovery: agent can browse pages to find the spec URL
    discovery = await _run_discovery_agent(query, all_results)

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; DataLakeDiscoveryBot/1.0)"},
        follow_redirects=True,
    ) as client:

        # Case 1: agent found a direct spec URL → verify it
        if discovery.spec_url:
            result = await _probe_url(client, discovery.spec_url)
            if result:
                logger.info("[discovery] Verified spec at %s (agent direct)", result["url"])
                title = discovery.title or _extract_title(result["data"], query)
                return {"url": result["url"], "title": title}
            # Agent was confident but probe failed — try common paths on that host
            logger.info(
                "[discovery] Agent spec_url %s failed probe, trying host paths",
                discovery.spec_url,
            )
            parsed = urlparse(discovery.spec_url)
            host = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else None
            if host:
                for path in _SPEC_PATHS:
                    result = await _probe_url(client, host + path)
                    if result:
                        logger.info("[discovery] Found spec at %s (path probe)", result["url"])
                        return {"url": result["url"], "title": _extract_title(result["data"], query)}

        # Case 2: agent found a candidate host but not the spec URL → probe paths
        if discovery.candidate_host:
            host = discovery.candidate_host.rstrip("/")
            logger.info("[discovery] Probing common paths on agent candidate: %s", host)
            for path in _SPEC_PATHS:
                result = await _probe_url(client, host + path)
                if result:
                    logger.info("[discovery] Found spec at %s (path probe on candidate)", result["url"])
                    return {"url": result["url"], "title": _extract_title(result["data"], query)}

        # Case 3: agent found nothing → keyword fallback from DDG results
        logger.info("[discovery] Agent found nothing, trying keyword fallback")
        keyword = query.lower().split()[0]
        fallback_urls = [
            r["href"]
            for r in all_results
            if keyword in r.get("href", "").lower() or keyword in r.get("title", "").lower()
            if r.get("href")
        ][:3]

        for url in fallback_urls:
            result = await _probe_url(client, url)
            if result:
                logger.info("[discovery] Found spec at %s (keyword fallback)", result["url"])
                return {"url": result["url"], "title": _extract_title(result["data"], query)}

            parsed = urlparse(url)
            if not (parsed.scheme and parsed.netloc):
                continue
            host = f"{parsed.scheme}://{parsed.netloc}"
            for path in _SPEC_PATHS:
                result = await _probe_url(client, host + path)
                if result:
                    logger.info("[discovery] Found spec at %s (fallback path probe)", result["url"])
                    return {"url": result["url"], "title": _extract_title(result["data"], query)}

    logger.info("[discovery] No valid OpenAPI spec found for query: %s", query)
    return None
