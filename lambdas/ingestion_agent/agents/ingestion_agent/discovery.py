"""OpenAPI URL discovery via web search.

Given a free-text API name (e.g. "Stripe", "PokeAPI"), this module searches
DuckDuckGo for candidate OpenAPI/Swagger specs, probes common URL patterns on
each host, validates each candidate, and returns the single best result — or
None if no valid spec is found.
"""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

# Common OpenAPI spec paths to probe on each candidate host
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

# URL fragments that strongly suggest an OpenAPI spec path
_SPEC_FRAGMENTS = ("/openapi", "/swagger", "/api-docs")

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


def _candidate_hosts(results: list[dict]) -> list[str]:
    """Extract unique base hosts from search results, preserving order."""
    seen: set[str] = set()
    hosts: list[str] = []
    for r in results:
        href = r.get("href", "")
        if not href:
            continue
        try:
            parsed = urlparse(href)
            host = f"{parsed.scheme}://{parsed.netloc}"
            if host not in seen:
                seen.add(host)
                hosts.append(host)
        except Exception:
            continue
    return hosts


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
    1. Search DuckDuckGo for ``"{query} openapi swagger json spec"``.
    2. Probe direct URLs from results that look like spec files.
    3. Probe common spec paths on the top candidate hosts.

    Returns ``{"url": str, "title": str}`` on success, ``None`` otherwise.
    """
    all_results = await asyncio.to_thread(_search_ddg, query)
    if not all_results:
        logger.info("No DDG results for query: %s", query)
        return None

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; DataLakeDiscoveryBot/1.0)"},
        follow_redirects=True,
    ) as client:
        # --- Pass 1: probe direct URLs from search results that look like specs ---
        for r in all_results:
            href = r.get("href", "")
            if not href:
                continue
            looks_like_spec = href.lower().endswith(
                (".json", ".yaml", ".yml")
            ) or any(frag in href for frag in _SPEC_FRAGMENTS)
            if not looks_like_spec:
                continue
            result = await _probe_url(client, href)
            if result:
                logger.info("Discovered spec at %s (direct URL)", result["url"])
                return {"url": result["url"], "title": _extract_title(result["data"], query)}

        # --- Pass 2: probe common spec paths on top candidate hosts ---
        hosts = _candidate_hosts(all_results)
        for host in hosts[:5]:
            for path in _SPEC_PATHS:
                result = await _probe_url(client, host + path)
                if result:
                    logger.info("Discovered spec at %s (path probe)", result["url"])
                    return {"url": result["url"], "title": _extract_title(result["data"], query)}

    logger.info("No valid OpenAPI spec found for query: %s", query)
    return None
