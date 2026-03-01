"""OpenAPI URL discovery via DuckDuckGo search + Claude Sonnet reasoning.

Given a free-text API name (e.g. "Projuris", "Star Wars"), this module:
1. Searches DuckDuckGo (no API key required) for the API's docs/spec URL.
2. Uses Claude Sonnet on Bedrock to reason about the search results and pick
   the most likely spec or documentation URL.
3. Probes the chosen URL (and its host) to verify it returns a valid spec.

This replaces the previous Nova Web Grounding approach, which was exclusive
to Amazon Nova 2 models and consistently returned marketing homepages instead
of actual API documentation subdomains.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import urllib.parse
from urllib.parse import urlparse

import boto3
import httpx
from botocore.config import Config
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
    # API index roots (e.g. SWAPI, Rick & Morty)
    "/api/",
    "/api",
    "/api/v1/",
    "/api/v2/",
    "/api/v3/",
]

_HTTP_TIMEOUT = 8.0

# Domains that are API tooling / documentation platforms, not actual API providers.
# Excluding them prevents the fallback prober from wasting requests on e.g.
# SmartBear (owners of Swagger), Stoplight, Postman, etc.
_TOOLING_DOMAINS = frozenset({
    "smartbear.com",
    "swagger.io",
    "openapis.org",
    "stoplight.io",
    "readme.io",
    "readme.com",
    "postman.com",
    "apiary.io",
    "restlet.com",
    "apigee.com",
    "redoc.ly",
    "redocly.com",
    "rapidapi.com",
    "apidog.com",
    "insomnia.rest",
})


def _is_tooling_url(url: str) -> bool:
    """Return True if *url* belongs to a generic API tooling/platform domain."""
    try:
        host = urlparse(url).netloc.lower().lstrip("www.")
        return any(host == d or host.endswith("." + d) for d in _TOOLING_DOMAINS)
    except Exception:
        return False


_DDG_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


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


async def _probe_url(
    client: httpx.AsyncClient, url: str, _depth: int = 0
) -> dict | None:
    """
    Fetch *url* and return ``{"url": str, "data": dict}`` if it is a valid
    OpenAPI spec or API index. Returns ``None`` otherwise.

    *_depth* is internal — callers should never pass it.  It prevents
    infinite recursion when HTML pages return other HTML pages:
    - depth 0: full handling (JSON / YAML / HTML with spec extraction)
    - depth 1: JSON / YAML only — HTML is silently ignored
    """
    from agents.ingestion_agent.spec_parser import extract_swagger_spec_url

    try:
        resp = await client.get(url, timeout=_HTTP_TIMEOUT, follow_redirects=True)
        if resp.status_code != 200:
            return None

        content_type = resp.headers.get("content-type", "")
        final_url = str(resp.url)

        if "json" in content_type or url.lower().endswith((".json",)):
            try:
                data = resp.json()
                if _is_openapi_spec(data) or _is_api_index(data):
                    return {"url": final_url, "data": data}
            except Exception:
                pass

        if "yaml" in content_type or url.lower().endswith((".yaml", ".yml")):
            try:
                import yaml
                data = yaml.safe_load(resp.text)
                if _is_openapi_spec(data) or _is_api_index(data):
                    return {"url": final_url, "data": data}
            except Exception:
                pass

        # HTML handling only at depth 0 — recursive calls must never re-enter
        # this block, otherwise a host that returns HTML for every path causes
        # unbounded recursion until Python's stack limit is hit (~1000 frames).
        if "html" in content_type and _depth == 0:
            spec_url = extract_swagger_spec_url(resp.text, url)
            if spec_url:
                inner = await _probe_url(client, spec_url, _depth=1)
                if inner:
                    return inner

            # Fallback: legacy Swagger UIs (e.g. Springfox / swagger-ui-dist)
            # compute the spec URL dynamically via JavaScript — no literal URL
            # in the HTML to parse.  Probe well-known spec paths on the same
            # host before giving up.
            from urllib.parse import urlparse as _urlparse
            _parsed = _urlparse(final_url)
            _host = f"{_parsed.scheme}://{_parsed.netloc}"
            for _path in ("/v3/api-docs", "/v2/api-docs", "/api-docs",
                          "/openapi.json", "/swagger.json", "/openapi.yaml"):
                _candidate = _host + _path
                if _candidate == url:
                    continue
                _inner = await _probe_url(client, _candidate, _depth=1)
                if _inner:
                    logger.info(
                        "[discovery] HTML at %s — spec found via host probe at %s",
                        url, _candidate,
                    )
                    return _inner

    except Exception as exc:
        logger.warning("[discovery] Probe failed for %s: %s", url, exc)

    return None


# ---------------------------------------------------------------------------
# DuckDuckGo search (no API key)
# ---------------------------------------------------------------------------

async def _duckduckgo_search(query: str, max_results: int = 8) -> list[dict]:
    """
    Search DuckDuckGo for ``query`` and return the top results as
    ``[{"url": str, "title": str, "snippet": str}]``.

    Uses the DuckDuckGo HTML endpoint — no API key needed.
    Result URLs are extracted from ``uddg=<encoded-url>`` href params.
    """
    # "swagger" helps rank the actual Swagger UI page over the generic docs root
    # (e.g. /ui/index.html vs just the domain). SmartBear/swagger.io are blocked
    # by _TOOLING_DOMAINS so they won't pollute the results.
    search_query = f"{query} REST API documentation openapi swagger"
    params = urllib.parse.urlencode({"q": search_query})

    try:
        async with httpx.AsyncClient(
            headers=_DDG_HEADERS,
            follow_redirects=True,
            timeout=15.0,
        ) as client:
            resp = await client.get(f"https://html.duckduckgo.com/html/?{params}")
            if resp.status_code not in (200, 202):
                logger.warning(
                    "[discovery] DuckDuckGo returned HTTP %s", resp.status_code
                )
                return []

            if resp.status_code == 202:
                logger.warning(
                    "[discovery] DuckDuckGo returned HTTP 202 (bot-check) — "
                    "attempting to parse body anyway"
                )

            html = resp.text

            # Extract (encoded_url, title) pairs from result__a anchors
            title_re = re.compile(
                r'<a[^>]+class="result__a"[^>]+href="[^"]*uddg=([^&"]+)[^"]*"[^>]*>'
                r'(.*?)</a>',
                re.IGNORECASE | re.DOTALL,
            )
            # Extract snippets from result__snippet anchors
            snippet_re = re.compile(
                r'<a[^>]+class="result__snippet"[^>]*>(.*?)</a>',
                re.IGNORECASE | re.DOTALL,
            )

            title_matches = title_re.findall(html)
            snippet_matches = [
                re.sub(r"<[^>]+>", "", s).strip()
                for s in snippet_re.findall(html)
            ]

            results: list[dict] = []
            for i, (encoded_url, raw_title) in enumerate(title_matches):
                if len(results) >= max_results:
                    break
                try:
                    url = urllib.parse.unquote(encoded_url)
                    if not url.startswith("http"):
                        continue
                    if _is_tooling_url(url):
                        logger.debug("[discovery] DDG skip tooling domain: %s", url)
                        continue
                    title = re.sub(r"<[^>]+>", "", raw_title).strip()
                    snippet = snippet_matches[i] if i < len(snippet_matches) else ""
                    results.append({"url": url, "title": title, "snippet": snippet})
                except Exception:
                    pass

            logger.info(
                "[discovery] DuckDuckGo returned %d results for '%s': %s",
                len(results), query, [r["url"] for r in results],
            )
            return results

    except Exception as exc:
        logger.warning("[discovery] DuckDuckGo search failed for '%s': %s", query, exc)
        return []


# ---------------------------------------------------------------------------
# Claude Sonnet reasoning — picks the best URL from search results
# ---------------------------------------------------------------------------

class _DiscoveryResult(BaseModel):
    spec_url: str | None = Field(
        default=None,
        description="Direct URL of the OpenAPI spec file, API index, or Swagger UI page.",
    )
    title: str | None = Field(
        default=None,
        description="Human-readable API name.",
    )
    candidate_host: str | None = Field(
        default=None,
        description="Base host to probe with common spec paths when spec_url is null.",
    )


_CLAUDE_PROMPT = """\
You are helping find the OpenAPI/Swagger documentation URL for the '{query}' API.

Here are web search results:
{results_json}

Analyze these results and identify the URL most likely to be:
1. A direct OpenAPI spec file (swagger.json, openapi.json, /v3/api-docs, /api-docs)
2. A Swagger UI or Redoc HTML documentation page (/swagger-ui.html, /ui/index.html, /docs)
3. An API index endpoint that returns JSON with resource URLs

Prefer:
- Subdomains like docs., api., developer., swagger.
- Paths containing api-docs, openapi, swagger, developer in the URL

Avoid:
- Marketing homepages (e.g. www.company.com with no API path)
- Blog posts or tutorials about the API

Return ONLY this JSON — no markdown, no explanation:
{{"spec_url": "https://...", "title": "API Name", "candidate_host": null}}

Rules:
- spec_url: the best candidate URL found (null if nothing looks like API docs)
- title: human-readable API name from the search results
- candidate_host: scheme://host to probe with common spec paths when spec_url is null
"""


_CLAUDE_KNOWLEDGE_PROMPT = """\
You are helping find the OpenAPI/Swagger documentation URL for the '{query}' API.
Web search is unavailable. Use your training knowledge to answer.

Return ONLY this JSON — no markdown, no explanation:
{{"spec_url": "https://...", "title": "API Name", "candidate_host": "https://..."}}

Rules:
- spec_url: the most likely direct URL for the OpenAPI spec file or Swagger UI
- title: human-readable API name
- candidate_host: scheme://host to probe with common spec paths (e.g. /swagger.json)
- Return null for any field you are not confident about
"""


def _claude_knowledge_fallback_sync(query: str) -> _DiscoveryResult:
    """
    When web search is unavailable, ask Claude to recall the API docs URL
    from its training knowledge.
    """
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    model_id = os.environ.get(
        "DISCOVERY_MODEL_ID",
        "us.anthropic.claude-3-5-sonnet-20241022-v2:0",
    )
    bedrock = boto3.client(
        "bedrock-runtime",
        region_name=region,
        config=Config(read_timeout=30, connect_timeout=10),
    )
    prompt = _CLAUDE_KNOWLEDGE_PROMPT.format(query=query)
    response = bedrock.converse(
        modelId=model_id,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
    )
    text_parts = [
        c["text"]
        for c in response.get("output", {}).get("message", {}).get("content", [])
        if "text" in c
    ]
    raw_text = "\n".join(text_parts)
    logger.info("[discovery] Claude knowledge fallback for '%s': %.400s", query, raw_text)
    match = re.search(r'\{[^{}]*"spec_url"[^{}]*\}', raw_text, re.DOTALL)
    if match:
        result = _DiscoveryResult.model_validate(json.loads(match.group()))
        logger.info(
            "[discovery] Claude knowledge picked: spec_url=%s candidate=%s",
            result.spec_url, result.candidate_host,
        )
        return result
    return _DiscoveryResult()


def _claude_pick_url_sync(query: str, results: list[dict]) -> _DiscoveryResult:
    """
    Use Claude Sonnet on Bedrock to reason about DuckDuckGo search results
    and return the most likely OpenAPI spec or documentation URL.

    Runs synchronously — call via asyncio.to_thread from async context.
    """
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    model_id = os.environ.get(
        "DISCOVERY_MODEL_ID",
        "us.anthropic.claude-3-5-sonnet-20241022-v2:0",
    )

    bedrock = boto3.client(
        "bedrock-runtime",
        region_name=region,
        config=Config(read_timeout=30, connect_timeout=10),
    )

    prompt = _CLAUDE_PROMPT.format(
        query=query,
        results_json=json.dumps(results, ensure_ascii=False, indent=2),
    )

    response = bedrock.converse(
        modelId=model_id,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
    )

    text_parts = [
        c["text"]
        for c in response.get("output", {}).get("message", {}).get("content", [])
        if "text" in c
    ]
    raw_text = "\n".join(text_parts)
    logger.info("[discovery] Claude reasoning for '%s': %.400s", query, raw_text)

    match = re.search(r'\{[^{}]*"spec_url"[^{}]*\}', raw_text, re.DOTALL)
    if match:
        result = _DiscoveryResult.model_validate(json.loads(match.group()))
        logger.info(
            "[discovery] Claude picked: spec_url=%s candidate=%s",
            result.spec_url, result.candidate_host,
        )
        return result

    logger.warning("[discovery] Claude returned no parseable JSON for '%s'", query)
    return _DiscoveryResult()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

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

    Uses DuckDuckGo (no API key) for web search and Claude Sonnet on Bedrock
    to reason about which result is the real API documentation URL.

    Returns ``{"url": str, "title": str}`` on success, ``None`` otherwise.
    """
    # Step 1: Search DuckDuckGo
    results = await _duckduckgo_search(query)

    # Step 2: Claude reasons about which URL is the spec/docs.
    # When DDG is blocked (returns 202 / empty), fall back to Claude's
    # training knowledge to suggest a candidate URL.
    if results:
        try:
            discovery = await asyncio.to_thread(_claude_pick_url_sync, query, results)
        except Exception as exc:
            logger.error("[discovery] Claude reasoning failed for '%s': %s", query, exc)
            discovery = _DiscoveryResult()
    else:
        logger.info(
            "[discovery] No DDG results for '%s' — using Claude knowledge fallback",
            query,
        )
        try:
            discovery = await asyncio.to_thread(_claude_knowledge_fallback_sync, query)
        except Exception as exc:
            logger.error("[discovery] Claude knowledge fallback failed for '%s': %s", query, exc)
            return None

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; DataLakeDiscoveryBot/1.0)"},
        follow_redirects=True,
    ) as client:

        # Case A: Claude returned a specific spec/docs URL → verify it
        if discovery.spec_url:
            result = await _probe_url(client, discovery.spec_url)
            if result:
                title = discovery.title or _extract_title(result["data"], query)
                logger.info("[discovery] Verified spec at %s", result["url"])
                return {"url": result["url"], "title": title}

            # URL suggested but probe failed → try common paths on that host
            parsed = urlparse(discovery.spec_url)
            if parsed.scheme and parsed.netloc:
                host = f"{parsed.scheme}://{parsed.netloc}"
                for path in _SPEC_PATHS:
                    result = await _probe_url(client, host + path)
                    if result:
                        logger.info(
                            "[discovery] Found spec at %s (path probe on Claude pick)",
                            result["url"],
                        )
                        return {
                            "url": result["url"],
                            "title": _extract_title(result["data"], query),
                        }

        # Case B: Claude returned only a candidate host → probe common paths
        if discovery.candidate_host:
            host = discovery.candidate_host.rstrip("/")
            logger.info("[discovery] Probing candidate host: %s", host)
            for path in _SPEC_PATHS:
                result = await _probe_url(client, host + path)
                if result:
                    return {
                        "url": result["url"],
                        "title": _extract_title(result["data"], query),
                    }

        # Case C: Claude found nothing useful → probe all DDG result URLs directly
        logger.info(
            "[discovery] Claude found no URL for '%s' — probing all DDG results",
            query,
        )
        for ddg_result in results:
            result = await _probe_url(client, ddg_result["url"])
            if result:
                title = _extract_title(result["data"], query) or ddg_result.get("title", query)
                logger.info(
                    "[discovery] Found spec at %s (DDG fallback)", result["url"]
                )
                return {"url": result["url"], "title": title}

    logger.info("[discovery] No spec found for query: %s", query)
    return None
