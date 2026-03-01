"""OpenAPI URL discovery via Amazon Nova Web Grounding.

Given a free-text API name (e.g. "Projuris", "Star Wars"), this module:
1. Asks Nova 2 Lite (with nova_grounding system tool) to find the spec URL.
2. Probes the returned URL (or candidate host) to verify it's a real spec.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
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

        if "html" in content_type:
            spec_url = extract_swagger_spec_url(resp.text, url)
            if spec_url:
                inner = await _probe_url(client, spec_url)
                if inner:
                    return inner

    except Exception as exc:
        logger.warning("[discovery] Probe failed for %s: %s", url, exc)

    return None


# ---------------------------------------------------------------------------
# Nova Web Grounding
# ---------------------------------------------------------------------------

class _DiscoveryResult(BaseModel):
    spec_url: str | None = Field(
        default=None,
        description="Direct URL of the OpenAPI spec file or API index root.",
    )
    title: str | None = Field(
        default=None,
        description="Human-readable API name.",
    )
    candidate_host: str | None = Field(
        default=None,
        description="Base host to probe with common spec paths when spec_url is null.",
    )


_DISCOVERY_PROMPT = """\
Search the web for the OpenAPI/Swagger spec URL of the '{query}' API.

Look for:
1. Direct spec files: swagger.json, openapi.json, /v3/api-docs, /api-docs
2. API index JSON (e.g. SWAPI: https://swapi.dev/api/ returns {{"people":"url",...}})
3. Developer portal pages that link to the spec URL

Return ONLY this JSON object — no markdown, no explanation:
{{"spec_url": "https://...", "title": "API Name", "candidate_host": null}}

Rules:
- spec_url: direct URL to the spec file or API index root (null if not found)
- title: human-readable API name
- candidate_host: base URL (scheme://host) to probe if spec_url is null
"""


def _nova_grounding_sync(query: str) -> _DiscoveryResult:
    """Call Nova Web Grounding synchronously (runs in thread pool)."""
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    model_id = os.environ.get("INGESTION_AGENT_MODEL_ID", "us.amazon.nova-2-lite-v1:0")

    client = boto3.client(
        "bedrock-runtime",
        region_name=region,
        config=Config(read_timeout=60, connect_timeout=10),
    )

    response = client.converse(
        modelId=model_id,
        messages=[{
            "role": "user",
            "content": [{"text": _DISCOVERY_PROMPT.format(query=query)}],
        }],
        toolConfig={"tools": [{"systemTool": {"name": "nova_grounding"}}]},
    )

    text_parts = [
        c["text"]
        for c in response.get("output", {}).get("message", {}).get("content", [])
        if "text" in c
    ]
    raw_text = "\n".join(text_parts)
    logger.info("[discovery] Nova Grounding raw for '%s': %.400s", query, raw_text)

    match = re.search(r'\{[^{}]*"spec_url"[^{}]*\}', raw_text, re.DOTALL)
    if match:
        result = _DiscoveryResult.model_validate(json.loads(match.group()))
        logger.info(
            "[discovery] Nova Grounding result: spec_url=%s candidate=%s",
            result.spec_url, result.candidate_host,
        )
        return result

    logger.warning("[discovery] Nova Grounding returned no parseable JSON for '%s'", query)
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

    Uses Amazon Nova Web Grounding (nova_grounding system tool) to search
    the web natively — no external API keys, no DuckDuckGo rate limits.

    Returns ``{"url": str, "title": str}`` on success, ``None`` otherwise.
    """
    try:
        discovery = await asyncio.to_thread(_nova_grounding_sync, query)
    except Exception as exc:
        logger.error("[discovery] Nova Grounding failed for '%s': %s", query, exc)
        return None

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; DataLakeDiscoveryBot/1.0)"},
        follow_redirects=True,
    ) as client:

        # Case A: grounding returned a direct spec URL → verify it
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
                        logger.info("[discovery] Found spec at %s (path probe)", result["url"])
                        return {"url": result["url"], "title": _extract_title(result["data"], query)}

        # Case B: only a candidate host → probe common spec paths
        if discovery.candidate_host:
            host = discovery.candidate_host.rstrip("/")
            logger.info("[discovery] Probing candidate host: %s", host)
            for path in _SPEC_PATHS:
                result = await _probe_url(client, host + path)
                if result:
                    logger.info("[discovery] Found spec at %s", result["url"])
                    return {"url": result["url"], "title": _extract_title(result["data"], query)}

    logger.info("[discovery] No spec found for query: %s", query)
    return None
