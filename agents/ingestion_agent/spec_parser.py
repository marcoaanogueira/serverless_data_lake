"""
OpenAPI spec parsing utilities.

Pure functions for parsing, summarizing, and resolving references in
OpenAPI/Swagger specifications. No external AI dependencies.
"""

from __future__ import annotations

import json


def build_spec_summary(spec: dict, source_url: str | None = None) -> str:
    """
    Build a compact summary of the OpenAPI spec for LLM consumption.

    Extracts only the relevant parts: info, servers, paths (with schemas),
    and security definitions. This avoids sending the full spec when it's
    very large.

    Args:
        spec: Parsed OpenAPI/Swagger spec dictionary.
        source_url: Original URL the spec was fetched from. Used as fallback
            to derive the base_url when the spec lacks servers/host fields.
    """
    summary_parts = []

    # API info
    info = spec.get("info", {})
    summary_parts.append(f"API: {info.get('title', 'Unknown')} v{info.get('version', '?')}")
    if info.get("description"):
        summary_parts.append(f"Description: {info['description'][:500]}")

    # Source URL (so the LLM can derive base_url when the spec lacks servers)
    if source_url:
        summary_parts.append(f"Source URL (where this spec was fetched from): {source_url}")

    # Servers / base URL
    servers = spec.get("servers", [])
    if servers:
        summary_parts.append(f"Servers: {json.dumps(servers)}")
    elif spec.get("host"):
        scheme = (spec.get("schemes") or ["https"])[0]
        base_path = spec.get("basePath", "")
        summary_parts.append(f"Base URL: {scheme}://{spec['host']}{base_path}")

    # Security schemes
    security_schemes = (
        spec.get("components", {}).get("securitySchemes")
        or spec.get("securityDefinitions")
        or {}
    )
    if security_schemes:
        summary_parts.append(f"Security Schemes: {json.dumps(security_schemes)}")

    # Paths with operations
    paths = spec.get("paths", {})
    for path, operations in paths.items():
        for method, operation in operations.items():
            if method.lower() not in ("get", "post", "put", "patch", "delete"):
                continue
            op_summary = operation.get("summary", operation.get("operationId", ""))
            op_desc = operation.get("description", "")[:200]
            params = [
                {"name": p.get("name"), "in": p.get("in"), "required": p.get("required")}
                for p in operation.get("parameters", [])
            ]

            # Extract response schema reference
            responses = operation.get("responses", {})
            response_200 = responses.get("200", responses.get("201", {}))
            response_schema = None
            content = response_200.get("content", {})
            if content:
                json_content = content.get("application/json", {})
                response_schema = json_content.get("schema")
            elif "schema" in response_200:
                response_schema = response_200["schema"]

            entry = {
                "path": path,
                "method": method.upper(),
                "summary": op_summary,
                "description": op_desc,
                "parameters": params,
            }
            if response_schema:
                entry["response_schema"] = simplify_schema(response_schema, spec)

            summary_parts.append(json.dumps(entry))

    return "\n".join(summary_parts)


def simplify_schema(schema: dict, root_spec: dict, depth: int = 0) -> dict:
    """Resolve $ref and simplify schema to a manageable size."""
    if depth > 3:
        return {"type": "object", "note": "truncated"}

    if "$ref" in schema:
        ref_path = schema["$ref"]
        resolved = resolve_ref(ref_path, root_spec)
        if resolved:
            return simplify_schema(resolved, root_spec, depth + 1)
        return {"$ref": ref_path}

    result: dict = {}
    if "type" in schema:
        result["type"] = schema["type"]
    if "properties" in schema:
        result["properties"] = {
            k: simplify_schema(v, root_spec, depth + 1)
            for k, v in list(schema["properties"].items())[:20]
        }
    if "items" in schema:
        result["items"] = simplify_schema(schema["items"], root_spec, depth + 1)
    if "required" in schema:
        result["required"] = schema["required"]

    return result


def resolve_ref(ref: str, spec: dict) -> dict | None:
    """Resolve a JSON $ref pointer within the spec."""
    if not ref.startswith("#/"):
        return None
    parts = ref[2:].split("/")
    current = spec
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current if isinstance(current, dict) else None
