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
    if paths:
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
    elif not spec.get("openapi") and not spec.get("swagger"):
        # Not a formal OpenAPI spec — likely an API index (e.g., Rick and Morty,
        # SWAPI) where keys are resource names and values are endpoint URLs.
        from urllib.parse import urlparse

        # Collect endpoint URLs and compute common base
        url_entries: list[tuple[str, str]] = []
        other_entries: list[tuple[str, Any]] = []
        for key, value in spec.items():
            if isinstance(value, str) and value.startswith("http"):
                url_entries.append((key, value))
            else:
                other_entries.append((key, value))

        # Derive base_url and relative paths from the endpoint URLs
        derived_base = ""
        if url_entries:
            parsed_urls = [urlparse(url) for _, url in url_entries]
            host_part = f"{parsed_urls[0].scheme}://{parsed_urls[0].netloc}"

            # Find common path prefix by splitting into segments
            all_segments = [p.path.strip("/").split("/") for p in parsed_urls]
            common_segs: list[str] = []
            for parts in zip(*all_segments):
                if len(set(parts)) == 1:
                    common_segs.append(parts[0])
                else:
                    break
            common_path = "/" + "/".join(common_segs) if common_segs else ""
            derived_base = host_part + common_path

        summary_parts.append(
            "\n--- API Index (not a formal OpenAPI spec) ---"
        )
        if derived_base:
            summary_parts.append(f"Derived base_url: {derived_base}")
        summary_parts.append(
            "IMPORTANT: Use the RELATIVE paths below as endpoint paths "
            "combined with the derived base_url above. "
            "Do NOT duplicate path segments that are already in the base_url."
        )

        for key, url in url_entries:
            parsed = urlparse(url)
            rel_path = parsed.path
            if derived_base:
                common_path_prefix = urlparse(derived_base).path
                if rel_path.startswith(common_path_prefix):
                    rel_path = rel_path[len(common_path_prefix):]
                    if not rel_path.startswith("/"):
                        rel_path = "/" + rel_path
            summary_parts.append(
                json.dumps({
                    "resource_key": key,
                    "path": rel_path,
                })
            )

        for key, value in other_entries:
            summary_parts.append(f"  {key}: {json.dumps(value)}")

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


def _resolve_schema_deep(schema: dict, spec: dict, depth: int = 0) -> dict:
    """Resolve $ref pointers recursively, returning the fully resolved schema."""
    if depth > 5:
        return schema
    if "$ref" in schema:
        resolved = resolve_ref(schema["$ref"], spec)
        if resolved:
            return _resolve_schema_deep(resolved, spec, depth + 1)
    return schema


def extract_field_descriptions(
    spec: dict,
    path: str,
    method: str = "GET",
) -> dict[str, str]:
    """
    Extract field-level descriptions from an OpenAPI response schema.

    Navigates the spec to find the 200/201 response schema for the given
    path and method, resolves $ref pointers, and extracts the ``description``
    attribute from each property.

    For array responses (``type: array`` with ``items``), the properties
    are extracted from the items schema.  For wrapper objects that contain
    an array property (e.g., ``{results: [{...}]}``), it attempts to find
    the data array and extract descriptions from its item schema.

    Args:
        spec: Parsed OpenAPI/Swagger specification dictionary.
        path: The API path (e.g., ``/pets``, ``/api/v1/orders``).
        method: HTTP method (default ``GET``).

    Returns:
        A dict mapping field names to their descriptions.  Only fields
        that have a non-empty ``description`` in the spec are included.
    """
    paths = spec.get("paths", {})
    path_obj = paths.get(path, {})
    operation = path_obj.get(method.lower(), {})

    if not operation:
        return {}

    # Find the 200 or 201 response schema
    responses = operation.get("responses", {})
    response_obj = responses.get("200", responses.get("201", {}))
    if not response_obj:
        return {}

    # Extract schema from OpenAPI 3.x content or Swagger 2.x schema
    schema = None
    content = response_obj.get("content", {})
    if content:
        json_content = content.get("application/json", {})
        schema = json_content.get("schema")
    elif "schema" in response_obj:
        schema = response_obj["schema"]

    if not schema:
        return {}

    # Resolve top-level $ref
    schema = _resolve_schema_deep(schema, spec)

    # Extract descriptions from the properties of the record schema
    return _extract_descriptions_from_schema(schema, spec)


def _extract_descriptions_from_schema(
    schema: dict,
    spec: dict,
) -> dict[str, str]:
    """
    Extract field descriptions from a resolved schema.

    Handles three common patterns:
      1. Direct object with properties → extract from properties
      2. Array with items → extract from items' properties
      3. Wrapper object (e.g., pagination envelope) → find the first
         array-of-objects property and extract from its items
    """
    schema = _resolve_schema_deep(schema, spec)
    schema_type = schema.get("type", "")

    # Case 1: Direct object with properties
    if schema_type == "object" and "properties" in schema:
        # Check if this is a wrapper object containing a data array
        # (e.g., {count, next, results: [...]})
        array_props = {}
        for prop_name, prop_schema in schema["properties"].items():
            resolved_prop = _resolve_schema_deep(prop_schema, spec)
            if resolved_prop.get("type") == "array" and "items" in resolved_prop:
                array_props[prop_name] = resolved_prop

        if array_props:
            # Prefer well-known data keys
            _preferred = [
                "results", "data", "items", "records", "entries",
                "content", "hits", "objects", "rows", "values",
            ]
            chosen = None
            for key in _preferred:
                if key in array_props:
                    chosen = array_props[key]
                    break
            if chosen is None:
                chosen = next(iter(array_props.values()))

            items_schema = _resolve_schema_deep(chosen.get("items", {}), spec)
            if items_schema.get("properties"):
                return _descriptions_from_properties(items_schema["properties"], spec)

        # No array wrapper found — extract directly from this object
        return _descriptions_from_properties(schema["properties"], spec)

    # Case 2: Array with items
    if schema_type == "array" and "items" in schema:
        items_schema = _resolve_schema_deep(schema["items"], spec)
        if items_schema.get("properties"):
            return _descriptions_from_properties(items_schema["properties"], spec)

    return {}


def _descriptions_from_properties(
    properties: dict,
    spec: dict,
) -> dict[str, str]:
    """Extract description strings from a properties dict."""
    descriptions: dict[str, str] = {}
    for field_name, field_schema in properties.items():
        resolved = _resolve_schema_deep(field_schema, spec)
        desc = resolved.get("description", "")
        if desc:
            descriptions[field_name] = desc
    return descriptions
