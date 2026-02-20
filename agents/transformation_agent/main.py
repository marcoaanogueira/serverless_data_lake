"""
CLI entry point for the Lakehouse Transformation Agent.

Reads table metadata from the endpoints API, samples data from the query API,
and generates a TransformationPlan with gold-layer pipelines.

Supports two input modes:
  1. Explicit tables: pass --tables with table names
  2. Piped from ingestion runner: reads the ingestion result from stdin
     and extracts the ingested table names automatically

Usage:
    # Explicit tables
    python -m agents.transformation_agent.main \
        --domain starwars \
        --tables people planets films \
        --api-url https://your-api-gw.execute-api.region.amazonaws.com

    # Full pipeline: ingestion → transformation (piped)
    python -m agents.ingestion_agent.main \
        --url https://swapi.dev/api/ \
        --token "" \
        --interests "people" "planets" "films" \
    | python -m agents.ingestion_agent.runner \
        --domain starwars \
        --api-url https://your-api-gw.execute-api.region.amazonaws.com \
    | python -m agents.transformation_agent.main \
        --domain starwars \
        --api-url https://your-api-gw.execute-api.region.amazonaws.com \
    | python -m agents.transformation_agent.runner \
        --api-url https://your-api-gw.execute-api.region.amazonaws.com
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys

import httpx

from agents.transformation_agent.models import TableMetadata

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Metadata fetching
# ---------------------------------------------------------------------------


async def fetch_endpoint_metadata(
    client: httpx.AsyncClient,
    api_url: str,
    domain: str,
    table_name: str,
) -> dict | None:
    """
    Fetch endpoint metadata (schema) for a specific table.

    Calls GET /endpoints/{domain}/{table_name} and returns the endpoint
    response including column definitions.
    """
    base = api_url.rstrip("/")
    try:
        resp = await client.get(f"{base}/endpoints/{domain}/{table_name}")
        if resp.status_code == 200:
            return resp.json()
        logger.warning(
            "[%s/%s] Endpoint metadata not found (HTTP %d)",
            domain, table_name, resp.status_code,
        )
    except httpx.RequestError as exc:
        logger.warning(
            "[%s/%s] Failed to fetch endpoint metadata: %s",
            domain, table_name, exc,
        )
    return None


async def fetch_endpoint_schema(
    client: httpx.AsyncClient,
    api_url: str,
    domain: str,
    table_name: str,
) -> list[dict]:
    """
    Fetch column definitions from the endpoint's YAML schema.

    Calls GET /endpoints/{domain}/{table_name}/yaml to get the raw schema.
    """
    base = api_url.rstrip("/")
    try:
        resp = await client.get(f"{base}/endpoints/{domain}/{table_name}/yaml")
        if resp.status_code == 200:
            import yaml
            schema_data = yaml.safe_load(resp.text)
            columns = schema_data.get("schema", {}).get("columns", [])
            return columns
    except Exception as exc:
        logger.warning(
            "[%s/%s] Failed to fetch schema YAML: %s",
            domain, table_name, exc,
        )
    return []


async def fetch_all_domain_endpoints(
    client: httpx.AsyncClient,
    api_url: str,
    domain: str,
) -> list[dict]:
    """Fetch all endpoints in a domain via GET /endpoints?domain={domain}."""
    base = api_url.rstrip("/")
    try:
        resp = await client.get(f"{base}/endpoints", params={"domain": domain})
        if resp.status_code == 200:
            return resp.json()
    except httpx.RequestError as exc:
        logger.warning(
            "[%s] Failed to list domain endpoints: %s", domain, exc,
        )
    return []


async def fetch_domain_tables(
    client: httpx.AsyncClient,
    api_url: str,
    domain: str,
) -> list[dict]:
    """
    Fetch all tables (silver + gold) in a domain via GET /consumption/tables.

    Returns tables filtered by domain.
    """
    base = api_url.rstrip("/")
    try:
        resp = await client.get(f"{base}/consumption/tables")
        if resp.status_code == 200:
            data = resp.json()
            tables = data.get("tables", [])
            return [t for t in tables if t.get("domain") == domain]
    except httpx.RequestError as exc:
        logger.warning(
            "[%s] Failed to fetch consumption tables: %s", domain, exc,
        )
    return []


async def sample_table(
    client: httpx.AsyncClient,
    api_url: str,
    domain: str,
    table_name: str,
    layer: str = "silver",
    limit: int = 5,
    max_retries: int = 3,
    retry_base_delay: float = 10.0,
) -> tuple[list[dict], int | None]:
    """
    Sample rows from a table via the query API.

    For silver tables that may not yet exist (still being written by the
    Processing Lambda after Kinesis Firehose flushes to Bronze), retries
    with exponential backoff before giving up.

    Retry schedule (silver only, default settings):
        attempt 1 — immediate
        attempt 2 — wait 10 s
        attempt 3 — wait 20 s
        attempt 4 — wait 40 s  (total wait ≤ 70 s)

    Returns (rows, row_count) where either may be empty/None if the table
    is unavailable after all attempts.
    """
    base = api_url.rstrip("/")

    # Only retry for silver tables; gold tables are pre-created jobs and
    # should already exist if referenced.
    attempts = max_retries + 1 if layer == "silver" else 1

    sql = f"SELECT * FROM {domain}.{layer}.{table_name} LIMIT {limit}"
    rows: list[dict] = []
    sample_ok = False

    for attempt in range(attempts):
        try:
            resp = await client.get(
                f"{base}/consumption/query",
                params={"sql": sql},
                timeout=30.0,
            )
            if resp.status_code == 200:
                data = resp.json()
                rows = data.get("data", [])
                sample_ok = True
                break
            else:
                if attempt < attempts - 1:
                    delay = retry_base_delay * (2 ** attempt)
                    logger.info(
                        "[%s.%s.%s] Silver table not ready yet (HTTP %d, "
                        "attempt %d/%d) — retrying in %.0fs...",
                        domain, layer, table_name, resp.status_code,
                        attempt + 1, attempts, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.warning(
                        "[%s.%s.%s] Sample query failed after %d attempt(s) (HTTP %d): %s",
                        domain, layer, table_name, attempts,
                        resp.status_code, resp.text[:200],
                    )
        except httpx.RequestError as exc:
            if attempt < attempts - 1:
                delay = retry_base_delay * (2 ** attempt)
                logger.info(
                    "[%s.%s.%s] Sample query error (attempt %d/%d) — "
                    "retrying in %.0fs: %s",
                    domain, layer, table_name, attempt + 1, attempts, delay, exc,
                )
                await asyncio.sleep(delay)
            else:
                logger.warning(
                    "[%s.%s.%s] Sample query request error after %d attempt(s): %s",
                    domain, layer, table_name, attempts, exc,
                )

    if not sample_ok:
        # Table is not accessible — skip count to avoid a redundant failing call.
        return rows, None

    # Fetch approximate row count (single attempt — table is confirmed accessible)
    count_sql = f"SELECT COUNT(*) as cnt FROM {domain}.{layer}.{table_name}"
    row_count: int | None = None
    try:
        resp = await client.get(
            f"{base}/consumption/query",
            params={"sql": count_sql},
            timeout=30.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            count_rows = data.get("data", [])
            if count_rows:
                row_count = count_rows[0].get("cnt")
    except httpx.RequestError:
        pass

    return rows, row_count


# ---------------------------------------------------------------------------
# Build table metadata
# ---------------------------------------------------------------------------


async def build_table_metadata(
    client: httpx.AsyncClient,
    api_url: str,
    domain: str,
    table_name: str,
    layer: str = "silver",
) -> TableMetadata:
    """
    Build complete table metadata by combining endpoint schema and sample data.
    """
    # Fetch column definitions from the endpoint schema
    columns = await fetch_endpoint_schema(client, api_url, domain, table_name)

    # If no columns from schema, try the consumption/tables endpoint
    if not columns:
        tables = await fetch_domain_tables(client, api_url, domain)
        for t in tables:
            if t.get("name") == table_name and t.get("layer") == layer:
                columns = t.get("columns", [])
                break

    # Fetch sample data and row count
    sample_data, row_count = await sample_table(
        client, api_url, domain, table_name, layer,
    )

    # If still no columns but we have sample data, infer from samples
    if not columns and sample_data:
        columns = [
            {"name": k, "type": _infer_type(v)}
            for k, v in sample_data[0].items()
        ]

    return TableMetadata(
        name=table_name,
        domain=domain,
        layer=layer,
        columns=columns,
        sample_data=sample_data,
        row_count=row_count,
    )


def _infer_type(value) -> str:
    """Infer a simple type string from a Python value."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "json"
    return "string"


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def generate_plan(
    domain: str,
    tables: list[str],
    api_url: str,
    timeout: float = 30.0,
) -> dict:
    """
    Generate a transformation plan for the given domain and tables.

    Steps:
      1. Fetch metadata for each ingested table (columns + sample data)
      2. Fetch all other tables in the domain (for correlation)
      3. Send everything to the PydanticAI analyzer
      4. Return the TransformationPlan as a dict

    Args:
        domain: Business domain (e.g., "starwars").
        tables: List of recently ingested table names.
        api_url: Base URL of the API gateway.
        timeout: HTTP timeout for API calls.

    Returns:
        TransformationPlan as a dictionary.
    """
    from agents.transformation_agent.analyzer import analyze_tables

    async with httpx.AsyncClient(
        follow_redirects=True, timeout=timeout,
    ) as client:
        # Step 1: Build metadata for each ingested table
        logger.info("Fetching metadata for %d ingested table(s)...", len(tables))
        ingested_metadata = []
        for table_name in tables:
            meta = await build_table_metadata(
                client, api_url, domain, table_name,
            )
            ingested_metadata.append(meta)
            logger.info(
                "[%s] Metadata: %d columns, %d sample rows, ~%s total rows",
                table_name,
                len(meta.columns),
                len(meta.sample_data),
                meta.row_count or "?",
            )

        # Step 2: Fetch all tables in the domain for correlation
        logger.info("Fetching all tables in domain '%s'...", domain)
        all_endpoints = await fetch_all_domain_endpoints(
            client, api_url, domain,
        )
        domain_tables = await fetch_domain_tables(client, api_url, domain)

        # Build metadata for existing tables not in the ingested set
        ingested_names = set(tables)
        existing_metadata = list(ingested_metadata)  # start with ingested

        for ep in all_endpoints:
            ep_name = ep.get("name", "")
            if ep_name and ep_name not in ingested_names:
                meta = await build_table_metadata(
                    client, api_url, domain, ep_name,
                )
                existing_metadata.append(meta)
                logger.info(
                    "[%s] Existing table: %d columns, %d sample rows",
                    ep_name, len(meta.columns), len(meta.sample_data),
                )

        # Also include gold-layer tables from consumption/tables
        for t in domain_tables:
            t_name = t.get("name", "")
            t_layer = t.get("layer", "")
            if t_layer == "gold" and t_name:
                gold_meta = TableMetadata(
                    name=t_name,
                    domain=domain,
                    layer="gold",
                    columns=t.get("columns", []),
                )
                existing_metadata.append(gold_meta)

    # Step 3: Generate transformation plan via LLM
    logger.info(
        "Analyzing %d ingested + %d total tables for transformations...",
        len(ingested_metadata),
        len(existing_metadata),
    )

    plan = await analyze_tables(
        domain=domain,
        ingested_tables=ingested_metadata,
        existing_tables=existing_metadata,
    )

    return plan.model_dump()


# ---------------------------------------------------------------------------
# Ingestion result parsing (for piped input)
# ---------------------------------------------------------------------------


def extract_tables_from_ingestion_result(ingestion_result: dict) -> list[str]:
    """
    Extract ingested table names from an ingestion runner result.

    The ingestion runner outputs:
    {
        "ok": true,
        "endpoints_created": ["people", "planets", "films"],
        "endpoints_skipped": ["species"],
        "records_loaded": {"people": 82, "planets": 60, "films": 7},
        ...
    }

    Tables come from endpoints_created + endpoints_skipped (both exist in the
    registry and have data). We also include tables from records_loaded as a
    fallback, since that's the definitive list of tables that received data.
    """
    tables: list[str] = []
    seen: set[str] = set()

    # Primary: endpoints that were created or already existed
    for name in ingestion_result.get("endpoints_created", []):
        if name not in seen:
            tables.append(name)
            seen.add(name)
    for name in ingestion_result.get("endpoints_skipped", []):
        if name not in seen:
            tables.append(name)
            seen.add(name)

    # Fallback: tables that actually received data
    for name in ingestion_result.get("records_loaded", {}):
        if name not in seen:
            tables.append(name)
            seen.add(name)

    return tables


def _is_ingestion_result(data: dict) -> bool:
    """Check if a dict looks like an ingestion runner result."""
    return (
        "endpoints_created" in data
        or "records_loaded" in data
        or ("ok" in data and "pipeline_completed" in data)
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Lakehouse Transformation Agent — generate gold-layer pipelines "
            "from ingested table metadata.\n\n"
            "Tables can be specified via --tables or piped from the ingestion runner."
        ),
    )
    parser.add_argument(
        "--domain",
        required=True,
        help="Business domain (e.g., starwars, sales)",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=None,
        help=(
            'Ingested table names (e.g., "people" "planets" "films"). '
            "If omitted, reads from stdin (ingestion runner output)."
        ),
    )
    parser.add_argument(
        "--api-url",
        required=True,
        help="Base URL of the API gateway",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=False,
        help="Enable verbose logging",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    log_level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    # Resolve table names: explicit --tables or piped from ingestion runner
    tables = args.tables
    if tables is None:
        if sys.stdin.isatty():
            logger.error(
                "No --tables provided and stdin is a terminal. "
                "Either pass --tables or pipe ingestion runner output."
            )
            sys.exit(1)

        stdin_data = json.load(sys.stdin)

        if _is_ingestion_result(stdin_data):
            tables = extract_tables_from_ingestion_result(stdin_data)
            logger.info(
                "Read ingestion result from stdin: %d table(s) — %s",
                len(tables), tables,
            )
        else:
            logger.error(
                "Stdin JSON is not a recognized ingestion result. "
                "Expected keys: endpoints_created, records_loaded. "
                "Got: %s",
                list(stdin_data.keys()),
            )
            sys.exit(1)

    if not tables:
        logger.error("No tables to process.")
        sys.exit(1)

    output = asyncio.run(
        generate_plan(
            domain=args.domain,
            tables=tables,
            api_url=args.api_url,
        )
    )

    # Silent interface: only structured JSON to stdout
    json.dump(output, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
