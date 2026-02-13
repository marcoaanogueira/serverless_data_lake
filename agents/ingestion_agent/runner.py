"""
Ingestion Runner — uses dlt to extract from source APIs and load into the data lake.

Bridges the IngestionPlan (agent output) to the serverless ingestion endpoint
using dlt (data load tool) for standardized extraction and loading.

Flow:
  1. Filter plan to GET-only endpoints
  2. Auto-create endpoints in the schema registry (POST /endpoints/infer + POST /endpoints)
  3. Build a dlt rest_api source from the plan
  4. Run a dlt pipeline with a custom destination that POSTs to /ingest/{domain}/{resource}/batch

Usage:
    # From a saved plan JSON file
    python -m agents.ingestion_agent.runner \
        --plan plan.json \
        --domain petstore \
        --api-url https://your-api-gw.execute-api.region.amazonaws.com

    # Pipe directly from the agent
    python -m agents.ingestion_agent.main \
        --url https://petstore3.swagger.io/api/v3/openapi.json \
        --token "" --interests "pets" \
    | python -m agents.ingestion_agent.runner \
        --domain petstore \
        --api-url http://localhost:8000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any

# ---------------------------------------------------------------------------
# Monkey-patch: dlt + Python 3.13 compatibility
# dlt iterates importlib.metadata.distributions() and crashes when a
# distribution has metadata=None.  We wrap distributions() to skip those.
# Must run BEFORE importing dlt.
# ---------------------------------------------------------------------------
import importlib.metadata as _meta

_orig_distributions = _meta.distributions


def _safe_distributions(**kwargs):  # type: ignore[no-untyped-def]
    return [d for d in _orig_distributions(**kwargs) if d.metadata is not None]


_meta.distributions = _safe_distributions  # type: ignore[assignment]
# ---------------------------------------------------------------------------

import dlt
import httpx
from dlt.sources.rest_api import rest_api_source

from agents.ingestion_agent.models import EndpointSpec, IngestionPlan

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data extraction helpers
# ---------------------------------------------------------------------------

def extract_data(response_json: Any, data_path: str) -> list[dict]:
    """
    Extract a list of records from an API response using a dot-separated path.

    Examples:
        data_path=""           → treat response as the data itself
        data_path="results"    → response["results"]
        data_path="data.items" → response["data"]["items"]
    """
    data = response_json

    if data_path:
        for key in data_path.split("."):
            if isinstance(data, dict):
                data = data.get(key, [])
            else:
                return []

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


# Well-known field names that typically hold the data array in paginated responses
_PREFERRED_DATA_KEYS = [
    "results", "data", "items", "records", "entries",
    "content", "hits", "objects", "rows", "values",
]


def detect_data_path(response_json: Any) -> tuple[str, list[dict]]:
    """
    Auto-detect the data array path in an API response.

    Examines the response structure to find where the actual data records
    live, automatically unwrapping pagination wrappers (count, next,
    previous, etc.) that the LLM often confuses with the real data.

    Strategy:
      1. If the response is already a list → data_path is ""
      2. Find all top-level keys whose value is a list of dicts
      3. If exactly one → that's our data
      4. If multiple → prefer well-known names (results, data, items, ...)
      5. If none at top level → check one level deeper
      6. Fallback → treat the whole response as a single record

    Returns:
        Tuple of (data_path, records) where data_path is the dot-separated
        path to the data array and records is the extracted list of dicts.
    """
    # Case 1: response is already a list
    if isinstance(response_json, list):
        if response_json and isinstance(response_json[0], dict):
            return "", response_json
        return "", response_json

    if not isinstance(response_json, dict):
        return "", []

    # Case 2: find top-level keys containing lists of dicts
    candidates: list[tuple[str, list[dict]]] = []
    for key, value in response_json.items():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            candidates.append((key, value))

    if len(candidates) == 1:
        return candidates[0][0], candidates[0][1]

    if len(candidates) > 1:
        # Prefer well-known data field names
        for preferred in _PREFERRED_DATA_KEYS:
            for key, value in candidates:
                if key == preferred:
                    return key, value
        # Fallback: pick the array with the most items
        candidates.sort(key=lambda x: len(x[1]), reverse=True)
        return candidates[0][0], candidates[0][1]

    # Case 3: no array-of-dicts at top level — check one level deeper
    for key, value in response_json.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                if (
                    isinstance(subvalue, list)
                    and subvalue
                    and isinstance(subvalue[0], dict)
                ):
                    return f"{key}.{subkey}", subvalue

    # Case 4: no nested arrays either — treat entire response as single record
    if response_json:
        return "", [response_json]
    return "", []


def detect_primary_key(sample: dict, resource_name: str = "") -> str | None:
    """
    Auto-detect the primary key from a sample record.

    Uses the same heuristic rules as the agent prompt, applied to real data:
      a) Field named exactly "id"
      b) Field named "{singular_resource}_id" (e.g., "person_id" for people)
      c) Exactly one field whose name ends with "_id"
      d) Field named "name" (natural key for entity resources)
      e) Field named "url" (some APIs like SWAPI use URL as unique identifier)

    Args:
        sample: A single record dict from the API response.
        resource_name: The resource name (e.g., "people", "planets") used
            to derive singular form for {resource}_id matching.

    Returns:
        The detected primary key field name, or None if no good candidate.
    """
    fields = set(sample.keys())

    # a) Explicit "id" field
    if "id" in fields:
        return "id"

    # b) {singular_resource}_id
    if resource_name:
        singular = resource_name.rstrip("s")  # simple depluralize
        candidate = f"{singular}_id"
        if candidate in fields:
            return candidate

    # c) Exactly one field ending with "_id"
    id_fields = [f for f in fields if f.endswith("_id")]
    if len(id_fields) == 1:
        return id_fields[0]

    # d) Field named "name"
    if "name" in fields:
        return "name"

    # e) Field named "url" (SWAPI, etc.)
    if "url" in fields:
        return "url"

    return None


# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------

@dataclass
class EndpointResult:
    """Result of ingesting one endpoint."""

    resource_name: str
    records_fetched: int = 0
    records_sent: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0


@dataclass
class RunResult:
    """Aggregate result of a full ingestion run."""

    endpoints_created: list[str] = field(default_factory=list)
    endpoints_skipped: list[str] = field(default_factory=list)
    pipeline_completed: bool = False
    errors: list[str] = field(default_factory=list)
    records_loaded: dict[str, int] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.pipeline_completed and len(self.errors) == 0

    @property
    def total_loaded(self) -> int:
        return sum(self.records_loaded.values())

    def summary(self) -> dict:
        return {
            "ok": self.ok,
            "endpoints_created": self.endpoints_created,
            "endpoints_skipped": self.endpoints_skipped,
            "pipeline_completed": self.pipeline_completed,
            "records_loaded": self.records_loaded,
            "total_loaded": self.total_loaded,
            "errors": self.errors,
        }


# ---------------------------------------------------------------------------
# Step 1: Auto-create endpoints in the schema registry
# ---------------------------------------------------------------------------

async def fetch_sample(
    client: httpx.AsyncClient,
    base_url: str,
    endpoint: EndpointSpec,
    token: str = "",
) -> tuple[dict | None, str]:
    """
    Fetch a single sample record from a source API endpoint.

    Auto-detects the data_path by examining the response structure instead of
    relying on the agent's (potentially wrong) data_path.  Falls back to the
    agent-provided data_path only if auto-detection finds nothing.

    Returns:
        Tuple of (sample_record, detected_data_path).
    """
    url = base_url.rstrip("/") + endpoint.path
    headers: dict[str, str] = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    response = await client.get(url, params=endpoint.params, headers=headers)
    response.raise_for_status()

    body = response.json()

    # Auto-detect where the real data lives
    detected_path, records = detect_data_path(body)

    if records:
        sample = records[0] if isinstance(records[0], dict) else None
        return sample, detected_path

    # Fallback: try agent's data_path (in case auto-detection missed it)
    if endpoint.data_path:
        fallback_records = extract_data(body, endpoint.data_path)
        if fallback_records:
            return fallback_records[0], endpoint.data_path

    return None, detected_path


async def infer_and_create_endpoint(
    client: httpx.AsyncClient,
    api_url: str,
    domain: str,
    endpoint: EndpointSpec,
    sample: dict,
) -> bool:
    """
    Infer schema from a sample record and create the endpoint in the registry.

    Returns True if the endpoint was created successfully.
    """
    base = api_url.rstrip("/")

    # 1. Infer schema from sample
    infer_resp = await client.post(
        f"{base}/endpoints/infer",
        json={"payload": sample},
    )
    infer_resp.raise_for_status()
    inferred = infer_resp.json()

    # 2. Resolve primary key: agent's choice → auto-detect from sample
    pk = endpoint.primary_key
    if not pk:
        pk = detect_primary_key(sample, endpoint.resource_name)
        if pk:
            logger.info(
                "[%s] Agent didn't set primary_key, auto-detected '%s' from sample.",
                endpoint.resource_name,
                pk,
            )

    # 3. Build column definitions from inferred schema
    columns = []
    for col in inferred["columns"]:
        col_def: dict[str, Any] = {
            "name": col["name"],
            "type": col["type"],
            "required": col.get("required", False),
            "primary_key": col.get("primary_key", False),
        }
        # Apply resolved primary key
        if pk and col["name"] == pk:
            col_def["primary_key"] = True
            col_def["required"] = True
        columns.append(col_def)

    # 3. Create endpoint
    create_resp = await client.post(
        f"{base}/endpoints",
        json={
            "name": endpoint.resource_name,
            "domain": domain,
            "mode": "manual",
            "columns": columns,
            "description": endpoint.description,
        },
    )
    create_resp.raise_for_status()
    return True


def _normalize_name(name: str) -> str:
    """Normalize an endpoint name by reducing common English plural suffixes."""
    # Handle compound names: normalize each segment independently
    parts = name.split("_")
    normalized = []
    for part in parts:
        if part.endswith("ies") and len(part) > 3:
            # abilities -> abilit(y), categories -> categor(y)
            part = part[:-3] + "y"
        elif part.endswith("ses") and len(part) > 3:
            # responses -> response
            part = part[:-1]
        elif part.endswith("s") and not part.endswith("ss") and len(part) > 2:
            # orders -> order, types -> type
            part = part[:-1]
        normalized.append(part)
    return "_".join(normalized)


def _find_similar_endpoint(
    name: str,
    existing_names: list[str],
    threshold: float = 0.8,
) -> str | None:
    """
    Find an existing endpoint name that is similar to the given name.

    Combines plural normalization with SequenceMatcher (Ratcliff/Obershelp)
    to handle common variations like singular/plural ("ability" vs "abilities"),
    typos, and minor suffix differences.

    Returns the best matching existing name if similarity >= threshold, else None.
    """
    norm_name = _normalize_name(name)
    best_match: str | None = None
    best_score = 0.0

    for existing in existing_names:
        norm_existing = _normalize_name(existing)
        # Compare normalized forms for better singular/plural handling
        score = SequenceMatcher(None, norm_name, norm_existing).ratio()
        if score >= threshold and score > best_score:
            best_score = score
            best_match = existing

    if best_match:
        logger.info(
            "[%s] Fuzzy match found: '%s' (similarity %.0f%%). Reusing existing endpoint.",
            name, best_match, best_score * 100,
        )

    return best_match


async def _fetch_existing_endpoints(
    client: httpx.AsyncClient,
    api_url: str,
    domain: str,
) -> list[str]:
    """Fetch names of all existing endpoints for a domain."""
    try:
        resp = await client.get(f"{api_url.rstrip('/')}/endpoints", params={"domain": domain})
        if resp.status_code == 200:
            return [ep["name"] for ep in resp.json()]
    except (httpx.RequestError, KeyError):
        logger.warning("Failed to fetch existing endpoints for domain '%s'.", domain)
    return []


async def setup_endpoints(
    plan: IngestionPlan,
    domain: str,
    api_url: str,
    token: str = "",
    timeout: float = 30.0,
) -> tuple[list[str], list[str], list[str]]:
    """
    Auto-create endpoints for all GET endpoints in the plan.

    For each endpoint:
      1. Checks for fuzzy name match against existing endpoints
      2. Fetches a sample record from the source API
      3. Infers schema via POST /endpoints/infer
      4. Creates the endpoint via POST /endpoints

    Returns:
        Tuple of (created, skipped, errors) lists of resource names.
    """
    created: list[str] = []
    skipped: list[str] = []
    errors: list[str] = []

    seen_names: set[str] = set()

    async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
        # Fetch existing endpoints once to enable fuzzy matching
        existing_names = await _fetch_existing_endpoints(client, api_url, domain)

        for ep in plan.get_endpoints:
            name = ep.resource_name

            # Skip duplicate resource names (LLM may generate multiple
            # endpoints with the same name)
            if name in seen_names:
                continue
            seen_names.add(name)

            # Check if endpoint already exists (exact match)
            if name in existing_names:
                logger.info("[%s] Endpoint already exists, skipping creation.", name)
                skipped.append(name)
                continue

            # Check for fuzzy match against existing endpoints
            similar = _find_similar_endpoint(name, existing_names)
            if similar:
                logger.info(
                    "[%s] Reusing existing endpoint '%s' instead of creating a near-duplicate.",
                    name, similar,
                )
                # Remap this endpoint to use the existing name in the plan
                ep.resource_name = similar
                skipped.append(similar)
                continue

            # Fetch sample from source API (auto-detects data_path)
            try:
                sample, detected_path = await fetch_sample(
                    client, plan.base_url, ep, token
                )
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                msg = f"[{name}] Failed to fetch sample: {exc}"
                logger.error(msg)
                errors.append(msg)
                continue

            if not sample:
                msg = f"[{name}] No sample data returned from {ep.path}"
                logger.warning(msg)
                errors.append(msg)
                continue

            # Propagate auto-detected data_path back to the endpoint
            # so dlt uses the correct data_selector
            if detected_path != ep.data_path:
                logger.info(
                    "[%s] Auto-detected data_path '%s' (agent had '%s')",
                    name,
                    detected_path,
                    ep.data_path,
                )
                ep.data_path = detected_path

            # Infer and create
            try:
                await infer_and_create_endpoint(client, api_url, domain, ep, sample)
                logger.info("[%s] Endpoint created in registry.", name)
                created.append(name)
            except httpx.HTTPStatusError as exc:
                msg = f"[{name}] Failed to create endpoint: HTTP {exc.response.status_code} — {exc.response.text}"
                logger.error(msg)
                errors.append(msg)
            except httpx.RequestError as exc:
                msg = f"[{name}] Request error creating endpoint: {exc}"
                logger.error(msg)
                errors.append(msg)

    return created, skipped, errors


# ---------------------------------------------------------------------------
# Step 2: Build dlt pipeline
# ---------------------------------------------------------------------------

def build_dlt_config(plan: IngestionPlan, token: str = "") -> dict:
    """
    Build a dlt rest_api source config from an IngestionPlan.

    Only includes GET endpoints. Deduplicates resources by name
    (keeps the first occurrence) since dlt rejects duplicate resource names.
    Adds auth token if provided.
    """
    safe_plan = plan.get_only()
    config = safe_plan.to_dlt_config()

    # Deduplicate resources by name — dlt raises ValueError on duplicates
    seen: set[str] = set()
    unique_resources: list[dict] = []
    for resource in config.get("resources", []):
        name = resource.get("name", "")
        if name not in seen:
            seen.add(name)
            unique_resources.append(resource)
        else:
            logger.warning(
                "Dropping duplicate resource '%s' (LLM generated multiple endpoints "
                "with the same name).",
                name,
            )
    config["resources"] = unique_resources

    # Set auth with token for dlt
    if token:
        config["client"]["auth"] = {
            "type": "bearer",
            "token": token,
        }
    else:
        config["client"].pop("auth", None)

    return config


def make_destination(api_url: str, domain: str, batch_size: int = 25):
    """
    Create a custom dlt destination that sends records to the ingestion API.

    Each batch of records extracted by dlt is POSTed to:
        POST /ingest/{domain}/{table_name}/batch
    """
    base = api_url.rstrip("/")

    @dlt.destination(
        batch_size=batch_size,
        loader_file_format="typed-jsonl",
        name="data_lake_ingestion",
        naming_convention="direct",
        skip_dlt_columns_and_tables=True,
    )
    def data_lake_destination(items: list[dict], table: dict) -> None:
        table_name = table["name"]
        url = f"{base}/ingest/{domain}/{table_name}/batch"

        # Convert dlt items to plain dicts, serializing DateTime → ISO string
        records = [
            {k: v.isoformat() if hasattr(v, "isoformat") else v for k, v in item.items()}
            for item in items
        ]

        response = httpx.post(
            url,
            json=records,
            params={"validate": "false"},
            timeout=30.0,
        )
        response.raise_for_status()

        body = response.json()
        sent = body.get("sent_count", len(records))
        logger.info(
            "[%s] dlt batch: sent %d/%d records",
            table_name,
            sent,
            len(records),
        )

    return data_lake_destination


def _extract_load_counts(info: Any) -> dict[str, int]:
    """Extract per-table load counts from a dlt LoadInfo object."""
    loaded: dict[str, int] = {}
    for package in getattr(info, "load_packages", []) or []:
        jobs = getattr(package, "jobs", None)
        if not jobs or not isinstance(jobs, dict):
            continue
        for job in jobs.get("completed_jobs", []) or []:
            table = getattr(job, "table_name", None)
            rows = getattr(job, "rows_count", 0)
            if table and not table.startswith("_dlt_"):
                loaded[table] = loaded.get(table, 0) + rows
    return loaded


def run_pipeline(
    plan: IngestionPlan,
    domain: str,
    api_url: str,
    token: str = "",
    batch_size: int = 25,
) -> dict[str, int]:
    """
    Run a dlt pipeline: rest_api source → data lake ingestion destination.

    Uses dev_mode=True so each run starts with a fresh schema, avoiding
    stale state from previous failed runs. If a primary_key column is
    missing from the data (UnboundColumnException), retries without
    primary keys — the LLM may have hallucinated a field name.

    Returns a dict of {resource_name: records_loaded}.
    """
    safe_plan = plan.get_only()
    if not safe_plan.endpoints:
        return {}

    config = build_dlt_config(plan, token)
    destination = make_destination(api_url, domain, batch_size)
    pipeline_name = f"{safe_plan.api_name}_{domain}"

    def _run(cfg: dict) -> Any:
        source = rest_api_source(cfg)
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dev_mode=True,
        )
        return pipeline.run(source)

    try:
        info = _run(config)
    except Exception as exc:
        exc_str = str(exc)
        if "UnboundColumnException" in exc_str:
            bad_keys = [
                r.get("primary_key", "?")
                for r in config.get("resources", [])
                if r.get("primary_key")
            ]
            logger.warning(
                "Primary key column(s) %s not found in data — "
                "retrying without primary keys.",
                bad_keys,
            )
            for resource in config.get("resources", []):
                resource.pop("primary_key", None)
            info = _run(config)
        elif "DictValidationException" in exc_str or "unexpected fields" in exc_str:
            logger.warning(
                "Paginator config rejected by dlt — retrying with 'auto' pagination."
            )
            config["client"]["paginator"] = "auto"
            info = _run(config)
        elif (
            "PageNumberPaginator" in exc_str
            or "OffsetPaginator" in exc_str
            or "Total" in exc_str and "not found in the response" in exc_str
            or "paginate_resource" in exc_str and "Paginator" in exc_str
        ):
            logger.warning(
                "Paginator failed during extraction (%s) — "
                "retrying with 'auto' pagination.",
                type(exc).__name__,
            )
            config["client"]["paginator"] = "auto"
            info = _run(config)
        else:
            raise

    return _extract_load_counts(info)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def run(
    plan: IngestionPlan,
    domain: str,
    api_url: str,
    token: str = "",
    batch_size: int = 25,
) -> RunResult:
    """
    Full ingestion run:
      1. Filter to GET-only endpoints
      2. Auto-create endpoints in registry (infer + create)
      3. Run dlt pipeline (rest_api source → ingestion destination)

    Args:
        plan: The IngestionPlan from the ingestion agent.
        domain: Business domain (e.g., "petstore", "sales").
        api_url: Base URL of the API gateway (endpoints + ingestion).
        token: Bearer token for the source API.
        batch_size: Records per batch sent to ingestion.

    Returns:
        RunResult with creation stats and pipeline results.
    """
    result = RunResult()
    safe_plan = plan.get_only()

    if not safe_plan.endpoints:
        logger.warning("No GET endpoints to process.")
        return result

    skipped_methods = [ep for ep in plan.endpoints if ep.method.upper() != "GET"]
    if skipped_methods:
        names = ", ".join(ep.resource_name for ep in skipped_methods)
        logger.warning("Skipping non-GET endpoints: %s", names)

    # Step 1: Auto-create endpoints
    logger.info("Setting up %d endpoint(s) in registry...", len(safe_plan.endpoints))
    created, skipped, errors = await setup_endpoints(
        safe_plan, domain, api_url, token
    )
    result.endpoints_created = created
    result.endpoints_skipped = skipped
    result.errors.extend(errors)

    # Step 2: Run dlt pipeline (skip if no endpoints were created or already existed)
    active_endpoints = created + skipped
    if not active_endpoints:
        msg = "Skipping dlt pipeline: no endpoints available in registry."
        logger.warning(msg)
        result.errors.append(msg)
        return result

    logger.info("Starting dlt pipeline for %s...", safe_plan.api_name)
    try:
        loaded = run_pipeline(safe_plan, domain, api_url, token, batch_size)
        result.records_loaded = loaded
        result.pipeline_completed = True
        logger.info("Pipeline completed. Records loaded: %s", loaded)
    except Exception as exc:
        msg = f"Pipeline failed: {exc}"
        logger.error(msg, exc_info=True)
        result.errors.append(msg)

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ingestion: dlt rest_api source → data lake ingestion endpoint.",
    )
    parser.add_argument(
        "--plan",
        default=None,
        help="Path to an IngestionPlan JSON file. If omitted, reads from stdin.",
    )
    parser.add_argument(
        "--domain",
        required=True,
        help="Business domain in the data lake (e.g., petstore, sales).",
    )
    parser.add_argument(
        "--api-url",
        required=True,
        help="Base URL of the API gateway (endpoints + ingestion APIs).",
    )
    parser.add_argument(
        "--token",
        default="",
        help="Bearer token for the source API.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=25,
        help="Records per batch POST to ingestion (default: 25).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=False,
        help="Enable verbose logging.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    # Load plan from file or stdin
    if args.plan:
        with open(args.plan) as f:
            plan_data = json.load(f)
    else:
        plan_data = json.load(sys.stdin)

    plan = IngestionPlan.model_validate(plan_data)

    result = asyncio.run(
        run(
            plan=plan,
            domain=args.domain,
            api_url=args.api_url,
            token=args.token,
            batch_size=args.batch_size,
        )
    )

    json.dump(result.summary(), sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")

    sys.exit(0 if result.ok else 1)


if __name__ == "__main__":
    main()
