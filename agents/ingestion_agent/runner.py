"""
Ingestion Runner — fetches data from source APIs and forwards to the data lake.

Bridges the gap between the IngestionPlan (agent output) and the
serverless ingestion endpoint. For each GET endpoint in the plan:
  1. Calls the source API using httpx
  2. Extracts records from the response using data_path
  3. POSTs each record (or batch) to /ingest/{domain}/{resource_name}

Usage:
    # From a saved plan JSON file
    python -m agents.ingestion_agent.runner \
        --plan plan.json \
        --domain petstore \
        --ingestion-url https://your-api-gw.execute-api.region.amazonaws.com

    # Pipe directly from the agent
    python -m agents.ingestion_agent.main \
        --url https://petstore3.swagger.io/api/v3/openapi.json \
        --token "" --interests "pets" \
    | python -m agents.ingestion_agent.runner \
        --domain petstore \
        --ingestion-url http://localhost:8000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass, field
from typing import Any

import httpx

from agents.ingestion_agent.models import EndpointSpec, IngestionPlan

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data extraction helpers
# ---------------------------------------------------------------------------

def extract_data(response_json: Any, data_path: str) -> list[dict]:
    """
    Extract a list of records from an API response using a dot-separated path.

    Examples:
        data_path=""       → treat response as the data itself
        data_path="results" → response["results"]
        data_path="data.items" → response["data"]["items"]
    """
    data = response_json

    if data_path:
        for key in data_path.split("."):
            if isinstance(data, dict):
                data = data.get(key, [])
            else:
                return []

    # Normalise: always return a list of records
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


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

    endpoints: list[EndpointResult] = field(default_factory=list)

    @property
    def total_fetched(self) -> int:
        return sum(ep.records_fetched for ep in self.endpoints)

    @property
    def total_sent(self) -> int:
        return sum(ep.records_sent for ep in self.endpoints)

    @property
    def ok(self) -> bool:
        return all(ep.ok for ep in self.endpoints)

    def summary(self) -> dict:
        return {
            "total_endpoints": len(self.endpoints),
            "total_fetched": self.total_fetched,
            "total_sent": self.total_sent,
            "ok": self.ok,
            "endpoints": [
                {
                    "resource": ep.resource_name,
                    "fetched": ep.records_fetched,
                    "sent": ep.records_sent,
                    "errors": ep.errors,
                }
                for ep in self.endpoints
            ],
        }


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

async def fetch_endpoint(
    client: httpx.AsyncClient,
    base_url: str,
    endpoint: EndpointSpec,
    token: str = "",
) -> list[dict]:
    """Fetch data from a single source API endpoint."""
    url = base_url.rstrip("/") + endpoint.path
    headers: dict[str, str] = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    response = await client.get(url, params=endpoint.params, headers=headers)
    response.raise_for_status()

    return extract_data(response.json(), endpoint.data_path)


async def send_to_ingestion(
    client: httpx.AsyncClient,
    ingestion_url: str,
    domain: str,
    resource_name: str,
    records: list[dict],
    batch_size: int = 25,
) -> int:
    """
    Send records to the data lake ingestion endpoint.

    Uses the batch endpoint when possible for efficiency.
    Returns the number of records successfully sent.
    """
    sent = 0
    url = f"{ingestion_url.rstrip('/')}/ingest/{domain}/{resource_name}/batch"

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        response = await client.post(
            url,
            json=batch,
            params={"validate": "false"},
        )
        response.raise_for_status()
        body = response.json()
        sent += body.get("sent_count", len(batch))

    return sent


async def run(
    plan: IngestionPlan,
    domain: str,
    ingestion_url: str,
    token: str = "",
    batch_size: int = 25,
    timeout: float = 30.0,
) -> RunResult:
    """
    Execute an ingestion plan: fetch data from source APIs, forward to the data lake.

    Only GET endpoints are processed. POST/PUT/DELETE are skipped.

    Args:
        plan: The IngestionPlan generated by the ingestion agent.
        domain: Business domain for the data lake (e.g. "petstore").
        ingestion_url: Base URL of the ingestion API.
        token: Bearer token for the source API (not the ingestion API).
        batch_size: Number of records per batch POST.
        timeout: HTTP request timeout in seconds.

    Returns:
        RunResult with per-endpoint stats.
    """
    safe_plan = plan.get_only()
    result = RunResult()

    if not safe_plan.endpoints:
        logger.warning("No GET endpoints to process in the plan.")
        return result

    logger.info(
        "Running ingestion for %d GET endpoint(s) → %s",
        len(safe_plan.endpoints),
        ingestion_url,
    )

    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=timeout,
    ) as client:
        for ep in safe_plan.endpoints:
            ep_result = EndpointResult(resource_name=ep.resource_name)

            # 1. Fetch from source API
            try:
                records = await fetch_endpoint(
                    client, safe_plan.base_url, ep, token
                )
                ep_result.records_fetched = len(records)
                logger.info(
                    "[%s] Fetched %d records from %s",
                    ep.resource_name,
                    len(records),
                    ep.path,
                )
            except httpx.HTTPStatusError as exc:
                msg = f"HTTP {exc.response.status_code} fetching {ep.path}"
                logger.error("[%s] %s", ep.resource_name, msg)
                ep_result.errors.append(msg)
                result.endpoints.append(ep_result)
                continue
            except httpx.RequestError as exc:
                msg = f"Request error fetching {ep.path}: {exc}"
                logger.error("[%s] %s", ep.resource_name, msg)
                ep_result.errors.append(msg)
                result.endpoints.append(ep_result)
                continue

            if not records:
                logger.info("[%s] No records to send.", ep.resource_name)
                result.endpoints.append(ep_result)
                continue

            # 2. Forward to ingestion endpoint
            try:
                sent = await send_to_ingestion(
                    client,
                    ingestion_url,
                    domain,
                    ep.resource_name,
                    records,
                    batch_size,
                )
                ep_result.records_sent = sent
                logger.info(
                    "[%s] Sent %d records to %s/%s",
                    ep.resource_name,
                    sent,
                    domain,
                    ep.resource_name,
                )
            except httpx.HTTPStatusError as exc:
                msg = f"HTTP {exc.response.status_code} sending to ingestion"
                logger.error("[%s] %s", ep.resource_name, msg)
                ep_result.errors.append(msg)
            except httpx.RequestError as exc:
                msg = f"Request error sending to ingestion: {exc}"
                logger.error("[%s] %s", ep.resource_name, msg)
                ep_result.errors.append(msg)

            result.endpoints.append(ep_result)

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ingestion: fetch from source APIs and send to the data lake ingestion endpoint.",
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
        "--ingestion-url",
        required=True,
        help="Base URL of the ingestion API (e.g., https://abc.execute-api.us-east-1.amazonaws.com).",
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
        help="Records per batch POST (default: 25).",
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

    skipped = [ep for ep in plan.endpoints if ep.method.upper() != "GET"]
    if skipped:
        names = ", ".join(ep.resource_name for ep in skipped)
        logger.warning("Skipping non-GET endpoints: %s", names)

    result = asyncio.run(
        run(
            plan=plan,
            domain=args.domain,
            ingestion_url=args.ingestion_url,
            token=args.token,
            batch_size=args.batch_size,
        )
    )

    json.dump(result.summary(), sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")

    sys.exit(0 if result.ok else 1)


if __name__ == "__main__":
    main()
