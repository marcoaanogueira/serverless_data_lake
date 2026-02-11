"""
CLI entry point for the Lakehouse Ingestion Agent.

Supports two execution modes:
  1. Direct mode (default): Calls PydanticAI analyzer directly via async.
  2. Strands mode (--strands): Routes through the full Strands Agent orchestration.

Usage:
    # Direct async mode (recommended)
    python -m agents.ingestion_agent.main \
        --url https://petstore3.swagger.io/api/v3/openapi.json \
        --token "your-api-token" \
        --interests "pets" "store inventory"

    # Via Strands orchestration
    python -m agents.ingestion_agent.main \
        --url https://petstore3.swagger.io/api/v3/openapi.json \
        --token "your-api-token" \
        --interests "pets" "store inventory" \
        --strands
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lakehouse Ingestion Agent — generate dlt-compatible ingestion plans from OpenAPI specs",
    )
    parser.add_argument(
        "--url",
        required=True,
        help="URL to the OpenAPI/Swagger spec (JSON or YAML)",
    )
    parser.add_argument(
        "--token",
        default="",
        help="Bearer token for API authentication",
    )
    parser.add_argument(
        "--interests",
        nargs="+",
        required=True,
        help='Subjects of interest in natural language (e.g., "vendas" "clientes" "produtos")',
    )
    parser.add_argument(
        "--strands",
        action="store_true",
        default=False,
        help="Route through Strands Agent orchestration instead of direct async",
    )
    parser.add_argument(
        "--dlt-config",
        action="store_true",
        default=False,
        help="Output the dlt rest_api config dict instead of the raw IngestionPlan",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=False,
        help="Enable verbose logging",
    )
    return parser.parse_args()


async def _async_main(args: argparse.Namespace) -> dict:
    from agents.ingestion_agent.agent import run_ingestion_agent

    plan = await run_ingestion_agent(
        openapi_url=args.url,
        token=args.token,
        interests=args.interests,
    )

    if args.dlt_config:
        return plan.to_dlt_config()

    return plan.model_dump()


def _strands_main(args: argparse.Namespace) -> dict:
    from agents.ingestion_agent.agent import run_ingestion_agent_via_strands

    result = run_ingestion_agent_via_strands(
        openapi_url=args.url,
        token=args.token,
        interests=args.interests,
    )

    if args.dlt_config and "endpoints" in result:
        from agents.ingestion_agent.models import IngestionPlan

        plan = IngestionPlan.model_validate(result)
        return plan.to_dlt_config()

    return result


def main() -> None:
    args = _parse_args()

    log_level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    if args.strands:
        output = _strands_main(args)
    else:
        output = asyncio.run(_async_main(args))

    # Silent interface: only structured JSON to stdout
    json.dump(output, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
