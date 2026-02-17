"""
Transformation Runner — submits generated pipelines to the transform_jobs API.

Reads a TransformationPlan (agent output) and creates/triggers gold-layer jobs
in the data lake.

Flow:
  1. Read TransformationPlan from stdin or file
  2. For each job: POST /transform/jobs to create it
  3. Optionally trigger execution via POST /transform/jobs/{domain}/{job_name}/run
  4. Output summary JSON to stdout

Usage:
    # From a saved plan JSON file
    python -m agents.transformation_agent.runner \
        --plan plan.json \
        --api-url https://your-api-gw.execute-api.region.amazonaws.com

    # Pipe directly from the agent
    python -m agents.transformation_agent.main \
        --domain starwars --tables people planets films \
        --api-url https://your-api-gw.execute-api.region.amazonaws.com \
    | python -m agents.transformation_agent.runner \
        --api-url https://your-api-gw.execute-api.region.amazonaws.com

    # Create and immediately trigger execution
    python -m agents.transformation_agent.runner \
        --plan plan.json \
        --api-url https://your-api-gw.execute-api.region.amazonaws.com \
        --run
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass, field

import httpx

from agents.transformation_agent.models import TransformationPlan, TransformJob

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------


@dataclass
class JobResult:
    """Result of creating/running a single transform job."""

    job_name: str
    domain: str
    created: bool = False
    triggered: bool = False
    execution_id: str | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.created and self.error is None


@dataclass
class RunResult:
    """Aggregate result of submitting a transformation plan."""

    jobs_created: list[str] = field(default_factory=list)
    jobs_skipped: list[str] = field(default_factory=list)
    jobs_triggered: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0 and len(self.jobs_created) > 0

    def summary(self) -> dict:
        return {
            "ok": self.ok,
            "jobs_created": self.jobs_created,
            "jobs_skipped": self.jobs_skipped,
            "jobs_triggered": self.jobs_triggered,
            "total_created": len(self.jobs_created),
            "total_triggered": len(self.jobs_triggered),
            "errors": self.errors,
        }


# ---------------------------------------------------------------------------
# Job submission
# ---------------------------------------------------------------------------


async def create_job(
    client: httpx.AsyncClient,
    api_url: str,
    job: TransformJob,
) -> JobResult:
    """
    Create a transform job via POST /transform/jobs.

    If the job already exists (409 Conflict), attempts to update it
    via PUT /transform/jobs/{domain}/{job_name}.
    """
    base = api_url.rstrip("/")
    result = JobResult(job_name=job.job_name, domain=job.domain)

    payload = {
        "domain": job.domain,
        "job_name": job.job_name,
        "query": job.query,
        "write_mode": job.write_mode,
        "schedule_type": job.schedule_type,
        "cron_schedule": job.cron_schedule,
    }
    if job.unique_key:
        payload["unique_key"] = job.unique_key
    if job.dependencies:
        payload["dependencies"] = job.dependencies

    try:
        resp = await client.post(f"{base}/transform/jobs", json=payload)

        if resp.status_code in (200, 201):
            result.created = True
            logger.info("[%s] Job created successfully.", job.job_name)
        elif resp.status_code == 409:
            # Job already exists — try update
            logger.info(
                "[%s] Job already exists, updating...", job.job_name,
            )
            update_resp = await client.put(
                f"{base}/transform/jobs/{job.domain}/{job.job_name}",
                json=payload,
            )
            if update_resp.status_code in (200, 201):
                result.created = True
                logger.info("[%s] Job updated successfully.", job.job_name)
            else:
                result.error = (
                    f"Update failed: HTTP {update_resp.status_code} — "
                    f"{update_resp.text[:200]}"
                )
                logger.error("[%s] %s", job.job_name, result.error)
        else:
            result.error = (
                f"Create failed: HTTP {resp.status_code} — "
                f"{resp.text[:200]}"
            )
            logger.error("[%s] %s", job.job_name, result.error)

    except httpx.RequestError as exc:
        result.error = f"Request error: {exc}"
        logger.error("[%s] %s", job.job_name, result.error)

    return result


async def trigger_job(
    client: httpx.AsyncClient,
    api_url: str,
    domain: str,
    job_name: str,
) -> tuple[bool, str | None, str | None]:
    """
    Trigger execution of a job via POST /transform/jobs/{domain}/{job_name}/run.

    Returns (success, execution_id, error).
    """
    base = api_url.rstrip("/")
    try:
        resp = await client.post(
            f"{base}/transform/jobs/{domain}/{job_name}/run",
        )
        if resp.status_code in (200, 201, 202):
            data = resp.json()
            execution_id = data.get("execution_id")
            logger.info(
                "[%s] Execution triggered: %s", job_name, execution_id,
            )
            return True, execution_id, None
        return False, None, f"HTTP {resp.status_code} — {resp.text[:200]}"
    except httpx.RequestError as exc:
        return False, None, f"Request error: {exc}"


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def run(
    plan: TransformationPlan,
    api_url: str,
    trigger_execution: bool = False,
    timeout: float = 30.0,
) -> RunResult:
    """
    Submit a transformation plan to the transform_jobs API.

    For each job in the plan:
      1. Create the job via POST /transform/jobs
      2. Optionally trigger execution via POST .../run

    Args:
        plan: The TransformationPlan from the transformation agent.
        api_url: Base URL of the API gateway.
        trigger_execution: Whether to trigger job execution after creation.
        timeout: HTTP timeout for API calls.

    Returns:
        RunResult with creation and execution stats.
    """
    result = RunResult()

    if not plan.jobs:
        logger.warning("No jobs in the transformation plan.")
        return result

    async with httpx.AsyncClient(
        follow_redirects=True, timeout=timeout,
    ) as client:
        for job in plan.jobs:
            # Create/update the job
            job_result = await create_job(client, api_url, job)

            if job_result.ok:
                result.jobs_created.append(job.job_name)
            elif job_result.error:
                result.errors.append(f"[{job.job_name}] {job_result.error}")
                continue

            # Optionally trigger execution
            if trigger_execution and job_result.ok:
                success, exec_id, error = await trigger_job(
                    client, api_url, job.domain, job.job_name,
                )
                if success:
                    result.jobs_triggered.append(job.job_name)
                elif error:
                    result.errors.append(
                        f"[{job.job_name}] Trigger failed: {error}"
                    )

    logger.info(
        "Plan submitted: %d created, %d triggered, %d errors",
        len(result.jobs_created),
        len(result.jobs_triggered),
        len(result.errors),
    )

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Submit transformation pipelines: reads a TransformationPlan and "
            "creates gold-layer jobs via the transform_jobs API."
        ),
    )
    parser.add_argument(
        "--plan",
        default=None,
        help="Path to a TransformationPlan JSON file. If omitted, reads from stdin.",
    )
    parser.add_argument(
        "--api-url",
        required=True,
        help="Base URL of the API gateway (transform_jobs API).",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        default=False,
        help="Trigger job execution after creation.",
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

    plan = TransformationPlan.model_validate(plan_data)

    logger.info(
        "Loaded plan: %d jobs for domain '%s' (tables: %s)",
        len(plan.jobs),
        plan.domain,
        plan.source_tables,
    )

    result = asyncio.run(
        run(
            plan=plan,
            api_url=args.api_url,
            trigger_execution=args.run,
        )
    )

    json.dump(result.summary(), sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")

    sys.exit(0 if result.ok else 1)


if __name__ == "__main__":
    main()
