"""
PydanticAI agent for identifying the primary key of an API resource.

This agent receives a sample record (dict) and the resource name, and
returns the most likely primary key field.  It runs AFTER the code has
already fetched a real sample from the source API, so it makes the
decision based on actual data — not guesses from an OpenAPI spec.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from pydantic import BaseModel, Field
from pydantic_ai import Agent

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "bedrock:us.amazon.nova-2-lite-v1:0"

PK_SYSTEM_PROMPT = """\
You are a data engineer. Given a sample JSON record from an API endpoint \
and the resource name, identify the single best field to use as the \
primary key for this table.

Rules (apply in order — return the FIRST match):
1. A field named exactly "id" (integer or string) — the most common PK.
2. A field named "{singular_resource}_id" (e.g., "person_id" for a \
   "people" resource, "film_id" for "films").
3. If exactly ONE field name ends with "_id", use it.
4. A field named "name" — natural key for entity resources (people, \
   planets, species, etc.).  Only use "name" when none of (1)-(3) matched.
5. A field named "url" that looks like a unique resource URL.
6. If nothing above matched, return null.

IMPORTANT:
- Return ONLY the field name (or null).  No explanations.
- The field you pick MUST exist in the sample record.
- Prefer numeric IDs over string fields when both are available.
- Do NOT invent field names that are not in the sample.
"""


class PrimaryKeyResult(BaseModel):
    """Structured output from the PK identifier agent."""

    primary_key: str | None = Field(
        default=None,
        description="The field name to use as primary key, or null if none found.",
    )


@dataclass
class PKDeps:
    """Dependencies injected into the PK agent."""

    sample: dict
    resource_name: str


def create_pk_agent() -> Agent[PKDeps, PrimaryKeyResult]:
    """Create the PydanticAI agent that identifies primary keys."""
    model = os.environ.get("INGESTION_AGENT_MODEL", DEFAULT_MODEL)

    agent = Agent(
        model,
        deps_type=PKDeps,
        output_type=PrimaryKeyResult,
        system_prompt=PK_SYSTEM_PROMPT,
        retries=2,
    )

    @agent.system_prompt
    async def inject_sample(ctx) -> str:
        fields = list(ctx.deps.sample.keys())
        sample_preview = {
            k: v for k, v in list(ctx.deps.sample.items())[:20]
        }
        return (
            f"\n--- Sample Record ---\n"
            f"Resource name: {ctx.deps.resource_name}\n"
            f"Fields: {fields}\n"
            f"Sample: {sample_preview}\n\n"
            f"Which field should be the primary key?"
        )

    return agent


async def identify_primary_key(
    sample: dict,
    resource_name: str,
) -> str | None:
    """
    Use the PK agent to identify the primary key from a sample record.

    Args:
        sample: A real record fetched from the API.
        resource_name: The resource/table name (e.g., "people", "films").

    Returns:
        The field name to use as PK, or None if the agent couldn't determine one.
    """
    agent = create_pk_agent()
    deps = PKDeps(sample=sample, resource_name=resource_name)

    result = await agent.run(
        "Identify the primary key field for this resource.",
        deps=deps,
    )

    pk = result.output.primary_key

    # Validate that the agent returned a field that actually exists
    if pk and pk not in sample:
        logger.warning(
            "[%s] PK agent returned '%s' but it's not in the sample fields %s. Ignoring.",
            resource_name,
            pk,
            list(sample.keys()),
        )
        return None

    logger.info(
        "[%s] PK agent identified primary_key='%s'",
        resource_name,
        pk,
    )
    return pk
