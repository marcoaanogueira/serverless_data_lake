"""
PydanticAI agent for generating field descriptions from sample data.

This agent receives a sample record and the resource name, and returns
human-readable descriptions for each field.  It runs AFTER the code has
already fetched a real sample from the source API, so it generates
descriptions based on actual data values — not guesses from a spec.

Fields that already have descriptions (e.g., from the OpenAPI spec)
are excluded from the request to avoid overwriting authoritative info.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

from pydantic import BaseModel, Field
from pydantic_ai import Agent

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "bedrock:us.amazon.nova-2-lite-v1:0"

DESCRIPTION_SYSTEM_PROMPT = """\
You are a data engineer. Given a sample JSON record from an API endpoint \
and the resource name, generate a short, clear description for each field \
listed below.

Rules:
1. Each description should be 1 short sentence (max ~15 words).
2. Describe WHAT the field represents, not its type or format.
3. Use the field name, sample value, and resource context to infer meaning.
4. Be specific — "Unique identifier for the pet" is better than "An ID field".
5. If a field contains a URL, describe what it points to.
6. If a field contains a list/array, describe what the list contains.
7. If a field contains a date/timestamp, describe what event it represents.
8. Return descriptions ONLY for the fields listed. Do NOT add extra fields.
9. If you truly cannot determine what a field represents, use a generic \
   description like "Value of {field_name} for this {resource}".
"""


class FieldDescriptions(BaseModel):
    """Structured output from the description generator agent."""

    descriptions: dict[str, str] = Field(
        default_factory=dict,
        description="Mapping of field names to their generated descriptions.",
    )


@dataclass
class DescriptionDeps:
    """Dependencies injected into the description agent."""

    sample: dict
    resource_name: str
    fields_to_describe: list[str] = field(default_factory=list)


def create_description_agent() -> Agent[DescriptionDeps, FieldDescriptions]:
    """Create the PydanticAI agent that generates field descriptions."""
    model = os.environ.get("INGESTION_AGENT_MODEL", DEFAULT_MODEL)

    agent = Agent(
        model,
        deps_type=DescriptionDeps,
        output_type=FieldDescriptions,
        system_prompt=DESCRIPTION_SYSTEM_PROMPT,
        retries=2,
    )

    @agent.system_prompt
    async def inject_sample(ctx) -> str:
        # Show only the fields that need descriptions, with their sample values
        sample_subset = {
            k: v for k, v in ctx.deps.sample.items()
            if k in ctx.deps.fields_to_describe
        }
        return (
            f"\n--- Sample Record ---\n"
            f"Resource name: {ctx.deps.resource_name}\n"
            f"Fields needing descriptions: {ctx.deps.fields_to_describe}\n"
            f"Sample values: {sample_subset}\n\n"
            f"Generate a short description for each field listed above."
        )

    return agent


async def generate_field_descriptions(
    sample: dict,
    resource_name: str,
    fields_to_describe: list[str],
) -> dict[str, str]:
    """
    Use the description agent to generate descriptions for fields.

    Args:
        sample: A real record fetched from the API.
        resource_name: The resource/table name (e.g., "pets", "orders").
        fields_to_describe: List of field names that need descriptions.

    Returns:
        A dict mapping field names to generated descriptions.
    """
    if not fields_to_describe:
        return {}

    agent = create_description_agent()
    deps = DescriptionDeps(
        sample=sample,
        resource_name=resource_name,
        fields_to_describe=fields_to_describe,
    )

    result = await agent.run(
        "Generate descriptions for the listed fields.",
        deps=deps,
    )

    descriptions = result.output.descriptions

    # Filter out any hallucinated fields not in the original request
    valid_descriptions = {
        k: v for k, v in descriptions.items()
        if k in fields_to_describe
    }

    logger.info(
        "[%s] Description agent generated %d/%d field description(s).",
        resource_name,
        len(valid_descriptions),
        len(fields_to_describe),
    )

    return valid_descriptions
