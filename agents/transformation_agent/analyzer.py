"""
PydanticAI-based transformation analyzer.

Receives table metadata (columns, types, descriptions) and sample data,
then generates a TransformationPlan with automated gold-layer pipelines.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

from pydantic_ai import Agent

from agents.transformation_agent.models import TableMetadata, TransformationPlan

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "bedrock:us.amazon.nova-2-lite-v1:0"

ANALYZER_SYSTEM_PROMPT = """\
You are an expert data engineer specialized in building analytical data pipelines \
for a medallion-architecture data lake (Bronze → Silver → Gold).

Your job is to analyze table metadata and sample data from the Silver layer \
and generate automated transformation pipelines (Gold layer jobs).

## INPUT
You will receive:
1. A list of NEWLY INGESTED tables (the tables the user just loaded).
2. A list of ALL EXISTING tables in the same domain (may include tables ingested previously).
3. For each table: column definitions (name, type, description) and sample rows.

## SQL SYNTAX
- Use simplified table references: `{domain}.silver.{table}` for Silver tables, \
  `{domain}.gold.{job_name}` for Gold tables.
- Use standard SQL (DuckDB dialect). DuckDB supports: UNNEST, LIST, STRUCT, \
  window functions, CTEs, QUALIFY, EXCLUDE, REPLACE, etc.
- IMPORTANT: Do NOT use functions or syntax that DuckDB does not support. \
  Stick to standard SQL with DuckDB extensions.
- For timestamps: use `CAST(col AS DATE)` or `DATE_TRUNC('day', col)` for grouping.
- For arrays/JSON stored as strings: use `json_extract` or `UNNEST(from_json(col, '["VARCHAR"]'))`.

## TRANSFORMATION CATEGORIES

Generate transformations in these categories, ordered by priority:

### 1. Basic Aggregations (ALWAYS generate these)
- Count by time period (daily/monthly counts per entity)
- Sum by time period (revenue, quantities, etc. — only for numeric columns that make sense)
- Distinct counts (unique entities per period)

### 2. Cross-Table Correlations (when tables share keys)
- JOINs between tables sharing foreign keys (e.g., people + planets via homeworld)
- Enriched/denormalized views combining attributes from multiple tables
- Counts/sums grouped by dimensions from related tables

### 3. Domain-Specific Analytics (when the data domain suggests them)
- **Finance/Sales**: revenue over time, cash flow, income statement (DRE), \
  average order value, customer lifetime value
- **Navigation/Web**: funnel analysis, conversion rates, session metrics
- **Operations**: throughput, utilization rates, operations vs revenue
- **HR/People**: headcount by department, turnover rates
- **Inventory**: stock levels, turnover rates, reorder points
- Use your judgment for other domains — think about what business questions \
  the data can answer.

### 4. Data Quality & Summary (optional, when useful)
- Completeness metrics (null rate per column)
- Summary statistics tables

## RULES

1. ONLY use tables that exist in the metadata provided. NEVER reference \
   tables that don't exist.
2. ONLY reference columns that exist in the table metadata. Check column \
   names carefully — use the exact names from the schema.
3. Every job MUST have a descriptive job_name in snake_case (e.g., \
   "daily_people_count", "revenue_by_planet").
4. Every job MUST have a description explaining what it does and why it's useful.
5. The `domain` field in every job MUST match the domain provided.
6. Use `write_mode: "overwrite"` for aggregation/summary tables. \
   Use `write_mode: "append"` with `unique_key` only for incremental event tables.
7. Use `cron_schedule: "day"` for daily aggregations, `"hour"` for near-real-time, \
   `"month"` for monthly summaries.
8. Focus on the NEWLY INGESTED tables as the primary subjects, but USE \
   existing domain tables for enrichment and correlation when they share keys.
9. Generate between 3 and 10 jobs depending on data complexity and correlation opportunities.
10. Provide a `rationale` explaining your overall approach and the correlations you found.
11. For `source_tables`, list ALL silver tables referenced in any of the generated queries.
12. If a column looks like a foreign key to another table (e.g., "homeworld" in people → \
    "url" in planets), create a JOIN-based transformation.
13. SQL queries must be syntactically valid. Always verify column names match the schema.
14. IMPORTANT: Return ONLY the structured TransformationPlan object. No explanations outside it.
"""


@dataclass
class AnalyzerDeps:
    """Dependencies injected into the PydanticAI analyzer agent."""

    domain: str
    ingested_tables: list[TableMetadata] = field(default_factory=list)
    existing_tables: list[TableMetadata] = field(default_factory=list)


def create_transformation_analyzer() -> Agent[AnalyzerDeps, TransformationPlan]:
    """
    Create the PydanticAI agent that analyzes table metadata and generates
    transformation pipelines.

    Model is configurable via TRANSFORMATION_AGENT_MODEL env var.
    """
    model = os.environ.get("TRANSFORMATION_AGENT_MODEL", DEFAULT_MODEL)
    logger.info("Using model: %s", model)

    agent = Agent(
        model,
        deps_type=AnalyzerDeps,
        output_type=TransformationPlan,
        system_prompt=ANALYZER_SYSTEM_PROMPT,
        retries=4,
    )

    @agent.system_prompt
    async def inject_table_context(ctx) -> str:
        """Inject table metadata and sample data into the prompt."""
        parts = [f"\n\n--- Domain: {ctx.deps.domain} ---\n"]

        # Newly ingested tables (primary focus)
        parts.append("\n## NEWLY INGESTED TABLES (primary focus)\n")
        for table in ctx.deps.ingested_tables:
            parts.append(_format_table_metadata(table))

        # Existing tables in the domain (for correlation)
        other_tables = [
            t for t in ctx.deps.existing_tables
            if t.name not in {it.name for it in ctx.deps.ingested_tables}
        ]
        if other_tables:
            parts.append("\n## EXISTING TABLES IN DOMAIN (available for correlation)\n")
            for table in other_tables:
                parts.append(_format_table_metadata(table))

        parts.append(
            f"\nGenerate a TransformationPlan for domain '{ctx.deps.domain}'. "
            f"Focus on the newly ingested tables but leverage existing tables "
            f"for cross-table correlations when they share keys or dimensions."
        )

        return "".join(parts)

    return agent


def _format_table_metadata(table: TableMetadata) -> str:
    """Format a table's metadata as a readable text block for the LLM."""
    lines = [f"\n### Table: {table.domain}.{table.layer}.{table.name}\n"]

    if table.row_count is not None:
        lines.append(f"Row count: ~{table.row_count}\n")

    # Column definitions
    lines.append("Columns:\n")
    for col in table.columns:
        col_type = col.get("type", "unknown")
        col_desc = col.get("description", "")
        pk = " [PK]" if col.get("primary_key") else ""
        req = " [required]" if col.get("required") else ""
        desc_str = f" — {col_desc}" if col_desc else ""
        lines.append(f"  - {col['name']}: {col_type}{pk}{req}{desc_str}\n")

    # Sample data
    if table.sample_data:
        lines.append(f"\nSample data ({len(table.sample_data)} rows):\n")
        for i, row in enumerate(table.sample_data[:5]):
            lines.append(f"  Row {i + 1}: {row}\n")

    return "".join(lines)


async def analyze_tables(
    domain: str,
    ingested_tables: list[TableMetadata],
    existing_tables: list[TableMetadata],
) -> TransformationPlan:
    """
    Analyze table metadata and generate a transformation plan.

    Args:
        domain: Business domain (e.g., "starwars", "sales").
        ingested_tables: Tables that were just ingested (primary focus).
        existing_tables: All tables in the domain (for correlation).

    Returns:
        TransformationPlan with gold-layer jobs.
    """
    analyzer = create_transformation_analyzer()
    deps = AnalyzerDeps(
        domain=domain,
        ingested_tables=ingested_tables,
        existing_tables=existing_tables,
    )

    result = await analyzer.run(
        "Analyze the table metadata and generate transformation pipelines "
        "for the gold layer. Focus on the newly ingested tables and leverage "
        "existing tables for cross-table correlations.",
        deps=deps,
    )

    plan = result.output

    # Ensure domain consistency
    for job in plan.jobs:
        job.domain = domain
    plan.domain = domain

    logger.info(
        "TransformationPlan generated: %d jobs for domain '%s' using tables %s",
        len(plan.jobs),
        domain,
        plan.source_tables,
    )

    return plan
