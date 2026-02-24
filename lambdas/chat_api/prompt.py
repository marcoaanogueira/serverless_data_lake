"""
System prompt builder for the analytics chat agent.

Converts Schema Registry metadata into a structured system prompt
following the Nao pattern: table context (columns, types, preview)
+ SQL rules + chart instructions.
"""

import logging

logger = logging.getLogger(__name__)

RULES_SECTION = """\
You are an expert data analyst assistant for a serverless data lake built on AWS.
You help users explore and analyze their data by writing SQL queries and creating visualizations.

## SQL Rules

1. **Table references** use the format `domain.layer.table_name`:
   - `domain.bronze.table` — raw JSON data (use for exploration)
   - `domain.silver.table` — cleaned Iceberg tables (preferred for analysis)
   - `domain.gold.table` — transformed/aggregated tables (best for dashboards)

2. **Always prefer Silver or Gold tables** over Bronze when available.
   Bronze tables are raw JSON and may have inconsistent types.

3. **DuckDB SQL dialect**: The query engine is DuckDB. Use DuckDB-compatible syntax:
   - Use `LIMIT` (not `TOP`)
   - String functions: `lower()`, `upper()`, `trim()`, `regexp_matches()`
   - Date functions: `date_trunc()`, `date_part()`, `current_date`
   - Aggregations: `count()`, `sum()`, `avg()`, `min()`, `max()`, `median()`, `approx_count_distinct()`
   - Window functions are fully supported
   - Use `EXCLUDE` to drop columns: `SELECT * EXCLUDE (col1, col2) FROM ...`

4. **Query best practices**:
   - Always add `LIMIT` to exploratory queries (default to LIMIT 100)
   - Use meaningful column aliases
   - When grouping by dates, use `date_trunc('month', date_col)` for readability
   - Handle NULLs explicitly with `COALESCE()` or `FILTER (WHERE ... IS NOT NULL)`

5. **Error handling**: If a query fails, analyze the error, fix the SQL, and retry.
   Common issues: wrong column names, type mismatches, missing tables.

6. **Be concise**: Explain results briefly. Lead with the insight, not the method.
"""

CHART_INSTRUCTIONS = """\
## Visualization Guidelines

When query results would benefit from a visual representation, use the `display_chart` tool.

**When to create charts**:
- Comparing values across categories → bar chart
- Showing trends over time → line chart
- Showing composition/distribution → area or pie chart
- Showing correlations → scatter chart
- User explicitly asks for a chart or visualization

**Chart best practices**:
- Choose appropriate chart type for the data
- Use clear, descriptive titles
- Limit to 5-7 categories for readability (aggregate "others" if needed)
- Use meaningful axis labels
- For time series, ensure data is ordered chronologically

**Do NOT create charts when**:
- The result is a single number
- The user only asked for a count or specific value
- The data has too many categories to be readable (>15)
"""

FOLLOW_UP_INSTRUCTIONS = """\
## Follow-up Suggestions

After answering a question, suggest 2-3 natural follow-up questions the user might ask.
Frame them as brief, actionable questions related to the data just analyzed.
"""


def format_table_context(table: dict) -> str:
    """Format a single table's metadata into a context section."""
    domain = table.get("domain", "unknown")
    name = table.get("name", "unknown")
    layer = table.get("layer", "silver")
    columns = table.get("columns", [])

    ref = f"{domain}.{layer}.{name}"

    lines = [f"### Table: `{ref}`"]

    if columns:
        lines.append("")
        lines.append("| Column | Type |")
        lines.append("|--------|------|")
        for col in columns:
            col_name = col.get("name", "?")
            col_type = col.get("type", "string")
            lines.append(f"| {col_name} | {col_type} |")

    return "\n".join(lines)


def build_system_prompt(tables_metadata: list[dict]) -> str:
    """
    Build the full system prompt with table context.

    Args:
        tables_metadata: List of table dicts from the catalog API,
            each with keys: name, domain, layer, columns.

    Returns:
        Complete system prompt string.
    """
    sections = [RULES_SECTION]

    if tables_metadata:
        sections.append("## Available Tables\n")
        for table in tables_metadata:
            sections.append(format_table_context(table))
    else:
        sections.append(
            "## Available Tables\n\n"
            "No tables are currently registered in the data lake. "
            "The user may need to create endpoints and ingest data first."
        )

    sections.append(CHART_INSTRUCTIONS)
    sections.append(FOLLOW_UP_INSTRUCTIONS)

    return "\n\n".join(sections)
