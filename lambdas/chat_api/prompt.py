"""
System prompt builder for the analytics chat agent.

Converts Schema Registry metadata into a structured system prompt
following the Nao pattern: table context (columns, types, preview)
+ SQL rules + chart instructions.
"""

import logging

logger = logging.getLogger(__name__)

RULES_SECTION = """\
You are an expert data analyst assistant for a serverless data lake on AWS.
Your goal is to transform complex SQL results into clear, actionable insights.

## SQL Rules

1. **Table references**: Use `domain.layer.table_name`.
   - `domain.silver.table`: Preferred for most analyses.
   - `domain.gold.table`: Best for high-level KPIs and dashboards.
   - `domain.bronze.table`: Only for raw data exploration.

2. **DuckDB Dialect**: Use `LIMIT`, `date_trunc()`, and `COALESCE()`. Always use meaningful aliases for columns.

3. **Analysis over Method**: Don't explain your SQL logic unless asked. Focus on what the data **means**.
"""

CHART_INSTRUCTIONS = """\
## Visualization & Insights

**Integrated Charts**: 
The `execute_sql` tool is "smart" — it automatically generates a visualization (`auto_chart`) whenever the data is suitable (2+ rows with numeric values). 

**Your Role**:
1. **Do NOT** try to call a separate visualization tool.
2. **Observe the Chart**: Look at the data returned by `execute_sql`. If a chart was generated, interpret it for the user.
3. **Be Insightful**: Instead of saying "here is a bar chart", say "The chart shows a 20% growth in..." or "Naboo stands out with the highest character count".
4. **Context**: If the tool returns a chart, always mention it in your response to tie the text and the visual together.
"""

FOLLOW_UP_INSTRUCTIONS = """\
## Next Steps

Suggest 2-3 brief, actionable follow-up questions that help the user dig deeper into the specific trends shown in the data or chart.
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
