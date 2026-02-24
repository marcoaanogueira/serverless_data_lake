"""
Agent tools for the analytics chat.

- execute_sql: Runs SQL queries via the existing query_api Lambda
- display_chart: Returns a chart specification for the frontend to render
"""

import json
import logging
import os

import boto3
from strands.types.tools import tool

logger = logging.getLogger(__name__)

API_GATEWAY_ENDPOINT = os.environ.get("API_GATEWAY_ENDPOINT", "")
API_KEY_SECRET_ARN = os.environ.get("API_KEY_SECRET_ARN", "")

_api_key_cache: str | None = None


def _get_api_key() -> str:
    """Retrieve the internal API key from Secrets Manager (cached)."""
    global _api_key_cache
    if _api_key_cache:
        return _api_key_cache
    if not API_KEY_SECRET_ARN:
        return ""
    sm = boto3.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=API_KEY_SECRET_ARN)
    _api_key_cache = resp["SecretString"]
    return _api_key_cache


def _call_query_api(sql: str) -> dict:
    """Call the query_api Lambda via API Gateway."""
    import urllib.request
    import urllib.parse
    import urllib.error

    url = f"{API_GATEWAY_ENDPOINT}/consumption/query?sql={urllib.parse.quote(sql)}"
    headers = {"Content-Type": "application/json"}

    api_key = _get_api_key()
    if api_key:
        headers["x-api-key"] = api_key

    req = urllib.request.Request(url, headers=headers, method="GET")

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            detail = json.loads(body).get("detail", body)
        except (json.JSONDecodeError, AttributeError):
            detail = body
        return {"error": str(detail), "status_code": e.code}
    except urllib.error.URLError as e:
        return {"error": f"Connection error: {str(e)}"}


@tool
def execute_sql(query: str) -> dict:
    """Execute a SQL query against the data lake using DuckDB/Iceberg.

    Table references use the format domain.layer.table_name:
    - domain.bronze.table for raw JSON data
    - domain.silver.table for cleaned Iceberg tables
    - domain.gold.table for transformed tables

    Args:
        query: The SQL query to execute.

    Returns:
        Query results with columns and data rows, or an error message.
    """
    logger.info(f"Executing SQL: {query}")
    result = _call_query_api(query)

    if "error" in result:
        logger.warning(f"Query error: {result['error']}")
        return {"error": result["error"], "query": query}

    data = result.get("data", [])
    row_count = result.get("row_count", len(data))

    # Truncate large results to avoid token bloat
    max_rows = 200
    truncated = False
    if len(data) > max_rows:
        data = data[:max_rows]
        truncated = True

    response = {
        "data": data,
        "row_count": row_count,
        "columns": list(data[0].keys()) if data else [],
        "query": query,
    }
    if truncated:
        response["note"] = (
            f"Results truncated to {max_rows} rows. "
            f"Total rows: {row_count}. Add LIMIT to your query for better performance."
        )
    return response


@tool
def display_chart(
    chart_type: str,
    title: str,
    data: list[dict],
    x_key: str,
    y_keys: list[str],
    config: dict | None = None,
) -> dict:
    """Create a chart visualization for the frontend to render with Recharts.

    Use this tool when query results would benefit from a visual representation.
    The frontend will render the chart using the returned specification.

    Args:
        chart_type: Type of chart. One of: bar, line, area, pie, scatter.
        title: Chart title describing what the visualization shows.
        data: Array of data points. Each item is a dict with keys matching x_key and y_keys.
        x_key: The key in each data item to use for the X axis (or pie labels).
        y_keys: List of keys in each data item to use for Y axis values (or pie values).
        config: Optional configuration with keys like colors (list of hex), stacked (bool), labels (dict of axis labels).

    Returns:
        Chart specification that the frontend will render.
    """
    valid_types = {"bar", "line", "area", "pie", "scatter"}
    if chart_type not in valid_types:
        return {"error": f"Invalid chart_type '{chart_type}'. Must be one of: {valid_types}"}

    return {
        "type": "chart",
        "chart_type": chart_type,
        "title": title,
        "data": data,
        "x_key": x_key,
        "y_keys": y_keys,
        "config": config or {},
    }
