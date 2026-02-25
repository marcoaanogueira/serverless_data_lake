import duckdb
import os
import re
import logging

import boto3
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum

from shared.schema_registry import SchemaRegistry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Security: SQL validation
# ---------------------------------------------------------------------------
MAX_QUERY_LENGTH = 10_000
MAX_RESULT_ROWS = 10_000

# Statements that are NOT allowed – anything besides SELECT
_BLOCKED_STATEMENTS = re.compile(
    r'\b('
    r'INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|TRUNCATE|MERGE'
    r'|GRANT|REVOKE|COMMIT|ROLLBACK|SAVEPOINT'
    r'|ATTACH|DETACH|INSTALL|LOAD|EXPORT|IMPORT'
    r'|COPY|CALL|SET|RESET|PRAGMA|CHECKPOINT|VACUUM'
    r'|CREATE\s+SECRET'
    r')\b',
    re.IGNORECASE,
)

# Functions/patterns that allow reading/writing arbitrary files or paths
_BLOCKED_FUNCTIONS = re.compile(
    r'\b('
    r'read_csv_auto|read_csv|read_parquet|read_json|read_json_auto'
    r'|read_blob|read_text|write_csv|write_parquet'
    r'|httpfs_|http_get|http_post'
    r'|glob|ls|copy'
    r')\s*\(',
    re.IGNORECASE,
)


def validate_query(sql: str) -> None:
    """Validate that a user-supplied SQL query is safe to execute.

    Raises HTTPException(400) if the query is rejected.
    """
    if len(sql) > MAX_QUERY_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"Query exceeds maximum length of {MAX_QUERY_LENGTH} characters.",
        )

    stripped = sql.strip().rstrip(";").strip()
    if not stripped:
        raise HTTPException(status_code=400, detail="Empty query.")

    # Only SELECT / WITH … SELECT are allowed
    if not re.match(r'\s*(SELECT|WITH)\b', stripped, re.IGNORECASE):
        raise HTTPException(
            status_code=400,
            detail="Only SELECT queries are allowed.",
        )

    if _BLOCKED_STATEMENTS.search(stripped):
        raise HTTPException(
            status_code=400,
            detail="Only SELECT queries are allowed. DDL/DML statements are blocked.",
        )

    if _BLOCKED_FUNCTIONS.search(stripped):
        raise HTTPException(
            status_code=400,
            detail="Direct file access functions are not allowed. Use table references like domain.silver.table instead.",
        )

AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
CATALOG_NAME = os.environ.get("CATALOG_NAME", "tadpole")
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "")

app = FastAPI()

# Add CORS middleware to handle preflight OPTIONS requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Setup directories before any DuckDB operations
HOME_DIR = "/tmp/duckdb"
EXTENSION_DIR = f"{HOME_DIR}/.duckdb/extensions"
os.makedirs(EXTENSION_DIR, exist_ok=True)

registry = SchemaRegistry()

logger.info(f"AWS_ACCOUNT_ID: {AWS_ACCOUNT_ID}")
logger.info(f"AWS_REGION: {AWS_REGION}")
logger.info(f"CATALOG_NAME: {CATALOG_NAME}")


def configure_duckdb():
    """Configure DuckDB with Glue Iceberg catalog via REST endpoint."""
    try:
        con = duckdb.connect(database=":memory:")

        # Set home and extension directory FIRST before loading any extensions
        con.execute(f"SET home_directory='{HOME_DIR}';")
        con.execute(f"SET extension_directory='{EXTENSION_DIR}';")

        # Now install and load extensions
        logger.info("Installing extensions...")
        con.execute("INSTALL httpfs;LOAD httpfs;")
        con.execute("INSTALL aws;LOAD aws;")
        con.execute("INSTALL iceberg;LOAD iceberg;")
        logger.info("Extensions loaded successfully")

        con.execute(
            """CREATE SECRET (
            TYPE S3,
            PROVIDER CREDENTIAL_CHAIN
        );"""
        )
        logger.info("Secret created")

        # Attach Glue Catalog via REST endpoint
        attach_sql = f"""
            ATTACH '{AWS_ACCOUNT_ID}' AS {CATALOG_NAME} (
                TYPE iceberg,
                ENDPOINT 'glue.{AWS_REGION}.amazonaws.com/iceberg',
                AUTHORIZATION_TYPE 'sigv4'
            );
        """
        logger.info(f"Attaching catalog with: {attach_sql}")
        con.execute(attach_sql)
        logger.info("Catalog attached successfully")

        return con
    except Exception as e:
        logger.error(f"Error configuring DuckDB: {str(e)}")
        raise


def _bronze_replacer(match: re.Match) -> str:
    """Replace domain.bronze.table with read_json_auto on the bronze S3 path."""
    domain = match.group(1)
    table = match.group(2)
    return (
        f"read_json_auto('s3://{BRONZE_BUCKET}/firehose-data/{domain}/{table}/**',"
        f" union_by_name=true, format='auto')"
    )


def rewrite_query(sql: str, catalog_name: str = "") -> str:
    """Rewrite user-friendly table references to DuckDB/Glue format.

    Transforms:
      domain.silver.table  →  catalog.domain_silver.table
      domain.gold.table    →  catalog.domain_gold.table
      domain.bronze.table  →  read_json_auto('s3://bucket/firehose-data/domain/table/**')
    """
    catalog = catalog_name or CATALOG_NAME
    # Bronze: read JSONL directly from S3
    sql = re.sub(
        r'\b(\w+)\.bronze\.(\w+)\b',
        _bronze_replacer,
        sql,
    )
    # Silver/Gold: Glue Iceberg catalog
    sql = re.sub(
        r'\b(\w+)\.(silver|gold)\.(\w+)\b',
        rf'{catalog}.\1_\2.\3',
        sql,
    )
    return sql


_BRONZE_S3_PATTERN = re.compile(
    r'No files found that match the pattern "s3://[^/]+/firehose-data/(\w+)/(\w+)/\*\*"'
)


_S3_PATH_PATTERN = re.compile(r's3://[^\s\'"]+')
_INTERNAL_PATH_PATTERN = re.compile(r'(/tmp/|/var/|/opt/|/home/)[^\s\'"]+')


def _friendly_error(message: str) -> str:
    """Rewrite cryptic DuckDB errors into user-friendly messages.

    Strips internal paths and S3 URIs to avoid leaking infrastructure details.
    """
    m = _BRONZE_S3_PATTERN.search(message)
    if m:
        domain, table = m.group(1), m.group(2)
        return f"Table '{domain}.bronze.{table}' does not exist or has no data."
    # Strip S3 paths and internal filesystem paths
    sanitized = _S3_PATH_PATTERN.sub('<redacted>', message)
    sanitized = _INTERNAL_PATH_PATTERN.sub('<redacted>', sanitized)
    return sanitized


@app.get("/consumption/query")
async def execute_query(sql: str = Query(..., description="SQL query to execute")):
    """Execute a SQL query against the Iceberg tables."""
    # --- Security: validate before doing anything ---
    validate_query(sql)

    try:
        con = configure_duckdb()
    except Exception as e:
        logger.error(f"Failed to configure DuckDB: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to initialize query engine.")

    rewritten_sql = rewrite_query(sql)
    try:
        result = con.execute(rewritten_sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchmany(MAX_RESULT_ROWS)
        truncated = len(rows) == MAX_RESULT_ROWS
    except Exception as e:
        logger.error(f"Query execution error: {str(e)}")
        raise HTTPException(status_code=400, detail=_friendly_error(str(e)))

    data = [dict(zip(columns, row)) for row in rows]
    response = {"data": data, "row_count": len(data)}
    if truncated:
        response["truncated"] = True
        response["max_rows"] = MAX_RESULT_ROWS
    return response


def _get_glue_columns(database: str, table_name: str) -> list[dict]:
    """Fetch column definitions from the Glue catalog for an Iceberg table."""
    try:
        glue = boto3.client("glue", region_name=AWS_REGION)
        resp = glue.get_table(DatabaseName=database, Name=table_name)
        glue_columns = resp["Table"].get("StorageDescriptor", {}).get("Columns", [])
        return [
            {"name": col["Name"], "type": col["Type"]}
            for col in glue_columns
        ]
    except Exception as e:
        logger.warning(f"Could not fetch Glue columns for {database}.{table_name}: {e}")
        return []


@app.get("/consumption/tables")
async def list_tables():
    """List all available silver and gold tables."""
    tables = []

    # Silver tables
    for st in registry.list_silver_tables():
        domain = st["domain"]
        name = st["name"]
        columns = _get_glue_columns(f"{domain}_silver", name)
        # Fallback to bronze schema if Glue has no columns yet
        if not columns:
            schema = registry.get(domain, name)
            if schema:
                columns = [
                    {"name": col.name, "type": col.type.value}
                    for col in schema.schema_def.columns
                ]
        tables.append({
            "name": name,
            "domain": domain,
            "layer": "silver",
            "location": st.get("location", ""),
            "columns": columns,
        })

    # Gold tables
    for job in registry.list_gold_jobs():
        domain = job.get("domain", "")
        name = job.get("job_name", job.get("name", ""))
        columns = _get_glue_columns(f"{domain}_gold", name)
        tables.append({
            "name": name,
            "domain": domain,
            "layer": "gold",
            "columns": columns,
        })

    return {"tables": tables, "count": len(tables)}


handler = Mangum(app, lifespan="off")
