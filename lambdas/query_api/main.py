import duckdb
import os
import re
import logging

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum

from shared.schema_registry import SchemaRegistry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        f" union_by_name=true, format='newline_delimited')"
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


@app.get("/consumption/query")
async def execute_query(sql: str = Query(..., description="SQL query to execute")):
    """Execute a SQL query against the Iceberg tables."""
    con = configure_duckdb()
    sql = rewrite_query(sql)
    result = con.execute(sql)
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()
    data = [dict(zip(columns, row)) for row in rows]
    return {"data": data, "row_count": len(data)}


@app.get("/consumption/tables")
async def list_tables():
    """List all available silver tables from the schema registry."""
    silver_tables = registry.list_silver_tables()

    tables = []
    for st in silver_tables:
        # Get column definitions from the bronze schema
        schema = registry.get(st["domain"], st["name"])
        columns = []
        if schema:
            columns = [
                {"name": col.name, "type": col.type.value}
                for col in schema.schema_def.columns
            ]

        tables.append({
            "name": st["name"],
            "domain": st["domain"],
            "location": st["location"],
            "columns": columns,
        })

    return {"tables": tables, "count": len(tables)}


handler = Mangum(app, lifespan="off")
