import duckdb
import os
import logging

from fastapi import FastAPI, Query, HTTPException
from mangum import Mangum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
CATALOG_NAME = os.environ.get("CATALOG_NAME", "tadpole")

app = FastAPI()

# Setup directories before any DuckDB operations
HOME_DIR = "/tmp/duckdb"
EXTENSION_DIR = f"{HOME_DIR}/.duckdb/extensions"
os.makedirs(EXTENSION_DIR, exist_ok=True)

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


@app.get("/consumption/query")
async def execute_query(sql: str = Query(..., description="SQL query to execute")):
    """Execute a SQL query against the Iceberg tables."""
    con = configure_duckdb()
    result = con.execute(sql)
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()
    data = [dict(zip(columns, row)) for row in rows]
    return {"data": data, "row_count": len(data)}


@app.get("/consumption/tables")
async def list_tables():
    """List all available tables in the catalog."""
    con = configure_duckdb()
    result = con.execute("SHOW ALL TABLES;").fetchall()
    tables = [{"database": row[0], "schema": row[1], "name": row[2]} for row in result]
    return {"tables": tables}


handler = Mangum(app, lifespan="off")
